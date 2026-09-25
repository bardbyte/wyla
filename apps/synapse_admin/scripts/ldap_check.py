#!/usr/bin/env python3
"""LDAP preflight for the Synapse sign-in: can we reach the directory over
LDAPS, bind as the service account, and see the base DN the app searches?

Per environment the script does:

  1. dns        Resolve the host (fails off the corporate network / VPN).
  2. connect    Open the LDAPS connection; report the server certificate
                (subject, issuer, days to expiry) and the TLS version.
  3. bind       Simple bind as LDAP_USER with LDAP_PASSWORD. If the plain
                user name is refused and it is not already a DN, UPN or
                DOMAIN\\user, the script also tries user@<domain from the base
                DN's DC parts> and <first DC>\\user, and reports which form the
                directory accepted (that is what the app must send). Active
                Directory's "data 52e / 525 / 533 / 775 ..." codes are
                translated (bad password, no such user, disabled, locked, ...).
  4. server     Root DSE facts: default naming context, host, LDAP versions.
  5. base dn    A base-scope read of LDAP_BASE_DN; noSuchObject means the DN
                does not exist on this server.
  6. lookup     A subtree search under the base DN for LDAP_LOOKUP (default:
                the bind user's short name) by sAMAccountName / UPN / uid / cn,
                proving that searches work and showing the DN that came back.

--inventory answers "what do we actually get from the directory" for that
person: every attribute on the entry (the identity ones with their values,
the rest by name), the account flags (userAccountControl decoded), the
direct groups from memberOf and the full nested set through Active
Directory's LDAP_MATCHING_RULE_IN_CHAIN, the manager's entry (name and
e-mail: the approval flow's approver), a scan for a band or level attribute
(title, extensionAttribute1..15), and a verdict on each thing the app could
use: an e-mail (mail vs userPrincipalName, and whether they agree), a stable
key (employeeID, objectGUID), a manager, groups. LDAP_GROUPS_PREFIX lists the
groups whose name starts so (for the role map). --redact masks personal
values; --out writes the JSON for identity_map.py.

Configuration comes from environment variables; NAME_<ENV> wins over NAME, so
a file with the plain deployment names (LDAP_SERVER, LDAP_BASE_DN, ...) works
for a single environment as well:

  AUTHCHECK_ENVS          comma list of environments, default E1,E2,E3
  LDAP_SERVER_<ENV>       ldaps://host:636 (ldap:// for a plain connection)
  LDAP_BASE_DN_<ENV>
  LDAP_USER_<ENV>         the bind identity as the deployment has it
  LDAP_PASSWORD_<ENV>     without it the script stops after the TLS check
  LDAP_BIND_DN_<ENV>      optional: an exact bind identity to use instead
  LDAP_LOOKUP_<ENV>       optional: an account name to search for under the base DN
  LDAP_GROUPS_PREFIX      --inventory: list the groups named like this prefix, e.g. Sem-
  LDAP_CA_CERT            PEM bundle with the corporate root (LDAPS certificates
                          are usually signed by an internal CA)
  LDAP_TLS_INSECURE=1     skip certificate verification, loudly

Requires the `ldap3` package:  pip install ldap3

Usage:
  python ldap_check.py --env-file .env.authcheck.local
  python ldap_check.py --envs E3 --json
  python ldap_check.py --env-file .env.authcheck.local --envs E1 --inventory --redact --out ldap.json

Exit code: 0 everything passed, 1 something FAILED, 2 configuration problem.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import ssl
import sys
import time
import urllib.parse
from dataclasses import asdict, dataclass

try:
    import ldap3
    from ldap3 import ALL, ALL_ATTRIBUTES, BASE, SIMPLE, SUBTREE, Connection, Server, Tls
    from ldap3.core.exceptions import LDAPException
    from ldap3.utils.conv import escape_filter_chars
except ImportError:  # reported in main(), so --help still works
    ldap3 = None

DEFAULT_ENVS = "E1,E2,E3"
_TIMEOUT = 15

# Active Directory's sub-codes on an invalidCredentials bind result.
AD_DATA = {
    "525": "no such user",
    "52e": "invalid credentials (wrong password, or this user form is not accepted)",
    "530": "not permitted to log on at this time",
    "531": "not permitted to log on from this workstation",
    "532": "password expired",
    "533": "account disabled",
    "701": "account expired",
    "773": "user must reset password",
    "775": "account locked out",
}


# --------------------------------------------------------------- config --
def load_env_file(path: str) -> int:
    """Load KEY=VALUE lines into os.environ. Values already exported win."""
    loaded = 0
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[7:]
            key, val = line.split("=", 1)
            key, val = key.strip(), val.split(" #", 1)[0].strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]
            if key and key not in os.environ:
                os.environ[key] = val
                loaded += 1
    return loaded


def cfg(name: str, env: str | None = None, default: str | None = None) -> str | None:
    """NAME_<ENV> first, then NAME; an empty value counts as unset."""
    if env:
        val = os.environ.get(f"{name}_{env}", "").strip()
        if val:
            return val
    val = os.environ.get(name, "").strip()
    return val or default


def split_list(value: str) -> list[str]:
    return [x for x in re.split(r"[\s,]+", value.strip()) if x]


# --------------------------------------------------------------- report --
@dataclass
class Check:
    env: str
    check: str
    status: str  # PASS FAIL WARN SKIP INFO
    detail: str


class Report:
    def __init__(self) -> None:
        self.checks: list[Check] = []
        self.facts: dict = {}

    def add(self, env: str, check: str, status: str, detail: str = "") -> None:
        self.checks.append(Check(env, check, status, detail))

    @property
    def failed(self) -> bool:
        return any(c.status == "FAIL" for c in self.checks)

    @property
    def inventoried(self) -> list[str]:
        """The environments a person was actually inventoried in (empty: the
        report holds the preflight and the provider's published metadata only)."""
        return sorted((self.facts.get("inventory") or {}).keys())

    def dump(self) -> dict:
        return {"ok": not self.failed, "checks": [asdict(c) for c in self.checks],
                "facts": self.facts, "inventory": self.inventoried,
                "note": ("inventory for " + ", ".join(self.inventoried) if self.inventoried else
                         "preflight and published metadata only: nobody signed in, so no claim, "
                         "token or attribute values are in this report; add --inventory")}


def print_report(rep: Report, title: str) -> None:
    w_env = max([3] + [len(c.env) for c in rep.checks])
    w_chk = max([5] + [len(c.check) for c in rep.checks])
    print(f"\n{title}\n{'=' * len(title)}")
    print(f"{'ENV':<{w_env}}  {'CHECK':<{w_chk}}  {'STATUS':<6}  DETAIL")
    for c in rep.checks:
        print(f"{c.env:<{w_env}}  {c.check:<{w_chk}}  {c.status:<6}  {c.detail}")
    counts: dict[str, int] = {}
    for c in rep.checks:
        counts[c.status] = counts.get(c.status, 0) + 1
    print("\n" + "  ".join(f"{k}={v}" for k, v in sorted(counts.items()))
          + ("\nRESULT: FAIL" if rep.failed else "\nRESULT: OK"))
    if rep.inventoried:
        print(f"INVENTORY: a person was signed in / looked up in {', '.join(rep.inventoried)}; "
              "the id_token.*, userinfo.*, attr.* and need: rows above are that person's real values")
    else:
        print("INVENTORY: not run. The rows above are the preflight and the provider's published "
              "metadata; no claim, token or attribute VALUES are in this report. "
              "Run again with --inventory to see what the provider returns for a person.")


# ------------------------------------------------------------------ ldap --
def parse_server(url: str) -> tuple[str, str, int]:
    """'ldaps://host:636' -> (scheme, host, port); bare 'host' means ldaps."""
    if "://" not in url:
        url = "ldaps://" + url
    parts = urllib.parse.urlsplit(url)
    scheme = (parts.scheme or "ldaps").lower()
    port = parts.port or (636 if scheme == "ldaps" else 389)
    return scheme, parts.hostname or "", port


def dc_domain(base_dn: str) -> str:
    dcs = [p.split("=", 1)[1].strip() for p in base_dn.split(",")
           if p.strip().upper().startswith("DC=")]
    return ".".join(dcs)


def bind_candidates(user: str, bind_dn: str | None, base_dn: str) -> list[str]:
    if bind_dn:
        return [bind_dn]
    cands = [user]
    if not any(ch in user for ch in "@\\="):
        domain = dc_domain(base_dn)
        if domain:
            cands.append(f"{user}@{domain}")
            cands.append(f"{domain.split('.')[0]}\\{user}")
    return cands


# ------------------------------------------------------------ inventory --
REDACT = False
INVENTORY = False
# under --redact these stay readable: they name things, not people
KEEP_UNDER_REDACT = {"objectclass", "memberof", "groups", "groups_nested", "useraccountcontrol",
                     "flags", "c", "co", "attributes_present", "dn_ou"}
# the identity attributes the app could read, in the order they are shown
IDENTITY_ATTRIBUTES = ("sAMAccountName", "userPrincipalName", "mail", "proxyAddresses",
                       "displayName", "givenName", "sn", "cn", "employeeID", "employeeNumber",
                       "employeeType", "objectGUID", "objectSid", "title", "department",
                       "division", "company", "manager", "physicalDeliveryOfficeName", "l", "st",
                       "c", "co", "telephoneNumber", "mobile", "description", "whenCreated",
                       "whenChanged", "lastLogonTimestamp", "pwdLastSet", "accountExpires",
                       "userAccountControl", "memberOf")
UAC_FLAGS = {0x2: "ACCOUNTDISABLE", 0x10: "LOCKOUT", 0x20: "PASSWD_NOTREQD",
             0x40: "PASSWD_CANT_CHANGE", 0x200: "NORMAL_ACCOUNT", 0x10000: "DONT_EXPIRE_PASSWORD",
             0x40000: "SMARTCARD_REQUIRED", 0x400000: "DONT_REQ_PREAUTH",
             0x800000: "PASSWORD_EXPIRED", 0x1000000: "TRUSTED_TO_AUTH_FOR_DELEGATION"}
IN_CHAIN = "1.2.840.113556.1.4.1941"


def uac_flags(value) -> list[str]:
    try:
        bits = int(str(value))
    except (TypeError, ValueError):
        return []
    return [name for bit, name in sorted(UAC_FLAGS.items()) if bits & bit]


def rdn(dn: str) -> str:
    """'CN=Sem-Stewards,OU=Groups,DC=corp,...' -> 'Sem-Stewards'."""
    first = str(dn).split(",", 1)[0]
    return first.split("=", 1)[1] if "=" in first else first


def mask(value, key: str = ""):
    """Personal values under --redact: an e-mail keeps its domain, a string its
    shape and length, a list its length; names of things stay."""
    if not REDACT or value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, list):
        return [mask(v, key) for v in value]
    if isinstance(value, dict):
        return {k: mask(v, k) for k, v in value.items()}
    if key.lower() in KEEP_UNDER_REDACT:
        return value
    text = str(value)
    if "@" in text and " " not in text:
        local, _, domain = text.partition("@")
        return f"{local[:1]}***@{domain}"
    if text.upper().startswith("CN="):
        return "CN=***," + text.split(",", 1)[1] if "," in text else "CN=***"
    if len(text) <= 3:
        return "***"
    return f"{text[:2]}...{text[-1]}({len(text)})"


def show(value) -> str:
    if isinstance(value, list):
        inner = ", ".join(str(v) for v in value[:8]) + (", ..." if len(value) > 8 else "")
        return f"list[{len(value)}]: {inner}"
    return f"{type(value).__name__}: {value}"


def attribute_values(entry, name: str):
    """The attribute's values as plain Python, or None when absent."""
    if name not in entry:
        return None
    values = entry[name].values
    if not values:
        return None
    out = [v.isoformat() if hasattr(v, "isoformat") else (v.decode("utf-8", "replace")
           if isinstance(v, bytes) else v) for v in values]
    return out[0] if len(out) == 1 else out


def inventory(env: str, rep: Report, conn, server, base_dn: str, entry_dn: str) -> None:
    """Everything the directory holds on this person, and the verdicts."""
    ok = conn.search(entry_dn, "(objectClass=*)", search_scope=BASE, attributes=[ALL_ATTRIBUTES])
    if not ok or not conn.entries:
        rep.add(env, "inventory", "FAIL", f"cannot read {entry_dn}: {conn.result.get('description')}")
        return
    entry = conn.entries[0]
    present = sorted(entry.entry_attributes, key=str.lower)
    rep.add(env, "inventory", "PASS", f"{len(present)} attributes on {mask(entry_dn, 'dn')}")
    person: dict = {"provider": "ldap", "dn": entry_dn,
                    "dn_ou": ",".join(entry_dn.split(",")[1:]), "attributes_present": present}
    shown = set()
    for name in IDENTITY_ATTRIBUTES:
        value = attribute_values(entry, name)
        if value is None:
            continue
        shown.add(name.lower())
        person[name] = value
        if name == "userAccountControl":
            flags = uac_flags(value)
            person["flags"] = flags
            rep.add(env, f"attr.{name}", "INFO", f"{value}: {', '.join(flags)}")
        elif name == "memberOf":
            groups = [rdn(g) for g in (value if isinstance(value, list) else [value])]
            person["groups"] = groups
            rep.add(env, "attr.memberOf", "INFO",
                    f"{len(groups)} direct group(s): {', '.join(groups[:15])}{', ...' if len(groups) > 15 else ''}")
        else:
            rep.add(env, f"attr.{name}", "INFO", show(mask(value, name)))
    others = [a for a in present if a.lower() not in shown]
    if others:
        rep.add(env, "attr.others", "INFO", f"{len(others)} more: {', '.join(others)}")

    # nested groups: Active Directory's matching rule walks the chain server-side
    nested: list[str] = []
    try:
        root = ((getattr(server.info, "other", {}) or {}).get("defaultNamingContext") or [base_dn])[0]
        flt = f"(&(objectClass=group)(member:{IN_CHAIN}:={escape_filter_chars(entry_dn)}))"
        for page in conn.extend.standard.paged_search(root, flt, search_scope=SUBTREE,
                                                      attributes=["cn"], paged_size=500,
                                                      generator=True):
            if page.get("type") == "searchResEntry":
                nested.append(str(page["attributes"].get("cn") or rdn(page["dn"])))
    except LDAPException as exc:
        rep.add(env, "groups (nested)", "WARN", f"the in-chain rule failed: {type(exc).__name__}: {exc}")
    if nested:
        direct = set(person.get("groups") or [])
        extra = sorted(set(nested) - direct)
        person["groups_nested"] = sorted(set(nested))
        rep.add(env, "groups (nested)", "INFO",
                f"{len(set(nested))} in total through nesting; {len(extra)} beyond memberOf"
                + (f": {', '.join(extra[:10])}{', ...' if len(extra) > 10 else ''}" if extra else ""))

    # the manager: the approval flow's approver
    manager_dn = person.get("manager")
    if manager_dn:
        ok = conn.search(str(manager_dn), "(objectClass=*)", search_scope=BASE,
                         attributes=["displayName", "mail", "sAMAccountName", "title", "userPrincipalName"])
        if ok and conn.entries:
            m = conn.entries[0]
            manager = {k: attribute_values(m, k) for k in ("displayName", "mail", "sAMAccountName",
                                                          "title", "userPrincipalName")}
            person["manager_entry"] = manager
            rep.add(env, "manager", "PASS",
                    f"{mask(manager.get('displayName'), 'displayName')} <{mask(manager.get('mail'), 'mail')}>"
                    f" ({mask(manager.get('title'), 'title')})")
        else:
            rep.add(env, "manager", "WARN", f"manager DN set but unreadable: {conn.result.get('description')}")
    else:
        rep.add(env, "manager", "WARN", "no manager attribute: the approval flow needs another source for the approver")

    # a band or level: title, or one of the extension attributes
    band_hits = []
    for name in present:
        low = name.lower()
        if low.startswith("extensionattribute") or low in ("title", "employeetype", "description", "info"):
            value = attribute_values(entry, name)
            if value is not None and re.search(r"\b(band|level|grade|B\d{2}|L\d{1,2}|\d{2})\b",
                                               str(value), re.IGNORECASE):
                band_hits.append(f"{name}={mask(value, name)}")
    ext = [a for a in present if a.lower().startswith("extensionattribute")]
    person["extension_attributes"] = {a: mask(attribute_values(entry, a), a) for a in ext}
    rep.add(env, "band / level", "INFO",
            ("candidates: " + "; ".join(band_hits[:6])) if band_hits else
            (f"nothing that reads as a band; extension attributes present: {', '.join(ext)}" if ext
             else "nothing that reads as a band, and no extension attributes"))

    # the join keys
    mail = str(person.get("mail") or "").lower()
    upn = str(person.get("userPrincipalName") or "").lower()
    sam = str(person.get("sAMAccountName") or "")
    if mail and upn:
        rep.add(env, "need: email", "PASS" if mail == upn else "WARN",
                f"mail and userPrincipalName {'agree' if mail == upn else 'DIFFER'}: "
                f"{mask(mail, 'mail')} | {mask(upn, 'upn')}"
                + ("" if mail == upn else " (the Okta e-mail claim will match one of them; check which)"))
    elif mail or upn:
        rep.add(env, "need: email", "WARN", f"only {'mail' if mail else 'userPrincipalName'} is set")
    else:
        rep.add(env, "need: email", "FAIL", "neither mail nor userPrincipalName is set")
    keys = [k for k in ("employeeID", "employeeNumber", "objectGUID") if person.get(k)]
    rep.add(env, "need: stable key", "PASS" if keys else "WARN",
            f"{', '.join(keys)} present (objectGUID never changes; employeeID follows HR)" if keys
            else "no employeeID, employeeNumber or objectGUID readable")
    rep.add(env, "need: username", "PASS" if sam else "WARN",
            f"sAMAccountName {mask(sam, 'sam')}" if sam else "no sAMAccountName")
    flags = person.get("flags") or []
    rep.add(env, "need: account active", "FAIL" if "ACCOUNTDISABLE" in flags else "PASS",
            "disabled in the directory" if "ACCOUNTDISABLE" in flags else "enabled"
            + (", locked out" if "LOCKOUT" in flags else ""))

    # groups by prefix, for the role map
    prefix = cfg("LDAP_GROUPS_PREFIX", env)
    if prefix:
        flt = f"(&(objectClass=group)(cn={escape_filter_chars(prefix)}*))"
        ok = conn.search(base_dn, flt, search_scope=SUBTREE, attributes=["cn", "description", "member"],
                         size_limit=200)
        names = []
        for g in conn.entries if ok else []:
            members = len(g.member.values) if "member" in g else 0
            names.append(f"{g.cn.value} ({members} members)")
        person["groups_by_prefix"] = names
        rep.add(env, f"groups {prefix}*", "INFO" if names else "WARN",
                f"{len(names)}: {'; '.join(names[:20])}" if names else f"no group named {prefix}* under {base_dn}")

    rep.facts.setdefault("inventory", {})[env] = mask(person)


def ad_reason(result: dict) -> str:
    desc = str(result.get("description", ""))
    msg = str(result.get("message", ""))
    m = re.search(r"data ([0-9a-fA-F]{3})", msg)
    if m and m.group(1).lower() in AD_DATA:
        return f"{desc} (AD data {m.group(1)}: {AD_DATA[m.group(1).lower()]})"
    return f"{desc}: {msg[:120]}" if msg else desc


def cert_summary(sock) -> tuple[str, int | None]:
    """(one-line certificate summary, days until expiry) for an SSL socket."""
    if not isinstance(sock, ssl.SSLSocket):
        return "plain connection (no TLS)", None
    cert = sock.getpeercert() or {}
    if not cert:
        return f"{sock.version()}; certificate details unavailable (verification is off)", None
    subject = dict(x[0] for x in cert.get("subject", ()))
    issuer = dict(x[0] for x in cert.get("issuer", ()))
    days = None
    if cert.get("notAfter"):
        days = int((ssl.cert_time_to_seconds(cert["notAfter"]) - time.time()) // 86400)
    sans = [v for k, v in cert.get("subjectAltName", ()) if k == "DNS"]
    return (f"{sock.version()}; CN={subject.get('commonName', '?')}; "
            f"issuer={issuer.get('commonName') or issuer.get('organizationName', '?')}; "
            f"expires in {days} days; SAN={','.join(sans[:4])}{'...' if len(sans) > 4 else ''}"), days


def run_env(env: str, rep: Report) -> None:
    server_url = cfg("LDAP_SERVER", env)
    base_dn = cfg("LDAP_BASE_DN", env)
    user = cfg("LDAP_USER", env)
    missing = [n for n, v in (("LDAP_SERVER", server_url), ("LDAP_BASE_DN", base_dn),
                              ("LDAP_USER", user)) if not v]
    if missing:
        rep.add(env, "config", "SKIP", "not set: " + ", ".join(f"{m}_{env}" for m in missing))
        return
    password = cfg("LDAP_PASSWORD", env)
    bind_dn = cfg("LDAP_BIND_DN", env)
    lookup = cfg("LDAP_LOOKUP", env) or user.split("\\")[-1].split("@")[0]
    ca_cert = cfg("LDAP_CA_CERT", env)
    insecure = os.environ.get("LDAP_TLS_INSECURE") == "1" or os.environ.get(
        "AUTHCHECK_TLS_INSECURE") == "1"

    scheme, host, port = parse_server(server_url)
    if not host:
        rep.add(env, "config", "FAIL", f"cannot parse LDAP_SERVER_{env}={server_url}")
        return

    # 1. dns
    try:
        addrs = sorted({ai[4][0] for ai in socket.getaddrinfo(host, port)})
    except socket.gaierror as e:
        rep.add(env, "dns", "FAIL",
                f"{host}: {e} (this host resolves only on the corporate network / VPN)")
        return
    rep.add(env, "dns", "PASS", f"{host} -> {', '.join(addrs[:4])}")

    # 2. connect
    tls = None
    if scheme == "ldaps":
        if insecure:
            print(f"!! {env}: LDAP certificate verification is OFF", file=sys.stderr)
        tls = Tls(validate=ssl.CERT_NONE if insecure else ssl.CERT_REQUIRED,
                  ca_certs_file=ca_cert or None)
    server = Server(host, port=port, use_ssl=(scheme == "ldaps"), tls=tls, get_info=ALL,
                    connect_timeout=_TIMEOUT)
    conn = Connection(server, receive_timeout=_TIMEOUT, raise_exceptions=False)
    t0 = time.time()
    try:
        conn.open()
    except (LDAPException, OSError) as e:
        why = f"{type(e).__name__}: {e}"
        hint = ""
        if "CERTIFICATE_VERIFY_FAILED" in why or "certificate" in why.lower():
            hint = " (internal CA? point LDAP_CA_CERT at the corporate root bundle)"
        rep.add(env, "connect", "FAIL", f"{scheme}://{host}:{port}: {why}{hint}")
        return
    ms = int((time.time() - t0) * 1000)
    summary, days = cert_summary(conn.socket)
    status = "WARN" if days is not None and days < 30 else "PASS"
    rep.add(env, "connect", status, f"{scheme}://{host}:{port} in {ms} ms; {summary}")

    if not password:
        rep.add(env, "bind", "SKIP", f"LDAP_PASSWORD_{env} not set; stopping after the TLS check")
        conn.unbind()
        return

    # 3. bind
    bound_as = None
    attempts = []
    for cand in bind_candidates(user, bind_dn, base_dn):
        conn.user, conn.password, conn.authentication = cand, password, SIMPLE
        try:
            ok = conn.bind()
        except LDAPException as e:
            attempts.append(f"{cand}: {type(e).__name__}: {e}")
            continue
        if ok:
            bound_as = cand
            break
        attempts.append(f"{cand}: {ad_reason(conn.result)}")
    if not bound_as:
        rep.add(env, "bind", "FAIL", "; ".join(attempts))
        conn.unbind()
        return
    note = "" if bound_as == user else f" (the plain '{user}' was refused; the app must send this form)"
    rep.add(env, "bind", "PASS", f"bound as {bound_as}{note}")
    try:
        who = conn.extend.standard.who_am_i()
        if who:
            rep.add(env, "whoami", "INFO", str(who))
    except LDAPException:
        pass

    # 4. server facts
    info = server.info
    if info is not None:
        other = getattr(info, "other", {}) or {}
        first = lambda k: (other.get(k) or [""])[0]  # noqa: E731
        rep.add(env, "server", "INFO",
                f"host {first('dnsHostName') or host}; default naming context "
                f"{first('defaultNamingContext') or '?'}; LDAP v"
                f"{','.join(str(v) for v in (info.supported_ldap_versions or []))}; "
                f"naming contexts {len(info.naming_contexts or [])}")
        rep.facts[env] = {"naming_contexts": list(info.naming_contexts or []),
                          "default_naming_context": first("defaultNamingContext"),
                          "dns_host_name": first("dnsHostName")}

    # 5. base dn
    ok = conn.search(base_dn, "(objectClass=*)", search_scope=BASE, attributes=["objectClass"])
    if ok and conn.entries:
        classes = [str(c) for c in conn.entries[0].objectClass.values]
        rep.add(env, "base dn", "PASS", f"{base_dn} exists ({', '.join(classes[-2:])})")
    else:
        rep.add(env, "base dn", "FAIL",
                f"{base_dn}: {conn.result.get('description')} "
                f"{str(conn.result.get('message', ''))[:100]}".strip()
                + (" -> the base DN does not exist on this server; check LDAP_BASE_DN"
                   if conn.result.get("description") == "noSuchObject" else ""))
        conn.unbind()
        return

    # 6. lookup
    name = escape_filter_chars(lookup)
    flt = (f"(|(sAMAccountName={name})(userPrincipalName={name}@*)(uid={name})(cn={name})"
           f"(mail={name}))")
    ok = conn.search(base_dn, flt, search_scope=SUBTREE, size_limit=3,
                     attributes=["distinguishedName", "cn", "memberOf"])
    if ok and conn.entries:
        entry = conn.entries[0]
        dn = entry.entry_dn
        groups = len(entry.memberOf.values) if "memberOf" in entry else 0
        rep.add(env, "lookup", "PASS", f"{lookup} -> {mask(dn, 'dn')} ({groups} group memberships)")
        if INVENTORY:
            inventory(env, rep, conn, server, base_dn, dn)
    else:
        root = ((getattr(server.info, "other", {}) or {}).get("defaultNamingContext") or [""])[0]
        rep.add(env, "lookup", "WARN",
                f"{lookup} not found under {base_dn} ({conn.result.get('description')}); "
                "the account may live in another OU, or the app searches a different attribute; "
                f"set LDAP_LOOKUP_{env} to a real end-user account to test the app's path. "
                f"This server answers for {root or dc_domain(base_dn)}: a person in another domain "
                "(another DC=... root) is invisible here, so point LDAP_SERVER at that domain's "
                "directory, or at a global catalog of the forest (port 3269)")
    conn.unbind()


def main(argv: list[str] | None = None) -> int:
    global _TIMEOUT
    ap = argparse.ArgumentParser(
        description="LDAP preflight for the Synapse sign-in (see the module docstring).")
    ap.add_argument("--env-file", help="KEY=VALUE file to load (exported vars win)")
    ap.add_argument("--envs", help="comma list, default $AUTHCHECK_ENVS or E1,E2,E3")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--timeout", type=int, default=_TIMEOUT, help="seconds per operation")
    ap.add_argument("--inventory", action="store_true",
                    help="list everything the directory holds on the looked-up person: attributes, "
                         "flags, direct and nested groups, the manager, a band candidate")
    ap.add_argument("--redact", action="store_true",
                    help="mask personal values in the inventory (e-mails keep their domain)")
    ap.add_argument("--out", metavar="FILE", help="write the JSON report here as well")
    args = ap.parse_args(argv)
    _TIMEOUT = args.timeout
    global REDACT, INVENTORY
    REDACT, INVENTORY = args.redact, args.inventory
    if ldap3 is None:
        print("the ldap3 package is required:  pip install ldap3", file=sys.stderr)
        return 2
    if args.env_file:
        if not os.path.exists(args.env_file):
            print(f"env file not found: {args.env_file}", file=sys.stderr)
            return 2
        load_env_file(args.env_file)
    envs = split_list(args.envs or cfg("AUTHCHECK_ENVS", default=DEFAULT_ENVS) or DEFAULT_ENVS)

    rep = Report()
    for env in envs:
        try:
            run_env(env, rep)
        except LDAPException as e:  # anything ldap3 raises that we did not expect
            rep.add(env, "ldap", "FAIL", f"{type(e).__name__}: {e}")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(rep.dump(), fh, indent=2, default=str)
    if args.json:
        print(json.dumps(rep.dump(), indent=2, default=str))
    else:
        print_report(rep, "LDAP sign-in preflight")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
