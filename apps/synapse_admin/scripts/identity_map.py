#!/usr/bin/env python3
"""Join the three inventories run for the SAME person and say what links them.

  python okta_check.py        --env-file .env.authcheck.local --inventory E1 --out okta.json
  python google_auth_check.py --env-file .env.authcheck.local --inventory E1 --out google.json
  python ldap_check.py        --env-file .env.authcheck.local --envs E1 --inventory --out ldap.json
  python identity_map.py --okta okta.json --google google.json --ldap ldap.json [--env E1] [--json]

The three providers describe one person three ways. This script puts the
identifiers side by side and answers the questions the sign-in design
turns on: which claim carries the e-mail and does it match the directory's
mail or its userPrincipalName; are Okta's groups the directory's groups;
is the Google account the corporate one and is it the same e-mail; which
identifier is stable enough to be the key; and what the directory adds
that Okta does not (the manager, an employee id, a band). It ends with
recommendations in the app's own setting names.

Any of the three files may be missing; the rest are joined. Values masked
by --redact are compared by shape only and the row says so.
Exit code: 0 (a design aid, not a gate); 2 when no file could be read.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass


@dataclass
class Row:
    topic: str
    status: str  # PASS WARN FAIL INFO SKIP
    detail: str


def load(path: str | None) -> dict | None:
    """The report's inventory section ({env: person}), or None."""
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            report = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"cannot read {path}: {exc}", file=sys.stderr)
        return None
    inv = (report.get("facts") or {}).get("inventory") if isinstance(report, dict) else None
    return inv if isinstance(inv, dict) and inv else None


def pick(inventory: dict | None, env: str | None) -> dict | None:
    if not inventory:
        return None
    if env:
        return inventory.get(env)
    return next(iter(inventory.values()))


def redacted(value) -> bool:
    text = str(value or "")
    return "***" in text or "..." in text


def norm(value) -> str:
    return str(value or "").strip().lower()


def same(a, b) -> bool | None:
    """True/False, or None when a masked value makes the comparison meaningless."""
    if not a or not b:
        return None
    if redacted(a) or redacted(b):
        return None
    return norm(a) == norm(b)


def compare(rows: list[Row], topic: str, a, b, left: str, right: str, *, warn_only: bool = False,
            when_true: str = "", when_false: str = "") -> None:
    verdict = same(a, b)
    if a is None or b is None or a == "" or b == "":
        rows.append(Row(topic, "SKIP", f"{left} or {right} not available"))
    elif verdict is None:
        rows.append(Row(topic, "INFO", f"{left} {a} | {right} {b}: masked values, compare by eye "
                                       "(run the inventories without --redact for the check)"))
    elif verdict:
        rows.append(Row(topic, "PASS", f"{left} = {right} ({a})" + (f"; {when_true}" if when_true else "")))
    else:
        rows.append(Row(topic, "WARN" if warn_only else "FAIL",
                        f"{left} {a} != {right} {b}" + (f"; {when_false}" if when_false else "")))


def join(okta: dict | None, google: dict | None, ldap: dict | None) -> list[Row]:
    rows: list[Row] = []
    # who each provider says this is
    if okta:
        rows.append(Row("okta", "INFO",
                        f"sub {okta.get('sub')}; {okta.get('email_claim') or 'no e-mail claim'} "
                        f"{okta.get('email')}; name {okta.get('name')}; groups "
                        f"{len(okta.get('groups') or [])} via {okta.get('groups_claim')}; "
                        f"id_token claims: {', '.join(okta.get('id_token_claims') or [])}"))
    else:
        rows.append(Row("okta", "SKIP", "no Okta inventory"))
    if google:
        rows.append(Row("google", "INFO",
                        f"sub {google.get('sub')}; email {google.get('email')} "
                        f"(verified={google.get('email_verified')}); hd {google.get('hd')}; "
                        f"name {google.get('name')}; refresh_token {google.get('refresh_token')}; "
                        f"bigquery {json.dumps(google.get('bigquery') or {})}"))
    else:
        rows.append(Row("google", "SKIP", "no Google inventory"))
    if ldap:
        rows.append(Row("ldap", "INFO",
                        f"sAMAccountName {ldap.get('sAMAccountName')}; UPN {ldap.get('userPrincipalName')}; "
                        f"mail {ldap.get('mail')}; displayName {ldap.get('displayName')}; employeeID "
                        f"{ldap.get('employeeID')}; objectGUID {ldap.get('objectGUID')}; groups "
                        f"{len(ldap.get('groups') or [])} direct, {len(ldap.get('groups_nested') or [])} nested; "
                        f"manager {'yes' if ldap.get('manager_entry') else 'no'}"))
    else:
        rows.append(Row("ldap", "SKIP", "no LDAP inventory"))

    o = okta or {}
    g = google or {}
    d = ldap or {}
    # the e-mail across the three
    compare(rows, "email: okta = google", o.get("email"), g.get("email"), "Okta", "Google",
            when_true="the Google connect step can be tied to the signed-in person by e-mail equality",
            when_false="the connect callback's e-mail equality check will refuse this pairing")
    compare(rows, "email: okta = ldap mail", o.get("email"), d.get("mail"), "Okta", "LDAP mail",
            warn_only=True)
    compare(rows, "email: okta = ldap upn", o.get("email"), d.get("userPrincipalName"), "Okta",
            "LDAP userPrincipalName", warn_only=True)
    if o.get("email") and d and not redacted(o.get("email")):
        hits = [k for k in ("mail", "userPrincipalName") if same(o.get("email"), d.get(k))]
        rows.append(Row("ldap lookup key", "PASS" if hits else "FAIL",
                        f"look the person up by {' or '.join(hits)} = the Okta e-mail" if hits else
                        "the Okta e-mail matches neither mail nor userPrincipalName: look up by "
                        "sAMAccountName from preferred_username instead, or by employeeID if Okta carries it"))
    compare(rows, "username: okta = ldap", o.get("preferred_username"),
            d.get("userPrincipalName") or d.get("sAMAccountName"), "Okta preferred_username",
            "LDAP UPN/sAMAccountName", warn_only=True)
    # the Google account's domain
    if g:
        hd = norm(g.get("hd"))
        domain = norm(g.get("email")).rpartition("@")[2]
        if hd:
            rows.append(Row("google: workspace", "PASS", f"hd={hd}: a managed corporate account"
                            + ("" if domain == hd or redacted(g.get("email")) else f" (e-mail domain {domain} differs)")))
        else:
            rows.append(Row("google: workspace", "WARN",
                            "no hd claim: a personal Google account; queries would run as a consumer identity"))
    # names
    compare(rows, "name: okta = ldap", o.get("name"), d.get("displayName"), "Okta name",
            "LDAP displayName", warn_only=True)
    compare(rows, "name: okta = google", o.get("name"), g.get("name"), "Okta name", "Google name",
            warn_only=True)
    # groups
    og = [str(x) for x in (o.get("groups") or [])]
    dg = {norm(x) for x in (d.get("groups_nested") or d.get("groups") or [])}
    if og and dg:
        shared = [x for x in og if norm(x) in dg]
        rows.append(Row("groups: okta in ldap", "PASS" if len(shared) == len(og) else "WARN",
                        f"{len(shared)}/{len(og)} Okta groups are directory groups"
                        + ("" if len(shared) == len(og) else
                           f"; Okta-only: {', '.join(x for x in og if norm(x) not in dg)[:200]}")))
    elif og:
        rows.append(Row("groups: okta in ldap", "INFO", f"Okta carries {len(og)} groups; no directory groups to compare"))
    elif dg:
        rows.append(Row("groups: okta in ldap", "WARN",
                        f"Okta carries no groups; the directory has {len(dg)} (roles would need the LDAP lookup)"))
    # stable keys
    keys = []
    if o.get("sub"):
        keys.append("Okta sub (per issuer; the store's ExternalIdentities key)")
    if d.get("objectGUID"):
        keys.append("LDAP objectGUID (never changes)")
    if d.get("employeeID") or d.get("employeeNumber"):
        keys.append("employeeID/employeeNumber (HR's key)")
    if g.get("sub"):
        keys.append("Google sub (per Google account)")
    rows.append(Row("stable keys", "PASS" if keys else "WARN",
                    "; ".join(keys) if keys else "no stable identifier found in any inventory"))
    # what the directory adds
    if d:
        adds = []
        if d.get("manager_entry"):
            adds.append("manager (the approval flow's approver)")
        if d.get("employeeID") or d.get("employeeNumber"):
            adds.append("employee id")
        if any("band" in str(v).lower() or "level" in str(v).lower()
               for v in (d.get("extension_attributes") or {}).values()) or d.get("title"):
            adds.append("title / band candidate")
        if d.get("department") or d.get("division"):
            adds.append("department / division (a business-unit scope)")
        rows.append(Row("ldap adds", "INFO", ", ".join(adds) if adds else "nothing beyond what Okta carries"))
    return rows


def recommend(okta: dict | None, google: dict | None, ldap: dict | None, rows: list[Row]) -> list[str]:
    out: list[str] = []
    by = {r.topic: r for r in rows}
    o = okta or {}
    g = google or {}
    d = ldap or {}
    if o:
        if o.get("email_claim"):
            out.append(f"AUTH_EMAIL_CLAIMS={o['email_claim']}  (the claim that carried the e-mail)")
        else:
            out.append("Grant the email scope, or add an e-mail claim on the authorization server: "
                       "the ID token carries none, and the app needs one to create the person.")
        if o.get("groups"):
            out.append(f"OKTA_GROUP_CLAIM={o.get('groups_claim')}  and AUTH_GROUP_ROLE_MAP over these "
                       f"names: {', '.join(str(x) for x in o['groups'][:8])}"
                       + (", ..." if len(o["groups"]) > 8 else ""))
        elif d.get("groups") or d.get("groups_nested"):
            out.append("Okta sends no groups: either add a groups claim on the authorization server "
                       "(the cleaner path), or turn on the LDAP lookup after sign-in and map memberOf to roles.")
        else:
            out.append("Neither Okta nor the directory shows groups: roles would be granted by an admin on the People page.")
        if not o.get("name") and not (o.get("given_name") and o.get("family_name")):
            out.append("Request the profile scope (or map name/given_name/family_name): the account row would otherwise show the e-mail.")
    if d:
        if o and by.get("ldap lookup key") and by["ldap lookup key"].status == "PASS":
            out.append("LDAP is optional for sign-in: Okta already identifies the person. Use it after sign-in for "
                       + (by.get("ldap adds").detail if by.get("ldap adds") else "the extra attributes") + ".")
        elif o and by.get("ldap lookup key") and by["ldap lookup key"].status == "FAIL":
            out.append("The LDAP lookup cannot key on the Okta e-mail; key on sAMAccountName (from preferred_username) or employeeID.")
        if d.get("manager_entry"):
            out.append("The approver for the approval flow can come from LDAP's manager attribute instead of SYNAPSE_USER_MANAGER.")
    if g:
        if by.get("email: okta = google") and by["email: okta = google"].status == "FAIL":
            out.append("Okta and Google e-mails differ: the Google callback refuses a connection whose e-mail is not the "
                       "signed-in person's. Decide the rule: same hd domain, or a mapping table, before rollout.")
        if not g.get("refresh_token"):
            out.append("No refresh token from Google: the stored connection would expire within the hour. Check the client type "
                       "(Web application) and that access_type=offline reaches Google.")
        bq = g.get("bigquery") or {}
        if bq.get("dry_run") == "denied":
            out.append(f"The account has the BigQuery scope but no access on {bq.get('project')}: IAM on the project, not the sign-in.")
        if g.get("hd") and o.get("email") and not redacted(o.get("email")) \
                and norm(o.get("email")).rpartition("@")[2] != norm(g.get("hd")):
            out.append("The Okta e-mail domain and the Google hd differ: the pairing rule must allow it explicitly.")
    if o and g and by.get("email: okta = google") and by["email: okta = google"].status == "PASS":
        out.append("With one e-mail across Okta and Google, the connect step can stay as their consent flow; Workforce "
                   "Identity Federation would remove the second consent later without changing the data model.")
    return out


def print_rows(rows: list[Row], recs: list[str]) -> None:
    title = "Identity map: one person, three providers"
    print(f"\n{title}\n{'=' * len(title)}")
    w = max(len(r.topic) for r in rows) if rows else 10
    for r in rows:
        print(f"{r.topic:<{w}}  {r.status:<5}  {r.detail}")
    print("\nRecommendations")
    print("---------------")
    for i, rec in enumerate(recs, 1):
        print(f"{i}. {rec}")
    if not recs:
        print("nothing to change")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Join the Okta, Google and LDAP inventories (see the docstring).")
    ap.add_argument("--okta", help="okta_check.py --inventory ... --out FILE")
    ap.add_argument("--google", help="google_auth_check.py --inventory ... --out FILE")
    ap.add_argument("--ldap", help="ldap_check.py --inventory ... --out FILE")
    ap.add_argument("--env", help="the environment to read from each file (default: the first)")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args(argv)
    okta = pick(load(args.okta), args.env)
    google = pick(load(args.google), args.env)
    ldap = pick(load(args.ldap), args.env)
    if not any((okta, google, ldap)):
        print("no inventory to read: pass --okta/--google/--ldap files written with --inventory --out",
              file=sys.stderr)
        return 2
    rows = join(okta, google, ldap)
    recs = recommend(okta, google, ldap, rows)
    if args.json:
        print(json.dumps({"rows": [asdict(r) for r in rows], "recommendations": recs}, indent=2))
    else:
        print_rows(rows, recs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
