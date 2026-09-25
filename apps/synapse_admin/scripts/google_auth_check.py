#!/usr/bin/env python3
"""Google sign-in preflight for Synapse: can we reach Google's OpenID
endpoints, does our OAuth client exist, and does it accept our callback URL?

Nothing here signs anyone in. The script does:

  1. discovery   GET accounts.google.com/.well-known/openid-configuration.
  2. jwks        Fetch the signing keys the app will verify ID tokens with.
  and then, per environment:
  3. callback    GET /o/oauth2/v2/auth with prompt=none. Google validates
                 client_id and redirect_uri BEFORE anything else:
                   302 to accounts.google.com/signin/oauth/error?authError=...
                        the blob decodes to the reason:
                        redirect_uri_mismatch -> callback NOT registered on this client
                        invalid_client        -> "The OAuth client was not found"
                   302 back to our callback with error=login_required or
                   302 to a Google sign-in page
                        -> client and callback are registered
  4. secret      Only when GOOGLE_CLIENT_SECRET(_<ENV>) is set: POST /token
                 with a dummy code. invalid_grant proves the id+secret pair
                 (Google authenticated the client, then rejected the code);
                 invalid_client means the secret does not match.

Then, unless --no-matrix, every distinct client is probed against the OTHER
environments' callbacks (only meaningful when environments use different
clients; one shared client is already probed against every callback).

Configuration comes from environment variables; NAME_<ENV> wins over NAME:

  AUTHCHECK_ENVS              comma list of environments, default E1,E2,E3
  GOOGLE_CLIENT_ID            shared client, or GOOGLE_CLIENT_ID_<ENV> per environment
  GOOGLE_CLIENT_SECRET        optional; enables check 4 (per-env variant too)
  GOOGLE_REDIRECT_URI_<ENV>   https:// is assumed when the scheme is missing
                              (Google refuses plain http except on localhost)
  GOOGLE_SCOPES               default "openid email profile"
  OKTA_REDIRECT_URI_<ENV>     optional; reported as parity with the Google callback

TLS: `truststore` (the OS keychain, where corporate roots live) when it is
installed, else AUTHCHECK_CA_BUNDLE / REQUESTS_CA_BUNDLE / SSL_CERT_FILE, else
the system roots. AUTHCHECK_TLS_INSECURE=1 turns verification off, loudly.
HTTPS_PROXY is honoured.

Usage:
  python google_auth_check.py --env-file .env.authcheck.local
  python google_auth_check.py --envs E1 --json

Exit code: 0 everything passed, 1 something FAILED, 2 configuration problem.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import json
import os
import re
import secrets
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass

UA = "synapse-authcheck/1.0"
DEFAULT_ENVS = "E1,E2,E3"
DISCOVERY = "https://accounts.google.com/.well-known/openid-configuration"
_TIMEOUT = 20


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


def norm_redirect(uri: str) -> str:
    uri = uri.strip()
    return uri if "://" in uri else "https://" + uri


# ------------------------------------------------------------------ http --
_INSECURE_WARNED = False


def tls_context() -> ssl.SSLContext:
    global _INSECURE_WARNED
    if os.environ.get("AUTHCHECK_TLS_INSECURE") == "1":
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        if not _INSECURE_WARNED:
            print("!! AUTHCHECK_TLS_INSECURE=1: certificate verification is OFF", file=sys.stderr)
            _INSECURE_WARNED = True
        return ctx
    try:
        import truststore  # type: ignore[import-not-found]

        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except ImportError:
        pass
    for var in ("AUTHCHECK_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        path = os.environ.get(var)
        if path and os.path.exists(path):
            return ssl.create_default_context(cafile=path)
    return ssl.create_default_context()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow a redirect: the Location header IS the answer we want."""

    def redirect_request(self, *args, **kwargs):
        return None


@dataclass
class Resp:
    status: int  # 0 means the request never got an HTTP answer; see .error
    headers: dict
    body: str
    error: str = ""

    @property
    def location(self) -> str:
        for key, val in self.headers.items():
            if key.lower() == "location":
                return val
        return ""

    def json(self):
        try:
            return json.loads(self.body)
        except ValueError:
            return None


def http(url: str, method: str = "GET", data: bytes | None = None,
         headers: dict | None = None) -> Resp:
    opener = urllib.request.build_opener(
        _NoRedirect(), urllib.request.HTTPSHandler(context=tls_context()))
    req = urllib.request.Request(
        url, data=data, method=method, headers={"User-Agent": UA, **(headers or {})})
    try:
        with opener.open(req, timeout=_TIMEOUT) as r:
            return Resp(r.status, dict(r.headers.items()), r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:  # includes the 3xx we refuse to follow
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            body = ""
        return Resp(e.code, dict(e.headers.items()) if e.headers else {}, body)
    except Exception as e:  # noqa: BLE001 - DNS, TLS, timeout, proxy: report, never crash
        why = f"{type(e).__name__}: {e}"
        if "Tunnel connection failed" in why or "403" in why:
            why += " (the HTTPS proxy refused this host; allow it or run from the corporate network)"
        return Resp(0, {}, "", error=why)


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

    def dump(self) -> dict:
        return {"ok": not self.failed, "checks": [asdict(c) for c in self.checks],
                "facts": self.facts}


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


# ---------------------------------------------------------------- google --
def same_endpoint(a: str, b: str) -> bool:
    pa, pb = urllib.parse.urlsplit(a), urllib.parse.urlsplit(b)
    return (pa.scheme.lower(), pa.netloc.lower(), pa.path.rstrip("/")) == (
        pb.scheme.lower(), pb.netloc.lower(), pb.path.rstrip("/"))


def pkce() -> dict:
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode()).digest()
    return {"code_challenge": base64.urlsafe_b64encode(digest).rstrip(b"=").decode(),
            "code_challenge_method": "S256"}


def _pb_strings(raw: bytes) -> list[str]:
    """Top-level length-delimited fields of a protobuf blob, as text; nested
    messages are flattened. Google's authError parameter is one of these:
    field 1 is the error code, field 2 the prose, later fields the offending
    parameter and its value."""
    out: list[str] = []
    i = 0
    while i < len(raw):
        tag = raw[i]
        i += 1
        wire = tag & 7
        if wire == 0:  # varint
            while i < len(raw) and raw[i] >= 0x80:
                i += 1
            i += 1
        elif wire == 2:  # length-delimited
            length, shift = 0, 0
            while i < len(raw):
                b = raw[i]
                i += 1
                length |= (b & 0x7F) << shift
                shift += 7
                if b < 0x80:
                    break
            chunk = raw[i:i + length]
            i += length
            try:
                text = chunk.decode("utf-8")
            except UnicodeDecodeError:
                text = ""
            if text and all(ch.isprintable() or ch in "\n\t" for ch in text):
                out.append(" ".join(text.split()))
            elif chunk:
                out.extend(_pb_strings(chunk))
        else:  # fixed64 / fixed32 / groups: not expected here, stop guessing
            break
    return out


def decode_auth_error(loc: str) -> tuple[str, str]:
    """(error_code, message) from Google's .../signin/oauth/error?authError=... URL."""
    q = urllib.parse.parse_qs(urllib.parse.urlsplit(loc).query)
    blob = q.get("authError", [""])[0]
    if not blob:
        return "", ""
    try:
        raw = base64.urlsafe_b64decode(blob + "=" * (-len(blob) % 4))
    except (ValueError, TypeError):
        return "", ""
    try:
        parts = _pb_strings(raw)
    except Exception:  # noqa: BLE001 - fall back to printable runs
        parts = [r.decode() for r in re.findall(rb"[\x20-\x7e]{4,}", raw)]
    if not parts:
        return "", ""
    return parts[0].strip(), " | ".join(p.strip() for p in parts[1:] if p.strip())[:220]


def probe_authorize(authz: str, client_id: str, redirect_uri: str,
                    scopes: list[str]) -> tuple[str, str]:
    """Ask Google's authorize endpoint with prompt=none and read its verdict on
    the client_id + redirect_uri pair. Returns (verdict, detail) with verdict one
    of REGISTERED, REGISTERED_OTHER, NOT_REGISTERED, BAD_CLIENT, REJECTED,
    UNREACHABLE, UNCLEAR."""
    params = {"client_id": client_id, "redirect_uri": redirect_uri, "response_type": "code",
              "scope": " ".join(scopes), "state": secrets.token_urlsafe(12),
              "nonce": secrets.token_urlsafe(12), "prompt": "none", **pkce()}
    resp = http(authz + "?" + urllib.parse.urlencode(params),
                headers={"Accept": "text/html"})
    if resp.status == 0:
        return "UNREACHABLE", resp.error
    loc = resp.location
    if resp.status in (301, 302, 303, 307, 308) and loc:
        parts = urllib.parse.urlsplit(loc)
        if same_endpoint(loc, redirect_uri):
            q = urllib.parse.parse_qs(parts.query)
            q.update(urllib.parse.parse_qs(parts.fragment))
            err = q.get("error", [""])[0]
            if err in ("login_required", "interaction_required", "consent_required"):
                return "REGISTERED", f"Google bounced to the callback with error={err} (no session, as expected)"
            if err:
                return "REGISTERED_OTHER", err
            extra = " with a code (a Google session exists here)" if "code" in q else ""
            return "REGISTERED", "redirected to the callback" + extra
        if "/signin/oauth/error" in parts.path or "authError=" in parts.query:
            code, msg = decode_auth_error(loc)
            if code == "redirect_uri_mismatch":
                return "NOT_REGISTERED", f"redirect_uri_mismatch: {msg}"
            if code in ("invalid_client", "unauthorized_client"):
                return "BAD_CLIENT", f"{code}: {msg}"
            if code in ("invalid_request", "invalid_scope", "unsupported_response_type"):
                return "REJECTED", f"{code}: {msg}"
            return "UNCLEAR", f"{code or 'error'}: {msg}"
        if parts.netloc.lower().endswith("google.com"):
            return "REGISTERED", f"sent to Google sign-in ({parts.path}); client and callback accepted"
        return "UNCLEAR", f"redirected to {loc[:140]}"
    text = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", resp.body))).strip()
    if "redirect_uri_mismatch" in text:
        return "NOT_REGISTERED", text[:180]
    if "invalid_client" in text:
        return "BAD_CLIENT", text[:180]
    if resp.status == 200:
        return "REGISTERED", "Google rendered its sign-in page; client and callback accepted"
    return "UNCLEAR", f"HTTP {resp.status}: {text[:180]}"


VERDICT_STATUS = {"REGISTERED": "PASS", "REGISTERED_OTHER": "WARN", "NOT_REGISTERED": "FAIL",
                  "BAD_CLIENT": "FAIL", "REJECTED": "FAIL", "UNREACHABLE": "FAIL",
                  "UNCLEAR": "WARN"}


def check_secret(env: str, rep: Report, token_ep: str, client_id: str, secret: str,
                 redirect: str) -> None:
    form = {"code": "not-a-real-code", "client_id": client_id, "client_secret": secret,
            "redirect_uri": redirect, "grant_type": "authorization_code"}
    r = http(token_ep, "POST", urllib.parse.urlencode(form).encode(),
             {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"})
    doc = r.json() or {}
    err, desc = doc.get("error", ""), doc.get("error_description", "")
    if err == "invalid_grant":
        rep.add(env, "client secret", "PASS",
                "the token endpoint authenticated the client and rejected the dummy code "
                f"(invalid_grant: {desc}); the id+secret pair is valid")
    elif err == "invalid_client":
        rep.add(env, "client secret", "FAIL",
                f"invalid_client: {desc}; the secret does not match this client id")
    elif err == "redirect_uri_mismatch":
        rep.add(env, "client secret", "FAIL", f"redirect_uri_mismatch: {desc}")
    elif r.status == 0:
        rep.add(env, "client secret", "FAIL", r.error)
    else:
        rep.add(env, "client secret", "WARN", f"HTTP {r.status} {err}: {desc}"[:200])


def run_env(env: str, rep: Report, disc: dict, clients: dict, redirects: dict) -> None:
    client_id = cfg("GOOGLE_CLIENT_ID", env)
    redirect = cfg("GOOGLE_REDIRECT_URI", env)
    missing = [n for n, v in (("GOOGLE_CLIENT_ID", client_id),
                              ("GOOGLE_REDIRECT_URI", redirect)) if not v]
    if missing:
        rep.add(env, "config", "SKIP",
                "not set: " + ", ".join(f"{m}_{env}" for m in missing)
                + (" (GOOGLE_CLIENT_ID may be shared)" if "GOOGLE_CLIENT_ID" in missing else ""))
        return
    redirect = norm_redirect(redirect)
    scopes = split_list(cfg("GOOGLE_SCOPES", env, "openid email profile"))
    secret = cfg("GOOGLE_CLIENT_SECRET", env)
    if not redirect.startswith("https://") and urllib.parse.urlsplit(redirect).hostname not in (
            "localhost", "127.0.0.1"):
        rep.add(env, "callback scheme", "FAIL",
                f"{redirect}: Google only accepts https callbacks (http is allowed on localhost only)")

    verdict, detail = probe_authorize(disc["authorization_endpoint"], client_id, redirect, scopes)
    rep.add(env, "callback", VERDICT_STATUS[verdict], f"{redirect}: {detail}")

    if secret:
        check_secret(env, rep, disc["token_endpoint"], client_id, secret, redirect)
    else:
        rep.add(env, "client secret", "SKIP",
                f"GOOGLE_CLIENT_SECRET(_{env}) not set (set it to prove the id+secret pair)")

    okta = cfg("OKTA_REDIRECT_URI", env)
    if okta:
        okta = norm_redirect(okta)
        same = same_endpoint(okta, redirect)
        rep.add(env, "same callback as Okta", "PASS" if same else "WARN",
                f"Google {redirect} | Okta {okta}" + ("" if same else " (they differ)"))

    redirects[env] = redirect
    if verdict not in ("BAD_CLIENT", "UNREACHABLE"):
        clients.setdefault(client_id, {"envs": [], "scopes": scopes})["envs"].append(env)


def run_matrix(rep: Report, disc: dict, clients: dict, redirects: dict) -> None:
    for client_id, info in clients.items():
        who = "/".join(info["envs"]) + " client"
        own = {redirects[e] for e in info["envs"] if e in redirects}
        for env, uri in redirects.items():
            if env in info["envs"] or uri in own:
                continue  # its own callbacks were probed above
            verdict, detail = probe_authorize(disc["authorization_endpoint"], client_id, uri,
                                              info["scopes"])
            if verdict.startswith("REGISTERED"):
                rep.add(env, "cross-env", "WARN",
                        f"{who} ({client_id}) ACCEPTS {uri}; one client serving several "
                        "environments is fine, a prod client accepting non-prod callbacks is not")
            elif verdict in ("NOT_REGISTERED", "BAD_CLIENT", "REJECTED"):
                rep.add(env, "cross-env", "INFO", f"{who} rejects {uri} (expected)")
            else:
                rep.add(env, "cross-env", "INFO", f"{who} x {uri}: {detail}")


def main(argv: list[str] | None = None) -> int:
    global _TIMEOUT
    ap = argparse.ArgumentParser(
        description="Google sign-in preflight for Synapse (see the module docstring).")
    ap.add_argument("--env-file", help="KEY=VALUE file to load (exported vars win)")
    ap.add_argument("--envs", help="comma list, default $AUTHCHECK_ENVS or E1,E2,E3")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--no-matrix", action="store_true",
                    help="skip probing each client against the other environments' callbacks")
    ap.add_argument("--timeout", type=int, default=_TIMEOUT, help="seconds per request")
    args = ap.parse_args(argv)
    _TIMEOUT = args.timeout
    if args.env_file:
        if not os.path.exists(args.env_file):
            print(f"env file not found: {args.env_file}", file=sys.stderr)
            return 2
        load_env_file(args.env_file)
    envs = split_list(args.envs or cfg("AUTHCHECK_ENVS", default=DEFAULT_ENVS) or DEFAULT_ENVS)

    rep = Report()
    discovery = cfg("GOOGLE_DISCOVERY_URL", default=DISCOVERY) or DISCOVERY
    t0 = time.time()
    resp = http(discovery)
    ms = int((time.time() - t0) * 1000)
    disc = resp.json() if resp.status == 200 else None
    if not isinstance(disc, dict) or "authorization_endpoint" not in disc:
        rep.add("all", "discovery", "FAIL",
                f"{discovery} -> {resp.error or f'HTTP {resp.status}'}")
    else:
        rep.facts["discovery"] = {k: disc.get(k) for k in (
            "issuer", "authorization_endpoint", "token_endpoint", "userinfo_endpoint",
            "jwks_uri", "revocation_endpoint", "scopes_supported",
            "code_challenge_methods_supported")}
        rep.add("all", "discovery", "PASS",
                f"issuer {disc.get('issuer')}; authorize {disc['authorization_endpoint']}; "
                f"token {disc.get('token_endpoint')} ({ms} ms)")
        jr = http(disc.get("jwks_uri", ""))
        keys = (jr.json() or {}).get("keys") if jr.status == 200 else None
        if keys:
            rep.add("all", "jwks", "PASS", f"{len(keys)} signing key(s) at {disc.get('jwks_uri')}")
        else:
            rep.add("all", "jwks", "FAIL", f"{disc.get('jwks_uri')} -> {jr.error or f'HTTP {jr.status}'}")

        clients: dict = {}
        redirects: dict = {}
        for env in envs:
            run_env(env, rep, disc, clients, redirects)
        if not args.no_matrix and clients and len(redirects) > 1:
            run_matrix(rep, disc, clients, redirects)

    if args.json:
        print(json.dumps(rep.dump(), indent=2))
    else:
        print_report(rep, "Google sign-in preflight")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
