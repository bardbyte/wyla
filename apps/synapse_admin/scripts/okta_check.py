#!/usr/bin/env python3
"""Okta preflight for the Synapse sign-in: can we reach the authorization
server, is our client registered on it, and does it accept our callback URL?

Nothing here signs anyone in. Per environment the script does five things:

  1. discovery   GET the .well-known document; read the issuer and endpoints.
  2. metadata    Are the scopes we ask for and the group claim published?
  3. jwks        Fetch the signing keys the app will verify ID tokens with.
  4. callback    GET /authorize with prompt=none. Okta validates client_id and
                 redirect_uri BEFORE anything else, so the answer is decisive:
                   302 back to our callback with error=login_required
                        -> client and callback are registered (no session, fine)
                   400 "The 'redirect_uri' parameter must be a Login redirect URI"
                        -> this callback is NOT registered on this client
                   400 "Invalid value for 'client_id'"
                        -> wrong client id for this authorization server
                   302 back to our callback with error=invalid_scope
                        -> callback fine, but a requested scope is not defined
  5. secret      Only when OKTA_CLIENT_SECRET_<ENV> is set: POST /introspect
                 with a dummy token. 200 {"active": false} proves the id+secret
                 pair; 401 invalid_client means the secret does not match.

Then, unless --no-matrix, every distinct client is probed against the OTHER
environments' callbacks, so you can see whether e.g. the prod client also
accepts the dev callback (it should not).

--inventory <ENV> goes one step further and answers "what do we actually
get from Okta": it signs YOU in once, through a browser, on that
environment's client, with a local callback (OKTA_LOCAL_REDIRECT_URI,
default http://localhost:8400/callback, which must be added to the client's
Login redirect URIs; use the non-production client). It then prints every
claim of the ID token, of the access token when it is a JWT, and of the
/userinfo answer, verifies the ID token's signature against the JWKS, and
gives a verdict on each thing the app needs: an email claim, a name, the
group claim, a stable subject, the token lifetimes. --redact masks the
personal values (an email keeps its domain, a name its shape) so the output
can be pasted anywhere; --out writes the JSON for identity_map.py.

Configuration comes from environment variables; NAME_<ENV> wins over NAME:

  AUTHCHECK_ENVS             comma list of environments, default E1,E2,E3
  OKTA_DISCOVERY_URL_<ENV>   the .well-known URL Okta shows, or just the issuer
  OKTA_CLIENT_ID_<ENV>
  OKTA_REDIRECT_URI_<ENV>    https:// is assumed when the scheme is missing
  OKTA_CLIENT_SECRET_<ENV>   optional; enables check 5
  OKTA_SCOPES                space or comma list, default "openid profile"
  OKTA_GROUP_CLAIM           optional, e.g. groups
  OKTA_LOCAL_REDIRECT_URI    --inventory: the local callback, default http://localhost:8400/callback
  OKTA_INVENTORY_SCOPES      --inventory: scopes to request, default OKTA_SCOPES + email groups
  GOOGLE_REDIRECT_URI_<ENV>  optional; reported as parity with the Okta callback

TLS: `truststore` (the OS keychain, where corporate roots live) when it is
installed, else AUTHCHECK_CA_BUNDLE / REQUESTS_CA_BUNDLE / SSL_CERT_FILE, else
the system roots. AUTHCHECK_TLS_INSECURE=1 turns verification off, loudly.
HTTPS_PROXY is honoured.

Usage:
  python okta_check.py --env-file .env.authcheck.local
  python okta_check.py --envs E1,E3 --json
  python okta_check.py --env-file .env.authcheck.local --inventory E1 --redact --out okta.json

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


# ------------------------------------------------------------------ okta --
def discovery_url(given: str) -> str:
    given = given.strip().rstrip("/")
    return given if "/.well-known/" in given else given + "/.well-known/openid-configuration"


def text_of(resp: Resp) -> str:
    """The human-readable message in an Okta error response, JSON or HTML."""
    doc = resp.json()
    if isinstance(doc, dict):
        parts = [str(doc.get(k)) for k in ("errorCode", "errorSummary", "error",
                                            "error_description") if doc.get(k)]
        if parts:
            return " ".join(parts)
    text = html.unescape(re.sub(r"<[^>]+>", " ", resp.body))
    return re.sub(r"\s+", " ", text).strip()


def excerpt(text: str, needle: str, width: int = 180) -> str:
    start = text.find("Description:")
    if start < 0:
        start = max(0, text.find(needle) - 40)
    return text[start:start + width].strip()


def same_endpoint(a: str, b: str) -> bool:
    pa, pb = urllib.parse.urlsplit(a), urllib.parse.urlsplit(b)
    return (pa.scheme.lower(), pa.netloc.lower(), pa.path.rstrip("/")) == (
        pb.scheme.lower(), pb.netloc.lower(), pb.path.rstrip("/"))


def pkce() -> dict:
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode()).digest()
    return {"code_challenge": base64.urlsafe_b64encode(digest).rstrip(b"=").decode(),
            "code_challenge_method": "S256"}


# ------------------------------------------------------------ inventory --
REDACT = False
# under --redact these stay readable: they name things, not people
KEEP_UNDER_REDACT = {"iss", "aud", "azp", "alg", "kid", "typ", "scp", "scope", "amr", "idp",
                     "cid", "ver", "groups", "hd", "token_type", "email_verified", "acr"}


def b64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def decode_jwt(token: str) -> tuple[dict, dict] | None:
    """(header, claims) of a JWT, unverified; None when the string is not one."""
    parts = (token or "").split(".")
    if len(parts) != 3:
        return None
    try:
        header = json.loads(b64url_decode(parts[0]))
        claims = json.loads(b64url_decode(parts[1]))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(header, dict) or not isinstance(claims, dict):
        return None
    return header, claims


def verify_rs256(token: str, jwks: dict) -> str:
    """'verified', 'failed: ...' or 'unverified: ...' (no cryptography package,
    or an algorithm this check does not cover)."""
    decoded = decode_jwt(token)
    if not decoded:
        return "unverified: not a JWT"
    header, _ = decoded
    if header.get("alg") != "RS256":
        return f"unverified: alg {header.get('alg')} is not RS256"
    key = next((k for k in (jwks or {}).get("keys", []) if k.get("kid") == header.get("kid")), None)
    if key is None:
        return f"failed: no key {header.get('kid')!r} in the JWKS"
    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding, rsa
    except ImportError:
        return "unverified: pip install cryptography to verify the signature here"
    head, body, sig = token.split(".")
    try:
        public = rsa.RSAPublicNumbers(int.from_bytes(b64url_decode(key["e"]), "big"),
                                      int.from_bytes(b64url_decode(key["n"]), "big")).public_key()
        public.verify(b64url_decode(sig), f"{head}.{body}".encode(), padding.PKCS1v15(),
                      hashes.SHA256())
    except Exception as exc:  # noqa: BLE001 - any failure is the same answer
        return f"failed: {type(exc).__name__}"
    return "verified"


def mask(value, key: str = ""):
    """Personal values under --redact: an e-mail keeps its domain, a string its
    shape and length, a list its length; names of things stay (KEEP_UNDER_REDACT)."""
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
    if len(text) <= 3:
        return "***"
    return f"{text[:2]}...{text[-1]}({len(text)})"


def show(value) -> str:
    """One line for a claim value in the table."""
    if isinstance(value, list):
        inner = ", ".join(str(v) for v in value[:12]) + (", ..." if len(value) > 12 else "")
        return f"list[{len(value)}]: {inner}"
    if isinstance(value, dict):
        return "object: " + ", ".join(sorted(value)[:12])
    return f"{type(value).__name__}: {value}"


def receive_code(redirect_uri: str, state: str, timeout: int, open_browser: bool,
                 url: str) -> dict:
    """Serve the local callback once and return its query as a dict, or
    {"error": ...}. The browser is opened on the authorize URL; the URL is
    printed too, for a machine without one."""
    import http.server

    parts = urllib.parse.urlsplit(redirect_uri)
    host, port, path = parts.hostname or "127.0.0.1", parts.port or 80, parts.path or "/"
    got: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - the stdlib's name
            u = urllib.parse.urlsplit(self.path)
            if u.path.rstrip("/") != path.rstrip("/"):
                self.send_response(404)
                self.end_headers()
                return
            got.update({k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<!doctype html><title>Synapse preflight</title>"
                             b"<p>Received. Close this tab and return to the terminal.</p>")

        def log_message(self, *args):  # quiet
            pass

    try:
        server = http.server.HTTPServer((host, port), Handler)
    except OSError as exc:
        return {"error": f"cannot listen on {host}:{port} for the callback: {exc}"}
    server.timeout = 1
    print(f"\nSign in here (the browser should open by itself):\n  {url}\n", file=sys.stderr)
    if open_browser:
        import webbrowser
        webbrowser.open(url)
    deadline = time.time() + timeout
    try:
        while not got and time.time() < deadline:
            server.handle_request()
    finally:
        server.server_close()
    if not got:
        return {"error": f"no callback arrived within {timeout}s"}
    if got.get("state") != state:
        return {"error": "the callback carried another state; start again"}
    return got


def exchange_code(token_ep: str, client_id: str, secret: str | None, code: str,
                  redirect_uri: str, verifier: str) -> tuple[dict, str]:
    """(token response, how the client authenticated) or ({}, error)."""
    form = {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri,
            "code_verifier": verifier}
    hdrs = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    attempts = []
    if secret:
        basic = base64.b64encode(f"{client_id}:{secret}".encode()).decode()
        attempts.append(("client_secret_basic", {**hdrs, "Authorization": f"Basic {basic}"}, form))
        attempts.append(("client_secret_post", hdrs,
                         {**form, "client_id": client_id, "client_secret": secret}))
    else:
        attempts.append(("none (public client, PKCE only)", hdrs, {**form, "client_id": client_id}))
    last = ""
    for how, headers, body in attempts:
        r = http(token_ep, "POST", urllib.parse.urlencode(body).encode(), headers)
        doc = r.json() or {}
        if r.status == 200 and doc.get("id_token"):
            return doc, how
        last = f"{how}: {doc.get('error', '')} {doc.get('error_description', '')}".strip() \
            if doc else f"{how}: {r.error or f'HTTP {r.status}'}"
        if doc.get("error") != "invalid_client":
            break
    return {}, last


CLAIM_ORDER = ("sub", "email", "email_verified", "preferred_username", "login", "name",
               "given_name", "family_name", "groups", "amr", "idp", "auth_time", "iss", "aud",
               "iat", "exp", "nonce", "at_hash", "jti", "ver")


def ordered(claims: dict) -> list[str]:
    return [c for c in CLAIM_ORDER if c in claims] + sorted(c for c in claims if c not in CLAIM_ORDER)


def inventory(env: str, rep: Report, *, timeout: int, open_browser: bool) -> None:
    """Sign in once on this environment's client and report every claim that
    comes back, then the verdict on each thing the app needs."""
    facts = rep.facts.get(env) or {}
    if not facts.get("authorization_endpoint"):
        rep.add(env, "inventory", "FAIL", "discovery did not answer above; nothing to sign in to")
        return
    client_id = cfg("OKTA_CLIENT_ID", env)
    secret = cfg("OKTA_CLIENT_SECRET", env)
    redirect_local = norm_redirect(cfg("OKTA_LOCAL_REDIRECT_URI", env,
                                       "http://localhost:8400/callback"))
    if redirect_local.startswith("https://localhost"):
        redirect_local = "http://" + redirect_local[len("https://"):]
    base_scopes = split_list(cfg("OKTA_SCOPES", env, "openid profile"))
    scopes = split_list(cfg("OKTA_INVENTORY_SCOPES", env, "")) or sorted(
        set(base_scopes) | {"openid", "email", "groups"}, key=lambda x: (x != "openid", x))
    claim_name = cfg("OKTA_GROUP_CLAIM", env) or "groups"

    state, nonce = secrets.token_urlsafe(16), secrets.token_urlsafe(16)
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    url = facts["authorization_endpoint"] + "?" + urllib.parse.urlencode({
        "client_id": client_id, "redirect_uri": redirect_local, "response_type": "code",
        "scope": " ".join(scopes), "state": state, "nonce": nonce,
        "code_challenge": challenge, "code_challenge_method": "S256"})
    got = receive_code(redirect_local, state, timeout, open_browser, url)
    if got.get("error"):
        why = got["error"]
        if got.get("error_description"):
            why = f"Okta refused: {why}: {got['error_description']}"
        if "redirect_uri" in why or why == "invalid_request":
            why += f" (add {redirect_local} to the client's Login redirect URIs)"
        if "invalid_scope" in why:
            why += " (a requested scope is not on this authorization server; set OKTA_INVENTORY_SCOPES)"
        rep.add(env, "sign-in", "FAIL", why)
        return
    tokens, how = exchange_code(facts["token_endpoint"], client_id, secret, got.get("code", ""),
                                redirect_local, verifier)
    if not tokens:
        rep.add(env, "sign-in", "FAIL", f"the code exchange failed: {how}")
        return
    at_kind = "JWT" if decode_jwt(tokens.get("access_token", "")) else "opaque"
    rep.add(env, "sign-in", "PASS",
            f"signed in with scopes '{' '.join(scopes)}' (client auth: {how}); tokens: id_token, "
            f"access_token ({at_kind}, {tokens.get('expires_in', '?')}s)"
            + (", refresh_token" if tokens.get("refresh_token") else ", no refresh_token")
            + f"; granted scope: {tokens.get('scope', '?')}")

    # the ID token
    decoded = decode_jwt(tokens["id_token"])
    if not decoded:
        rep.add(env, "id_token", "FAIL", "not a JWT")
        return
    header, claims = decoded
    jr = http(facts.get("jwks_uri") or "")
    jwks = jr.json() if jr.status == 200 else {}
    verdict = verify_rs256(tokens["id_token"], jwks or {})
    problems = []
    if claims.get("iss") != facts.get("issuer"):
        problems.append(f"iss {claims.get('iss')} != discovery issuer {facts.get('issuer')}")
    aud = claims.get("aud")
    if client_id not in (aud if isinstance(aud, list) else [aud]):
        problems.append("aud does not name our client")
    if claims.get("nonce") != nonce:
        problems.append("nonce mismatch")
    status = "PASS" if verdict == "verified" and not problems else ("WARN" if verdict.startswith("unverified") and not problems else "FAIL")
    rep.add(env, "id_token.verify", status,
            f"alg {header.get('alg')} kid {header.get('kid')}: signature {verdict}"
            + ("; " + "; ".join(problems) if problems else "; iss, aud and nonce match"))
    life = (claims.get("exp") or 0) - (claims.get("iat") or 0)
    rep.add(env, "id_token.lifetime", "INFO", f"{life}s ({life // 60} min)")
    for name in ordered(claims):
        rep.add(env, f"id_token.{name}", "INFO", show(mask(claims[name], name)))

    # the access token, when it is a JWT (custom authorization servers)
    at = decode_jwt(tokens.get("access_token", ""))
    at_claims = at[1] if at else {}
    if at_claims:
        for name in ordered(at_claims):
            if name in ("iss", "aud", "iat", "exp", "jti", "ver", "auth_time", "nonce"):
                continue
            rep.add(env, f"access_token.{name}", "INFO", show(mask(at_claims[name], name)))

    # userinfo
    ui_claims: dict = {}
    if facts.get("userinfo_endpoint"):
        ur = http(facts["userinfo_endpoint"], headers={
            "Authorization": f"Bearer {tokens.get('access_token', '')}", "Accept": "application/json"})
        ui_claims = ur.json() if ur.status == 200 and isinstance(ur.json(), dict) else {}
        if ui_claims:
            extra = [c for c in ordered(ui_claims) if c not in claims]
            rep.add(env, "userinfo", "PASS",
                    f"{len(ui_claims)} claims; beyond the id_token: {', '.join(extra) or 'none'}")
            for name in extra:
                rep.add(env, f"userinfo.{name}", "INFO", show(mask(ui_claims[name], name)))
        else:
            rep.add(env, "userinfo", "WARN", f"HTTP {ur.status} {ur.error or text_of(ur)[:120]}")

    # the verdicts the app needs
    everything = {**ui_claims, **at_claims, **claims}
    email = next((c for c in ("email", "preferred_username", "login") if everything.get(c)
                  and "@" in str(everything.get(c))), None)
    where = "id_token" if email in claims else ("userinfo" if email in ui_claims else "access_token")
    rep.add(env, "need: email", "PASS" if email else "FAIL",
            f"{email} in the {where} (set AUTH_EMAIL_CLAIMS={email})" if email
            else "no claim carries an e-mail: grant the email scope, or add an email claim on the server")
    name_ok = everything.get("name") or (everything.get("given_name") and everything.get("family_name"))
    rep.add(env, "need: name", "PASS" if name_ok else "WARN",
            "name" + (" and given_name/family_name" if everything.get("given_name") else "") + " present"
            if name_ok else "no name claims (profile scope missing, or not mapped); the app falls back to the e-mail")
    groups = everything.get(claim_name)
    if isinstance(groups, list):
        where = "id_token" if claim_name in claims else ("access_token" if claim_name in at_claims else "userinfo")
        rep.add(env, "need: groups", "PASS",
                f"{claim_name} in the {where}: {len(groups)} group(s); AUTH_GROUP_ROLE_MAP maps these names to roles")
    else:
        rep.add(env, "need: groups", "WARN",
                f"no '{claim_name}' claim anywhere: on the authorization server add a Claims rule "
                f"'{claim_name}' (Groups, filter) for the ID token, or set OKTA_GROUP_CLAIM to the claim it uses")
    rep.add(env, "need: subject", "PASS" if claims.get("sub") else "FAIL",
            "sub present: the stable key the store links the person on" if claims.get("sub") else "no sub")
    amr = everything.get("amr")
    rep.add(env, "need: mfa", "INFO",
            f"amr {amr}: {'a second factor was used' if isinstance(amr, list) and any(a in ('mfa', 'otp', 'sms', 'hwk', 'swk', 'pop') for a in amr) else 'no second factor visible'}"
            if amr else "no amr claim (Okta shows the factors only when the server maps them)")
    rep.add(env, "need: refresh", "INFO",
            "refresh_token present (offline_access granted)" if tokens.get("refresh_token")
            else "no refresh_token: fine for a browser sign-in (the app keeps its own session)")

    rep.facts.setdefault("inventory", {})[env] = mask({
        "provider": "okta", "issuer": facts.get("issuer"), "client_id": client_id,
        "scopes_requested": scopes, "scopes_granted": split_list(str(tokens.get("scope", ""))),
        "client_auth": how, "id_token_lifetime_s": life,
        "access_token_kind": at_kind, "access_token_expires_in": tokens.get("expires_in"),
        "refresh_token": bool(tokens.get("refresh_token")),
        "sub": claims.get("sub"), "email": everything.get(email) if email else None,
        "email_claim": email, "preferred_username": everything.get("preferred_username"),
        "name": everything.get("name"), "given_name": everything.get("given_name"),
        "family_name": everything.get("family_name"),
        "groups": groups if isinstance(groups, list) else None, "groups_claim": claim_name,
        "amr": amr, "idp": everything.get("idp"),
        "id_token_claims": ordered(claims), "access_token_claims": ordered(at_claims),
        "userinfo_claims": ordered(ui_claims), "signature": verdict,
    })


def probe_authorize(authz: str, client_id: str, redirect_uri: str,
                    scopes: list[str]) -> tuple[str, str]:
    """Ask /authorize with prompt=none and read Okta's verdict on the
    client_id + redirect_uri pair. Returns (verdict, detail) with verdict one of
    REGISTERED, REGISTERED_BAD_SCOPE, REGISTERED_OTHER, NOT_REGISTERED,
    BAD_CLIENT, UNREACHABLE, UNCLEAR."""
    params = {"client_id": client_id, "redirect_uri": redirect_uri, "response_type": "code",
              "scope": " ".join(scopes), "state": secrets.token_urlsafe(12),
              "nonce": secrets.token_urlsafe(12), "prompt": "none", **pkce()}
    resp = http(authz + "?" + urllib.parse.urlencode(params),
                headers={"Accept": "text/html,application/json"})
    if resp.status == 0:
        return "UNREACHABLE", resp.error
    loc = resp.location
    if resp.status in (301, 302, 303, 307, 308) and loc:
        if same_endpoint(loc, redirect_uri):
            parts = urllib.parse.urlsplit(loc)
            q = urllib.parse.parse_qs(parts.query)
            q.update(urllib.parse.parse_qs(parts.fragment))
            err = q.get("error", [""])[0]
            desc = q.get("error_description", [""])[0]
            if err in ("login_required", "interaction_required", "consent_required"):
                return "REGISTERED", f"Okta bounced to the callback with error={err} (no session, as expected)"
            if err == "invalid_scope":
                return "REGISTERED_BAD_SCOPE", desc or err
            if err:
                return "REGISTERED_OTHER", f"{err}: {desc}" if desc else err
            extra = " with a code (an Okta session exists here)" if "code" in q else ""
            return "REGISTERED", "redirected to the callback" + extra
        return "REGISTERED", (f"sent to Okta sign-in ({urllib.parse.urlsplit(loc).path}); "
                              "client and callback accepted")
    text = text_of(resp)
    if "redirect_uri" in text:
        return "NOT_REGISTERED", excerpt(text, "redirect_uri")
    if "client_id" in text or ("client" in text.lower() and "invalid" in text.lower()):
        return "BAD_CLIENT", excerpt(text, "client")
    if resp.status == 200:
        return "REGISTERED", "Okta rendered its sign-in page (prompt=none not honoured); callback accepted"
    return "UNCLEAR", f"HTTP {resp.status}: {text[:180]}"


VERDICT_STATUS = {"REGISTERED": "PASS", "REGISTERED_BAD_SCOPE": "PASS", "REGISTERED_OTHER": "WARN",
                  "NOT_REGISTERED": "FAIL", "BAD_CLIENT": "FAIL", "UNREACHABLE": "FAIL",
                  "UNCLEAR": "WARN"}


def check_secret(env: str, rep: Report, doc: dict, issuer: str, client_id: str,
                 secret: str) -> None:
    endpoint = doc.get("introspection_endpoint") or issuer.rstrip("/") + "/v1/introspect"
    form = {"token": "not-a-real-token", "token_type_hint": "access_token"}
    basic = base64.b64encode(f"{client_id}:{secret}".encode()).decode()
    r1 = http(endpoint, "POST", urllib.parse.urlencode(form).encode(),
              {"Authorization": f"Basic {basic}", "Accept": "application/json",
               "Content-Type": "application/x-www-form-urlencoded"})
    j1 = r1.json() or {}
    if r1.status == 200 and "active" in j1:
        rep.add(env, "client secret", "PASS",
                "introspection accepted client_secret_basic; the id+secret pair is valid")
        return
    r2 = http(endpoint, "POST",
              urllib.parse.urlencode({**form, "client_id": client_id,
                                      "client_secret": secret}).encode(),
              {"Accept": "application/json",
               "Content-Type": "application/x-www-form-urlencoded"})
    j2 = r2.json() or {}
    if r2.status == 200 and "active" in j2:
        rep.add(env, "client secret", "PASS",
                "introspection accepted client_secret_post only (send the secret in the body, "
                "not as Basic auth)")
        return
    why = (j2.get("error_description") or j2.get("error") or j1.get("error_description")
           or j1.get("error") or r2.error or r1.error or f"HTTP {r1.status}/{r2.status}")
    rep.add(env, "client secret", "FAIL", f"{endpoint}: {why}")


def run_env(env: str, rep: Report, clients: dict, redirects: dict) -> None:
    disc_given = cfg("OKTA_DISCOVERY_URL", env)
    client_id = cfg("OKTA_CLIENT_ID", env)
    redirect = cfg("OKTA_REDIRECT_URI", env)
    missing = [n for n, v in (("OKTA_DISCOVERY_URL", disc_given), ("OKTA_CLIENT_ID", client_id),
                              ("OKTA_REDIRECT_URI", redirect)) if not v]
    if missing:
        rep.add(env, "config", "SKIP", "not set: " + ", ".join(f"{m}_{env}" for m in missing))
        return
    redirect = norm_redirect(redirect)
    scopes = split_list(cfg("OKTA_SCOPES", env, "openid profile"))
    claim = cfg("OKTA_GROUP_CLAIM", env)
    secret = cfg("OKTA_CLIENT_SECRET", env)

    # 1. discovery
    url = discovery_url(disc_given)
    t0 = time.time()
    resp = http(url)
    ms = int((time.time() - t0) * 1000)
    doc = resp.json() if resp.status == 200 else None
    if not isinstance(doc, dict) or "authorization_endpoint" not in doc:
        why = resp.error or f"HTTP {resp.status}: {text_of(resp)[:160]}"
        rep.add(env, "discovery", "FAIL", f"{url} -> {why}")
        return
    issuer = str(doc.get("issuer", ""))
    authz = doc["authorization_endpoint"]
    rep.facts[env] = {k: doc.get(k) for k in (
        "issuer", "authorization_endpoint", "token_endpoint", "introspection_endpoint",
        "userinfo_endpoint", "jwks_uri", "end_session_endpoint", "revocation_endpoint",
        "scopes_supported", "claims_supported", "grant_types_supported",
        "code_challenge_methods_supported", "token_endpoint_auth_methods_supported",
        "id_token_signing_alg_values_supported", "response_types_supported")}
    rep.add(env, "discovery", "PASS", f"issuer {issuer} ({ms} ms)")
    rep.add(env, "capabilities", "INFO",
            f"grants {','.join(doc.get('grant_types_supported') or ['?'])}; client auth "
            f"{','.join(doc.get('token_endpoint_auth_methods_supported') or ['?'])}; PKCE "
            f"{','.join(doc.get('code_challenge_methods_supported') or ['none'])}; id_token algs "
            f"{','.join(doc.get('id_token_signing_alg_values_supported') or ['?'])}; logout endpoint "
            f"{'yes' if doc.get('end_session_endpoint') else 'no'}; userinfo "
            f"{'yes' if doc.get('userinfo_endpoint') else 'no'}")
    if doc.get("claims_supported"):
        rep.add(env, "claims published", "INFO", ", ".join(doc["claims_supported"]))
    disc_host = urllib.parse.urlsplit(url).netloc.lower()
    if urllib.parse.urlsplit(issuer).netloc.lower() != disc_host:
        rep.add(env, "issuer host", "WARN", f"issuer {issuer} is not on {disc_host}")

    # 2. metadata
    supported = doc.get("scopes_supported") or []
    if supported:
        absent = [s for s in scopes if s not in supported]
        if absent:
            rep.add(env, "scopes published", "WARN",
                    f"{' '.join(absent)} not in scopes_supported (custom scopes are listed only "
                    "with 'Include in public metadata' on; the callback probe asks for them for real)")
        else:
            rep.add(env, "scopes published", "PASS", f"{' '.join(scopes)} all in scopes_supported")
    else:
        rep.add(env, "scopes published", "SKIP", "no scopes_supported in discovery")
    if claim:
        claims = doc.get("claims_supported") or []
        if not claims:
            rep.add(env, f"claim {claim}", "SKIP", "no claims_supported in discovery")
        elif claim in claims:
            rep.add(env, f"claim {claim}", "PASS", "listed in claims_supported")
        else:
            rep.add(env, f"claim {claim}", "WARN",
                    "not in claims_supported (custom claims are listed only with 'Include in "
                    "public metadata' on; confirm on a real ID token)")

    # 3. jwks
    jwks_uri = doc.get("jwks_uri")
    if jwks_uri:
        jr = http(jwks_uri)
        keys = (jr.json() or {}).get("keys") if jr.status == 200 else None
        if keys:
            rep.add(env, "jwks", "PASS", f"{len(keys)} signing key(s) at {jwks_uri}")
        else:
            rep.add(env, "jwks", "FAIL", f"{jwks_uri} -> {jr.error or f'HTTP {jr.status}'}")

    # 4. callback
    verdict, detail = probe_authorize(authz, client_id, redirect, scopes)
    rep.add(env, "callback", VERDICT_STATUS[verdict], f"{redirect}: {detail}")
    if verdict == "REGISTERED_BAD_SCOPE":
        rep.add(env, "scopes (live)", "FAIL", f"Okta rejected '{' '.join(scopes)}': {detail}")
    elif verdict == "REGISTERED":
        rep.add(env, "scopes (live)", "PASS", f"'{' '.join(scopes)}' accepted on /authorize")

    # 5. secret
    if secret:
        check_secret(env, rep, doc, issuer, client_id, secret)
    else:
        rep.add(env, "client secret", "SKIP",
                f"OKTA_CLIENT_SECRET_{env} not set (set it to prove the id+secret pair)")

    # parity with the Google callback, when configured
    google = cfg("GOOGLE_REDIRECT_URI", env)
    if google:
        google = norm_redirect(google)
        same = same_endpoint(google, redirect)
        rep.add(env, "same callback as Google", "PASS" if same else "WARN",
                f"Okta {redirect} | Google {google}" + ("" if same else " (they differ)"))

    redirects[env] = redirect
    if verdict not in ("BAD_CLIENT", "UNREACHABLE"):
        key = (issuer, client_id)
        clients.setdefault(key, {"envs": [], "authz": authz, "scopes": scopes})["envs"].append(env)


def run_matrix(rep: Report, clients: dict, redirects: dict) -> None:
    for (_issuer, client_id), info in clients.items():
        who = "/".join(info["envs"]) + " client"
        own = {redirects[e] for e in info["envs"] if e in redirects}
        for env, uri in redirects.items():
            if env in info["envs"] or uri in own:
                continue  # its own callbacks were probed above
            verdict, detail = probe_authorize(info["authz"], client_id, uri, info["scopes"])
            if verdict.startswith("REGISTERED"):
                rep.add(env, "cross-env", "WARN",
                        f"{who} ({client_id}) ACCEPTS {uri}; one client serving several "
                        "environments is fine, a prod client accepting non-prod callbacks is not")
            elif verdict in ("NOT_REGISTERED", "BAD_CLIENT"):
                rep.add(env, "cross-env", "INFO", f"{who} rejects {uri} (expected)")
            else:
                rep.add(env, "cross-env", "INFO", f"{who} x {uri}: {detail}")


def main(argv: list[str] | None = None) -> int:
    global _TIMEOUT
    ap = argparse.ArgumentParser(
        description="Okta preflight for the Synapse sign-in (see the module docstring).")
    ap.add_argument("--env-file", help="KEY=VALUE file to load (exported vars win)")
    ap.add_argument("--envs", help="comma list, default $AUTHCHECK_ENVS or E1,E2,E3")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--no-matrix", action="store_true",
                    help="skip probing each client against the other environments' callbacks")
    ap.add_argument("--timeout", type=int, default=_TIMEOUT, help="seconds per request")
    ap.add_argument("--inventory", metavar="ENV",
                    help="sign in once on this environment's client through the browser and list "
                         "every claim Okta returns (needs OKTA_LOCAL_REDIRECT_URI on the client)")
    ap.add_argument("--redact", action="store_true",
                    help="mask personal values in the inventory (e-mails keep their domain)")
    ap.add_argument("--no-browser", action="store_true",
                    help="print the sign-in URL instead of opening a browser")
    ap.add_argument("--wait", type=int, default=180, help="seconds to wait for the sign-in")
    ap.add_argument("--out", metavar="FILE", help="write the JSON report here as well")
    args = ap.parse_args(argv)
    _TIMEOUT = args.timeout
    global REDACT
    REDACT = args.redact
    if args.env_file:
        if not os.path.exists(args.env_file):
            print(f"env file not found: {args.env_file}", file=sys.stderr)
            return 2
        load_env_file(args.env_file)
    envs = split_list(args.envs or cfg("AUTHCHECK_ENVS", default=DEFAULT_ENVS) or DEFAULT_ENVS)

    rep = Report()
    clients: dict = {}
    redirects: dict = {}
    for env in envs:
        run_env(env, rep, clients, redirects)
    if not args.no_matrix and clients and len(redirects) > 1:
        run_matrix(rep, clients, redirects)
    if args.inventory:
        env = args.inventory.strip()
        if env not in envs:
            run_env(env, rep, clients, redirects)
        inventory(env, rep, timeout=args.wait, open_browser=not args.no_browser)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(rep.dump(), fh, indent=2)
    if args.json:
        print(json.dumps(rep.dump(), indent=2))
    else:
        print_report(rep, "Okta sign-in preflight")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
