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

--inventory <ENV> answers "what do we actually get from Google": it runs the
consent once, through a browser, on that environment's client with a local
callback (GOOGLE_LOCAL_REDIRECT_URI, default http://localhost:8400/callback;
Google allows http on localhost, add it to the client's redirect URIs), asks
for offline access, then prints every claim of the ID token and of /userinfo,
the scopes actually granted, whether a refresh token came back (the app's
stored connection needs one), whether the account is a Workspace account
(the hd claim) and, with GOOGLE_BQ_PROJECT set, whether the token can run a
BigQuery dry run there. --redact masks personal values; --out writes the
JSON for identity_map.py.

Configuration comes from environment variables; NAME_<ENV> wins over NAME:

  AUTHCHECK_ENVS              comma list of environments, default E1,E2,E3
  GOOGLE_CLIENT_ID            shared client, or GOOGLE_CLIENT_ID_<ENV> per environment
  GOOGLE_CLIENT_SECRET        optional; enables check 4 (per-env variant too)
  GOOGLE_REDIRECT_URI_<ENV>   https:// is assumed when the scheme is missing
                              (Google refuses plain http except on localhost)
  GOOGLE_SCOPES               default "openid email profile"
  GOOGLE_LOCAL_REDIRECT_URI   --inventory: the local callback, default http://localhost:8400/callback
  GOOGLE_INVENTORY_SCOPES     --inventory: default GOOGLE_SCOPES + the BigQuery scope
  GOOGLE_BQ_PROJECT           --inventory: a project to dry-run "SELECT 1" in (no bytes billed)
  GOOGLE_EXPECTED_HD          --inventory: the Workspace domain the account should belong to
  OKTA_REDIRECT_URI_<ENV>     optional; reported as parity with the Google callback

TLS: `truststore` (the OS keychain, where corporate roots live) when it is
installed, else AUTHCHECK_CA_BUNDLE / REQUESTS_CA_BUNDLE / SSL_CERT_FILE, else
the system roots. AUTHCHECK_TLS_INSECURE=1 turns verification off, loudly.
HTTPS_PROXY is honoured.

Usage:
  python google_auth_check.py --env-file .env.authcheck.local
  python google_auth_check.py --envs E1 --json
  python google_auth_check.py --env-file .env.authcheck.local --inventory E1 --redact --out google.json

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
              "Run again with --inventory <ENV> to see what the provider returns for a person.")


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


# ------------------------------------------------------------ inventory --
REDACT = False
KEEP_UNDER_REDACT = {"iss", "aud", "azp", "alg", "kid", "typ", "scope", "hd", "token_type", "groups",
                     "email_verified", "access_type", "locale"}
BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"
TOKENINFO = "https://oauth2.googleapis.com/tokeninfo"
BIGQUERY = "https://bigquery.googleapis.com/bigquery/v2"


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
    except Exception as exc:  # noqa: BLE001
        return f"failed: {type(exc).__name__}"
    return "verified"


def mask(value, key: str = ""):
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
    if text.startswith("http"):
        return "url(" + urllib.parse.urlsplit(text).netloc + ")"
    if len(text) <= 3:
        return "***"
    return f"{text[:2]}...{text[-1]}({len(text)})"


def show(value) -> str:
    if isinstance(value, list):
        inner = ", ".join(str(v) for v in value[:12]) + (", ..." if len(value) > 12 else "")
        return f"list[{len(value)}]: {inner}"
    if isinstance(value, dict):
        return "object: " + ", ".join(sorted(value)[:12])
    return f"{type(value).__name__}: {value}"


def receive_code(redirect_uri: str, state: str, timeout: int, open_browser: bool,
                 url: str) -> dict:
    """Serve the local callback once and return its query as a dict, or {"error": ...}."""
    import http.server

    parts = urllib.parse.urlsplit(redirect_uri)
    host, port, path = parts.hostname or "127.0.0.1", parts.port or 80, parts.path or "/"
    got: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
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

        def log_message(self, *args):
            pass

    try:
        server = http.server.HTTPServer((host, port), Handler)
    except OSError as exc:
        return {"error": f"cannot listen on {host}:{port} for the callback: {exc}"}
    server.timeout = 1
    print(f"\nConsent here (the browser should open by itself):\n  {url}\n", file=sys.stderr)
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


def bq_get(path: str, token: str) -> Resp:
    return http(BIGQUERY + path, headers={"Authorization": f"Bearer {token}",
                                          "Accept": "application/json"})


def bq_dry_run(project: str, token: str) -> tuple[str, str]:
    """('ok' | 'denied' | 'error', detail): a dry run of SELECT 1 costs nothing
    and proves the scope, the API enablement and the IAM on the project."""
    body = json.dumps({"query": "SELECT 1", "useLegacySql": False, "dryRun": True}).encode()
    r = http(f"{BIGQUERY}/projects/{urllib.parse.quote(project)}/queries", "POST", body,
             {"Authorization": f"Bearer {token}", "Content-Type": "application/json",
              "Accept": "application/json"})
    doc = r.json() or {}
    if r.status == 200:
        return "ok", f"dry run accepted in {project} (jobComplete={doc.get('jobComplete')})"
    err = (doc.get("error") or {})
    msg = str(err.get("message") or r.error or f"HTTP {r.status}")[:200]
    if r.status in (401, 403):
        return "denied", f"HTTP {r.status}: {msg}"
    return "error", f"HTTP {r.status}: {msg}"


GOOGLE_CLAIM_ORDER = ("sub", "email", "email_verified", "hd", "name", "given_name", "family_name",
                      "picture", "locale", "iss", "aud", "azp", "iat", "exp", "nonce", "at_hash")


def ordered(claims: dict) -> list[str]:
    return ([c for c in GOOGLE_CLAIM_ORDER if c in claims]
            + sorted(c for c in claims if c not in GOOGLE_CLAIM_ORDER))


def inventory(env: str, rep: Report, disc: dict, *, timeout: int, open_browser: bool) -> None:
    client_id = cfg("GOOGLE_CLIENT_ID", env)
    secret = cfg("GOOGLE_CLIENT_SECRET", env)
    if not client_id:
        rep.add(env, "inventory", "FAIL", f"GOOGLE_CLIENT_ID(_{env}) not set")
        return
    redirect_local = norm_redirect(cfg("GOOGLE_LOCAL_REDIRECT_URI", env,
                                       "http://localhost:8400/callback"))
    if redirect_local.startswith("https://localhost"):
        redirect_local = "http://" + redirect_local[len("https://"):]
    base_scopes = split_list(cfg("GOOGLE_SCOPES", env, "openid email profile"))
    scopes = split_list(cfg("GOOGLE_INVENTORY_SCOPES", env, "")) or [
        *dict.fromkeys([*base_scopes, "openid", "email", "profile", BIGQUERY_SCOPE])]
    expected_hd = (cfg("GOOGLE_EXPECTED_HD", env) or "").lower().lstrip("@")
    project = cfg("GOOGLE_BQ_PROJECT", env)

    state, nonce = secrets.token_urlsafe(16), secrets.token_urlsafe(16)
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    url = disc["authorization_endpoint"] + "?" + urllib.parse.urlencode({
        "client_id": client_id, "redirect_uri": redirect_local, "response_type": "code",
        "scope": " ".join(scopes), "state": state, "nonce": nonce, "access_type": "offline",
        "prompt": "consent", "include_granted_scopes": "true",
        "code_challenge": challenge, "code_challenge_method": "S256"})
    got = receive_code(redirect_local, state, timeout, open_browser, url)
    if got.get("error"):
        why = got["error"]
        if got.get("error_description"):
            why = f"Google refused: {why}: {got['error_description']}"
        rep.add(env, "consent", "FAIL", why)
        return
    form = {"grant_type": "authorization_code", "code": got.get("code", ""),
            "redirect_uri": redirect_local, "code_verifier": verifier, "client_id": client_id}
    if secret:
        form["client_secret"] = secret
    r = http(disc["token_endpoint"], "POST", urllib.parse.urlencode(form).encode(),
             {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"})
    tokens = r.json() or {}
    if r.status != 200 or not tokens.get("access_token"):
        rep.add(env, "consent", "FAIL",
                f"the code exchange failed: {tokens.get('error', '')} {tokens.get('error_description', '')}"
                f"{r.error}".strip()
                + ("" if secret else " (a Web application client needs GOOGLE_CLIENT_SECRET)"))
        return
    granted = split_list(str(tokens.get("scope", "")))
    rep.add(env, "consent", "PASS",
            f"consent given; tokens: access_token ({tokens.get('expires_in', '?')}s)"
            + (", id_token" if tokens.get("id_token") else ", no id_token")
            + (", refresh_token" if tokens.get("refresh_token") else ", no refresh_token"))
    missing = [s_ for s_ in scopes if s_ not in granted and s_ != "openid"
               and not any(g.endswith(s_) or s_.endswith(g.split("/")[-1]) for g in granted)]
    rep.add(env, "scopes granted", "PASS" if not missing else "WARN",
            ", ".join(granted) + (f"; NOT granted: {', '.join(missing)} (unticked on the consent screen?)"
                                  if missing else ""))

    claims: dict = {}
    if tokens.get("id_token"):
        decoded = decode_jwt(tokens["id_token"])
        if decoded:
            header, claims = decoded
            jr = http(disc.get("jwks_uri", ""))
            verdict = verify_rs256(tokens["id_token"], jr.json() if jr.status == 200 else {})
            problems = []
            if claims.get("iss") not in ("https://accounts.google.com", "accounts.google.com"):
                problems.append(f"iss {claims.get('iss')}")
            if claims.get("aud") != client_id:
                problems.append("aud is not our client")
            if claims.get("nonce") != nonce:
                problems.append("nonce mismatch")
            status = "PASS" if verdict == "verified" and not problems else (
                "WARN" if verdict.startswith("unverified") and not problems else "FAIL")
            rep.add(env, "id_token.verify", status,
                    f"alg {header.get('alg')} kid {header.get('kid')}: signature {verdict}"
                    + ("; " + "; ".join(problems) if problems else "; iss, aud and nonce match"))
            for name in ordered(claims):
                rep.add(env, f"id_token.{name}", "INFO", show(mask(claims[name], name)))

    ui: dict = {}
    if disc.get("userinfo_endpoint"):
        ur = http(disc["userinfo_endpoint"], headers={
            "Authorization": f"Bearer {tokens['access_token']}", "Accept": "application/json"})
        ui = ur.json() if ur.status == 200 and isinstance(ur.json(), dict) else {}
        extra = [c for c in ordered(ui) if c not in claims]
        rep.add(env, "userinfo", "PASS" if ui else "WARN",
                f"{len(ui)} claims; beyond the id_token: {', '.join(extra) or 'none'}" if ui
                else f"HTTP {ur.status} {ur.error}")
        for name in extra:
            rep.add(env, f"userinfo.{name}", "INFO", show(mask(ui[name], name)))

    tr = http(TOKENINFO + "?" + urllib.parse.urlencode({"access_token": tokens["access_token"]}))
    ti = tr.json() if tr.status == 200 and isinstance(tr.json(), dict) else {}
    if ti:
        rep.add(env, "tokeninfo", "INFO",
                f"aud {ti.get('aud')}; scope {ti.get('scope')}; expires_in {ti.get('expires_in')}; "
                f"access_type {ti.get('access_type')}")

    everything = {**ui, **claims}
    hd = str(everything.get("hd") or "").lower()
    if expected_hd:
        rep.add(env, "need: workspace account", "PASS" if hd == expected_hd else "FAIL",
                f"hd={hd or 'absent'}" + ("" if hd == expected_hd else
                                           f" (expected {expected_hd}: a personal Google account, or another domain)"))
    else:
        rep.add(env, "need: workspace account", "PASS" if hd else "WARN",
                f"hd={hd}: a Workspace account" if hd else
                "no hd claim: a personal Google account, not a Workspace one (set GOOGLE_EXPECTED_HD to pin the domain)")
    rep.add(env, "need: verified email", "PASS" if everything.get("email_verified") is True else "FAIL",
            f"{mask(everything.get('email'), 'email')} email_verified={everything.get('email_verified')}")
    rep.add(env, "need: refresh token", "PASS" if tokens.get("refresh_token") else "FAIL",
            "refresh_token present: the app can hold the connection and mint access tokens later"
            if tokens.get("refresh_token") else
            "no refresh_token: the stored connection cannot outlive this hour (access_type=offline and "
            "prompt=consent were sent; a client of type other than Web application behaves so)")
    bq_scope = any(g.rstrip("/").endswith("/bigquery") or g.endswith("bigquery.readonly") for g in granted)
    rep.add(env, "need: bigquery scope", "PASS" if bq_scope else "WARN",
            "granted" if bq_scope else "not granted; queries as this person will fail")
    bq: dict = {"project": project}
    if bq_scope:
        pr = bq_get("/projects?maxResults=50", tokens["access_token"])
        projects = [p_.get("id") for p_ in ((pr.json() or {}).get("projects") or [])] if pr.status == 200 else None
        if projects is not None:
            bq["projects_visible"] = len(projects)
            rep.add(env, "bigquery projects", "INFO",
                    f"{len(projects)} project(s) visible to this account: {', '.join(projects[:8])}"
                    + (", ..." if len(projects) > 8 else ""))
        else:
            rep.add(env, "bigquery projects", "WARN", f"HTTP {pr.status} {pr.error or text_of(pr)[:120]}")
        if project:
            kind, detail = bq_dry_run(project, tokens["access_token"])
            bq["dry_run"] = kind
            rep.add(env, "bigquery dry-run", {"ok": "PASS", "denied": "FAIL", "error": "WARN"}[kind], detail)
        else:
            rep.add(env, "bigquery dry-run", "SKIP", "GOOGLE_BQ_PROJECT not set")

    rep.facts.setdefault("inventory", {})[env] = mask({
        "provider": "google", "client_id": client_id, "scopes_requested": scopes,
        "scopes_granted": granted, "refresh_token": bool(tokens.get("refresh_token")),
        "access_token_expires_in": tokens.get("expires_in"),
        "sub": everything.get("sub"), "email": everything.get("email"),
        "email_verified": everything.get("email_verified"), "hd": hd or None,
        "name": everything.get("name"), "given_name": everything.get("given_name"),
        "family_name": everything.get("family_name"), "picture": bool(everything.get("picture")),
        "id_token_claims": ordered(claims), "userinfo_claims": ordered(ui), "bigquery": bq,
    })


def text_of(resp: Resp) -> str:
    text = html.unescape(re.sub(r"<[^>]+>", " ", resp.body))
    return re.sub(r"\s+", " ", text).strip()


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
    ap.add_argument("--inventory", metavar="ENV",
                    help="run the consent once on this environment's client through the browser "
                         "and list everything Google returns (needs GOOGLE_LOCAL_REDIRECT_URI on the client)")
    ap.add_argument("--redact", action="store_true",
                    help="mask personal values in the inventory (e-mails keep their domain)")
    ap.add_argument("--no-browser", action="store_true",
                    help="print the consent URL instead of opening a browser")
    ap.add_argument("--wait", type=int, default=180, help="seconds to wait for the consent")
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
        if args.inventory:
            inventory(args.inventory.strip(), rep, disc, timeout=args.wait,
                      open_browser=not args.no_browser)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(rep.dump(), fh, indent=2)
    if args.json:
        print(json.dumps(rep.dump(), indent=2))
    else:
        print_report(rep, "Google sign-in preflight")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
