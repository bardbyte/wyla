"""The sign-in preflight scripts (apps/synapse_admin/scripts) read a provider's
verdict off its authorize endpoint without following redirects. These tests
pin that reading with canned responses; nothing here touches the network."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


okta = load("okta_check")
google = load("google_auth_check")

CALLBACK = "https://dev.example.com/callback"


def canned(mod, status, body="", location=None):
    """Replace the module's http() with one that returns a fixed response."""
    headers = {"Location": location} if location else {}
    mod.http = lambda *a, **k: mod.Resp(status, headers, body)


# ---------------------------------------------------------------- okta ---
def test_okta_bounce_to_callback_means_registered():
    canned(okta, 302, location=CALLBACK + "?error=login_required&state=x")
    verdict, detail = okta.probe_authorize("https://okta/authorize", "cid", CALLBACK, ["openid"])
    assert verdict == "REGISTERED" and "login_required" in detail


def test_okta_invalid_scope_still_proves_the_callback():
    canned(okta, 302, location=CALLBACK + "?error=invalid_scope&error_description=One+or+more+scopes")
    verdict, detail = okta.probe_authorize("https://okta/authorize", "cid", CALLBACK, ["openid", "x"])
    assert verdict == "REGISTERED_BAD_SCOPE" and "scopes" in detail


def test_okta_unregistered_callback_and_bad_client_read_off_the_error_page():
    page = ("<html><p>Error Code: invalid_request</p><p>Description: The 'redirect_uri' parameter "
            "must be a Login redirect URI in the client app settings: https://o/admin</p></html>")
    canned(okta, 400, body=page)
    verdict, detail = okta.probe_authorize("https://okta/authorize", "cid", CALLBACK, ["openid"])
    assert verdict == "NOT_REGISTERED" and detail.startswith("Description: The 'redirect_uri'")

    canned(okta, 400, body='{"errorCode":"invalid_client","errorSummary":"Invalid value for \'client_id\' parameter."}')
    verdict, _ = okta.probe_authorize("https://okta/authorize", "cid", CALLBACK, ["openid"])
    assert verdict == "BAD_CLIENT"


def test_okta_redirect_to_its_own_sign_in_counts_as_registered():
    canned(okta, 302, location="https://org.okta.com/login/login.htm?fromURI=%2Foauth2")
    verdict, _ = okta.probe_authorize("https://okta/authorize", "cid", CALLBACK, ["openid"])
    assert verdict == "REGISTERED"


def test_okta_unreachable_is_reported_not_raised():
    okta.http = lambda *a, **k: okta.Resp(0, {}, "", error="URLError: tunnel failed")
    verdict, detail = okta.probe_authorize("https://okta/authorize", "cid", CALLBACK, ["openid"])
    assert verdict == "UNREACHABLE" and "tunnel" in detail


def test_discovery_url_accepts_issuer_or_well_known():
    assert okta.discovery_url("https://o/oauth2/as1/") == "https://o/oauth2/as1/.well-known/openid-configuration"
    given = "https://o/.well-known/oauth-authorization-server/oauth2/as1"
    assert okta.discovery_url(given) == given


def test_redirect_defaults_to_https_and_compares_loosely():
    assert okta.norm_redirect("dev.example.com/callback") == CALLBACK
    assert okta.same_endpoint(CALLBACK + "/?a=1", "HTTPS://DEV.example.com/callback")
    assert not okta.same_endpoint(CALLBACK, "https://qa.example.com/callback")


# -------------------------------------------------------------- google ---
def _auth_error(code: str, msg: str) -> str:
    import base64
    raw = bytes([0x0A, len(code)]) + code.encode() + bytes([0x12, len(msg)]) + msg.encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def test_google_error_blob_decodes_code_and_message():
    loc = ("https://accounts.google.com/signin/oauth/error?authError="
           + _auth_error("redirect_uri_mismatch", "The redirect URI in the request does not match."))
    assert google.decode_auth_error(loc) == (
        "redirect_uri_mismatch", "The redirect URI in the request does not match.")


def test_google_verdicts():
    canned(google, 302, location="https://accounts.google.com/signin/oauth/error?authError="
           + _auth_error("redirect_uri_mismatch", "no match"))
    assert google.probe_authorize("https://g/auth", "cid", CALLBACK, ["openid"])[0] == "NOT_REGISTERED"

    canned(google, 302, location="https://accounts.google.com/signin/oauth/error?authError="
           + _auth_error("invalid_client", "The OAuth client was not found."))
    assert google.probe_authorize("https://g/auth", "cid", CALLBACK, ["openid"])[0] == "BAD_CLIENT"

    canned(google, 302, location=CALLBACK + "?error=login_required&state=x")
    assert google.probe_authorize("https://g/auth", "cid", CALLBACK, ["openid"])[0] == "REGISTERED"

    canned(google, 302, location="https://accounts.google.com/v3/signin/identifier?flowName=GeneralOAuthFlow")
    assert google.probe_authorize("https://g/auth", "cid", CALLBACK, ["openid"])[0] == "REGISTERED"


# ---------------------------------------------------------------- ldap ---
def test_ldap_helpers():
    pytest.importorskip("ldap3")
    ldap = load("ldap_check")
    assert ldap.parse_server("ldaps://dc1.example.com:636") == ("ldaps", "dc1.example.com", 636)
    assert ldap.parse_server("dc1.example.com") == ("ldaps", "dc1.example.com", 636)
    assert ldap.parse_server("ldap://dc1.example.com") == ("ldap", "dc1.example.com", 389)
    base = "OU=People,DC=corp,DC=example,DC=com"
    assert ldap.bind_candidates("svc.app", None, base) == [
        "svc.app", "svc.app@corp.example.com", "corp\\svc.app"]
    assert ldap.bind_candidates("svc.app@corp.example.com", None, base) == ["svc.app@corp.example.com"]
    assert ldap.bind_candidates("svc.app", "CN=svc.app," + base, base) == ["CN=svc.app," + base]
    reason = ldap.ad_reason({"description": "invalidCredentials",
                             "message": "80090308: LdapErr: DSID-0C09042A, comment: AcceptSecurityContext error, data 52e, v3839"})
    assert reason.startswith("invalidCredentials (AD data 52e: invalid credentials")


def test_env_file_loader_keeps_exported_values(tmp_path, monkeypatch):
    env = tmp_path / "x.env"
    env.write_text('# c\nexport A_E1="one"\nB=two # trailing\nC=\nA_E1=ignored\n')
    monkeypatch.delenv("A_E1", raising=False)
    monkeypatch.setenv("B", "shell-wins")
    okta.load_env_file(str(env))
    assert okta.cfg("A", "E1") == "one"
    assert okta.cfg("B") == "shell-wins"
    assert okta.cfg("C", default="d") == "d"
    monkeypatch.delenv("A_E1")


# ----------------------------------------------------------- inventory ---
def _rsa_jwt(claims: dict, kid: str = "k1"):
    """A signed RS256 token and the JWKS that verifies it."""
    import base64
    import json as _json
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()  # noqa: E731
    head = b64(_json.dumps({"alg": "RS256", "kid": kid}).encode())
    body = b64(_json.dumps(claims).encode())
    sig = key.sign(f"{head}.{body}".encode(), padding.PKCS1v15(), hashes.SHA256())
    numbers = key.public_key().public_numbers()
    jwk = {"kty": "RSA", "kid": kid, "alg": "RS256",
           "n": b64(numbers.n.to_bytes((numbers.n.bit_length() + 7) // 8, "big")),
           "e": b64(numbers.e.to_bytes((numbers.e.bit_length() + 7) // 8, "big"))}
    return f"{head}.{body}.{b64(sig)}", {"keys": [jwk]}


def test_jwt_decode_and_signature_verdicts():
    pytest.importorskip("cryptography")
    token, jwks = _rsa_jwt({"sub": "00u1", "email": "ana@example.com", "groups": ["Sem-Admins"]})
    header, claims = okta.decode_jwt(token)
    assert header["alg"] == "RS256" and claims["email"] == "ana@example.com"
    assert okta.decode_jwt("opaque-token") is None and okta.decode_jwt("a.b") is None
    assert okta.verify_rs256(token, jwks) == "verified"
    broken = token[:-4] + ("AAAA" if not token.endswith("AAAA") else "BBBB")
    assert okta.verify_rs256(broken, jwks).startswith("failed")
    assert okta.verify_rs256(token, {"keys": []}).startswith("failed: no key")
    assert google.verify_rs256(token, jwks) == "verified"           # the same helper, both scripts


def test_redaction_keeps_names_of_things_and_masks_people():
    for mod in (okta, google):
        mod.REDACT = True
        try:
            assert mod.mask("ana@example.com", "email") == "a***@example.com"
            assert mod.mask("Ana Lyst", "name") == "An...t(8)"
            assert mod.mask(["Sem-Admins", "Sem-Stewards"], "groups") == ["Sem-Admins", "Sem-Stewards"]
            assert mod.mask("https://org.okta.com/oauth2/as1", "iss") == "https://org.okta.com/oauth2/as1"
            assert mod.mask(1700000000, "exp") == 1700000000 and mod.mask(True, "email_verified") is True
            assert mod.mask({"email": "bo@x.io", "groups": ["G"]}) == {"email": "b***@x.io", "groups": ["G"]}
        finally:
            mod.REDACT = False
    assert okta.mask("ana@example.com", "email") == "ana@example.com"   # off: untouched
    assert okta.ordered({"exp": 1, "sub": "s", "zz": 1, "email": "e"}) == ["sub", "email", "exp", "zz"]


def test_local_callback_listener_receives_the_code(monkeypatch):
    import socket
    import threading
    import urllib.request
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    redirect = f"http://127.0.0.1:{port}/callback"
    result: dict = {}

    def serve():
        result.update(okta.receive_code(redirect, "st-1", timeout=10, open_browser=False, url="about:blank"))

    thread = threading.Thread(target=serve)
    thread.start()
    deadline = __import__("time").time() + 5
    while __import__("time").time() < deadline:
        try:
            with urllib.request.urlopen(redirect + "?code=abc&state=st-1", timeout=1) as r:
                assert b"Received" in r.read()
            break
        except OSError:
            __import__("time").sleep(0.05)
    thread.join(timeout=10)
    assert result == {"code": "abc", "state": "st-1"}


def test_local_callback_refuses_a_foreign_state(monkeypatch):
    import socket
    import threading
    import urllib.request
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    redirect = f"http://127.0.0.1:{port}/callback"
    result: dict = {}
    thread = threading.Thread(target=lambda: result.update(
        google.receive_code(redirect, "expected", timeout=10, open_browser=False, url="about:blank")))
    thread.start()
    deadline = __import__("time").time() + 5
    while __import__("time").time() < deadline:
        try:
            urllib.request.urlopen(redirect + "?code=abc&state=other", timeout=1).read()
            break
        except OSError:
            __import__("time").sleep(0.05)
    thread.join(timeout=10)
    assert "another state" in result.get("error", "")


def test_code_exchange_falls_back_to_the_body_secret():
    calls = []

    def fake_http(url, method="GET", data=None, headers=None):
        calls.append((headers or {}).get("Authorization", "body"))
        if len(calls) == 1:
            return okta.Resp(401, {}, '{"error":"invalid_client"}')
        return okta.Resp(200, {}, '{"id_token":"h.p.s","access_token":"at","expires_in":3600}')

    okta.http = fake_http
    tokens, how = okta.exchange_code("https://okta/v1/token", "cid", "sec", "code", "http://localhost:8400/callback", "v")
    assert tokens["id_token"] == "h.p.s" and how == "client_secret_post"
    assert calls[0].startswith("Basic ") and calls[1] == "body"

    okta.http = lambda *a, **k: okta.Resp(400, {}, '{"error":"invalid_grant","error_description":"bad code"}')
    tokens, how = okta.exchange_code("https://okta/v1/token", "cid", None, "code", "http://localhost:8400/callback", "v")
    assert tokens == {} and "invalid_grant" in how and how.startswith("none (public client")


def test_bigquery_dry_run_verdicts():
    canned(google, 200, body='{"jobComplete": true, "totalBytesProcessed": "0"}')
    assert google.bq_dry_run("proj", "tok") == ("ok", "dry run accepted in proj (jobComplete=True)")
    canned(google, 403, body='{"error": {"code": 403, "message": "Access Denied: Project proj"}}')
    kind, detail = google.bq_dry_run("proj", "tok")
    assert kind == "denied" and "Access Denied" in detail
    canned(google, 500, body="")
    assert google.bq_dry_run("proj", "tok")[0] == "error"


def test_ldap_inventory_helpers():
    pytest.importorskip("ldap3")
    ldap = load("ldap_check")
    assert ldap.uac_flags(514) == ["ACCOUNTDISABLE", "NORMAL_ACCOUNT"]
    assert ldap.uac_flags("66048") == ["NORMAL_ACCOUNT", "DONT_EXPIRE_PASSWORD"]
    assert ldap.uac_flags("x") == []
    assert ldap.rdn("CN=Sem-Stewards,OU=Groups,DC=corp,DC=example") == "Sem-Stewards"
    ldap.REDACT = True
    try:
        assert ldap.mask("CN=Ana Lyst,OU=People,DC=corp", "dn") == "CN=***,OU=People,DC=corp"
        assert ldap.mask(["Sem-Admins"], "groups") == ["Sem-Admins"]
        assert ldap.mask("ana@corp.example", "mail") == "a***@corp.example"
    finally:
        ldap.REDACT = False


# --------------------------------------------------------- identity map ---
identity_map = load("identity_map")

OKTA_PERSON = {"provider": "okta", "sub": "00u1", "email": "ana@corp.example", "email_claim": "email",
               "preferred_username": "ana@corp.example", "name": "Ana Lyst",
               "groups": ["Sem-Admins", "Okta-Only"], "groups_claim": "groups",
               "id_token_claims": ["sub", "email", "name", "groups"]}
GOOGLE_PERSON = {"provider": "google", "sub": "1093", "email": "ana@corp.example", "email_verified": True,
                 "hd": "corp.example", "name": "Ana Lyst", "refresh_token": True,
                 "bigquery": {"project": "p", "dry_run": "ok", "projects_visible": 2}}
LDAP_PERSON = {"provider": "ldap", "sAMAccountName": "alyst", "userPrincipalName": "alyst@corp.example",
               "mail": "ana@corp.example", "displayName": "Ana Lyst", "employeeID": "E123",
               "objectGUID": "{uuid}", "groups": ["Sem-Admins", "Domain Users"],
               "groups_nested": ["Sem-Admins", "Domain Users", "All-Staff"],
               "manager_entry": {"displayName": "Jane Doe", "mail": "jane@corp.example"},
               "title": "Analyst B35", "department": "Risk", "extension_attributes": {"extensionAttribute3": "Band 35"}}


def test_identity_map_joins_one_person_across_the_three():
    rows = identity_map.join(OKTA_PERSON, GOOGLE_PERSON, LDAP_PERSON)
    by = {r.topic: r for r in rows}
    assert by["email: okta = google"].status == "PASS"
    assert by["email: okta = ldap mail"].status == "PASS"
    assert by["email: okta = ldap upn"].status == "WARN"            # UPN is the sAMAccountName form
    assert by["ldap lookup key"].status == "PASS" and "mail" in by["ldap lookup key"].detail
    assert by["google: workspace"].status == "PASS"
    assert by["groups: okta in ldap"].status == "WARN" and "Okta-Only" in by["groups: okta in ldap"].detail
    assert by["stable keys"].status == "PASS" and "objectGUID" in by["stable keys"].detail
    assert "manager" in by["ldap adds"].detail
    recs = identity_map.recommend(OKTA_PERSON, GOOGLE_PERSON, LDAP_PERSON, rows)
    text = "\n".join(recs)
    assert "AUTH_EMAIL_CLAIMS=email" in text and "OKTA_GROUP_CLAIM=groups" in text
    assert "LDAP is optional for sign-in" in text and "manager attribute" in text


def test_identity_map_with_gaps_and_masked_values():
    okta_only = {**OKTA_PERSON, "groups": None, "email": "a***@corp.example"}
    rows = identity_map.join(okta_only, None, LDAP_PERSON)
    by = {r.topic: r for r in rows}
    assert by["google"].status == "SKIP"
    assert by["email: okta = ldap mail"].status == "INFO" and "masked" in by["email: okta = ldap mail"].detail
    assert by["groups: okta in ldap"].status == "WARN" and "LDAP lookup" in by["groups: okta in ldap"].detail
    recs = identity_map.recommend(okta_only, None, LDAP_PERSON, rows)
    assert any("Okta sends no groups" in r for r in recs)
    personal = {**GOOGLE_PERSON, "hd": None, "email": "ana@gmail.example", "refresh_token": False}
    rows = identity_map.join(OKTA_PERSON, personal, None)
    by = {r.topic: r for r in rows}
    assert by["email: okta = google"].status == "FAIL" and by["google: workspace"].status == "WARN"
    recs = identity_map.recommend(OKTA_PERSON, personal, None, rows)
    assert any("e-mails differ" in r for r in recs) and any("No refresh token" in r for r in recs)


def test_identity_map_reads_the_report_files(tmp_path):
    okta_file = tmp_path / "okta.json"
    okta_file.write_text(__import__("json").dumps({"ok": True, "checks": [], "facts": {"inventory": {"E1": OKTA_PERSON}}}))
    assert identity_map.pick(identity_map.load(str(okta_file)), "E1") == OKTA_PERSON
    assert identity_map.pick(identity_map.load(str(okta_file)), None) == OKTA_PERSON
    assert identity_map.load(str(tmp_path / "missing.json")) is None
    assert identity_map.main(["--okta", str(okta_file)]) == 0
    assert identity_map.main(["--okta", str(tmp_path / "missing.json")]) == 2


def test_reports_say_whether_a_person_was_inventoried(capsys):
    for mod in (okta, google):
        rep = mod.Report()
        rep.add("E1", "discovery", "PASS", "issuer x")
        dumped = rep.dump()
        assert dumped["inventory"] == [] and "nobody signed in" in dumped["note"]
        mod.print_report(rep, "t")
        assert "INVENTORY: not run" in capsys.readouterr().out
        rep.facts["inventory"] = {"E1": {"provider": "okta", "sub": "s"}}
        assert rep.dump()["inventory"] == ["E1"] and rep.dump()["note"] == "inventory for E1"
        mod.print_report(rep, "t")
        assert "real values" in capsys.readouterr().out
