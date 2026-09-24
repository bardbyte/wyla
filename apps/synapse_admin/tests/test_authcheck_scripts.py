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
