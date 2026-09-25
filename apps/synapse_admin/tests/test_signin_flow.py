"""Okta sign-in end to end, against the real routes on the sqlite identity
store: a status the sign-in page can draw from, the hop to Okta with
state, nonce and PKCE parked in the store, the callback that exchanges
the code, verifies the ID token, creates the person with roles from
their groups and sets the cookies; then the CSRF gate on a state-changing
call, sign-out, the replayed and refused callbacks, and the local
email-and-password path staying shut unless a deployment opens it."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from apps.synapse_admin.backend import auth, okta  # noqa: E402
from apps.synapse_admin.backend.app import create_app  # noqa: E402
from sahs.identity.oidc import OidcClient, OidcSettings  # noqa: E402
from test_oidc import CLIENT, ISSUER, FakeOkta  # noqa: E402


class SigningOkta(FakeOkta):
    """The fake from the harness tests, but the ID token it mints carries
    the nonce the app asked for and the groups the test says."""

    def __init__(self) -> None:
        super().__init__()
        self.nonce = ""
        self.groups: list[str] = ["Sem-Stewards"]
        self.email: str | None = "ana@example.com"

    def http(self, url, data, headers):
        if url == ISSUER + "/v1/token":
            form = {k: v[0] for k, v in parse_qs((data or b"").decode()).items()}
            self.calls.append((url, headers, form))
            if form.get("code") != "good-code":
                return 400, json.dumps({"error": "invalid_grant"}).encode()
            over = {"groups": self.groups}
            if self.email is None:
                over["_drop_email"] = True
            token = self.id_token(nonce=self.nonce, **over)
            return 200, json.dumps({"id_token": token, "access_token": "at",
                                    "token_type": "Bearer"}).encode()
        return super().http(url, data, headers)

    def id_token(self, *, nonce: str, **overrides) -> str:
        drop = overrides.pop("_drop_email", False)
        if drop:
            overrides["email"] = ""
        return super().id_token(nonce=nonce, **overrides)


@pytest.fixture()
def fake(monkeypatch, tmp_path) -> SigningOkta:
    monkeypatch.setenv("SAHS_STORE", "sqlite")
    monkeypatch.setenv("SAHS_IDENTITY_SQLITE", str(tmp_path / "identity.sqlite3"))
    monkeypatch.setenv("AUTH_PEPPER", "test-pepper")
    monkeypatch.setenv("AUTH_COOKIE_SECURE", "auto")
    monkeypatch.delenv("AUTH_LOCAL_LOGIN", raising=False)
    monkeypatch.setenv("OKTA_ISSUER", ISSUER)
    monkeypatch.setenv("OKTA_CLIENT_ID", CLIENT)
    monkeypatch.setenv("OKTA_CLIENT_SECRET", "sec")
    monkeypatch.setenv("OKTA_REDIRECT_URI", "http://testserver/callback")
    monkeypatch.setenv("AUTH_GROUP_ROLE_MAP", "Sem-Admins=admin, Sem-Stewards=steward, *=analyst")
    monkeypatch.setenv("MERIDIAN_BUILDS_DIR", str(tmp_path / "builds"))
    monkeypatch.setenv("MERIDIAN_GRAPH_DIR", str(tmp_path / "graph"))
    provider = SigningOkta()
    client = OidcClient(OidcSettings.from_env(), http=provider.http)
    auth._identity.cache_clear()
    auth._session_cache.clear()
    okta.reset_client()
    monkeypatch.setattr(okta, "_client", lambda: client)
    yield provider
    auth._identity.cache_clear()
    auth._session_cache.clear()


@pytest.fixture()
def client(fake) -> TestClient:
    return TestClient(create_app())


def _start(client: TestClient, fake: SigningOkta, next_path: str = "") -> str:
    """Take the hop to Okta and return the state; the fake learns the nonce."""
    response = client.get("/api/auth/okta/start", params={"next": next_path} if next_path else None,
                          follow_redirects=False)
    assert response.status_code == 307, response.text
    parts = urlsplit(response.headers["location"])
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == ISSUER + "/v1/authorize"
    q = {k: v[0] for k, v in parse_qs(parts.query).items()}
    assert q["client_id"] == CLIENT and q["redirect_uri"] == "http://testserver/callback"
    assert q["state"].startswith("okta.") and q["code_challenge_method"] == "S256"
    fake.nonce = q["nonce"]
    return q["state"]


def _sign_in(client: TestClient, fake: SigningOkta, next_path: str = ""):
    state = _start(client, fake, next_path)
    return client.get("/callback", params={"code": "good-code", "state": state},
                      follow_redirects=False)


def test_status_says_what_the_sign_in_page_needs(client):
    status = client.get("/api/auth/okta").json()
    assert status["configured"] is True and status["provider"] == "okta"
    assert status["issuer_host"] == "org.example"
    assert status["start"] == "/api/auth/okta/start"
    assert status["local_login"] is False
    assert client.get("/api/auth/me").status_code == 401


def test_the_whole_hop_creates_the_person_from_their_groups(client, fake):
    response = _sign_in(client, fake, "/#/skills")
    assert response.status_code == 303, response.text
    assert response.headers["location"] == "/#/skills"
    cookies = response.headers.get_list("set-cookie")
    session = next(c for c in cookies if c.startswith("synapse_session="))
    csrf = next(c for c in cookies if c.startswith("synapse_csrf="))
    assert "HttpOnly" in session and "SameSite=strict" in session
    assert "Secure" not in session and "Secure" not in csrf      # auto, over http: a laptop

    me = client.get("/api/auth/me")
    assert me.status_code == 200, me.text
    user = me.json()["user"]
    assert user["email"] == "ana@example.com" and user["name"] == "Ana Lyst"
    assert user["roles"] == ["analyst", "steward"]                # "*=analyst": everyone is at least an analyst
    assert user["surfaces"] == ["synapse"]
    assert "metrics.certify" in user["permissions"] and "users.manage" not in user["permissions"]

    store = auth._identity()
    audit = store._query("SELECT Action, Outcome, Details FROM AuditEvents ORDER BY OccurredAt")
    actions = [(a["Action"], a["Outcome"]) for a in audit]
    assert ("login.ok", "success") in actions
    ok = next(a for a in audit if a["Action"] == "login.ok")
    details = ok["Details"] if isinstance(ok["Details"], dict) else json.loads(ok["Details"])
    assert details["via"] == "okta" and details["groups"] == ["Sem-Stewards"]
    external = store._query("SELECT Provider, Subject FROM ExternalIdentities")
    assert [(e["Provider"], e["Subject"]) for e in external] == [("okta", "00u1")]


def test_roles_follow_the_groups_on_every_sign_in_and_admins_land_on_the_console(client, fake):
    assert _sign_in(client, fake).headers["location"] == "/synapse/"
    fake.groups = ["Sem-Admins", "Sem-Stewards"]
    response = _sign_in(client, fake)
    assert response.headers["location"] == "/"
    user = client.get("/api/auth/me").json()["user"]
    assert user["roles"] == ["admin", "analyst", "steward"]
    assert user["surfaces"] == ["admin", "synapse"]
    fake.groups = ["Something-Else"]
    _sign_in(client, fake)
    assert client.get("/api/auth/me").json()["user"]["roles"] == ["analyst"]
    users = auth._identity().list_users(10)
    assert len(users) == 1                                        # the same person, relinked


def test_state_changing_calls_need_the_csrf_header_and_sign_out_clears_it(client, fake):
    _sign_in(client, fake)
    csrf = client.cookies.get("synapse_csrf")
    assert csrf
    refused = client.post("/api/auth/logout")
    assert refused.status_code == 403 and "CSRF" in refused.text
    assert client.get("/api/auth/me").status_code == 200
    accepted = client.post("/api/auth/logout", headers={"x-csrf-token": csrf})
    assert accepted.status_code == 200, accepted.text
    assert client.get("/api/auth/me").status_code == 401


def test_a_state_is_good_once_and_a_bad_token_or_refusal_is_reported(client, fake):
    state = _start(client, fake)
    first = client.get("/callback", params={"code": "good-code", "state": state}, follow_redirects=False)
    assert first.status_code == 303
    replay = client.get("/callback", params={"code": "good-code", "state": state}, follow_redirects=False)
    assert replay.status_code == 400 and "expired or was already used" in replay.text

    state = _start(client, fake)
    fake.nonce = "not-the-one-we-sent"
    wrong = client.get("/callback", params={"code": "good-code", "state": state}, follow_redirects=False)
    assert wrong.status_code == 401 and "Okta sign-in failed" in wrong.text

    state = _start(client, fake)
    denied = client.get("/callback", params={"state": state, "error": "access_denied",
                                             "error_description": "User denied"},
                        follow_redirects=False)
    assert denied.status_code == 400 and "access_denied" in denied.text

    state = _start(client, fake)
    fake.email = None
    nameless = client.get("/callback", params={"code": "good-code", "state": state}, follow_redirects=False)
    assert nameless.status_code == 401 and "no email" in nameless.text

    unknown = client.get("/callback", params={"code": "x", "state": "okta.never-issued"}, follow_redirects=False)
    assert unknown.status_code == 400
    audit = auth._identity()._query("SELECT Action, Outcome FROM AuditEvents WHERE Action = 'login.failed'")
    assert len(audit) >= 2


def test_next_is_only_ever_a_path_on_this_site(client, fake):
    for evil in ("https://evil.example/", "//evil.example/x", "/a\\b"):
        response = _sign_in(client, fake, evil)
        assert response.status_code == 303
        assert response.headers["location"] == "/synapse/"
    assert _sign_in(client, fake, "/synapse/#/chat").headers["location"] == "/synapse/#/chat"


def test_a_state_that_is_not_ours_goes_to_the_google_connect_flow(client):
    response = client.get("/callback", params={"code": "c", "state": "g-123"}, follow_redirects=False)
    assert response.status_code == 400 and "Google OAuth state" in response.text


def test_local_login_stays_shut_unless_opened(client, fake, monkeypatch):
    body = {"email": "ana@example.com", "password": "passw0rd!y", "name": "Ana"}
    for path in ("/api/auth/signup", "/api/auth/login"):
        response = client.post(path, json=body)
        assert response.status_code == 403 and "sign in with Okta" in response.text
    monkeypatch.setenv("AUTH_LOCAL_LOGIN", "1")
    assert client.get("/api/auth/okta").json()["local_login"] is True
    signed_up = client.post("/api/auth/signup", json=body)
    assert signed_up.status_code in (200, 201), signed_up.text
    assert client.get("/api/auth/me").json()["user"]["email"] == "ana@example.com"


def test_the_session_cookie_is_secure_when_the_request_came_over_https(client, fake):
    state = _start(client, fake)
    response = client.get("/callback", params={"code": "good-code", "state": state},
                          headers={"x-forwarded-proto": "https"}, follow_redirects=False)
    assert response.status_code == 303
    session = next(c for c in response.headers.get_list("set-cookie") if c.startswith("synapse_session="))
    assert "Secure" in session


def test_without_a_store_the_sign_in_routes_say_so(fake, monkeypatch):
    monkeypatch.setenv("SAHS_STORE", "local")
    auth._identity.cache_clear()
    app_client = TestClient(create_app())
    assert app_client.get("/api/auth/okta").json()["configured"] is False
    start = app_client.get("/api/auth/okta/start", follow_redirects=False)
    assert start.status_code == 503 and "SAHS_STORE" in start.text
    # local mode: the console is a single developer, no cookie needed
    assert app_client.get("/api/auth/me").json()["user"]["user_id"] == "local"


def test_signin_states_expire(client, fake, monkeypatch):
    from datetime import timedelta
    import sahs.identity.store as store_module
    state = _start(client, fake)
    real = store_module.utcnow
    monkeypatch.setattr(store_module, "utcnow", lambda: real() + timedelta(seconds=okta.STATE_TTL_SECONDS + 5))
    stale = client.get("/callback", params={"code": "good-code", "state": state}, follow_redirects=False)
    assert stale.status_code == 400 and "expired" in stale.text
