"""The whole chain, end to end, on fakes: Okta sign-in, the Google
consent hop, the connection at rest, and a live BigQuery query that
runs AS THE PERSON through both lanes (Ask and chat), under both
stores (the sqlite stand-in and the Spanner code path on the SDK
double). Nothing here reaches a real Okta, Google, BigQuery or
Spanner: every hop is a fake injected through a seam the code has.

    Okta      FakeOkta (tests/test_oidc.py) through OidcClient(http=...)
    Google    one fake urlopen for the token endpoint (exchange and
              refresh), userinfo and tokeninfo, patched into
              backend.auth, sahs.util.google_auth.oauth and .token
    BigQuery  a fake opener on BQConnection: jobs (dry run) and
              jobs.query (rows), recording every Authorization header
    Spanner   FakeSpannerDatabase (tests/fake_spanner.py)

What is NOT provable here: the real consent screen, the scopes Google
actually grants, Google's refresh-token issuance rules, the enterprise
proxy path. docs/reports/auth-e2e.md says so, hop by hop.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
import urllib.error
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from cryptography.fernet import Fernet
from fastapi import Depends
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
FX = SILO / "tests" / "fixtures"
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from apps.synapse_admin.backend import ask, auth, chat, okta  # noqa: E402
from apps.synapse_admin.backend.app import create_app  # noqa: E402
from fake_spanner import FakeSpannerDatabase  # noqa: E402
from sahs.identity.oidc import OidcClient, OidcSettings  # noqa: E402
from test_oidc import CLIENT, ISSUER  # noqa: E402
from apps.synapse_admin.tests.test_ask_loop import ScriptedModel  # noqa: E402
from apps.synapse_admin.tests.test_signin_flow import SigningOkta  # noqa: E402

ANA = "ana@example.com"
BQ_SCOPE = "https://www.googleapis.com/auth/bigquery"
EMAIL_SCOPE = "https://www.googleapis.com/auth/userinfo.email"
GRANTED = f"openid email {EMAIL_SCOPE} {BQ_SCOPE}"
CHAT_SQL = ("SELECT country_cd, sum(trans_usd_am) AS spend "
            "FROM dw.gms_transaction GROUP BY country_cd")
_OKTA = ("OKTA_ISSUER", "OKTA_CLIENT_ID", "OKTA_CLIENT_SECRET", "OKTA_REDIRECT_URI",
         "AUTH_GROUP_ROLE_MAP")
_SPANNER = ("SPANNER_PROJECT_ID", "SPANNER_INSTANCE_ID", "SPANNER_DATABASE_ID",
            "SPANNER_EMULATOR_HOST", "SYNAPSE_SPANNER_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS")
_BQ = ("SYNAPSE_BQ_SA_KEY", "SAHS_SECRETS_DIR", "BQ_DATA_PROJECT", "SYNAPSE_BQ_DATA_PROJECT",
       "SAHS_LIVE_MAX_BYTES", "ASK_EXECUTE", "SAHS_ENV_FILE", "VERTEX_MODEL")


# ── the fakes ────────────────────────────────────────────────
class _Response:
    """What urlopen (and an opener's open) hands back: a context manager
    with read()."""

    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc) -> None:
        return None


def _http_error(url: str, status: int, payload: dict) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(url, status, "refused", {},
                                  io.BytesIO(json.dumps(payload).encode("utf-8")))


class FakeGoogle:
    """Google's OAuth surface: the token endpoint (a code exchange and a
    refresh), userinfo and tokeninfo. Every call is recorded; every
    access token it mints is remembered so the BigQuery fake can refuse
    any bearer that did not come from here."""

    def __init__(self) -> None:
        self.email = ANA
        self.subject = "google-sub-1"
        self.email_verified = True
        self.scope = GRANTED
        self.refresh_token: str | None = "rt-secret-1"
        self.good_code = "good-google-code"
        self.exchanges: list[dict] = []
        self.refreshes: list[dict] = []
        self.userinfo_calls: list[str] = []
        self.tokeninfo_calls: list[str] = []
        self.minted: list[str] = []
        self.tokeninfo_scope = GRANTED
        self.tokeninfo_email = ANA

    def _mint(self, kind: str) -> str:
        token = f"ya29.{kind}-{len(self.minted) + 1}"
        self.minted.append(token)
        return token

    def urlopen(self, request, timeout=None):
        url = request.full_url
        form = {k: v[0] for k, v in parse_qs((request.data or b"").decode()).items()}
        if url == auth._GOOGLE_TOKEN_ENDPOINT:
            assert request.get_header("Content-type") == "application/x-www-form-urlencoded"
            if form.get("grant_type") == "authorization_code":
                self.exchanges.append(form)
                if form.get("code") != self.good_code:
                    raise _http_error(url, 400, {"error": "invalid_grant"})
                payload = {"access_token": self._mint("exchange"), "expires_in": 3599,
                           "scope": self.scope, "token_type": "Bearer"}
                if self.refresh_token:
                    payload["refresh_token"] = self.refresh_token
                return _Response(payload)
            if form.get("grant_type") == "refresh_token":
                self.refreshes.append(form)
                if form.get("refresh_token") != self.refresh_token:
                    raise _http_error(url, 400, {"error": "invalid_grant"})
                return _Response({"access_token": self._mint("refreshed"),
                                  "expires_in": 3600, "scope": self.scope,
                                  "token_type": "Bearer"})
            raise _http_error(url, 400, {"error": "unsupported_grant_type"})
        if url == auth._GOOGLE_USERINFO_ENDPOINT:
            bearer = (request.get_header("Authorization") or "")[7:]
            self.userinfo_calls.append(bearer)
            if bearer not in self.minted:
                raise _http_error(url, 401, {"error": "invalid_token"})
            return _Response({"sub": self.subject, "email": self.email,
                              "email_verified": self.email_verified})
        if url == "https://oauth2.googleapis.com/tokeninfo":
            self.tokeninfo_calls.append(form.get("access_token", ""))
            return _Response({"email": self.tokeninfo_email, "email_verified": True,
                              "scope": self.tokeninfo_scope, "expires_in": "3000"})
        raise AssertionError(f"unexpected Google call: {url}")


class FakeBigQuery:
    """BigQuery's REST surface behind BQConnection.opener(): jobs (a dry
    run) and jobs.query (rows). Refuses any bearer Google did not mint."""

    def __init__(self, google: FakeGoogle) -> None:
        self.google = google
        self.calls: list[dict] = []
        self.bytes = 4321

    def open(self, request, timeout=None):
        url = request.full_url
        body = json.loads(request.data.decode("utf-8"))
        bearer = (request.get_header("Authorization") or "")[7:]
        self.calls.append({"url": url, "bearer": bearer, "body": body})
        if bearer not in self.google.minted:
            raise _http_error(url, 401, {"error": {"message": "Invalid Credentials"}})
        if url.endswith("/jobs") and body.get("configuration", {}).get("dryRun"):
            return _Response({"statistics": {"query": {
                "totalBytesProcessed": str(self.bytes),
                "schema": {"fields": [{"name": "country_cd", "type": "STRING"},
                                      {"name": "spend", "type": "FLOAT"}]}}}})
        if url.endswith("/queries"):
            return _Response({"jobComplete": True,
                              "schema": {"fields": [{"name": "country_cd", "type": "STRING"},
                                                    {"name": "spend", "type": "FLOAT"}]},
                              "rows": [{"f": [{"v": "CA"}, {"v": "7.0"}]},
                                       {"f": [{"v": "US"}, {"v": "9.0"}]}],
                              "totalBytesProcessed": str(self.bytes)})
        raise AssertionError(f"unexpected BigQuery call: {url}")

    def dry_runs(self) -> list[dict]:
        return [c for c in self.calls if c["url"].endswith("/jobs")]

    def queries(self) -> list[dict]:
        return [c for c in self.calls if c["url"].endswith("/queries")]


def _never(*args, **kwargs):
    raise AssertionError("a real network call was attempted")


# ── the world ────────────────────────────────────────────────
@pytest.fixture(scope="module")
def compiled(tmp_path_factory) -> dict:
    """One compiled fixture build: the ACL the sandbox consults."""
    tmp = tmp_path_factory.mktemp("auth_e2e")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "auth_e2e"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    builds = tmp / "builds"
    _dir, _manifest, failures = compile_build(graph_dir, builds)
    assert not failures
    return {"builds": builds, "graph": graph_dir}


def _reset(okta_client: bool = True) -> None:
    auth._identity.cache_clear()
    auth._session_cache.clear()
    with auth._google_providers_lock:
        auth._google_providers.clear()
    if okta_client:
        okta.reset_client()
    chat._RUNTIME = None
    chat._RUNTIMES.clear()
    ask._RUNTIME = None
    ask._RUNTIMES.clear()


class World:
    def __init__(self, store: str, okta_fake: SigningOkta, google: FakeGoogle,
                 bigquery: FakeBigQuery, spanner: FakeSpannerDatabase | None,
                 key: str) -> None:
        self.store = store
        self.okta = okta_fake
        self.google = google
        self.bigquery = bigquery
        self.spanner = spanner
        self.key = key

    def rows(self, table: str, where: str = "", params: dict | None = None) -> list[dict]:
        """The table's rows, from whichever store runs."""
        if self.spanner is not None:
            return self.spanner.rows(table, where, params)
        return auth._identity()._query(
            f"SELECT * FROM {table}" + (f" WHERE {where}" if where else ""), params)


@pytest.fixture(params=["sqlite", "spanner"])
def world(request, monkeypatch, tmp_path, compiled) -> World:
    """A deployment with Okta in front, BigQuery as the person, the
    store the parameter names, and every outside host a fake."""
    for name in _OKTA + _SPANNER + _BQ + ("AUTH_LOCAL_LOGIN", "SYNAPSE_USER_NAME"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SAHS_STORE", request.param)
    monkeypatch.setenv("SAHS_IDENTITY_SQLITE", str(tmp_path / "identity.sqlite3"))
    monkeypatch.setenv("AUTH_PEPPER", "test-pepper")
    monkeypatch.setenv("AUTH_COOKIE_SECURE", "auto")
    monkeypatch.setenv("OKTA_ISSUER", ISSUER)
    monkeypatch.setenv("OKTA_CLIENT_ID", CLIENT)
    monkeypatch.setenv("OKTA_CLIENT_SECRET", "sec")
    monkeypatch.setenv("OKTA_REDIRECT_URI", "http://testserver/callback")
    monkeypatch.setenv("AUTH_GROUP_ROLE_MAP", "Sem-Admins=admin, Sem-Stewards=steward, *=analyst")
    monkeypatch.setenv("MERIDIAN_BUILDS_DIR", str(compiled["builds"]))
    monkeypatch.setenv("MERIDIAN_GRAPH_DIR", str(compiled["graph"]))
    # BigQuery as the person
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("SAHS_BQ_AUTH_MODE", "user")
    monkeypatch.setenv("GOOGLE_OAUTH_CLIENT_ID", "google-client-id")
    monkeypatch.setenv("GOOGLE_OAUTH_CLIENT_SECRET", "google-client-secret")
    monkeypatch.setenv("GOOGLE_OAUTH_REDIRECT_URI", "http://testserver/callback")
    monkeypatch.setenv("GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY", key)
    monkeypatch.setenv("BQ_PROJECT_ID", "test-project")
    monkeypatch.setenv("BIGQUERY_API_BASE_URL", "https://bigquery.test")
    monkeypatch.setenv("BQ_LOCATION", "US")
    monkeypatch.setenv("SAHS_ALLOW_LIVE", "1")

    spanner = None
    if request.param == "spanner":
        from sahs.identity import database as database_module
        spanner = FakeSpannerDatabase(tmp_path / "fake-spanner.sqlite3")
        monkeypatch.setenv("SPANNER_PROJECT_ID", "test-project")
        monkeypatch.setenv("SPANNER_INSTANCE_ID", "test-instance")
        monkeypatch.setenv("SPANNER_DATABASE_ID", "test-database")
        monkeypatch.setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
        monkeypatch.setattr(database_module.SpannerDatabase, "from_settings",
                            classmethod(lambda cls, settings: cls(spanner)))

    # Okta: the harness fake behind the OIDC client's http seam
    okta_fake = SigningOkta()
    client = OidcClient(OidcSettings.from_env(), http=okta_fake.http)
    _reset()
    monkeypatch.setattr(okta, "_client", lambda: client)

    # Google and BigQuery: one fake urlopen, one fake opener; the real
    # urlopen behind the app's Google calls is a trap
    google = FakeGoogle()
    bigquery = FakeBigQuery(google)
    from sahs.util import auth as harness_auth
    from sahs.util.google_auth import oauth as oauth_module
    from sahs.util.google_auth import token as token_module
    monkeypatch.setattr(auth, "urlopen", google.urlopen)
    monkeypatch.setattr(oauth_module, "urlopen", google.urlopen)
    monkeypatch.setattr(token_module, "urlopen", google.urlopen)
    monkeypatch.setattr(harness_auth.BQConnection, "opener", lambda self: bigquery)
    monkeypatch.setattr(harness_auth, "plane_opener", _never)
    monkeypatch.setattr("urllib.request.urlopen", _never)
    yield World(request.param, okta_fake, google, bigquery, spanner, key)
    _reset(okta_client=False)          # the patched client is a lambda until monkeypatch undoes it


@pytest.fixture()
def client(world) -> TestClient:
    return TestClient(create_app())


def _csrf(client: TestClient) -> dict[str, str]:
    return {"x-csrf-token": client.cookies.get("synapse_csrf") or ""}


def _test_route(app, path: str, endpoint) -> None:
    """A route for a test, ahead of the frontend's catch-all mount."""
    app.add_api_route(path, endpoint, methods=["GET"])
    app.router.routes.insert(0, app.router.routes.pop())


def _sign_in(client: TestClient, world: World, next_path: str = "") -> dict:
    """The Okta hop, start to cookie; returns the person."""
    hop = client.get("/api/auth/okta/start", params={"next": next_path} if next_path else None,
                     follow_redirects=False)
    assert hop.status_code == 307, hop.text
    parts = urlsplit(hop.headers["location"])
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == ISSUER + "/v1/authorize"
    q = {k: v[0] for k, v in parse_qs(parts.query).items()}
    assert q["state"].startswith("okta.")
    world.okta.nonce = q["nonce"]
    back = client.get("/callback", params={"code": "good-code", "state": q["state"]},
                      follow_redirects=False)
    assert back.status_code == 303, back.text
    assert "signin" not in back.headers["location"]
    cookies = back.headers.get_list("set-cookie")
    assert any(c.startswith("synapse_session=") and "HttpOnly" in c for c in cookies)
    assert any(c.startswith("synapse_csrf=") for c in cookies)
    me = client.get("/api/auth/me")
    assert me.status_code == 200, me.text
    return me.json()["user"]


def _start_google(client: TestClient, world: World, popup: bool = True) -> tuple[str, dict]:
    hop = client.get("/api/auth/google/start", params={"popup": "1"} if popup else None,
                     follow_redirects=False)
    assert hop.status_code == 307, hop.text
    parts = urlsplit(hop.headers["location"])
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == auth._GOOGLE_AUTHORIZATION_ENDPOINT
    q = {k: v[0] for k, v in parse_qs(parts.query).items()}
    return q["state"], q


def _connect(client: TestClient, world: World, popup: bool = True):
    state, q = _start_google(client, world, popup)
    return client.get("/callback", params={"code": world.google.good_code, "state": state},
                      follow_redirects=False), q


def _connection_row(world: World, user_id: str) -> dict | None:
    rows = world.rows("GoogleOAuthConnections", "UserId = @u AND RevokedAt IS NULL", {"u": user_id})
    return rows[0] if rows else None


# ── 1. the chain: Okta, then Google, the row at rest, disconnect ──
def test_okta_sign_in_then_google_connect_end_to_end(client, world):
    ana = _sign_in(client, world, "/synapse/#/chat")
    assert ana["email"] == ANA and ana["roles"] == ["analyst", "steward"]
    assert world.rows("ExternalIdentities")[0]["Subject"] == "00u1"

    # not connected yet, and the workspace says a connection is required
    before = client.get("/api/auth/google/connection").json()
    assert before == {"available": True, "connected": False, "requires_user_oauth": True,
                      "provider": "google", "email": ""}

    # the consent hop: PKCE, offline access, forced consent, state parked
    state, q = _start_google(client, world)
    assert q["client_id"] == "google-client-id"
    assert q["redirect_uri"] == "http://testserver/callback"
    assert q["response_type"] == "code" and q["code_challenge_method"] == "S256"
    assert q["access_type"] == "offline" and q["prompt"] == "consent"
    assert set(q["scope"].split()) == {"openid", "email", EMAIL_SCOPE, BQ_SCOPE}
    parked = world.rows("AuthStates")
    assert len(parked) == 1 and parked[0]["Kind"] == "google_connect"
    assert parked[0]["State"] == state and parked[0]["UserId"] == ana["user_id"]

    # the callback: exchange, userinfo, the row, the popup page
    done = client.get("/callback", params={"code": world.google.good_code, "state": state},
                      follow_redirects=False)
    assert done.status_code == 200, done.text
    assert done.headers["content-type"].startswith("text/html")
    assert "google-connected" in done.text and "window.close()" in done.text
    assert world.rows("AuthStates") == []                                   # consumed
    exchange = world.google.exchanges[-1]
    assert exchange["code"] == world.google.good_code
    assert exchange["grant_type"] == "authorization_code"
    assert exchange["redirect_uri"] == "http://testserver/callback"
    assert exchange["client_id"] == "google-client-id"
    assert exchange["client_secret"] == "google-client-secret"
    assert auth._pkce_challenge(exchange["code_verifier"]) == q["code_challenge"]
    assert world.google.userinfo_calls == [world.google.minted[0]]

    row = _connection_row(world, ana["user_id"])
    assert row is not None
    assert row["GoogleEmail"] == ANA and row["GoogleSubject"] == "google-sub-1"
    cipher = row["RefreshTokenCiphertext"]
    cipher = cipher.encode() if isinstance(cipher, str) else bytes(cipher)
    assert b"rt-secret-1" not in cipher
    assert Fernet(world.key).decrypt(cipher) == b"rt-secret-1"
    scopes = row["Scopes"]
    scopes = json.loads(scopes) if isinstance(scopes, str) else list(scopes)
    assert BQ_SCOPE in scopes and EMAIL_SCOPE in scopes
    connected = client.get("/api/auth/google/connection").json()
    assert connected["connected"] is True and connected["email"] == ANA
    assert connected["requires_user_oauth"] is True

    # a state is good once
    replay = client.get("/callback", params={"code": world.google.good_code, "state": state},
                        follow_redirects=False)
    assert replay.status_code == 400 and "invalid or expired" in replay.text

    # the non-popup path lands the analyst on the chat
    landed, _ = _connect(client, world, popup=False)
    assert landed.status_code == 303
    assert landed.headers["location"] == "/synapse/#/chat/new?google=connected"

    # disconnect revokes the row (kept, with RevokedAt) and reads as not connected
    gone = client.delete("/api/auth/google/connection", headers=_csrf(client))
    assert gone.status_code == 200 and gone.json()["connected"] is False
    assert _connection_row(world, ana["user_id"]) is None
    revoked = world.rows("GoogleOAuthConnections", "UserId = @u", {"u": ana["user_id"]})
    assert len(revoked) == 1 and revoked[0]["RevokedAt"] is not None
    assert client.get("/api/auth/google/connection").json()["connected"] is False
    assert auth._identity().google_connection(ana["user_id"]) is None


def test_the_callback_refuses_what_it_must(client, world):
    ana = _sign_in(client, world)

    # Google's account is not the Okta person's
    world.google.email = "someone.else@example.com"
    refused, _ = _connect(client, world)
    assert refused.status_code == 403 and "does not match" in refused.text
    assert _connection_row(world, ana["user_id"]) is None
    world.google.email = ANA

    # an unverified Google email
    world.google.email_verified = False
    refused, _ = _connect(client, world)
    assert refused.status_code == 401 and "not verified" in refused.text
    world.google.email_verified = True

    # the BigQuery scope was not granted
    world.google.scope = f"openid email {EMAIL_SCOPE}"
    refused, _ = _connect(client, world)
    assert refused.status_code == 400 and BQ_SCOPE in refused.text
    assert _connection_row(world, ana["user_id"]) is None
    world.google.scope = GRANTED

    # no refresh token came back (a consent Google considered already given)
    world.google.refresh_token = None
    refused, _ = _connect(client, world)
    assert refused.status_code == 400 and "refresh token" in refused.text
    assert _connection_row(world, ana["user_id"]) is None
    world.google.refresh_token = "rt-secret-1"

    # Google refused the consent
    state, _ = _start_google(client, world)
    denied = client.get("/callback", params={"state": state, "error": "access_denied"},
                        follow_redirects=False)
    assert denied.status_code == 400 and "access_denied" in denied.text

    # a bad code: Google's 400 is our 502, and the row stays absent
    state, _ = _start_google(client, world)
    bad = client.get("/callback", params={"code": "not-the-code", "state": state},
                     follow_redirects=False)
    assert bad.status_code == 502 and _connection_row(world, ana["user_id"]) is None

    # the callback without the session, or with another person's session
    state, _ = _start_google(client, world)
    stranger = TestClient(client.app)
    anon = stranger.get("/callback", params={"code": world.google.good_code, "state": state},
                        follow_redirects=False)
    assert anon.status_code == 401
    state, _ = _start_google(client, world)
    world.okta.email, world.okta.subject = "bo@example.com", "00u2"
    _sign_in(stranger, world)
    other = stranger.get("/callback", params={"code": world.google.good_code, "state": state},
                         follow_redirects=False)
    assert other.status_code == 401 and "does not match OAuth request" in other.text
    world.okta.email, world.okta.subject = ANA, "00u1"
    assert world.rows("GoogleOAuthConnections") == []

    # and the happy path still works after all that
    done, _ = _connect(client, world)
    assert done.status_code == 200 and _connection_row(world, ana["user_id"]) is not None


# ── 2. the user-delegated query ──────────────────────────────
def _events(client: TestClient, url: str) -> list[dict]:
    body = client.get(url, params={"once": "1"}).text
    return [json.loads(line[6:]) for line in body.splitlines() if line.startswith("data: ")]


def _bearers(world: World) -> set[str]:
    return {c["bearer"] for c in world.bigquery.calls}


def _ask_turn(client: TestClient, runtime, sid: str, text: str, csrf: dict) -> list[dict]:
    """One Ask turn to its end, read off the session's bus (the SSE
    stream closes on the first turn_done under once=1): when the
    resolver stops on a question, the first chip answers it. Returns
    the events of the last turn."""
    bus = runtime.runtime(sid).bus

    def turn(body: dict) -> list[dict]:
        before = bus.head()
        accepted = client.post(f"/api/sessions/{sid}/messages", json=body, headers=csrf)
        assert accepted.status_code == 202 and accepted.json()["available"], accepted.text
        assert runtime.wait(sid, 30.0), "the turn never finished"
        return bus.since(before)

    events = turn({"text": text})
    clarify = next((e for e in events if e["ev"] == "clarify_request"), None)
    if clarify is not None:
        option = clarify["options"][0]
        events = turn({"text": option.get("label") or "that one",
                       "choice": {"slot": clarify["slot"], "value": option["value"],
                                  "label": option.get("label", "")}})
    return events


def test_the_ask_lane_runs_the_query_as_the_person(client, world, monkeypatch):
    """/api/sessions/{id}/messages with the scripted model proposing a
    query: the runner the app builds mints an access token from the
    stored refresh token, the sandbox dry-runs and then runs through
    it, and BigQuery sees the person's bearer, never a service account."""
    from sahs.ask.runtime import AskRuntime
    model = ScriptedModel()
    monkeypatch.setattr(AskRuntime, "model_for", lambda self, budget: model)
    ana = _sign_in(client, world)
    done, _ = _connect(client, world)
    assert done.status_code == 200
    csrf = _csrf(client)

    made = client.post("/api/sessions", json={"kind": "analyst"}, headers=csrf)
    assert made.status_code == 201, made.text
    sid = made.json()["session"]["id"]
    runtime = ask._RUNTIMES[ana["user_id"]]
    assert runtime.runner is not None and runtime.runner.token_provider is not None
    events = _ask_turn(client, runtime, sid, "acquirer net spend by day", csrf)
    assert events[-1]["ev"] == "turn_done" and events[-1]["status"] == "answered", events

    # the token: minted by a refresh with the stored refresh token
    answer = next(e for e in events if e["ev"] == "answer_payload")["payload"]
    assert world.google.refreshes, ("no access token was minted; the answer says: "
                                    + "; ".join(answer.get("limits", [])))
    refresh = world.google.refreshes[0]
    assert refresh["refresh_token"] == "rt-secret-1"
    assert refresh["grant_type"] == "refresh_token"
    assert refresh["client_id"] == "google-client-id"
    assert refresh["client_secret"] == "google-client-secret"
    minted = {t for t in world.google.minted if t.startswith("ya29.refreshed")}
    # BigQuery: a dry run first, then jobs.query, both as the person
    dry, ran = world.bigquery.dry_runs(), world.bigquery.queries()
    assert dry and ran, world.bigquery.calls
    assert world.bigquery.calls[0]["url"].endswith("/jobs")            # the dry run came first
    assert _bearers(world) <= minted and _bearers(world)
    assert "ya29.exchange-1" not in _bearers(world)                    # the consent token is never reused
    assert all(c["url"].startswith("https://bigquery.test/bigquery/v2/projects/test-project/")
               for c in world.bigquery.calls)
    assert ran[0]["body"]["useLegacySql"] is False and ran[0]["body"]["maxResults"] >= 1
    assert "gms_transaction" in ran[0]["body"]["query"]
    assert "LIMIT" in ran[0]["body"]["query"].upper()                  # the row cap rides
    # the answer carries rows, not a dry-run disclaimer
    answer = next(e for e in events if e["ev"] == "answer_payload")["payload"]
    assert answer["rows"] == [["CA", "7.0"], ["US", "9.0"]]
    assert not any("not executed" in line for line in answer.get("limits", []))
    verdict = next(e for e in events if e["ev"] == "verify_verdict")
    executes = next(c for c in verdict["will_verify"] if c["id"] == "executes")
    assert executes["passed"] is True, verdict

    # one provider per person: a second message (a new request-scoped
    # runner) reuses the cached access token, no second refresh trip
    assert len(world.google.refreshes) == 1
    world.bigquery.calls.clear()
    events = _ask_turn(client, runtime, sid, "acquirer net spend by day", csrf)
    assert events[-1]["status"] == "answered", events[-1]
    assert world.bigquery.calls, "the second turn ran nothing"
    assert len(world.google.refreshes) == 1

    # disconnected: refused by name, with the hint, and no BigQuery call
    assert client.delete("/api/auth/google/connection", headers=csrf).status_code == 200
    world.bigquery.calls.clear()
    refreshes = len(world.google.refreshes)
    events = _ask_turn(client, runtime, sid, "acquirer net spend by day", csrf)
    answer = next(e for e in events if e["ev"] == "answer_payload")["payload"]
    assert answer["rows"] is None
    assert any("google_oauth_required" in line for line in answer["limits"]), answer["limits"]
    assert world.bigquery.calls == []
    assert len(world.google.refreshes) == refreshes                     # nothing to refresh with

    # a person who never connected: the same refusal
    bo = TestClient(client.app)
    world.okta.email, world.okta.subject = "bo@example.com", "00u2"
    _sign_in(bo, world)
    world.okta.email, world.okta.subject = ANA, "00u1"
    sid_bo = bo.post("/api/sessions", json={"kind": "analyst"}, headers=_csrf(bo)).json()["session"]["id"]
    bo_runtime = next(r for uid, r in ask._RUNTIMES.items() if uid != ana["user_id"])
    events = _ask_turn(bo, bo_runtime, sid_bo, "acquirer net spend by day", _csrf(bo))
    answer = next(e for e in events if e["ev"] == "answer_payload")["payload"]
    assert any("google_oauth_required" in line for line in answer["limits"]), answer["limits"]
    assert world.bigquery.calls == []


def _chat_run(client: TestClient, world: World, ana: dict, csrf: dict,
              sql: str = CHAT_SQL) -> tuple[list[dict], dict]:
    """A proposed query pressed Run on the chat: no model call, the
    sandbox and the runner do the work. Returns (events, the last
    assistant message)."""
    made = client.post("/api/chat/sessions", headers=csrf)
    assert made.status_code == 201, made.text
    sid = made.json()["session"]["id"]
    runtime = chat._RUNTIMES[ana["user_id"]]
    runtime.store.add_message(sid, "assistant", "Here is the query.", turn_id="t0",
                              payload={"proposal": {"title": "Spend by country", "sql": sql,
                                                    "status": "composed",
                                                    "meridian_line": "Composed from certified parts."}})
    accepted = client.post(f"/api/chat/sessions/{sid}/run", json={"sql": sql, "limit": 50},
                           headers=csrf)
    assert accepted.status_code == 202, accepted.text
    assert accepted.json()["available"] is True, accepted.json()
    assert runtime.wait(sid, 30.0), "the run never finished"
    events = _events(client, f"/api/chat/sessions/{sid}/stream")
    shown = client.get(f"/api/chat/sessions/{sid}").json()
    return events, shown["messages"][-1]


def test_the_chat_lane_runs_the_query_as_the_person(client, world):
    """The chat runtime must carry the person's runner too: Run on a
    proposed query dry-runs and runs as the person; disconnected, it
    refuses by name with the hint."""
    ana = _sign_in(client, world)
    done, _ = _connect(client, world)
    assert done.status_code == 200
    csrf = _csrf(client)
    runtime_made = client.post("/api/chat/sessions", headers=csrf)
    assert runtime_made.status_code == 201
    runtime = chat._RUNTIMES[ana["user_id"]]
    assert runtime.runner is not None, "the chat runtime has no per-person runner"
    assert runtime.runner.token_provider is not None

    events, last = _chat_run(client, world, ana, csrf)
    assert events[-1]["ev"] == "turn_done" and events[-1]["status"] == "answered", events[-1]
    result = next(e for e in events if e["ev"] == "tool_result")
    content = json.loads(result["content"])
    assert content["mode"] == "run" and content["row_count"] == 2, content
    assert content["rows"] == [{"country_cd": "CA", "spend": "7.0"},
                               {"country_cd": "US", "spend": "9.0"}]
    dry, ran = world.bigquery.dry_runs(), world.bigquery.queries()
    assert dry and ran and world.bigquery.calls[0]["url"].endswith("/jobs")
    minted = {t for t in world.google.minted if t.startswith("ya29.refreshed")}
    assert _bearers(world) <= minted and _bearers(world)
    assert "LIMIT 50" in ran[0]["body"]["query"]
    assert world.google.refreshes[0]["refresh_token"] == "rt-secret-1"
    assert "2 rows" in last["text"] and last["payload"]["artifacts"]

    # the cost gate stands between the dry run and the run
    world.bigquery.calls.clear()
    world.bigquery.bytes = 10 ** 12
    events, last = _chat_run(client, world, ana, csrf)
    content = json.loads(next(e for e in events if e["ev"] == "tool_result")["content"])
    assert "cost_gate_budget" in content["error"] and content["kind"] == "cost"
    assert world.bigquery.dry_runs() and not world.bigquery.queries()
    world.bigquery.bytes = 4321

    # disconnected: google_oauth_required, the hint, nothing sent to BigQuery.
    # The ask lane has built providers of its own for this person by now;
    # the chat runtime's cached access token must not outlive the connection
    assert auth._google_runner(ana["user_id"]) is not None
    assert client.delete("/api/auth/google/connection", headers=csrf).status_code == 200
    world.bigquery.calls.clear()
    refreshes = len(world.google.refreshes)
    events, last = _chat_run(client, world, ana, csrf)
    assert len(world.google.refreshes) == refreshes                     # nothing to refresh with
    content = json.loads(next(e for e in events if e["ev"] == "tool_result")["content"])
    assert content["error"].startswith("google_oauth_required"), content
    assert content["kind"] == "access" and content["yours_to_fix"] is False
    assert "connect Google BigQuery" in content["hint"]
    assert "configuration, not the query" in last["text"] and "connect Google" in last["text"]
    assert world.bigquery.calls == []


def test_a_workspace_without_a_bigquery_project_still_opens_the_lanes(client, world, monkeypatch):
    """SAHS_BQ_AUTH_MODE=user with no BQ project: the runner cannot be
    built, and that must be a logged refusal at query time, never a 500
    on opening a session."""
    monkeypatch.delenv("BQ_PROJECT_ID")
    ana = _sign_in(client, world)
    csrf = _csrf(client)
    assert client.post("/api/sessions", json={"kind": "analyst"}, headers=csrf).status_code == 201
    assert client.post("/api/chat/sessions", headers=csrf).status_code == 201
    assert ask._RUNTIMES[ana["user_id"]].runner is None
    assert chat._RUNTIMES[ana["user_id"]].runner is None
    assert auth._google_runner(ana["user_id"]) is None


# ── 3. the Bearer path: a Google access token on a cookie session ──
def test_the_bearer_path_validates_the_google_token_for_the_cookie_user(client, world):
    app = client.app

    def bq(authz: auth.GoogleBigQueryAuthorization = Depends(auth.google_bigquery_user)) -> dict:
        return {"email": authz.user["email"], "token": authz.access_token}

    def bq_if_live(authz=Depends(auth.google_bigquery_user_if_live)) -> dict:
        return {"required": authz is not None}

    _test_route(app, "/test/bq", bq)
    _test_route(app, "/test/bq-if-live", bq_if_live)

    ana = _sign_in(client, world)
    world.google.minted.append("ya29.from-the-browser")

    # no Google token: refused with the connect hint
    missing = client.get("/test/bq")
    assert missing.status_code == 401 and "connect Google" in missing.text

    # the cookie names the person, the Bearer is the Google token
    ok = client.get("/test/bq", headers={"Authorization": "Bearer ya29.from-the-browser"})
    assert ok.status_code == 200, ok.text
    assert ok.json() == {"email": ANA, "token": "ya29.from-the-browser"}
    assert world.google.tokeninfo_calls == ["ya29.from-the-browser"]
    # and the cookie person is still who /me says
    assert client.get("/api/auth/me", headers={"Authorization": "Bearer ya29.from-the-browser"}
                      ).json()["user"]["user_id"] == ana["user_id"]

    # the token belongs to someone else
    world.google.tokeninfo_email = "someone.else@example.com"
    other = client.get("/test/bq", headers={"Authorization": "Bearer ya29.from-the-browser"})
    assert other.status_code == 403 and "does not match" in other.text
    world.google.tokeninfo_email = ANA

    # the token lacks the BigQuery scope
    world.google.tokeninfo_scope = f"openid {EMAIL_SCOPE}"
    scoped = client.get("/test/bq", headers={"Authorization": "Bearer ya29.from-the-browser"})
    assert scoped.status_code == 401 and "missing required scopes" in scoped.text
    world.google.tokeninfo_scope = GRANTED

    # a Bearer that is a session token still signs in on its own
    token = client.cookies.get("synapse_session")
    bare = TestClient(app)
    assert bare.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"}).status_code == 200
    # a Bearer that is nothing we know, with no cookie, is nobody
    assert bare.get("/api/auth/me", headers={"Authorization": "Bearer ya29.from-the-browser"}
                    ).status_code == 401

    # the request-scoped runner runs BigQuery with exactly that bearer
    runner = auth._google_request_runner("ya29.from-the-browser")
    got = runner.run("SELECT country_cd FROM dw.gms_transaction", 10)
    assert got["rows"] == [["CA", "7.0"], ["US", "9.0"]] and got["bytes_processed"] == 4321
    assert world.bigquery.queries()[-1]["bearer"] == "ya29.from-the-browser"

    # the soft dependency asks only when live runs are on
    assert client.get("/test/bq-if-live", headers={"Authorization": "Bearer ya29.from-the-browser"}
                      ).json() == {"required": True}


def test_the_soft_bearer_dependency_is_silent_when_live_is_off(client, world, monkeypatch):
    app = client.app

    def bq_if_live(authz=Depends(auth.google_bigquery_user_if_live)) -> dict:
        return {"required": authz is not None}

    _test_route(app, "/test/bq-if-live", bq_if_live)
    _sign_in(client, world)
    monkeypatch.delenv("SAHS_ALLOW_LIVE")
    assert client.get("/test/bq-if-live").json() == {"required": False}
    monkeypatch.setenv("SAHS_ALLOW_LIVE", "1")
    monkeypatch.setenv("ASK_EXECUTE", "snapshot")
    assert client.get("/test/bq-if-live").json() == {"required": False}


# ── 4. the one shared callback ───────────────────────────────
def test_the_one_callback_dispatches_on_the_state_prefix(client, world):
    # an Okta state the store never issued: the sign-in page, in words
    okta_unknown = client.get("/callback", params={"code": "c", "state": "okta.never"},
                              follow_redirects=False)
    assert okta_unknown.status_code == 303 and "#/signin?error=" in okta_unknown.headers["location"]
    # any other state is Google's, and an unknown one says so
    google_unknown = client.get("/callback", params={"code": "c", "state": "not-ours"},
                                follow_redirects=False)
    assert google_unknown.status_code == 400 and "Google OAuth state" in google_unknown.text
    assert world.google.exchanges == []
    # no state at all is Google's complaint too
    bare = client.get("/callback", params={"code": "c"}, follow_redirects=False)
    assert bare.status_code == 400 and "Google callback" in bare.text
    # a real Okta state signs in; a real Google state connects
    ana = _sign_in(client, world)
    done, _ = _connect(client, world)
    assert done.status_code == 200 and _connection_row(world, ana["user_id"]) is not None
    # the same handler under the API prefix
    state, _ = _start_google(client, world)
    again = client.get("/api/auth/okta/callback",
                       params={"code": world.google.good_code, "state": state},
                       follow_redirects=False)
    assert again.status_code == 200 and "google-connected" in again.text
