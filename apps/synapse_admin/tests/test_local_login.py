"""The laptop's front door and where its rows land.

AUTH_LOCAL_LOGIN=1 with no OKTA_* opens the email-and-password form:
sign-up, sign-out, sign-in set the session cookie and /api/whoami (and
/api/auth/me) name the person. OKTA_* with the flag unset keeps the
local routes shut and reports Okta. And under SAHS_STORE=spanner every
write the app makes — the person, the session, the audit, the chats,
messages, projects, memories, artifacts — goes through
``SpannerDatabase`` into the Spanner tables: proven here on the fake
SDK database from the harness tests, never on a real Spanner."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from apps.synapse_admin.backend import ask, auth, chat, okta  # noqa: E402
from apps.synapse_admin.backend.app import create_app  # noqa: E402
from fake_spanner import FakeSpannerDatabase  # noqa: E402
from test_oidc import CLIENT, ISSUER  # noqa: E402

ANA = {"email": "ana@example.com", "password": "passw0rd!yes",
       "first_name": "Ana", "last_name": "Lyst"}
BO = {"email": "bo@example.com", "password": "passw0rd!too", "name": "Bo"}
_OKTA = ("OKTA_ISSUER", "OKTA_CLIENT_ID", "OKTA_CLIENT_SECRET", "OKTA_REDIRECT_URI",
         "AUTH_GROUP_ROLE_MAP")
_SPANNER = ("SPANNER_PROJECT_ID", "SPANNER_INSTANCE_ID", "SPANNER_DATABASE_ID",
            "SPANNER_EMULATOR_HOST", "SYNAPSE_SPANNER_SA_KEY",
            "GOOGLE_APPLICATION_CREDENTIALS")


def _reset() -> None:
    auth._identity.cache_clear()
    auth._session_cache.clear()
    okta.reset_client()
    chat._RUNTIME = None
    chat._RUNTIMES.clear()
    ask._RUNTIME = None
    ask._RUNTIMES.clear()


@pytest.fixture()
def laptop(monkeypatch, tmp_path):
    """A laptop with no Okta: the store on, the flag on, the pepper set."""
    for name in _OKTA + _SPANNER + ("AUTH_LOCAL_LOGIN", "AUTH_PEPPER", "SYNAPSE_USER_NAME"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SAHS_STORE", "sqlite")
    monkeypatch.setenv("SAHS_IDENTITY_SQLITE", str(tmp_path / "identity.sqlite3"))
    monkeypatch.setenv("AUTH_PEPPER", "laptop-pepper")
    monkeypatch.setenv("AUTH_COOKIE_SECURE", "auto")
    monkeypatch.setenv("AUTH_LOCAL_LOGIN", "1")
    monkeypatch.setenv("MERIDIAN_BUILDS_DIR", str(tmp_path / "builds"))
    monkeypatch.setenv("MERIDIAN_GRAPH_DIR", str(tmp_path / "graph"))
    _reset()
    yield tmp_path
    _reset()


@pytest.fixture()
def spanner(laptop, monkeypatch):
    """The same laptop on SAHS_STORE=spanner, the SDK's Database object
    replaced by the fake: the app's SpannerDatabase runs as deployed."""
    from sahs.identity import database as database_module
    fake = FakeSpannerDatabase(laptop / "fake-spanner.sqlite3")
    monkeypatch.setenv("SAHS_STORE", "spanner")
    monkeypatch.setenv("SPANNER_PROJECT_ID", "test-project")
    monkeypatch.setenv("SPANNER_INSTANCE_ID", "test-instance")
    monkeypatch.setenv("SPANNER_DATABASE_ID", "test-database")
    monkeypatch.setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
    monkeypatch.setattr(database_module.SpannerDatabase, "from_settings",
                        classmethod(lambda cls, settings: cls(fake)))
    _reset()
    return fake


def _client() -> TestClient:
    return TestClient(create_app())


def _csrf(client: TestClient) -> dict[str, str]:
    return {"x-csrf-token": client.cookies.get("synapse_csrf") or ""}


def test_the_status_says_the_form_is_open_and_okta_is_not(laptop):
    client = _client()
    status = client.get("/api/auth/okta").json()
    assert status["configured"] is False and status["local_login"] is True
    assert status["local_login_reason"] == "" and status["store"] == "sqlite"
    assert client.get("/api/auth/me").status_code == 401
    assert client.get("/api/whoami").status_code == 401


def test_signup_then_login_sets_the_cookie_and_whoami_names_the_person(laptop):
    client = _client()
    signed_up = client.post("/api/auth/signup", json=ANA)
    assert signed_up.status_code == 201, signed_up.text
    cookies = signed_up.headers.get_list("set-cookie")
    session = next(c for c in cookies if c.startswith("synapse_session="))
    assert "HttpOnly" in session and "Secure" not in session          # auto, over http
    assert any(c.startswith("synapse_csrf=") for c in cookies)
    user = signed_up.json()["user"]
    assert user["email"] == "ana@example.com" and user["name"] == "Ana Lyst"
    assert user["roles"] == ["analyst"] and user["surfaces"] == ["synapse"]
    assert client.get("/api/whoami").json()["user"]["user_id"] == user["user_id"]

    out = client.post("/api/auth/logout", headers=_csrf(client))
    assert out.status_code == 200 and client.get("/api/whoami").status_code == 401

    wrong = client.post("/api/auth/login", json={**ANA, "password": "not-this-one"})
    assert wrong.status_code == 401
    back = client.post("/api/auth/login", json={"email": ANA["email"], "password": ANA["password"]})
    assert back.status_code == 200, back.text
    assert any(c.startswith("synapse_session=") for c in back.headers.get_list("set-cookie"))
    for path in ("/api/whoami", "/api/auth/me"):
        me = client.get(path)
        assert me.status_code == 200 and me.json()["user"]["email"] == "ana@example.com"

    audit = auth._identity()._query("SELECT Action, Outcome FROM AuditEvents ORDER BY OccurredAt")
    assert [(a["Action"], a["Outcome"]) for a in audit] == [
        ("signup.ok", "success"), ("logout", "success"),
        ("login.failed", "denied"), ("login.ok", "success")]


def test_the_first_account_is_the_admin_when_the_env_names_it(laptop, monkeypatch):
    monkeypatch.setenv("AUTH_BOOTSTRAP_ADMIN_EMAIL", ANA["email"])
    client = _client()
    user = client.post("/api/auth/signup", json=ANA).json()["user"]
    assert user["roles"] == ["admin"] and user["surfaces"] == ["admin", "synapse"]
    assert client.get("/api/admin/users").json()["users"][0]["email"] == ANA["email"]


def test_with_okta_and_no_flag_the_local_routes_are_shut(laptop, monkeypatch):
    monkeypatch.delenv("AUTH_LOCAL_LOGIN")
    monkeypatch.setenv("OKTA_ISSUER", ISSUER)
    monkeypatch.setenv("OKTA_CLIENT_ID", CLIENT)
    monkeypatch.setenv("OKTA_CLIENT_SECRET", "sec")
    monkeypatch.setenv("OKTA_REDIRECT_URI", "http://testserver/callback")
    monkeypatch.setenv("AUTH_GROUP_ROLE_MAP", "Sem-Admins=admin, *=analyst")
    _reset()
    client = _client()
    status = client.get("/api/auth/okta").json()
    assert status["configured"] is True and status["provider"] == "okta"
    assert status["issuer_host"] == "org.example" and status["start"] == "/api/auth/okta/start"
    assert status["local_login"] is False
    assert "AUTH_LOCAL_LOGIN" in status["local_login_reason"]
    for path, body in (("/api/auth/signup", ANA), ("/api/auth/login", ANA),
                       ("/api/auth/reset-password", {"email": ANA["email"], "password": ANA["password"]})):
        refused = client.post(path, json=body)
        assert refused.status_code == 403 and "sign in with Okta" in refused.text, path
    assert auth._identity().list_users(10) == []                    # nobody got in


def test_the_status_names_the_missing_variable_on_a_misconfigured_laptop(laptop, monkeypatch):
    client = _client()
    monkeypatch.setenv("AUTH_PEPPER", "short")                        # the Spanner rule: 8+
    status = client.get("/api/auth/okta").json()
    assert status["local_login"] is False and "AUTH_PEPPER" in status["local_login_reason"]
    monkeypatch.setenv("AUTH_PEPPER", "laptop-pepper")
    monkeypatch.setenv("SAHS_STORE", "local")
    status = client.get("/api/auth/okta").json()
    assert status["local_login"] is False and status["store"] == "local"
    assert "SAHS_STORE" in status["local_login_reason"]
    page = client.get("/js/pages/signin.js").text
    assert "status.local_login_reason" in page                        # the card shows it
    assert "status.local_login_reason" in client.get("/synapse/js/pages/signin.js").text


def test_on_spanner_every_row_lands_in_the_spanner_tables(spanner, laptop):
    client = _client()
    status = client.get("/api/auth/okta").json()
    assert status["local_login"] is True and status["store"] == "spanner"

    # nobody signed in: the chats are a person's, so the door is shut
    assert client.post("/api/chat/sessions").status_code == 401
    assert client.get("/api/chat/sessions").status_code == 401

    signed_up = client.post("/api/auth/signup", json=ANA)
    assert signed_up.status_code == 201, signed_up.text
    ana = signed_up.json()["user"]
    assert client.post("/api/auth/logout", headers=_csrf(client)).status_code == 200
    assert client.post("/api/auth/login", json={"email": ANA["email"],
                                                "password": ANA["password"]}).status_code == 200
    assert client.get("/api/whoami").json()["user"]["user_id"] == ana["user_id"]
    csrf = _csrf(client)

    # the identity rows, through SpannerDatabase into the fake
    assert spanner.count("Users") == 1 and spanner.count("UserCredentials") == 1
    assert spanner.count("AuthSessions") == 2 and spanner.count("UserRoles") == 1
    assert spanner.count("Roles") >= 3 and spanner.count("RolePermissions") > 0
    assert [a["Action"] for a in spanner.rows("AuditEvents")] == ["signup.ok", "logout", "login.ok"]
    assert spanner.count("LoginAttempts", "Succeeded = 1") == 1

    # a chat, renamed and starred; a project; the memory document; an
    # artifact — each through the same database object
    made = client.post("/api/chat/sessions", headers=csrf)
    assert made.status_code == 201, made.text
    session = made.json()["session"]
    assert client.post(f"/api/chat/sessions/{session['id']}/rename",
                       json={"title": "Merchant churn"}, headers=csrf).json()["title"] == "Merchant churn"
    assert client.post(f"/api/chat/sessions/{session['id']}/star",
                       json={"on": True}, headers=csrf).json()["starred"] is True
    project = client.post("/api/chat/projects", json={"name": "Churn", "instructions": "brief"},
                          headers=csrf).json()["project"]
    assert client.post(f"/api/chat/sessions/{session['id']}/project",
                       json={"project_id": project["id"]}, headers=csrf).json()["ok"] is True
    saved = client.put("/api/chat/memory.md",
                       json={"text": "# me\n\n- prefers CAD\n- weekly grain\n"}, headers=csrf).json()
    assert saved["added"] == 2
    memories = client.get("/api/chat/memories").json()["memories"]
    assert [m["text"] for m in memories] == ["prefers CAD", "weekly grain"]
    assert client.post(f"/api/chat/memories/{memories[0]['id']}/retire", headers=csrf).json()["retired"]

    runtime = chat._RUNTIMES[ana["user_id"]]
    from sahs.assistant.spanner_store import SpannerAssistantStore
    assert isinstance(runtime.store, SpannerAssistantStore)
    assert runtime.user_name == "Ana Lyst"
    runtime.store.add_message(session["id"], "user", "how should I think about churn?")
    runtime.store.add_message(session["id"], "assistant", "as a rate and a mix", turn_id="t1",
                              payload={"chips": []})
    art = runtime.store.add_artifact(session["id"], turn_id="t1", type="chart",
                                     title="Churn by month", spec={"kind": "line"})

    shown = client.get(f"/api/chat/sessions/{session['id']}").json()
    assert shown["session"]["title"] == "Merchant churn" and shown["session"]["starred"]
    assert shown["session"]["project_id"] == project["id"]
    assert [m["role"] for m in shown["messages"]] == ["user", "assistant"]
    assert shown["messages"][1]["payload"] == {"chips": []}
    assert [a["artifact_id"] for a in shown["artifacts"]] == [art["artifact_id"]]
    assert client.get(f"/api/chat/artifacts/{art['artifact_id']}").json()["artifact"]["spec"] == {"kind": "line"}
    listed = client.get("/api/chat/sessions").json()["sessions"]
    assert [s["id"] for s in listed] == [session["id"]] and listed[0]["messages"] == 2
    found = client.get("/api/chat/search", params={"q": "churn"}).json()["sessions"]
    assert [s["id"] for s in found] == [session["id"]]

    # the rows are in the Spanner tables, owned by the person
    owner = {"u": ana["user_id"]}
    assert spanner.count("ChatSessions", "OwnerUserId = @u", owner) == 1
    assert spanner.rows("ChatSessions")[0]["Title"] == "Merchant churn"
    assert spanner.count("ChatMessages", "OwnerUserId = @u", owner) == 2
    assert spanner.count("ChatProjects", "OwnerUserId = @u", owner) == 1
    assert spanner.count("ChatMemories", "UserId = @u AND Status = 'active'", owner) == 1
    assert spanner.count("ChatMemories", "UserId = @u AND Status = 'retired'", owner) == 1
    assert spanner.count("ChatArtifacts") == 1
    assert json.loads(spanner.rows("ChatArtifacts")[0]["Spec"]) == {"kind": "line"}

    # and not in any sqlite chat file on disk
    for path in (laptop / "graph").rglob("sessions.sqlite3"):
        conn = sqlite3.connect(path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0, path
            assert conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0] == 0, path
        finally:
            conn.close()

    # a second person sees their own shelf and nothing of Ana's
    bo_client = TestClient(client.app)
    assert bo_client.post("/api/auth/signup", json=BO).status_code == 201
    assert bo_client.get("/api/chat/sessions").json()["sessions"] == []
    assert bo_client.get("/api/chat/memories").json()["memories"] == []
    assert bo_client.get(f"/api/chat/sessions/{session['id']}").json()["available"] is False
    assert bo_client.get(f"/api/chat/artifacts/{art['artifact_id']}").json()["available"] is False
    assert spanner.count("Users") == 2


def _frames(client: TestClient, url: str, **params) -> list[dict]:
    """The SSE frames of one stream, as (id, event) pairs with the data."""
    out = []
    with client.stream("GET", url, params=params) as response:
        assert response.status_code == 200
        frame: dict = {}
        for line in response.iter_lines():
            line = line.rstrip("\r\n")
            if not line:
                if frame:
                    out.append(frame)
                frame = {}
            elif line.startswith("id: "):
                frame["id"] = int(line[4:])
            elif line.startswith("event: "):
                frame["event"] = line[7:]
            elif line.startswith("data: "):
                frame["data"] = json.loads(line[6:])
    return out


def test_a_turns_events_land_in_chat_events_and_replay_after_a_restart(spanner, laptop):
    """Every record the chat's bus emits is also a ChatEvents row; when
    the pod restarts (no runtime, an empty bus) the stream and the
    session's turn window are served from the table."""
    client = _client()
    assert client.post("/api/auth/signup", json=ANA).status_code == 201
    ana = client.get("/api/whoami").json()["user"]
    csrf = _csrf(client)
    session = client.post("/api/chat/sessions", headers=csrf).json()["session"]
    runtime = chat._RUNTIMES[ana["user_id"]]
    rt = runtime.runtime(session["id"])
    rt.bus.emit("turn_started", turn_id="t1")
    rt.bus.emit("say_token", turn_id="t1", delta="churn is a rate")
    rt.bus.emit("turn_done", turn_id="t1", tokens={"in": 3})
    rows = spanner.rows("ChatEvents", "SessionId = @s", {"s": session["id"]})
    assert [(r["Seq"], r["Ev"], r["TurnId"]) for r in rows] == [
        (1, "turn_started", "t1"), (2, "say_token", "t1"), (3, "turn_done", "t1")]
    assert json.loads(rows[1]["Payload"])["delta"] == "churn is a rate"
    live = _frames(client, f"/api/chat/sessions/{session['id']}/stream", after=0, once=True)
    assert [(f["id"], f["event"]) for f in live] == [
        (1, "turn_started"), (2, "say_token"), (3, "turn_done")]

    # the pod restarts: the runtimes are gone, and with them every bus
    chat._RUNTIMES.clear()
    shown = client.get(f"/api/chat/sessions/{session['id']}").json()
    assert shown["head"] == 3 and shown["running"] is False and shown["turn_id"] == ""
    replay = _frames(client, f"/api/chat/sessions/{session['id']}/stream", after=0, once=True)
    assert [(f["id"], f["event"]) for f in replay] == [
        (1, "turn_started"), (2, "say_token"), (3, "turn_done")]
    assert replay[1]["data"]["delta"] == "churn is a rate"
    assert replay[2]["data"]["tokens"] == {"in": 3}
    resumed = _frames(client, f"/api/chat/sessions/{session['id']}/stream", after=2, once=True)
    assert [(f["id"], f["event"]) for f in resumed] == [(3, "turn_done")]
    # a new turn on the rebuilt bus numbers on from the table
    rt = chat._RUNTIMES[ana["user_id"]].runtime(session["id"])
    assert rt.bus.emit("turn_started", turn_id="t2")["seq"] == 4
    assert spanner.count("ChatEvents", "SessionId = @s", {"s": session["id"]}) == 4

    # a restart mid-turn: the session says which turn was cut and where it began
    chat._RUNTIMES.clear()
    shown = client.get(f"/api/chat/sessions/{session['id']}").json()
    assert shown["running"] is False and shown["turn_id"] == "t2" and shown["turn_after"] == 3
    # another person's stream of this chat replays nothing
    bo_client = TestClient(client.app)
    assert bo_client.post("/api/auth/signup", json=BO).status_code == 201
    assert bo_client.get(f"/api/chat/sessions/{session['id']}").json()["available"] is False


def test_the_ask_lane_lands_in_the_chat_tables_with_the_owner(spanner, laptop):
    """The E18 lane (/api/sessions*) on a store: its runtime's store is
    the chat-table store bound to the person, sessions are kind
    analyst or steward, and the message, plan and feedback rows carry
    the owner."""
    from sahs.assistant.spanner_store import SpannerAssistantStore
    client = _client()
    assert client.post("/api/auth/signup", json=ANA).status_code == 201
    ana = client.get("/api/whoami").json()["user"]
    csrf = _csrf(client)
    made = client.post("/api/sessions", json={"kind": "analyst"}, headers=csrf)
    assert made.status_code == 201, made.text
    session = made.json()["session"]
    assert session["kind"] == "analyst" and session["actor"] == ana["user_id"]
    runtime = ask._RUNTIMES[ana["user_id"]]
    assert isinstance(runtime.store, SpannerAssistantStore)
    assert runtime.store.owner_user_id == ana["user_id"]
    # the steward hat needs metrics.certify, which an analyst lacks
    assert client.post("/api/sessions", json={"kind": "steward"}, headers=csrf).status_code == 403

    runtime.store.add_message(session["id"], "user", "what is churn?", turn_id="t1")
    runtime.store.add_message(session["id"], "assistant", "a rate", turn_id="t1",
                              payload={"chat": {"kind": "answer"}})
    runtime.store.add_plan_version(session["id"], {"metric": "churn"}, turn_id="t1",
                                   summary="churn by month")
    recorded = client.post(f"/api/sessions/{session['id']}/feedback",
                           json={"vote": "up", "turn_id": "t1", "note": "good"}, headers=csrf)
    assert recorded.status_code == 201, recorded.text
    shown = client.get(f"/api/sessions/{session['id']}").json()
    assert [m["text"] for m in shown["messages"]] == ["what is churn?", "a rate"]
    assert shown["messages"][1]["payload"] == {"chat": {"kind": "answer"}}
    assert shown["plan_versions"][0]["plan"] == {"metric": "churn"}
    assert shown["session"]["title"] == "" and shown["running"] is False
    restored = client.post(f"/api/sessions/{session['id']}/plan/restore",
                           json={"version": 1}, headers=csrf).json()
    assert restored["restored"] is False                      # v1 is the current plan
    listed = client.get("/api/sessions").json()["sessions"]
    assert [s["id"] for s in listed] == [session["id"]] and listed[0]["running"] is False
    # the chat shelf (kind assistant) does not list the analyst session, nor the reverse
    assert client.get("/api/chat/sessions").json()["sessions"] == []
    chat_session = client.post("/api/chat/sessions", headers=csrf).json()["session"]
    assert [s["id"] for s in client.get("/api/sessions").json()["sessions"]] == [session["id"]]
    assert [s["id"] for s in client.get("/api/chat/sessions").json()["sessions"]] == [chat_session["id"]]

    owner = {"u": ana["user_id"]}
    assert spanner.count("ChatSessions", "OwnerUserId = @u AND Kind = 'analyst'", owner) == 1
    assert spanner.count("ChatMessages", "OwnerUserId = @u", owner) == 2
    assert spanner.count("ChatPlanVersions", "SessionId = @s", {"s": session["id"]}) == 1
    feedback = spanner.rows("ChatFeedback", "UserId = @u", owner)
    assert len(feedback) == 1 and feedback[0]["Vote"] == "up" and feedback[0]["Note"] == "good"
    # and not in the lane's sqlite file
    for path in (laptop / "graph" / "runs" / "ask").rglob("sessions.sqlite3"):
        conn = sqlite3.connect(path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0, path
        finally:
            conn.close()
    # another person sees nothing of it
    bo_client = TestClient(client.app)
    assert bo_client.post("/api/auth/signup", json=BO).status_code == 201
    assert bo_client.get("/api/sessions").json()["sessions"] == []
    assert bo_client.get(f"/api/sessions/{session['id']}").json()["available"] is False


def test_the_google_consent_state_is_parked_in_the_store(spanner, laptop, monkeypatch):
    """The Google hop's state lives in AuthStates like the Okta hop's: a
    callback on another pod finds it, a replay does not."""
    monkeypatch.setenv("SAHS_BQ_AUTH_MODE", "user")
    monkeypatch.setenv("GOOGLE_OAUTH_CLIENT_ID", "gid")
    monkeypatch.setenv("GOOGLE_OAUTH_CLIENT_SECRET", "gsecret")
    monkeypatch.setenv("GOOGLE_OAUTH_REDIRECT_URI", "http://testserver/callback")
    monkeypatch.setenv("GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY", "k" * 44)
    client = _client()
    assert client.post("/api/auth/signup", json=ANA).status_code == 201
    hop = client.get("/api/auth/google/start", follow_redirects=False)
    assert hop.status_code == 307
    from urllib.parse import parse_qs, urlsplit
    state = parse_qs(urlsplit(hop.headers["location"]).query)["state"][0]
    parked = spanner.rows("AuthStates")
    assert len(parked) == 1 and parked[0]["Kind"] == "google_connect" and parked[0]["State"] == state
    # the callback pops it before anything else; a token exchange never
    # happens here, so the refusal comes from the exchange step
    monkeypatch.setattr(auth, "_google_json_request",
                        lambda url, *, data, headers=None: {"access_token": ""})
    first = client.get("/callback", params={"code": "c", "state": state})
    assert first.status_code == 401 and "did not issue an access token" in first.text
    assert spanner.count("AuthStates") == 0
    replay = client.get("/callback", params={"code": "c", "state": state})
    assert replay.status_code == 400 and "invalid or expired" in replay.text
