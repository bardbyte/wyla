"""Content to Spanner, through the app: under SAHS_STORE=spanner (the
SDK's Database object replaced by the fake) and the sqlite stand-in,
a file uploaded to a chat, a person's own skill, a submission and its
approval, and a knowledge file staged at the door all land in the
content tables — ChatFiles and ChatFileChunks, UserSkills,
ReviewSubmissions and its children, KnowledgeFiles — and nothing of
theirs is written under the graph directory. The filesystem path
under SAHS_STORE=local is pinned by the other suites."""

from __future__ import annotations

import base64
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from apps.synapse_admin.backend import auth, chat, okta  # noqa: E402
from apps.synapse_admin.backend.app import create_app  # noqa: E402
from fake_spanner import FakeSpannerDatabase  # noqa: E402

ANA = {"email": "ana@example.com", "password": "passw0rd!yes",
       "first_name": "Ana", "last_name": "Lyst"}
BO = {"email": "bo@example.com", "password": "passw0rd!too", "name": "Bo"}
PACK = ("# Approvals triage\n\nThe moves for an approvals question.\n\n"
        "## Rate first\n1. search(\"approval rate\") for the definition.\n\n"
        "## Never\n- never quote a rate without its denominator\n")
_OKTA = ("OKTA_ISSUER", "OKTA_CLIENT_ID", "OKTA_CLIENT_SECRET", "OKTA_REDIRECT_URI",
         "AUTH_GROUP_ROLE_MAP")
_SPANNER = ("SPANNER_PROJECT_ID", "SPANNER_INSTANCE_ID", "SPANNER_DATABASE_ID",
            "SPANNER_EMULATOR_HOST", "SYNAPSE_SPANNER_SA_KEY",
            "GOOGLE_APPLICATION_CREDENTIALS")


def _reset() -> None:
    from sahs.assistant import skills_loader
    auth._identity.cache_clear()
    auth._session_cache.clear()
    okta.reset_client()
    chat._RUNTIME = None
    chat._RUNTIMES.clear()
    skills_loader._OWN_SHELVES.clear()


@pytest.fixture()
def laptop(monkeypatch, tmp_path):
    """A laptop with no Okta: the sqlite store on, the flag on, the
    pepper set, the graph and the sources under tmp."""
    for name in _OKTA + _SPANNER + ("AUTH_LOCAL_LOGIN", "AUTH_PEPPER", "SYNAPSE_USER_NAME",
                                    "SYNAPSE_USER_MANAGER", "SYNAPSE_USER_MANAGER_BAND"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SAHS_STORE", "sqlite")
    monkeypatch.setenv("SAHS_IDENTITY_SQLITE", str(tmp_path / "identity.sqlite3"))
    monkeypatch.setenv("AUTH_PEPPER", "laptop-pepper")
    monkeypatch.setenv("AUTH_COOKIE_SECURE", "auto")
    monkeypatch.setenv("AUTH_LOCAL_LOGIN", "1")
    monkeypatch.setenv("MERIDIAN_BUILDS_DIR", str(tmp_path / "builds"))
    monkeypatch.setenv("MERIDIAN_GRAPH_DIR", str(tmp_path / "graph"))
    monkeypatch.setenv("MERIDIAN_SOURCES_DIR", str(tmp_path / "sources"))
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


@pytest.fixture(params=["sqlite", "spanner"])
def backend(request, laptop):
    """(the laptop, a reader of the raw rows) on each store."""
    if request.param == "spanner":
        fake = request.getfixturevalue("spanner")
        return laptop, fake.rows
    return laptop, lambda table, where="", params=None: auth._identity()._query(
        f"SELECT * FROM {table}" + (f" WHERE {where}" if where else ""), params)


def _client() -> TestClient:
    return TestClient(create_app())


def _csrf(client: TestClient) -> dict[str, str]:
    return {"x-csrf-token": client.cookies.get("synapse_csrf") or ""}


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def test_every_piece_of_content_lands_in_the_store(backend):
    laptop, rows = backend
    client = _client()
    signed_up = client.post("/api/auth/signup", json=ANA)
    assert signed_up.status_code == 201, signed_up.text
    ana = signed_up.json()["user"]
    csrf = _csrf(client)

    # a file on a chat: the manifest row and the bytes, no workspace
    session = client.post("/api/chat/sessions", headers=csrf).json()["session"]
    sid = session["id"]
    csv = b"a,b\r\n1,2\r\n"
    added = client.post(f"/api/chat/sessions/{sid}/files",
                        json={"name": "rows.csv", "data_b64": _b64(csv)}, headers=csrf)
    assert added.status_code == 201, added.text
    file = added.json()["file"]
    assert file["rides"] == "text" and file["name"] == "rows.csv"
    refused = client.post(f"/api/chat/sessions/{sid}/files",
                          json={"name": "a.exe", "data_b64": _b64(b"x")}, headers=csrf).json()
    assert refused["available"] is False and "not a data file" in refused["reason"]
    listed = client.get(f"/api/chat/sessions/{sid}/files").json()["files"]
    assert [f["id"] for f in listed] == [file["id"]]
    assert client.get(f"/api/chat/sessions/{sid}").json()["files"] == listed
    assert rows("ChatFiles")[0]["Name"] == "rows.csv"
    assert rows("ChatFiles")[0]["Text"] == "a,b\n1,2\n"
    chunks = rows("ChatFileChunks")
    assert len(chunks) == 1 and bytes(chunks[0]["Chunk"]) == csv
    runtime = chat._RUNTIMES[ana["user_id"]]
    from sahs.assistant.content_store import SpannerContentStore
    assert isinstance(runtime.content_store, SpannerContentStore)
    assert runtime.reviews is runtime.content_store
    # the parts the loop would send ride from the store
    parts, used = runtime._file_parts(sid, [file["id"]])
    assert parts == [{"text": "[attached file: rows.csv]\na,b\n1,2\n\n[end of file]"}]
    assert [u["id"] for u in used] == [file["id"]]
    assert client.delete(f"/api/chat/sessions/{sid}/files/{file['id']}",
                         headers=csrf).json()["removed"] is True
    assert rows("ChatFiles") == [] and rows("ChatFileChunks") == []

    # an own skill: a UserSkills row, on the shelf as mine, for me alone
    saved = client.post("/api/chat/skills/mine",
                        json={"name": "Churn Triage", "text": PACK}, headers=csrf)
    assert saved.status_code == 201, saved.text
    assert saved.json()["skill"]["name"] == "churn-triage"
    assert saved.json()["skill"]["path"] == f"UserSkills/{ana['user_id']}/churn-triage"
    shelf = {p["name"]: p for p in client.get("/api/chat/skills").json()["skills"]}
    assert shelf["churn-triage"]["mine"] and shelf["churn-triage"]["author"] == "You"
    assert shelf["churn-triage"]["text"] == PACK and shelf["analysis-playbooks"]["origin"] == "built-in"
    assert rows("UserSkills", "UserId = @u", {"u": ana["user_id"]})[0]["Name"] == "churn-triage"
    pinned = client.post(f"/api/chat/sessions/{sid}/skills",
                         json={"names": ["churn-triage"]}, headers=csrf).json()
    assert pinned["ok"] and pinned["skills"] == ["churn-triage"]

    # a submission, its read, the approval: the review tables, then the
    # knowledge file as a KnowledgeFiles row
    board = client.get("/api/chat/reviews").json()
    assert board["available"] and board["approver"]["self_review"]
    sub = client.post("/api/chat/reviews", json={
        "kind": "knowledge", "name": "TLS glossary", "business_unit": "TLS",
        "purpose": "definitions",
        "text": "# TLS glossary\n\nNet sales: gross less cancellations.\n"},
        headers=csrf).json()
    assert sub["available"], sub
    rid = sub["submission"]["id"]
    assert sub["submission"]["status"] == "pending" and sub["submission"]["version"] == 1
    assert runtime.reviews.wait(rid, 10)
    got = client.get(f"/api/chat/reviews/{rid}").json()["submission"]
    assert got["ai_review"]["by"] == "checks" and got["ai_status"] in ("failed", "done")
    assert got["text"].startswith("# TLS glossary")
    assert client.get("/api/chat/reviews").json()["pending"] == 1
    assert rows("ReviewSubmissions")[0]["Status"] == "pending"
    assert rows("ReviewSubmissions")[0]["SubmitterUserId"] == ana["user_id"]
    assert len(rows("ReviewVersions")) == 1
    assert [e["Event"] for e in rows("ReviewEvents")] == ["submitted", "ai_review", "ai_review"]
    assert rows("KnowledgeFiles") == []
    shelf_before = client.get("/api/meridian/artifacts").json()
    assert shelf_before["staging_store"] is True and shelf_before["staged"] == []
    ok = client.post(f"/api/chat/reviews/{rid}/decision",
                     json={"decision": "approve", "comment": "good"}, headers=csrf).json()
    assert ok["available"], ok
    assert ok["submission"]["status"] == "published"
    assert ok["submission"]["published_path"] == "KnowledgeFiles/tls_tls-glossary.md"
    assert rows("ReviewSubmissions")[0]["Status"] == "published"
    staged = rows("KnowledgeFiles")
    assert len(staged) == 1 and staged[0]["BusinessUnit"] == "TLS"
    assert staged[0]["Name"] == "tls-glossary" and staged[0]["StagedBy"] == ana["user_id"]
    # the shelf lists it as staged, and reads it back
    shelf = client.get("/api/meridian/artifacts").json()
    assert shelf["staged"] == ["tls_tls-glossary.md"]
    entry = next(f for f in shelf["files"] if f["staged"])
    assert entry["rel"] == "artifacts/tls_tls-glossary.md" and entry["author"] == "TLS"
    assert entry["title"] == "TLS glossary" and entry["kind"] == "md"
    text = client.get("/api/meridian/artifact_file",
                      params={"rel": "artifacts/tls_tls-glossary.md"}).json()
    assert text["found"] and "gross less cancellations" in text["content"]
    # a skill through the board publishes onto my shelf
    skill = client.post("/api/chat/reviews", json={
        "kind": "skill", "name": "Approvals triage", "purpose": "approvals asks",
        "text": PACK}, headers=csrf).json()
    assert skill["available"], skill
    assert runtime.reviews.wait(skill["submission"]["id"], 10)
    ok = client.post(f"/api/chat/reviews/{skill['submission']['id']}/decision",
                     json={"decision": "approve"}, headers=csrf).json()
    assert ok["submission"]["status"] == "published"
    assert {r["Name"] for r in rows("UserSkills")} == {"churn-triage", "approvals-triage"}
    assert "approvals-triage" in {p["name"] for p in client.get("/api/chat/skills").json()["skills"]}
    # deleting the published pack withdraws its record
    assert client.delete("/api/chat/skills/mine/approvals-triage", headers=csrf).json()["removed"]
    assert client.get(f"/api/chat/reviews/{skill['submission']['id']}").json()["submission"]["status"] == "withdrawn"
    assert {r["Name"] for r in rows("UserSkills")} == {"churn-triage"}
    # the notices, then read
    assert client.get("/api/chat/reviews").json()["unread"] > 0
    assert client.post("/api/chat/reviews/seen", headers=csrf).json()["available"]
    assert client.get("/api/chat/reviews").json()["unread"] == 0
    assert rows("ReviewSeen")[0]["UserId"] == ana["user_id"]

    # the staging door: a KnowledgeFiles row too
    door = client.post("/api/meridian/artifacts", json={
        "business_unit": "CFR", "name": "Card rules", "content": "# Card rules\n\nwords\n"},
        headers=csrf).json()
    assert door["staged"] and door["file"] == "cfr_card-rules.md", door
    twice = client.post("/api/meridian/artifacts", json={
        "business_unit": "CFR", "name": "Card rules", "content": "# again\n"},
        headers=csrf).json()
    assert not twice["staged"] and "already staged" in twice["reason"]
    assert {r["Name"] for r in rows("KnowledgeFiles")} == {"tls-glossary", "card-rules"}
    assert sorted(client.get("/api/meridian/artifacts").json()["staged"]) == [
        "cfr_card-rules.md", "tls_tls-glossary.md"]

    # nothing of this on the filesystem
    graph = laptop / "graph"
    for folder in ("runs/chat/workspaces", "runs/reviews", "skills/users", "skills"):
        assert not (graph / folder).exists(), folder
    assert not list((laptop / "graph").rglob("manifest.json"))
    assert not (laptop / "sources").exists()

    # a second person: their own shelf, their own files; the board and
    # the knowledge files shared
    bo_client = TestClient(client.app)
    assert bo_client.post("/api/auth/signup", json=BO).status_code == 201
    assert not any(p["mine"] for p in bo_client.get("/api/chat/skills").json()["skills"])
    assert bo_client.get(f"/api/chat/sessions/{sid}/files").json()["available"] is False
    assert bo_client.get("/api/chat/reviews").json()["pending"] == 0
    assert {s["name"] for s in bo_client.get("/api/chat/reviews").json()["submissions"]} == {
        "tls-glossary", "approvals-triage"}
    assert sorted(bo_client.get("/api/meridian/artifacts").json()["staged"]) == [
        "cfr_card-rules.md", "tls_tls-glossary.md"]


def test_the_staging_door_needs_a_person_under_a_store(spanner, laptop):
    client = _client()
    refused = client.post("/api/meridian/artifacts", json={
        "business_unit": "CFR", "name": "Card rules", "content": "# Card rules\n"}).json()
    assert not refused["staged"] and "sign in" in refused["reason"]
    assert spanner.count("KnowledgeFiles") == 0
    assert client.get("/api/meridian/artifacts").json()["staged"] == []
