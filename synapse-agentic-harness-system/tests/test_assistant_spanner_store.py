"""The assistant store on the chat tables (002_chat.sql): the same
verbs as the sqlite AssistantStore, the same shapes back, on the sqlite
stand-in directly and through ``SpannerDatabase`` over the fake SDK
database — so the Spanner code path (typed params, JsonObject cells,
mutations) runs in the test without a Spanner. Every row carries the
owner; another owner sees nothing."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from fake_spanner import FakeSpannerDatabase  # noqa: E402
from sahs.assistant.spanner_store import SpannerAssistantStore  # noqa: E402
from sahs.assistant.store import AssistantStore  # noqa: E402
from sahs.identity.database import SpannerDatabase, SqliteDatabase  # noqa: E402


@pytest.fixture(params=["sqlite", "spanner"])
def backend(request):
    """(database, a reader of the raw rows) for each backend."""
    if request.param == "sqlite":
        db = SqliteDatabase(":memory:")
        return db, lambda table: db.query(f"SELECT * FROM {table}")
    fake = FakeSpannerDatabase()
    return SpannerDatabase(fake), fake.rows


@pytest.fixture()
def store(backend) -> SpannerAssistantStore:
    return SpannerAssistantStore(backend[0], "u-ana")


def test_needs_an_owner(backend):
    with pytest.raises(ValueError, match="owner"):
        SpannerAssistantStore(backend[0], "")


def test_sessions_and_messages_have_the_sqlite_store_shape(store, backend, tmp_path):
    reference = AssistantStore(tmp_path / "ref.sqlite3")
    ours = store.create_session("assistant", build_id="b1", actor="ana")
    theirs = reference.create_session("assistant", build_id="b1", actor="ana")
    assert set(ours) >= set(theirs), set(theirs) - set(ours)
    assert ours["kind"] == "assistant" and ours["build_id"] == "b1"
    assert ours["skills"] == [] and ours["notes"] == [] and ours["handoff"] is None
    assert ours["starred"] is False and ours["archived"] is False and ours["model"] == ""
    with pytest.raises(ValueError):
        store.create_session("wizard")

    sid = ours["id"]
    first = store.add_message(sid, "user", "hello", turn_id="t1",
                              payload={"files": []})
    second = store.add_message(sid, "assistant", "hi", turn_id="t1",
                               payload=[{"chip": 1}])
    third = store.add_message(sid, "assistant", "plain")
    assert set(first) == set(reference.add_message(theirs["id"], "user", "x"))
    rows = store.messages(sid)
    assert [m["id"] for m in rows] == [first["id"], second["id"], third["id"]]
    assert rows[0]["payload"] == {"files": []} and rows[1]["payload"] == [{"chip": 1}]
    assert rows[2]["payload"] is None and rows[2]["role"] == "assistant"
    assert set(rows[0]) == set(reference.messages(theirs["id"])[0])
    listed = store.list_sessions()
    assert [s["id"] for s in listed] == [sid] and listed[0]["messages"] == 3
    with pytest.raises(KeyError):
        store.add_message("s_nope", "user", "x")

    # the rows are the DDL's: owner on every row, Seq in order
    raw = backend[1]("ChatMessages")
    assert [r["Seq"] for r in raw] == [1, 2, 3]
    assert {r["OwnerUserId"] for r in raw} == {"u-ana"}
    assert backend[1]("ChatSessions")[0]["MessageCount"] == 3


def test_session_edits_land_in_the_columns(store, backend):
    sid = store.create_session("assistant")["id"]
    store.set_title(sid, "Merchant churn " * 20)
    store.set_skills(sid, ["a", "b"])
    store.set_flag(sid, "starred", True)
    store.set_flag(sid, "archived", True)
    store.set_model(sid, " Gateway ")
    store.set_handoff(sid, {"status": "done", "say": "x"})
    store.set_notes(sid, [f"n{i}" for i in range(12)])
    got = store.get_session(sid)
    assert len(got["title"]) == 120 and got["skills"] == ["a", "b"]
    assert got["starred"] is True and got["archived"] is True and got["model"] == "gateway"
    assert got["handoff"] == {"status": "done", "say": "x"}
    assert got["notes"] == [f"n{i}" for i in range(4, 12)]
    project = store.create_project("Churn", instructions="be brief", skills=["a"])
    store.set_project(sid, project["id"])
    assert store.get_session(sid)["project_id"] == project["id"]
    store.set_project(sid, "")
    assert store.get_session(sid)["project_id"] == ""
    assert store.get_session("s_nope") is None
    store.set_title("s_nope", "nothing to update")               # a no-op, not an error


def test_a_model_choice_round_trips_whole(store, backend):
    """A catalog choice longer than a plane's name — plane:model, as
    the composer sends a 3.x engine — is stored whole on both backends
    (008_chat_model.sql: STRING(64), no plane CHECK) and read back as
    sent; a choice over the column is refused by name, never cut."""
    from sahs.assistant.spanner_store import MODEL_CHOICE_CHARS
    sid = store.create_session("assistant")["id"]
    choice = "gateway:gemini-3.7-flash"
    assert len(choice) > 16                      # the old truncation
    store.set_model(sid, f" {choice.upper()} ")
    assert store.get_session(sid)["model"] == choice
    assert backend[1]("ChatSessions")[0]["Model"] == choice
    longest = "gateway:" + "m" * (MODEL_CHOICE_CHARS - len("gateway:"))
    store.set_model(sid, longest)
    assert store.get_session(sid)["model"] == longest
    with pytest.raises(ValueError, match="65 characters"):
        store.set_model(sid, longest + "x")
    assert store.get_session(sid)["model"] == longest      # untouched
    store.set_model(sid, "")
    assert store.get_session(sid)["model"] == ""


def test_projects_memories_and_artifacts(store):
    project = store.create_project("Churn", instructions="be brief", skills=["a", "b", "c", "d", "e"])
    assert project["skills"] == ["a", "b", "c", "d"] and project["archived"] is False
    assert store.update_project(project["id"], name="Churn 2", archived=True)["name"] == "Churn 2"
    assert store.list_projects() == [] and store.list_projects(include_archived=True)[0]["archived"]
    assert store.update_project("p_nope", name="x") is None
    assert store.update_project(project["id"])["name"] == "Churn 2"     # nothing to change

    everywhere = store.add_memory(" prefers CAD ", source="person")
    scoped = store.add_memory("this project is about churn", scope=f"project:{project['id']}")
    assert everywhere["text"] == "prefers CAD" and everywhere["status"] == "active"
    assert [m["id"] for m in store.list_memories()] == [everywhere["id"]]
    both = store.list_memories(project_id=project["id"])
    assert [m["id"] for m in both] == [everywhere["id"], scoped["id"]]
    assert set(both[0]) == {"id", "text", "scope", "status", "source", "created_at"}
    assert store.retire_memory(everywhere["id"]) is True
    assert store.retire_memory(everywhere["id"]) is False
    assert store.list_memories() == []
    assert [m["id"] for m in store.list_memories(status="retired")] == [everywhere["id"]]

    sid = store.create_session("assistant")["id"]
    a = store.add_artifact(sid, turn_id="t1", type="chart", title="Revenue", spec={"kind": "line"})
    b = store.add_artifact(sid, turn_id="t1", type="table", title="Rows", spec={"columns": []})
    assert a["version"] == 1 and a["spec"] == {"kind": "line"}
    a2 = store.update_artifact(a["artifact_id"], turn_id="t2", spec={"kind": "bar"}, title="Revenue v2")
    assert a2["version"] == 2 and a2["type"] == "chart" and a2["title"] == "Revenue v2"
    assert store.update_artifact("a_nope", turn_id="t", spec={}) is None
    latest = store.get_artifact(a["artifact_id"])
    assert latest["version"] == 2 and latest["spec"] == {"kind": "bar"}
    assert store.get_artifact(a["artifact_id"], 1)["spec"] == {"kind": "line"}
    assert store.get_artifact(a["artifact_id"], 3) is None
    # the shelf orders by the latest version's creation, as the sqlite
    # store does: the edit made a newer than b
    shelf = store.list_artifacts(sid)
    assert [(x["artifact_id"], x["version"]) for x in shelf] == [(b["artifact_id"], 1), (a["artifact_id"], 2)]
    assert "spec" not in shelf[0]
    assert [v["version"] for v in store.artifact_versions(a["artifact_id"])] == [1, 2]
    assert set(store.artifact_versions(a["artifact_id"])[0]) == {"version", "title", "turn_id", "created_at"}


def test_plan_versions_and_feedback(store):
    sid = store.create_session("analyst")["id"]
    v1 = store.add_plan_version(sid, {"metric": "revenue"}, turn_id="t1", summary="first")
    v2 = store.add_plan_version(sid, {"metric": "revenue", "region": "CA"}, parent=1, turn_id="t2")
    assert (v1["version"], v2["version"], v2["parent"]) == (1, 2, 1)
    chain = store.plan_versions(sid)
    assert [p["plan"] for p in chain] == [{"metric": "revenue"}, {"metric": "revenue", "region": "CA"}]
    assert store.latest_plan(sid)["version"] == 2 and store.latest_plan("s_nope") is None
    fb = store.add_feedback(sid, "answer", "up", turn_id="t2", note="good")
    assert fb["actor"] == "u-ana"
    assert [f["vote"] for f in store.feedback(sid)] == ["up"]
    with pytest.raises(ValueError):
        store.add_feedback(sid, "answer", "sideways")


def test_another_owner_sees_nothing(backend):
    ana = SpannerAssistantStore(backend[0], "u-ana")
    bo = SpannerAssistantStore(backend[0], "u-bo")
    sid = ana.create_session("assistant")["id"]
    ana.add_message(sid, "user", "mine")
    art = ana.add_artifact(sid, turn_id="t", type="chart", title="x", spec={})
    ana.add_memory("ana likes charts")
    project = ana.create_project("Ana's")
    assert bo.list_sessions() == [] and bo.get_session(sid) is None
    assert bo.messages(sid) == []
    assert bo.get_artifact(art["artifact_id"]) is None
    assert bo.artifact_versions(art["artifact_id"]) == []
    assert bo.list_memories() == [] and bo.list_projects() == []
    assert bo.get_project(project["id"]) is None
    with pytest.raises(KeyError):
        bo.add_message(sid, "user", "not mine")
    assert ana.list_sessions()[0]["messages"] == 1


def test_the_fake_sdk_sees_spanner_typed_cells():
    """The double reads back what the SDK would hand the store: JSON as
    JsonObject, timestamps as datetimes, booleans as booleans."""
    from datetime import datetime

    from google.cloud.spanner_v1 import JsonObject

    fake = FakeSpannerDatabase()
    store = SpannerAssistantStore(SpannerDatabase(fake), "u-ana")
    sid = store.create_session("assistant")["id"]
    store.set_handoff(sid, {"status": "done"})
    store.set_flag(sid, "starred", True)
    with fake.snapshot() as snap:
        result = snap.execute_sql("SELECT Handoff, Starred, CreatedAt, Skills FROM ChatSessions")
        (handoff, starred, created, skills), = list(result)
    assert isinstance(handoff, JsonObject) and handoff == {"status": "done"}
    assert starred is True and isinstance(created, datetime) and skills == []
    assert fake.transactions >= 3
    assert store.get_session(sid)["created_at"].endswith("+00:00")
