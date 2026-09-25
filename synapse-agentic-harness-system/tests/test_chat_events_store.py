"""The turn's event stream into ChatEvents (002_chat.sql): every record
the runtime's bus emits lands in the table through the store, a store
that fails never fails a turn, and a runtime built after a restart
(an empty bus) replays the chat from the table and numbers on from
where the table ends — on the sqlite stand-in and on the fake SDK
database, never a Spanner."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from fake_spanner import FakeSpannerDatabase  # noqa: E402
from sahs.assistant.spanner_store import SpannerAssistantStore  # noqa: E402
from sahs.identity.database import SpannerDatabase, SqliteDatabase  # noqa: E402


@pytest.fixture(params=["sqlite", "spanner"])
def backend(request):
    if request.param == "sqlite":
        db = SqliteDatabase(":memory:")
        return db, lambda table: db.query(f"SELECT * FROM {table}")
    fake = FakeSpannerDatabase()
    return SpannerDatabase(fake), fake.rows


def _record(seq: int, ev: str, turn_id: str = "t1", **fields):
    return {"schema": "meridian.event/1", "ts": "2026-01-01T00:00:00+00:00",
            "seq": seq, "session_id": "s", "ev": ev, "turn_id": turn_id, **fields}


def test_the_store_keeps_events_in_order_for_the_owner(backend):
    db, rows = backend
    ana = SpannerAssistantStore(db, "u-ana")
    sid = ana.create_session("assistant")["id"]
    assert ana.last_event_seq(sid) == 0 and ana.events(sid) == []
    assert ana.last_turn(sid) == {"turn_id": "", "first_seq": None, "closed": True}
    ana.add_event(sid, _record(1, "turn_started"))
    ana.add_event(sid, _record(2, "say_token", delta="hi"))
    assert ana.last_turn(sid) == {"turn_id": "t1", "first_seq": 1, "closed": False}
    ana.add_event(sid, _record(3, "turn_done", tokens={"in": 1}))
    raw = rows("ChatEvents")
    assert [(r["Seq"], r["Ev"], r["TurnId"]) for r in raw] == [
        (1, "turn_started", "t1"), (2, "say_token", "t1"), (3, "turn_done", "t1")]
    assert ana.last_event_seq(sid) == 3
    replay = ana.events(sid, 1)
    assert [e["seq"] for e in replay] == [2, 3]
    assert replay[0]["delta"] == "hi" and replay[1]["tokens"] == {"in": 1}
    assert replay[0]["ev"] == "say_token" and replay[0]["session_id"] == "s"
    assert ana.last_turn(sid) == {"turn_id": "t1", "first_seq": 1, "closed": True}
    # another owner reads nothing of it
    bo = SpannerAssistantStore(db, "u-bo")
    assert bo.events(sid) == [] and bo.last_event_seq(sid) == 0
    assert bo.last_turn(sid)["turn_id"] == ""


def _runtime(tmp_path, store, name="rt"):
    from sahs.assistant import AssistantRuntime
    runtime = AssistantRuntime(builds_root=tmp_path / "builds", graph_root=tmp_path / "graph",
                               store_path=tmp_path / name / "sessions.sqlite3",
                               events_dir=tmp_path / name / "events")
    runtime.store = store                       # as backend/chat.py does
    return runtime


def test_the_bus_sinks_every_record_into_the_table_and_a_fresh_runtime_replays(tmp_path):
    fake = FakeSpannerDatabase()
    store = SpannerAssistantStore(SpannerDatabase(fake), "u-ana")
    sid = store.create_session("assistant")["id"]
    first = _runtime(tmp_path, store, "one")
    rt = first.runtime(sid)
    assert rt.store is store and rt.bus.head() == 0
    rt.bus.emit("turn_started", turn_id="t1")
    rt.bus.emit("say_token", turn_id="t1", delta="churn is a rate")
    rt.bus.emit("turn_done", turn_id="t1", tokens={"in": 3})
    assert rt.store_errors == 0
    assert [r["Seq"] for r in fake.rows("ChatEvents")] == [1, 2, 3]
    assert json.loads(fake.rows("ChatEvents")[1]["Payload"])["delta"] == "churn is a rate"
    # the JSONL sink stays as it was
    lines = (tmp_path / "one" / "events" / f"{sid}.jsonl").read_text().splitlines()
    assert len(lines) == 3
    assert first.events_since(sid, 0) == rt.bus.since(0)       # the bus answers first

    # the pod restarts: a runtime with an empty bus
    second = _runtime(tmp_path, store, "two")
    assert second.turn_window(sid) == {"running": False, "turn_id": "", "after": None}
    replay = second.events_since(sid, 0)
    assert [(e["seq"], e["ev"]) for e in replay] == [
        (1, "turn_started"), (2, "say_token"), (3, "turn_done")]
    assert replay[1]["delta"] == "churn is a rate"
    assert second.events_since(sid, 2)[0]["seq"] == 3
    assert second.events_since(sid, 3) == []
    rt2 = second.runtime(sid)
    assert rt2.bus.head() == 3 and rt2.bus.since(0) == []    # numbering resumes
    rec = rt2.bus.emit("turn_started", turn_id="t2")
    assert rec["seq"] == 4
    assert [r["Seq"] for r in fake.rows("ChatEvents")] == [1, 2, 3, 4]
    assert [e["seq"] for e in second.events_since(sid, 3)] == [4]
    assert [e["seq"] for e in second.events_since(sid, 0)] == [1, 2, 3, 4]

    # a third pod comes up mid-turn: the table says where t2 began
    third = _runtime(tmp_path, store, "three")
    assert third.turn_window(sid) == {"running": False, "turn_id": "t2", "after": 3,
                                      "interrupted": True}


def test_a_store_that_fails_never_fails_the_turn(tmp_path, caplog):
    class Broken(SpannerAssistantStore):
        def add_event(self, session_id, record):
            raise RuntimeError("spanner is away")

        def last_event_seq(self, session_id):
            raise RuntimeError("spanner is away")

    fake = FakeSpannerDatabase()
    store = Broken(SpannerDatabase(fake), "u-ana")
    sid = store.create_session("assistant")["id"]
    runtime = _runtime(tmp_path, store)
    rt = runtime.runtime(sid)
    with caplog.at_level("WARNING", logger="sahs.assistant.runtime"):
        rec = rt.bus.emit("turn_started", turn_id="t1")
    assert rec["seq"] == 1 and rt.store_errors == 1 and rt.bus.sink_errors == 0
    assert "spanner is away" in caplog.text
    assert fake.count("ChatEvents") == 0
    assert runtime.events_since(sid, 0) == [rec]               # the bus still serves it
    assert (tmp_path / "rt" / "events" / f"{sid}.jsonl").exists()

    # a gap the table cannot fill is asked about once, not on every poll
    fresh = _runtime(tmp_path, store, "fresh")
    fresh.runtime(sid).bus.resume(5)                           # as if 5 were stored
    asked = 0
    real = store.events

    def counting(session_id, after_seq=0, limit=4000):
        nonlocal asked
        asked += 1
        return real(session_id, after_seq, limit)

    store.events = counting
    assert fresh.events_since(sid, 0) == [] and fresh.events_since(sid, 0) == []
    assert asked == 1 and fresh.runtime(sid).empty_gap == (0, 6)


def test_without_a_store_that_keeps_events_nothing_changes(tmp_path):
    from sahs.assistant import AssistantRuntime
    runtime = AssistantRuntime(builds_root=tmp_path / "builds", graph_root=tmp_path / "graph",
                               store_path=tmp_path / "sessions.sqlite3",
                               events_dir=tmp_path / "events")
    sid = runtime.store.create_session("assistant")["id"]
    rt = runtime.runtime(sid)
    assert rt.store is None and rt.bus.sinks == []
    rt.bus.emit("turn_started", turn_id="t1")
    assert runtime.events_since(sid, 0)[0]["seq"] == 1 and runtime.events_since(sid, 1) == []
    assert runtime.turn_window(sid) == {"running": False, "turn_id": "", "after": None}
