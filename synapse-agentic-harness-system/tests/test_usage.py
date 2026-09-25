"""What a chat cost, stored and shown (009_usage.sql): the budget
counts the turn's own tokens, the runtime adds a finished turn's usage
to its chat's row and onto its final message, every store the runtime
can hold takes it — the sqlite AssistantStore, the chat tables on the
sqlite stand-in and through ``SpannerDatabase`` over the fake SDK — a
second turn adds, a task's sub-turn never counts twice, a person's
aggregate is the sum of their chats and no one else's, and the search
rows carry it."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from fake_spanner import FakeSpannerDatabase  # noqa: E402
from sahs.ask.budget import Budget  # noqa: E402
from sahs.assistant.spanner_store import (USAGE_COLUMNS,  # noqa: E402
                                          SpannerAssistantStore, usage_by_owner)
from sahs.assistant.store import AssistantStore, usage_of  # noqa: E402
from sahs.identity.database import SpannerDatabase, SqliteDatabase  # noqa: E402

USAGE_KEYS = {"tokens_in", "tokens_out", "tokens", "model_calls", "elapsed_ms", "turns"}


@pytest.fixture(params=["sqlite", "spanner"])
def backend(request):
    """(database, a reader of the raw rows) for each chat-table backend."""
    if request.param == "sqlite":
        db = SqliteDatabase(":memory:")
        return db, lambda table: db.query(f"SELECT * FROM {table}")
    fake = FakeSpannerDatabase()
    return SpannerDatabase(fake), fake.rows


def _done(turn_id: str = "t1", *, tin: int = 7900, tout: int = 310, calls: int = 2,
          ms: float = 12400.0, **extra) -> dict:
    """A turn_done record as the loop's _finish emits it: the turn's own
    split from Budget.tick, the loop's model_calls, the wall time."""
    return {"ev": "turn_done", "turn_id": turn_id, "status": "answered",
            "elapsed_ms": ms, "model_calls": calls, "turn_tokens_in": tin,
            "turn_tokens_out": tout, "turn_tokens": tin + tout, "turn_calls": calls,
            "tokens_in": 99999, "tokens_out": 99999, "turn_cost_usd": None, **extra}


def test_the_budget_counts_the_turns_own_tokens(monkeypatch):
    budget = Budget()
    budget.start_turn()
    budget.charge(tokens_in=100, tokens_out=20)
    budget.charge(tokens_in=200, tokens_out=30)
    tick = budget.tick()
    assert (tick["turn_tokens_in"], tick["turn_tokens_out"], tick["turn_tokens"],
            tick["turn_calls"]) == (300, 50, 350, 2)
    assert tick["tokens_in"] == 300 and tick["cost_usd"] is None
    assert tick["turn_cost_usd"] is None                # no rate: no number
    budget.start_turn()                                  # the next turn starts clean
    budget.charge(tokens_in=10, tokens_out=1)
    tick = budget.tick()
    assert (tick["turn_tokens_in"], tick["turn_tokens_out"]) == (10, 1)
    assert (tick["tokens_in"], tick["tokens_out"]) == (310, 51)   # the session keeps counting
    monkeypatch.setenv("SYNAPSE_COST_IN", "1000000")     # $1 a token, to read it plainly
    monkeypatch.setenv("SYNAPSE_COST_OUT", "2000000")
    assert budget.tick()["turn_cost_usd"] == 12.0 and budget.tick()["cost_usd"] == 412.0
    # what the runtime reads off the record
    usage = usage_of(_done(tin=300, tout=50, calls=2, ms=1234.6, turn_cost_usd=0.5))
    assert usage == {"tokens_in": 300, "tokens_out": 50, "tokens": 350, "calls": 2,
                     "elapsed_ms": 1234, "cost_usd": 0.5}
    # the runtime's own error path carries no model_calls: the budget's count
    assert usage_of({"ev": "turn_done", "turn_calls": 3, "turn_tokens_in": 1})["calls"] == 3


def test_a_turn_lands_on_the_chat_row_and_a_second_adds_on_every_store(backend, tmp_path):
    db, rows = backend
    stores = [SpannerAssistantStore(db, "u-ana"), AssistantStore(tmp_path / "chat.sqlite3")]
    for store in stores:
        sid = store.create_session("assistant")["id"]
        fresh = store.get_session(sid)
        assert USAGE_KEYS <= set(fresh) and all(fresh[k] == 0 for k in USAGE_KEYS)
        store.add_message(sid, "user", "hello", turn_id="t1")
        store.add_message(sid, "assistant", "hi", turn_id="t1",
                          payload={"chips": [], "elapsed_ms": 12400.0})
        store.add_usage(sid, tokens_in=7900, tokens_out=310, calls=2, elapsed_ms=12400.4)
        got = store.get_session(sid)
        assert (got["tokens_in"], got["tokens_out"], got["tokens"], got["model_calls"],
                got["elapsed_ms"], got["turns"]) == (7900, 310, 8210, 2, 12400, 1)
        store.add_usage(sid, tokens_in=100, tokens_out=10, calls=1, elapsed_ms=600)
        got = store.list_sessions()[0]
        assert got["id"] == sid
        assert (got["tokens_in"], got["tokens_out"], got["tokens"], got["model_calls"],
                got["elapsed_ms"], got["turns"]) == (8000, 320, 8320, 3, 13000, 2)
        # the turn's usage onto its final assistant message, the rest kept
        usage = {"tokens_in": 7900, "tokens_out": 310, "tokens": 8210, "calls": 2,
                 "elapsed_ms": 12400, "cost_usd": None}
        assert store.set_message_usage(sid, "t1", usage) is True
        last = store.messages(sid)[-1]
        assert last["role"] == "assistant" and last["payload"]["usage"] == usage
        assert last["payload"]["chips"] == [] and last["payload"]["elapsed_ms"] == 12400.0
        assert store.set_message_usage(sid, "t_none", usage) is False
        assert store.add_usage("s_nope", 1, 1, 1, 1) is None      # no row, no error
    # the chat tables: the DDL's columns, the owner's row only
    raw = rows("ChatSessions")
    assert len(raw) == 1 and [raw[0][c] for c in USAGE_COLUMNS] == [8000, 320, 3, 13000, 2]
    other = SpannerAssistantStore(db, "u-bo")
    other.add_usage(raw[0]["SessionId"], 5000, 5000, 5, 5000)     # not their chat
    assert rows("ChatSessions")[0]["TokensIn"] == 8000


def test_a_persons_aggregate_is_the_sum_of_their_chats_and_nobody_elses(backend, tmp_path):
    db, _rows = backend
    ana = SpannerAssistantStore(db, "u-ana")
    bo = SpannerAssistantStore(db, "u-bo")
    a1 = ana.create_session("assistant")["id"]
    a2 = ana.create_session("assistant")["id"]
    b1 = bo.create_session("assistant")["id"]
    ana.create_session("assistant")                     # a chat with no turn yet
    ana.add_usage(a1, 1000, 100, 2, 3000)
    ana.add_usage(a1, 500, 50, 1, 1000)
    ana.add_usage(a2, 200, 20, 1, 500)
    bo.add_usage(b1, 7, 7, 7, 7)
    totals = usage_by_owner(db)
    assert totals["u-ana"] == {"tokens_in": 1700, "tokens_out": 170, "tokens": 1870,
                               "model_calls": 4, "elapsed_ms": 4500, "turns": 3, "chats": 3}
    assert totals["u-bo"] == {"tokens_in": 7, "tokens_out": 7, "tokens": 14, "model_calls": 7,
                              "elapsed_ms": 7, "turns": 1, "chats": 1}
    assert set(totals) == {"u-ana", "u-bo"}
    mine = ana.list_sessions()
    assert sum(s["tokens"] for s in mine) == 1870 and sum(s["turns"] for s in mine) == 3
    # the single developer's store: its own totals, every chat summed
    local = AssistantStore(tmp_path / "chat.sqlite3")
    assert local.usage_totals() == {"tokens_in": 0, "tokens_out": 0, "tokens": 0,
                                    "model_calls": 0, "elapsed_ms": 0, "turns": 0, "chats": 0}
    s1 = local.create_session("assistant")["id"]
    s2 = local.create_session("assistant")["id"]
    local.add_usage(s1, 1000, 100, 2, 3000)
    local.add_usage(s2, 1, 1, 1, 1)
    assert local.usage_totals() == {"tokens_in": 1001, "tokens_out": 101, "tokens": 1102,
                                    "model_calls": 3, "elapsed_ms": 3001, "turns": 2, "chats": 2}


def test_a_stand_in_file_from_before_009_learns_the_columns_on_open(tmp_path):
    path = tmp_path / "old.sqlite3"
    db = SqliteDatabase(path)
    # the chat tables as 002 had them: no usage columns
    db.ensure("""
CREATE TABLE IF NOT EXISTS ChatSessions (
  SessionId TEXT PRIMARY KEY, OwnerUserId TEXT NOT NULL,
  Kind TEXT NOT NULL DEFAULT 'assistant', Title TEXT NOT NULL DEFAULT '',
  BuildId TEXT NOT NULL DEFAULT '', ProjectId TEXT, Model TEXT NOT NULL DEFAULT '',
  Skills TEXT NOT NULL DEFAULT '[]', Starred INTEGER NOT NULL DEFAULT 0,
  Archived INTEGER NOT NULL DEFAULT 0, Handoff TEXT, Notes TEXT,
  MessageCount INTEGER NOT NULL DEFAULT 0, CreatedAt TEXT NOT NULL, UpdatedAt TEXT NOT NULL);
""")
    db.run(lambda tx: tx.insert(
        "ChatSessions",
        ("SessionId", "OwnerUserId", "Kind", "Title", "BuildId", "Model", "Skills", "Starred",
         "Archived", "MessageCount", "CreatedAt", "UpdatedAt"),
        [("s_old", "u-ana", "assistant", "", "", "", "[]", 0, 0, 0,
          "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00")]))
    have = {r["name"] for r in db.query("PRAGMA table_info(ChatSessions)")}
    assert not (have & set(USAGE_COLUMNS))
    store = SpannerAssistantStore(db, "u-ana")           # the migration runs on open
    have = {r["name"] for r in db.query("PRAGMA table_info(ChatSessions)")}
    assert set(USAGE_COLUMNS) <= have
    assert store.get_session("s_old")["tokens"] == 0     # the old row reads as zero
    store.add_usage("s_old", 5, 5, 1, 5)
    assert store.get_session("s_old")["tokens"] == 10
    SpannerAssistantStore(db, "u-bo")                    # a second open: nothing to add
    # the sqlite AssistantStore migrates the same way: an old sessions
    # table without the columns
    old = AssistantStore(tmp_path / "chat.sqlite3")
    with old._conn() as conn:
        for column in ("tokens_in", "tokens_out", "model_calls", "elapsed_ms", "turns"):
            conn.execute(f"ALTER TABLE sessions DROP COLUMN {column}")
    again = AssistantStore(tmp_path / "chat.sqlite3")
    sid = again.create_session("assistant")["id"]
    again.add_usage(sid, 3, 3, 1, 3)
    assert again.get_session(sid)["tokens"] == 6


def _runtime(tmp_path, store=None, name="rt"):
    from sahs.assistant import AssistantRuntime
    runtime = AssistantRuntime(builds_root=tmp_path / "builds", graph_root=tmp_path / "graph",
                               store_path=tmp_path / name / "sessions.sqlite3",
                               events_dir=tmp_path / name / "events")
    if store is not None:
        runtime.store = store                   # as backend/chat.py does
    return runtime


def test_the_runtime_settles_a_turns_usage_when_turn_done_lands(backend, tmp_path):
    """The bus sink: the parent turn's turn_done adds to the row and
    writes the message's usage; a task's turn_done (a sub-turn of a
    multi-task turn) is left to the parent, so it counts once; a store
    that has no add_usage never fails the turn."""
    db, rows = backend
    for store in (SpannerAssistantStore(db, "u-ana"), None):
        runtime = _runtime(tmp_path, store, "spanner" if store else "local")
        store = runtime.store
        sid = store.create_session("assistant")["id"]
        rt = runtime.runtime(sid)
        rt.bus.emit("turn_started", turn_id="t1", text="churn?")
        store.add_message(sid, "assistant", "one task's answer", turn_id="t1.a",
                          payload={"task": {"id": "a"}})
        # a task's own turn_done: tagged, so the sink leaves it alone
        rt.bus.emit("turn_done", task="a", **{k: v for k, v in
                                              _done("t1.a", tin=500, tout=50, calls=1,
                                                    ms=900).items() if k != "ev"})
        assert store.get_session(sid)["turns"] == 0
        store.add_message(sid, "assistant", "the answer", turn_id="t1",
                          payload={"chips": [], "plan": {"tasks": []}})
        rt.bus.emit("turn_done", **{k: v for k, v in _done("t1").items() if k != "ev"})
        got = store.get_session(sid)
        assert (got["tokens_in"], got["tokens_out"], got["model_calls"], got["elapsed_ms"],
                got["turns"]) == (7900, 310, 2, 12400, 1)
        last = store.messages(sid)[-1]
        assert last["turn_id"] == "t1" and last["payload"]["usage"] == {
            "tokens_in": 7900, "tokens_out": 310, "tokens": 8210, "calls": 2,
            "elapsed_ms": 12400, "cost_usd": None}
        assert last["payload"]["plan"] == {"tasks": []}          # the rest kept
        # a second turn adds; an error turn with nothing said still counts
        # as a turn (the runtime's own turn_done carries the budget's tick)
        rt.bus.emit("turn_started", turn_id="t2", text="again")
        store.add_message(sid, "assistant", "more", turn_id="t2")
        rt.bus.emit("turn_done", **{k: v for k, v in
                                    _done("t2", tin=100, tout=10, calls=1, ms=100).items()
                                    if k != "ev"})
        rt.bus.emit("turn_done", turn_id="t3", status="error", turn_tokens_in=0,
                    turn_tokens_out=0, turn_calls=0, tokens_in=0)
        got = store.get_session(sid)
        assert (got["tokens"], got["model_calls"], got["turns"]) == (8320, 3, 3)
        assert runtime.sessions()[0]["tokens"] == 8320
        assert rt.bus.sink_errors == 0
    # a store without the verb: the turn goes on, the warning is logged
    class Bare:
        def __getattr__(self, name):
            raise AttributeError(name)
    runtime = _runtime(tmp_path, None, "bare")
    sid = runtime.store.create_session("assistant")["id"]
    rt = runtime.runtime(sid)
    runtime.store = Bare()
    rt.bus.emit("turn_done", turn_id="t9", turn_tokens_in=1)
    assert rt.bus.sink_errors == 0


def test_the_search_rows_carry_the_chats_usage(tmp_path):
    from sahs.assistant.search import search_sessions
    store = AssistantStore(tmp_path / "chat.sqlite3")
    sid = store.create_session("assistant")["id"]
    store.set_title(sid, "churn by region")
    store.add_message(sid, "user", "churn by region", turn_id="t1")
    store.add_usage(sid, 8000, 210, 3, 5000)
    store.add_usage(sid, 100, 10, 1, 500)
    for query in ("", "churn"):
        rows = search_sessions(store.list_sessions(), store.messages, query)
        assert len(rows) == 1
        assert (rows[0]["tokens"], rows[0]["tokens_in"], rows[0]["tokens_out"],
                rows[0]["model_calls"], rows[0]["turns"], rows[0]["elapsed_ms"]) == (
                    8320, 8100, 220, 4, 2, 5500)
    assert json.dumps(rows)                              # plain JSON for the route
