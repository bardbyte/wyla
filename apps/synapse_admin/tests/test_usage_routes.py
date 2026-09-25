"""What a chat cost, through the app under a store: on the sqlite
stand-in and on SAHS_STORE=spanner over the fake SDK database, a turn's
usage lands on the signed-in person's chat row and its final message, a
second turn adds, GET /api/chat/sessions, /sessions/{id} and /search
carry the totals, and GET /api/admin/users gives every person the sum
of their own chats and nothing of anyone else's."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
SILO = REPO_ROOT / "synapse-agentic-harness-system"
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))
sys.path.insert(0, str(REPO_ROOT / "apps" / "synapse_admin" / "tests"))

from apps.synapse_admin.backend import chat  # noqa: E402
from test_content_store_app import (ANA, BO, _client, _csrf,  # noqa: E402,F401
                                    backend, laptop, spanner)


def _turn(rt, turn_id: str, *, tin: int, tout: int, calls: int, ms: float) -> None:
    """A turn's end as the loop emits it: the turn's own split from the
    budget's tick, the loop's model_calls, the wall time."""
    rt.bus.emit("turn_done", turn_id=turn_id, status="answered", elapsed_ms=ms,
                model_calls=calls, turn_tokens_in=tin, turn_tokens_out=tout,
                turn_tokens=tin + tout, turn_calls=calls, tokens_in=tin, tokens_out=tout,
                turn_cost_usd=None)


def test_the_routes_carry_each_persons_usage(backend, monkeypatch):
    _laptop, rows = backend
    monkeypatch.setenv("AUTH_BOOTSTRAP_ADMIN_EMAIL", ANA["email"])
    client = _client()
    ana = client.post("/api/auth/signup", json=ANA).json()["user"]
    assert "admin" in ana["roles"]
    csrf = _csrf(client)
    sid = client.post("/api/chat/sessions", headers=csrf).json()["session"]["id"]
    runtime = chat._RUNTIMES[ana["user_id"]]
    rt = runtime.runtime(sid)
    rt.bus.emit("turn_started", turn_id="t1", text="churn?")
    runtime.store.add_message(sid, "user", "what is churn", turn_id="t1")
    runtime.store.add_message(sid, "assistant", "a rate", turn_id="t1",
                              payload={"chips": [], "trace": []})
    _turn(rt, "t1", tin=7900, tout=310, calls=2, ms=12400.0)
    runtime.store.add_message(sid, "assistant", "still a rate", turn_id="t2")
    _turn(rt, "t2", tin=100, tout=10, calls=1, ms=600.0)
    assert rt.bus.sink_errors == 0
    # the shelf, the chat, the search: the same totals
    mine = next(s for s in client.get("/api/chat/sessions").json()["sessions"]
                if s["id"] == sid)
    assert (mine["tokens_in"], mine["tokens_out"], mine["tokens"], mine["model_calls"],
            mine["elapsed_ms"], mine["turns"]) == (8000, 320, 8320, 3, 13000, 2)
    detail = client.get(f"/api/chat/sessions/{sid}").json()
    assert detail["session"]["tokens"] == 8320 and detail["session"]["turns"] == 2
    by_turn = {m["turn_id"]: m for m in detail["messages"] if m["role"] == "assistant"}
    assert by_turn["t1"]["payload"]["usage"] == {
        "tokens_in": 7900, "tokens_out": 310, "tokens": 8210, "calls": 2,
        "elapsed_ms": 12400, "cost_usd": None}
    assert by_turn["t1"]["payload"]["chips"] == []               # the rest kept
    assert by_turn["t2"]["payload"]["usage"]["tokens"] == 110
    found = client.get("/api/chat/search?q=churn").json()["sessions"]
    assert [s["id"] for s in found] == [sid]
    assert found[0]["tokens"] == 8320 and found[0]["turns"] == 2
    assert found[0]["tokens_in"] == 8000 and found[0]["model_calls"] == 3
    # the rows: the DDL's columns, on this person's chat
    raw = rows("ChatSessions", "SessionId = @id", {"id": sid})
    assert [raw[0][c] for c in ("TokensIn", "TokensOut", "ModelCalls", "ElapsedMs", "Turns")] \
        == [8000, 320, 3, 13000, 2]
    # a second person: their chat, their turn, nothing of Ana's
    bo_client = _client()
    bo = bo_client.post("/api/auth/signup", json=BO).json()["user"]
    bo_sid = bo_client.post("/api/chat/sessions",
                            headers=_csrf(bo_client)).json()["session"]["id"]
    bo_rt = chat._RUNTIMES[bo["user_id"]].runtime(bo_sid)
    _turn(bo_rt, "t1", tin=5, tout=5, calls=1, ms=5.0)
    theirs = bo_client.get("/api/chat/sessions").json()["sessions"]
    assert [s["id"] for s in theirs] == [bo_sid] and theirs[0]["tokens"] == 10
    assert bo_client.get(f"/api/chat/sessions/{sid}").json()["available"] is False
    # the People page: one aggregate per person, the sum of their chats
    people = client.get("/api/admin/users").json()
    assert people["available"] and "usage_note" not in people
    by_email = {u["email"]: u["usage"] for u in people["users"]}
    assert by_email[ANA["email"]] == {
        "tokens_in": 8000, "tokens_out": 320, "tokens": 8320, "model_calls": 3,
        "elapsed_ms": 13000, "turns": 2, "chats": 1}
    assert by_email[BO["email"]] == {
        "tokens_in": 5, "tokens_out": 5, "tokens": 10, "model_calls": 1,
        "elapsed_ms": 5, "turns": 1, "chats": 1}
    # another chat of Ana's with no turn yet counts as a chat, adds nothing
    client.post("/api/chat/sessions", headers=csrf)
    again = {u["email"]: u["usage"] for u in client.get("/api/admin/users").json()["users"]}
    assert again[ANA["email"]]["chats"] == 2 and again[ANA["email"]]["tokens"] == 8320
    # a person who cannot manage users cannot read the column
    assert bo_client.get("/api/admin/users").status_code == 403
