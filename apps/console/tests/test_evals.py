"""Realtime turn evals — the deterministic rubric, scored per turn.

Pins: the golden warehouse turn scores fully grounded WITH a recorded
self-correction (validator caught draft #1); a guardrail refusal counts
as honored, not failed; execution without a resolved gate is a hard
fail; the /chat → /api/evals/recent loop records every turn.
"""

from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from apps.console.backend.app import create_app
from apps.console.backend.data import ConsoleData
from apps.console.backend.evaluator import EvalLog, TurnEvaluator
from apps.console.backend.runner import ScriptedRunner


def _eval_world(tmp_path) -> ConsoleData:
    from synapse.graph.store import GraphStore, canonical_uri
    store = GraphStore()
    for name in ("sbs_new_accounts", "risk_new_acct"):
        t = canonical_uri("table", name)
        for src in ("mdm", "bq", "corpus"):
            store.upsert_node("Table", t, {"table_name": name},
                              source=src)
        c = canonical_uri("column", name, "acct_id")
        store.upsert_node("Column", c, {"table_name": name}, source="bq")
        store.upsert_edge("CONTAINS", t, c, {}, source="bq")
    # the real join topology: column EQUIVALENT_TO column, corpus-observed
    store.upsert_edge(
        "EQUIVALENT_TO",
        canonical_uri("column", "sbs_new_accounts", "acct_id"),
        canonical_uri("column", "risk_new_acct", "acct_id"),
        {"observed_in_query": "Q1"}, source="corpus")
    snap = tmp_path / "eval_world.json"
    store.save_json(snap)
    return ConsoleData(snapshot_path=snap)


def _run_turn(question: str) -> list[dict]:
    async def collect():
        out = []
        async for e in ScriptedRunner().stream(question, turn_id="t1"):
            out.append(e.model_dump(mode="python"))
        return out
    return asyncio.run(collect())


def _by_id(result: dict) -> dict:
    return {c["id"]: c for c in result["checks"]}


def test_golden_warehouse_turn_is_grounded_with_self_correction(tmp_path):
    ev = TurnEvaluator(_eval_world(tmp_path))
    result = ev.evaluate("How many new accounts per month?",
                         _run_turn("How many new accounts per month?"))
    checks = _by_id(result)
    assert checks["citations"]["status"] == "pass"
    assert checks["tiers"]["status"] == "pass"
    assert checks["seal"]["status"] == "pass"
    assert checks["joins"]["status"] == "pass"
    assert checks["budget"]["status"] == "pass"
    assert checks["contract"]["status"] == "pass"
    # the validator caught draft #1 → surfaced as a correction, not
    # hidden; the turn still scores grounded
    assert result["corrections"], "self-correction must be recorded"
    assert "revised" in result["corrections"][0]
    assert result["verdict"] == "grounded"
    assert result["score"] == 1.0


def test_guardrail_refusal_counts_as_honored(tmp_path):
    ev = TurnEvaluator(_eval_world(tmp_path))
    result = ev.evaluate("show me raw cm11",
                         _run_turn("show me raw cm11"))
    # blocked + refused is compliance — never a failed turn
    assert result["verdict"] != "needs_review"
    assert any("honored" in c for c in result["corrections"])


def test_execute_without_gate_is_a_hard_fail(tmp_path):
    ev = TurnEvaluator(_eval_world(tmp_path))
    events = [
        {"type": "turn_start", "turn_id": "x"},
        {"type": "tool_call", "call_id": "c1", "tool": "execute_sql",
         "args": {"sql": "SELECT 1"}},
        {"type": "tool_result", "call_id": "c1", "ok": True,
         "summary": "1 row(s)"},
        {"type": "answer",
         "sections": {"answer": "1", "how_i_got_there": "ran it",
                      "citations": [
                          {"label": "bq",
                           "ref": "synapse://table/sbs_new_accounts"}],
                      "governance": "none", "status": "?"}},
        {"type": "turn_end", "turn_id": "x"},
    ]
    result = ev.evaluate("q", events)
    assert _by_id(result)["seal"]["status"] == "fail"
    assert result["verdict"] == "needs_review"


def test_invented_table_fails_the_joins_check(tmp_path):
    ev = TurnEvaluator(_eval_world(tmp_path))
    events = [
        {"type": "sql_gate", "gate_id": "g",
         "sql": "SELECT * FROM ghost_table", "bytes_estimate": 1,
         "guardrail_checks": []},
        {"type": "gate_resolved", "gate_id": "g", "decision": "approved",
         "actor": "user"},
    ]
    checks = _by_id(ev.evaluate("q", events))
    assert checks["joins"]["status"] == "fail"
    assert "ghost_table" in checks["joins"]["explanation"]


def test_chat_records_into_evals_feed(tmp_path):
    client = TestClient(create_app(
        ScriptedRunner(), data=_eval_world(tmp_path)))
    with client.stream("POST", "/chat", json={
            "message": "How many new accounts per month?"}) as r:
        for _ in r.iter_lines():
            pass
    body = client.get("/api/evals/recent").json()
    assert body["summary"]["n_turns"] == 1
    turn = body["turns"][0]
    assert turn["verdict"] == "grounded"
    assert turn["corrections"]
    assert {c["id"] for c in turn["checks"]} >= {
        "citations", "tiers", "seal", "joins", "budget"}


def test_eval_log_rollup(tmp_path):
    log = EvalLog(TurnEvaluator(_eval_world(tmp_path)), maxlen=2)
    turn = _run_turn("How many new accounts per month?")
    log.record("t1", "q1", turn)
    log.record("t2", "q2", turn)
    log.record("t3", "q3", turn)          # ring: t1 evicted
    recent = log.recent()
    assert recent["summary"]["n_turns"] == 2
    assert [t["turn_id"] for t in recent["turns"]] == ["t3", "t2"]
    assert recent["summary"]["grounded_rate"] == 1.0
    assert recent["summary"]["self_corrections"] == 2
