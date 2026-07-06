"""Console backend Phase 0 — the event protocol + scripted loop + SSE.

These pin the CONTRACT the React app renders against: the event types,
their order per intent, the legibility layer (verbs not JSON), the
provenance payload, and the SSE framing. All offline — ScriptedRunner
needs no model or creds.
"""

from __future__ import annotations

import asyncio
import json

from fastapi.testclient import TestClient

from apps.console.backend.app import create_app
from apps.console.backend.events import ToolCall, to_sse
from apps.console.backend.runner import ScriptedRunner
from apps.console.backend.verbs import verb_for


def _collect(message: str, **kw) -> list:
    """Drive the async event stream to completion — no pytest-asyncio dep."""
    async def _run() -> list:
        runner = ScriptedRunner(**kw)
        return [e async for e in runner.stream(message, turn_id="t1")]
    return asyncio.run(_run())


def _types(events) -> list[str]:
    return [e.type for e in events]


# ─── the legibility layer ────────────────────────────────────


def test_verbs_humanize_tools_never_raw_json():
    assert verb_for("inspect_table", {"table_name": "sbs_new_accounts"}) == \
        "Reading the metadata spine for sbs_new_accounts"
    assert "roll_rate_calc" in verb_for("get_lineage",
                                        {"table_name": "roll_rate_calc"})
    # unknown tool degrades to a humanized name, never a traceback
    assert verb_for("some_new_tool") == "Some new tool"
    assert verb_for("inspect_table", {}) == \
        "Reading the metadata spine for the table"


# ─── the three golden flows ──────────────────────────────────


def test_governance_flow_fuses_witnesses_and_answers():
    events = _collect("Who owns sbs_new_accounts and what feeds it?")
    assert _types(events)[0] == "turn_start"
    assert _types(events)[-1] == "turn_end"
    tool_calls = [e for e in events if e.type == "tool_call"]
    assert {tc.tool for tc in tool_calls} == {"inspect_table", "get_lineage"}
    # every tool_call is a human verb, never raw JSON
    assert all(not tc.verb.startswith("{") for tc in tool_calls)
    answer = next(e for e in events if e.type == "answer")
    assert "New Accounts" in answer.sections.answer
    assert answer.sections.citations                 # grounded, cited
    assert answer.sections.status.startswith("grounded")


def test_guardrail_flow_refuses_and_offers_alternative():
    events = _collect("Show me spend by cm11_encrypted")
    result = next(e for e in events if e.type == "tool_result")
    assert result.ok is False                        # the block
    answer = next(e for e in events if e.type == "answer")
    assert "can't" in answer.sections.answer.lower() or \
        "cannot" in answer.sections.answer.lower()
    assert "masked" in answer.sections.answer.lower()  # compliant path
    assert "guardrail" in answer.sections.status.lower()


def test_warehouse_flow_gates_sql_then_charts_when_approved():
    events = _collect("How many new accounts per month?",
                            approve_sql=True)
    gate = next(e for e in events if e.type == "sql_gate")
    assert gate.requires_approval is True
    assert gate.bytes_estimate and gate.bytes_estimate > 0
    assert any("cm11" in c for c in gate.guardrail_checks)
    # approved → sandbox compute + artifact + answer
    assert "sandbox" in _types(events)
    assert "artifact" in _types(events)
    answer = next(e for e in events if e.type == "answer")
    assert "8.4" in answer.sections.answer


def test_warehouse_flow_holds_when_not_approved():
    events = _collect("How many new accounts per month?",
                            approve_sql=False)
    assert "sql_gate" in _types(events)
    assert "artifact" not in _types(events)          # never ran
    answer = next(e for e in events if e.type == "answer")
    assert "approval" in answer.sections.answer.lower()


# ─── SSE framing + endpoints ─────────────────────────────────


def test_to_sse_frames_one_event_per_data_line():
    frame = to_sse(ToolCall(call_id="c", tool="inspect_table",
                            verb="Reading …"))
    assert frame.startswith("data: ")
    assert frame.endswith("\n\n")
    payload = json.loads(frame[len("data: "):].strip())
    assert payload["type"] == "tool_call"


def test_health_reports_active_runner():
    client = TestClient(create_app(ScriptedRunner()))
    body = client.get("/health").json()
    assert body["ok"] is True
    assert body["runner"] == "ScriptedRunner"


def test_chat_endpoint_streams_sse_events_in_order():
    client = TestClient(create_app(ScriptedRunner()))
    with client.stream("POST", "/chat",
                       json={"message": "who owns sbs_new_accounts?"}) as r:
        assert r.status_code == 200
        assert "text/event-stream" in r.headers["content-type"]
        types_seen = []
        for line in r.iter_lines():
            if line.startswith("data: "):
                types_seen.append(json.loads(line[len("data: "):])["type"])
    assert types_seen[0] == "turn_start"
    assert types_seen[-1] == "turn_end"
    assert "answer" in types_seen


def test_approve_endpoint_records_gate_decision():
    app = create_app(ScriptedRunner())
    client = TestClient(app)
    body = client.post("/approve",
                       json={"gate_id": "g1", "approved": True}).json()
    assert body == {"gate_id": "g1", "approved": True}
    assert app.state.gate_decisions["g1"] is True
