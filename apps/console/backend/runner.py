"""The agent loop, as an async event stream.

Two implementations of one protocol:

  ScriptedRunner  — offline, deterministic. Emits a realistic golden
                    transcript per canned intent. This is what the React
                    app develops against, what the tests pin, and the
                    living spec of "what a turn looks like." No model,
                    no creds, runs anywhere.

  ADKRunner       — real Gemini 3.1 Pro on Vertex via google-adk,
                    consuming build_analyst_tools() (the same
                    GraphService/warehouse/render/sandbox tools the MCP
                    server exposes). Laptop-only (needs Vertex). Maps
                    ADK's event stream onto ConsoleEvents.

The frontend cannot tell them apart — that is the point. Build and demo
the whole UI on ScriptedRunner; flip to ADKRunner when creds are live.
"""

from __future__ import annotations

from typing import AsyncIterator, Protocol

from apps.console.backend.events import (
    Answer, AnswerSections, Artifact, Citation, ConsoleEvent, ErrorEvent,
    GateResolved, Provenance, SqlGate, Sandbox, Text, Thinking, ToolCall,
    ToolResult, TurnEnd, TurnStart,
)
from apps.console.backend.verbs import verb_for


class Runner(Protocol):
    def stream(self, user_message: str, *, turn_id: str,
               conversation_id: str | None = None,
               ) -> AsyncIterator[ConsoleEvent]: ...


# ─── offline golden transcripts ──────────────────────────────


import re


def _classify(message: str) -> str:
    m = message.lower()
    if any(w in m for w in ("cm11", "encrypted", "pii", "raw ")):
        return "guardrail"
    # phrases are safe as substrings; single words need boundaries so
    # "count" doesn't match inside "account(s)"
    if (any(p in m for p in ("how many", "how much", "per month",
                             "trend", "chart"))
            or re.search(r"\b(count|query|run|execute)\b", m)):
        return "warehouse"
    return "governance"


class ScriptedRunner:
    """Deterministic golden transcripts — the frontend's development
    fixture and the loop's behavioral spec."""

    def __init__(self, *, approve_sql: bool = True) -> None:
        # in a real run the frontend resolves the gate; the scripted
        # runner takes the decision up front so tests are deterministic
        self.approve_sql = approve_sql

    async def stream(self, user_message: str, *, turn_id: str,
                     conversation_id: str | None = None,
                     ) -> AsyncIterator[ConsoleEvent]:
        yield TurnStart(turn_id=turn_id)
        intent = _classify(user_message)
        if intent == "guardrail":
            async for e in self._guardrail_flow():
                yield e
        elif intent == "warehouse":
            async for e in self._warehouse_flow():
                yield e
        else:
            async for e in self._governance_flow():
                yield e
        yield TurnEnd(turn_id=turn_id, usage={"runner": "scripted"})

    # the fused-witness answer: one question, three sources
    async def _governance_flow(self) -> AsyncIterator[ConsoleEvent]:
        yield Thinking(delta="This needs ownership, the feeding pipeline, "
                       "and who queries it — governance + lineage + usage.")
        yield ToolCall(call_id="c1", tool="inspect_table",
                       verb=verb_for("inspect_table",
                                     {"table_name": "sbs_new_accounts"}),
                       args_summary="sbs_new_accounts",
                       args={"table_name": "sbs_new_accounts"})
        yield ToolResult(call_id="c1", summary="Owner: New Accounts (SBS); "
                         "lifecycle: active",
                         provenance=Provenance(tier="grounded", score=0.9,
                                               sources=["mdm", "skills"],
                                               evidence_count=4))
        yield ToolCall(call_id="c2", tool="get_lineage",
                       verb=verb_for("get_lineage",
                                     {"table_name": "sbs_new_accounts"}),
                       args={"table_name": "sbs_new_accounts"})
        yield ToolResult(call_id="c2",
                         summary="Fed by the new-accounts approval pipeline",
                         provenance=Provenance(tier="grounded", score=0.8,
                                               sources=["mdm"],
                                               evidence_count=2))
        yield Text(delta="New Accounts (SBS) owns sbs_new_accounts; ")
        yield Text(delta="it's fed by the approval pipeline.")
        yield Answer(sections=AnswerSections(
            answer="**New Accounts (SBS)** owns `sbs_new_accounts`, fed by "
                   "the new-accounts approval pipeline.",
            how_i_got_there="Read the MDM metadata spine for ownership + "
                            "lifecycle, then traced lineage for the feeding "
                            "pipeline.",
            citations=[
                Citation(label="mdm:ownership",
                         ref="synapse://table/sbs_new_accounts"),
                Citation(label="mdm:lineage",
                         ref="synapse://table/sbs_new_accounts"),
            ],
            governance="No PII columns surfaced in this answer.",
            status="grounded · 2 sources"))

    # the trust moment: ask the forbidden thing on purpose
    async def _guardrail_flow(self) -> AsyncIterator[ConsoleEvent]:
        yield Thinking(delta="They're asking for cm11 at cardmember grain — "
                       "the skill guardrail forbids exposing cm11_encrypted.")
        yield ToolCall(call_id="c1", tool="validate_sql_plan",
                       verb=verb_for("validate_sql_plan"),
                       args_summary="exposes cm11_encrypted")
        yield ToolResult(call_id="c1", ok=False,
                         summary="BLOCKED: cm11_encrypted exposure violates "
                                 "the RollRates skill guardrail",
                         provenance=Provenance(tier="grounded", score=1.0,
                                               sources=["skills"],
                                               evidence_count=1))
        yield Answer(sections=AnswerSections(
            answer="I can't expose `cm11_encrypted` at cardmember grain — "
                   "it's blocked by a governance guardrail. I can give you "
                   "the same analysis on the **masked** account key instead.",
            how_i_got_there="Statically validated the plan; the RollRates "
                            "skill marks cm11_encrypted as never-expose.",
            citations=[Citation(label="skill:SBS_RollRates/guardrail#cm11",
                                ref="synapse://guardrail/sbs_rollrates/cm11")],
            governance="Refused: PII exposure. Compliant alternative offered.",
            status="guardrail enforced"))

    # the live-number flow: gate → cost → approve → chart
    async def _warehouse_flow(self) -> AsyncIterator[ConsoleEvent]:
        yield Thinking(delta="A live count — draft SQL, validate, dry-run "
                       "for cost, then ask before running.")
        yield ToolCall(call_id="c1", tool="find_columns_for_concept",
                       verb=verb_for("find_columns_for_concept",
                                     {"concept": "new accounts by month"}),
                       args={"concept": "new accounts by month"})
        yield ToolResult(call_id="c1",
                         summary="acct_open_dt, acct_id on sbs_new_accounts",
                         provenance=Provenance(tier="grounded", score=0.85,
                                               sources=["bq", "mdm"],
                                               evidence_count=3))
        yield SqlGate(
            gate_id="g1",
            sql="SELECT DATE_TRUNC(acct_open_dt, MONTH) m, COUNT(*) n\n"
                "FROM sbs_new_accounts GROUP BY 1 ORDER BY 1",
            bytes_estimate=1_240_000_000,
            guardrail_checks=["read-only ✓", "no cm11 exposure ✓",
                              "row cap 100 ✓"])
        if not self.approve_sql:
            yield GateResolved(gate_id="g1", decision="held", actor="user")
            yield Answer(sections=AnswerSections(
                answer="Holding — the query is drafted and validated, "
                       "awaiting your approval to run.",
                status="awaiting approval"))
            return
        yield GateResolved(gate_id="g1", decision="approved", actor="user",
                           ledger_id="4821", rows_returned=8)
        yield Sandbox(code="pct = (n[-1]-n[-2])/n[-2]*100",
                      stdout="", result={"mom_pct": 8.4}, ok=True)
        yield Artifact(artifact_id="a1", kind="chart",
                       title="New accounts by month",
                       html="<div style='font:14px system-ui'>"
                            "[chart: new accounts by month]</div>")
        yield Answer(sections=AnswerSections(
            answer="New accounts are up **8.4% MoM**, most recent full month "
                   "leading.",
            how_i_got_there="Resolved the columns, validated + dry-ran the "
                            "SQL (1.24 GB), ran it row-capped, computed the "
                            "delta in the sandbox, charted it.",
            citations=[
                Citation(label="bq:sbs_new_accounts",
                         ref="synapse://table/sbs_new_accounts"),
                Citation(label="ledger#4821", ref="ledger:#4821"),
            ],
            governance="Read-only, row-capped, on the audit ledger.",
            status="grounded · live query"))


# ─── real Gemini/Vertex runner (laptop) ──────────────────────


class ADKRunner:
    """Real Gemini 3.1 Pro via google-adk over the analyst tools.

    Lazy-imports ADK so the offline path (ScriptedRunner + tests) never
    needs Vertex. Maps ADK's event stream to ConsoleEvents. The mapping
    is intentionally thin — ADK already gives function_call /
    function_response events; we humanize them via verb_for and lift the
    provenance envelope out of each tool result."""

    def __init__(self, *, model: str | None = None) -> None:
        self._model = model
        self._runner = None
        self._session_service = None
        self._sessions: set[str] = set()      # conversation memory

    def _ensure(self):
        if self._runner is not None:
            return
        import os

        from google.adk import Runner
        from google.adk.sessions import InMemorySessionService

        from apps.analyst.agent import build_agent
        self._session_service = InMemorySessionService()
        agent = build_agent(
            self._model or os.environ.get("GEMINI_MODEL",
                                          "gemini-3.1-pro-preview"))
        self._runner = Runner(
            app_name="synapse_console", agent=agent,
            session_service=self._session_service)

    async def stream(self, user_message: str, *, turn_id: str,
                     conversation_id: str | None = None,
                     ) -> AsyncIterator[ConsoleEvent]:
        yield TurnStart(turn_id=turn_id)
        session_id = conversation_id or turn_id
        try:
            self._ensure()
            from google.genai import types
            content = types.Content(
                role="user", parts=[types.Part(text=user_message)])
            # one ADK session per CONVERSATION, created once and reused —
            # follow-up turns see prior tool results and answers
            if session_id not in self._sessions:
                await self._session_service.create_session(
                    app_name="synapse_console", user_id="console",
                    session_id=session_id)
                self._sessions.add(session_id)
            async for ev in self._runner.run_async(
                    user_id="console", session_id=session_id,
                    new_message=content):
                for out in _map_adk_event(ev):
                    yield out
        except Exception as exc:  # never fail silently — and fail legibly
            yield ErrorEvent(message=_explain_failure(exc),
                             recoverable=True)
        yield TurnEnd(turn_id=turn_id, usage={"runner": "adk"})


def _explain_failure(exc: Exception) -> str:
    """Map raw Vertex/network failures to the action that fixes them.
    The raw error rides along — legible never means lossy."""
    raw = f"{type(exc).__name__}: {exc}"
    msg = str(exc).lower()
    # adk/genai version mismatch: adk's pydantic models evaluate their
    # annotations against the INSTALLED genai's `types` module, so an
    # old genai next to a newer adk dies at import with shape-shifting
    # errors — "unsupported operand type(s) for |: 'function' and
    # 'NoneType'", "module 'google.genai.types' has no attribute …",
    # or a ValidationError on thinking/generate_content_config.
    mismatch = (
        ("unsupported operand" in msg and "|" in msg)
        or "google.genai.types" in msg
        or ("has no attribute" in msg and "genai" in msg)
        or ("validation error" in msg and
            ("thinking" in msg or "generatecontentconfig" in msg))
    )
    if mismatch:
        return ("The google-adk and google-genai versions this server "
                "loaded don't match. Install a certified pair in the "
                "SAME venv the server runs from — "
                "pip install 'google-adk==1.31.1' 'google-genai==1.73.1' "
                "— then relaunch with python -m uvicorn (a bare `uvicorn` "
                "can resolve to a different environment). Check what the "
                f"server actually sees at /api/config. [{raw}]")
    if "certificate_verify_failed" in msg or "ssl" in msg:
        return ("The secure connection to Vertex was intercepted "
                "(corporate proxy). Set GEMINI_TLS_INSECURE=1 on the "
                "intranet, or GEMINI_CA_BUNDLE to your CA file, then "
                f"restart the console server. [{raw}]")
    if "permission" in msg or "403" in msg or "credential" in msg \
            or "unauthenticated" in msg or "401" in msg:
        return ("Vertex declined the credentials. Check "
                "GOOGLE_APPLICATION_CREDENTIALS points at the service-"
                "account key and the account has Vertex AI access. "
                f"[{raw}]")
    if "resource_exhausted" in msg or "429" in msg or "quota" in msg:
        return ("Vertex is rate-limiting this project right now — retry "
                f"in a moment; nothing is lost. [{raw}]")
    if "not found" in msg and "model" in msg:
        return ("The configured model is not available to this project. "
                f"Check GEMINI_MODEL. [{raw}]")
    if isinstance(exc, (ConnectionError, TimeoutError)) \
            or "timed out" in msg or "connection" in msg:
        return ("Could not reach Vertex — check the VPN/network and "
                f"retry. [{raw}]")
    return raw


def _map_adk_event(ev) -> list[ConsoleEvent]:
    """ADK event → zero or more ConsoleEvents. Kept defensive: ADK's
    surface shifts across versions, so every access is guarded."""
    out: list[ConsoleEvent] = []
    content = getattr(ev, "content", None)
    parts = getattr(content, "parts", None) or []
    for part in parts:
        fc = getattr(part, "function_call", None)
        if fc is not None:
            args = dict(getattr(fc, "args", {}) or {})
            out.append(ToolCall(
                call_id=getattr(fc, "id", "") or getattr(fc, "name", ""),
                tool=getattr(fc, "name", "tool"),
                verb=verb_for(getattr(fc, "name", "tool"), args),
                args=args))
            continue
        fr = getattr(part, "function_response", None)
        if fr is not None:
            resp = getattr(fr, "response", {}) or {}
            out.append(ToolResult(
                call_id=getattr(fr, "id", "") or getattr(fr, "name", ""),
                ok=not bool(resp.get("error")),
                summary=_summarize_payload(resp),
                provenance=_lift_provenance(resp),
                payload=resp if isinstance(resp, dict) else None))
            continue
        text = getattr(part, "text", None)
        if text:
            # thought parts stream into the work log, not the answer —
            # watching the model reason is the latency-masking channel
            if getattr(part, "thought", False):
                out.append(Thinking(delta=text))
            else:
                out.append(Text(delta=text))
    return out


def _summarize_payload(resp: dict) -> str:
    """One legible line per tool result. Real tools rarely ship a
    `summary` field — derive one from the payload's shape instead of
    rendering an empty row in the work log."""
    if not isinstance(resp, dict):
        return str(resp)[:200]
    if resp.get("summary"):
        return str(resp["summary"])[:200]
    err = resp.get("error")
    if err:
        msg = err.get("message") if isinstance(err, dict) else err
        return f"✗ {str(msg)[:180]}"
    data = resp.get("data") if isinstance(resp.get("data"), dict) else resp
    rows = data.get("rows")
    if isinstance(rows, list):
        return f"{len(rows)} row(s) returned"
    for key in ("total_bytes_processed", "bytes_processed",
                "bytes_estimate"):
        if isinstance(data.get(key), (int, float)):
            gb = data[key] / 1e9
            return f"dry run OK · ~{gb:.2f} GB scan"
    status = resp.get("status")
    for key in ("columns", "items", "matches", "hits", "tables",
                "paths", "identified_by"):
        if isinstance(data.get(key), list):
            return f"{status or 'ok'} · {len(data[key])} {key}"
    if status:
        return str(status)[:200]
    return ""


def _lift_provenance(resp: dict) -> Provenance | None:
    env = resp.get("provenance") or resp.get("_envelope") or {}
    if not isinstance(env, dict) or not env:
        return None
    # ConfidenceTier is a CLOSED enum — an off-ladder string from a raw
    # payload degrades to "guessed" (the honest floor) instead of
    # discarding the whole envelope and losing sources/evidence
    tier = str(env.get("tier", env.get("confidence_tier", "guessed")))
    if tier not in ("deprecated", "guessed", "inferred", "grounded",
                    "human_asserted"):
        tier = "guessed"
    try:
        return Provenance(
            tier=tier,  # type: ignore[arg-type]
            score=max(0.0, min(1.0, float(
                env.get("score", env.get("confidence_score", 0.0))))),
            sources=list(env.get("sources", []) or []),
            evidence_count=int(env.get("evidence_count", 0) or 0))
    except Exception:
        return None
