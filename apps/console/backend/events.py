"""The console event protocol — the contract between the agent loop and
the React frontend.

Everything the UI renders is one of these events, streamed over SSE as
`data: {json}\\n\\n`. The frontend is a pure function of this stream:
the clean conversation renders `text`/`answer`; the trace rail renders
`thinking`/`tool_call`/`tool_result`/`sandbox`; the HITL beat renders
`sql_gate`; artifacts embed on `artifact`.

Design rule (why this file exists at all): the agent loop must EMIT
structured, human-legible events — never raw tool JSON. Legibility is a
property of the protocol, not something the frontend reverse-engineers.
So `tool_call` carries a `verb` ("Reading the metadata spine for
sbs_new_accounts"), and `tool_result` carries a rendered `provenance`
envelope, not the raw payload alone.
"""

from __future__ import annotations

from typing import Any, Literal, Union

from pydantic import BaseModel, Field

# The graph's confidence ladder — a CLOSED enum so the frontend can
# exhaustively style every tier (ux-research S1-4: an unstyled tier
# rendered as plain text destroys calibration). Producers that lift
# provenance from raw payloads must fall back to "guessed" rather than
# emit a string outside this set.
ConfidenceTier = Literal[
    "deprecated", "guessed", "inferred", "grounded", "human_asserted",
]


class ConsoleEventBase(BaseModel):
    """Every event carries an optional wall-clock timestamp (ISO 8601).

    The trace is an audit surface, not just a spinner — reviewers need
    'when', and elapsed-time UI needs a base to count from
    (ux-research: timestamps had no typed channel)."""

    ts: str | None = None


class Provenance(BaseModel):
    """The trust payload, pre-rendered for a chip: tier + score + who
    said it. Copied from the tool result's envelope so the frontend
    never parses raw graph internals.

    Chips must be doors, not decorations: the frontend renders these
    fields AND makes the chip clickable through to the witness panel
    (per-source breakdown) — which is why sources/evidence_count ride
    along on every result instead of hiding in the payload."""

    tier: ConfidenceTier
    score: float = Field(ge=0.0, le=1.0)
    sources: list[str] = Field(default_factory=list)
    evidence_count: int = 0


class Citation(BaseModel):
    """A resolvable reference, not a string. `ref` points at a graph
    node (canonical URI) or ledger entry so the UI can open the actual
    evidence — the research's calibration finding: citations that don't
    resolve are ornament."""

    label: str                                 # what the reader sees
    ref: str                                   # synapse://… node URI or ledger:#id


class TurnStart(ConsoleEventBase):
    type: Literal["turn_start"] = "turn_start"
    turn_id: str


class Thinking(ConsoleEventBase):
    """Model reasoning tokens — surfaced in the trace rail, never in the
    clean conversation. This is the latency-masking channel: while
    Gemini thinks, the user watches it think."""

    type: Literal["thinking"] = "thinking"
    delta: str


class Text(ConsoleEventBase):
    """Assistant answer tokens for the clean conversation pane."""

    type: Literal["text"] = "text"
    delta: str


class ToolCall(ConsoleEventBase):
    """A tool invocation, rendered as a human verb — NOT raw JSON.

    `verb` is what a VP reads ("Checking who owns roll_rate_calc");
    `args_summary` is the one-line detail an analyst wants; the full
    args live in `args` for the expandable rail."""

    type: Literal["tool_call"] = "tool_call"
    call_id: str
    tool: str
    verb: str
    args_summary: str = ""
    args: dict[str, Any] = Field(default_factory=dict)


class ToolResult(ConsoleEventBase):
    type: Literal["tool_result"] = "tool_result"
    call_id: str
    ok: bool = True
    summary: str = ""                          # one-line legible result
    provenance: Provenance | None = None
    payload: dict[str, Any] | None = None      # full result for the rail


class SqlGate(ConsoleEventBase):
    """The human-in-the-loop moment. The loop PAUSES here; the frontend
    renders an approve/deny beat with the cost + the guardrail checks
    that already passed. Resumed by POST /approve, after which a
    GateResolved event ALWAYS follows — a gate never just vanishes."""

    type: Literal["sql_gate"] = "sql_gate"
    gate_id: str
    sql: str
    bytes_estimate: int | None = None
    guardrail_checks: list[str] = Field(default_factory=list)
    requires_approval: bool = True


class GateResolved(ConsoleEventBase):
    """The gate's outcome as a first-class, typed event (ux-research
    S1-3: approver, timestamp, and ledger id had no channel — the audit
    story lived only in prose). `held` is a live state, not a dead end:
    the SQL stays approvable and the frontend must keep that affordance."""

    type: Literal["gate_resolved"] = "gate_resolved"
    gate_id: str
    decision: Literal["approved", "held"]
    actor: str = "user"                        # who decided (audit line)
    ledger_id: str | None = None               # warehouse audit-ledger ref
    rows_returned: int | None = None           # set on approved+executed


class Sandbox(ConsoleEventBase):
    """The python sandbox tool, rendered as a notebook-style cell: the
    code the agent wrote + its output. Seeing it COMPUTE (not guess) is
    itself the trust signal."""

    type: Literal["sandbox"] = "sandbox"
    code: str
    stdout: str = ""
    result: Any = None
    ok: bool = True


class Artifact(ConsoleEventBase):
    """A rendered chart/dashboard — self-contained HTML the frontend
    embeds in a sandboxed iframe."""

    type: Literal["artifact"] = "artifact"
    artifact_id: str
    kind: Literal["chart", "dashboard"]
    title: str = ""
    html: str


class AnswerSections(BaseModel):
    """The answer contract the agent always emits — each field a distinct
    visual block. A VP reads `answer`; an analyst expands the rest."""

    answer: str
    how_i_got_there: str = ""
    citations: list[Citation] = Field(default_factory=list)
    governance: str = ""
    status: str = ""                           # e.g. "grounded · 3 sources"


class Answer(ConsoleEventBase):
    """RECONCILIATION RULE (ux-research S0-2): `answer` SUPERSEDES the
    turn's streamed `text` deltas. The frontend replaces the streamed
    text block with this card in place — never renders both. Streamed
    text is the in-flight rendering; Answer is its final form."""

    type: Literal["answer"] = "answer"
    sections: AnswerSections


class ErrorEvent(ConsoleEventBase):
    """Failures are first-class, never silent (same ethos as the
    enrichment failure digest)."""

    type: Literal["error"] = "error"
    message: str
    recoverable: bool = True


class TurnEnd(ConsoleEventBase):
    type: Literal["turn_end"] = "turn_end"
    turn_id: str
    usage: dict[str, Any] = Field(default_factory=dict)


ConsoleEvent = Union[
    TurnStart, Thinking, Text, ToolCall, ToolResult,
    SqlGate, GateResolved, Sandbox, Artifact, Answer, ErrorEvent, TurnEnd,
]


def to_sse(event: BaseModel) -> str:
    """One event → one SSE frame.

    Serialization must NEVER kill the stream: `args`/`payload`/`result`
    are typed Any, and a live tool result can smuggle in a value pydantic
    can't serialize (protobuf composite, Decimal, bytes). One bad event
    used to raise inside the StreamingResponse generator and hang the UI
    mid-turn. Fall back to a stringifying dump — degraded is legible,
    dead is not."""
    import json

    try:
        body = event.model_dump_json()
    except Exception:
        try:
            body = json.dumps(event.model_dump(mode="python"), default=str)
        except Exception as exc:  # last resort: a legible error event
            body = json.dumps({
                "type": "error", "recoverable": True,
                "message": f"event serialization failed: {exc}"[:300]})
    return f"data: {body}\n\n"
