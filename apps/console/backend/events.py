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


class Provenance(BaseModel):
    """The trust payload, pre-rendered for a chip: tier + score + who
    said it. Copied from the tool result's envelope so the frontend
    never parses raw graph internals."""

    tier: str                                  # grounded | inferred | guessed | ...
    score: float = Field(ge=0.0, le=1.0)
    sources: list[str] = Field(default_factory=list)
    evidence_count: int = 0


class TurnStart(BaseModel):
    type: Literal["turn_start"] = "turn_start"
    turn_id: str


class Thinking(BaseModel):
    """Model reasoning tokens — surfaced in the trace rail, never in the
    clean conversation. This is the latency-masking channel: while
    Gemini thinks, the user watches it think."""

    type: Literal["thinking"] = "thinking"
    delta: str


class Text(BaseModel):
    """Assistant answer tokens for the clean conversation pane."""

    type: Literal["text"] = "text"
    delta: str


class ToolCall(BaseModel):
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


class ToolResult(BaseModel):
    type: Literal["tool_result"] = "tool_result"
    call_id: str
    ok: bool = True
    summary: str = ""                          # one-line legible result
    provenance: Provenance | None = None
    payload: dict[str, Any] | None = None      # full result for the rail


class SqlGate(BaseModel):
    """The human-in-the-loop moment. The loop PAUSES here; the frontend
    renders an approve/deny beat with the cost + the guardrail checks
    that already passed. Resumed by POST /approve."""

    type: Literal["sql_gate"] = "sql_gate"
    gate_id: str
    sql: str
    bytes_estimate: int | None = None
    guardrail_checks: list[str] = Field(default_factory=list)
    requires_approval: bool = True


class Sandbox(BaseModel):
    """The python sandbox tool, rendered as a notebook-style cell: the
    code the agent wrote + its output. Seeing it COMPUTE (not guess) is
    itself the trust signal."""

    type: Literal["sandbox"] = "sandbox"
    code: str
    stdout: str = ""
    result: Any = None
    ok: bool = True


class Artifact(BaseModel):
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
    citations: list[str] = Field(default_factory=list)
    governance: str = ""
    status: str = ""                           # e.g. "grounded · 3 sources"


class Answer(BaseModel):
    type: Literal["answer"] = "answer"
    sections: AnswerSections


class ErrorEvent(BaseModel):
    """Failures are first-class, never silent (same ethos as the
    enrichment failure digest)."""

    type: Literal["error"] = "error"
    message: str
    recoverable: bool = True


class TurnEnd(BaseModel):
    type: Literal["turn_end"] = "turn_end"
    turn_id: str
    usage: dict[str, Any] = Field(default_factory=dict)


ConsoleEvent = Union[
    TurnStart, Thinking, Text, ToolCall, ToolResult,
    SqlGate, Sandbox, Artifact, Answer, ErrorEvent, TurnEnd,
]


def to_sse(event: BaseModel) -> str:
    """One event → one SSE frame."""
    return f"data: {event.model_dump_json()}\n\n"
