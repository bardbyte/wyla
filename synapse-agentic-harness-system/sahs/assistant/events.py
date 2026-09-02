"""The assistant's event stream (Synapse v2 §2/§10).

Same envelope as everything else (``meridian.event/1``, the ask
surface's ``EventBus`` reused verbatim) with the v2 turn's own type
family. The stream is still the single source of UI truth: the chat
page and the artifact panel are pure consumers, replay equals live,
and the "what the model saw" record rides along as it did in v1.
"""

from __future__ import annotations

from sahs.ask.events import EventBus, now_iso, sse_frame  # noqa: F401

# the pinned family for one assistant turn, in the order a healthy
# turn emits them. There is no classify, no resolve_started, and no
# contract gate — the model drives; the harness streams and records.
ASSISTANT_EVENTS: tuple[str, ...] = (
    "turn_started",
    "model_prompt",       # what the model saw: system once, then steps
    "tool_step",          # one look: tool, args, the compact summary
    "tool_result",        # the full result behind the summary
    "say_token",          # streamed assistant prose
    "artifact",           # an artifact created/updated: full spec rides
    "chips",              # follow-up suggestions when the turn ends
    "budget_tick",
    "turn_done",
    "error",
)
