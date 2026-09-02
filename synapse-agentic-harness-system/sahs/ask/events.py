"""The event stream: the single source of UI truth for Ask (E18).

Same envelope as every other Meridian surface (``meridian.event/1``,
keyed by ``ev``), carrying ``session_id``/``turn_id`` where the batch
pipeline carries ``run_id``. The events FILE is the record: replay is
re-consuming it, so a reconnecting browser sees the same turn it
would have seen live.

The bus is deliberately poll-based rather than callback-based: the
turn pipeline runs in a worker thread (the resolver is CPU work, the
model calls are blocking HTTP) while SSE consumers live on the event
loop. A lock-protected append log that consumers read by sequence
number is thread-safe by construction, needs no loop plumbing, and
loses nothing on reconnect.
"""

from __future__ import annotations

import datetime as _dt
import json
import threading
from pathlib import Path
from typing import Any

SCHEMA = "meridian.event/1"

# the pinned event family for a turn (E18; loop events added by Agent
# Loop v1 §2). Order here is the order a healthy governed turn emits
# them; clarify_request and error are the two legitimate early exits.
# The loop_* four fire only when navigation engages: each model step
# emits loop_step (the ≤3-line summary the model kept) and
# loop_artifact (the full tool result), so the events file IS the
# trajectory and the "what the model saw" panel re-reads it.
EVENTS: tuple[str, ...] = (
    "turn_started",
    "classify_result",
    "plan_delta",
    "resolve_started",
    "resolve_result",
    "loop_started",
    "loop_prompt",
    "loop_step",
    "loop_artifact",
    "loop_done",
    "clarify_request",
    "contract_ready",
    "generate_token",
    "verify_progress",
    "verify_verdict",
    "answer_payload",
    "notebook_artifact",
    "budget_tick",
    "budget_grace",
    "turn_done",
    "error",
)


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


class EventBus:
    """Append-only event log for one session, readable by sequence.

    ``emit`` is safe from any thread; ``since`` is safe from the event
    loop. Every event carries a monotonic ``seq`` so SSE can resume
    from ``Last-Event-ID`` without gaps or repeats.
    """

    def __init__(self, session_id: str, path: Path | None = None,
                 keep: int = 4000,
                 events: tuple[str, ...] = EVENTS) -> None:
        self.session_id = session_id
        self.path = path
        # the family this bus enforces: the ask surface's by default;
        # the v2 assistant passes its own. Still pinned per surface.
        self._family = events
        self._keep = keep
        self._lock = threading.Lock()
        self._events: list[dict[str, Any]] = []
        self._seq = 0
        self._closed_turns: set[str] = set()
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, ev: str, *, turn_id: str = "",
             **fields: Any) -> dict[str, Any]:
        if ev not in self._family:
            raise ValueError(f"unregistered event {ev!r}: the family is "
                             "pinned so the UI can be a pure consumer")
        with self._lock:
            self._seq += 1
            record: dict[str, Any] = {
                "schema": SCHEMA, "ts": now_iso(), "seq": self._seq,
                "session_id": self.session_id, "ev": ev,
            }
            if turn_id:
                record["turn_id"] = turn_id
            record.update({k: v for k, v in fields.items() if v is not None})
            self._events.append(record)
            if ev == "turn_done" and turn_id:
                self._closed_turns.add(turn_id)
            if len(self._events) > self._keep:
                del self._events[: len(self._events) - self._keep]
        if self.path is not None:
            # the record on disk: append-only, one JSON object per line
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        return record

    def since(self, seq: int) -> list[dict[str, Any]]:
        with self._lock:
            return [e for e in self._events if e["seq"] > seq]

    def head(self) -> int:
        with self._lock:
            return self._seq

    def turn_closed(self, turn_id: str) -> bool:
        with self._lock:
            return turn_id in self._closed_turns


def sse_frame(record: dict[str, Any]) -> str:
    """One server-sent event: id for resume, event for routing, data."""
    return (f"id: {record['seq']}\n"
            f"event: {record['ev']}\n"
            f"data: {json.dumps(record, ensure_ascii=False)}\n\n")
