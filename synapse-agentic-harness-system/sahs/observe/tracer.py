"""Event record → trace: the translator, and the fake it is tested on.

One assistant turn is one trace. Each model call is a generation
whose usage is the delta between the budget ticks around it; each
tool call is a span; artifacts and proposals are events; turn_done
closes the trace and scores its status. The tracer keeps only what
an open turn needs and forgets it on turn_done.

Tool results carry real warehouse rows: the span keeps the compact
summary unless ``full_results`` is on.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

TRACE_NAME = "assistant.turn"
CLIP = 4000


class Emitter(Protocol):
    """What the tracer asks of a backend. Observation handles live
    behind ``(trace_id, key)`` so the translator stays stateless
    about the SDK."""

    def trace_open(self, trace_id: str, *, name: str, session_id: str,
                   user_id: str, input: Any, metadata: dict[str, Any],
                   tags: list[str]) -> None: ...

    def generation_open(self, trace_id: str, key: str, *, name: str,
                        model: str, input: Any, metadata: dict[str, Any],
                        prompt: tuple[str, int] | None) -> None: ...

    def generation_close(self, trace_id: str, key: str, *, output: Any,
                         usage: dict[str, int] | None,
                         metadata: dict[str, Any]) -> None: ...

    def span_open(self, trace_id: str, key: str, *, name: str, input: Any,
                  metadata: dict[str, Any]) -> None: ...

    def span_close(self, trace_id: str, key: str, *, output: Any,
                   metadata: dict[str, Any], level: str) -> None: ...

    def event(self, trace_id: str, *, name: str,
              metadata: dict[str, Any]) -> None: ...

    def score(self, trace_id: str, *, name: str, value: Any,
              comment: str, data_type: str) -> None: ...

    def trace_close(self, trace_id: str, *, output: Any,
                    metadata: dict[str, Any], level: str,
                    status_message: str) -> None: ...

    def flush(self) -> None: ...


class Recorder:
    """The emitter the tests read: every call, in order, as data."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _note(self, method: str, **kw: Any) -> None:
        self.calls.append((method, kw))

    def trace_open(self, trace_id: str, **kw: Any) -> None:
        self._note("trace_open", trace_id=trace_id, **kw)

    def generation_open(self, trace_id: str, key: str, **kw: Any) -> None:
        self._note("generation_open", trace_id=trace_id, key=key, **kw)

    def generation_close(self, trace_id: str, key: str, **kw: Any) -> None:
        self._note("generation_close", trace_id=trace_id, key=key, **kw)

    def span_open(self, trace_id: str, key: str, **kw: Any) -> None:
        self._note("span_open", trace_id=trace_id, key=key, **kw)

    def span_close(self, trace_id: str, key: str, **kw: Any) -> None:
        self._note("span_close", trace_id=trace_id, key=key, **kw)

    def event(self, trace_id: str, **kw: Any) -> None:
        self._note("event", trace_id=trace_id, **kw)

    def score(self, trace_id: str, **kw: Any) -> None:
        self._note("score", trace_id=trace_id, **kw)

    def trace_close(self, trace_id: str, **kw: Any) -> None:
        self._note("trace_close", trace_id=trace_id, **kw)

    def flush(self) -> None:
        self._note("flush")

    def of(self, method: str) -> list[dict[str, Any]]:
        return [kw for m, kw in self.calls if m == method]


def trace_id_for(session_id: str, turn_id: str) -> str:
    """Deterministic, 32 hex chars (what Langfuse accepts): the same
    turn always maps to the same trace, so a backfill overwrites
    instead of duplicating."""
    return hashlib.sha256(f"{session_id}/{turn_id}".encode()).hexdigest()[:32]


def _clip(value: Any, cap: int = CLIP) -> Any:
    if isinstance(value, str) and len(value) > cap:
        return value[:cap] + f"… [{len(value) - cap} more chars]"
    return value


_ENVELOPE = ("schema", "ts", "seq", "session_id", "ev", "turn_id")
_ZERO_TICK = {"tokens_in": 0, "tokens_out": 0, "tokens": 0}


@dataclass
class _Turn:
    key: str
    trace_id: str
    session_id: str
    model: str
    prompt: tuple[str, int] | None = None
    system: str = ""
    gen_key: str = ""
    gen_text: list[str] = field(default_factory=list)
    gen_thought: list[str] = field(default_factory=list)
    prose: list[str] = field(default_factory=list)
    open_tools: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class TurnTracer:
    def __init__(self, emitter: Emitter, *, user_id: str = "",
                 model_of: Callable[[str], str] | None = None,
                 prompt_of: Callable[[str], tuple[str, int] | None] | None
                 = None,
                 full_results: bool = False,
                 environment: str = "") -> None:
        self.emitter = emitter
        self.user_id = user_id
        self.model_of = model_of
        # version string → (registered prompt name, Langfuse version):
        # the link a generation carries; None when unregistered
        self.prompt_of = prompt_of
        self.full_results = full_results
        self.environment = environment
        self._turns: dict[str, _Turn] = {}
        self._last_tick: dict[str, dict[str, int]] = {}

    # the bus calls the tracer with each record
    def __call__(self, record: dict[str, Any]) -> None:
        self.handle(record)

    def handle(self, record: dict[str, Any]) -> None:
        ev = record.get("ev")
        turn_id = record.get("turn_id", "")
        session_id = record.get("session_id", "")
        key = f"{session_id}/{turn_id}"
        if ev == "turn_started":
            self._start(key, session_id, turn_id, record)
            return
        turn = self._turns.get(key)
        if turn is None:
            if ev == "budget_tick":
                self._last_tick[session_id] = self._tick(record)
            return                    # a turn we did not see start
        handler = getattr(self, f"_on_{ev}", None)
        if handler is not None:
            handler(turn, record)

    def flush(self) -> None:
        self.emitter.flush()

    # ── the handlers, in the order a healthy turn emits them ──
    def _start(self, key: str, session_id: str, turn_id: str,
               record: dict[str, Any]) -> None:
        plane = str(record.get("plane") or "")
        model = plane
        if self.model_of is not None:
            try:
                model = str(self.model_of(plane) or plane)
            except Exception:                 # noqa: BLE001
                model = plane
        turn = _Turn(key=key, trace_id=trace_id_for(session_id, turn_id),
                     session_id=session_id, model=model)
        if self.prompt_of is not None:
            try:
                turn.prompt = self.prompt_of(str(record.get("version") or ""))
            except Exception:                 # noqa: BLE001
                turn.prompt = None
        self._turns[key] = turn
        metadata = {k: v for k, v in record.items()
                    if k not in _ENVELOPE and k != "text"}
        metadata["turn_id"] = turn_id
        metadata["model"] = model
        tags = ["assistant"]
        if plane:
            tags.append(f"plane:{plane}")
        if record.get("mode"):
            tags.append(f"mode:{record['mode']}")
        tags += [f"skill:{s}" for s in record.get("skills") or []]
        if self.environment:
            tags.append(f"env:{self.environment}")
        self.emitter.trace_open(
            turn.trace_id, name=TRACE_NAME, session_id=session_id,
            user_id=self.user_id, input=record.get("text", ""),
            metadata=metadata, tags=tags)

    def _on_model_prompt(self, turn: _Turn, record: dict[str, Any]) -> None:
        if record.get("kind") == "system":
            turn.system = str(record.get("content") or "")
            return
        n = record.get("n", 0)
        self._close_generation(turn, usage=None, dangling=True)
        turn.gen_key = f"gen{n}"
        turn.gen_text, turn.gen_thought = [], []
        payload: dict[str, Any] = {"contents": _clip(record.get("content"))}
        if n == 1 and turn.system:
            payload["system"] = _clip(turn.system, 12000)
        self.emitter.generation_open(
            turn.trace_id, turn.gen_key, name=f"model call {n}",
            model=turn.model, input=payload, metadata={"n": n},
            prompt=turn.prompt)

    def _on_thinking(self, turn: _Turn, record: dict[str, Any]) -> None:
        turn.gen_thought.append(str(record.get("delta") or ""))

    def _on_say_token(self, turn: _Turn, record: dict[str, Any]) -> None:
        delta = str(record.get("delta") or "")
        turn.gen_text.append(delta)
        turn.prose.append(delta)

    def _on_budget_tick(self, turn: _Turn, record: dict[str, Any]) -> None:
        tick = self._tick(record)
        before = self._last_tick.get(turn.session_id, _ZERO_TICK)
        usage = {"input": tick["tokens_in"] - before["tokens_in"],
                 "output": tick["tokens_out"] - before["tokens_out"],
                 "total": tick["tokens"] - before["tokens"]}
        self._last_tick[turn.session_id] = tick
        self._close_generation(
            turn, usage=usage,
            metadata={"cost_usd_session": record.get("cost_usd"),
                      "turn_tokens": record.get("turn_tokens")})

    def _on_tool_call(self, turn: _Turn, record: dict[str, Any]) -> None:
        key = f"tool{record.get('n', 0)}"
        turn.open_tools[key] = {}
        self.emitter.span_open(
            turn.trace_id, key, name=str(record.get("tool") or "tool"),
            input={"args": record.get("args"), "input": record.get("input")},
            metadata={"n": record.get("n")})

    def _on_tool_step(self, turn: _Turn, record: dict[str, Any]) -> None:
        key = f"tool{record.get('n', 0)}"
        if key in turn.open_tools:
            turn.open_tools[key] = {
                "summary": record.get("summary", ""),
                "ref": record.get("ref"),
                "elapsed_ms": record.get("elapsed_ms")}

    def _on_tool_result(self, turn: _Turn, record: dict[str, Any]) -> None:
        ref = record.get("ref")
        key = next((k for k, v in turn.open_tools.items()
                    if v.get("ref") == ref), None)
        if key is None:
            return
        step = turn.open_tools.pop(key)
        summary = str(step.get("summary") or "")
        output: dict[str, Any] = {"summary": summary}
        if self.full_results:
            output["content"] = _clip(record.get("content"), 20000)
        self.emitter.span_close(
            turn.trace_id, key, output=output,
            metadata={"ref": ref, "elapsed_ms": step.get("elapsed_ms")},
            level="ERROR" if summary.lower().startswith("error")
            else "DEFAULT")

    def _on_artifact(self, turn: _Turn, record: dict[str, Any]) -> None:
        self.emitter.event(
            turn.trace_id, name=f"artifact:{record.get('type', '')}",
            metadata={k: record.get(k) for k in
                      ("artifact_id", "version", "title")})

    def _on_proposal(self, turn: _Turn, record: dict[str, Any]) -> None:
        self.emitter.event(
            turn.trace_id, name="proposal",
            metadata={k: _clip(v, 2000) for k, v in record.items()
                      if k not in _ENVELOPE})

    def _on_error(self, turn: _Turn, record: dict[str, Any]) -> None:
        if record.get("code") == "trace":
            return                            # the traceback tail: noise
        turn.errors.append(
            f"{record.get('code', 'error')}: {record.get('message', '')}")

    def _on_turn_done(self, turn: _Turn, record: dict[str, Any]) -> None:
        self._close_generation(turn, usage=None, dangling=True)
        for key in list(turn.open_tools):
            turn.open_tools.pop(key)
            self.emitter.span_close(
                turn.trace_id, key, output={"summary": "no result seen"},
                metadata={}, level="WARNING")
        status = str(record.get("status") or "")
        metadata = {k: v for k, v in record.items() if k not in _ENVELOPE}
        if turn.errors:
            metadata["errors"] = turn.errors
        level = "ERROR" if status == "error" or turn.errors else "DEFAULT"
        self.emitter.trace_close(
            turn.trace_id, output="".join(turn.prose), metadata=metadata,
            level=level, status_message=turn.errors[0] if turn.errors
            else "")
        self.emitter.score(turn.trace_id, name="turn_status", value=status,
                           comment="", data_type="CATEGORICAL")
        self._turns.pop(turn.key, None)

    # ── helpers ──────────────────────────────────────────────
    @staticmethod
    def _tick(record: dict[str, Any]) -> dict[str, int]:
        return {k: int(record.get(k) or 0) for k in _ZERO_TICK}

    def _close_generation(self, turn: _Turn, *, usage: dict | None,
                          metadata: dict[str, Any] | None = None,
                          dangling: bool = False) -> None:
        if not turn.gen_key:
            return
        meta = dict(metadata or {})
        if dangling:
            meta["closed_by"] = "next event"
        output: dict[str, Any] = {"text": "".join(turn.gen_text)}
        if turn.gen_thought:
            output["thought"] = _clip("".join(turn.gen_thought))
        self.emitter.generation_close(turn.trace_id, turn.gen_key,
                                      output=output, usage=usage,
                                      metadata=meta)
        turn.gen_key = ""


def replay_file(path: Path, tracer: TurnTracer) -> int:
    """Feed an events file through the tracer, in order. Returns the
    number of records read. The trace is what the live turn gave,
    minus wall-clock timing (the original ``ts`` rides in metadata
    only through the records the tracer already keeps)."""
    n = 0
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        tracer.handle(json.loads(line))
        n += 1
    return n


__all__ = ["Emitter", "Recorder", "TurnTracer", "replay_file",
           "trace_id_for", "TRACE_NAME"]
