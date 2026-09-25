"""Event record → trace: the translator, and the fake it is tested on.

One assistant turn is one trace. Each model call is a generation
whose usage is the delta between the budget ticks around it; each
tool call is a span; artifacts, proposals, chips, the loader record
and the plan are events; turn_done closes the trace and scores its
status and its refusals. A compound ask nests each task's sub-turn
(``<turn>.<task>``, every record tagged ``task``) under a task span of
the parent trace, so a multi-task turn is still one trace. The tracer
keeps only what an open turn needs and forgets it on turn_done.

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
# the scores a turn's trace carries (docs/runbooks/langfuse-insight.md)
SCORE_STATUS = "turn_status"          # categorical: the turn_done status
SCORE_REFUSED = "refused"             # numeric: tool steps refused
SCORE_TASKS_FAILED = "tasks_failed"   # numeric: tasks of a plan not done
SCORE_FEEDBACK = "feedback"           # numeric: 1 up, 0 down (ChatFeedback)
FINGERPRINT_KEYS = ("prompt_version", "prompt_prefix", "prompt_parts")


class Emitter(Protocol):
    """What the tracer asks of a backend. Observation handles live
    behind ``(trace_id, key)`` so the translator stays stateless
    about the SDK; ``parent`` names the key a child hangs under
    ('' is the trace's root)."""

    def trace_open(self, trace_id: str, *, name: str, session_id: str,
                   user_id: str, input: Any, metadata: dict[str, Any],
                   tags: list[str]) -> None: ...

    def generation_open(self, trace_id: str, key: str, *, name: str,
                        model: str, input: Any, metadata: dict[str, Any],
                        prompt: tuple[str, int] | None,
                        parent: str = "") -> None: ...

    def generation_close(self, trace_id: str, key: str, *, output: Any,
                         usage: dict[str, int] | None,
                         metadata: dict[str, Any]) -> None: ...

    def span_open(self, trace_id: str, key: str, *, name: str, input: Any,
                  metadata: dict[str, Any], parent: str = "") -> None: ...

    def span_close(self, trace_id: str, key: str, *, output: Any,
                   metadata: dict[str, Any], level: str) -> None: ...

    def event(self, trace_id: str, *, name: str,
              metadata: dict[str, Any], parent: str = "") -> None: ...

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
    instead of duplicating. A task's sub-turn (``<turn>.<task>``)
    maps to its parent's trace."""
    turn_id = turn_id.split(".", 1)[0]
    return hashlib.sha256(f"{session_id}/{turn_id}".encode()).hexdigest()[:32]


def _clip(value: Any, cap: int = CLIP) -> Any:
    if isinstance(value, str) and len(value) > cap:
        return value[:cap] + f"… [{len(value) - cap} more chars]"
    return value


_ENVELOPE = ("schema", "ts", "seq", "session_id", "ev", "turn_id")
_ZERO_TICK = {"tokens_in": 0, "tokens_out": 0, "tokens": 0}
_TASK_DONE = ("done",)


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
    refused: list[str] = field(default_factory=list)
    checks: int = 0
    gens: int = 0
    ticks: int = 0
    planning: bool = False
    fingerprint: dict[str, Any] = field(default_factory=dict)
    # a compound ask: the task spans this (parent) turn opened, by
    # task id → what the sub-turn left behind
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)
    # a sub-turn: its task id, the parent turn's key, and the span the
    # children hang under
    task: str = ""
    parent_key: str = ""
    parent: str = ""

    @property
    def prefix(self) -> str:
        return f"{self.task}/" if self.task else ""


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
        turn_id = str(record.get("turn_id") or "")
        session_id = str(record.get("session_id") or "")
        key = f"{session_id}/{turn_id}"
        if ev == "turn_started":
            parent_key = f"{session_id}/{turn_id.split('.', 1)[0]}"
            if record.get("task") and "." in turn_id \
                    and parent_key in self._turns:
                self._start_sub(key, parent_key, record)
            else:
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
                     session_id=session_id, model=model,
                     planning=bool(record.get("planning")))
        turn.prompt = self._prompt_link(str(record.get("version") or ""))
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
        if record.get("planning"):
            tags.append("compound")
        if self.environment:
            tags.append(f"env:{self.environment}")
        self.emitter.trace_open(
            turn.trace_id, name=TRACE_NAME, session_id=session_id,
            user_id=self.user_id, input=record.get("text", ""),
            metadata=metadata, tags=tags)

    def _start_sub(self, key: str, parent_key: str,
                   record: dict[str, Any]) -> None:
        """A task's sub-turn: no trace of its own — a span of the
        parent's, opened by task_started (or here, when the parent
        never announced the task)."""
        parent = self._turns[parent_key]
        task = str(record.get("task"))
        span = f"task:{task}"
        if task not in parent.tasks:
            self._open_task(parent, task, {
                "goal": record.get("text", ""),
                "sub_turn": record.get("turn_id")}, record.get("text", ""))
        turn = _Turn(key=key, trace_id=parent.trace_id,
                     session_id=parent.session_id, model=parent.model,
                     prompt=parent.prompt, task=task, parent_key=parent_key,
                     parent=span)
        turn.prompt = self._prompt_link(str(record.get("version") or "")) \
            or parent.prompt
        self._turns[key] = turn

    def _open_task(self, parent: _Turn, task: str,
                   metadata: dict[str, Any], goal: Any) -> None:
        parent.tasks[task] = {"prose": "", "status": "", "errors": []}
        self.emitter.span_open(
            parent.trace_id, f"task:{task}", name=f"task {task}",
            input=goal, metadata={"task": task, **metadata}, parent="")

    def _on_task_started(self, turn: _Turn, record: dict[str, Any]) -> None:
        task = str(record.get("task") or "")
        if not task or task in turn.tasks:
            return
        self._open_task(turn, task, {
            k: record.get(k) for k in ("kind", "depends_on", "sub_turn",
                                       "n", "of", "max_calls")},
            record.get("goal", ""))

    def _on_task_done(self, turn: _Turn, record: dict[str, Any]) -> None:
        task = str(record.get("task") or "")
        left = turn.tasks.get(task)
        if left is None:
            return
        status = str(record.get("status") or left.get("status") or "")
        left["status"] = status
        metadata = {k: record.get(k) for k in
                    ("status", "reason", "cost", "saved", "checked",
                     "refused", "artifacts")}
        if left.get("errors"):
            metadata["errors"] = left["errors"]
        self.emitter.span_close(
            turn.trace_id, f"task:{task}",
            output={"text": left.get("prose", ""),
                    "say": record.get("say", "")},
            metadata=metadata,
            level="DEFAULT" if status in _TASK_DONE else "WARNING")
        left["closed"] = True

    def _on_plan_made(self, turn: _Turn, record: dict[str, Any]) -> None:
        turn.planning = True
        self.emitter.event(
            turn.trace_id, name="plan",
            metadata={k: record.get(k) for k in
                      ("tasks", "synthesis", "repairs", "pool", "waves")},
            parent=turn.parent)

    def _on_skills_loaded(self, turn: _Turn,
                          record: dict[str, Any]) -> None:
        records = [r for r in record.get("skills") or []
                   if isinstance(r, dict)]
        self.emitter.event(
            turn.trace_id, name="skills_loaded",
            metadata={"loaded": list(record.get("skills_loaded") or []),
                      "modes": {str(r.get("skill_name")): r.get("mode")
                                for r in records},
                      "aggregate_skill_chars":
                          record.get("aggregate_skill_chars"),
                      "whole_load_limit": record.get("whole_load_limit"),
                      "retrieval_chunks": record.get("retrieval_chunks")},
            parent=turn.parent)

    def _on_model_prompt(self, turn: _Turn, record: dict[str, Any]) -> None:
        if record.get("kind") == "system":
            turn.system = str(record.get("content") or "")
            turn.fingerprint = {k: record[k] for k in FINGERPRINT_KEYS
                                if k in record}
            version = str(record.get("prompt_version") or "")
            if version:
                turn.prompt = self._prompt_link(version) or turn.prompt
            return
        n = record.get("n", 0)
        self._close_generation(turn, usage=None, dangling=True)
        turn.gen_key = f"{turn.prefix}gen{n}"
        turn.gens += 1
        turn.gen_text, turn.gen_thought = [], []
        payload: dict[str, Any] = {"contents": _clip(record.get("content"))}
        if n == 1 and turn.system:
            payload["system"] = _clip(turn.system, 12000)
        self.emitter.generation_open(
            turn.trace_id, turn.gen_key, name=f"model call {n}",
            model=turn.model, input=payload,
            metadata={"n": n, "task": turn.task or None,
                      **turn.fingerprint},
            prompt=turn.prompt, parent=turn.parent)

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
        turn.ticks += 1
        metadata = {"cost_usd_session": record.get("cost_usd"),
                    "turn_tokens": record.get("turn_tokens")}
        if not turn.gen_key and usage["total"]:
            # tokens spent with no model_prompt in front of them: the
            # planner's one-shot on a compound turn, else a call the
            # record did not announce — kept as a generation so the
            # session's usage still adds up
            planner = turn.planning and turn.gens == 0
            turn.gen_key = f"{turn.prefix}gen-tick{turn.ticks}"
            self.emitter.generation_open(
                turn.trace_id, turn.gen_key,
                name="planner" if planner else "model call (untracked)",
                model=turn.model, input=None,
                metadata={"n": None, "task": turn.task or None,
                          "one_shot": "planner" if planner else None},
                prompt=None, parent=turn.parent)
        self._close_generation(turn, usage=usage, metadata=metadata)

    def _on_tool_call(self, turn: _Turn, record: dict[str, Any]) -> None:
        key = f"{turn.prefix}tool{record.get('n', 0)}"
        turn.open_tools[key] = {}
        self.emitter.span_open(
            turn.trace_id, key, name=str(record.get("tool") or "tool"),
            input={"args": record.get("args"), "input": record.get("input")},
            metadata={"n": record.get("n"), "task": turn.task or None},
            parent=turn.parent)

    def _on_tool_step(self, turn: _Turn, record: dict[str, Any]) -> None:
        key = f"{turn.prefix}tool{record.get('n', 0)}"
        summary = str(record.get("summary") or "")
        tool = str(record.get("tool") or "")
        if tool == "check":
            turn.checks += 1
        if summary.startswith("ERROR"):
            turn.refused.append(f"{tool}: {summary[6:].strip(': ')}"[:240])
        if key in turn.open_tools:
            turn.open_tools[key] = {
                "summary": summary,
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
                      ("artifact_id", "version", "title")},
            parent=turn.parent)

    def _on_proposal(self, turn: _Turn, record: dict[str, Any]) -> None:
        self.emitter.event(
            turn.trace_id, name="proposal",
            metadata={k: _clip(v, 2000) for k, v in record.items()
                      if k not in _ENVELOPE},
            parent=turn.parent)

    def _on_chips(self, turn: _Turn, record: dict[str, Any]) -> None:
        suggestions = record.get("suggestions") or []
        self.emitter.event(
            turn.trace_id, name="chips",
            metadata={"suggestions": [
                s.get("label", "") if isinstance(s, dict) else str(s)
                for s in suggestions][:6],
                "clarify": _clip(record.get("clarify"), 1000)},
            parent=turn.parent)

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
        if turn.task:
            # a sub-turn: what it left behind goes on the task span,
            # which task_done closes on the parent
            parent = self._turns.get(turn.parent_key)
            left = parent.tasks.get(turn.task) if parent else None
            if left is not None:
                left["prose"] = "".join(turn.prose)
                left["status"] = status
                left["errors"] = list(turn.errors)
                parent.refused += turn.refused
                parent.checks += turn.checks
            else:
                self.emitter.span_close(
                    turn.trace_id, f"task:{turn.task}",
                    output={"text": "".join(turn.prose)},
                    metadata={"status": status, "errors": turn.errors},
                    level="ERROR" if turn.errors else "DEFAULT")
            self._turns.pop(turn.key, None)
            return
        for task, left in turn.tasks.items():
            if not left.get("closed"):
                self.emitter.span_close(
                    turn.trace_id, f"task:{task}",
                    output={"text": left.get("prose", "")},
                    metadata={"status": left.get("status") or "unfinished"},
                    level="WARNING")
        metadata = {k: v for k, v in record.items() if k not in _ENVELOPE}
        metadata.update(turn.fingerprint)
        if turn.errors:
            metadata["errors"] = turn.errors
        if turn.refused:
            metadata["refused"] = turn.refused
        if turn.tasks:
            metadata["tasks"] = {t: left.get("status")
                                 for t, left in turn.tasks.items()}
        level = "ERROR" if status == "error" or turn.errors else "DEFAULT"
        self.emitter.trace_close(
            turn.trace_id, output="".join(turn.prose), metadata=metadata,
            level=level, status_message=turn.errors[0] if turn.errors
            else "")
        self.emitter.score(turn.trace_id, name=SCORE_STATUS, value=status,
                           comment="", data_type="CATEGORICAL")
        self.emitter.score(turn.trace_id, name=SCORE_REFUSED,
                           value=float(len(turn.refused)),
                           comment="; ".join(turn.refused[:3]),
                           data_type="NUMERIC")
        if turn.tasks:
            failed = [t for t, left in turn.tasks.items()
                      if left.get("status") not in _TASK_DONE]
            self.emitter.score(turn.trace_id, name=SCORE_TASKS_FAILED,
                               value=float(len(failed)),
                               comment=", ".join(failed),
                               data_type="NUMERIC")
        self._turns.pop(turn.key, None)

    # ── helpers ──────────────────────────────────────────────
    def _prompt_link(self, version: str) -> tuple[str, int] | None:
        if self.prompt_of is None or not version:
            return None
        try:
            return self.prompt_of(version)
        except Exception:                     # noqa: BLE001
            return None

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


def feedback_score(emitter: Emitter, session_id: str, turn_id: str,
                   vote: str, *, subject: str = "", note: str = "") -> str:
    """A thumbs vote from the record (ChatFeedback) as a score on the
    turn's trace: 1 for up, 0 for down, the note as the comment.
    Returns the trace id it landed on."""
    trace_id = trace_id_for(session_id, turn_id)
    emitter.score(trace_id, name=SCORE_FEEDBACK,
                  value=1.0 if vote == "up" else 0.0,
                  comment=" — ".join(p for p in (subject, note) if p)[:500],
                  data_type="NUMERIC")
    return trace_id


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


def replay_records(records: list[dict[str, Any]], tracer: TurnTracer) -> int:
    """The same replay from records already in memory (the store's
    ``events``): what ``backfill --from spanner`` feeds."""
    for record in records:
        tracer.handle(record)
    return len(records)


__all__ = ["Emitter", "Recorder", "TurnTracer", "feedback_score",
           "replay_file", "replay_records", "trace_id_for", "TRACE_NAME",
           "SCORE_STATUS", "SCORE_REFUSED", "SCORE_TASKS_FAILED",
           "SCORE_FEEDBACK", "FINGERPRINT_KEYS"]
