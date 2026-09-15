"""The discovery agent: the sample's instruction, one tool, and the tool
round done properly.

The instruction (``SKILL.md``) tells the model to read the question,
decompose it, and fire a baseline search plus up to three variations in
ONE turn; this loop is what makes that batch work. Every function call
the model emits in a turn is executed, and every answer goes back in one
user turn, in order, so the model sees the batch it asked for. The
model's own parts are echoed verbatim (thought signatures included), a
tool that fails answers with data rather than an exception, and two
ceilings (model calls, wall time) end a run in plain language with what
was found so far. Entries are merged across every search: an entry
three searches returned has three witnesses, and sorts first.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterator

from .env import PACKAGE_DIR, env_float, env_int, first_env
from .tools import DECLARATIONS
from .transport import TransportError

SKILL_PATH = PACKAGE_DIR / "SKILL.md"
DEFAULT_MAX_CALLS = 6           # model calls per question: a ceiling, not a plan
DEFAULT_WALL_SECONDS = 300.0
DEFAULT_THINKING = "low"        # search decomposition is not a deep problem
MAX_OUTPUT_TOKENS = 8192


def load_instruction() -> str:
    """The agent's instruction, read from the file next to this module:
    the sample's own way of loading it."""
    return SKILL_PATH.read_text(encoding="utf-8")


@dataclass
class Exploration:
    """What one question produced: the model's answer, the entries found
    (merged, with witness counts), the searches that ran, the trace."""

    question: str
    answer: str = ""
    entries: list[dict[str, Any]] = field(default_factory=list)
    searches: list[dict[str, Any]] = field(default_factory=list)
    trace: list[dict[str, Any]] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=lambda: {
        "prompt_tokens": 0, "output_tokens": 0, "thought_tokens": 0})
    model_calls: int = 0
    status: str = "partial"          # answered | partial | empty
    stop_reason: str = ""
    elapsed_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DiscoveryAgent:
    """The loop. ``model`` is anything with ``converse`` (a Vertex or a
    gateway model, or a scripted double); ``kit`` maps tool names to
    plain functions taking keyword arguments."""

    def __init__(self, model: Any,
                 kit: dict[str, Callable[..., Any]] | None = None,
                 declarations: list[dict[str, Any]] | None = None, *,
                 instruction: str | None = None,
                 max_calls: int | None = None,
                 wall_seconds: float | None = None,
                 thinking_level: str | None = None,
                 max_output_tokens: int = MAX_OUTPUT_TOKENS,
                 log: Callable[[str], None] | None = None,
                 clock: Callable[[], float] = time.perf_counter) -> None:
        self.model = model
        self.kit = dict(kit or {})
        self.declarations = list(DECLARATIONS if declarations is None
                                 else declarations)
        self.instruction = (load_instruction() if instruction is None
                            else instruction)
        self.max_calls = (env_int("KC_MAX_MODEL_CALLS", DEFAULT_MAX_CALLS)
                          if max_calls is None else int(max_calls))
        self.wall_seconds = (env_float("KC_WALL_SECONDS", DEFAULT_WALL_SECONDS)
                             if wall_seconds is None else float(wall_seconds))
        self.thinking_level = (first_env("KC_THINKING_LEVEL") or DEFAULT_THINKING
                               if thinking_level is None else thinking_level)
        self.max_output_tokens = max_output_tokens
        self.log = log
        self.clock = clock

    def _note(self, message: str) -> None:
        if self.log is not None:
            self.log(f"    [agent] {message}")

    # ── the run ──────────────────────────────────────────────
    def run(self, question: str, *,
            on_text: Callable[[str], None] | None = None,
            on_thought: Callable[[str], None] | None = None,
            on_call: Callable[[str, dict[str, Any]], None] | None = None
            ) -> Exploration:
        started = self.clock()
        out = Exploration(question=question)
        contents: list[dict[str, Any]] = [
            {"role": "user", "parts": [{"text": question}]}]
        merged: dict[str, dict[str, Any]] = {}
        said: list[str] = []
        try:
            while True:
                if out.model_calls >= self.max_calls:
                    out.stop_reason = (
                        f"I hit my ceiling of {self.max_calls} model calls "
                        "for one question. Try it again with a little more "
                        "detail, or raise KC_MAX_MODEL_CALLS.")
                    break
                if self.clock() - started >= self.wall_seconds:
                    out.stop_reason = (
                        f"this took longer than the {self.wall_seconds:.0f} s "
                        "I allow for one question (KC_WALL_SECONDS).")
                    break
                out.model_calls += 1
                pending: list[dict[str, Any]] = []
                spoken: list[str] = []
                done: dict[str, Any] = {}
                for event in self._converse(contents):
                    kind = event.get("kind")
                    if kind == "text":
                        delta = str(event.get("delta") or "")
                        if delta:
                            spoken.append(delta)
                            if on_text is not None:
                                on_text(delta)
                    elif kind == "thought":
                        delta = str(event.get("delta") or "")
                        if delta.strip():
                            out.trace.append({"kind": "thought",
                                              "call": out.model_calls,
                                              "text": delta})
                            if on_thought is not None:
                                on_thought(delta)
                    elif kind == "call":
                        pending.append(event)
                    elif kind == "done":
                        done = event
                usage = done.get("usage") or {}
                for key in out.usage:
                    out.usage[key] += int(usage.get(key) or 0)
                if spoken:
                    said.append("".join(spoken))

                if not pending:
                    if said:
                        out.status = "answered"
                    else:
                        finish = str(done.get("finish") or "")
                        out.status = "empty"
                        out.stop_reason = (
                            "the model came back with nothing usable"
                            + (f" (it finished with {finish})"
                               if finish and finish != "STOP" else "")
                            + ". Try it again with a little more detail.")
                    break

                # the model turn, verbatim: every signature rides along
                contents.append({"role": "model",
                                 "parts": done.get("parts") or [
                                     {"functionCall": {
                                         "name": c["name"],
                                         "args": c.get("args") or {}}}
                                     for c in pending]})
                # every call answered, in order, in ONE user turn
                responses: list[dict[str, Any]] = []
                for call in pending:
                    name = str(call.get("name", ""))
                    args = (call.get("args")
                            if isinstance(call.get("args"), dict) else {})
                    if on_call is not None:
                        on_call(name, args)
                    t0 = self.clock()
                    result = self._dispatch(name, args)
                    elapsed_ms = round((self.clock() - t0) * 1000, 1)
                    self._record(out, merged, name, args, result, elapsed_ms)
                    payload = (result if isinstance(result, dict)
                               else {"result": result})
                    response: dict[str, Any] = {"name": name,
                                                "response": payload}
                    if call.get("id"):
                        response["id"] = call["id"]
                    responses.append({"functionResponse": response})
                contents.append({"role": "user", "parts": responses})
        finally:
            out.answer = "\n\n".join(said)
            rows = sorted(merged.values(),
                          key=lambda r: (-r["witnesses"], r["_order"]))
            for row in rows:
                row.pop("_order", None)
            out.entries = rows
            out.elapsed_ms = round((self.clock() - started) * 1000, 1)
        return out

    # ── the pieces ───────────────────────────────────────────
    def _converse(self, contents: list[dict[str, Any]]
                  ) -> Iterator[dict[str, Any]]:
        """One model call, asked once more on a transport failure only
        while nothing has reached the caller: a retry after the model
        started answering would duplicate what was already heard."""
        yielded = False
        attempts = 0
        while True:
            attempts += 1
            try:
                for event in self.model.converse(
                        contents, system=self.instruction,
                        tools=self.declarations,
                        thinking_level=self.thinking_level,
                        max_output_tokens=self.max_output_tokens):
                    yielded = True
                    yield event
                return
            except TransportError as e:
                if yielded or attempts >= 2:
                    raise
                self._note(f"the model call failed before it said anything "
                           f"({e}) — asking once more")

    def _dispatch(self, name: str, args: dict[str, Any]) -> Any:
        """A tool result, or an error dict the model can read. The turn
        never dies on a tool."""
        fn = self.kit.get(name)
        if fn is None:
            return {"error": f"unknown tool {name!r}",
                    "hint": "the tools are " + ", ".join(self.kit)}
        try:
            return fn(**args)
        except TypeError as e:
            return {"error": f"the arguments did not match: {e}",
                    "hint": "knowledge_catalog_search(query: str)"}
        except Exception as e:                       # noqa: BLE001
            return {"error": f"{type(e).__name__}: {e}",
                    "hint": "try a different call"}

    @staticmethod
    def _record(out: Exploration, merged: dict[str, dict[str, Any]],
                name: str, args: dict[str, Any], result: Any,
                elapsed_ms: float) -> None:
        """The searches table and the witness merge: an entry counts one
        witness per DISTINCT query that returned it."""
        query = str(args.get("query", "")) if isinstance(args, dict) else ""
        rows = result.get("results") if isinstance(result, dict) else None
        error = result.get("error") if isinstance(result, dict) else None
        row = {"call": out.model_calls, "tool": name, "query": query,
               "count": len(rows) if isinstance(rows, list) else 0,
               "total_size": (int(result.get("total_size") or 0)
                              if isinstance(result, dict) else 0),
               "error": str(error or ""), "elapsed_ms": elapsed_ms}
        out.searches.append(row)
        out.trace.append({"kind": "tool", **row})
        for hit in rows if isinstance(rows, list) else []:
            if not isinstance(hit, dict):
                continue
            key = str(hit.get("entry_name") or "")
            if not key:
                continue
            kept = merged.get(key)
            if kept is None:
                kept = dict(hit)
                kept["queries"] = []
                kept["witnesses"] = 0
                kept["_order"] = len(merged)
                merged[key] = kept
            if query not in kept["queries"]:
                kept["queries"].append(query)
                kept["witnesses"] = len(kept["queries"])
