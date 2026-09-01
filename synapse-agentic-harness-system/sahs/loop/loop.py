"""The agent loop (Agent Loop v1 §2): the model drives tool calls over
the cards, indexes, and snapshot until it can answer, must ask, or
honestly stops.

One door: ``run_turn``'s data path IS the loop. Its deterministic
opening (classify → apply → delta-resolve) is the fast path — "the
case where the engineer already knows" (§10) — and costs zero model
calls, which preserves the Stage A first-turn pin and instant
mutations. ``navigate_loop`` here is the loop's model-driven middle,
engaged when the opening could not complete the plan; it ends in one
of three honest states: ``finish()`` (the same contract → generate →
verify → render every answered turn shares), ``ask_user`` (one
question, chips with evidence, the turn ends), or a stated partial.

Determinism relocated, not lost: the tools decide HOW (binder,
typechecker, sandbox); the model decides only where to look next. At
``final`` the deterministic binder runs once more over the plan
(filters bind at scope, grain fills from the metric row), so no
answer reaches the contract on the model's say-so alone.

Context discipline (§6): every tool result becomes an artifact event;
the prompt keeps a ≤3-line summary per step; the plan and notes are
re-injected fresh at each step because they ARE the state; under
budget pressure the history collapses to the last two results and
never drops the plan.

The navigation lane ships behind ``SYNAPSE_NAVIGATE=1`` (or the
``navigate=`` parameter). Justifying principle: every landed step is
gated on the E19 suite, and T3's ``t3_nothing_matches`` pins the
deterministic no-candidates chips until §9.4's navigation tasks grade
the loop's replacement behavior. Deletion condition: the flag
defaults on when those tasks land and hold the bar.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from sahs.tools.api import Build

from .prompt import PROMPT_VERSION, system_prompt
from .skills import Skill
from .tools import LoopState, render_tool_block, toolkit

MAX_STEPS = 24                 # §2: Budget(tool_calls=24, ...)
WALL_SECONDS = 180.0
STRIKES = 2                    # malformed replies before failing closed


@dataclass
class LoopBudget:
    """Per-turn loop budget: steps and wall clock. Token and cost caps
    stay with the session ``Budget`` (one meter, one breaker), which
    the loop consults every iteration so both share one exit."""

    max_steps: int = MAX_STEPS
    wall_seconds: float = WALL_SECONDS
    steps: int = 0
    started: float = field(default_factory=time.monotonic)

    def charge(self) -> None:
        self.steps += 1

    @property
    def left(self) -> int:
        return max(0, self.max_steps - self.steps)

    def tripped(self) -> str:
        if self.steps >= self.max_steps:
            return f"the step budget ran out ({self.max_steps} tool calls)"
        if time.monotonic() - self.started >= self.wall_seconds:
            return "the wall clock ran out"
        return ""


# ─── compaction (§6): results become ≤3-line summaries ───────


def _short(value: Any, cap: int = 90) -> str:
    text = json.dumps(value, default=str) if not isinstance(value, str) \
        else value
    return text if len(text) <= cap else text[:cap - 1] + "…"


def compact_result(tool: str, result: dict[str, Any]) -> str:
    """What survives into the prompt. Errors survive whole — the error
    channel is the teaching channel. ``read_card`` keeps the used
    section verbatim (bounded); everything else compresses to counts
    and top lines, with the full result in the artifact event."""
    if not isinstance(result, dict):
        return _short(result)
    if result.get("error"):
        return (f"ERROR: {_short(result['error'], 160)}\n"
                f"hint: {_short(result.get('hint', ''), 200)}")
    if tool == "grep_cards":
        top = "; ".join(f"{h['card']}:{h['line']} {_short(h['text'], 60)}"
                        for h in result.get("hits", [])[:2])
        return f"{result.get('count', 0)} hits. {top}"
    if tool == "read_card":
        text = result.get("text", "")
        if result.get("section"):
            kept = text if len(text) <= 1500 else text[:1500] + "\n…"
            return (f"{result['card']} · section {result['section']} "
                    f"(verbatim):\n{kept}")
        head = "\n".join(text.splitlines()[:4])
        return (f"{result.get('card')} · sections: "
                f"{', '.join(result.get('sections', []))}\n{head}")
    if tool == "search_semantics":
        top = "; ".join(
            f"{r['kind']} {_short(r.get('label') or r.get('text') or r.get('a', ''), 40)}"
            + (f" ({r.get('status')}, {r.get('table')})"
               if r.get("kind") == "metric" else "")
            for r in result.get("results", [])[:3])
        return f"{result.get('count', 0)} ranked. {top}"
    if tool == "resolve":
        metrics = result.get("metrics", [])
        top = (f"top: {metrics[0].get('label')} on "
               f"{metrics[0].get('table')}" if metrics else "no metric")
        return (f"confidence {result.get('confidence')}; {top}; "
                f"ambiguities: {len(result.get('ambiguities', []))}")
    if tool == "sample_values":
        values = result.get("values", [])
        return (f"{len(values)} observed values: "
                f"{_short(values[:6], 140)} (compiled, not live)")
    if tool == "get_join_paths":
        return "; ".join(f"{'↔'.join(p['tables'])}: {p['tier']}"
                         for p in result.get("paths", [])) or "no pairs"
    if tool == "run_sql":
        if result.get("mode") == "dry_run":
            return (f"valid; bytes {result.get('bytes_processed')}; "
                    f"schema {_short(result.get('result_schema'), 120)}")
        return (f"{result.get('row_count', 0)} rows from "
                f"{result.get('source', 'snapshot')}; first: "
                f"{_short((result.get('rows') or ['(none)'])[0], 140)}")
    if tool == "plan_set":
        changed = ", ".join(c["slot"] for c in result.get("changes", []))
        warned = ", ".join(w["code"] for w in result.get("warnings", []))
        return (f"plan v{result.get('plan', {}).get('version')}: "
                f"changed {changed or 'nothing'}"
                + (f"; warnings: {warned}" if warned else ""))
    if tool == "note":
        return f"noted ({result.get('notes')})"
    if tool == "delegate_scout":
        # §6: scout summaries arrive already compact — keep them
        return (f"scout ({result.get('steps', 0)} looks, "
                f"{len(result.get('cards_read', []))} cards):\n"
                + str(result.get("summary", ""))[:1600])
    return _short(result, 240)


def _render_context(question: str, opening: str, plan: Any,
                    notes: list[str], history: list[dict[str, Any]],
                    left: int, max_steps: int) -> str:
    pressure = left <= 2
    kept = history[-2:] if pressure else history
    lines = [f"QUESTION: {question}", "", f"OPENING: {opening}", "",
             f"PLAN v{plan.version}: "
             + json.dumps({k: v for k, v in plan.to_dict().items()
                           if v and k not in ("provenance",
                                              "filter_bindings")},
                          default=str)]
    if notes:
        lines.append("NOTES:")
        lines += [f"- {n}" for n in notes[-8:]]
    lines.append("")
    if kept:
        lines.append("STEPS SO FAR:"
                     + (" (earlier steps compacted away; the plan and"
                        " notes above are complete)" if pressure
                        and len(history) > 2 else ""))
        for entry in kept:
            if "note" in entry:
                lines.append(f"  [harness] {entry['note']}")
            else:
                lines.append(f"  {entry['n']}. {entry['tool']}"
                             f"({_short(entry['args'], 80)}) → "
                             + entry["summary"].replace("\n", "\n     "))
    else:
        lines.append("STEPS SO FAR: none yet.")
    lines.append("")
    lines.append(f"BUDGET: {left} of {max_steps} steps left.")
    lines.append("Reply with the next step:")
    return "\n".join(lines)


def _opening_line(resolver: dict[str, Any] | None,
                  clarify: dict[str, Any] | None,
                  resume: dict[str, Any] | None) -> str:
    if resume:
        return (f"you asked: {resume.get('question', '')!r}; the user "
                f"chose: {resume.get('answer', '')!r}. Continue from "
                "the plan below.")
    bits = []
    resolver = resolver or {}
    bits.append(f"binder confidence {resolver.get('confidence')}")
    candidates = resolver.get("candidates") or []
    if candidates:
        bits.append("candidates: " + "; ".join(
            f"{c.get('label')} on {c.get('table')}"
            for c in candidates[:3]))
    else:
        bits.append("no metric candidates")
    if clarify:
        bits.append(f"it would have asked: {clarify.get('question')!r}")
    return "the deterministic binder ran first: " + " · ".join(bits)


# ─── the loop ────────────────────────────────────────────────


def navigate_loop(*, build: Build, store: Any, bus: Any, budget: Any,
                  abort: Any, model: Any, session: dict[str, Any],
                  turn_id: str, plan: Any,
                  finish: Callable[[Any, list[str]], str],
                  resolver: dict[str, Any] | None = None,
                  clarify: dict[str, Any] | None = None,
                  resume: dict[str, Any] | None = None,
                  skills: list[Skill] | None = None,
                  substrate: Any = None, snapshot_runner: Any = None,
                  ledger_path: Path | None = None,
                  max_steps: int = MAX_STEPS,
                  wall_seconds: float = WALL_SECONDS) -> str:
    """The model-driven middle of a turn. Returns the turn status:
    ``answered`` | ``clarify`` | ``partial``. ModelUnavailable and
    Aborted propagate to ``run_turn``'s handlers unchanged."""
    from sahs.ask.resolve import resolve_plan   # late: avoids a cycle

    session_id = session["id"]
    state = LoopState(plan=plan,
                      notes=list((resume or {}).get("notes") or []))

    def scout(question: str) -> dict[str, Any]:
        from .scout import run_scout        # late: scout uses toolkit
        return run_scout(build, model, question, substrate=substrate)

    kit = toolkit(build, state, substrate=substrate,
                  snapshot_runner=snapshot_runner,
                  ledger_path=ledger_path, scout=scout)
    system = system_prompt(build, skills,
                           tool_block=render_tool_block(kit))
    loop_budget = LoopBudget(max_steps=max_steps,
                             wall_seconds=wall_seconds)
    opening = _opening_line(resolver, clarify, resume)
    question = plan.question
    history: list[dict[str, Any]] = []
    strikes = 0
    finals_refused = 0
    stop_reason = ""

    bus.emit("loop_started", turn_id=turn_id, reason=opening,
             steps_max=max_steps, build_id=build.version,
             prompt_version=PROMPT_VERSION,
             skills=[s.name for s in (skills or [])])
    # the panel's ground truth: the system prompt the model will see,
    # emitted once (it is identical for every step of this loop)
    bus.emit("loop_prompt", turn_id=turn_id, n=0, kind="system",
             content=system[:12000])

    while True:
        tripped = loop_budget.tripped()
        session_tripped = budget.exceeded()
        if tripped or session_tripped:
            stop_reason = tripped or f"the session breaker ({session_tripped})"
            break
        abort.check()

        prompt = _render_context(question, opening, state.plan,
                                 state.notes, history,
                                 loop_budget.left, max_steps)
        bus.emit("loop_prompt", turn_id=turn_id,
                 n=loop_budget.steps + 1, kind="step",
                 content=prompt[:8000])
        step = model.json(prompt, system=system, temperature=0.0,
                          max_tokens=700)
        loop_budget.charge()
        bus.emit("budget_tick", turn_id=turn_id, **budget.tick())

        if not isinstance(step, dict) or not (step.get("final")
                                              or step.get("tool")):
            strikes += 1
            history.append({"note": "that reply was not one strict "
                                    "JSON step: return {\"think\", "
                                    "\"tool\", \"args\"} or {\"think\","
                                    " \"final\": true}"})
            if strikes >= STRIKES:
                stop_reason = "the model stopped speaking strict JSON"
                break
            continue

        think = str(step.get("think", ""))[:400]

        # ── final: the binder completes, then the shared exit ─
        if step.get("final"):
            outcome = resolve_plan(build, state.plan, touched=[])
            bus.emit("resolve_result", turn_id=turn_id,
                     **outcome.result)
            ready, missing = outcome.plan.ready()
            if not ready:
                # a metric or grain miss is the MODEL'S to fix (read
                # the card, set the slot, or ask): teach, don't chip
                finals_refused += 1
                history.append({
                    "note": "final refused: the plan still needs "
                            + ", ".join(missing)
                            + ". Keep working, or ask_user."})
                if finals_refused >= 2:
                    stop_reason = ("the model tried twice to finish "
                                   "an incomplete plan")
                    break
                continue
            if outcome.clarify:
                # ready, yet a filter stayed genuinely ambiguous at
                # scope: the binder's chips ask, evidence attached
                _stash_clarify(store, bus, session_id, turn_id,
                               outcome.plan, outcome.clarify,
                               state.notes)
                bus.emit("loop_done", turn_id=turn_id, outcome="ask",
                         steps=loop_budget.steps)
                return "clarify"
            state.plan = outcome.plan
            tables = [state.plan.table] if state.plan.table else []
            status = finish(state.plan, tables)
            bus.emit("loop_done", turn_id=turn_id, outcome="answered",
                     steps=loop_budget.steps,
                     subgraph_used=state.subgraph)
            return status

        # ── a tool call ──────────────────────────────────────
        name = str(step.get("tool", ""))
        args = step.get("args") or {}
        spec = kit.get(name)
        if spec is None:
            summary = (f"ERROR: unknown tool {name!r}. hint: the tools "
                       f"are {', '.join(kit)}")
            bus.emit("loop_step", turn_id=turn_id, n=loop_budget.steps,
                     tool=name, args=_short(args, 120), think=think,
                     summary=summary, ref="")
            history.append({"n": loop_budget.steps, "tool": name,
                            "args": args, "summary": summary})
            continue
        if not isinstance(args, dict):
            args = {}
        try:
            result = spec.fn(**args)
        except TypeError:
            result = {"error": "the args did not match the signature",
                      "hint": spec.signature}
        except Exception as e:      # a tool bug never kills the turn
            result = {"error": f"{type(e).__name__}: {e}",
                      "hint": "try a different call"}

        ref = f"a{loop_budget.steps}"
        summary = compact_result(name, result)
        bus.emit("loop_step", turn_id=turn_id, n=loop_budget.steps,
                 tool=name, args=_short(args, 120), think=think,
                 summary=summary, ref=ref)
        bus.emit("loop_artifact", turn_id=turn_id, ref=ref, tool=name,
                 content=json.dumps(result, default=str)[:6000])

        if name == "plan_set" and result.get("ok"):
            bus.emit("plan_delta", turn_id=turn_id,
                     version=state.plan.version,
                     parent=state.plan.parent,
                     changes=result.get("changes", []),
                     summary=state.plan.summary())
        if name == "ask_user" and result.get("ok"):
            _stash_clarify(store, bus, session_id, turn_id, state.plan,
                           state.pending_question, state.notes)
            bus.emit("loop_done", turn_id=turn_id, outcome="ask",
                     steps=loop_budget.steps)
            return "clarify"

        history.append({"n": loop_budget.steps, "tool": name,
                        "args": args, "summary": summary, "ref": ref})

    # ── the third honest state: here is where I stopped ──────
    ready, missing = state.plan.ready()
    text = f"I stopped before finishing: {stop_reason}."
    if state.notes:
        text += " What I found: " + "; ".join(state.notes[-3:]) + "."
    if state.plan.metric_id or state.plan.filters:
        text += f" The plan so far: {state.plan.summary()}."
    if missing:
        text += " Still unbound: " + ", ".join(missing) + "."
    text += (" You can narrow the question, or name the table or "
             "metric if you know it.")
    for start in range(0, len(text), 48):
        bus.emit("generate_token", turn_id=turn_id,
                 delta=text[start:start + 48])
    store.add_message(session_id, "assistant", text, turn_id=turn_id,
                      payload={"loop_partial": True,
                               "loop_notes": state.notes})
    bus.emit("loop_done", turn_id=turn_id, outcome="partial",
             steps=loop_budget.steps, reason=stop_reason,
             subgraph_used=state.subgraph)
    return "partial"


def _stash_clarify(store: Any, bus: Any, session_id: str, turn_id: str,
                   plan: Any, clarify: dict[str, Any] | None,
                   notes: list[str]) -> None:
    """ask_user ends the turn the way a resolver clarify does: partial
    plan stored (a question is a success state), chips emitted, and
    the notes ride the stored message so a chip answer resumes the
    loop with what it had learned."""
    store.add_plan_version(session_id, plan.to_dict(),
                           parent=plan.parent, turn_id=turn_id,
                           summary=plan.summary())
    payload_clarify = clarify or {}
    bus.emit("clarify_request", turn_id=turn_id, **payload_clarify)
    store.add_message(session_id, "assistant", "", turn_id=turn_id,
                      payload={"clarify": payload_clarify,
                               "loop_notes": list(notes)})
