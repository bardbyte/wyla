"""The assistant loop (Synapse v2 §2): thin, Claude-shaped.

No classify step, no pre-resolve, no contract gate. The model drives:
each step is a tool call, streamed prose, or done-with-chips. The
harness contributes exactly what §2 lists — budgets and breakers in
code, compaction (results → refs, ≤3-line summaries), streaming, the
artifact panel events, and the rendering rules enforced where
artifacts are validated. Everything else is the model reasoning with
good tools.

Honesty carries over from v1 unchanged: three exits (answered, an
open question via ask_user's chips, or a stated partial), strict-JSON
failures fail closed after a taught retry, the stop button lands in a
recorded stop instead of a vanished turn, and every prompt the model
saw is in the event stream.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from sahs.ask.budget import Aborted
from sahs.loop.digest import synapse_digest
from sahs.loop.loop import LoopBudget, _short, compact_result
from sahs.loop.skills import Skill, render_skills
from sahs.tools.api import Build

from .events import EventBus
from .sandbox import prepare_workspace
from .skills_loader import all_skills, render_skill_index
from .store import AssistantStore
from .tools import AssistantState, assistant_toolkit, render_tool_block

ASSISTANT_VERSION = "assistant/1"
MAX_STEPS = 24
WALL_SECONDS = 300.0
STRIKES = 2
SAY_CHUNK = 48

# the first sentence is the transport routing key for scripted tests.
IDENTITY = """You are Synapse, an analytical colleague over the \
Meridian governed graph.

You are a general reasoner first: a thinking question gets thinking, \
with no tools. A data question gets the graph: find the definition, \
check it, compute, and show your receipts. A deliverable gets an \
artifact the user keeps. Same voice throughout — warm, brief, plain, \
never mystical about yourself.

You never invent a table, column, metric, or number: if it is not in \
a card, an index, or a tool result, it does not exist for you. \
Numbers come from tools; reasoning comes from you."""

RULES = """## Rendering and disclosure (enforced, not advisory)
- Any number you show carries its status (certified / pending / \
composed / exploratory) and its meridian line. The artifact validator \
refuses undisclosed numbers; say the status in prose too, in one \
clause, and offer "certified only" when something is pending.
- Composed numbers keep an EXPLORATORY watermark until a passing \
check stands behind them.
- Prefer certified; say so plainly when something is pending or \
mined; an honest "here is where I stopped" beats a confident guess.
- sample_values before filter literals; read_card before using \
anything; get_join_paths before any join; keep a working note with \
plan_set as questions get complex."""

PROTOCOL = """## Each step
Reply with STRICT JSON, exactly one object, no prose around it:
  {"think": "<brief>", "tool": "<name>", "args": {...}}     one look
  {"say": "<markdown for the user>"}                        stream prose
  {"say": "<closing prose>", "done": true,
   "chips": ["<follow-up>", "..."]}                         finish
A plain question you can answer from reasoning alone: one {"say", \
"done", "chips"} step. chips are 2-4 short follow-ups the user might \
tap next — specific to what you just showed, never generic.

Your tools:

"""

_DIGEST_CACHE: dict[str, str] = {}


def system_prompt(build: Build, skills: list[Skill] | None,
                  tool_block: str,
                  skill_index: list[Any] | None = None) -> str:
    digest = _DIGEST_CACHE.get(build.version)
    if digest is None:
        digest = synapse_digest(build)
        _DIGEST_CACHE[build.version] = digest
    parts = [IDENTITY, "", digest, "", RULES]
    loaded = render_skills(skills or [])
    if loaded:
        parts += ["", loaded]
    # progressive disclosure: names and one-liners only — the full
    # pack enters the turn via load_skill, or not at all
    shelf = render_skill_index(
        skill_index or [],
        exclude=frozenset(s.name for s in (skills or [])))
    if shelf:
        parts += ["", shelf]
    parts += ["", PROTOCOL]
    return "\n".join(parts) + tool_block


def _history_block(store: AssistantStore, session_id: str,
                   limit: int = 12) -> str:
    rows = store.messages(session_id)[-limit:]
    if not rows:
        return "(a fresh session)"
    lines = []
    for row in rows:
        text = (row["text"] or "").strip().replace("\n", " ")
        if not text and row.get("payload"):
            text = "(chips shown)"
        lines.append(f"{row['role']}: {text[:500]}")
    return "\n".join(lines)


def _compact(tool: str, result: Any) -> str:
    if isinstance(result, dict):
        if tool in ("run_sql", "whatif") and result.get("saved_as"):
            changed = result.get("changed") or {}
            return (compact_result(tool, result)
                    + f"\nrows saved: meridian.rows("
                      f"{result['saved_as']!r})"
                    + (f"\nchanged {changed['find']!r} → "
                       f"{changed['replace']!r}" if changed else ""))
        if tool == "compare" and result.get("totals"):
            totals = " · ".join(f"{k} {v:,.4g}"
                                for k, v in result["totals"].items())
            return (f"aligned {len(result.get('frame', []))} rows "
                    f"on {', '.join(result.get('aligned_on', []))} "
                    f"({result.get('column')}): {totals}"
                    + (f"; only_in_a {result['only_in_a']}, "
                       f"only_in_b {result['only_in_b']}"
                       if result.get("only_in_a")
                       or result.get("only_in_b") else ""))
        if tool == "python":
            head = str(result.get("stdout", ""))[:600]
            tail = (f"\nstderr: {str(result.get('stderr'))[:300]}"
                    if result.get("stderr") else "")
            files = (f"\nfiles: {', '.join(result['files'])}"
                     if result.get("files") else "")
            return (f"{'ok' if result.get('ok') else 'FAILED'} in "
                    f"{result.get('elapsed_ms')}ms\n{head}{tail}{files}")
        if tool in ("artifact", "artifact_update", "constellation"):
            if result.get("error"):
                probs = "; ".join(
                    f"{p.get('code')}: {p.get('detail')} — "
                    f"{p.get('hint')}"
                    for p in result.get("problems", []))[:700]
                return (compact_result(tool, result)
                        + (f"\nproblems: {probs}" if probs else ""))
            mark = (f" · {result['watermark']}"
                    if result.get("watermark") else "")
            return (f"{result.get('type')} \"{result.get('title')}\" "
                    f"v{result.get('version')} is in the panel{mark} "
                    f"(id {result.get('artifact_id')})")
        if tool == "list_artifacts":
            return f"{result.get('count', 0)} artifacts: " + "; ".join(
                f"{a['artifact_id']} {a['type']} \"{a['title']}\" "
                f"v{a['version']}" for a in result.get("artifacts", []))
        if tool == "load_skill":
            # the pack text IS the point: it must reach the next
            # prompt whole, never compacted to three lines
            if result.get("text"):
                return (f"skill {result.get('name')} "
                        f"({result.get('origin')}) loaded — this "
                        "doctrine now applies:\n"
                        + str(result["text"]).strip())
            if result.get("note"):
                return str(result["note"])
        if tool == "list_skills":
            return f"{result.get('count', 0)} packs: " + "; ".join(
                f"{s['name']} ({s['origin']}) — {s['description']}"
                for s in result.get("skills", []))[:900]
    return compact_result(tool, result)


def run_assistant_turn(*, build: Build, store: AssistantStore,
                       bus: EventBus, budget: Any, abort: Any,
                       model: Any, session: dict[str, Any],
                       turn_id: str, text: str, workspace: Path,
                       skills: list[Skill] | None = None,
                       substrate: Any = None,
                       snapshot_runner: Any = None,
                       graph_root: Path | None = None,
                       max_steps: int = MAX_STEPS,
                       wall_seconds: float = WALL_SECONDS) -> str:
    session_id = session["id"]
    started = time.perf_counter()
    bus.emit("turn_started", turn_id=turn_id, text=text,
             build_id=build.version, version=ASSISTANT_VERSION,
             skills=[s.name for s in (skills or [])])
    budget.start_turn()
    prepare_workspace(workspace, build.root)

    state = AssistantState()
    kit = assistant_toolkit(build, state, store=store,
                            session_id=session_id, turn_id=turn_id,
                            workspace=workspace, model=model,
                            substrate=substrate,
                            snapshot_runner=snapshot_runner,
                            graph_root=graph_root)
    system = system_prompt(build, skills, render_tool_block(kit),
                           skill_index=all_skills(graph_root))
    bus.emit("model_prompt", turn_id=turn_id, n=0, kind="system",
             content=system[:12000])
    history = _history_block(store, session_id)

    loop_budget = LoopBudget(max_steps=max_steps,
                             wall_seconds=wall_seconds)
    steps: list[dict[str, Any]] = []
    said: list[str] = []
    chips: list[str] = []
    strikes = 0
    stop_reason = ""
    status = "partial"

    def _stream(prose: str) -> None:
        for start in range(0, len(prose), SAY_CHUNK):
            bus.emit("say_token", turn_id=turn_id,
                     delta=prose[start:start + SAY_CHUNK])

    def _prompt() -> str:
        pressure = loop_budget.left <= 2
        kept = steps[-2:] if pressure else steps
        lines = ["CONVERSATION SO FAR:", history, "",
                 f"THE USER JUST SAID: {text}", ""]
        if state.artifacts_touched:
            lines.append("ARTIFACTS TOUCHED THIS TURN: "
                         + ", ".join(dict.fromkeys(
                             state.artifacts_touched)))
        if said:
            lines.append("YOU HAVE SAID SO FAR THIS TURN: "
                         + " ".join(said)[-600:])
        if kept:
            lines.append("STEPS THIS TURN:"
                         + (" (earlier steps compacted away)"
                            if pressure and len(steps) > 2 else ""))
            for entry in kept:
                if "note" in entry:
                    lines.append(f"  [harness] {entry['note']}")
                else:
                    lines.append(
                        f"  {entry['n']}. {entry['tool']}"
                        f"({_short(entry['args'], 80)}) → "
                        + entry["summary"].replace("\n", "\n     "))
        else:
            lines.append("STEPS THIS TURN: none yet.")
        lines.append("")
        lines.append(f"BUDGET: {loop_budget.left} of {max_steps} "
                     "steps left.")
        lines.append("Next step:")
        return "\n".join(lines)

    try:
        while True:
            tripped = loop_budget.tripped()
            session_tripped = budget.exceeded()
            if tripped or session_tripped:
                stop_reason = tripped or (
                    f"the session breaker ({session_tripped})")
                break
            abort.check()

            prompt = _prompt()
            bus.emit("model_prompt", turn_id=turn_id,
                     n=loop_budget.steps + 1, kind="step",
                     content=prompt[:8000])
            step = model.json(prompt, system=system, temperature=0.0,
                              max_tokens=1200)
            loop_budget.charge()
            bus.emit("budget_tick", turn_id=turn_id, **budget.tick())

            if not isinstance(step, dict) or not (
                    step.get("tool") or step.get("say")
                    or step.get("done")):
                strikes += 1
                steps.append({"note": "that reply was not one strict "
                                      "JSON step: {\"think\",\"tool\","
                                      "\"args\"} or {\"say\": …} or "
                                      "{\"say\",\"done\":true,"
                                      "\"chips\":[…]}"})
                if strikes >= STRIKES:
                    stop_reason = ("the model stopped speaking "
                                   "strict JSON")
                    break
                continue

            if step.get("say"):
                prose = str(step["say"])
                said.append(prose)
                _stream(prose)

            if step.get("done"):
                chips = [str(c)[:80] for c in
                         (step.get("chips") or [])][:4]
                status = "answered" if (said or
                                        state.artifacts_touched) \
                    else "partial"
                if status == "partial":
                    fallback = ("I finished without saying anything "
                                "usable — ask again and I will do "
                                "better.")
                    said.append(fallback)
                    _stream(fallback)
                break

            if not step.get("tool"):
                continue                      # a bare say: keep going

            name = str(step.get("tool", ""))
            args = step.get("args") if isinstance(step.get("args"),
                                                  dict) else {}
            spec = kit.get(name)
            think = str(step.get("think", ""))[:400]
            if spec is None:
                summary = (f"ERROR: unknown tool {name!r}. hint: the "
                           f"tools are {', '.join(kit)}")
                bus.emit("tool_step", turn_id=turn_id,
                         n=loop_budget.steps, tool=name,
                         args=_short(args, 120), think=think,
                         summary=summary, ref="")
                steps.append({"n": loop_budget.steps, "tool": name,
                              "args": args, "summary": summary})
                continue
            try:
                result = spec.fn(**args)
            except TypeError:
                result = {"error": "the args did not match the "
                                   "signature",
                          "hint": spec.signature}
            except Exception as e:
                result = {"error": f"{type(e).__name__}: {e}",
                          "hint": "try a different call"}

            ref = f"a{loop_budget.steps}"
            summary = _compact(name, result)
            bus.emit("tool_step", turn_id=turn_id, n=loop_budget.steps,
                     tool=name, args=_short(args, 120), think=think,
                     summary=summary, ref=ref)
            payload = {k: v for k, v in result.items()
                       if k != "_artifact"} \
                if isinstance(result, dict) else result
            bus.emit("tool_result", turn_id=turn_id, ref=ref,
                     tool=name,
                     content=json.dumps(payload, default=str)[:6000])
            if isinstance(result, dict) and result.get("_artifact"):
                row = result["_artifact"]
                bus.emit("artifact", turn_id=turn_id,
                         artifact_id=row["artifact_id"],
                         version=row["version"], type=row["type"],
                         title=row["title"], spec=row["spec"])
            if name == "ask_user" and isinstance(result, dict) \
                    and result.get("ok"):
                # the turn ends on a question, v1-style: chips carry
                # evidence, the answer arrives as the next message
                store.add_message(
                    session_id, "assistant",
                    " ".join(said) or result["clarify"]["question"],
                    turn_id=turn_id,
                    payload={"clarify": result["clarify"]})
                bus.emit("chips", turn_id=turn_id,
                         clarify=result["clarify"])
                _finish(bus, budget, turn_id, "clarify", started,
                        skills_loaded=list(state.skills_loaded))
                return "clarify"
            steps.append({"n": loop_budget.steps, "tool": name,
                          "args": args, "summary": summary,
                          "ref": ref})

    except Aborted:
        stop_reason = "stopped by the analyst"

    if status != "answered" and stop_reason:
        closing = f"I stopped before finishing: {stop_reason}."
        if said:
            closing = (" ".join(said)[:300]
                       + f"\n\n— I stopped there: {stop_reason}.")
        elif state.notes:
            closing += (" What I had: "
                        + "; ".join(state.notes[-3:]) + ".")
        _stream(closing)
        said = [closing]
        status = "stopped" if "analyst" in stop_reason else "partial"

    prose = "\n\n".join(dict.fromkeys(said)) if said else ""
    if prose or state.artifacts_touched:
        store.add_message(
            session_id, "assistant", prose, turn_id=turn_id,
            payload={"chips": chips,
                     "artifacts": list(dict.fromkeys(
                         state.artifacts_touched))})
    if chips:
        bus.emit("chips", turn_id=turn_id, suggestions=chips)
    if not (session.get("title") or "").strip() and prose:
        title = text.strip()[:60]
        session["title"] = title
        store.set_title(session_id, title)
    _finish(bus, budget, turn_id, status, started,
            steps=loop_budget.steps,
            subgraph_used=state.subgraph,
            skills_loaded=list(state.skills_loaded))
    return status


def _finish(bus: EventBus, budget: Any, turn_id: str, status: str,
            started: float, **extra: Any) -> None:
    bus.emit("turn_done", turn_id=turn_id, status=status,
             elapsed_ms=round((time.perf_counter() - started) * 1000,
                              1),
             **extra, **budget.tick())
