"""delegate_scout (Agent Loop v1 §3/§9.5): the Task analogue — a
read-only worker with a hard cap and a compact report.

Three pins:
  * **read-only, absolutely.** The scout's toolkit is the loop's
    minus every writer: no plan_set, no note, no ask_user. It looks;
    the navigator decides.
  * **no worker spawns workers.** The scout's kit carries no
    delegate_scout — the E18 orchestration pin, preserved by
    construction and by test.
  * **summaries arrive already compact (§6).** At most ~400 tokens,
    with the card ids it leaned on, so the parent context pays a
    fixed price no matter how far the scout wandered — and the
    scout's cards merge into the parent's sub-graph so disclosure
    stays complete.
"""

from __future__ import annotations

from typing import Any

from sahs.tools.api import Build

from .tools import LoopState, render_tool_block, toolkit

SCOUT_MAX_STEPS = 8
SCOUT_SUMMARY_CHARS = 1600         # ≈ 400 tokens
SCOUT_STRIKES = 2

SCOUT_SYSTEM = """You are a read-only scout over a governed data \
graph. Answer ONE question by looking, then report.

Reply with STRICT JSON, one object per step:
  {"think": "<a sentence>", "tool": "<name>", "args": {...}}
  {"summary": "<the finding, at most 400 tokens, naming the cards \
it rests on>"}

You cannot write the plan, ask anyone, or delegate further. If the
budget ends before you are sure, summarize what you found and what
you could not settle — an honest partial beats a guess.

Your tools:

"""


def run_scout(build: Build, model: Any, question: str, *,
              substrate: Any = None,
              max_steps: int = SCOUT_MAX_STEPS) -> dict[str, Any]:
    """One scout errand. Never raises for scout-side trouble: the
    parent gets a summary either way, and the summary says what
    happened."""
    from .loop import _render_context, compact_result   # shared shape

    state = LoopState()
    kit = toolkit(build, state, substrate=substrate)
    kit = {name: spec for name, spec in kit.items() if not spec.writes}
    system = SCOUT_SYSTEM + render_tool_block(kit)
    history: list[dict[str, Any]] = []
    strikes = 0
    steps = 0

    while steps < max_steps:
        prompt = _render_context(question,
                                 "you are scouting for the navigator",
                                 state.plan, state.notes, history,
                                 max_steps - steps, max_steps)
        try:
            step = model.json(prompt, system=system, temperature=0.0,
                              max_tokens=600)
        except Exception as e:
            return _report(f"the scout's model call failed "
                           f"({type(e).__name__}): nothing learned",
                           steps, state)
        steps += 1
        if isinstance(step, dict) and step.get("summary"):
            summary = str(step["summary"])[:SCOUT_SUMMARY_CHARS]
            return _report(summary, steps, state)
        if not isinstance(step, dict) or not step.get("tool"):
            strikes += 1
            history.append({"note": "reply with one strict JSON "
                                    "step, or {\"summary\": ...}"})
            if strikes >= SCOUT_STRIKES:
                return _report("the scout stopped: it could not "
                               "keep to the step protocol", steps,
                               state)
            continue
        name = str(step.get("tool", ""))
        args = step.get("args") if isinstance(step.get("args"),
                                              dict) else {}
        spec = kit.get(name)
        if spec is None:
            history.append({"note": f"unknown tool {name!r}: the "
                                    "scout's tools are "
                                    + ", ".join(kit)})
            continue
        try:
            result = spec.fn(**args)
        except TypeError:
            result = {"error": "the args did not match the signature",
                      "hint": spec.signature}
        except Exception as e:
            result = {"error": f"{type(e).__name__}: {e}",
                      "hint": "try a different call"}
        history.append({"n": steps, "tool": name, "args": args,
                        "summary": compact_result(name, result)})

    return _report("the scout ran out of budget; what it read is in "
                   "cards_read", steps, state)


def _report(summary: str, steps: int,
            state: LoopState) -> dict[str, Any]:
    return {"summary": summary[:SCOUT_SUMMARY_CHARS], "steps": steps,
            "cards_read": list(state.subgraph["cards_read"])}
