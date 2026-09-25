"""The planner (multi-task turns, docs/multi-task-turns.md): decide
whether one message is several jobs, and split it when it is.

Two gates, in order, so a simple ask costs nothing extra:

1. ``should_plan`` is deterministic and cheap — length, conjunctions,
   enumerations, several question marks, several ask verbs. Most
   messages fail it and the turn runs exactly as it always did: no
   model call, no new event, the same bytes in the prompt.
2. Behind it, ONE JSON one-shot (the same ``model.json`` door the
   judge, the title and the memory pass use; the agent folds the
   temperature policy and the "json" thinking stop per engine) returns
   the plan: at most six tasks, each with a goal a colleague could do
   from the goal alone, its dependencies, and its kind.

The plan is never trusted blindly: ``validate_plan`` renumbers the
ids, drops unknown and self dependencies, breaks cycles, drops empty
goals, and gives up (no plan — one ordinary turn) when fewer than two
tasks survive or more than six were asked for. Every repair is kept
on the plan so the record says what was changed.
"""

from __future__ import annotations

import json
import re
from typing import Any

MAX_TASKS = 6
MIN_TASKS = 2
KINDS = ("answer", "query", "artifact")
DEFAULT_KIND = "answer"
GOAL_CAP = 400
SYNTHESIS_CAP = 600
# a message shorter than this cannot hold two jobs worth splitting
MIN_WORDS = 8
LONG_ASK = 240

# words that join one job to the next; the strong ones alone make a
# message worth the planner's look
_STRONG = re.compile(
    r"\b(and also|as well as|after that|then|for each|followed by|"
    r"once you have|once that is done|in addition)\b", re.IGNORECASE)
_WEAK = re.compile(r"\b(also|plus|next|finally|lastly|besides)\b",
                   re.IGNORECASE)
# "1. … 2. …", "a) …", "- …" at the start of lines, or inline "1) … 2)"
_ENUM = re.compile(r"(?:(?<=^)|(?<=\n)|(?<=\s))(?:\d+[.)]|[a-hA-H][.)]|[-*•])\s+\S")
_ASK_VERBS = re.compile(
    r"\b(compare|explain|build|show|list|chart|draft|check|find|"
    r"summari[sz]e|break down|reconcile|forecast|describe|count|"
    r"measure|rank|plot|write|create|tell me|give me|what|which|why|how)"
    r"\b", re.IGNORECASE)


def compound_signals(text: str) -> dict[str, int]:
    """The deterministic signals that a message holds several jobs,
    each counted; ``score`` is their weighted sum."""
    text = str(text or "")
    strong = len(_STRONG.findall(text))
    weak = len(_WEAK.findall(text))
    enum = len(_ENUM.findall(text))
    questions = text.count("?")
    asks = len(_ASK_VERBS.findall(text))
    score = 0
    score += 2 * min(strong, 2)
    score += min(weak, 2)
    if enum >= 2:
        score += 2
    if questions >= 2:
        score += 2
    if asks >= 3:
        score += 1
    if len(text) >= LONG_ASK:
        score += 1
    return {"strong": strong, "weak": weak, "enumeration": enum,
            "questions": questions, "asks": asks,
            "long": int(len(text) >= LONG_ASK), "score": score}


def should_plan(text: str, mode: str = "chat", depth: str = "medium") -> bool:
    """The cheap gate: True when the message looks like several jobs
    and the depth allows the planner's one extra call. Minimal depth
    never plans (a one-line answer was asked for); both modes may."""
    text = str(text or "").strip()
    if (depth or "").strip().lower() == "minimal":
        return False
    if len(text.split()) < MIN_WORDS:
        return False
    return compound_signals(text)["score"] >= 2


# ─── the one-shot ────────────────────────────────────────────

PLAN_SYSTEM = """You split one message from a person to an analytical \
assistant into the separate jobs it asks for. You do none of the jobs.

Return STRICT JSON, nothing else:
{"tasks": [{"id": "t1", "goal": "...", "depends_on": [], "kind": "answer|query|artifact"}], "synthesis": "..."}

Rules:
- At most 6 tasks. One task per distinct question or deliverable. If the message is one job, return {"tasks": [], "synthesis": ""}.
- A goal is self-contained: a colleague could do it from the goal alone. Keep the person's own words, names and filters in it. Never add asks the person did not make.
- depends_on names only tasks whose OUTPUT this one needs (rows, a finding, an artifact). Independent tasks have depends_on []. No cycles.
- kind: answer (explain or reason, no data needed), query (needs the graph or the warehouse), artifact (a chart, table, dashboard or document is the deliverable).
- synthesis: one sentence on how the final answer should be put together (what leads, what is compared)."""


def plan_prompt(text: str, mode: str = "chat") -> str:
    return (f"MODE: {mode or 'chat'}\n\nMESSAGE:\n{str(text or '').strip()}")


def plan_for(model: Any, text: str, *, mode: str = "chat",
             depth: str = "medium") -> dict[str, Any] | None:
    """The plan for a message, or None: the gate first, then one JSON
    one-shot, then the validator. A model that returns nothing usable
    is a None, never an error — the turn runs as one."""
    if not should_plan(text, mode, depth):
        return None
    raw = model.json(plan_prompt(text, mode), system=PLAN_SYSTEM,
                     temperature=0.0, max_tokens=1024)
    plan, _notes = validate_plan(raw)
    return plan


# ─── validation and repair ───────────────────────────────────

_ID = re.compile(r"^[A-Za-z0-9_-]{1,24}$")


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        return [v.strip() for v in value.split(",") if v.strip()]
    return []


def validate_plan(raw: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """→ (plan, notes). The plan holds ``tasks`` in an order that
    respects their dependencies (ids renumbered t1..tN), ``synthesis``
    and ``repairs`` (the notes). None when there is no plan worth
    running: not an object, no tasks, one task, more than six, or
    nothing left after repair."""
    notes: list[str] = []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return None, ["the plan was not JSON"]
    if not isinstance(raw, dict):
        return None, ["the plan was not an object"]
    tasks_in = raw.get("tasks")
    if not isinstance(tasks_in, list) or not tasks_in:
        return None, ["no tasks: one job"]
    if len(tasks_in) > MAX_TASKS:
        return None, [f"{len(tasks_in)} tasks is over the ceiling of "
                      f"{MAX_TASKS}: running the ask as one turn"]

    # 1 · shape: a dict with a goal; ids renumbered, the given id kept
    #     only to resolve dependencies
    given_to_new: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    for i, item in enumerate(tasks_in):
        if not isinstance(item, dict):
            notes.append(f"task {i + 1} was not an object: dropped")
            continue
        goal = str(item.get("goal") or "").strip()
        if not goal:
            notes.append(f"task {i + 1} had no goal: dropped")
            continue
        new_id = f"t{len(rows) + 1}"
        given = str(item.get("id") or "").strip()
        if given and _ID.match(given):
            if given in given_to_new:
                notes.append(f"duplicate id {given!r}: renumbered")
            else:
                given_to_new[given] = new_id
        if given != new_id and given:
            notes.append(f"{given!r} renumbered to {new_id}")
        kind = str(item.get("kind") or "").strip().lower()
        if kind not in KINDS:
            if kind:
                notes.append(f"{new_id}: unknown kind {kind!r} read as "
                             f"{DEFAULT_KIND}")
            kind = DEFAULT_KIND
        rows.append({"id": new_id, "goal": goal[:GOAL_CAP],
                     "kind": kind,
                     "_deps": [str(d).strip() for d in
                               _as_list(item.get("depends_on"))]})
        given_to_new.setdefault(new_id, new_id)

    # 2 · dependencies: known ids only (given or renumbered), never
    #     itself, each once
    ids = {r["id"] for r in rows}
    for row in rows:
        deps: list[str] = []
        for dep in row["_deps"]:
            resolved = given_to_new.get(dep) or (dep if dep in ids else "")
            if not resolved:
                notes.append(f"{row['id']}: dependency {dep!r} is not a "
                             "task: dropped")
            elif resolved == row["id"]:
                notes.append(f"{row['id']}: depends on itself: dropped")
            elif resolved not in deps:
                deps.append(resolved)
        row["depends_on"] = deps
        del row["_deps"]

    # 3 · acyclic: Kahn's order; what is left in a cycle loses its
    #     remaining dependencies and runs as independent work
    ordered: list[dict[str, Any]] = []
    remaining = list(rows)
    done: set[str] = set()
    while remaining:
        # the first task whose inputs are done, one per pass, so the
        # model's own order survives wherever it was already valid
        ready = [r for r in remaining
                 if all(d in done for d in r["depends_on"])][:1]
        if not ready:
            # one edge at a time: the first task still waiting loses
            # the dependencies that cannot be met, and the order is
            # recomputed, so a two-task cycle keeps one of its edges
            r = remaining[0]
            cut = [d for d in r["depends_on"] if d not in done]
            notes.append(f"{r['id']}: a dependency cycle through "
                         + ", ".join(cut) + " was broken")
            r["depends_on"] = [d for d in r["depends_on"] if d in done]
            continue
        for r in ready:
            ordered.append(r)
            done.add(r["id"])
            remaining.remove(r)

    if len(ordered) < MIN_TASKS:
        notes.append(f"{len(ordered)} task(s) after repair: one job")
        return None, notes
    synthesis = str(raw.get("synthesis") or "").strip()[:SYNTHESIS_CAP]
    return {"tasks": ordered, "synthesis": synthesis,
            "repairs": notes, "source": "model"}, notes


def waves(tasks: list[dict[str, Any]]) -> list[list[str]]:
    """The tasks grouped by the earliest wave each can run in: the
    first wave is everything without dependencies, the next what
    depends only on the first, and so on. For the record and the docs;
    the runner schedules dynamically."""
    depth: dict[str, int] = {}
    for t in tasks:
        depth[t["id"]] = 1 + max((depth.get(d, 0) for d in t["depends_on"]),
                                 default=0)
    out: list[list[str]] = []
    for t in tasks:
        level = depth[t["id"]] - 1
        while len(out) <= level:
            out.append([])
        out[level].append(t["id"])
    return out


__all__ = ["MAX_TASKS", "KINDS", "PLAN_SYSTEM", "compound_signals",
           "should_plan", "plan_prompt", "plan_for", "validate_plan",
           "waves"]
