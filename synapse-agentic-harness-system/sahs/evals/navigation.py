"""Navigation evals (Agent Loop v1 §8/§9.4): grade outcomes, read
trajectories.

The task set holds questions whose answer lives in a card the fast
path cannot bind. Each run drives the REAL turn engine (AskRuntime →
run_turn → navigate_loop) with the navigation lane on; the grader
reads the final stored plan and the event stream — never the tool
sequence — and reports:

  * **recall** — did the loop end where the task's evidence says an
    honest analyst would: the expected metric bound (mode ``bind``),
    bound-or-asked (``either``), or an honest non-answer
    (``no_answer``)?
  * **precision** — did it stay out of tables the task forbids? The
    sub-graph the loop records (cards read + the bound table) is the
    evidence.
  * **trajectory hygiene** (soft, §8): tool calls per task, asks per
    task, literal-check rate (sample_values before a filtered plan),
    read-before-use rate (a look before the first bind), budget
    stops. Hygiene never gates: it feeds the weekly trajectory read.

The two-number line for navigation: found% over graded tasks /
wrong-when-found% (a bind outside the task's expectation). The full
per-task table lands beside it, because an aggregate without its
rows is a mood, not a measurement.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Callable

from sahs.tools.api import Build

TASKS_PATH = (Path(__file__).resolve().parents[2] / "tests" / "tasks"
              / "navigation" / "navigation.jsonl")


def load_tasks(path: Path = TASKS_PATH) -> list[dict[str, Any]]:
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


# ─── one task, one session, one verdict ──────────────────────


def _events_for_turn(events: list[dict[str, Any]],
                     name: str) -> list[dict[str, Any]]:
    return [e for e in events if e["ev"] == name]


def grade_task(task: dict[str, Any], events: list[dict[str, Any]],
               plan: dict[str, Any] | None) -> dict[str, Any]:
    """Outcome + hygiene for one completed session. Pure function of
    the records, so a stored events file regrades identically."""
    expect = task.get("expect", {})
    done = _events_for_turn(events, "turn_done")
    status = done[-1]["status"] if done else "missing"
    answered = status == "answered"
    asked = status == "clarify"
    partial = status == "partial"
    plan = plan or {}
    bound = plan.get("metric_id", "")
    table = plan.get("table", "")

    wanted = expect.get("metric_one_of") or []
    metric_ok = (bound in wanted) if wanted else bool(bound)
    table_ok = (table == expect["table"]) if expect.get("table") \
        else True
    filter_ok = True
    if expect.get("filter_column"):
        needles = list(plan.get("filter_bindings", {}).values()) \
            + list(plan.get("filters", {}).keys())
        filter_ok = any(expect["filter_column"] in str(n).lower()
                        for n in needles)
    time_ok = bool(plan.get("time_window")) if expect.get("time") \
        else True
    dims_ok = bool(plan.get("dimensions")) if expect.get("dims") \
        else True

    mode = expect.get("mode", "either")
    bound_right = (metric_ok and table_ok and filter_ok and time_ok
                   and dims_ok)
    if mode == "bind":
        found = answered and bound_right
    elif mode == "no_answer":
        found = not answered           # ask and partial are the wins
    else:                              # either
        found = (answered and bound_right) or asked
    wrong = answered and not bound_right and mode != "no_answer"

    # precision: the sub-graph must stay out of forbidden tables
    loop_done = _events_for_turn(events, "loop_done")
    cards_read: list[str] = []
    steps = 0
    if loop_done:
        steps = int(loop_done[-1].get("steps") or 0)
        cards_read = list((loop_done[-1].get("subgraph_used") or {})
                          .get("cards_read") or [])
    touched = {c.split("/", 1)[1].replace("__", ".")
               for c in cards_read if c.startswith("tables/")}
    if table:
        touched.add(table)
    forbidden = set(expect.get("forbidden_tables") or [])
    dragged = sorted(touched & forbidden)

    # hygiene raw material
    tools_used = [e["tool"] for e in _events_for_turn(events,
                                                      "loop_step")]
    sampled = "sample_values" in tools_used
    first_bind = next((i for i, t in enumerate(tools_used)
                       if t == "plan_set"), None)
    looked_first = (first_bind is None or first_bind > 0
                    or not tools_used)

    return {"id": task["id"], "route": task.get("route", ""),
            "status": status, "found": found, "wrong": wrong,
            "bound": bound, "table": table, "dragged": dragged,
            "precision_ok": not dragged, "steps": steps,
            "asked": asked, "budget_stop": partial,
            "sampled_before_filter": sampled if plan.get("filters")
            else None,
            "looked_before_bind": looked_first if bound else None}


def run_navigation(build: Build, model_factory: Callable[..., Any],
                   tasks: list[dict[str, Any]] | None = None,
                   *, limit: int = 0,
                   wait_seconds: float = 120.0) -> dict[str, Any]:
    """Drive every task through the real engine with navigation on,
    then grade. ``model_factory(budget)`` supplies the model — Vertex
    on the laptop, a script in the harness tests."""
    import os

    from sahs.ask import AskRuntime
    os.environ["SYNAPSE_NAVIGATE"] = "1"
    tasks = list(tasks if tasks is not None else load_tasks())
    if limit:
        tasks = tasks[:limit]
    rows = []
    for task in tasks:
        tmp = Path(tempfile.mkdtemp(prefix="nav_"))
        runtime = AskRuntime(builds_root=build.root.parent,
                             graph_root=tmp / "graph",
                             store_path=tmp / "s.sqlite3",
                             model_factory=model_factory)
        session = runtime.create_session("analyst")
        runtime.start_turn(session["id"], task["question"])
        finished = runtime.wait(session["id"], wait_seconds)
        events = runtime.runtime(session["id"]).bus.since(0)
        plan_row = runtime.store.latest_plan(session["id"])
        row = grade_task(task, events,
                         plan_row["plan"] if plan_row else None)
        if not finished:
            row.update(status="timeout", found=False)
        rows.append(row)
    return summarize(rows)


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    graded = len(rows)
    found = sum(1 for r in rows if r["found"])
    wrong = sum(1 for r in rows if r["wrong"])
    with_filters = [r for r in rows
                    if r["sampled_before_filter"] is not None]
    with_binds = [r for r in rows
                  if r["looked_before_bind"] is not None]
    return {
        "tasks": graded,
        "found": found,
        "found_pct": round(100.0 * found / graded, 1) if graded else 0,
        "wrong_when_found": wrong,
        "precision_violations": sum(1 for r in rows
                                    if not r["precision_ok"]),
        "hygiene": {
            "steps_per_task": round(sum(r["steps"] for r in rows)
                                    / graded, 2) if graded else 0,
            "asks": sum(1 for r in rows if r["asked"]),
            "budget_stops": sum(1 for r in rows if r["budget_stop"]),
            "literal_check_rate": round(
                sum(1 for r in with_filters
                    if r["sampled_before_filter"])
                / len(with_filters), 2) if with_filters else None,
            "read_before_use_rate": round(
                sum(1 for r in with_binds if r["looked_before_bind"])
                / len(with_binds), 2) if with_binds else None,
        },
        "rows": rows,
    }


def render_markdown(report: dict[str, Any], *, label: str) -> str:
    hygiene = report["hygiene"]
    lines = [
        "# Navigation baseline — " + label, "",
        f"**found {report['found_pct']} / "
        f"wrong-when-found {report['wrong_when_found']}** over "
        f"{report['tasks']} tasks · precision violations: "
        f"{report['precision_violations']}", "",
        "Hygiene (soft, feeds the trajectory read): "
        f"{hygiene['steps_per_task']} steps/task · "
        f"{hygiene['asks']} asks · "
        f"{hygiene['budget_stops']} budget stops · "
        f"literal-check {hygiene['literal_check_rate']} · "
        f"read-before-use {hygiene['read_before_use_rate']}", "",
        "| task | route | status | found | bound | dragged |",
        "|---|---|---|---|---|---|",
    ]
    for row in report["rows"]:
        lines.append(
            f"| {row['id']} | {row['route']} | {row['status']} | "
            f"{'yes' if row['found'] else 'NO'} | "
            f"{row['bound'] or '-'} | "
            f"{', '.join(row['dragged']) or '-'} |")
    lines.append("")
    return "\n".join(lines)
