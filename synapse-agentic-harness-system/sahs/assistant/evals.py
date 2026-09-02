"""The v2 eval suites (Synapse v2 §11/§13.6): grade outcomes, read
trajectories — the navigation harness's doctrine, applied to the
assistant.

Three suites, each with its own two-number line:

  * **artifact** — did the deliverable land, and is it honest?
    ``produced% / undisclosed-when-produced%``. Structural grading
    re-reads the RECORD (the artifact events): every numeric part
    carries provenance, composed-without-cited-facts wears the
    watermark, expected statuses appear, and the PPTX export
    actually builds.
  * **reasoning** — a thinking question gets thinking.
    ``answered-clean% / tool-leak%``. Clean = no tools (within the
    task's allowance), enough words, and the rubric satisfied — by
    keyword floor in the harness, by a calibrated judge when one is
    passed (the real baseline).
  * **playbook** — a "why" must run the decomposition WITH its
    checks. ``checked% / unchecked-answer%``. Evidence is the event
    stream: check tools called with passing facts, enough queries,
    the playbook pack actually loaded when the task names one.

Trajectory hygiene rides along and never gates: sampled-before-
filtered, read-before-queried, steps per task, artifact refusals
and whether the model recovered from them (the teaching loop,
measured). Graders are pure functions of the records, so a stored
event file regrades identically. E19's capability matrix stays; this
adds the assistant lanes beside it.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Callable

from sahs.tools.api import Build

TASKS_ROOT = (Path(__file__).resolve().parents[2] / "tests" / "tasks"
              / "assistant")
KINDS = ("artifact", "reasoning", "playbook", "recovery")
# v3: one check tool, six kinds — task files name the KINDS
CHECK_KINDS = ("part_whole", "crosscheck", "coverage", "fanout",
               "reconcile", "answer")
CHECK_TOOLS = CHECK_KINDS
LOOK_TOOLS = ("search", "read", "sample_values")
# calls that are not work: they never count as "tools used"
QUIET_TOOLS = ("suggest_next", "note", "remember")


def load_tasks(kind: str | None = None) -> list[dict[str, Any]]:
    kinds = [kind] if kind else list(KINDS)
    tasks: list[dict[str, Any]] = []
    for name in kinds:
        path = TASKS_ROOT / f"{name}.jsonl"
        tasks += [json.loads(line) for line in
                  path.read_text(encoding="utf-8").splitlines()
                  if line.strip()]
    return tasks


# ─── reading the record ──────────────────────────────────────


def _by(events: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    return [e for e in events if e["ev"] == name]


def _prose(events: list[dict[str, Any]]) -> str:
    return "".join(e.get("delta", "") for e in _by(events, "say_token"))


def _final_artifacts(events: list[dict[str, Any]]
                     ) -> list[dict[str, Any]]:
    """Latest version of each artifact the turn produced, straight
    from the artifact events (the record, not the store)."""
    latest: dict[str, dict[str, Any]] = {}
    for event in _by(events, "artifact"):
        held = latest.get(event["artifact_id"])
        if held is None or event["version"] >= held["version"]:
            latest[event["artifact_id"]] = event
    return list(latest.values())


def _numeric_parts(type: str, spec: dict[str, Any]
                   ) -> list[dict[str, Any]]:
    """The (type, spec) leaves that show numbers and owe disclosure."""
    if type == "dashboard":
        parts = []
        for panel in spec.get("panels") or []:
            parts += _numeric_parts(panel.get("type", ""),
                                    panel.get("spec") or {})
        return parts
    if type in ("chart", "kpi"):
        return [spec]
    if type == "table":
        numeric = any(isinstance(v, (int, float))
                      and not isinstance(v, bool)
                      for r in spec.get("rows") or [] if isinstance(r, dict)
                      for v in r.values())
        return [spec] if numeric else []
    return []


def _disclosure_problems(type: str, spec: dict[str, Any]) -> list[str]:
    problems = []
    for part in _numeric_parts(type, spec):
        prov = part.get("provenance") or {}
        if not prov.get("status") or not prov.get("meridian_line"):
            problems.append("undisclosed_number")
        if prov.get("status") == "composed" \
                and not prov.get("facts_verified") \
                and part.get("watermark") != "EXPLORATORY":
            problems.append("naked_composed")
    return problems


def _statuses(type: str, spec: dict[str, Any]) -> set[str]:
    return {str((p.get("provenance") or {}).get("status", ""))
            for p in _numeric_parts(type, spec)} - {""}


def _work_steps(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in _by(events, "tool_step")
            if e.get("tool") not in QUIET_TOOLS]


def _hygiene(events: list[dict[str, Any]]) -> dict[str, Any]:
    steps = _work_steps(events)
    tools = [e["tool"] for e in steps]
    first_query = next((i for i, e in enumerate(steps)
                        if e["tool"] == "run_sql"), None)
    filtered = any("'" in str(e.get("args", ""))
                   for e in steps if e["tool"] == "run_sql")
    refusals = sum(1 for e in steps
                   if e["tool"] == "artifact"
                   and "refused" in str(e.get("summary", "")))
    landed = any(e["tool"] == "artifact"
                 and "refused" not in str(e.get("summary", ""))
                 and "ERROR" not in str(e.get("summary", ""))
                 for e in steps)
    # a failed dry run is the model's to fix when it is sql or cost,
    # and the user's when it is environment or access (the taught
    # error says which): recovery, and not looping, are graded
    sql_steps = [e for e in steps if e["tool"] == "run_sql"]
    failed = [i for i, e in enumerate(sql_steps)
              if str(e.get("summary", "")).startswith("ERROR")]
    fixed = [i for i, e in enumerate(sql_steps)
             if not str(e.get("summary", "")).startswith("ERROR")]
    recovered = (any(i > failed[0] for i in fixed)
                 if failed else None)
    results = [e for e in _by(events, "tool_result")
               if e.get("tool") == "run_sql"]
    env_at = next((i for i, e in enumerate(results)
                   if '"kind": "environment"' in str(e.get("content", ""))
                   or '"kind": "access"' in str(e.get("content", ""))),
                  None)
    retries_after_env = (len(results) - env_at - 1
                         if env_at is not None else None)
    return {
        "sql_failures": len(failed),
        "recovered_from_sql_error": recovered,
        "retries_after_environment": retries_after_env,
        "steps": len(steps),
        "sampled_before_filter": (
            None if not filtered else any(
                e["tool"] == "sample_values"
                for e in steps[:first_query or 0])),
        "read_before_query": (
            None if first_query is None else any(
                e["tool"] in LOOK_TOOLS
                for e in steps[:first_query])),
        "artifact_refusals": refusals,
        "recovered_from_refusal": (landed if refusals else None),
        "tools": tools,
    }


# ─── the graders: pure functions of the records ──────────────


def grade_artifact(task: dict[str, Any],
                   events: list[dict[str, Any]]) -> dict[str, Any]:
    expect = task.get("expect", {})
    rows = _final_artifacts(events)
    hit = next((r for r in rows if r["type"] == expect.get("type")),
               None)
    produced = hit is not None
    problems: list[str] = []
    export_ok: bool | None = None
    statuses: set[str] = set()
    if hit:
        spec = hit.get("spec") or {}
        problems = _disclosure_problems(hit["type"], spec)
        statuses = _statuses(hit["type"], spec)
        for want in expect.get("statuses") or []:
            if want not in statuses:
                problems.append(f"missing_status:{want}")
        if expect.get("min_panels") and len(
                spec.get("panels") or []) < expect["min_panels"]:
            problems.append("too_few_panels")
        if expect.get("export"):
            try:
                from .export import artifact_pptx
                export_ok = artifact_pptx(
                    {"type": hit["type"], "title": hit["title"],
                     "version": hit["version"],
                     "spec": spec})[:2] == b"PK"
            except Exception:
                export_ok = False
            if not export_ok:
                problems.append("export_failed")
    found = produced and not problems
    return {"id": task["id"], "kind": "artifact",
            "produced": produced, "found": found,
            "wrong": produced and bool(problems),
            "problems": problems, "statuses": sorted(statuses),
            "export_ok": export_ok, "hygiene": _hygiene(events)}


def grade_reasoning(task: dict[str, Any],
                    events: list[dict[str, Any]],
                    judge: Callable[[str, str], bool] | None = None
                    ) -> dict[str, Any]:
    expect = task.get("expect", {})
    prose = _prose(events)
    words = len(prose.split())
    tools = len(_work_steps(events))
    done = _by(events, "turn_done")
    answered = bool(done) and done[-1]["status"] == "answered"
    leak = tools > int(expect.get("max_tools", 0))
    lower = prose.lower()
    keyword_ok = all(any(k in lower for k in group)
                     for group in expect.get("must_mention") or [])
    judged = judge(task["question"], prose) if judge else None
    substance = (judged if judged is not None else keyword_ok) \
        and words >= int(expect.get("min_words", 0))
    found = answered and not leak and substance
    return {"id": task["id"], "kind": "reasoning",
            "found": found, "wrong": answered and leak,
            "answered": answered, "tools_used": tools,
            "words": words, "keyword_ok": keyword_ok,
            "judged": judged, "hygiene": _hygiene(events)}


def grade_playbook(task: dict[str, Any],
                   events: list[dict[str, Any]]) -> dict[str, Any]:
    expect = task.get("expect", {})
    steps = _by(events, "tool_step")
    done = _by(events, "turn_done")
    answered = bool(done) and done[-1]["status"] == "answered"
    queries = sum(1 for e in steps if e["tool"] == "run_sql")
    wanted = expect.get("checks_any") or list(CHECK_KINDS)
    passing: set[str] = set()
    for event in _by(events, "tool_result"):
        content = str(event.get("content", ""))
        if event.get("tool") != "check" \
                or '"passed": true' not in content:
            continue
        for kind in wanted:
            if f'"kind": "{kind}"' in content:
                passing.add(kind)
    checked = bool(passing)
    skill_ok = True
    if expect.get("skill"):
        loaded = list((done[-1].get("skills_loaded") if done else [])
                      or [])
        started = _by(events, "turn_started")
        preloaded = list((started[-1].get("skills") if started else [])
                         or [])
        skill_ok = expect["skill"] in loaded + preloaded
    enough = queries >= int(expect.get("min_queries", 0))
    found = answered and checked and skill_ok and enough
    return {"id": task["id"], "kind": "playbook",
            "found": found,
            "wrong": answered and not checked,
            "answered": answered, "checks_passed": sorted(passing),
            "queries": queries, "skill_ok": skill_ok,
            "hygiene": _hygiene(events)}


def grade_recovery(task: dict[str, Any],
                   events: list[dict[str, Any]]) -> dict[str, Any]:
    """A failed dry run: the model fixes what is its own and retries,
    or recognises configuration, tells the user, and stops."""
    expect = task.get("expect", {})
    done = _by(events, "turn_done")
    answered = bool(done) and done[-1]["status"] == "answered"
    hygiene = _hygiene(events)
    prose = _prose(events).lower()
    queries = sum(1 for e in _work_steps(events)
                  if e["tool"] == "run_sql")
    behaved: list[bool] = []
    if expect.get("recovers_from_sql_error"):
        behaved.append(hygiene["recovered_from_sql_error"] is True)
    if expect.get("stops_on_environment"):
        retries = hygiene["retries_after_environment"]
        behaved.append(retries is not None
                       and retries <= int(expect.get("max_retries", 1)))
    mentions_ok = all(any(k.lower() in prose for k in group)
                      for group in expect.get("mentions_any") or [])
    enough = queries >= int(expect.get("min_queries", 0))
    found = answered and all(behaved) and mentions_ok and enough
    return {"id": task["id"], "kind": "recovery",
            "found": found,
            "wrong": answered and not (all(behaved) and mentions_ok),
            "answered": answered, "queries": queries,
            "mentions_ok": mentions_ok, "hygiene": hygiene}


GRADERS = {"artifact": grade_artifact, "reasoning": grade_reasoning,
           "playbook": grade_playbook, "recovery": grade_recovery}


def grade(task: dict[str, Any], events: list[dict[str, Any]],
          judge: Callable[[str, str], bool] | None = None
          ) -> dict[str, Any]:
    kind = task.get("kind", "artifact")
    if kind == "reasoning":
        return grade_reasoning(task, events, judge)
    return GRADERS[kind](task, events)


# ─── driving the real engine ─────────────────────────────────


def run_task(build: Build, model_factory: Callable[..., Any],
             task: dict[str, Any], *, wait_seconds: float = 180.0,
             judge: Callable[[str, str], bool] | None = None,
             snapshot_runner: Any = None,
             substrate: Any = None) -> dict[str, Any]:
    from sahs.assistant import AssistantRuntime
    fault = task.get("fault") or {}
    if fault.get("dry_run_errors"):
        # the same task fails the same way offline and on the laptop:
        # the first dry runs answer with the scripted warehouse errors,
        # the rest go through to whatever substrate is real here
        from sahs.evals.substrate import FaultySubstrate
        substrate = FaultySubstrate(list(fault["dry_run_errors"]),
                                    inner=substrate)
    tmp = Path(tempfile.mkdtemp(prefix="chat_eval_"))
    runtime = AssistantRuntime(builds_root=build.root.parent,
                               graph_root=tmp / "graph",
                               store_path=tmp / "chat.sqlite3",
                               model_factory=model_factory,
                               snapshot_runner=snapshot_runner,
                               substrate=substrate)
    session = runtime.create_session()
    runtime.start_turn(session["id"], task["question"])
    finished = runtime.wait(session["id"], wait_seconds)
    events = runtime.runtime(session["id"]).bus.since(0)
    row = grade(task, events, judge)
    if not finished:
        row.update(found=False, timeout=True)
    return row


def run_suite(build: Build, model_factory: Callable[..., Any],
              tasks: list[dict[str, Any]] | None = None, *,
              limit: int = 0, wait_seconds: float = 180.0,
              judge: Callable[[str, str], bool] | None = None,
              snapshot_runner: Any = None,
              substrate: Any = None) -> dict[str, Any]:
    tasks = list(tasks if tasks is not None else load_tasks())
    if limit:
        tasks = tasks[:limit]
    rows = [run_task(build, model_factory, task,
                     wait_seconds=wait_seconds, judge=judge,
                     snapshot_runner=snapshot_runner,
                     substrate=substrate)
            for task in tasks]
    return summarize(rows)


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def line(kind: str) -> dict[str, Any]:
        mine = [r for r in rows if r["kind"] == kind]
        if not mine:
            return {}
        found = sum(1 for r in mine if r["found"])
        wrong = sum(1 for r in mine if r["wrong"])
        return {"tasks": len(mine), "found": found,
                "found_pct": round(100.0 * found / len(mine), 1),
                "wrong": wrong}
    hygienes = [r["hygiene"] for r in rows]

    def rate(key: str) -> float | None:
        seen = [h[key] for h in hygienes if h.get(key) is not None]
        return round(sum(1 for s in seen if s) / len(seen), 2) \
            if seen else None
    stops = [h["retries_after_environment"] for h in hygienes
             if h.get("retries_after_environment") is not None]
    return {
        "tasks": len(rows),
        "suites": {k: line(k) for k in KINDS if line(k)},
        "hygiene": {
            "steps_per_task": round(
                sum(h["steps"] for h in hygienes) / len(rows), 2)
            if rows else 0,
            "literal_check_rate": rate("sampled_before_filter"),
            "read_before_query_rate": rate("read_before_query"),
            "artifact_refusals": sum(h["artifact_refusals"]
                                     for h in hygienes),
            "refusal_recovery_rate": rate("recovered_from_refusal"),
            "sql_failures": sum(h.get("sql_failures", 0)
                                for h in hygienes),
            "sql_recovery_rate": rate("recovered_from_sql_error"),
            "environment_stop_rate": (
                round(sum(1 for r in stops if r <= 1) / len(stops), 2)
                if stops else None),
        },
        "rows": rows,
    }


def render_markdown(report: dict[str, Any], *, label: str) -> str:
    lines = ["# Assistant eval suites — " + label, ""]
    names = {"artifact": "produced-and-honest / dishonest-when-"
                         "produced",
             "reasoning": "answered-clean / tool-leak",
             "playbook": "checked / unchecked-answer",
             "recovery": "recovered-or-reported / looped-or-silent"}
    for kind, suite in report["suites"].items():
        lines.append(f"**{kind}: {suite['found_pct']}% / "
                     f"{suite['wrong']}** ({names[kind]}, "
                     f"{suite['tasks']} tasks)")
    hygiene = report["hygiene"]
    lines += ["",
              f"hygiene — steps/task {hygiene['steps_per_task']} · "
              f"literal-check {hygiene['literal_check_rate']} · "
              f"read-before-query "
              f"{hygiene['read_before_query_rate']} · "
              f"refusals {hygiene['artifact_refusals']} "
              f"(recovery {hygiene['refusal_recovery_rate']}) · "
              f"sql failures {hygiene.get('sql_failures', 0)} "
              f"(recovery {hygiene.get('sql_recovery_rate')}, "
              f"configuration stop "
              f"{hygiene.get('environment_stop_rate')})", "",
              "| task | kind | found | notes |",
              "| --- | --- | --- | --- |"]
    for row in report["rows"]:
        notes = row.get("problems") or row.get("checks_passed") \
            or ("no tools" if row["kind"] == "reasoning"
                and not row.get("tools_used") else "")
        if row["kind"] == "recovery":
            h = row.get("hygiene") or {}
            notes = ("recovered" if h.get("recovered_from_sql_error")
                     else "reported configuration"
                     if h.get("retries_after_environment") == 0
                     else f"retries {h.get('retries_after_environment')}"
                     if h.get("retries_after_environment") is not None
                     else "no recovery")
        lines.append(f"| {row['id']} | {row['kind']} | "
                     f"{'✓' if row['found'] else '✗'} | "
                     f"{', '.join(notes) if isinstance(notes, list) else notes} |")
    return "\n".join(lines) + "\n"


__all__ = ["KINDS", "load_tasks", "grade", "grade_artifact",
           "grade_reasoning", "grade_playbook", "run_task",
           "run_suite", "summarize", "render_markdown"]
