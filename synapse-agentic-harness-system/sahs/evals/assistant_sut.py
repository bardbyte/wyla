"""The assistant as a system under test, for the precedents, silver
and scenarios datasets (``sahs/observe/datasets.py``).

``assistant_sut`` drives one real turn per ``nl2sql`` task through
``AssistantRuntime`` (the same loop the app runs, on whatever model
factory it is handed: a ``ScriptedAgent`` in the tests, the engine of
the environment on a laptop) and reads the answer off the record: the
SQL on the proposal card (or the query the loop ran itself), and the
skills the loader loaded. ``planner_sut`` asks the planner for the
task list of a ``decompose`` task. Both declare the kinds they
answer, so ``run_evals.py`` measures them on those alone.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Callable

from sahs.evals.grading import SutAnswer
from sahs.evals.schema import Task


def answer_from_events(events: list[dict[str, Any]]) -> SutAnswer:
    """What the record says the turn answered: the proposal's SQL
    first, the last ``run_sql`` the loop called second, else an
    abstention with the turn's status as the reason."""
    skills: list[str] = []
    sql = ""
    status = ""
    for record in events:
        ev = record.get("ev")
        if ev == "skills_loaded":
            for name in record.get("skills_loaded") or []:
                if name not in skills:
                    skills.append(str(name))
        elif ev == "proposal":
            proposal = record.get("proposal")
            if isinstance(proposal, dict) and proposal.get("sql"):
                sql = str(proposal["sql"])
        elif ev == "tool_call" and record.get("tool") == "run_sql" and not sql:
            args = record.get("args") if isinstance(record.get("args"),
                                                   dict) else {}
            if args.get("sql"):
                sql = str(args["sql"])
        elif ev == "turn_done" and "." not in str(record.get("turn_id") or ""):
            status = str(record.get("status") or "")
    if sql:
        return SutAnswer(kind="sql", sql=sql, skills=skills)
    return SutAnswer(kind="abstain", reason=f"no sql: {status or 'no turn'}",
                     skills=skills)


def assistant_sut(build: Any, model_factory: Callable[..., Any], *,
                  wait_seconds: float = 180.0, substrate: Any = None,
                  snapshot_runner: Any = None, mode: str = "",
                  depth: str = "", root: Path | None = None
                  ) -> Callable[[Task], SutAnswer]:
    """A SUT that runs each task's prompt as one assistant turn in a
    fresh session and reads the answer off the events."""
    from sahs.assistant import AssistantRuntime
    tmp = Path(root) if root else Path(tempfile.mkdtemp(prefix="sut_"))
    runtime = AssistantRuntime(builds_root=build.root.parent,
                               graph_root=tmp / "graph",
                               store_path=tmp / "chat.sqlite3",
                               model_factory=model_factory,
                               snapshot_runner=snapshot_runner,
                               substrate=substrate)

    def sut(task: Task) -> SutAnswer:
        session = runtime.create_session()
        runtime.start_turn(session["id"], task.prompt, depth=depth,
                           mode=mode)
        runtime.wait(session["id"], wait_seconds)
        events = runtime.runtime(session["id"]).bus.since(0)
        return answer_from_events(events)

    sut.answerable_kinds = ("nl2sql",)          # type: ignore[attr-defined]
    sut.runtime = runtime                       # type: ignore[attr-defined]
    return sut


def planner_sut(model: Any, *, mode: str = "chat",
                depth: str = "medium") -> Callable[[Task], SutAnswer]:
    """A SUT that asks the planner (``sahs.assistant.planner``) to
    split the prompt; None (one job) is an empty plan."""
    from sahs.assistant.planner import plan_for

    def sut(task: Task) -> SutAnswer:
        plan = plan_for(model, task.prompt, mode=mode, depth=depth)
        return SutAnswer(kind="plan",
                         tasks=list(plan["tasks"]) if plan else [])

    sut.answerable_kinds = ("decompose",)       # type: ignore[attr-defined]
    return sut


__all__ = ["answer_from_events", "assistant_sut", "planner_sut"]
