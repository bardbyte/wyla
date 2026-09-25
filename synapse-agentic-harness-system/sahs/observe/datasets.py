"""Three dataset builders beside the task files: precedents (gold),
silver (production turns worth keeping) and scenarios (compound asks
and their plans). Each is a pure function of rows that yields Langfuse
dataset items; ``write_jsonl`` puts them in a file to review, and
``push_items`` uploads a file's worth. The eval harness reads the same
items back as tasks (``items_to_tasks``) so ``run_evals.py --langfuse``
runs against any of them.

The item shape (one JSON object per line)::

    {"schema": "wyla.precedent/1" | "wyla.silver/1" | "wyla.scenario/1",
     "id": ..., "input": {"prompt": ...}, "expected_output": {...},
     "metadata": {...}}

Precedents are the analyst question-to-SQL pairs. Nothing is checked
in under that name yet, so the loader reads a documented JSONL shape
(``PRECEDENT_SCHEMA``, one row per pair)::

    {"id": "prec_001", "question": "...", "sql": "SELECT ...",
     "skill": "<the skill pack the answer lives in>",
     "frame": {"metric": ..., "population": ..., "grain": ...,
               "dates": ..., "joins": ...},
     "source": "<where the pair came from>", "notes": "..."}

``tests/fixtures/precedents/precedents.jsonl`` is the example.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from sahs.canon.canonical import try_canon

from .record import DONE_STATUSES

PRECEDENT_SCHEMA = "wyla.precedent/1"
SILVER_SCHEMA = "wyla.silver/1"
SCENARIO_SCHEMA = "wyla.scenario/1"
ITEM_SCHEMAS = (PRECEDENT_SCHEMA, SILVER_SCHEMA, SCENARIO_SCHEMA)
DATASET_NAMES = {PRECEDENT_SCHEMA: "wyla-precedents",
                 SILVER_SCHEMA: "wyla-silver",
                 SCENARIO_SCHEMA: "wyla-scenarios"}
BUILDERS = ("precedents", "silver", "scenarios")


def _fp(sql: str) -> str:
    result, err = try_canon(sql or "")
    return "" if err is not None or result is None else (result.fp_expr or "")


# ── precedents: the gold ─────────────────────────────────────


def read_precedents(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def precedent_items(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """One item per pair: the question in, the SQL, its semantic
    frame and the skill expected out; the canonical fingerprint in
    metadata so the harness grades by fingerprint, not by string."""
    items = []
    for i, row in enumerate(rows, 1):
        question = str(row.get("question") or row.get("prompt") or "").strip()
        sql = str(row.get("sql") or "").strip()
        if not question or not sql:
            continue
        item_id = str(row.get("id") or f"prec_{i:03d}")
        items.append({
            "schema": PRECEDENT_SCHEMA, "id": item_id,
            "input": {"prompt": question},
            "expected_output": {"sql": sql,
                                "frame": dict(row.get("frame") or {}),
                                "skill": str(row.get("skill") or "")},
            "metadata": {"source": str(row.get("source") or "precedents"),
                         "skill": str(row.get("skill") or ""),
                         "notes": str(row.get("notes") or ""),
                         "canonical_fp": _fp(sql),
                         "tags": list(row.get("tags") or [])}})
    return items


# ── silver: production turns worth keeping ────────────────────


def is_silver(turn: dict[str, Any]) -> bool:
    """A turn a person thumbed up, or one that finished with nothing
    refused — and never one thumbed down."""
    votes = {str(v.get("vote")) for v in turn.get("feedback") or []}
    if "down" in votes:
        return False
    if "up" in votes:
        return True
    return turn.get("status") in DONE_STATUSES and not turn.get("refused")


def silver_items(turns: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """The silver set: question in; the SQL on the card (when there
    was one) and the answer out; what loaded, the build and the prompt
    version beside them; the source turn named in metadata so a
    reviewer can open the trace."""
    items = []
    for turn in turns:
        if not turn.get("question") or not is_silver(turn):
            continue
        sql = str(turn.get("sql") or "")
        proposal = turn.get("proposal") or {}
        items.append({
            "schema": SILVER_SCHEMA,
            "id": f"{turn['session_id']}/{turn['turn_id']}",
            "input": {"prompt": turn["question"],
                      "skills": list(turn.get("skills") or []),
                      "mode": turn.get("mode", "")},
            "expected_output": {
                "sql": sql,
                "answer": str(turn.get("answer") or "")[:4000],
                "title": str(proposal.get("title") or ""),
                "metric_id": str(proposal.get("metric_id") or "")},
            "metadata": {
                "source_turn": {"session_id": turn["session_id"],
                                "turn_id": turn["turn_id"],
                                "owner": turn.get("owner", "")},
                "build_id": turn.get("build_id", ""),
                "plane": turn.get("plane", ""),
                "prompt_version": turn.get("prompt_version", ""),
                "skills_loaded": list(turn.get("skills_loaded") or []),
                "status": turn.get("status", ""),
                "feedback": [str(v.get("vote")) for v in
                             turn.get("feedback") or []],
                "canonical_fp": _fp(sql) if sql else "",
                "artifacts": [a.get("type") for a in
                              turn.get("artifacts") or []],
                "started_at": turn.get("started_at", "")}})
    return items


# ── scenarios: compound asks and their plans ─────────────────


def scenario_items(turns: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Every turn a planner split: the ask in, the task list (ids,
    goals, kinds, dependencies), the synthesis line and the waves
    out; how each task ended in metadata."""
    items = []
    for turn in turns:
        plan = turn.get("plan")
        if not isinstance(plan, dict) or not plan.get("tasks"):
            continue
        tasks = [{"id": t.get("id"), "goal": t.get("goal"),
                  "kind": t.get("kind", "answer"),
                  "depends_on": list(t.get("depends_on") or [])}
                 for t in plan["tasks"] if isinstance(t, dict)]
        items.append({
            "schema": SCENARIO_SCHEMA,
            "id": f"{turn['session_id']}/{turn['turn_id']}",
            "input": {"prompt": turn.get("question", ""),
                      "mode": turn.get("mode", "")},
            "expected_output": {"tasks": tasks,
                                "synthesis": str(plan.get("synthesis") or ""),
                                "waves": plan.get("waves")},
            "metadata": {
                "source_turn": {"session_id": turn["session_id"],
                                "turn_id": turn["turn_id"],
                                "owner": turn.get("owner", "")},
                "build_id": turn.get("build_id", ""),
                "prompt_version": turn.get("prompt_version", ""),
                "status": turn.get("status", ""),
                "task_status": {str(t.get("task")): t.get("status")
                                for t in turn.get("tasks") or []},
                "repairs": list(plan.get("repairs") or []),
                "started_at": turn.get("started_at", "")}})
    return items


# ── files and upload ─────────────────────────────────────────


def write_jsonl(items: Iterable[dict[str, Any]], path: Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def read_items(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in
            Path(path).read_text(encoding="utf-8").splitlines()
            if line.strip()]


def is_item_file(rows: list[dict[str, Any]]) -> bool:
    return bool(rows) and str(rows[0].get("schema", "")) in ITEM_SCHEMAS


def push_items(client: Any, items: list[dict[str, Any]], *,
               name: str = "", description: str = "",
               metadata: dict[str, Any] | None = None) -> tuple[str, int]:
    """Upsert one dataset (named by the items' schema unless told)
    with the items keyed by id: a re-push updates in place."""
    if not items:
        return name, 0
    name = name or DATASET_NAMES.get(str(items[0].get("schema")), "wyla-items")
    client.create_dataset(name=name, description=description or name,
                          metadata={"schema": items[0].get("schema"),
                                    **(metadata or {})})
    for item in items:
        client.create_dataset_item(
            dataset_name=name, id=item["id"], input=item.get("input"),
            expected_output=item.get("expected_output"),
            metadata=item.get("metadata"))
    return name, len(items)


# ── the harness reads the items as tasks ─────────────────────


def items_to_tasks(items: Iterable[dict[str, Any]]) -> list[Any]:
    """Precedent and silver items become ``nl2sql`` tasks graded by
    canonical fingerprint (a silver item with no SQL is skipped — its
    answer is prose, which fingerprints cannot grade); scenario items
    become ``decompose`` tasks graded on the task list. The task id is
    the item id, so a run's verdicts link to the items."""
    from sahs.evals.schema import (Task, TaskGold, TaskGrading,
                                   TaskProvenance)
    tasks = []
    for item in items:
        schema = str(item.get("schema") or "")
        expected = item.get("expected_output") or {}
        meta = item.get("metadata") or {}
        prompt = str((item.get("input") or {}).get("prompt") or "")
        if schema in (PRECEDENT_SCHEMA, SILVER_SCHEMA):
            sql = str(expected.get("sql") or "")
            if not sql:
                continue
            fp = str(meta.get("canonical_fp") or _fp(sql))
            skill = str(expected.get("skill") or "")
            tags = [f"source={schema.split('.')[1].split('/')[0]}"]
            if skill:
                tags.append(f"skill={skill}")
            tags += [str(t) for t in meta.get("tags") or []]
            source_id = meta.get("source_turn", {}).get("turn_id") \
                if isinstance(meta.get("source_turn"), dict) else None
            tasks.append(Task(
                id=str(item["id"]), kind="nl2sql", prompt=prompt,
                gold=TaskGold(sql=sql, canonical_fp=fp or None),
                grading=TaskGrading(graders=["parse", "canon_ast"],
                                    accepted_fps=[fp] if fp else []),
                provenance=TaskProvenance(source=schema,
                                          source_id=source_id),
                tags=tags))
        elif schema == SCENARIO_SCHEMA:
            tasks.append(Task(
                id=str(item["id"]), kind="decompose", prompt=prompt,
                gold=TaskGold(expected_tasks=list(expected.get("tasks") or []),
                              synthesis=str(expected.get("synthesis") or "")),
                grading=TaskGrading(graders=["decompose"]),
                provenance=TaskProvenance(source=schema),
                tags=["source=scenarios"]))
    return tasks


__all__ = ["BUILDERS", "DATASET_NAMES", "ITEM_SCHEMAS", "PRECEDENT_SCHEMA",
           "SCENARIO_SCHEMA", "SILVER_SCHEMA", "is_item_file", "is_silver",
           "items_to_tasks", "precedent_items", "push_items",
           "read_items", "read_precedents", "scenario_items",
           "silver_items", "write_jsonl"]
