"""The annotation-queue round trip for AMBIGUOUS verdicts.

An ambiguous trial (fingerprint mismatch, schema match) is neither a
pass nor a fail until a person says so. The eval run puts its trace
on the ``wyla-ambiguous`` queue; a steward scores it ``resolution`` =
accept or fail in the Langfuse UI; ``pull_resolutions`` reads those
scores back and writes accepted fingerprints into the task file's
``grading.accepted_fps`` — which the user then commits. The suite
learns through git; nothing in Langfuse is read at grading time.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Iterator

from sahs.evals.schema import Task, read_tasks, write_tasks

QUEUE_NAME = "wyla-ambiguous"
SCORE_NAME = "resolution"
CATEGORIES = ({"value": 1, "label": "accept"}, {"value": 0, "label": "fail"})


def _pages(fetch: Any, **kw: Any) -> Iterator[Any]:
    page = 1
    while True:
        resp = fetch(page=page, limit=100, **kw)
        yield from resp.data
        if page >= int(getattr(resp.meta, "total_pages", 1) or 1):
            return
        page += 1


def ensure_score_config(client: Any) -> str:
    for config in _pages(client.api.score_configs.get):
        if config.name == SCORE_NAME and not config.is_archived:
            return config.id
    from langfuse.api import ConfigCategory
    made = client.api.score_configs.create(
        name=SCORE_NAME, data_type="CATEGORICAL",
        categories=[ConfigCategory(**c) for c in CATEGORIES],
        description="Human resolution of an AMBIGUOUS eval verdict: "
                    "accept admits the answer's fingerprint into the "
                    "task's accepted set; fail keeps it out.")
    return made.id


def ensure_queue(client: Any) -> str:
    for queue in _pages(client.api.annotation_queues.list_queues):
        if queue.name == QUEUE_NAME:
            return queue.id
    made = client.api.annotation_queues.create_queue(
        name=QUEUE_NAME, score_config_ids=[ensure_score_config(client)],
        description="Ambiguous eval verdicts awaiting a steward's "
                    "accept / fail (docs/runbooks/langfuse.md)")
    return made.id


def enqueue_trace(client: Any, queue_id: str, trace_id: str) -> str:
    """Put a trace on the queue. Returns '' on success, else the
    reason (a re-run re-queues the same trace: not an eval failure)."""
    try:
        client.api.annotation_queues.create_queue_item(
            queue_id, object_id=trace_id, object_type="TRACE")
        return ""
    except Exception as e:                            # noqa: BLE001
        return f"{type(e).__name__}: {e}"[:200]


def resolutions(client: Any) -> Iterator[dict[str, Any]]:
    """Every human ``resolution`` score, newest page last."""
    for score in _pages(client.api.scores.get_many, name=SCORE_NAME):
        value = getattr(score, "string_value", None)
        if value is None:
            raw = getattr(score, "value", None)
            value = next((c["label"] for c in CATEGORIES
                          if c["value"] == raw), str(raw))
        yield {"score_id": score.id, "trace_id": score.trace_id,
               "value": str(value).lower(),
               "comment": getattr(score, "comment", None) or ""}


def trial_metadata(client: Any, trace_id: str) -> dict[str, Any] | None:
    """The eval trial's metadata (task_id, answer_fp, run_name): on
    the root observation the recorder wrote, else the trace."""
    trace = client.api.trace.get(trace_id)
    for obs in getattr(trace, "observations", None) or []:
        if getattr(obs, "name", "") == "eval.trial" \
                and isinstance(getattr(obs, "metadata", None), dict):
            return obs.metadata
    meta = getattr(trace, "metadata", None)
    return meta if isinstance(meta, dict) else None


def pull_resolutions(client: Any, task_paths: Iterable[Path]
                     ) -> dict[str, Any]:
    """Apply every accept to its task's accepted set; report fails and
    anything that could not be matched. Rewrites only changed files."""
    paths = [Path(p) for p in task_paths]
    tasks: dict[str, tuple[Path, Task]] = {}
    by_path: dict[Path, list[Task]] = {}
    for path in paths:
        by_path[path] = read_tasks(path)
        for task in by_path[path]:
            tasks[task.id] = (path, task)
    accepted: list[str] = []
    already: list[str] = []
    failed: list[str] = []
    unmatched: list[dict[str, Any]] = []
    changed: set[Path] = set()
    for res in resolutions(client):
        meta = trial_metadata(client, res["trace_id"]) or {}
        task_id, fp = meta.get("task_id"), meta.get("answer_fp")
        if not task_id or task_id not in tasks:
            unmatched.append({**res, "task_id": task_id})
            continue
        path, task = tasks[task_id]
        if res["value"] == "accept":
            if not fp:
                unmatched.append({**res, "task_id": task_id,
                                  "why": "no answer_fp on the trial"})
            elif fp in task.grading.accepted_fps:
                already.append(task_id)
            else:
                task.grading.accepted_fps.append(fp)
                accepted.append(task_id)
                changed.add(path)
        elif res["value"] == "fail":
            failed.append(task_id)
        else:
            unmatched.append({**res, "task_id": task_id,
                              "why": f"unknown value {res['value']!r}"})
    for path in sorted(changed):
        write_tasks(by_path[path], path)
    return {"accepted": accepted, "already_accepted": already,
            "failed": failed, "unmatched": unmatched,
            "files_written": [str(p) for p in sorted(changed)]}


__all__ = ["CATEGORIES", "QUEUE_NAME", "SCORE_NAME", "ensure_queue",
           "ensure_score_config", "enqueue_trace", "pull_resolutions",
           "resolutions", "trial_metadata"]
