"""Datasets and dataset runs: the task files mirrored, the harness's
verdicts scored.

The grader stays ``sahs.evals.grading``; Langfuse gets the verdict
it produced, per task, linked to the dataset item the task came
from. A run is named by its four coordinates (SUT, tasks version,
canon version, time) so two lines are comparable only when they
should be.
"""

from __future__ import annotations

import datetime as _dt
import functools
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Iterable

from sahs.evals.grading import SutAnswer, TrialResult
from sahs.evals.schema import Task

from .annotations import enqueue_trace, ensure_queue
from .langfuse_emitter import set_trace_attributes
from .tracer import trace_id_for

DATASET_PREFIX = "wyla"


def tasks_version(paths: Iterable[Path]) -> str:
    """Eight hex chars over the task files' bytes, in path order: the
    dataset coordinate of an eval line."""
    digest = hashlib.sha256()
    for path in sorted(Path(p) for p in paths):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:8]


def dataset_name(path: Path) -> str:
    path = Path(path)
    parent = path.parent.name
    return (f"{DATASET_PREFIX}-{parent}" if parent == path.stem
            else f"{DATASET_PREFIX}-{parent}-{path.stem}")


def task_item(row: dict[str, Any]) -> dict[str, Any]:
    """One dataset item from one task row, whatever its shape: the
    whole row rides in metadata so nothing is lossy; input and
    expected are the best-effort projection the UI shows."""
    if row.get("schema", "").startswith("meridian.task/"):
        task = Task.model_validate(row)
        return {"id": task.id,
                "input": {"prompt": task.prompt,
                          "context": task.context.model_dump(
                              exclude_defaults=True)},
                "expected_output": task.gold.model_dump(exclude_none=True),
                "metadata": task.dump()}
    prompt = row.get("prompt") or row.get("question") or row.get("turns")
    expected = (row.get("gold") or row.get("expect")
                or row.get("expected") or row.get("rubric"))
    return {"id": str(row.get("id")),
            "input": {"prompt": prompt, "context": row.get("context")},
            "expected_output": expected,
            "metadata": row}


def read_rows(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in
            Path(path).read_text(encoding="utf-8").splitlines()
            if line.strip()]


def push_datasets(client: Any, paths: Iterable[Path]) -> dict[str, int]:
    """Upsert one dataset per task file (items keyed by task id, so a
    re-push updates in place). Returns items pushed per dataset."""
    pushed: dict[str, int] = {}
    for path in paths:
        path = Path(path)
        name = dataset_name(path)
        client.create_dataset(
            name=name,
            description=f"tests/tasks/{path.parent.name}/{path.name}",
            metadata={"tasks_version": tasks_version([path]),
                      "source": str(path.name)})
        n = 0
        for row in read_rows(path):
            item = task_item(row)
            client.create_dataset_item(dataset_name=name, **item)
            n += 1
        pushed[name] = n
    return pushed


def run_name_for(sut: str, paths: Iterable[Path], canon_version: str,
                 when: _dt.datetime | None = None) -> str:
    when = when or _dt.datetime.now(_dt.timezone.utc)
    return (f"{sut}·tasks@{tasks_version(paths)}·canon{canon_version}"
            f"·{when.strftime('%Y%m%dT%H%M%SZ')}")


class ExperimentRecorder:
    """Wraps a SUT to keep each answer, and turns each graded trial
    into one trace linked to the dataset item as a run item, with the
    verdict as its score. Same trace id for the same run and task, so
    a re-run under the same name overwrites rather than duplicates."""

    def __init__(self, client: Any, items: Iterable[Any], *,
                 run_name: str,
                 metadata: dict[str, Any] | None = None,
                 queue_id: str = "") -> None:
        self.client = client
        self.items = {item.id: item for item in items}
        self.run_name = run_name
        self.metadata = dict(metadata or {})
        # the annotation queue AMBIGUOUS trials go to ('' = none)
        self.queue_id = queue_id
        self.answers: dict[str, SutAnswer] = {}
        self.missing: list[str] = []
        self.recorded = 0
        self.queued: list[str] = []
        self.queue_errors: list[str] = []

    def sut(self, inner: Callable[[Task], SutAnswer]
            ) -> Callable[[Task], SutAnswer]:
        @functools.wraps(inner)
        def wrapped(task: Task) -> SutAnswer:
            answer = inner(task)
            self.answers[task.id] = answer
            return answer
        return wrapped

    def on_trial(self, trial: TrialResult) -> None:
        item = self.items.get(trial.task_id)
        if item is None:
            self.missing.append(trial.task_id)
            return
        answer = self.answers.get(trial.task_id)
        root = self.client.start_observation(
            name="eval.trial", as_type="span",
            trace_context={"trace_id": trace_id_for(self.run_name,
                                                    trial.task_id)},
            input=getattr(item, "input", None),
            output=answer.model_dump(exclude_none=True)
            if answer is not None else None,
            metadata={"task_id": trial.task_id, "kind": trial.kind,
                      "verdict": trial.verdict, "reason": trial.reason,
                      "answer_fp": trial.answer_fp,
                      "warnings": trial.warnings,
                      "run_name": self.run_name})
        set_trace_attributes(
            root, name="eval.trial", tags=["eval", f"verdict:{trial.verdict}"],
            metadata={"task_id": trial.task_id, "answer_fp": trial.answer_fp,
                      "run_name": self.run_name, "verdict": trial.verdict})
        self.client.api.dataset_run_items.create(
            run_name=self.run_name, dataset_item_id=item.id,
            metadata=self.metadata, trace_id=root.trace_id,
            observation_id=root.id)
        root.score_trace(name="verdict", value=trial.verdict,
                         data_type="CATEGORICAL", comment=trial.reason)
        root.score_trace(name="pass",
                         value=1.0 if trial.verdict == "pass" else 0.0,
                         data_type="NUMERIC")
        root.update(level="DEFAULT" if trial.verdict == "pass"
                    else "WARNING").end()
        self.recorded += 1
        if trial.verdict == "ambiguous" and self.queue_id:
            problem = enqueue_trace(self.client, self.queue_id, root.trace_id)
            if problem:
                self.queue_errors.append(f"{trial.task_id}: {problem}")
            else:
                self.queued.append(trial.task_id)


def experiment_recorder(client: Any, paths: Iterable[Path], *,
                        sut: str, canon_version: str,
                        run_name: str = "",
                        extra: dict[str, Any] | None = None,
                        queue: bool = True) -> ExperimentRecorder:
    """Push the task files (idempotent), gather their items, and hand
    back a recorder named by the run's coordinates. With ``queue``,
    ambiguous trials land on the annotation queue (created if
    missing)."""
    paths = [Path(p) for p in paths]
    push_datasets(client, paths)
    items: list[Any] = []
    for path in paths:
        items.extend(client.get_dataset(dataset_name(path)).items)
    name = run_name or run_name_for(sut, paths, canon_version)
    metadata = {"sut": sut, "tasks_version": tasks_version(paths),
                "canon_version": canon_version,
                "tasks": [p.name for p in paths], **(extra or {})}
    return ExperimentRecorder(client, items, run_name=name,
                              metadata=metadata,
                              queue_id=ensure_queue(client) if queue else "")


__all__ = ["ExperimentRecorder", "dataset_name", "experiment_recorder",
           "push_datasets", "read_rows", "run_name_for", "task_item",
           "tasks_version"]
