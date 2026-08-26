"""meridian.task/1 — the eval task contract (G.a, materialized in P0).

Every task the harness runs conforms to this model. The gold set's 158
prompt→SQL pairs materialize here with their canonical fingerprints
cached; empty-SQL pairs go to a triage backlog (genuine abstention gold
vs broken extraction — a human call, never a silent default). The E2
verdict lattice reads ``grading.accepted_fps``: triage-approved
alternative formulations join that set, which is how the suite learns
without silently moving the floor.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

SCHEMA = "meridian.task/1"

TaskKind = Literal["nl2sql", "abstain", "disambiguate", "resolve_bind"]


class TaskContext(BaseModel):
    tables_allowed: list[str] = Field(default_factory=list)
    bu: str | None = None
    region: str | None = None


class TaskGold(BaseModel):
    sql: str | None = None
    canonical_fp: str | None = None
    abstain_reason: str | None = None
    expected_options: list[str] = Field(default_factory=list)
    expected_bindings: dict[str, list[str]] = Field(default_factory=dict)


class TaskGrading(BaseModel):
    graders: list[str] = Field(default_factory=list)
    accepted_fps: list[str] = Field(default_factory=list)
    result_schema: dict[str, Any] | None = None
    dry_run: Literal["required", "skip"] = "skip"


class TaskProvenance(BaseModel):
    source: str
    source_id: str | int | None = None


class Task(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_: str = Field(default=SCHEMA, alias="schema")
    id: str
    kind: TaskKind
    prompt: str
    context: TaskContext = Field(default_factory=TaskContext)
    gold: TaskGold = Field(default_factory=TaskGold)
    grading: TaskGrading = Field(default_factory=TaskGrading)
    provenance: TaskProvenance
    tags: list[str] = Field(default_factory=list)

    def dump(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True)


def write_tasks(tasks: list[Task], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for task in tasks:
            f.write(json.dumps(task.dump(), ensure_ascii=False,
                               sort_keys=True) + "\n")
    return path


def read_tasks(path: Path) -> list[Task]:
    tasks = []
    for line in Path(path).read_text(encoding="utf-8").split("\n"):
        if line.strip():
            tasks.append(Task.model_validate(json.loads(line)))
    return tasks
