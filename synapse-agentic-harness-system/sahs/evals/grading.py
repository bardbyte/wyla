"""Graders + the E2 verdict lattice — cheap first, honest always.

nl2sql verdicts (pinned):
    1. answer's fp_expr ∈ task.grading.accepted_fps            → PASS
    2. fp mismatch AND (dry-run invalid OR schema ≠ gold's)    → FAIL
    3. fp mismatch AND schema matches                          → AMBIGUOUS
       — counted in neither pass nor fail; its own rate; appended to
       the triage file. Human triage either admits the fp into the
       task's accepted set (the suite learns) or fails it. Ambiguous
       items never silently move the floor in either direction.

Bytes-scanned band is a WARNING, never a gate (partition pruning makes
it flaky). Static result-shape (SELECT-list arity) runs before any
dry-run — no free FAIL should ever cost a network call.

Abstention class: answering when the task says abstain is its own loud
failure reason (``answered_should_abstain``) — a confident wrong answer
is worse than silence, and the report keeps that class visible.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from sahs.canon.canonical import try_canon
from sahs.evals.schema import Task
from sahs.evals.substrate import DryRunOutcome, ExecutionSubstrate

Verdict = Literal["pass", "fail", "ambiguous"]


class SutAnswer(BaseModel):
    """What a system-under-test returns for one task."""

    kind: Literal["sql", "abstain", "disambiguate", "bindings"]
    sql: str | None = None
    options: list[str] = Field(default_factory=list)
    bindings: dict[str, list[str]] = Field(default_factory=dict)
    reason: str | None = None


@dataclass
class TrialResult:
    task_id: str
    kind: str
    verdict: Verdict
    reason: str
    warnings: list[str] = field(default_factory=list)
    answer_fp: str | None = None


def _static_select_arity(sql: str) -> int | None:
    result, err = try_canon(sql)
    if err is not None or result.ast is None:
        return None
    expressions = getattr(result.ast, "expressions", None)
    if not expressions:
        return None
    from sqlglot import exp
    if any(isinstance(e, exp.Star) for e in expressions):
        return None                      # star arity unknowable statically
    return len(expressions)


def _schemas_match(a: list[dict] | None, b: list[dict] | None) -> bool | None:
    """None = can't tell (a substrate returned no schema)."""
    if a is None or b is None:
        return None
    norm = lambda s: [(f.get("name", "").lower(), f.get("type", "").upper())
                      for f in s]
    return norm(a) == norm(b)


def grade_nl2sql(task: Task, answer: SutAnswer,
                 substrate: ExecutionSubstrate | None) -> TrialResult:
    if answer.kind == "abstain":
        return TrialResult(task.id, task.kind, "fail", "abstained_on_answerable")
    if answer.kind != "sql" or not (answer.sql or "").strip():
        return TrialResult(task.id, task.kind, "fail", "no_sql_answer")

    result, err = try_canon(answer.sql)
    if err is not None:
        return TrialResult(task.id, task.kind, "fail",
                           f"canon:{err.category}")
    if result.kind not in ("select", "union"):
        # lenient parsers accept bare expressions; an answer must be a
        # query statement, not something that merely tokenizes
        return TrialResult(task.id, task.kind, "fail",
                           f"not_a_query:{result.kind}")
    if result.fp_expr in task.grading.accepted_fps:
        return TrialResult(task.id, task.kind, "pass", "fp_match",
                           answer_fp=result.fp_expr)

    # cheap static shape check before any network
    gold_arity = (_static_select_arity(task.gold.sql)
                  if task.gold.sql else None)
    answer_arity = _static_select_arity(answer.sql)
    if gold_arity is not None and answer_arity is not None \
            and gold_arity != answer_arity:
        return TrialResult(task.id, task.kind, "fail",
                           f"select_arity {answer_arity}≠{gold_arity}",
                           answer_fp=result.fp_expr)

    if task.grading.dry_run == "skip" or substrate is None:
        return TrialResult(task.id, task.kind, "fail",
                           "fp_mismatch_no_substrate",
                           answer_fp=result.fp_expr)

    outcome: DryRunOutcome = substrate.dry_run(answer.sql)
    if not outcome.valid:
        return TrialResult(task.id, task.kind, "fail",
                           f"dry_run_invalid: {outcome.error[:120]}",
                           answer_fp=result.fp_expr)
    gold_schema = None
    if task.grading.result_schema:
        gold_schema = task.grading.result_schema.get("fields")
    match = _schemas_match(outcome.result_schema, gold_schema)
    if match is False:
        return TrialResult(task.id, task.kind, "fail", "schema_mismatch",
                           answer_fp=result.fp_expr)
    warnings = []
    if outcome.bytes_processed is not None:
        band = (task.grading.result_schema or {}).get("bytes_band")
        if band and not (band[0] <= outcome.bytes_processed <= band[1]):
            warnings.append(
                f"bytes {outcome.bytes_processed} outside band {band}")
    return TrialResult(task.id, task.kind, "ambiguous",
                       "fp_mismatch_schema_match" if match else
                       "fp_mismatch_schema_unknown",
                       warnings=warnings, answer_fp=result.fp_expr)


def grade_abstain(task: Task, answer: SutAnswer) -> TrialResult:
    if answer.kind == "abstain":
        return TrialResult(task.id, task.kind, "pass", "abstained")
    return TrialResult(task.id, task.kind, "fail",
                       "answered_should_abstain")


def grade_disambiguate(task: Task, answer: SutAnswer) -> TrialResult:
    if answer.kind != "disambiguate":
        return TrialResult(task.id, task.kind, "fail",
                           "bound_instead_of_asking")
    want = set(task.gold.expected_options)
    got = set(answer.options)
    if want and not want.issubset(got):
        return TrialResult(task.id, task.kind, "fail",
                           f"options_missing:{sorted(want - got)}")
    if not answer.options:
        return TrialResult(task.id, task.kind, "fail", "no_named_options")
    return TrialResult(task.id, task.kind, "pass", "named_options")


def grade_resolve_bind(task: Task, answer: SutAnswer) -> TrialResult:
    if answer.kind == "disambiguate":
        return TrialResult(task.id, task.kind, "fail",
                           "asked_on_unambiguous")
    if answer.kind != "bindings":
        return TrialResult(task.id, task.kind, "fail", "no_bindings")
    for slot, expected in task.gold.expected_bindings.items():
        got = answer.bindings.get(slot, [])
        if sorted(got) != sorted(expected):
            return TrialResult(
                task.id, task.kind, "fail",
                f"slot {slot}: {sorted(got)} ≠ {sorted(expected)}")
    return TrialResult(task.id, task.kind, "pass", "bindings_match")


def grade(task: Task, answer: SutAnswer,
          substrate: ExecutionSubstrate | None) -> TrialResult:
    if task.kind == "nl2sql":
        return grade_nl2sql(task, answer, substrate)
    if task.kind == "abstain":
        return grade_abstain(task, answer)
    if task.kind == "disambiguate":
        return grade_disambiguate(task, answer)
    return grade_resolve_bind(task, answer)


def append_ambiguous(triage_path: Path, task: Task,
                     trial: TrialResult) -> None:
    triage_path.parent.mkdir(parents=True, exist_ok=True)
    with triage_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "task_id": task.id, "answer_fp": trial.answer_fp,
            "reason": trial.reason, "accepted_fps":
                task.grading.accepted_fps,
            "resolution": None,          # human: "accept" | "fail"
        }, ensure_ascii=False) + "\n")
