"""Reference SUTs — the harness's calibration instruments.

oracle  echoes gold — must score 100% pass, 0 ambiguous (harness sanity)
null    always abstains — must score exactly the abstention share
        (calibrates the floor: silence is only right where silence is gold)
"""

from __future__ import annotations

from sahs.evals.grading import SutAnswer
from sahs.evals.schema import Task


def oracle(task: Task) -> SutAnswer:
    if task.kind == "nl2sql":
        return SutAnswer(kind="sql", sql=task.gold.sql or "")
    if task.kind == "abstain":
        return SutAnswer(kind="abstain",
                         reason=task.gold.abstain_reason or "gold")
    if task.kind == "disambiguate":
        return SutAnswer(kind="disambiguate",
                         options=list(task.gold.expected_options))
    return SutAnswer(kind="bindings",
                     bindings=dict(task.gold.expected_bindings))


def null(task: Task) -> SutAnswer:
    return SutAnswer(kind="abstain", reason="null-sut")


BUILTIN_SUTS = {"oracle": oracle, "null": null}
