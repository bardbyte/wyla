"""The query contract (E18): acceptance before work, default-FAIL.

Every criterion starts false and flips only on evidence. Nothing
renders as an answer until the contract is satisfied, and the criteria
the verifier could not evaluate stay false — an UNKNOWN is a failure,
never a pass. That asymmetry is the whole point: a skeptical separate
judge is tractable, a self-critical generator is not.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Criterion:
    id: str
    text: str
    passed: bool = False           # default-FAIL, always
    evidence: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "text": self.text, "passed": self.passed,
                "evidence": self.evidence}


@dataclass
class Contract:
    plan_version: int
    criteria: list[Criterion] = field(default_factory=list)

    def get(self, criterion_id: str) -> Criterion | None:
        return next((c for c in self.criteria if c.id == criterion_id), None)

    def flip(self, criterion_id: str, passed: bool,
             evidence: str = "") -> Criterion:
        criterion = self.get(criterion_id)
        if criterion is None:
            raise KeyError(f"no criterion {criterion_id!r} in the contract")
        criterion.passed = bool(passed)
        criterion.evidence = evidence
        return criterion

    @property
    def verdict(self) -> str:
        return "pass" if all(c.passed for c in self.criteria) else "fail"

    def failures(self) -> list[Criterion]:
        return [c for c in self.criteria if not c.passed]

    def to_dict(self) -> dict[str, Any]:
        return {"plan_version": self.plan_version,
                "verdict": self.verdict,
                "will_verify": [c.to_dict() for c in self.criteria]}


def build_contract(plan: Any, *, multi_table: bool = False) -> Contract:
    """The will_verify list for one plan. Written as promises the
    analyst can read, because they are shown to the analyst."""
    criteria = [
        Criterion("executes",
                  "the query runs against the promoted build"),
        Criterion("contract_ast",
                  "every table, column and metric it names is real"),
        Criterion("grain_declared",
                  f"one row means: {plan.grain or '(undeclared)'}"),
        Criterion("cost_gate",
                  "the scan stays inside the cost gates"),
        Criterion("grounded",
                  "the written answer says only what the query supports"),
    ]
    if multi_table:
        criteria.insert(3, Criterion(
            "fan_out_guard",
            "the join is raw-safe, so no row is counted twice"))
    return Contract(plan_version=getattr(plan, "version", 1),
                    criteria=criteria)
