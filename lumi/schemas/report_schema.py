"""Coverage + vocabulary reports — the deterministic and LLM verdicts on the enrichment."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

FailureReason = Literal[
    "missing_measure",
    "missing_dimension",
    "missing_explore",
    "schema_gap",
    "parse_error",
]

EnrichmentSource = Literal["gold_query", "mdm", "inferred", "existing_preserved"]

VocabSeverity = Literal["low", "medium", "high"]


class CoverageFailure(BaseModel):
    query_id: str
    user_prompt: str
    reason: FailureReason
    detail: str = Field(..., description="What specifically is missing.")
    suggested_fix: str | None = None


class CoverageReport(BaseModel):
    total_queries: int
    passed: int
    partial: int
    failed: int
    failures: list[CoverageFailure] = Field(default_factory=list)
    coverage_by_source: dict[str, int] = Field(
        default_factory=lambda: {
            "gold_query": 0,
            "mdm": 0,
            "inferred": 0,
            "existing_preserved": 0,
        },
        description="How many fields landed in each enrichment layer (keys = EnrichmentSource).",
    )

    @property
    def coverage_pct(self) -> float:
        if self.total_queries == 0:
            return 0.0
        return round(100.0 * self.passed / self.total_queries, 2)


class VocabIssue(BaseModel):
    view: str
    field: str | None = Field(None, description="None = view-level issue.")
    issue: str
    recommendation: str
    severity: VocabSeverity


class VocabReport(BaseModel):
    consistent: bool = False
    issues: list[VocabIssue] = Field(default_factory=list)

    @property
    def high_severity_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "high")
