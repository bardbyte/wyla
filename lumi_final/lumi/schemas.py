"""LUMI schemas — all Pydantic models in one file.

This file grows across sessions:
- Session 1: TableContext, SQLFingerprint components
- Session 2: TablePriority, EnrichmentPlan, PlanApproval (NEW for 7-stage flow)
- Session 3: EnrichedOutput, LookMLField, NLQuestionVariant
- Session 4: CoverageReport, QueryCoverage, GateResult

The 7-stage flow:
  Parse → Discover → Stage → Plan → [HUMAN GATE] → Enrich → Validate → Publish
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ─── Session 1: Parse + Discover ─────────────────────────────


class TableContext(BaseModel):
    """Everything the LLM needs to enrich one table.
    Produced by sql_to_context.prepare_enrichment_context().
    """

    table_name: str

    # From sqlglot (deterministic)
    columns_referenced: list[str]
    aggregations: list[dict]           # {function, column, alias, outer_expr}
    case_whens: list[dict]             # {alias, source_column, sql, mapped_values}
    ctes_referencing_this: list[dict]  # {alias, structural_filters, sql, source_tables}
    joins_involving_this: list[dict]   # {other_table, left_key, right_key, order}
    filters_on_this: list[dict]        # {column, operator, value, is_structural}
    date_functions: list[dict]         # {column, function}

    # From MDM (API call)
    mdm_columns: list[dict]            # {name, type, description, is_pii?}
    mdm_table_description: str | None = None
    mdm_coverage_pct: float = 0.0

    # From baseline (file read)
    existing_view_lkml: str | None = None

    # Cross-query context
    queries_using_this: list[str]      # which input SQLs reference this table


# ─── Session 2: Stage + Plan (7-stage upgrade) ───────────────


class TablePriority(BaseModel):
    """Output of the Stage step — ranks which table to plan/enrich first.

    Produced by stage_3_stage_tables(table_contexts).
    Higher rank = process earlier. Tables with no upstream dependencies
    rank above tables that depend on them (CTE source tables first).
    """

    table_name: str
    priority_rank: int = Field(..., ge=1, description="1 = first to process")
    reason: str = Field(..., description="Human-readable why")
    blocks: list[str] = Field(
        default_factory=list,
        description="Tables that depend on this one (cannot start until this completes)",
    )
    blocked_by: list[str] = Field(
        default_factory=list,
        description="Tables this one depends on (must complete first)",
    )
    query_count: int = Field(0, description="How many input SQLs reference this table")
    complexity_score: int = Field(
        0,
        ge=0,
        description="0=simple, 1=CTEs, 2=joins, 3=both. Used as tie-breaker.",
    )


PlanComplexity = Literal["simple", "medium", "complex"]


class EnrichmentPlan(BaseModel):
    """Output of the Plan step — what we WILL produce, before we produce it.

    Cheap to generate (~1K tokens), cheap to review. Goal: catch
    misalignment with intent before spending the ~10K-token enrichment call.
    Written to review_queue/<table_name>.plan.md as human-readable markdown.
    """

    table_name: str
    proposed_dimensions: list[dict] = Field(
        default_factory=list,
        description="[{name, type, source_column, description_summary}]",
    )
    proposed_measures: list[dict] = Field(
        default_factory=list,
        description="[{name, type, source_column, description_summary}]",
    )
    proposed_dimension_groups: list[dict] = Field(
        default_factory=list,
        description="Date columns to be promoted to dimension_groups",
    )
    proposed_derived_tables: list[dict] = Field(
        default_factory=list,
        description="[{name, source_cte, structural_filters, primary_key}]",
    )
    proposed_explore: dict | None = Field(
        None,
        description="{base_view, joins:[...], always_filter, sql_always_where}",
    )
    proposed_filter_catalog_count: int = 0
    proposed_metric_catalog_count: int = 0
    proposed_nl_question_count: int = 0

    complexity: PlanComplexity = "simple"
    estimated_input_tokens: int = Field(
        0, description="Best-guess context size for the enrichment call"
    )
    estimated_output_tokens: int = Field(0)
    reasoning: str = Field(
        ..., description="Why this plan — what observations drove the choices"
    )
    risks: list[str] = Field(
        default_factory=list,
        description="Things that might go wrong: ambiguous PK, missing MDM, complex CTE",
    )
    questions_for_reviewer: list[str] = Field(
        default_factory=list,
        description="Optional explicit asks: 'Should X be many_to_one or many_to_many?'",
    )


ApprovalSource = Literal["human", "auto_low_risk", "auto_skip"]


class PlanApproval(BaseModel):
    """The human-approval gate output. Records who approved/rejected what.

    File convention: review_queue/<table_name>.approval.json once approved.
    For low-risk plans (no risks listed, complexity=simple), auto-approval
    can be configured.
    """

    table_name: str
    approved: bool
    approver: ApprovalSource = "human"
    feedback: str | None = Field(
        None, description="If rejected: why. If approved: optional notes."
    )
    modifications: dict | None = Field(
        None,
        description="Human edits to plan (e.g., {'remove_dimensions': [...], 'rename_measure': {...}})",
    )


# ─── Session 3: Enrich ───────────────────────────────────────


class NLQuestionVariant(BaseModel):
    """A natural language question that an input SQL can answer.
    Produced as side output of enrichment for Radix golden dataset.
    """

    question: str
    explore: str
    measures: list[str]
    dimensions: list[str]
    filters: dict[str, str]
    difficulty: Literal["easy", "medium", "hard"] = "medium"
    source_sql_id: str


class EnrichedOutput(BaseModel):
    """Complete output of one enrichment call (one per table)."""

    view_lkml: str
    derived_table_views: list[str] = Field(default_factory=list)
    explore_lkml: str | None = None
    filter_catalog: list[dict] = Field(default_factory=list)
    metric_catalog: list[dict] = Field(default_factory=list)
    nl_questions: list[NLQuestionVariant] = Field(default_factory=list)


# ─── Session 4: Validate ─────────────────────────────────────


class QueryCoverage(BaseModel):
    """Coverage assessment for one input SQL query."""

    query_id: str
    covered: bool
    measures_present: list[str] = Field(default_factory=list)
    measures_missing: list[str] = Field(default_factory=list)
    dimensions_present: list[str] = Field(default_factory=list)
    dimensions_missing: list[str] = Field(default_factory=list)
    filters_resolvable: list[str] = Field(default_factory=list)
    filters_missing: list[str] = Field(default_factory=list)
    explore_exists: bool = False
    joins_correct: bool = False
    derived_tables_exist: bool = False
    structural_filters_baked: bool = False
    gap_category: str | None = None  # prompt_fix | mdm_fix | irreducible


class CoverageReport(BaseModel):
    """Full pipeline coverage assessment."""

    total_queries: int
    covered: int
    coverage_pct: float
    per_query: list[QueryCoverage]
    all_lookml_valid: bool
    top_gaps: list[str] = Field(default_factory=list)


class GateResult(BaseModel):
    """Result of a stage guardrail check."""

    stage: str
    status: Literal["pass", "warn", "fail"]
    checks: list[dict]
    blocking_failures: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
