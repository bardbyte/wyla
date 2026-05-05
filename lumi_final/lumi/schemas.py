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
    # CREATE [TEMP] TABLE bodies that read from this table — semantic
    # equivalent of CTEs. Same shape plus is_temp/is_replace. Reused
    # temp tables are PDT (persistent derived table) candidates downstream.
    temp_tables_referencing_this: list[dict] = Field(default_factory=list)
    joins_involving_this: list[dict]   # {other_table, left_key, right_key, order}
    filters_on_this: list[dict]        # {column, operator, value, is_structural}
    date_functions: list[dict]         # {column, function}

    # From MDM (API call) — see lumi.mdm._digest for the full per-column
    # shape. Each item now carries 30+ keys including: is_primary,
    # is_dedupe_key, pii_role_id, partition/cluster info, derived_logic,
    # attribute_format, plus *_extra catch-alls for forward-compat.
    mdm_columns: list[dict]
    mdm_table_description: str | None = None
    mdm_coverage_pct: float = 0.0
    # Table-level metadata from MDM dataset_details + dataset_source_details
    # + decommission_details. Keys: table_type, feed_type, data_category,
    # data_sub_category, retention_period, is_internal, is_searchable,
    # is_sor_certified, country, region, mdm_dataset_extra (catch-all), etc.
    # Empty dict when MDM has no entry for the table.
    mdm_dataset_details: dict = Field(default_factory=dict)
    # Ownership: aim_id, imr_queue, app_team_sn_workgroup,
    # business_contacts (with email + type), tech_contacts (same shape),
    # status. Drives the view header comment + escalation routing.
    mdm_ownership: dict = Field(default_factory=dict)

    # From baseline (file read + lkml parse)
    existing_view_lkml: str | None = None
    # Parsed once at discover-time so the planner + enricher can reason
    # about WHICH baseline fields are auto-generated stubs vs human-curated.
    # Each item carries the lkml dict + a 'quality' string:
    #   "rich"  — human-edited (description ≥ 30 chars, has label, has tags)
    #   "stub"  — auto-generated (no description or < 30 chars, no label)
    #   "ok"    — has description but missing other niceties
    baseline_dimensions: list[dict] = Field(default_factory=list)
    baseline_dimension_groups: list[dict] = Field(default_factory=list)
    baseline_measures: list[dict] = Field(default_factory=list)
    # Aggregated counts so the planner can render a one-line "what's missing"
    # summary in review_queue/<table>.plan.md without re-walking the lists.
    # Keys: dims_total, dims_missing_description, dims_short_description (<30 chars),
    #       dims_missing_label, measures_total, measures_missing_value_format,
    #       dates_as_plain_dim (date column with no dimension_group).
    baseline_quality_signals: dict = Field(default_factory=dict)
    # View-level + structural baseline signals — every piece of human-
    # curated work we can preserve for grounding.
    baseline_view_description: str | None = None
    baseline_view_label: str | None = None
    # Authoritative BQ FQN if baseline declares one (overrides LumiConfig
    # default). Pattern: `axp-lumi.dw.<table>` or `${BQ_PROJECT}.dw.<table>`.
    baseline_sql_table_name: str | None = None
    # If the baseline IS a derived_table, its SQL — tells us the team's
    # modeling preference so enrichment doesn't propose a different shape.
    baseline_derived_table_sql: str | None = None
    # Pre-existing primary_key dim NAME (not just bool). Critical for
    # PK preservation in enrichment.
    baseline_primary_key_column: str | None = None
    # Refinement chain: which views this baseline extends (Looker `extends:`).
    # Tells us where to add new fields without breaking inheritance.
    baseline_extends_chain: list[str] = Field(default_factory=list)
    # Pre-curated structural blocks — preserve verbatim, never overwrite.
    baseline_sets: list[dict] = Field(default_factory=list)
    baseline_parameters: list[dict] = Field(default_factory=list)
    baseline_access_filter: list[dict] = Field(default_factory=list)
    # drill_fields list curated by humans → match style for new dims.
    baseline_drill_fields_curated: list[str] = Field(default_factory=list)
    # Pre-filtered measures (e.g. measure: revenue_consumer with
    # filters: [bus_seg: "Consumer"]) reveal canonical slicing patterns.
    # Format: {name, type, sql, filters, description}
    baseline_filtered_measures: list[dict] = Field(default_factory=list)
    # SQL aliases — when baseline dim NAME differs from the source column
    # ({dim_name: source_column}). E.g. "customer_segment" -> "bus_seg".
    # Goldmine for synonym preservation in tags.
    baseline_sql_aliases: dict[str, str] = Field(default_factory=dict)

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
    # Surgical scope for the Enrich call — what specific gaps in the
    # baseline this plan intends to fix. Drives the review markdown's
    # "what's missing" line and lets the enrich prompt say "enrich ONLY
    # these N fields" instead of regenerating wholesale.
    fields_to_enrich: list[dict] = Field(
        default_factory=list,
        description="[{name, kind: dim|dim_group|measure, gap: missing_description|"
                    "short_description|missing_label|missing_value_format|promote_to_dim_group}]",
    )


ApprovalSource = Literal["human", "auto_low_risk", "auto_skip", "pending"]


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
    # When the LLM judges an existing baseline description as wrong (not
    # just terse), it puts the proposed replacement here instead of
    # silently overwriting. Publish routes these to a side file —
    # output/proposed_overwrites.md — for human review next iteration.
    # Each item: {field_kind, field_name, attribute, baseline_value,
    #             proposed_value, reason}
    proposed_overwrites: list[dict] = Field(default_factory=list)
    # Per-field confidence label so the human reviewing knows what was
    # grounded vs inferred vs guessed. Keys are field names ("bus_seg",
    # "total_billed_business"); values are one of:
    #   "grounded"  — backed by MDM description, baseline content, or
    #                 query-usage evidence
    #   "inferred"  — supported by a deterministic signal (naming
    #                 convention, prefix pattern, structural inference)
    #                 but not directly described
    #   "guessed"   — best-effort with no anchor; needs human review
    field_confidences: dict[str, str] = Field(default_factory=dict)
    # Fields where the LLM admitted it couldn't ground its claim. Each
    # item: {field_kind, field_name, attribute, value, confidence, reason}.
    # Surfaced via output/uncertain_fields.md so a reviewer can verify
    # before the LookML lands in production.
    uncertain_fields: list[dict] = Field(default_factory=list)


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
