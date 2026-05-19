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


class ViewDescription(BaseModel):
    """Disambiguating description for one view.

    Single-line LookML descriptions don't survive cold-start retrieval:
    when 5 views all relate to "cardmember", Radix needs to know WHICH
    one to load for "show me cardmember spend last quarter" vs "show me
    current cardmember status." This structure forces the planner to
    surface the disambiguation explicitly.

    Renders into LookML two ways:
      1. The ``description:`` parameter (Looker UI surface) — synthesized
         one-liner including grain + scope.
      2. A ``# DISAMBIGUATION ===`` comment block above the view —
         indexable by Radix and readable by humans editing the file.
    """
    one_liner: str = Field(
        default="",
        description="≤140 chars. Plain-English answer to 'what is this view?'",
    )
    grain: str = Field(
        default="",
        description="What ONE row represents — 'one row per cardmember per day'.",
    )
    scope: str = Field(
        default="",
        description="Universe / population — 'active US cardmembers, last 90 days'.",
    )
    when_to_use: str = Field(
        default="",
        description="Question patterns this view IS for — 'use when "
                    "asking about cardmember demographics or status'.",
    )
    when_not_to_use: str = Field(
        default="",
        description="Question patterns that belong elsewhere — "
                    "'don't use for transaction-grain spend; use cm_txn'.",
    )
    distinguishes_from: list[dict] = Field(
        default_factory=list,
        description="[{view_name, how_it_differs}] — explicit contrast with "
                    "sibling views sharing the primary entity. Empty list is "
                    "a critic block when ontology shows siblings.",
    )


class ExplorePlan(BaseModel):
    """An explore designed at the corpus level, not the table level.

    Tier 2's first-principles correction: explores model question
    patterns, not tables. The corpus's gold queries cluster naturally
    by (tables touched, GROUP BY shape, structural filters); each
    cluster IS an explore. This contract carries everything needed to
    emit a Looker explore that scores well in Radix's coverage³ ×
    base_view_bonus retrieval ranking.
    """
    cluster_id: str
    explore_name: str = Field(
        ...,
        description="LookML explore identifier — snake_case, "
                    "describes the question pattern.",
    )
    base_view: str
    dim_views: list[str] = Field(
        default_factory=list,
        description="Tables joined into the base view.",
    )
    joins: list[dict] = Field(
        default_factory=list,
        description="[{right_table, left_key, right_key, relationship}] — "
                    "relationships derived from corpus cardinality inference.",
    )
    always_filter: dict = Field(
        default_factory=dict,
        description="Mandatory filters on the explore. Partition columns "
                    "from MDM auto-included with default windows.",
    )
    sql_always_where: str = Field(
        default="",
        description="WHERE clause baked into every query. Use for "
                    "structural invariants (data_source = 'cornerstone').",
    )
    description: ExploreDescription | None = Field(
        None,
        description="Disambiguating description (one_liner, primary_questions, "
                    "anti_questions, canonical_filters, join_paths) — drives "
                    "Radix's description_similarity scoring.",
    )
    member_query_count: int = Field(
        default=0,
        description="How many gold queries match this explore's pattern.",
    )
    base_view_bonus_estimate: float = Field(
        default=1.0, ge=0.0,
        description="Estimated Radix base_view_bonus (1.0 = no bonus, "
                    "2.0 = max). Higher = explore is well-aligned to the "
                    "question pattern.",
    )


class ExploreDescription(BaseModel):
    """Disambiguating description for one explore.

    The explore is what Radix loads to answer a question — its
    description shapes retrieval. We capture the question patterns this
    explore actually answers (so retrieval routes correctly) AND the
    patterns it does NOT answer (so retrieval doesn't misroute).
    """
    one_liner: str = Field(
        default="",
        description="≤140 chars. What this explore is FOR.",
    )
    primary_questions: list[str] = Field(
        default_factory=list,
        description="3-5 NL patterns this explore answers, in analyst "
                    "vocabulary. Drives Radix retrieval — include "
                    "canonical entity names + join chain hint.",
    )
    anti_questions: list[str] = Field(
        default_factory=list,
        description="2-3 NL patterns that BELONG in another explore. "
                    "Tells Radix where NOT to route.",
    )
    canonical_filters: dict = Field(
        default_factory=dict,
        description="Filters baked into the explore "
                    "(e.g. {data_source: 'cornerstone'}). Documented invariants.",
    )
    join_paths: list[str] = Field(
        default_factory=list,
        description="Human-readable chain summaries mirroring canonical paths "
                    "— 'cardmember → account → transaction (one_to_many → one_to_many)'.",
    )


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
    # Disambiguating descriptions — what makes Radix retrieval correct.
    # Authored by the planner using ontology + nearby-tables context.
    # Empty/missing on legacy plans; critic gates new plans on these
    # being substantive when the table has sibling-entity tables.
    proposed_view_description: ViewDescription | None = Field(
        None,
        description="Structured view description for Radix retrieval grounding.",
    )
    proposed_explore_description: ExploreDescription | None = Field(
        None,
        description="Structured explore description for Radix retrieval grounding.",
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
    # Provenance: was this plan authored by Gemini or by the deterministic
    # skeleton? When the LLM call failed, ``reason`` carries the error
    # message so the human reviewer knows why their plan is less rich.
    # {"mode": "llm" | "skeleton", "reason": str | None}
    authoring: dict = Field(
        default_factory=lambda: {"mode": "skeleton", "reason": None},
        description="Plan provenance — LLM-authored or deterministic skeleton",
    )


# ─── Domain Ontology (system-level, built once, used by every table) ─


class OntologyEntity(BaseModel):
    """One business entity in the domain ontology.

    Built by ``lumi.ontology_builder`` from a single upfront Gemini call
    that reads every table's MDM + baseline + the cross-corpus join
    chains. Each entity carries its synonyms (cardmember = customer = cm
    = cust) and the columns that identify it across tables.
    """
    name: str = Field(description="Canonical entity name in snake_case")
    synonyms: list[str] = Field(
        default_factory=list,
        description="Other names referring to the same entity",
    )
    grain_description: str = Field(
        default="",
        description="What one row represents (e.g. 'one row per cardmember per day')",
    )
    grain_columns: dict[str, list[str]] = Field(
        default_factory=dict,
        description="table_name → [columns that identify this entity]",
    )
    description: str = Field(
        default="",
        description="Brief paragraph on what this entity represents",
    )
    evidence: list[str] = Field(
        default_factory=list,
        description="Why this entity is identified — MDM cite, JOIN evidence, etc.",
    )


class OntologyRelationship(BaseModel):
    """One relationship between two entities (e.g. cardmember → account)."""
    from_entity: str
    to_entity: str
    cardinality: Literal[
        "one_to_one", "one_to_many", "many_to_one", "many_to_many", "unknown",
    ] = "unknown"
    evidence: str = Field(default="", description="Observed evidence for this relationship")


class DomainOntology(BaseModel):
    """The system-level ontology built once for the whole corpus.

    Persisted to ``data/ontology.json``. Injected as a ``## Domain
    ontology`` prompt section into both the plan stage and the enrich
    stage. Solves the cardmember↔customer↔cust_xref_id semantic-
    equivalence problem at the system level rather than per-table.
    """
    entities: list[OntologyEntity] = Field(default_factory=list)
    relationships: list[OntologyRelationship] = Field(default_factory=list)
    # Pre-computed lookup: which entity is each table primarily about?
    # Drives the "## Domain ontology" prompt section's relevance filter.
    table_to_primary_entity: dict[str, str] = Field(default_factory=dict)
    # Provenance — was this ontology LLM-authored or deterministic fallback?
    authoring: dict = Field(
        default_factory=lambda: {"mode": "deterministic", "reason": None},
    )

    def entities_for_table(
        self, table_name: str,
    ) -> list[OntologyEntity]:
        """Return entities that have any column on the given table."""
        out: list[OntologyEntity] = []
        for entity in self.entities:
            if table_name in entity.grain_columns:
                out.append(entity)
        return out

    def primary_entity_for_table(
        self, table_name: str,
    ) -> OntologyEntity | None:
        primary_name = self.table_to_primary_entity.get(table_name)
        if not primary_name:
            return None
        return next(
            (e for e in self.entities if e.name == primary_name), None,
        )

    def related_entities_for_table(
        self, table_name: str, *, limit: int = 5,
    ) -> list[OntologyEntity]:
        """Entities related (via OntologyRelationship) to this table's entity."""
        primary = self.primary_entity_for_table(table_name)
        if primary is None:
            return []
        related_names: set[str] = set()
        for rel in self.relationships:
            if rel.from_entity == primary.name:
                related_names.add(rel.to_entity)
            if rel.to_entity == primary.name:
                related_names.add(rel.from_entity)
        return [
            e for e in self.entities if e.name in related_names
        ][:limit]


# ─── Critic agent — plan quality gate ─────────────────────────


CritiqueSeverity = Literal["block", "warn", "info"]


CritiqueCategory = Literal[
    # 1. Will Radix's NL2SQL retrieval find this view for typical questions?
    #    Names+descriptions+synonyms must align with how analysts ASK.
    "radix_retrieval_alignment",
    # 2. Vocabulary completeness — every entity from the ontology that this
    #    table represents must surface as canonical name + synonyms.
    "vocabulary_completeness",
    # 3. Disambiguation — when two columns mean different things despite
    #    similar names (cm11 the cardmember PK vs. cm15 the spouse FK),
    #    descriptions must say so explicitly.
    "disambiguation",
    # 4. Logical-type correctness — yesno on flag, number on amount,
    #    string on category, dimension_group on date. Wrong type = wrong SQL.
    "logical_type",
    # 5. Ontology consistency — same entity must use the same canonical
    #    name across tables; synonyms must match the system ontology.
    "ontology_consistency",
    # 6. Equivalence preservation — JOIN-proven equivalent columns
    #    (cm11 ≡ cust_xref_id) must be described as the same entity.
    "equivalence_preservation",
    # 7. Partition / freshness coherence — partition column must surface
    #    as a dim_group with timeframes; freshness must match MDM hints.
    "partition_freshness",
    # 8. Primary-key rationality — the proposed PK must actually identify
    #    one row (not a high-cardinality FK or a dim).
    "pk_rationality",
    # 9. Structural-filter baking — filters that are model invariants
    #    (data_source = 'cornerstone') must NOT be exposed; they must be
    #    baked into derived_table SQL or sql_always_where.
    "structural_filter_baking",
    # 10. Risk acknowledgement — sparse MDM, missing baseline, complex
    #     CTE chains must show up in `risks`, not be silently glossed.
    "risk_acknowledgement",
    # 11. Reasoning grounding — the `reasoning` field must cite real
    #     evidence (MDM, baseline, query usage). No vague hand-waving.
    "reasoning_grounding",
    # 12. JOIN cardinality correctness — proposed_explore.joins[i].relationship
    #     must match the corpus-inferred cardinality. Wrong relationship
    #     → silent fan-out → wrong numbers in production.
    "join_cardinality_correctness",
    # 13. JOIN path grounding — proposed joins should match canonical
    #     paths actually observed in queries. Inventing joins no analyst
    #     uses misroutes Radix retrieval.
    "join_path_grounding",
    # 14. Disambiguation completeness — view + explore descriptions must
    #     name what makes THIS view different from siblings sharing the
    #     primary entity. Empty distinguishes_from with siblings present
    #     is a blocking issue: Radix can't route correctly without it.
    "disambiguation_completeness",
    # 15. Field searchability — every dim/measure embedded into Radix's
    #     pgvector index needs label + description + hint. Hint is the
    #     Looker parameter that holds alternative names / business jargon /
    #     query phrasing — directly improves BGE recall. Missing hints
    #     on a majority of fields is blocking: the view is invisible to
    #     analysts who don't already know the canonical column name.
    "field_searchability",
    # 16. Symmetric aggregates — when an explore has many_to_many joins
    #     OR joins multiple fact tables, every measure must declare
    #     `symmetric_aggregates: yes` or Looker silently fans out values
    #     across the join. Wrong totals, wrong revenue, wrong everything.
    "symmetric_aggregates",
]


class CritiqueIssue(BaseModel):
    """One issue raised by the critic against a plan.

    Every issue carries severity + category + the precise locus (which
    field) + cited evidence + a concrete recommendation. The plan's
    self-repair loop reads these and re-prompts the planner with the
    issues appended to the prompt.
    """
    category: CritiqueCategory
    severity: CritiqueSeverity = "warn"
    locus: str = Field(
        default="",
        description="What part of the plan this is about — "
                    "e.g. 'proposed_dimensions[3].name=cm11' or 'reasoning' or "
                    "'proposed_explore.joins[0]'",
    )
    finding: str = Field(
        ...,
        description="What's wrong, in one sentence. No vague language.",
    )
    evidence: str = Field(
        default="",
        description="Cited evidence from MDM / baseline / queries / ontology "
                    "that supports the finding.",
    )
    recommendation: str = Field(
        ...,
        description="Concrete fix — what the planner should change on retry.",
    )


class CritiqueReport(BaseModel):
    """Output of the critic agent for one plan.

    The plan loop reads `block_count > 0` to decide whether to retry.
    `summary` is shown to the human reviewer in the markdown / interactive
    review. `radix_retrieval_score` (0-10) is a single overall number
    asking 'would Radix find this view for the natural questions
    analysts ask?' — calibrated by the critic itself.
    """
    table_name: str
    issues: list[CritiqueIssue] = Field(default_factory=list)
    overall_verdict: Literal["approve", "approve_with_warnings", "retry", "reject"] = (
        "approve_with_warnings"
    )
    radix_retrieval_score: int = Field(
        default=5, ge=0, le=10,
        description="Will Radix find this view for typical NL questions? 0-10.",
    )
    summary: str = Field(
        default="",
        description="2-3 sentences for the human — what's strong, what to watch.",
    )
    # Convenience tallies — populated post-parse.
    block_count: int = 0
    warn_count: int = 0
    info_count: int = 0

    def recompute_counts(self) -> None:
        self.block_count = sum(1 for i in self.issues if i.severity == "block")
        self.warn_count = sum(1 for i in self.issues if i.severity == "warn")
        self.info_count = sum(1 for i in self.issues if i.severity == "info")


# ─── Ontology event store ────────────────────────────────────


OntologyEventType = Literal[
    # parse_sqls hook — every JOIN ON pair is an equivalence claim.
    "equivalence_observed",
    # fetch_mdm hook — entity hints from data_category + business_name.
    "entity_hint",
    # fetch_mdm hook — synonym candidate from MDM business_name vs column name.
    "synonym_candidate",
    # parse_baseline hook — human-curated synonym candidate from sql_aliases.
    "curated_synonym",
    # parse_baseline + fetch_mdm hook — primary_key (and is_dedupe_key) is
    # a strong PK-class identity claim.
    "curated_pk",
    # approve_plan hook — the human-approved plan locks in vocabulary.
    "vocabulary_lock",
    # critic_finding hook — critic surfaces a refinement.
    "entity_refinement",
    # parse_sqls hook (corpus-level) — JOIN cardinality inferred from
    # GROUP BY + aggregation + join_type evidence. Also re-used by
    # fetch_mdm for column.external_references (declared FKs).
    "cardinality_observed",
    # parse_sqls hook — multi-hop JOIN chain seen in real queries.
    "join_path_observed",
    # fetch_mdm hook — column governance facts: PII, CDE, GDPR, sensitive,
    # mandatory, attribute_format, clustered. Properties on the Column
    # node; don't promote to entities but enrich grounding.
    "column_governance_observed",
    # fetch_mdm hook — partition + time_partition_type declared in MDM.
    # Creates TimeGrain node + always-filter candidate.
    "partition_observed",
    # fetch_mdm hook — derived_logic in MDM is a Metric formula candidate.
    "derived_formula_observed",
    # fetch_mdm hook — table-level metadata (table_type, feed_type,
    # data_category, ownership, bq_fqn). Properties on the Table node.
    "table_metadata_observed",
    # fetch_mdm hook — is_decommissioned at table OR column level.
    # Drives DEPRECATES edge and demotion.
    "deprecation_observed",
    # parse_sqls hook — corpus aggregations (SUM/AVG/COUNT_DISTINCT...).
    # Each unique (table, column, fn) becomes a Metric candidate.
    "metric_observed",
    # parse_sqls hook — CASE WHEN boundaries + derived_dim_proposals.
    # (source_column, kind, value, business_meaning) → Threshold node.
    "threshold_observed",
    # parse_sqls hook — WHERE predicates (including IN-lists).
    # Filter node + N FilterValue children.
    "filter_observed",
    # parse_sqls hook — date_function inference (DATE_TRUNC, EXTRACT).
    # Complement to partition_observed; corpus-side grain signal.
    "time_grain_observed",
    # parse_sqls hook (corpus-level) — explore-cluster signature.
    # QuestionPattern node + member_query_ids.
    "question_pattern_observed",
    # parse_sqls hook — cohort_scope_signals (named CTE cohorts).
    # Cohort node + applied-to relations.
    "cohort_observed",
]


class OntologyEvent(BaseModel):
    """One append-only event in the ontology store.

    Events are emitted by per-signal-source hooks (parse_sqls, fetch_mdm,
    parse_baseline, approve_plan). The store collects them in
    ``data/ontology/events/<date>.jsonl``. Promotion of events into the
    canonical ontology happens explicitly via ``promote_candidates()``,
    so anyone can append but only confidence threshold + evidence count
    promote events into ``current.json``.
    """
    event_type: OntologyEventType
    source: str = Field(
        ...,
        description="Which hook emitted this — 'parse_sqls' | 'fetch_mdm' | "
                    "'parse_baseline' | 'approve_plan' | 'critic'",
    )
    table_name: str | None = None
    column_name: str | None = None
    entity_name: str | None = None
    payload: dict = Field(
        default_factory=dict,
        description="Event-type-specific data — e.g. {other_table, other_column} "
                    "for equivalence_observed; {synonym, canonical} for synonyms.",
    )
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence: str = Field(default="")
    observed_at: str = Field(
        default="",
        description="ISO timestamp — set by the store on append.",
    )
    # Deterministic content hash so re-emitting the same event from the
    # same source is a no-op. Computed from (event_type, source, table,
    # column, entity, sorted payload). Set by the store on append.
    content_hash: str = Field(default="")


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
    # Disambiguating descriptions carried over from the EnrichmentPlan
    # so publish can render them into the .view.lkml + .explore.lkml.
    # Empty/None on legacy enrichments — publish degrades gracefully.
    view_description: ViewDescription | None = None
    explore_description: ExploreDescription | None = None


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
