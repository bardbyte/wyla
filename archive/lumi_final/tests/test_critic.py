"""Critic agent tests.

Verifies the deterministic pre-flight catches all 11 categories where
they apply, plus the LLM path under mock and the merge logic.
"""

from __future__ import annotations

from unittest.mock import patch

from lumi.critic import (
    _deterministic_findings,
    critique_plan,
    format_issues_for_repair,
    render_critique_markdown,
)
from lumi.schemas import (
    CritiqueIssue,
    CritiqueReport,
    DomainOntology,
    EnrichmentPlan,
    ExploreDescription,
    OntologyEntity,
    TableContext,
    ViewDescription,
)


def _default_view_description() -> ViewDescription:
    """A minimally-complete description that satisfies disambiguation gate.

    Used in tests not specifically about disambiguation so the new
    disambiguation_completeness checks don't pollute the assertions.
    """
    return ViewDescription(
        one_liner="Generic test view for unit tests",
        grain="one row per test fixture",
        scope="test scope",
        when_to_use="for unit tests",
        when_not_to_use="not for production",
    )


def _default_explore_description() -> ExploreDescription:
    return ExploreDescription(
        one_liner="Test explore",
        primary_questions=["question 1", "question 2", "question 3"],
        anti_questions=["use a different explore for X"],
    )


def _dim(name: str, src: str | None = None, **overrides) -> dict:
    """Dim dict with all Tier-1 retrieval keys populated by default."""
    base = {
        "name": name,
        "source_column": src or name,
        "type": "string",
        "label": name.replace("_", " ").title(),
        "description": f"Description for {name}",
        "hint": f"Alternative names for {name}",
        "tags": [name],
    }
    base.update(overrides)
    return base


def _measure(name: str, src: str | None = None, agg: str = "sum", **overrides) -> dict:
    """Measure dict with all Tier-1 retrieval keys populated by default."""
    base = {
        "name": name,
        "source_column": src or name,
        "type": agg,
        "value_format_name": "decimal_2",
        "label": name.replace("_", " ").title(),
        "description": f"Sum of {src or name}",
        "hint": f"Total / aggregate / volume of {src or name}",
        "tags": [name, src or name],
    }
    base.update(overrides)
    return base


def _ctx(**kw) -> TableContext:
    return TableContext(
        table_name=kw.get("table_name", "t1"),
        columns_referenced=kw.get("columns_referenced", ["id", "name"]),
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=kw.get("filters_on_this", []),
        date_functions=[], mdm_columns=kw.get("mdm_columns", []),
        mdm_table_description=kw.get("mdm_table_description"),
        mdm_coverage_pct=kw.get("mdm_coverage_pct", 0.5),
        mdm_dataset_details=kw.get("mdm_dataset_details", {}),
        existing_view_lkml=kw.get("existing_view_lkml", "view: t1 {}"),
        baseline_primary_key_column=kw.get("baseline_primary_key_column"),
        queries_using_this=kw.get("queries_using_this", ["q1", "q2", "q3"]),
    )


def _plan(**kw) -> EnrichmentPlan:
    defaults = dict(
        table_name="t1",
        proposed_dimensions=[],
        proposed_measures=[],
        proposed_dimension_groups=[],
        reasoning=(
            "This table represents the cardmember entity at one row per "
            "cardmember per day grain. Plan adds N new dimensions and "
            "preserves the baseline PK."
        ),
        proposed_view_description=_default_view_description(),
        proposed_explore_description=_default_explore_description(),
    )
    defaults.update(kw)
    return EnrichmentPlan(**defaults)


# ─── Placeholder names (block) ──────────────────────────────


def test_placeholder_name_is_blocking():
    ctx = _ctx()
    plan = _plan(proposed_dimensions=[
        {"name": "?", "source_column": "x", "type": "string"},
    ])
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert any(
        i.severity == "block" and i.category == "reasoning_grounding"
        for i in issues
    )


def test_clean_plan_produces_no_blockers():
    ctx = _ctx()
    plan = _plan(proposed_dimensions=[_dim("card_member_id", src="cm11")])
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert not [i for i in issues if i.severity == "block"]


# ─── PK rationality ─────────────────────────────────────────


def test_pk_rationality_flags_missing_baseline_pk():
    ctx = _ctx(baseline_primary_key_column="cm_id")
    plan = _plan(proposed_dimensions=[
        {"name": "name", "source_column": "name", "type": "string"},
    ])
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert any(i.category == "pk_rationality" for i in issues)


def test_pk_present_no_finding():
    ctx = _ctx(baseline_primary_key_column="cm_id")
    plan = _plan(proposed_dimensions=[
        {"name": "cm_id", "source_column": "cm_id", "type": "string"},
    ])
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert not any(i.category == "pk_rationality" for i in issues)


# ─── Structural-filter baking ──────────────────────────────


def test_structural_filter_not_baked_blocks():
    ctx = _ctx(
        filters_on_this=[{
            "column": "data_source", "operator": "=", "value": "cornerstone",
            "is_structural": True,
        }],
    )
    plan = _plan(
        proposed_dimensions=[{"name": "x", "source_column": "x", "type": "string"}],
        proposed_explore={"base_view": "t1", "joins": []},
    )
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert any(
        i.category == "structural_filter_baking" and i.severity == "block"
        for i in issues
    )


def test_structural_filter_baked_in_sql_always_where():
    ctx = _ctx(
        filters_on_this=[{
            "column": "data_source", "operator": "=", "value": "cornerstone",
            "is_structural": True,
        }],
    )
    plan = _plan(
        proposed_dimensions=[{"name": "x", "source_column": "x", "type": "string"}],
        proposed_explore={
            "base_view": "t1", "joins": [],
            "sql_always_where": "${TABLE}.data_source = 'cornerstone'",
        },
    )
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert not any(
        i.category == "structural_filter_baking" for i in issues
    )


# ─── Partition / freshness ──────────────────────────────────


def test_partition_column_not_a_dim_group():
    ctx = _ctx(mdm_columns=[
        {"name": "snapshot_dt", "is_partitioned": True, "type": "DATE"},
    ])
    plan = _plan(proposed_dimensions=[
        {"name": "x", "source_column": "x", "type": "string"},
    ])
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert any(i.category == "partition_freshness" for i in issues)


# ─── Logical type ───────────────────────────────────────────


def test_yesno_flag_proposed_as_string_warns():
    ctx = _ctx(mdm_columns=[{"name": "is_active", "type": "BOOLEAN"}])
    plan = _plan(proposed_dimensions=[
        {"name": "is_active", "source_column": "is_active", "type": "string"},
    ])
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert any(i.category == "logical_type" for i in issues)


def test_numeric_proposed_as_string_warns():
    ctx = _ctx(mdm_columns=[{"name": "amount", "type": "NUMERIC"}])
    plan = _plan(proposed_dimensions=[
        {"name": "amount", "source_column": "amount", "type": "string"},
    ])
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert any(i.category == "logical_type" for i in issues)


# ─── Vocabulary completeness ────────────────────────────────


def test_reasoning_missing_entity_name_warns():
    ctx = _ctx(table_name="cardmember_dim")
    plan = _plan(
        reasoning="Plan adds 5 dimensions to cover query usage.",
        proposed_dimensions=[
            {"name": "id", "source_column": "id", "type": "string"},
        ],
    )
    ontology = DomainOntology(
        entities=[OntologyEntity(name="cardmember", synonyms=["card member", "cm"])],
        table_to_primary_entity={"cardmember_dim": "cardmember"},
    )
    issues = _deterministic_findings(ctx, plan, ontology=ontology)
    assert any(i.category == "vocabulary_completeness" for i in issues)


def test_reasoning_with_entity_name_passes():
    ctx = _ctx(table_name="cardmember_dim")
    plan = _plan(
        reasoning="Cardmember dimension at point-in-time grain. Plan adds dims.",
        proposed_dimensions=[
            {"name": "id", "source_column": "id", "type": "string"},
        ],
    )
    ontology = DomainOntology(
        entities=[OntologyEntity(name="cardmember", synonyms=["card member"])],
        table_to_primary_entity={"cardmember_dim": "cardmember"},
    )
    issues = _deterministic_findings(ctx, plan, ontology=ontology)
    assert not any(i.category == "vocabulary_completeness" for i in issues)


# ─── Reasoning grounding ────────────────────────────────────


def test_thin_reasoning_warns():
    ctx = _ctx()
    plan = _plan(reasoning="Adds dims.")
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert any(
        i.category == "reasoning_grounding" and "thin" in i.finding.lower()
        for i in issues
    )


# ─── Risk acknowledgement ──────────────────────────────────


def test_sparse_context_without_risks_warns():
    ctx = _ctx(
        mdm_coverage_pct=0.0,
        existing_view_lkml=None,
        queries_using_this=["q1"],
    )
    plan = _plan(risks=[])
    issues = _deterministic_findings(ctx, plan, ontology=None)
    assert any(i.category == "risk_acknowledgement" for i in issues)


# ─── Verdict + score logic ──────────────────────────────────


def test_critique_with_block_yields_retry_verdict():
    ctx = _ctx()
    plan = _plan(proposed_dimensions=[
        {"name": "?", "source_column": "x", "type": "string"},
    ])
    report = critique_plan(ctx, plan, with_llm=False)
    assert report.overall_verdict == "retry"
    assert report.block_count >= 1


def test_critique_clean_plan_approves():
    ctx = _ctx()
    plan = _plan(proposed_dimensions=[_dim("id")])
    report = critique_plan(ctx, plan, with_llm=False)
    assert report.overall_verdict in {"approve", "approve_with_warnings"}


# ─── format_issues_for_repair ──────────────────────────────


def test_format_for_repair_includes_block_and_warn_only():
    report = CritiqueReport(
        table_name="t1",
        issues=[
            CritiqueIssue(
                category="reasoning_grounding", severity="block",
                locus="x", finding="bad", recommendation="fix it",
            ),
            CritiqueIssue(
                category="logical_type", severity="warn",
                locus="y", finding="check", recommendation="verify",
            ),
            CritiqueIssue(
                category="reasoning_grounding", severity="info",
                locus="z", finding="nit", recommendation="consider",
            ),
        ],
    )
    text = format_issues_for_repair(report)
    assert "bad" in text
    assert "check" in text
    assert "nit" not in text  # info dropped from repair


def test_render_critique_markdown_zero_issues():
    report = CritiqueReport(table_name="t1", radix_retrieval_score=8)
    md = render_critique_markdown(report)
    assert "No issues raised" in md


# ─── LLM path with mock ─────────────────────────────────────


def test_llm_critique_path_merges_with_deterministic():
    ctx = _ctx(baseline_primary_key_column="cm_id")
    plan = _plan(proposed_dimensions=[
        {"name": "name", "source_column": "name", "type": "string"},
    ])
    fake_llm = CritiqueReport(
        table_name="t1",
        issues=[CritiqueIssue(
            category="radix_retrieval_alignment",
            severity="warn",
            locus="proposed_dimensions[0]",
            finding="vague name",
            recommendation="rename to something analyst-friendly",
        )],
        overall_verdict="approve_with_warnings",
        radix_retrieval_score=6,
        summary="LLM verdict",
    )
    with patch("lumi.critic._llm_critique", return_value=fake_llm):
        report = critique_plan(ctx, plan, with_llm=True)
    # Both LLM and deterministic findings present
    cats = {i.category for i in report.issues}
    assert "radix_retrieval_alignment" in cats
    assert "pk_rationality" in cats  # deterministic
    assert report.summary == "LLM verdict"
