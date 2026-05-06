"""Critic enforcement of JOIN cardinality + path grounding."""

from __future__ import annotations

from lumi.critic import _check_join_cardinality, _check_join_path_grounding
from lumi.joins import infer_canonical_paths, infer_join_cardinalities
from lumi.schemas import EnrichmentPlan, TableContext
from lumi.sql_to_context import parse_sqls


def _ctx() -> TableContext:
    return TableContext(
        table_name="cardmember",
        columns_referenced=["cm11"],
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=[], date_functions=[], mdm_columns=[],
        mdm_table_description=None, mdm_coverage_pct=0.5,
        existing_view_lkml="view: cardmember {}",
        queries_using_this=["q1"],
    )


# ─── Cardinality check ──────────────────────────────────────


def test_blocks_when_proposed_relationship_contradicts_evidence():
    """Plan says many_to_one but corpus proves one_to_many → BLOCK."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ] * 4)  # high confidence
    cards = infer_join_cardinalities(fps)

    plan = EnrichmentPlan(
        table_name="cardmember",
        proposed_dimensions=[{"name": "cm11", "source_column": "cm11", "type": "string"}],
        proposed_measures=[],
        proposed_explore={
            "base_view": "cardmember",
            "joins": [{
                "right_table": "transaction",
                "left_key": "cm11",
                "right_key": "cm_id",
                "relationship": "many_to_one",  # WRONG
            }],
        },
        reasoning="x" * 100,
    )
    issues = _check_join_cardinality(_ctx(), plan, cards)
    blocks = [i for i in issues if i.severity == "block"]
    assert blocks
    assert blocks[0].category == "join_cardinality_correctness"
    assert "one_to_many" in blocks[0].finding


def test_passes_when_relationship_matches_evidence():
    """Plan says one_to_many AND corpus proves one_to_many → no issue."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ] * 4)
    cards = infer_join_cardinalities(fps)
    plan = EnrichmentPlan(
        table_name="cardmember",
        proposed_dimensions=[{"name": "cm11", "source_column": "cm11", "type": "string"}],
        proposed_measures=[],
        proposed_explore={
            "base_view": "cardmember",
            "joins": [{
                "right_table": "transaction",
                "relationship": "one_to_many",  # CORRECT
            }],
        },
        reasoning="x" * 100,
    )
    issues = _check_join_cardinality(_ctx(), plan, cards)
    assert not [i for i in issues if i.severity == "block"]


def test_warns_when_relationship_field_missing():
    """No relationship: field → warn (Looker default may be wrong)."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ])
    cards = infer_join_cardinalities(fps)
    plan = EnrichmentPlan(
        table_name="cardmember",
        proposed_dimensions=[{"name": "cm11", "source_column": "cm11", "type": "string"}],
        proposed_measures=[],
        proposed_explore={
            "base_view": "cardmember",
            "joins": [{"right_table": "transaction"}],  # no relationship
        },
        reasoning="x" * 100,
    )
    issues = _check_join_cardinality(_ctx(), plan, cards)
    warns = [i for i in issues if i.severity == "warn"]
    assert warns
    assert "no `relationship`" in warns[0].finding.lower() or "no relationship" in warns[0].finding.lower()


def test_unknown_cardinality_does_not_block():
    """When the corpus has no decisive signal (cardinality=unknown),
    the critic must not block — there's nothing to contradict."""
    # Symmetrical *_id keys + no GROUP BY / no aggs + INNER JOIN →
    # every heuristic returns "unknown".
    fps = parse_sqls([
        "SELECT a.x, b.y FROM table_a a JOIN table_b b ON a.k = b.k",
    ])
    cards = infer_join_cardinalities(fps)
    # Card is "unknown".
    assert cards and cards[0].cardinality == "unknown"

    ctx = TableContext(
        table_name="table_a",
        columns_referenced=["k"], aggregations=[], case_whens=[],
        ctes_referencing_this=[], temp_tables_referencing_this=[],
        joins_involving_this=[], filters_on_this=[], date_functions=[],
        mdm_columns=[], mdm_table_description=None, mdm_coverage_pct=0.5,
        existing_view_lkml="view: table_a {}",
        queries_using_this=["q1"],
    )
    plan = EnrichmentPlan(
        table_name="table_a",
        proposed_dimensions=[{"name": "k", "source_column": "k", "type": "string"}],
        proposed_measures=[],
        proposed_explore={
            "base_view": "table_a",
            "joins": [{
                "right_table": "table_b",
                "relationship": "one_to_one",  # arbitrary — corpus is silent
            }],
        },
        reasoning="x" * 100,
    )
    issues = _check_join_cardinality(ctx, plan, cards)
    assert not [i for i in issues if i.severity == "block"]


# ─── Path grounding check ──────────────────────────────────


def test_warns_when_explore_proposes_unobserved_join():
    """Plan joins to a table no canonical path uses → warn."""
    fps = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm JOIN account a ON cm.cm11 = a.cm_id",
    ])
    paths = infer_canonical_paths(fps)
    plan = EnrichmentPlan(
        table_name="cardmember",
        proposed_dimensions=[{"name": "cm11", "source_column": "cm11", "type": "string"}],
        proposed_measures=[],
        proposed_explore={
            "base_view": "cardmember",
            "joins": [{
                "right_table": "merchant",  # no path uses this
                "relationship": "many_to_one",
            }],
        },
        reasoning="x" * 100,
    )
    issues = _check_join_path_grounding(_ctx(), plan, paths)
    warns = [i for i in issues if i.severity == "warn"]
    assert warns
    assert warns[0].category == "join_path_grounding"


def test_no_warning_when_join_in_canonical_path():
    """Plan joins to a table that IS in a canonical path → no warning."""
    fps = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm JOIN account a ON cm.cm11 = a.cm_id",
    ])
    paths = infer_canonical_paths(fps)
    plan = EnrichmentPlan(
        table_name="cardmember",
        proposed_dimensions=[{"name": "cm11", "source_column": "cm11", "type": "string"}],
        proposed_measures=[],
        proposed_explore={
            "base_view": "cardmember",
            "joins": [{
                "right_table": "account",
                "relationship": "many_to_one",
            }],
        },
        reasoning="x" * 100,
    )
    issues = _check_join_path_grounding(_ctx(), plan, paths)
    assert not [i for i in issues if i.category == "join_path_grounding"]


def test_no_warning_when_no_paths_observed():
    """No corpus paths for this base → can't enforce, info-only fall-through."""
    plan = EnrichmentPlan(
        table_name="cardmember",
        proposed_dimensions=[{"name": "cm11", "source_column": "cm11", "type": "string"}],
        proposed_measures=[],
        proposed_explore={
            "base_view": "cardmember",
            "joins": [{"right_table": "merchant"}],
        },
        reasoning="x" * 100,
    )
    issues = _check_join_path_grounding(_ctx(), plan, canonical_paths=[])
    assert not issues
