"""Tier 4 — aggregate_table emit, view_label aliasing, coverage validator."""

from __future__ import annotations

from pathlib import Path

from lumi.coverage import validate_corpus_coverage
from lumi.explore_clusters import build_explore_plans
from lumi.publish import (
    _annotate_with_aliasing,
    _render_aggregate_table,
    _render_clustered_explore,
    publish_to_disk,
)
from lumi.schemas import EnrichedOutput, ExplorePlan, TableContext
from lumi.sql_to_context import parse_sqls


def _ctx(name: str, columns: list[str], **kw) -> TableContext:
    return TableContext(
        table_name=name,
        columns_referenced=columns,
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=[], date_functions=[],
        mdm_columns=kw.get("mdm_columns", []),
        mdm_table_description=None, mdm_coverage_pct=0.5,
        existing_view_lkml=None, queries_using_this=[],
    )


# ─── T4.1: aggregate_table render ─────────────────────────


def test_aggregate_table_renders_with_dims_measures_filters():
    agg = {
        "name": "agg__transaction__cm11",
        "base_view": "transaction",
        "group_by": ["cm11"],
        "measures": ["amount"],
        "frequency": 5,
        "filters": {"data_source": "cornerstone"},
    }
    out = _render_aggregate_table(agg)
    assert "aggregate_table: agg__transaction__cm11" in out
    assert "transaction.cm11" in out
    assert "transaction.total_amount" in out
    assert "data_source" in out
    assert "sql_trigger_value" in out


def test_aggregate_table_in_explore_when_base_view_matches():
    plan = ExplorePlan(
        cluster_id="cluster_001",
        explore_name="transaction_by_cm",
        base_view="transaction",
        dim_views=["cardmember"],
        joins=[{
            "right_table": "cardmember",
            "left_key": "cm_id", "right_key": "cm11",
            "relationship": "many_to_one",
        }],
        always_filter={},
        member_query_count=5,
        base_view_bonus_estimate=1.6,
    )
    agg_tables = [{
        "name": "agg__transaction__cm11",
        "base_view": "transaction",
        "group_by": ["cm11"],
        "measures": ["amount"],
        "frequency": 5,
        "filters": {},
    }]
    out = _render_clustered_explore(plan, aggregate_tables=agg_tables)
    assert "aggregate_table: agg__transaction__cm11" in out


def test_aggregate_table_skipped_when_base_view_mismatch():
    plan = ExplorePlan(
        cluster_id="cluster_001",
        explore_name="x",
        base_view="cardmember",
        dim_views=[],
        joins=[],
        always_filter={},
    )
    agg_tables = [{
        "name": "agg__transaction__x",
        "base_view": "transaction",  # different
        "group_by": ["x"], "measures": ["y"],
        "frequency": 5, "filters": {},
    }]
    out = _render_clustered_explore(plan, aggregate_tables=agg_tables)
    assert "aggregate_table" not in out


# ─── T4.2: view_label aliasing ────────────────────────────


def test_dim_used_by_two_explores_gets_view_label():
    """A dim shared across explores gets role-prefixed view_label."""
    plans = [
        ExplorePlan(
            cluster_id="c1", explore_name="transaction_by_cm",
            base_view="transaction", dim_views=["cardmember"],
            joins=[{
                "right_table": "cardmember", "left_key": "cm_id",
                "right_key": "cm11", "relationship": "many_to_one",
            }],
        ),
        ExplorePlan(
            cluster_id="c2", explore_name="revenue_by_cm",
            base_view="revenue", dim_views=["cardmember"],
            joins=[{
                "right_table": "cardmember", "left_key": "cm_id",
                "right_key": "cm11", "relationship": "many_to_one",
            }],
        ),
    ]
    annotated = _annotate_with_aliasing(plans)
    # Each cardmember join gets a view_label tied to the explore role.
    labels = [
        j.get("view_label")
        for ep in annotated for j in (ep.joins or [])
    ]
    # Both labels are non-empty and contain "Cardmember"
    assert all(label and "Cardmember" in label for label in labels)
    assert len(set(labels)) == 2  # different roles → different labels


def test_dim_used_by_one_explore_gets_no_label():
    plans = [
        ExplorePlan(
            cluster_id="c1", explore_name="x", base_view="t1",
            dim_views=["dim_a"],
            joins=[{
                "right_table": "dim_a", "left_key": "k",
                "right_key": "k", "relationship": "many_to_one",
            }],
        ),
    ]
    annotated = _annotate_with_aliasing(plans)
    assert annotated[0].joins[0].get("view_label") is None


def test_view_label_renders_into_explore_lkml():
    plan = ExplorePlan(
        cluster_id="c1", explore_name="x", base_view="t1",
        dim_views=["dim_a"],
        joins=[{
            "right_table": "dim_a", "left_key": "k", "right_key": "k",
            "relationship": "many_to_one",
            "view_label": "Spending Cardmember",
        }],
    )
    out = _render_clustered_explore(plan)
    assert 'view_label: "Spending Cardmember"' in out


# ─── T4.3: coverage validator ─────────────────────────────


def test_coverage_marks_query_covered_when_explore_matches():
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ])
    contexts = {
        "cardmember": _ctx("cardmember", ["cm11"]),
        "transaction": _ctx("transaction", ["cm_id", "amount"]),
    }
    plans = build_explore_plans(fps, contexts, min_cluster_size=1)
    cov = validate_corpus_coverage(fps, plans, contexts)
    assert cov.total_queries == 1
    assert cov.covered == 1
    assert cov.coverage_pct == 100.0


def test_coverage_marks_uncovered_when_no_explore_matches():
    fps = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm GROUP BY cm.cm11",
    ])
    # Plans don't include cardmember at all.
    plans = []
    contexts = {"cardmember": _ctx("cardmember", ["cm11"])}
    cov = validate_corpus_coverage(fps, plans, contexts)
    assert cov.covered == 0
    assert "no_matching_explore" in str(cov.uncovered_top_reasons)


def test_coverage_marks_missing_measure():
    """Query references a measure column not present on the explore's chain."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ])
    contexts = {
        # transaction context lacks `amount` column → measure unresolvable
        "cardmember": _ctx("cardmember", ["cm11"]),
        "transaction": _ctx("transaction", ["cm_id"]),  # no `amount`
    }
    plans = build_explore_plans(fps, contexts, min_cluster_size=1)
    cov = validate_corpus_coverage(fps, plans, contexts)
    r = cov.per_query[0]
    assert "amount" in r.measures_missing
    assert not r.is_covered


def test_coverage_fallback_when_signature_doesnt_match_but_tables_subset():
    """Query touches a strict subset of an explore's tables → still covered."""
    # First a multi-table query (defines the explore)
    fps_for_explore = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm JOIN account a ON cm.cm11 = a.cm_id "
        "JOIN transaction t ON a.acct_id = t.acct_id GROUP BY cm.cm11",
    ])
    contexts = {
        "cardmember": _ctx("cardmember", ["cm11"]),
        "account": _ctx("account", ["cm_id", "acct_id"]),
        "transaction": _ctx("transaction", ["acct_id", "amount"]),
    }
    plans = build_explore_plans(fps_for_explore, contexts, min_cluster_size=1)

    # Now a simpler query against the same superset (no transaction needed)
    test_fp = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm GROUP BY cm.cm11",
    ])
    cov = validate_corpus_coverage(test_fp, plans, contexts)
    # Should match the broader explore as fallback.
    assert cov.per_query[0].matched_explore is not None


def test_coverage_marks_unparseable_or_no_tables_uncovered():
    """Empty SQL or unparseable inputs land in the uncovered bucket."""
    fps = parse_sqls(["", "this is not sql"])
    cov = validate_corpus_coverage(fps, [], {})
    assert all(not r.is_covered for r in cov.per_query)
    assert cov.covered == 0


# ─── End-to-end publish smoke ─────────────────────────────


def test_publish_renders_aggregate_table_into_model_file(tmp_path: Path):
    """Full publish pipeline — explore_plans + aggregate_tables → model file."""
    enriched = {
        "transaction": EnrichedOutput(
            view_lkml="view: transaction { sql_table_name: dw.transaction ;; }\n",
        ),
    }
    plan = ExplorePlan(
        cluster_id="c1",
        explore_name="transaction_by_cm",
        base_view="transaction",
        dim_views=[],
        joins=[],
        always_filter={},
    )
    agg_tables = [{
        "name": "agg__transaction__cm",
        "base_view": "transaction",
        "group_by": ["cm11"], "measures": ["amount"],
        "frequency": 4, "filters": {},
    }]
    baseline_dir = tmp_path / "baseline"
    out_dir = tmp_path / "out"
    baseline_dir.mkdir()
    res = publish_to_disk(
        enriched, baseline_dir=baseline_dir, output_dir=out_dir,
        explore_plans=[plan], aggregate_tables=agg_tables,
    )
    assert res["status"] == "ok"
    model_text = (out_dir / "models" / "lumi_enriched.model.lkml").read_text()
    assert "explore: transaction_by_cm" in model_text
    assert "aggregate_table: agg__transaction__cm" in model_text
