"""Tier 2: explore-cluster + ExplorePlan tests."""

from __future__ import annotations

from lumi.explore_clusters import (
    build_explore_plans,
    cluster_queries,
    propose_explore_for_cluster,
    render_clusters_for_prompt,
)
from lumi.joins import infer_join_cardinalities
from lumi.publish import _render_clustered_explore
from lumi.schemas import ExplorePlan, TableContext
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


# ─── Cluster grouping ──────────────────────────────────────


def test_clusters_share_signature_across_queries():
    """Same tables + GROUP BY + structural filters → same cluster."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
        "SELECT cm.cm11, COUNT(*) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ])
    clusters = cluster_queries(fps, min_cluster_size=1)
    # Both queries → same signature → one cluster of 2.
    assert len(clusters) == 1
    assert clusters[0].frequency == 2


def test_different_group_by_yields_different_clusters():
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
        "SELECT t.merchant_id, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY t.merchant_id",
    ])
    clusters = cluster_queries(fps, min_cluster_size=1)
    assert len(clusters) == 2


def test_different_table_set_yields_different_clusters():
    fps = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm JOIN transaction t ON cm.cm11 = t.cm_id "
        "GROUP BY cm.cm11",
        "SELECT cm.cm11 FROM cardmember cm JOIN account a ON cm.cm11 = a.cm_id "
        "GROUP BY cm.cm11",
    ])
    clusters = cluster_queries(fps, min_cluster_size=1)
    assert len(clusters) == 2


def test_cluster_member_count_aggregates():
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ] * 5)
    clusters = cluster_queries(fps, min_cluster_size=1)
    assert clusters[0].frequency == 5


def test_canonical_filters_threshold():
    """Filter must appear in > 50% of cluster members to be canonical."""
    fps = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm JOIN transaction t "
        "ON cm.cm11 = t.cm_id WHERE bus_seg = 'Consumer' GROUP BY cm.cm11",
        "SELECT cm.cm11 FROM cardmember cm JOIN transaction t "
        "ON cm.cm11 = t.cm_id WHERE bus_seg = 'Consumer' GROUP BY cm.cm11",
        "SELECT cm.cm11 FROM cardmember cm JOIN transaction t "
        "ON cm.cm11 = t.cm_id WHERE bus_seg = 'Commercial' GROUP BY cm.cm11",
    ])
    clusters = cluster_queries(fps, min_cluster_size=1)
    assert clusters[0].frequency == 3
    canonical_cols = {f["column"] for f in clusters[0].canonical_filters}
    assert "bus_seg" in canonical_cols
    # 'Consumer' appears in 2/3 → canonical. 'Commercial' in 1/3 → not.
    consumer_entries = [
        f for f in clusters[0].canonical_filters
        if f["value"] == "Consumer"
    ]
    assert consumer_entries
    assert consumer_entries[0]["frequency"] == 2


# ─── Base view selection + proposal ────────────────────────


def test_base_view_picks_table_with_aggregations():
    """The fact-grain table (where aggregations live) wins as base."""
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ] * 3)
    clusters = cluster_queries(fps, min_cluster_size=1)
    contexts = {
        "cardmember": _ctx("cardmember", ["cm11"]),
        "transaction": _ctx("transaction", ["cm_id", "amount"]),
    }
    proposal = propose_explore_for_cluster(clusters[0], contexts)
    # transaction has the aggregation source → base view
    assert proposal.base_view == "transaction"
    assert "cardmember" in proposal.dim_views


def test_proposal_uses_corpus_cardinality():
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ] * 4)
    clusters = cluster_queries(fps, min_cluster_size=1)
    contexts = {
        "cardmember": _ctx("cardmember", ["cm11"]),
        "transaction": _ctx("transaction", ["cm_id", "amount"]),
    }
    cardinalities = infer_join_cardinalities(fps)
    proposal = propose_explore_for_cluster(
        clusters[0], contexts, cardinalities=cardinalities,
    )
    # base = transaction; join to cardmember → many transactions per cm
    join = proposal.joins[0]
    # transaction → cardmember is many_to_one
    assert join["right_table"] == "cardmember"
    assert join["relationship"] == "many_to_one"


def test_partition_columns_become_always_filter():
    """MDM partition flags → always_filter on the explore."""
    fps = parse_sqls([
        "SELECT t.cm_id FROM transaction t GROUP BY t.cm_id",
    ])
    clusters = cluster_queries(fps, min_cluster_size=1)
    contexts = {
        "transaction": _ctx(
            "transaction",
            ["cm_id", "trans_dt"],
            mdm_columns=[
                {"name": "cm_id", "type": "STRING"},
                {"name": "trans_dt", "type": "DATE", "is_partitioned": True},
            ],
        ),
    }
    proposal = propose_explore_for_cluster(clusters[0], contexts)
    assert "trans_dt" in proposal.always_filter
    assert proposal.always_filter["trans_dt"] == "30 days"


def test_structural_filter_becomes_always_filter():
    fps = parse_sqls([
        "SELECT cm.cm11 FROM cardmember cm GROUP BY cm.cm11",
    ])
    clusters = cluster_queries(fps, min_cluster_size=1)
    # Inject a structural filter on the cluster
    clusters[0].structural_filters = [
        {"column": "data_source", "value": "cornerstone"},
    ]
    contexts = {"cardmember": _ctx("cardmember", ["cm11"])}
    proposal = propose_explore_for_cluster(clusters[0], contexts)
    assert proposal.always_filter.get("data_source") == "cornerstone"


# ─── ExplorePlan via build_explore_plans ───────────────────


def test_build_explore_plans_returns_pydantic_models():
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ] * 3)
    contexts = {
        "cardmember": _ctx("cardmember", ["cm11"]),
        "transaction": _ctx("transaction", ["cm_id", "amount"]),
    }
    plans = build_explore_plans(fps, contexts, min_cluster_size=1)
    assert plans
    assert isinstance(plans[0], ExplorePlan)
    assert plans[0].base_view == "transaction"
    assert plans[0].member_query_count == 3
    assert plans[0].base_view_bonus_estimate >= 1.0


def test_explore_name_reflects_grouping():
    fps = parse_sqls([
        "SELECT t.cm_id, SUM(t.amount) FROM transaction t GROUP BY t.cm_id",
    ])
    contexts = {"transaction": _ctx("transaction", ["cm_id", "amount"])}
    plans = build_explore_plans(fps, contexts, min_cluster_size=1)
    # name pattern: <base_view>_by_<group>
    assert plans[0].explore_name.startswith("transaction_by_")
    assert "cm_id" in plans[0].explore_name


# ─── Render ────────────────────────────────────────────────


def test_render_clustered_explore_includes_relationship_and_always_filter():
    plan = ExplorePlan(
        cluster_id="cluster_001",
        explore_name="transaction_by_cm",
        base_view="transaction",
        dim_views=["cardmember"],
        joins=[{
            "right_table": "cardmember",
            "left_key": "cm_id",
            "right_key": "cm11",
            "relationship": "many_to_one",
        }],
        always_filter={"trans_dt": "30 days"},
        member_query_count=5,
        base_view_bonus_estimate=1.6,
    )
    out = _render_clustered_explore(plan)
    assert "explore: transaction_by_cm" in out
    assert "relationship: many_to_one" in out
    assert "always_filter:" in out
    assert "trans_dt" in out
    # Comment block above explore.
    assert "# === EXPLORE: transaction_by_cm ===" in out
    assert "cluster_001" in out


def test_render_clusters_for_prompt_outputs_summary():
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ])
    clusters = cluster_queries(fps, min_cluster_size=1)
    md = render_clusters_for_prompt(clusters)
    assert "Question-pattern clusters" in md
    assert "frequency 1" in md
