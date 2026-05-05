"""TableNarrative + SQL-alias capture tests."""

from __future__ import annotations

from lumi.narrative import (
    build_table_narrative,
    is_meaningful_alias,
    render_table_narrative,
)
from lumi.schemas import TableContext
from lumi.sql_to_context import parse_sqls


def _ctx(
    table: str = "cornerstone_metrics",
    *,
    columns: list[str] | None = None,
    mdm_columns: list[dict] | None = None,
    mdm_table_description: str | None = None,
    mdm_dataset_details: dict | None = None,
    baseline_dimensions: list[dict] | None = None,
    baseline_view_description: str | None = None,
    baseline_sql_aliases: dict | None = None,
    queries_using: list[str] | None = None,
) -> TableContext:
    return TableContext(
        table_name=table,
        columns_referenced=columns or [],
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=[], date_functions=[],
        mdm_columns=mdm_columns or [],
        mdm_table_description=mdm_table_description,
        mdm_coverage_pct=0.5,
        mdm_dataset_details=mdm_dataset_details or {},
        existing_view_lkml=None,
        baseline_dimensions=baseline_dimensions or [],
        baseline_view_description=baseline_view_description,
        baseline_sql_aliases=baseline_sql_aliases or {},
        queries_using_this=queries_using or [],
    )


# ─── Alias quality filter ────────────────────────────────────


def test_meaningful_aliases_pass():
    for good in [
        "total_billed_business", "unique_customers", "consumer_segment",
        "q4_2024_revenue", "customerLifecycleStage", "active_only",
    ]:
        assert is_meaningful_alias(good), f"{good} should pass"


def test_noise_aliases_filtered():
    for bad in [
        "a", "b", "c", "t1", "t2", "x", "x12", "tmp",
        "tmp_result", "result_x", "col_3", "col1",
        "count", "total", "n", "ID", "data",
    ]:
        assert not is_meaningful_alias(bad), f"{bad} should be filtered"


# ─── SQL alias capture in fingerprint ────────────────────────


def test_select_aliases_captured_on_fingerprint():
    sqls = [
        "SELECT SUM(billed_business) AS total_revenue, "
        "COUNT(DISTINCT cm_id) AS unique_customers FROM t",
    ]
    fps = parse_sqls(sqls)
    fp = fps[0]
    aliases = {entry["alias"]: entry["column"] for entry in fp.select_aliases}
    assert aliases.get("total_revenue") == "billed_business"
    assert aliases.get("unique_customers") == "cm_id"


def test_select_aliases_handles_simple_column_alias():
    fps = parse_sqls(["SELECT bus_seg AS customer_segment FROM t"])
    aliases = {e["alias"]: e["column"] for e in fps[0].select_aliases}
    assert aliases.get("customer_segment") == "bus_seg"


# ─── Narrative builder — identity ────────────────────────────


def test_narrative_captures_table_identity():
    ctx = _ctx(
        table="custins_customer_insights_cardmember",
        mdm_table_description="Daily snapshot of cardmember-level insights",
        mdm_dataset_details={
            "table_type": "DERIVED",
            "feed_type": "LumiFirst",
            "data_category": "Products & Services",
            "data_sub_category": "Card Add-on",
            "retention_period": 3650,
            "is_internal": True,
            "is_history_required": True,
            "business_name": "Customer Insights",
        },
    )
    n = build_table_narrative(ctx, all_fingerprints=[])
    assert n.table_name == "custins_customer_insights_cardmember"
    assert n.table_type == "DERIVED"
    assert n.feed_type == "LumiFirst"
    assert n.data_category == "Products & Services"
    assert n.data_sub_category == "Card Add-on"
    assert n.retention_period == 3650
    assert n.is_internal is True
    assert n.is_history_required is True


def test_narrative_captures_baseline_view_description():
    ctx = _ctx(
        baseline_view_description="Cornerstone daily metrics fact table",
    )
    n = build_table_narrative(ctx, all_fingerprints=[])
    assert n.baseline_view_description == "Cornerstone daily metrics fact table"


# ─── Description corpus ──────────────────────────────────────


def test_description_corpus_prefers_mdm_over_baseline():
    """When MDM has a description, we use it. Falls back to baseline
    only when MDM is empty AND baseline has ≥30 chars."""
    ctx = _ctx(
        columns=["bus_seg", "rpt_dt"],
        mdm_columns=[
            {
                "name": "bus_seg", "type": "STRING",
                "business_name": "Business Segment",
                "description": "MDM-sourced description of segment",
            },
            {
                "name": "rpt_dt", "type": "DATE",
                "business_name": "Report Date",
                "description": None,
            },
        ],
        baseline_dimensions=[
            {
                "name": "bus_seg", "type": "string", "sql": "${TABLE}.bus_seg",
                "description": "Baseline-sourced description of segment that is long enough",
            },
            {
                "name": "rpt_dt", "type": "date", "sql": "${TABLE}.rpt_dt",
                "description": "Baseline-sourced report date description that is long enough",
            },
        ],
    )
    n = build_table_narrative(ctx, all_fingerprints=[])
    by_col = {cd.column: cd for cd in n.column_descriptions}
    assert by_col["bus_seg"].source == "mdm"
    assert "MDM-sourced" in by_col["bus_seg"].description
    # rpt_dt has no MDM desc → falls through to baseline
    assert by_col["rpt_dt"].source == "baseline"


def test_description_corpus_flags_missing_columns():
    ctx = _ctx(
        columns=["mystery_col"],
        mdm_columns=[{"name": "mystery_col", "type": "STRING"}],
    )
    n = build_table_narrative(ctx, all_fingerprints=[])
    by_col = {cd.column: cd for cd in n.column_descriptions}
    assert by_col["mystery_col"].source == "missing"
    assert by_col["mystery_col"].description is None


# ─── Alias intelligence aggregation ──────────────────────────


def test_narrative_aggregates_aliases_across_corpus():
    sqls = [
        "SELECT SUM(billed_business) AS total_revenue FROM cornerstone_metrics",
        "SELECT SUM(billed_business) AS revenue_summary FROM cornerstone_metrics",
        "SELECT bus_seg AS customer_segment FROM cornerstone_metrics",
        "SELECT bus_seg AS x FROM cornerstone_metrics",  # noise filtered
    ]
    fps = parse_sqls(sqls)
    ctx = _ctx(table="cornerstone_metrics", columns=["billed_business", "bus_seg"])
    n = build_table_narrative(ctx, all_fingerprints=fps)
    # Two meaningful aliases for billed_business
    bb_aliases = n.column_to_aliases.get("billed_business", [])
    assert "total_revenue" in bb_aliases
    assert "revenue_summary" in bb_aliases
    # Customer_segment for bus_seg — the noise alias `x` filtered out
    assert "customer_segment" in n.column_to_aliases.get("bus_seg", [])
    assert "x" not in n.alias_to_column


# ─── Semantic clustering ─────────────────────────────────────


def test_semantic_clusters_from_naming_patterns():
    ctx = _ctx(
        columns=["cm_id", "cust_xref_id", "rpt_dt", "billed_business",
                 "fee_amt", "is_active_flag", "bus_seg_cd"],
        mdm_columns=[
            {"name": "cm_id"}, {"name": "cust_xref_id"},
            {"name": "rpt_dt"}, {"name": "billed_business"},
            {"name": "fee_amt"}, {"name": "is_active_flag"},
            {"name": "bus_seg_cd"},
        ],
    )
    n = build_table_narrative(ctx, all_fingerprints=[])
    # cm_id, cust_xref_id → Customer / Identity
    assert "Customer / Identity" in n.semantic_clusters
    cust = n.semantic_clusters["Customer / Identity"]
    assert "cm_id" in cust
    # rpt_dt → Time / Reporting (alone, gets filtered as <2-col cluster)
    # billed_business + fee_amt → Financial
    fin = n.semantic_clusters.get("Financial", [])
    assert "billed_business" in fin and "fee_amt" in fin


# ─── Filter-value frequencies ────────────────────────────────


def test_filter_value_frequencies_aggregate_across_queries():
    sqls = [
        "SELECT a FROM t WHERE bus_seg = 'Consumer'",
        "SELECT a FROM t WHERE bus_seg = 'Consumer'",
        "SELECT a FROM t WHERE bus_seg = 'Commercial'",
        "SELECT a FROM t WHERE data_source = 'cornerstone'",
        "SELECT a FROM t WHERE data_source = 'cornerstone'",
        "SELECT a FROM t WHERE data_source = 'cornerstone'",
    ]
    fps = parse_sqls(sqls)
    ctx = _ctx(table="t", columns=["bus_seg", "data_source", "a"])
    n = build_table_narrative(ctx, all_fingerprints=fps)
    bus_freq = dict(n.filter_value_frequencies.get("bus_seg", []))
    assert bus_freq.get("Consumer") == 2
    assert bus_freq.get("Commercial") == 1
    ds_freq = dict(n.filter_value_frequencies.get("data_source", []))
    assert ds_freq.get("cornerstone") == 3


# ─── PII roles surfaced ──────────────────────────────────────


def test_pii_role_assignments_surfaced():
    ctx = _ctx(
        columns=["cm11", "bus_seg"],
        mdm_columns=[
            {"name": "cm11", "is_pii": True, "pii_role_id": "NGBD-SDE-CM11"},
            {"name": "bus_seg", "is_pii": False, "pii_role_id": None},
        ],
    )
    n = build_table_narrative(ctx, all_fingerprints=[])
    assert len(n.pii_role_assignments) == 1
    assn = n.pii_role_assignments[0]
    assert assn["column"] == "cm11"
    assert assn["pii_role_id"] == "NGBD-SDE-CM11"


# ─── Render Markdown output ──────────────────────────────────


def test_render_markdown_complete():
    ctx = _ctx(
        table="custins_customer_insights_cardmember",
        mdm_table_description="Daily cardmember-level metrics from cornerstone",
        mdm_dataset_details={
            "table_type": "DERIVED",
            "feed_type": "LumiFirst",
            "data_category": "Customer",
            "data_sub_category": "Insights",
            "retention_period": 3650,
            "is_internal": True,
            "business_name": "Customer Insights",
        },
        columns=["cm11", "bus_seg", "rpt_dt"],
        mdm_columns=[
            {
                "name": "cm11", "is_pii": True,
                "pii_role_id": "NGBD-SDE-CM11",
                "business_name": "Card Member 11",
                "description": None,
            },
            {
                "name": "bus_seg", "type": "STRING",
                "business_name": "Business Segment",
                "description": "Consumer/Commercial/GNS classification of the customer",
            },
        ],
    )
    n = build_table_narrative(ctx, all_fingerprints=[])
    md = render_table_narrative(n)
    # Major sections present
    assert "Table narrative" in md
    assert "DERIVED" in md
    assert "LumiFirst" in md
    assert "Daily cardmember-level metrics" in md
    assert "NGBD-SDE-CM11" in md
    # Description corpus surfaced
    assert "bus_seg" in md
    assert "Customer × time grain" in md or "grain" in md.lower()
