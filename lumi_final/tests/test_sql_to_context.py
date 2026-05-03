"""Session 1 tests for lumi.sql_to_context.

TDD per parent CLAUDE.md rule 8 — these tests are written FIRST and define
the contract that prepare_enrichment_context() must satisfy.

Uses MockMDMClient (in-process, no network) so tests run on any machine.
The real probe_mdm.py runs separately on Saheb's work laptop to verify
shape against production MDM.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lumi.schemas import TableContext
from lumi.sql_to_context import (
    discover_tables,
    parse_sqls,
    prepare_enrichment_context,
)


# ─── Mock MDM client ─────────────────────────────────────────


class MockMDMClient:
    """In-process stand-in for the real MDM HTTP API. Returns canned
    digested responses (matching scripts/probe_mdm.py:digest output).
    """

    def __init__(self, responses: dict[str, dict] | None = None) -> None:
        self._responses = responses or {}

    def fetch(self, table_name: str) -> dict:
        if table_name in self._responses:
            return self._responses[table_name]
        # Default empty response — matches what the real API returns for
        # tables MDM doesn't know about. discover_tables() must handle this
        # without crashing.
        return {
            "table_name": table_name,
            "table_business_name": None,
            "table_description": None,
            "column_count": 0,
            "mdm_coverage_pct": 0.0,
            "columns": [],
        }


@pytest.fixture
def mock_mdm() -> MockMDMClient:
    return MockMDMClient(
        {
            "cornerstone_metrics": {
                "table_name": "cornerstone_metrics",
                "table_business_name": "Cornerstone Performance Metrics",
                "table_description": "Daily aggregated business metrics from Cornerstone source.",
                "column_count": 12,
                "mdm_coverage_pct": 0.83,
                "columns": [
                    {"name": "rpt_dt", "business_name": "Report Date", "type": "DATE",
                     "description": "Date of metric snapshot"},
                    {"name": "bus_seg", "business_name": "Business Segment", "type": "STRING",
                     "description": "Consumer / Commercial / GNS"},
                    {"name": "data_source", "business_name": "Data Source", "type": "STRING",
                     "description": "Source system"},
                    {"name": "billed_business", "business_name": "Billed Business",
                     "type": "NUMBER", "description": "Total billed business in USD"},
                    {"name": "new_accounts_acquired", "business_name": "New Accounts Acquired",
                     "type": "NUMBER", "description": "Count of new accounts opened"},
                    {"name": "generation", "business_name": "Generation", "type": "STRING",
                     "description": "Customer generational cohort"},
                    {"name": "sub_product_group", "business_name": "Sub Product Group",
                     "type": "STRING", "description": "Card sub-product grouping"},
                    {"name": "fico_band", "business_name": "FICO Band", "type": "STRING",
                     "description": "FICO score banded category"},
                    {"name": "accounts_in_force", "business_name": "Accounts in Force",
                     "type": "NUMBER", "description": "Active account count"},
                    {"name": "bluebox_discount_revenue", "business_name": "Bluebox Discount Revenue",
                     "type": "NUMBER", "description": "Revenue from Bluebox discounts"},
                ],
            },
            "risk_pers_acct_history": {
                "table_name": "risk_pers_acct_history",
                "table_business_name": "Risk Personal Account History",
                "table_description": "Account-level risk history including delinquency aging.",
                "column_count": 8,
                "mdm_coverage_pct": 0.5,
                "columns": [
                    {"name": "acct_cust_xref_id", "business_name": "Account Customer Cross-Ref",
                     "type": "STRING", "description": "Account-customer linkage key"},
                    {"name": "acct_bal_age_mth01_cd", "business_name": "Balance Age Month 01 Code",
                     "type": "STRING", "description": "Code indicating account aging bucket"},
                    {"name": "acct_bus_unit_cd", "business_name": "Business Unit Code",
                     "type": "INTEGER", "description": None},
                    {"name": "acct_srce_sys_cd", "business_name": "Source System Code",
                     "type": "STRING", "description": None},
                    {"name": "acct_as_of_dt", "business_name": "As-of Date", "type": "DATE",
                     "description": "Snapshot date"},
                ],
            },
        }
    )


@pytest.fixture
def empty_baseline_dir(tmp_path: Path) -> Path:
    """Create an empty baseline_views/ dir for tests where baselines aren't
    yet populated."""
    d = tmp_path / "baseline_views"
    d.mkdir()
    return d


# ─── Stage 1 unit tests — parse_sqls() ───────────────────────


def test_parse_q1_simple_aggregation(q1_sql):
    """Q1: SUM(billed_business) WHERE bus_seg + data_source + rpt_dt."""
    fps = parse_sqls([q1_sql])
    assert len(fps) == 1
    fp = fps[0]
    assert "cornerstone_metrics" in fp.tables
    aggs = fp.aggregations
    assert any(a.get("function") == "SUM" and a.get("column") == "billed_business" for a in aggs)
    filter_cols = [f.get("column") for f in fp.filters]
    assert "bus_seg" in filter_cols
    assert "data_source" in filter_cols
    assert "rpt_dt" in filter_cols


def test_parse_q9_cte_extraction(q9_sql):
    """Q9 has 2 CTEs (rpah, rich) with structural filters."""
    fps = parse_sqls([q9_sql])
    fp = fps[0]
    cte_aliases = {c["alias"] for c in fp.ctes}
    assert {"rpah", "rich"} <= cte_aliases

    rpah_cte = next(c for c in fp.ctes if c["alias"] == "rpah")
    # acct_srce_sys_cd = 'TRIUMPH' is a structural filter
    structural_cols = {f["column"] for f in rpah_cte.get("structural_filters", [])}
    assert "acct_srce_sys_cd" in structural_cols


def test_parse_q9_case_when_extraction(q9_sql):
    """Q9 has 2 CASE WHENs: fico_band and age_bucket."""
    fps = parse_sqls([q9_sql])
    fp = fps[0]
    case_when_aliases = {cw["alias"] for cw in fp.case_whens}
    assert "fico_band" in case_when_aliases
    assert "age_bucket" in case_when_aliases


def test_parse_q10_three_hop_join(q10_sql):
    """Q10: rpah → drm_prod → drm_hier (joins must preserve order)."""
    fps = parse_sqls([q10_sql])
    fp = fps[0]
    joins = sorted(fp.joins, key=lambda j: j.get("order", 0))
    assert len(joins) >= 2
    join_tables = [j.get("right_table") or j.get("other_table") for j in joins]
    # drm_product_member appears before drm_product_hier
    assert any("drm_product_member" in t or "drm_prod" in t for t in join_tables[:1])


def test_parse_q2_date_function_extraction(q2_sql):
    """Q2: EXTRACT(YEAR FROM rpt_dt) lands in date_functions."""
    fps = parse_sqls([q2_sql])
    fp = fps[0]
    assert any(
        d.get("column") == "rpt_dt" and "YEAR" in str(d.get("function", "")).upper()
        for d in fp.date_functions
    )


# ─── Stage 2 unit tests — discover_tables() ─────────────────


def test_discover_returns_table_context_per_table(q1_sql, mock_mdm, empty_baseline_dir):
    fps = parse_sqls([q1_sql])
    contexts = discover_tables(fps, mock_mdm, str(empty_baseline_dir))
    assert "cornerstone_metrics" in contexts
    ctx = contexts["cornerstone_metrics"]
    assert isinstance(ctx, TableContext)
    assert ctx.table_name == "cornerstone_metrics"


def test_discover_pulls_mdm_per_table(q1_sql, mock_mdm, empty_baseline_dir):
    fps = parse_sqls([q1_sql])
    contexts = discover_tables(fps, mock_mdm, str(empty_baseline_dir))
    ctx = contexts["cornerstone_metrics"]
    assert ctx.mdm_table_description and "Cornerstone" in ctx.mdm_table_description
    assert ctx.mdm_coverage_pct >= 0.5
    col_names = {c.get("name") for c in ctx.mdm_columns}
    assert "billed_business" in col_names


def test_discover_handles_missing_mdm_gracefully(empty_baseline_dir):
    """Tables MDM doesn't know about must yield TableContext with mdm_coverage_pct=0,
    not crash.
    """
    fps = parse_sqls(["SELECT a FROM unknown_table_xyz"])
    contexts = discover_tables(fps, MockMDMClient(), str(empty_baseline_dir))
    assert "unknown_table_xyz" in contexts
    assert contexts["unknown_table_xyz"].mdm_coverage_pct == 0.0


def test_discover_loads_baseline_when_present(q1_sql, mock_mdm, tmp_path):
    """If data/baseline_views/<table>.view.lkml exists, load it as
    existing_view_lkml.
    """
    baseline_dir = tmp_path / "baseline_views"
    baseline_dir.mkdir()
    baseline_file = baseline_dir / "cornerstone_metrics.view.lkml"
    baseline_file.write_text(
        "view: cornerstone_metrics {\n  sql_table_name: `axp-lumi.dw.cornerstone_metrics` ;;\n}",
        encoding="utf-8",
    )

    fps = parse_sqls([q1_sql])
    contexts = discover_tables(fps, mock_mdm, str(baseline_dir))
    ctx = contexts["cornerstone_metrics"]
    assert ctx.existing_view_lkml is not None
    assert "sql_table_name" in ctx.existing_view_lkml


# ─── Cross-cutting: prepare_enrichment_context() ────────────


def test_multi_query_dedup(q1_sql, q4_sql, mock_mdm, empty_baseline_dir):
    """Q1 + Q4 both reference cornerstone_metrics → ONE context, both queries
    listed in queries_using_this.
    """
    contexts = prepare_enrichment_context(
        [q1_sql, q4_sql], mock_mdm, str(empty_baseline_dir)
    )
    cs = contexts["cornerstone_metrics"]
    assert len(cs.queries_using_this) == 2


def test_all_10_queries_no_crashes(all_sqls, mock_mdm, empty_baseline_dir):
    """Smoke test: all 10 sample SQLs through the full prepare pipeline
    without exceptions, producing a non-empty context dict.
    """
    contexts = prepare_enrichment_context(
        all_sqls, mock_mdm, str(empty_baseline_dir)
    )
    assert len(contexts) >= 3  # at least 3 unique tables across Q1-Q10
    for table_name, ctx in contexts.items():
        assert isinstance(ctx, TableContext)
        assert ctx.table_name == table_name


def test_filters_marked_structural_when_inside_cte(q9_sql, mock_mdm, empty_baseline_dir):
    """Filters that live inside a CTE (not user-selectable) are marked
    is_structural=True on the table they apply to.
    """
    contexts = prepare_enrichment_context(
        [q9_sql], mock_mdm, str(empty_baseline_dir)
    )
    rpah = contexts["risk_pers_acct_history"]
    structural_filter_cols = {
        f["column"] for f in rpah.filters_on_this if f.get("is_structural")
    }
    assert "acct_srce_sys_cd" in structural_filter_cols


# ─── conftest.py extension — q4_sql fixture ─────────────────


@pytest.fixture
def q4_sql():
    """Q4 also references cornerstone_metrics — used by multi-query dedup test."""
    from tests.fixtures.sample_sqls import Q4_SQL
    return Q4_SQL


@pytest.fixture
def q2_sql():
    from tests.fixtures.sample_sqls import Q2_SQL
    return Q2_SQL
