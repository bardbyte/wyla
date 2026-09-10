"""The laptop flow the user will actually run: --lumi-session +
--mdm-crawl together. session1_output.json carries corpus + LookML +
STALE MDM digests; the fresh crawl must overwrite the MDM digest for
crawled tables while lumi's corpus/baseline artifacts survive."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from synapse.graph.builder import build_graph_from_sources
from synapse.graph.store import canonical_uri
from synapse.loaders.lumi_loader import load_lumi_for_table
from synapse.loaders.mdm_crawler import crawl_mdm_for_table

MDM_FIXTURES = Path(__file__).parent / "fixtures" / "mdm"

SESSION = {
    "roll_rate_calc": {
        # stale MDM snapshot from the old lumi run — must LOSE to the crawl
        "mdm_columns": [
            {"name": "rpt_month", "type": "DATE",
             "description": "stale month description"},
        ],
        "mdm_table_description": "STALE description from session1",
        "mdm_dataset_details": {}, "mdm_ownership": {},
        "mdm_coverage_pct": 0.5,
        # corpus signal — must SURVIVE
        "columns_referenced": ["rpt_month", "bal_lag1"],
        "aggregations": [{"function": "SUM", "column": "bal_lag1",
                          "alias": "total_bal", "query_id": "S1"}],
        "joins_involving_this": [], "case_whens": [], "filters_on_this": [],
        "date_functions": [], "ctes_referencing_this": [],
        "temp_tables_referencing_this": [],
        "queries_using_this": [
            {"query_id": "S1",
             "sql": "SELECT rpt_month, SUM(bal_lag1) AS total_bal "
                    "FROM common.roll_rate_calc GROUP BY rpt_month"},
        ],
        # baseline LookML — must SURVIVE
        "existing_view_lkml": "view: roll_rate_calc { sql_table_name: "
                              "common.roll_rate_calc ;; }",
    },
}


@pytest.fixture
def sources(tmp_path: Path) -> Path:
    sources_dir = tmp_path / "sources"
    session_path = tmp_path / "session1_output.json"
    session_path.write_text(json.dumps(SESSION), encoding="utf-8")
    # pipeline stage order: lumi (4) BEFORE the crawl (5b)
    result = load_lumi_for_table(
        "roll_rate_calc", lumi_path=session_path, out_dir=sources_dir)
    assert result.status in ("ok", "partial"), result.error
    result = crawl_mdm_for_table(
        "roll_rate_calc", out_dir=sources_dir,
        base_url="", cache_dir=MDM_FIXTURES)
    assert result.status == "ok", result.warnings
    return sources_dir


def test_fresh_crawl_overwrites_stale_session_mdm(sources: Path):
    blob = json.loads(
        (sources / "mdm_cache" / "roll_rate_calc.json").read_text())
    assert blob["table_description"] != "STALE description from session1"
    assert blob["dataset_parent_id"] == "dsp-9001"   # crawler-only fact
    assert blob["business_unit"] == "Risk"
    assert blob["lineage_upstream"]                   # crawler-only fact


def test_lumi_corpus_and_baseline_artifacts_survive(sources: Path):
    assert (sources / "baseline_views" / "roll_rate_calc.view.lkml").exists()
    signals = json.loads(
        (sources / "lumi_signals" / "roll_rate_calc.json").read_text())
    assert signals["aggregations"][0]["alias"] == "total_bal"
    assert list((sources / "gold_queries").glob("*.sql"))


def test_compiled_graph_fuses_all_three_witnesses(sources: Path):
    store = build_graph_from_sources(sources)
    node = store.get(canonical_uri("table", "roll_rate_calc"))
    assert node is not None
    # mdm (fresh crawl) + corpus (session signals) + baseline_lookml
    assert {"mdm", "corpus", "baseline_lookml"} <= set(
        node.provenance.sources)
    assert node.properties["company_domain"] == "Risk"       # fresh, not stale
    # corpus metric landed from the session's aggregation signal
    metric = store.get(canonical_uri("metric", "roll_rate_calc",
                                     "sum_bal_lag1"))
    assert metric is not None
    # multi-witness agreement lifts the table above single-source tier
    assert node.provenance.confidence_tier in ("inferred", "grounded")
    assert len(set(node.provenance.sources)) >= 3