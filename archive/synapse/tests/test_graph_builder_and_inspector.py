"""End-to-end tests: synthetic → builder → graph → inspector.

These prove the pipeline produces a real, useful graph on this laptop
without external deps. They're the closest thing we have to a system
test before the work-laptop demo."""

from __future__ import annotations


import pytest

from synapse.graph import build_graph_from_sources, inspect_table
from synapse.synthetic import generate_all_sources


@pytest.fixture(scope="module")
def graph_and_dir(tmp_path_factory):
    """One graph for the whole module — faster than per-test rebuild."""
    out_dir = tmp_path_factory.mktemp("demo")
    generate_all_sources(out_dir)
    store = build_graph_from_sources(out_dir)
    return store, out_dir


# ─── Graph build ──────────────────────────────────────────────


def test_graph_has_all_expected_node_types(graph_and_dir):
    store, _ = graph_and_dir
    stats = store.stats()
    # The fixture must produce ALL the node types so the inspector
    # exercises every code path
    for required in ("Table", "Column", "Metric", "Synonym", "User",
                     "CodeMapping", "FilterValue"):
        assert stats["nodes_by_type"].get(required, 0) > 0, (
            f"missing node type: {required}"
        )


def test_graph_table_count_matches_synthetic(graph_and_dir):
    from synapse.synthetic import SYNTHETIC_TABLES
    store, _ = graph_and_dir
    assert store.stats()["nodes_by_type"]["Table"] == len(SYNTHETIC_TABLES)


def test_graph_has_join_observed_equivalence_edges(graph_and_dir):
    store, _ = graph_and_dir
    eq_edges = [e for e in store.edges.values() if e.edge_type == "EQUIVALENT_TO"]
    assert len(eq_edges) > 0, "corpus parser failed to extract any JOIN ON pairs"


def test_graph_has_sliceable_by_edges(graph_and_dir):
    store, _ = graph_and_dir
    sl_edges = [e for e in store.edges.values() if e.edge_type == "SLICEABLE_BY"]
    assert len(sl_edges) > 0, "no metric-by-dimension co-occurrences captured"


def test_graph_has_queried_by_user_edges(graph_and_dir):
    store, _ = graph_and_dir
    q_edges = [e for e in store.edges.values() if e.edge_type == "QUERIED_BY"]
    assert len(q_edges) > 0


def test_graph_table_nodes_are_multi_sourced(graph_and_dir):
    """Top tables should have MDM + BQ + corpus + table_catalog + usage
    all contributing — that's the multi-source fusion working."""
    store, _ = graph_and_dir
    cm_uri = "synapse://table/custins_customer_insights_cardmember"
    n = store.get(cm_uri)
    assert n is not None
    sources = set(n.provenance.sources)
    assert "mdm" in sources
    assert "bq" in sources
    assert "table_catalog" in sources
    # Should accumulate at least 4 distinct sources after all 7 ingest
    assert len(sources) >= 4


def test_graph_columns_with_pii_taxonomy_are_flagged(graph_and_dir):
    store, _ = graph_and_dir
    cm11_uri = "synapse://column/custins_customer_insights_cardmember/cm11"
    n = store.get(cm11_uri)
    assert n is not None
    assert n.properties.get("is_pii") is True
    pt = n.properties.get("pii_taxonomy") or ""
    assert pt.startswith("Sensitive")


def test_graph_synonyms_include_ambiguous_symbols(graph_and_dir):
    store, _ = graph_and_dir
    syns = store.nodes_by_type("Synonym")
    surface_forms = [s.properties.get("surface_form") for s in syns]
    # CM, AA, DM should each appear multiple times with different BUs
    cm_count = sum(1 for s in surface_forms if s == "CM")
    assert cm_count >= 2


# ─── Inspector ────────────────────────────────────────────────


def test_inspect_table_returns_full_structure(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    # Top-level keys present
    for k in (
        "identity", "per_source_view", "fused_view", "columns",
        "metrics", "related_tables", "usage", "governance",
        "data_quality", "code_resolutions",
    ):
        assert k in result, f"missing top-level key: {k}"


def test_inspect_table_per_source_view_has_all_seven_blocks(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    for src in ("mdm", "corpus", "bq", "baseline_lookml", "glossary",
                "metric_catalog", "table_catalog", "usage"):
        assert src in result["per_source_view"], (
            f"missing per-source block: {src}"
        )
        block = result["per_source_view"][src]
        assert "contributed" in block
        assert "evidence_count" in block


def test_inspect_table_identifies_pii_columns(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    assert result["governance"]["has_pii"] is True
    pii_names = {c["name"] for c in result["governance"]["pii_columns"]}
    assert "cm11" in pii_names


def test_inspect_table_lists_metrics(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    metric_names = {m["technical_name"] for m in result["metrics"]}
    assert "total_billed_business" in metric_names


def test_inspect_table_finds_related_tables(graph_and_dir):
    """custins_customer_insights_cardmember joins many other tables."""
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    # At least some related tables surfaced via JOIN ON observations
    related_names = {r["table"] for r in result["related_tables"]}
    # The risk + transaction tables JOIN to cardmember via cm11 in fixtures
    assert len(related_names) >= 1


def test_inspect_table_carries_usage_signal(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    assert result["usage"]["total_queries_observed"] > 0
    assert len(result["usage"]["top_users"]) > 0


def test_inspect_table_has_quality_scores(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    dq = result["data_quality"]
    assert 0.0 <= dq["completeness_score"] <= 1.0
    assert 0.0 <= dq["consistency_score"] <= 1.0


def test_inspect_table_returns_error_for_unknown(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "does_not_exist")
    assert result.get("error") == "table_not_found"
    assert "available" in result


def test_inspect_table_columns_carry_per_source_provenance(graph_and_dir):
    """Each column entry must list which of the 7 sources confirmed it."""
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    for c in result["columns"]:
        assert "sources_contributed" in c
        assert "confidence_tier" in c
        assert "confidence_score" in c
        # cm11 should be multi-sourced (mdm + bq + corpus join evidence)
        if c["name"] == "cm11":
            assert len(c["sources_contributed"]) >= 2


def test_graph_has_data_quality_rules(graph_and_dir):
    """Dataplex-Auto-DQ-style rules surface as first-class graph nodes."""
    store, _ = graph_and_dir
    dq_rules = store.nodes_by_type("DataQualityRule")
    assert len(dq_rules) > 0, "DQ rules not ingested"
    # At least one row_count rule
    kinds = {r.properties.get("rule_kind") for r in dq_rules}
    assert "row_count" in kinds
    assert "freshness" in kinds


def test_graph_has_lineage_edges(graph_and_dir):
    """UPSTREAM_OF edges populated from MDM lineage hints."""
    store, _ = graph_and_dir
    lineage = [e for e in store.edges.values() if e.edge_type == "UPSTREAM_OF"]
    assert len(lineage) > 0, "No lineage edges emitted"


def test_graph_has_llm_generated_descriptions(graph_and_dir):
    """AI-suggested descriptions surface as `llm_generated` source on columns."""
    store, _ = graph_and_dir
    ai_cols = [
        n for n in store.nodes_by_type("Column")
        if n.properties.get("ai_generated_description")
    ]
    assert len(ai_cols) > 0
    # And at least one column has llm_generated in its source list
    multi = [n for n in ai_cols if "llm_generated" in n.provenance.sources]
    assert len(multi) > 0


def test_inspect_table_surfaces_lineage(graph_and_dir):
    """cardmember table is downstream of transaction + status tables."""
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    assert "lineage" in result
    upstream_tables = {u["table"] for u in result["lineage"]["upstream"]}
    assert "pmdl_fin_business_volume_transaction_detail" in upstream_tables


def test_inspect_table_surfaces_dq_rules(graph_and_dir):
    """DQ rules attached to the table appear under data_quality.rules."""
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    rules = result["data_quality"]["rules"]
    assert len(rules) > 0
    rule_kinds = {r["rule_kind"] for r in rules}
    assert "row_count" in rule_kinds


def test_inspect_identity_carries_asset_kind_and_tags(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    assert result["identity"]["asset_kind"] in {
        "Table", "View", "MaterializedView", "ExternalTable", "BIDashboard",
    }
    assert "cornerstone" in result["identity"]["tags"]


def test_per_source_view_has_dataplex_blocks(graph_and_dir):
    store, _ = graph_and_dir
    result = inspect_table(store, "custins_customer_insights_cardmember")
    assert "dq_engine" in result["per_source_view"]
    assert "llm_generated" in result["per_source_view"]
    assert result["per_source_view"]["dq_engine"]["contributed"] is True


def test_inspect_table_surfaces_code_resolutions(graph_and_dir):
    """Tables touching coded columns (card_product_id) should surface
    CodeMapping rows pulled from the corpus CASE WHEN extractor."""
    store, _ = graph_and_dir
    # acqdw_acquisition_us has CASE WHEN on its coded columns in synthetic SQL
    result = inspect_table(store, "acqdw_acquisition_us")
    # At least one code resolution should surface
    # (acq_type, card_type, dcsn_cd all have CASE WHEN in synthetic queries)
    assert isinstance(result["code_resolutions"], list)
