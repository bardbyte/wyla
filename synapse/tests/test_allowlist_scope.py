"""The builder allowlist scopes a build to its manifest tables.

CTE aliases and template placeholders that corpus/skills SQL parsing
mints as Table nodes (`base`, `the`, `your_project.your_dataset.source`)
are the noise that inflates a broad build. When a build is scoped to a
manifest, the allowlist prunes every table-scoped node outside the set —
so a focused 5-table graph is exactly those five and their real columns.
"""

from __future__ import annotations

from synapse.graph.builder import _prune_to_allowlist
from synapse.graph.store import GraphStore, canonical_uri

FIVE = {
    "custins_customer_insights_cardmember",
    "fin_consumer_business_card_member_status",
    "risk_pers_acct",
    "risk_pers_acct_history",
    "risk_indv_cust_hist",
}


def _seed() -> GraphStore:
    store = GraphStore()
    # two in-scope tables (one qualified, one bare) + their columns
    for tbl in ("axp-lumi.dw.risk_pers_acct",
                "custins_customer_insights_cardmember"):
        turi = canonical_uri("table", tbl)
        store.upsert_node("Table", turi, {"table_name": tbl}, source="mdm")
        curi = canonical_uri("column", tbl, "acct_id")
        store.upsert_node("Column", curi,
                          {"table_name": tbl, "name": "acct_id"}, source="bq")
        store.upsert_edge("CONTAINS", turi, curi, {}, source="mdm")
    # a real metric + filter + dq rule on an in-scope table
    store.upsert_node("Metric", canonical_uri("metric", "risk_pers_acct", "n"),
                      {"sourced_from_table": "risk_pers_acct"}, source="corpus")
    store.upsert_node("DataQualityRule",
                      canonical_uri("dqrule", "risk_pers_acct", "nn"),
                      {"target_table": "risk_pers_acct"}, source="dq_engine")
    # NOISE: CTE aliases + template placeholder + an out-of-scope table
    for junk in ("base", "the", "your_project.your_dataset.source_table",
                 "drm_product_hier"):
        juri = canonical_uri("table", junk)
        store.upsert_node("Table", juri, {"table_name": junk}, source="skills")
        jcol = canonical_uri("column", junk, "x")
        store.upsert_node("Column", jcol, {"table_name": junk, "name": "x"},
                          source="corpus")
    # cross-cutting nodes — must survive
    store.upsert_node("Synonym", "synapse://synonym/cm",
                      {"surface_form": "CM"}, source="glossary")
    store.upsert_node("Entity", "synapse://entity/customer",
                      {"description": "the customer"}, source="human_approval")
    return store


def test_allowlist_prunes_noise_and_out_of_scope():
    store = _seed()
    tables_before = {n.properties.get("table_name")
                     for n in store.nodes_by_type("Table")}
    assert "base" in tables_before and "drm_product_hier" in tables_before

    _prune_to_allowlist(store, FIVE)

    tables_after = {n.properties.get("table_name")
                    for n in store.nodes_by_type("Table")}
    # the two in-scope tables survive (qualified name kept in property)
    assert tables_after == {"axp-lumi.dw.risk_pers_acct",
                            "custins_customer_insights_cardmember"}
    # every junk / out-of-scope table is gone
    for junk in ("base", "the", "your_project.your_dataset.source_table",
                 "drm_product_hier"):
        assert junk not in tables_after


def test_allowlist_keeps_in_scope_columns_metrics_dq():
    store = _seed()
    _prune_to_allowlist(store, FIVE)
    cols = {n.properties["table_name"] for n in store.nodes_by_type("Column")}
    assert cols == {"axp-lumi.dw.risk_pers_acct",
                    "custins_customer_insights_cardmember"}
    assert len(store.nodes_by_type("Metric")) == 1
    assert len(store.nodes_by_type("DataQualityRule")) == 1


def test_allowlist_keeps_cross_cutting_nodes():
    store = _seed()
    _prune_to_allowlist(store, FIVE)
    # Synonym + Entity are not table-scoped — never pruned
    assert len(store.nodes_by_type("Synonym")) == 1
    assert len(store.nodes_by_type("Entity")) == 1


def test_allowlist_drops_dangling_edges():
    store = _seed()
    # edge from an in-scope table to an out-of-scope column (a cross-table join)
    a = canonical_uri("column", "risk_pers_acct", "acct_id")
    b = canonical_uri("column", "drm_product_hier", "x")
    store.upsert_edge("EQUIVALENT_TO", a, b, {}, source="corpus")
    _prune_to_allowlist(store, FIVE)
    # the edge to the pruned node is gone; no edge references a dropped uri
    uris = set(store.nodes)
    for e in store.edges.values():
        assert e.from_uri in uris and e.to_uri in uris
