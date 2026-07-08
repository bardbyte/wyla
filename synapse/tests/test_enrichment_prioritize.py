"""Column priority scoring — the triage that decides which columns earn an
LLM call. One column of each archetype; assert tier, ranking, and that the
budget-aware selection always keeps identifiers and drops the skip tail.
"""

from __future__ import annotations

import pytest

from synapse.enrichment.prioritize import (
    prioritize_columns,
    select_for_enrichment,
)
from synapse.graph.store import GraphStore, canonical_uri


@pytest.fixture
def store() -> GraphStore:
    """risk_indv_cust_hist with five archetype columns:
    an identifier, an analyst-salient column, a coded column, an
    MDM-gap-with-evidence column, and a bare numeric tail column."""
    s = GraphStore()
    t = canonical_uri("table", "risk_indv_cust_hist")
    s.upsert_node("Table", t, {"table_name": "risk_indv_cust_hist",
                               "row_count": 1_000_000}, source="mdm")

    def col(name, props):
        c = canonical_uri("column", "risk_indv_cust_hist", name)
        s.upsert_node("Column", c, {"table_name": "risk_indv_cust_hist",
                                    **props}, source="mdm")
        s.upsert_edge("CONTAINS", t, c, {}, source="mdm")

    # identifier — declared PK, id name
    col("cust_xref_id", {"data_type": "STRING", "is_primary": True,
                         "approx_distinct": 990_000, "null_fraction": 0.0,
                         "description": "Customer cross-reference id"})
    # salient — a human analyst filters + groups on it, no description
    col("region_cd", {"data_type": "STRING", "reference_count": 12,
                      "is_group_by": True, "is_filter": True})
    # coded — low-card categorical with top values, no description
    col("decision_cd", {"data_type": "STRING", "cardinality_bucket": "low",
                        "distinct_sample": [{"value": "A"}, {"value": "D"}]})
    # gap — computed column MDM didn't describe
    col("risk_score_adj", {"data_type": "FLOAT64", "derived_logic":
                           "risk_score * seasonality_factor"})
    # skip — bare numeric feature, no usage, no evidence
    col("cdss_bus_generic_approvd_mrc3", {"data_type": "FLOAT64",
                                          "approx_distinct": 800_000,
                                          "null_fraction": 0.3})
    return s


def test_tiers_are_assigned_correctly(store):
    by_col = {p.column: p for p in
              prioritize_columns(store, "risk_indv_cust_hist")}
    assert by_col["cust_xref_id"].tier == "identifier"
    assert by_col["region_cd"].tier == "salient"
    assert by_col["decision_cd"].tier == "coded"
    assert by_col["risk_score_adj"].tier == "gap"
    assert by_col["cdss_bus_generic_approvd_mrc3"].tier == "skip"


def test_identifier_outranks_everything(store):
    ranked = prioritize_columns(store, "risk_indv_cust_hist")
    assert ranked[0].column == "cust_xref_id"
    # the skip column sorts last with score 0
    assert ranked[-1].tier == "skip" and ranked[-1].score == 0.0


def test_ranking_order_matches_tier_priority(store):
    ranked = [p.column for p in
              prioritize_columns(store, "risk_indv_cust_hist")]
    # identifier > salient > coded > gap > skip
    assert ranked.index("cust_xref_id") < ranked.index("region_cd")
    assert ranked.index("region_cd") < ranked.index("decision_cd")
    assert ranked.index("decision_cd") < ranked.index("risk_score_adj")
    assert ranked.index("risk_score_adj") \
        < ranked.index("cdss_bus_generic_approvd_mrc3")


def test_reasons_are_carried(store):
    by_col = {p.column: p for p in
              prioritize_columns(store, "risk_indv_cust_hist")}
    assert any("analyst-used" in r for r in by_col["region_cd"].reasons)
    assert any("derived_logic" in r
               for r in by_col["risk_score_adj"].reasons)
    assert any("grounded-only" in r
               for r in by_col["cdss_bus_generic_approvd_mrc3"].reasons)


def test_selection_drops_the_skip_tail(store):
    chosen = select_for_enrichment(
        prioritize_columns(store, "risk_indv_cust_hist"))
    assert "cust_xref_id" in chosen
    assert "region_cd" in chosen and "decision_cd" in chosen
    # the bare numeric tail column is never sent for an LLM call
    assert "cdss_bus_generic_approvd_mrc3" not in chosen


def test_tight_budget_still_keeps_identifiers(store):
    # budget of 1 — identifiers are always in, even past the cap
    chosen = select_for_enrichment(
        prioritize_columns(store, "risk_indv_cust_hist"), max_columns=1)
    assert "cust_xref_id" in chosen


def test_inferred_identifier_without_declared_key(store):
    # add a table whose key is NOT declared — inferred from uniqueness+name
    t = canonical_uri("table", "risk_pers_acct")
    store.upsert_node("Table", t, {"table_name": "risk_pers_acct",
                                   "row_count": 1_000_000}, source="bq")
    c = canonical_uri("column", "risk_pers_acct", "acct_id")
    store.upsert_node("Column", c, {"table_name": "risk_pers_acct",
                                    "data_type": "STRING",
                                    "approx_distinct": 999_000,
                                    "null_fraction": 0.0}, source="bq")
    store.upsert_edge("CONTAINS", t, c, {}, source="bq")
    by_col = {p.column: p for p in prioritize_columns(store, "risk_pers_acct")}
    assert by_col["acct_id"].tier == "identifier"        # no declared PK
    assert any("~unique" in r for r in by_col["acct_id"].reasons)
