"""Signal-quality primitives — querier provenance + identifier inference.

These make the enrichment priority score trustworthy: only human analysts
vote on salience, and identifiers are inferred from the data profile when
no primary key is declared (the common case).
"""

from __future__ import annotations

from synapse.enrichment.signals import (
    classify_querier,
    cross_table_column_counts,
    key_score,
    looks_like_identifier,
    split_queries_by_querier,
)
from synapse.graph.store import GraphStore, canonical_uri


# ─── querier provenance: analysts vote, machines don't ──────────


def test_human_corporate_email_is_an_analyst():
    assert classify_querier("jane.smith@axp.com") == "analyst"
    assert classify_querier("Jane.Smith@AXP.com") == "analyst"       # case
    assert classify_querier("bob@us.axp.com") == "analyst"           # subdomain


def test_service_and_external_are_operational():
    # GCP service accounts, whatever the project
    assert classify_querier(
        "svc-lumi@prj-p-lumi-gpt.iam.gserviceaccount.com") == "operational"
    # service-shaped local part even on the corp domain
    assert classify_querier("svc-etl@axp.com") == "operational"
    assert classify_querier("prj-loader@axp.com") == "operational"
    # external / partner / personal
    assert classify_querier("someone@gmail.com") == "operational"
    assert classify_querier("contractor@partner.com") == "operational"
    # malformed
    assert classify_querier("") == "operational"
    assert classify_querier(None) == "operational"
    assert classify_querier("not-an-email") == "operational"


def test_split_keeps_operational_but_separates_it():
    queries = [
        {"user_email": "jane.smith@axp.com", "sql": "SELECT decision_cd ..."},
        {"user_email": "svc-etl@prj.iam.gserviceaccount.com", "sql": "MERGE ..."},
        {"user_email": "john.doe@axp.com", "sql": "SELECT bal ..."},
    ]
    analyst, operational = split_queries_by_querier(queries)
    assert len(analyst) == 2 and len(operational) == 1
    # nothing discarded — operational is retained, just sorted
    assert operational[0]["sql"].startswith("MERGE")


# ─── identifier inference (no declared PK) ──────────────────────


def test_identifier_name_prior():
    for yes in ("acct_id", "cust_xref_id", "na_pcn_no", "member_key", "cm11"):
        assert looks_like_identifier(yes), yes
    for no in ("decision_cd", "fraud_decline_in", "bal", "rpt_month", ""):
        assert not looks_like_identifier(no), no


def test_declared_key_short_circuits():
    score, reasons = key_score(name="acct_id", declared=True)
    assert score == 1.0 and reasons == ["declared key"]


def test_unique_nonnull_idname_scores_as_a_key():
    # ~unique + non-null + id-name → a confident candidate PK, no declaration
    score, reasons = key_score(
        approx_distinct=998_000, row_count=1_000_000,
        null_fraction=0.0, name="acct_id")
    assert score >= 0.85
    assert any("unique" in r for r in reasons) and "non-null" in reasons


def test_history_table_entity_key_scores_mid_not_unique():
    # acct_id in a history table: high cardinality but many rows each — an
    # entity key, not a row-unique PK
    score, reasons = key_score(
        approx_distinct=200_000, row_count=1_000_000,
        null_fraction=0.0, name="acct_id", cross_table_count=3)
    assert 0.5 <= score < 1.0
    assert any("high-cardinality" in r for r in reasons)
    assert any("shared across 3 tables" in r for r in reasons)


def test_plain_measure_is_not_a_key():
    score, _ = key_score(
        approx_distinct=5000, row_count=1_000_000,
        null_fraction=0.2, name="bal")
    assert score < 0.3


def test_cross_table_counts_find_shared_keys():
    store = GraphStore()
    for table in ("risk_pers_acct", "risk_pers_acct_history",
                  "risk_indv_cust_hist"):
        t = canonical_uri("table", table)
        store.upsert_node("Table", t, {"table_name": table}, source="mdm")
        for col in ("acct_id", "bal"):
            c = canonical_uri("column", table, col)
            store.upsert_node("Column", c, {"table_name": table}, source="mdm")
            store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    counts = cross_table_column_counts(store)
    assert counts["acct_id"] == 3        # shared → entity-key backbone
    assert counts["bal"] == 3
