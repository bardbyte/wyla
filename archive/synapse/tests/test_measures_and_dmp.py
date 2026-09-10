"""The two catalog exports: mined measures + DMP curated metrics.

Pins the contract for the newest pair of witnesses: the mined measures
catalog fuses as ``usage_mined`` (weight 2, user_count-scaled, table
JOINS_WITH co-occurrence edges, confidence threshold honored but never
silent), the DMP metric catalog fuses as ``dmp`` (weight 5, curated
family, gap-fill only). Both share the metric URI scheme with the
internal metric-catalog pass so cross-catalog sightings of the same
metric FUSE into one node whose tier climbs — and both work through
the pipeline in full builds and --append-to.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from synapse.graph.store import (
    SOURCE_WEIGHTS, GraphStore, apply_weight_overrides, canonical_uri,
    confidence_from_sources,
)
from synapse.loaders.dmp_loader import load_dmp_export
from synapse.loaders.measures_catalog_loader import (
    load_measures_catalog, load_table_aliases,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE = REPO_ROOT / "synapse" / "scripts" / "pipeline.py"


def _base_store() -> GraphStore:
    store = GraphStore()
    t = canonical_uri("table", "gms_transaction")
    store.upsert_node(
        "Table", t,
        {"table_name": "gms_transaction",
         "description": "Global merchant transaction spine."},
        source="mdm")
    c = canonical_uri("column", "gms_transaction", "net_purchase_amt")
    store.upsert_node(
        "Column", c,
        {"table_name": "gms_transaction", "name": "net_purchase_amt"},
        source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    return store


def _measures_file(tmp_path: Path) -> Path:
    path = tmp_path / "measures_catalog.json"
    path.write_text(json.dumps({
        "summary": {"total_measures": 5, "tables": 3},
        "measures": [
            {"id": "gms_transaction__total_merchant_spend",
             "name": "Total Merchant Spend",
             "table": "gms_transaction",
             "joined_tables": ["gms_merchant_char"],
             "business_unit": "GCS", "data_category": "Merchant",
             "expression": "SUM(net_purchase_amt)",
             "agg_function": "SUM", "column": "net_purchase_amt",
             "confidence": "high", "execution_count": 4200,
             "user_count": 30, "query_count": 80,
             "first_seen": "2025-01-03", "last_seen": "2025-07-30",
             "group_by_patterns": ["merchant_id"],
             "common_filters": ["txn_dt >= ..."],
             "complexity_tier": "simple", "score": 0.92},
            {"id": "gms_transaction__txn_count",
             "name": "Transaction Count", "table": "gms_transaction",
             "joined_tables": ["gms_merchant_char"],
             "expression": "COUNT(*)", "agg_function": "COUNT",
             "confidence": "medium", "query_count": 27,
             "last_seen": "2025-06-11"},
            # below the default threshold — no metric node, but its join
            # observation is real and must still count
            {"id": "gms_transaction__weird_ratio", "name": "weird ratio",
             "table": "gms_transaction",
             "joined_tables": ["wwcas_authorization"],
             "expression": "SUM(a)/SUM(b)", "confidence": "low",
             "query_count": 2},
            # the alias wrinkle: drifted spelling must fuse onto the
            # canonical table, not mint a doppelgänger
            {"id": "loyalty_rc_cm_offr_enroll__enroll_count",
             "name": "Enrollment Count",
             "table": "loyalty_rc_cm_offr_enroll",
             "expression": "COUNT(enroll_id)", "confidence": "high"},
            "not-an-object",
        ],
    }), encoding="utf-8")
    return path


def _dmp_file(tmp_path: Path) -> Path:
    path = tmp_path / "metrics_dmp.json"
    path.write_text(json.dumps({"metric_catalog": [
        # overlaps the mined measure — same table + name → same URI
        {"metricCatalogId": "MC-101", "author": "b.chen",
         "associatedDataProductNames": ["gms_transaction"],
         "metricDomain": "Merchant", "lineOfBusiness": "GCS",
         "metricName": "Total Merchant Spend",
         "metricDescription": "Net purchase volume across merchants.",
         "businessFriendlyMetricName": "Merchant Spend (Net)",
         "questionAnswered": "How much did merchants process?",
         "sqlExpression": "SUM(net_purchase_amt)",
         "status": "Published", "createdAt": "2024-11-01"},
        # no data products — table must come from the referenced SQL
        {"metricCatalogId": "MC-102", "metricName": "Auth Approval Rate",
         "associatedDataProductNames": [],
         "referencedSqlQuery": "SELECT ... FROM wwcas_authorization a "
                               "JOIN risk_pers_acct r ON ...",
         "sqlExpression": "approved / decisioned"},
        # unresolvable — skipped WITH a reason, never invented
        {"metricCatalogId": "MC-103", "metricName": "Orphan Metric"},
    ]}), encoding="utf-8")
    return path


def _aliases_file(tmp_path: Path) -> Path:
    path = tmp_path / "aliases.json"
    path.write_text(json.dumps(
        {"loyalty_rc_cm_offr_enroll": "loyalty_rc_cm_offer_enroll"}),
        encoding="utf-8")
    return path


def test_new_sources_are_registered_witnesses():
    assert SOURCE_WEIGHTS["dmp"] == 5
    assert SOURCE_WEIGHTS["usage_mined"] == 2
    # curated + mined agreeing = two distinct witnesses → inferred
    score, tier = confidence_from_sources(["dmp", "usage_mined"])
    assert tier == "inferred"
    assert abs(score - (5 + 2) / 15.0) < 1e-9
    # heavy usage alone climbs to inferred but can never reach grounded
    score, tier = confidence_from_sources(
        ["usage_mined"], {"usage_mined": 5})
    assert tier == "inferred" and score < 0.90


def test_measures_loader_threshold_scaling_joins_and_aliases(tmp_path):
    store = _base_store()
    aliases = load_table_aliases(_aliases_file(tmp_path))
    res = load_measures_catalog(
        store, _measures_file(tmp_path), aliases=aliases)

    # default threshold medium: high + medium + aliased high land,
    # the low one is counted — never silently dropped
    assert res["metrics"] == 3
    assert res["below_threshold"] == {"low": 1}
    assert any("not an object" in s for s in res["skipped"])

    # metric props + user_count evidence scaling (30 users → cap at 5)
    m = store.get(canonical_uri(
        "metric", "gms_transaction", "total_merchant_spend"))
    assert m.properties["business_name"] == "Total Merchant Spend"
    assert m.properties["formula"] == "SUM(net_purchase_amt)"
    assert m.properties["mined_confidence"] == "high"
    assert m.provenance.evidence_count_by_source["usage_mined"] == 5
    assert m.provenance.confidence_tier == "inferred"   # 10/15 ≥ .45

    # no user_count → confidence fallback (medium → 2 witnesses)
    m2 = store.get(canonical_uri("metric", "gms_transaction", "txn_count"))
    assert m2.provenance.evidence_count_by_source["usage_mined"] == 2
    assert m2.provenance.confidence_tier == "guessed"

    # the pre-existing table gained ONE aggregated testimony counting
    # only INGESTED measures (2 — the low-confidence row doesn't testify),
    # no doppelgänger minted; its MDM wording survived
    t = store.get(canonical_uri("table", "gms_transaction"))
    assert set(t.provenance.sources) == {"mdm", "usage_mined"}
    assert t.provenance.evidence_count_by_source["usage_mined"] == 2
    assert t.properties["description"].startswith("Global merchant")
    assert t.properties["business_unit"] == "GCS"       # gap-filled

    # JOINS_WITH: query-weighted co-occurrence, low-confidence rows count
    joins = store.outgoing(canonical_uri("table", "gms_transaction"),
                           "JOINS_WITH")
    by_target = {e.to_uri.rsplit("/", 1)[-1]: e for e in joins}
    assert by_target["gms_merchant_char"].properties["observed_count"] \
        == 80 + 27
    assert by_target["wwcas_authorization"].properties["observed_count"] == 2
    assert res["join_edges"] == 2

    # endpoint tables minted only-if-missing, at the single-witness floor
    minted = store.get(canonical_uri("table", "gms_merchant_char"))
    assert minted.provenance.confidence_tier == "guessed"

    # alias resolution: drifted spelling fused onto the canonical node
    assert store.get(canonical_uri(
        "metric", "loyalty_rc_cm_offer_enroll", "enroll_count")) is not None
    assert store.get(canonical_uri(
        "table", "loyalty_rc_cm_offr_enroll")) is None

    # COMPUTED_FROM lands on the table, and on the column ONLY because
    # the column node already existed (usage passes never mint columns)
    c_uri = canonical_uri("column", "gms_transaction", "net_purchase_amt")
    col_edges = store.incoming(c_uri, "COMPUTED_FROM")
    assert len(col_edges) == 1


def test_dmp_loader_gap_fills_resolves_and_skips(tmp_path):
    store = _base_store()
    # the graph already holds curated wording for the overlapping metric
    m_uri = canonical_uri("metric", "gms_transaction",
                          "total_merchant_spend")
    store.upsert_node(
        "Metric", m_uri,
        {"business_name": "Internal Catalog Name",
         "formula": "SUM(net_purchase_amt)",
         "sourced_from_table": "gms_transaction"},
        source="metric_catalog")

    res = load_dmp_export(store, _dmp_file(tmp_path))
    assert res["metrics"] == 2
    assert any("Orphan Metric" in s for s in res["skipped"])

    # gap-fill: the earlier catalog's wording is never overwritten,
    # DMP's unique facts land, and the DMP witness fuses → tier climbs
    m = store.get(m_uri)
    assert m.properties["business_name"] == "Internal Catalog Name"
    assert m.properties["question_answered"] \
        == "How much did merchants process?"
    assert m.properties["author"] == "b.chen"
    assert set(m.provenance.sources) == {"metric_catalog", "dmp"}
    assert m.provenance.confidence_tier == "inferred"

    # the known table gains the dmp witness + is_in_dmp, keeps wording
    t = store.get(canonical_uri("table", "gms_transaction"))
    assert "dmp" in t.provenance.sources
    assert t.properties["is_in_dmp"] is True
    assert t.properties["description"].startswith("Global merchant")

    # SQL-fallback resolution minted the unknown tables at the floor
    auth = store.get(canonical_uri("table", "wwcas_authorization"))
    assert auth is not None
    assert auth.provenance.confidence_tier == "guessed"
    m2 = store.get(canonical_uri(
        "metric", "wwcas_authorization", "auth_approval_rate"))
    assert m2 is not None
    # COMPUTED_FROM lands on every resolved table
    assert {e.to_uri.rsplit("/", 1)[-1]
            for e in store.outgoing(m2.canonical_uri, "COMPUTED_FROM")} \
        == {"wwcas_authorization", "risk_pers_acct"}


def test_weight_overrides_change_tiers_and_reject_typos(tmp_path):
    import pytest
    previous = apply_weight_overrides({"usage_mined": 8})
    try:
        # 8×5 = 40/15 → 0.99 ≥ 0.90: heavy usage now reaches grounded
        score, tier = confidence_from_sources(
            ["usage_mined"], {"usage_mined": 5})
        assert tier == "grounded"
    finally:
        apply_weight_overrides(previous)
    score, tier = confidence_from_sources(
        ["usage_mined"], {"usage_mined": 5})
    assert tier == "inferred"                    # restored
    with pytest.raises(ValueError):
        apply_weight_overrides({"usage_minedd": 3})
    with pytest.raises(ValueError):
        apply_weight_overrides({"usage_mined": "3"})


def test_pipeline_fresh_build_from_only_the_two_exports(tmp_path):
    """Recreate-from-scratch: an empty sources dir + the two files is a
    complete, servable snapshot."""
    out = tmp_path / "fresh.json"
    empty_sources = tmp_path / "sources"
    empty_sources.mkdir()
    proc = subprocess.run(
        [sys.executable, str(PIPELINE),
         "--sources-dir", str(empty_sources),
         "--out", str(out),
         "--measures-catalog", str(_measures_file(tmp_path)),
         "--dmp-export", str(_dmp_file(tmp_path)),
         "--table-aliases", str(_aliases_file(tmp_path))],
        capture_output=True, text=True, timeout=180)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "dmp witness (weight 5)" in proc.stdout
    assert "usage_mined witness (weight 2)" in proc.stdout

    store = GraphStore.load_json(out)
    # DMP ran first, so on the shared metric its curated wording won;
    # the mined pass corroborated with 5 capped user-witnesses —
    # curated + heavily-used pushes the score to the grounded gate
    m = store.get(canonical_uri(
        "metric", "gms_transaction", "total_merchant_spend"))
    assert m.properties["business_name"] == "Merchant Spend (Net)"
    assert set(m.provenance.sources) == {"dmp", "usage_mined"}
    assert m.provenance.confidence_tier == "grounded"
    # JOINS_WITH survives the snapshot round-trip
    joins = store.outgoing(canonical_uri("table", "gms_transaction"),
                           "JOINS_WITH")
    assert {e.to_uri.rsplit("/", 1)[-1] for e in joins} \
        == {"gms_merchant_char", "wwcas_authorization"}


def test_pipeline_append_with_weights_override_cli(tmp_path):
    """Growing the existing snapshot: --append-to fuses the new
    witnesses onto known nodes, and --weights-override retunes the
    experiment without touching code."""
    snap = tmp_path / "graph.json"
    base = _base_store()
    base.save_json(snap)
    v0 = base.snapshot_version

    proc = subprocess.run(
        [sys.executable, str(PIPELINE),
         "--append-to", str(snap),
         "--measures-catalog", str(_measures_file(tmp_path)),
         "--dmp-export", str(_dmp_file(tmp_path)),
         "--weights-override", '{"usage_mined": 8}'],
        capture_output=True, text=True, timeout=180)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "Append mode" in proc.stdout
    assert "Source-weights override" in proc.stdout
    assert "usage_mined: 2 → 8" in proc.stdout

    after = GraphStore.load_json(snap)
    assert after.snapshot_version != v0
    t = after.get(canonical_uri("table", "gms_transaction"))
    assert {"mdm", "dmp", "usage_mined"} <= set(t.provenance.sources)
    # under the tuned prior the heavily-used mined metric reaches
    # grounded (8×5 = 40/15 → 0.99) — the knob demonstrably changes tiers
    m = after.get(canonical_uri(
        "metric", "gms_transaction", "total_merchant_spend"))
    assert m.provenance.confidence_tier == "grounded"
