"""Phase-B grounding lever: DQ synthesis from the BQ profile.

Add the system-attested dq_engine witness (a rule synthesized from the BQ
profile) to a corpus+bq column → 3 witnesses, score 0.73 → grounded. This
is how the focused build lifts profiled columns without touching the
calibrator. (PII/governance resolution is pinned in test_mdm_spine.py.)
"""

from __future__ import annotations

from synapse.graph.builder import _synthesize_dq_from_profile
from synapse.graph.store import GraphStore, canonical_uri


def test_dq_synth_lifts_profiled_column_to_grounded():
    store = GraphStore()
    uri = canonical_uri("column", "risk_pers_acct", "acct_bal")
    # a column witnessed by corpus + bq, carrying a BQ profile. (mdm + bq
    # alone already grounds under the new weighting — this uses the weaker
    # corpus+bq pair, which sits at inferred, to show DQ do the lifting.)
    store.upsert_node("Column", uri,
                      {"table_name": "risk_pers_acct", "name": "acct_bal",
                       "is_join_key": True},
                      source="corpus")
    store.upsert_node("Column", uri,
                      {"null_fraction": 0.0, "approx_distinct": 900,
                       "cardinality_bucket": "high"},
                      source="bq")
    assert store.get(uri).provenance.confidence_tier == "inferred"  # corpus+bq

    made = _synthesize_dq_from_profile(store)

    assert made >= 1
    node = store.get(uri)
    assert "dq_engine" in node.provenance.sources          # witness recorded
    assert node.provenance.confidence_tier == "grounded"   # 3 witnesses → grounded
    # the rule node + VALIDATED_BY edge exist
    rules = store.nodes_by_type("DataQualityRule")
    assert any(r.properties["rule_kind"] == "not_null" for r in rules)
    assert any(e.edge_type == "VALIDATED_BY" for e in store.edges.values())


def test_dq_synth_skips_unprofiled_columns():
    store = GraphStore()
    uri = canonical_uri("column", "risk_pers_acct", "note")
    store.upsert_node("Column", uri,
                      {"table_name": "risk_pers_acct", "name": "note"},
                      source="mdm")  # no BQ profile
    _synthesize_dq_from_profile(store)
    # no profile → no rule, no dq_engine witness, still not grounded
    assert "dq_engine" not in store.get(uri).provenance.sources
    assert not store.nodes_by_type("DataQualityRule")


def test_dq_synth_row_count_rule_on_table():
    store = GraphStore()
    turi = canonical_uri("table", "risk_pers_acct")
    store.upsert_node("Table", turi,
                      {"table_name": "risk_pers_acct", "row_count": 845_000},
                      source="mdm")
    _synthesize_dq_from_profile(store)
    assert "dq_engine" in store.get(turi).provenance.sources
    assert any(r.properties["rule_kind"] == "row_count"
               for r in store.nodes_by_type("DataQualityRule"))
