"""Phase-B grounding levers: DQ synthesis + robust PII resolution.

The column-grounding arithmetic: mdm(3) + bq(4) = 2 witnesses → inferred.
Add the system-attested dq_engine witness (a rule synthesized from the BQ
profile) → 3 witnesses, score 0.73 → grounded. This is how the focused
build lifts profiled columns without touching the calibrator.
"""

from __future__ import annotations

from synapse.graph.builder import _mdm_pii, _synthesize_dq_from_profile
from synapse.graph.store import GraphStore, canonical_uri


def test_dq_synth_lifts_profiled_column_to_grounded():
    store = GraphStore()
    uri = canonical_uri("column", "risk_pers_acct", "acct_bal")
    # a column witnessed by mdm + bq, carrying a BQ profile
    store.upsert_node("Column", uri,
                      {"table_name": "risk_pers_acct", "name": "acct_bal"},
                      source="mdm")
    store.upsert_node("Column", uri,
                      {"null_fraction": 0.0, "approx_distinct": 900,
                       "cardinality_bucket": "high"},
                      source="bq")
    assert store.get(uri).provenance.confidence_tier == "inferred"  # 2 witnesses

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


def test_pii_resolves_from_flat_nested_and_role():
    # flat is_pii
    assert _mdm_pii({"name": "a", "is_pii": True})[1] is True
    # nested sensitivity_details dict
    assert _mdm_pii({"name": "b", "sensitivity_details": {"is_pii": True}})[1]
    # nested array keyed by attribute_name
    role, flag = _mdm_pii({"name": "fico_score", "sensitivity_details": [
        {"attribute_name": "fico_score", "is_pii": True,
         "pii_role_id": "Sensitive>FinancialAmount"}]})
    assert flag is True and role == "Sensitive>FinancialAmount"
    # role-derived: a sensitive pii_role_id implies PII even without a flag
    role, flag = _mdm_pii({"name": "cm11",
                           "pii_role_id": "Sensitive>Identifier>MemberID"})
    assert flag is True
    # a plain Internal column is not PII
    assert _mdm_pii({"name": "x", "pii_role_id": "Internal"})[1] is False
    assert _mdm_pii({"name": "y"})[1] is False
