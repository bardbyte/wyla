"""Auto-understand entities — the cardmember behavior, restored.

An entity proposed from identifier columns materializes automatically as an
``inferred`` Entity node (no steward gate to CREATE it); a steward can later
UPGRADE it to ``human_asserted``. With min_supporting_tables=1 a single
table is enough, and the entity strengthens as more tables share the key.
"""

from __future__ import annotations

from types import SimpleNamespace

from synapse.graph.entities import apply_entities, auto_materialize_entities
from synapse.graph.store import GraphStore, canonical_uri


def _proposal(name, cols, tables):
    refs = [f"{t}::{c}" for t in tables for c in cols]
    return SimpleNamespace(
        proposed_name=name, identified_by_columns=cols,
        materialized_in_tables=tables, evidence_packet_refs=refs,
        aggregate_self_confidence=0.8)


def test_auto_entity_is_inferred_over_mdm_columns():
    store = GraphStore()
    c_uri = canonical_uri("column", "risk_pers_acct", "acct_id")
    store.upsert_node("Column", c_uri,
                      {"table_name": "risk_pers_acct", "name": "acct_id"},
                      source="mdm")

    report = auto_materialize_entities(
        store, [_proposal("Cardmember Account", ["acct_id"], ["risk_pers_acct"])])

    assert report["entities_added"] == 1 and report["edges_added"] == 1
    e = store.get(canonical_uri("entity", "Cardmember Account"))
    assert e is not None and e.node_type == "Entity"
    # LLM inference + MDM-grounded column → inferred (not guessed, not human)
    assert e.provenance.confidence_tier == "inferred"
    assert {"llm_generated", "mdm"} <= set(e.provenance.sources)
    ids = [ed for ed in store.edges.values()
           if ed.edge_type == "IDENTIFIES" and ed.to_uri == e.canonical_uri]
    assert len(ids) == 1


def test_single_table_entity_materializes_at_min_one():
    # the whole point of min_supporting_tables=1: one table is enough today
    store = GraphStore()
    store.upsert_node("Column", canonical_uri("column", "custins", "cust_xref_id"),
                      {"table_name": "custins", "name": "cust_xref_id"}, source="mdm")
    auto_materialize_entities(store, [_proposal("Customer", ["cust_xref_id"], ["custins"])])
    assert store.get(canonical_uri("entity", "Customer")) is not None


def test_phantom_entity_stays_guessed_not_inferred():
    # a proposal whose columns aren't in the graph → no mdm backing → guessed
    store = GraphStore()
    report = auto_materialize_entities(store, [_proposal("Ghost", ["nope"], ["t"])])
    assert report["edges_added"] == 0
    assert report["edges_skipped_missing_column"] == 1
    assert store.get(canonical_uri("entity", "Ghost")).provenance.confidence_tier == "guessed"


def test_steward_upgrades_auto_entity_to_human_asserted():
    store = GraphStore()
    store.upsert_node("Column", canonical_uri("column", "t", "k"),
                      {"table_name": "t", "name": "k"}, source="mdm")
    auto_materialize_entities(store, [_proposal("Customer", ["k"], ["t"])])
    assert store.get(canonical_uri("entity", "Customer")
                     ).provenance.confidence_tier == "inferred"
    # a steward approval is an UPGRADE, not a prerequisite
    apply_entities(store, [{"name": "Customer", "description": "the customer",
                            "tables": ["t"], "identified_by_columns": ["k"],
                            "evidence": ["t::k"]}])
    assert store.get(canonical_uri("entity", "Customer")
                     ).provenance.confidence_tier == "human_asserted"
