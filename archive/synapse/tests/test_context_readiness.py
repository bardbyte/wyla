"""Failure digest + context-readiness scorecard — in-band must not mean
invisible, and 'rich enough' must be a number, not a vibe."""

from __future__ import annotations

from synapse.enrichment.enricher import collect_enrichment_failures
from synapse.enrichment.schemas import (
    ColumnObservation, EnrichmentBundle, SelfAssessment,
)
from synapse.graph.inspector import context_readiness
from synapse.graph.store import GraphStore, canonical_uri


def _sa(notes: list[str] | None = None) -> SelfAssessment:
    return SelfAssessment(
        tables_skipped_for_lack_of_signal=[],
        columns_marked_ambiguous=0,
        proposed_entities_with_low_evidence=[],
        requires_steward_attention=notes or [])


def test_failure_digest_counts_empty_bundles_with_reasons():
    empty = EnrichmentBundle(
        table_name="t1",
        self_assessment=_sa(["vertex call failed: RuntimeError: 503"]))
    full = EnrichmentBundle(
        table_name="t2",
        column_observations=[ColumnObservation(
            column_name="c", candidate_role="attribute",
            self_confidence=0.9, evidence_used=["mdm"])],
        self_assessment=_sa())
    digest = collect_enrichment_failures({"t1": empty, "t2": full})
    assert digest["empty_bundles"] == 1
    assert digest["n_bundles"] == 2
    assert digest["notes"][0][0].startswith("vertex call failed")
    assert digest["notes"][0][1] == 1


def test_failure_digest_all_clean_run():
    full = EnrichmentBundle(
        table_name="t",
        table_description_proposal="A table.",
        self_assessment=_sa())
    digest = collect_enrichment_failures({"t": full})
    assert digest["empty_bundles"] == 0
    assert digest["notes"] == []


# ─── scorecard ───────────────────────────────────────────────


def _store() -> GraphStore:
    store = GraphStore()
    t_uri = canonical_uri("table", "acc")
    store.upsert_node("Table", t_uri, {"table_name": "acc"}, source="mdm")
    for col, props in [
        ("c1", {"business_name": "Customer ID"}),     # has meaning
        ("c2", {}),                                    # bare schema
    ]:
        c_uri = canonical_uri("column", "acc", col)
        store.upsert_node("Column", c_uri,
                          {"table_name": "acc", **props}, source="mdm")
        store.upsert_edge("CONTAINS", t_uri, c_uri, {}, source="mdm")
    return store


def test_scorecard_measures_column_meaning_coverage():
    rows = context_readiness(_store(), ["acc", "ghost_table"])
    acc = rows[0]
    assert acc["in_graph"] is True
    assert acc["n_columns"] == 2
    assert acc["pct_columns_with_meaning"] == 50
    assert acc["has_lineage"] is False
    assert acc["has_governance"] is False
    assert rows[1] == {"table": "ghost_table", "in_graph": False}


def test_scorecard_counts_ai_descriptions_as_meaning():
    store = _store()
    store.upsert_node(
        "Column", canonical_uri("column", "acc", "c2"),
        {"ai_generated_description": "LLM-described."},
        source="llm_generated")
    rows = context_readiness(store, ["acc"])
    assert rows[0]["pct_columns_with_meaning"] == 100


def test_scorecard_sees_lineage_and_governance():
    store = _store()
    up = canonical_uri("table", "raw_acc")
    store.upsert_node("Table", up, {"table_name": "raw_acc"}, source="mdm")
    store.upsert_edge("UPSTREAM_OF", up, canonical_uri("table", "acc"),
                      {}, source="mdm")
    store.upsert_node("Table", canonical_uri("table", "acc"),
                      {"table_name": "acc", "business_unit": "SBS"},
                      source="mdm")
    row = context_readiness(store, ["acc"])[0]
    assert row["has_lineage"] is True
    assert row["has_governance"] is True
