"""The entity layer — things, not strings.

Pins the full bootstrap: memory → proposals (pure reduction, threshold
2 for the 5-table scope) → review YAML → apply as human_asserted with
grounding discipline → witness #6 on future compiles → grounding-index
pickup. Zero LLM calls anywhere in this path.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml

from synapse.enrichment.enricher import _grounding_index, propose_entities
from synapse.graph.builder import build_graph_from_sources
from synapse.graph.entities import (
    apply_entities, ingest_entities_file, load_bundles_from_memory,
    read_approved, write_review_yaml,
)
from synapse.graph.store import GraphStore, canonical_uri

REPO_ROOT = Path(__file__).resolve().parents[2]


def _memory_blob() -> dict:
    """Two tables agreeing on Account via acct_id; one single-table
    entity (Merchant) that must NOT clear the 2-table bar."""
    def obs(col, entity, conf=0.85):
        return {"column_name": col, "candidate_role": "identifier",
                "candidate_entity_name": entity, "self_confidence": conf,
                "evidence_used": ["mdm"]}
    sa = {"tables_skipped_for_lack_of_signal": [],
          "columns_marked_ambiguous": 0,
          "proposed_entities_with_low_evidence": [],
          "requires_steward_attention": []}
    return {
        "accounts": {"table_name": "accounts",
                     "column_observations": [obs("acct_id", "Account")],
                     "self_assessment": sa},
        "history": {"table_name": "history",
                    "column_observations": [obs("acct_id", "Account"),
                                            obs("merch_id", "Merchant")],
                    "self_assessment": sa},
    }


def _store_with_columns() -> GraphStore:
    store = GraphStore()
    for table, cols in [("accounts", ["acct_id"]),
                        ("history", ["acct_id", "merch_id"])]:
        t_uri = canonical_uri("table", table)
        store.upsert_node("Table", t_uri, {"table_name": table}, source="mdm")
        for col in cols:
            c_uri = canonical_uri("column", table, col)
            store.upsert_node("Column", c_uri, {"table_name": table},
                              source="mdm")
            store.upsert_edge("CONTAINS", t_uri, c_uri, {}, source="mdm")
    return store


# ─── propose: pure reduction with the 5-table-scope threshold ─


def test_propose_from_memory_respects_table_threshold(tmp_path):
    mem = tmp_path / "enrichment_memory.json"
    mem.write_text(json.dumps(_memory_blob()), encoding="utf-8")
    bundles = load_bundles_from_memory(mem)
    assert set(bundles) == {"accounts", "history"}

    two = propose_entities(bundles, min_supporting_tables=2,
                           min_aggregate_confidence=0.6)
    assert [p.proposed_name for p in two] == ["Account"]   # Merchant excluded

    one = propose_entities(bundles, min_supporting_tables=1,
                           min_aggregate_confidence=0.6)
    assert {p.proposed_name for p in one} == {"Account", "Merchant"}


def test_review_yaml_defaults_to_not_approved(tmp_path):
    mem = tmp_path / "m.json"
    mem.write_text(json.dumps(_memory_blob()), encoding="utf-8")
    proposals = propose_entities(load_bundles_from_memory(mem),
                                 min_supporting_tables=2,
                                 min_aggregate_confidence=0.6)
    review = write_review_yaml(proposals, tmp_path / "review.yaml")
    doc = yaml.safe_load(review.read_text(encoding="utf-8"))
    assert doc["proposals"][0]["name"] == "Account"
    assert doc["proposals"][0]["approve"] is False       # silence ≠ consent
    assert read_approved(review) == []                   # nothing approved yet


# ─── apply: human_asserted, grounded edges only ──────────────


def test_apply_mints_human_asserted_entity_with_grounded_edges():
    store = _store_with_columns()
    report = apply_entities(store, [{
        "name": "Account",
        "identified_by_columns": ["acct_id"],
        "tables": ["accounts", "history"],
        "evidence": ["accounts::acct_id", "history::acct_id",
                     "ghost_table::acct_id"],           # one ghost ref
    }])
    assert report["entities_added"] == 1
    assert report["edges_added"] == 2
    assert report["edges_skipped_missing_column"] == 1  # ghost skipped

    node = store.get(canonical_uri("entity", "Account"))
    assert node is not None and node.node_type == "Entity"
    assert node.provenance.confidence_tier == "human_asserted"
    assert node.provenance.confidence_score >= 0.99

    edge = store.edges[
        f"{canonical_uri('column', 'accounts', 'acct_id')}"
        f"::IDENTIFIES::{canonical_uri('entity', 'Account')}"]
    assert "human_approval" in edge.provenance.sources


def test_apply_survives_snapshot_round_trip(tmp_path):
    store = _store_with_columns()
    apply_entities(store, [{"name": "Account",
                            "evidence": ["accounts::acct_id"]}])
    snap = tmp_path / "graph_snapshot.json"
    store.save_json(snap)
    loaded = GraphStore.load_json(snap)
    node = loaded.get(canonical_uri("entity", "Account"))
    assert node is not None
    assert node.provenance.confidence_tier == "human_asserted"


# ─── witness #6: builder ingest on compile ───────────────────


def test_builder_ingests_entities_yaml_as_witness(tmp_path):
    (tmp_path / "entities.yaml").write_text(yaml.safe_dump({
        "entities": [{"name": "Account",
                      "evidence": ["accounts::acct_id"]}],
    }), encoding="utf-8")
    store = build_graph_from_sources(tmp_path)
    node = store.get(canonical_uri("entity", "Account"))
    assert node is not None                              # entity minted
    # no column witnesses in this compile → the edge was skipped, not faked
    assert not [e for e in store.edges.values()
                if e.edge_type == "IDENTIFIES"]


def test_ingest_is_noop_when_file_absent(tmp_path):
    store = GraphStore()
    report = ingest_entities_file(store, tmp_path / "entities.yaml")
    assert report["entities_added"] == 0
    assert len(store.nodes) == 0


# ─── downstream: the grounding index knows entities ──────────


def test_grounding_index_includes_entity_names():
    store = _store_with_columns()
    apply_entities(store, [{"name": "Card Product", "evidence": []}])
    known = _grounding_index(store)
    assert "cardproduct" in known    # synonyms/demo-questions can ground on it


# ─── the CLI end to end ──────────────────────────────────────


def test_cli_propose_then_apply_round_trip(tmp_path):
    mem = tmp_path / "enrichment_memory.json"
    mem.write_text(json.dumps(_memory_blob()), encoding="utf-8")
    snap = tmp_path / "graph_snapshot.json"
    _store_with_columns().save_json(snap)
    review = tmp_path / "entity_review.yaml"
    approvals = tmp_path / "config" / "entities.yaml"
    script = str(REPO_ROOT / "synapse" / "scripts" / "entities.py")

    proc = subprocess.run(
        [sys.executable, script, "propose", "--memory", str(mem),
         "--out", str(review)],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "1 entity proposal(s)" in proc.stdout

    # steward flips the flag
    doc = yaml.safe_load(review.read_text(encoding="utf-8"))
    doc["proposals"][0]["approve"] = True
    review.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, script, "apply", "--review", str(review),
         "--snapshot", str(snap), "--save-approvals", str(approvals)],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "human_asserted" in proc.stdout

    loaded = GraphStore.load_json(snap)
    assert loaded.get(canonical_uri("entity", "Account")) is not None
    persisted = yaml.safe_load(approvals.read_text(encoding="utf-8"))
    assert persisted["entities"][0]["name"] == "Account"
    assert "approve" not in persisted["entities"][0]     # filtered file


def test_cli_apply_refuses_when_nothing_approved(tmp_path):
    review = tmp_path / "review.yaml"
    review.write_text(yaml.safe_dump(
        {"proposals": [{"name": "Account", "approve": False}]}),
        encoding="utf-8")
    snap = tmp_path / "snap.json"
    GraphStore().save_json(snap)
    proc = subprocess.run(
        [sys.executable,
         str(REPO_ROOT / "synapse" / "scripts" / "entities.py"),
         "apply", "--review", str(review), "--snapshot", str(snap)],
        capture_output=True, text=True)
    assert proc.returncode == 1
    assert "nothing approved" in proc.stderr
