"""Incremental appends + the Collibra witness.

Pins the "graph accumulates live" contract: --append-to loads an
existing snapshot and fuses only what the run stages on top of it —
new tables mint, KNOWN tables gain the new source as one more witness
(tier recomputes upward), non-empty values never regress, sticky flags
stick, and pruning can never drop pre-existing nodes. Collibra fuses as
a weighted witness (5), never as an authority.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from synapse.graph.builder import build_graph_from_sources
from synapse.graph.store import (
    SOURCE_WEIGHTS, GraphStore, canonical_uri, confidence_from_sources,
)
from synapse.loaders.collibra_loader import load_collibra_export

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE = REPO_ROOT / "synapse" / "scripts" / "pipeline.py"


def _base_store() -> GraphStore:
    store = GraphStore()
    t = canonical_uri("table", "sbs_new_accounts")
    store.upsert_node(
        "Table", t,
        {"table_name": "sbs_new_accounts",
         "description": "Small-business new account originations."},
        source="mdm")
    c = canonical_uri("column", "sbs_new_accounts", "acct_id")
    store.upsert_node("Column", c,
                      {"table_name": "sbs_new_accounts", "name": "acct_id"},
                      source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    return store


def _export(tmp_path: Path) -> Path:
    path = tmp_path / "collibra_export.json"
    path.write_text(json.dumps({"assets": [
        {"type": "Table", "name": "sbs_new_accounts",
         "description": "Collibra's own wording, must not overwrite.",
         "domain": {"name": "Small Business"},
         "responsibilities": [
             {"role": "Steward", "user": {"name": "J. Rivera"}}],
         "status": {"name": "Approved"}},
        {"type": {"name": "Column"},
         "fullName": "sbs_new_accounts.acct_id",
         "classifications": [{"name": "PII"}]},
        {"type": "Table", "name": "sbs_merchants",
         "description": "Merchant master for small business.",
         "domain": "Small Business"},
        {"type": "Column", "name": "merch_id",
         "table": "sbs_merchants"},
        {"type": "Report", "name": "quarterly_deck"},
    ]}), encoding="utf-8")
    return path


def test_collibra_is_a_registered_witness():
    assert SOURCE_WEIGHTS["collibra"] == 5
    assert SOURCE_WEIGHTS["knowledge_catalog"] == 4
    score, tier = confidence_from_sources(["mdm", "collibra"])
    assert tier == "inferred"          # 2 distinct witnesses
    assert abs(score - (8 + 5) / 15.0) < 1e-9


def test_collibra_fuses_as_witness_never_authority(tmp_path):
    store = _base_store()
    res = load_collibra_export(store, _export(tmp_path))
    assert res["tables"] == 2 and res["columns"] == 2
    assert any("Report" in s for s in res["skipped"])

    t = store.get(canonical_uri("table", "sbs_new_accounts"))
    # one more distinct witness → tier climbs guessed → inferred
    assert set(t.provenance.sources) == {"mdm", "collibra"}
    assert t.provenance.confidence_tier == "inferred"
    # monotonic: MDM's non-empty description is never overwritten
    assert t.properties["description"].startswith("Small-business")
    # but the fields the graph lacked are filled
    assert t.properties["steward"] == "J. Rivera"
    assert t.properties["catalog_status"] == "Approved"

    # sticky PII lands on the existing column
    c = store.get(canonical_uri("column", "sbs_new_accounts", "acct_id"))
    assert c.properties["is_pii"] is True
    assert c.properties["is_sensitive"] is True

    # unknown table mints at the single-witness floor
    m = store.get(canonical_uri("table", "sbs_merchants"))
    assert m is not None
    assert m.provenance.confidence_tier == "guessed"
    assert store.get(canonical_uri("column", "sbs_merchants", "merch_id"))


def test_append_build_protects_preexisting_from_pruning(tmp_path):
    store = _base_store()
    empty_sources = tmp_path / "sources"
    empty_sources.mkdir()
    out = build_graph_from_sources(
        empty_sources, allowlist={"some_other_table"}, into=store)
    assert out is store
    # the allowlist names a different table, yet nothing pre-existing died
    assert out.get(canonical_uri("table", "sbs_new_accounts")) is not None


def test_snapshot_round_trips_the_new_sources(tmp_path):
    store = _base_store()
    load_collibra_export(store, _export(tmp_path))
    snap = tmp_path / "snap.json"
    store.save_json(snap)
    again = GraphStore.load_json(snap)
    t = again.get(canonical_uri("table", "sbs_new_accounts"))
    assert "collibra" in t.provenance.sources


def test_pipeline_append_to_cli_end_to_end(tmp_path):
    """The flag itself: build base → append a Collibra export → the same
    snapshot path now carries the fused graph and a new version."""
    snap = tmp_path / "graph.json"
    base = _base_store()
    base.save_json(snap)
    v0 = base.snapshot_version

    proc = subprocess.run(
        [sys.executable, str(PIPELINE),
         "--append-to", str(snap),
         "--collibra-export", str(_export(tmp_path))],
        capture_output=True, text=True, timeout=180)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "Append mode" in proc.stdout

    after = GraphStore.load_json(snap)
    t = after.get(canonical_uri("table", "sbs_new_accounts"))
    assert set(t.provenance.sources) == {"mdm", "collibra"}
    assert t.provenance.confidence_tier == "inferred"
    assert after.get(canonical_uri("table", "sbs_merchants")) is not None
    assert after.snapshot_version != v0
