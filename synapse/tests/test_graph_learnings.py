"""The one-table build's lessons, as machinery: steward briefings and
the failed-query corrections loop.

Pins: briefings ingest with grounding discipline (ghost tables skipped,
never minted), the human_approval provenance lift, flow into the
enrichment context and inspect_table, corrections onto the CORRECT
column with dedupe, the service tool's filter, and the apply-to-
existing-snapshot CLI round trip.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from synapse.enrichment.enricher import enrich_graph
from synapse.enrichment.schemas import EnrichmentBundle, SelfAssessment
from synapse.graph.briefings import MAX_BRIEFING_CHARS, ingest_briefings_dir
from synapse.graph.corrections import ingest_corrections_file
from synapse.graph.store import GraphStore, canonical_uri
from synapse.mcp.service import GraphService

REPO_ROOT = Path(__file__).resolve().parents[2]


def _store() -> GraphStore:
    store = GraphStore()
    t = canonical_uri("table", "accounts")
    store.upsert_node("Table", t, {"table_name": "accounts"}, source="mdm")
    c = canonical_uri("column", "accounts", "fico_score")
    store.upsert_node("Column", c, {"table_name": "accounts",
                                    "name": "fico_score"}, source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    return store


# ─── briefings ───────────────────────────────────────────────


def test_briefing_ingest_grounds_and_lifts_provenance(tmp_path):
    briefs = tmp_path / "briefings"
    briefs.mkdir()
    (briefs / "accounts.md").write_text(
        "Grain: one row per acct_id × month.", encoding="utf-8")
    (briefs / "ghost_table.md").write_text("nope", encoding="utf-8")
    (briefs / "README.md").write_text("the template", encoding="utf-8")

    store = _store()
    report = ingest_briefings_dir(store, briefs)
    assert report["applied"] == ["accounts"]
    assert report["skipped_missing_table"] == ["ghost_table"]

    node = store.get(canonical_uri("table", "accounts"))
    assert "one row per acct_id" in node.properties["briefing"]
    assert "human_approval" in node.provenance.sources
    assert node.provenance.confidence_tier == "human_asserted"


def test_briefing_matches_qualified_filenames(tmp_path):
    briefs = tmp_path / "briefings"
    briefs.mkdir()
    (briefs / "proj.dataset.accounts.md").write_text("grain note",
                                                     encoding="utf-8")
    store = _store()
    report = ingest_briefings_dir(store, briefs)
    assert report["applied"] == ["accounts"]     # normalized match


def test_oversized_briefing_is_capped_not_swallowed(tmp_path):
    briefs = tmp_path / "briefings"
    briefs.mkdir()
    (briefs / "accounts.md").write_text("x" * (MAX_BRIEFING_CHARS + 500),
                                        encoding="utf-8")
    store = _store()
    report = ingest_briefings_dir(store, briefs)
    assert report["truncated"] == ["accounts"]
    text = store.get(canonical_uri("table", "accounts")).properties[
        "briefing"]
    assert text.endswith("[briefing truncated]")


def test_briefing_rides_into_enrichment_context_and_inspect(tmp_path):
    briefs = tmp_path / "briefings"
    briefs.mkdir()
    (briefs / "accounts.md").write_text("Grain: acct_id × month.",
                                        encoding="utf-8")
    store = _store()
    ingest_briefings_dir(store, briefs)

    seen: dict = {}

    class Capture:
        def enrich(self, *, skill_md, context, table_name):
            seen[table_name] = context.get("table_briefing")
            return EnrichmentBundle(
                table_name=table_name, column_observations=[],
                self_assessment=SelfAssessment(
                    tables_skipped_for_lack_of_signal=[],
                    columns_marked_ambiguous=0,
                    proposed_entities_with_low_evidence=[],
                    requires_steward_attention=["capture only"]))

    enrich_graph(store, Capture(), only_tables=["accounts"])
    assert seen["accounts"] == "Grain: acct_id × month."

    inspected = GraphService(store).inspect_table("accounts")
    assert inspected["data"]["briefing"] == "Grain: acct_id × month."


# ─── corrections ─────────────────────────────────────────────


def test_corrections_land_on_the_correct_column(tmp_path):
    path = tmp_path / "corrections.json"
    path.write_text(json.dumps([
        {"table": "accounts", "wrong_name": "fico",
         "correct_name": "fico_score", "evidence_count": 3},
        {"table": "accounts", "wrong_name": "fico",
         "correct_name": "fico_score", "evidence_count": 1},  # dedupe: max
        {"table": "accounts", "wrong_name": "credit",
         "correct_name": "ghost_col", "evidence_count": 9},   # skipped
    ]), encoding="utf-8")
    store = _store()
    report = ingest_corrections_file(store, path)
    assert report["applied"] == 2
    assert len(report["skipped_missing_column"]) == 1

    col = store.get(canonical_uri("column", "accounts", "fico_score"))
    corr = col.properties["naming_corrections"]
    assert corr == [{"wrong_name": "fico", "evidence_count": 3}]
    assert "bq" in col.provenance.sources


def test_corrections_tool_filters_and_reports(tmp_path):
    path = tmp_path / "corrections.json"
    path.write_text(json.dumps([
        {"table": "accounts", "wrong_name": "fico",
         "correct_name": "fico_score", "evidence_count": 2},
    ]), encoding="utf-8")
    store = _store()
    ingest_corrections_file(store, path)
    svc = GraphService(store)

    hit = svc.get_failed_query_corrections("fico")
    assert hit["data"]["count"] == 1
    assert hit["data"]["corrections"][0]["correct_name"] == "fico_score"

    miss = svc.get_failed_query_corrections("balance")
    assert miss["data"]["count"] == 0            # honest empty, still ok


# ─── the apply-to-existing-snapshot CLI ──────────────────────


def test_apply_curation_cli_round_trip(tmp_path):
    snap = tmp_path / "graph_snapshot.json"
    _store().save_json(snap)
    briefs = tmp_path / "briefings"
    briefs.mkdir()
    (briefs / "accounts.md").write_text("Grain: acct_id × month.",
                                        encoding="utf-8")
    corrections = tmp_path / "corrections.json"
    corrections.write_text(json.dumps([
        {"table": "accounts", "wrong_name": "fico",
         "correct_name": "fico_score"}]), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable,
         str(REPO_ROOT / "synapse" / "scripts" / "apply_curation.py"),
         "--snapshot", str(snap), "--briefings", str(briefs),
         "--corrections", str(corrections)],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "briefings: 1 applied" in proc.stdout
    assert "corrections: 1 applied" in proc.stdout

    loaded = GraphStore.load_json(snap)
    assert loaded.get(canonical_uri("table", "accounts")).properties[
        "briefing"].startswith("Grain")
    assert loaded.get(canonical_uri("column", "accounts", "fico_score")
                      ).properties["naming_corrections"]
