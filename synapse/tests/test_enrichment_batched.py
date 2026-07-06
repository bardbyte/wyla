"""Batched LLM enrichment (witness #5) + manifest scoping of the session stage."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from synapse.enrichment.enricher import MockLLMClient, enrich_graph
from synapse.enrichment.schemas import (
    ColumnObservation, EnrichmentBundle, SelfAssessment,
)
from synapse.enrichment.vertex_client import parse_bundle_text
from synapse.graph.store import GraphStore, canonical_uri

REPO_ROOT = Path(__file__).resolve().parents[2]


def _store_with(table: str, n_cols: int) -> GraphStore:
    store = GraphStore()
    t_uri = canonical_uri("table", table)
    store.upsert_node("Table", t_uri, {"table_name": table}, source="mdm")
    for i in range(n_cols):
        c_uri = canonical_uri("column", table, f"col_{i:03d}")
        store.upsert_node(
            "Column", c_uri,
            {"table_name": table, "data_type": "STRING"}, source="mdm")
        store.upsert_edge("CONTAINS", t_uri, c_uri, {}, source="mdm")
    return store


class CountingClient:
    """Mock that records every call's batch metadata and answers per-chunk."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def enrich(self, *, skill_md: str, context: dict[str, Any],
               table_name: str) -> EnrichmentBundle:
        cols = [c["name"] for c in context["inspection"]["columns"]]
        self.calls.append({"table": table_name,
                           "batch": context.get("batch"),
                           "n_cols": len(cols)})
        return EnrichmentBundle(
            table_name=table_name,
            table_description_proposal=(
                "desc from first chunk" if len(self.calls) == 1 else None),
            column_observations=[
                ColumnObservation(
                    column_name=c, proposed_description=f"about {c}",
                    candidate_role="attribute", self_confidence=0.8)
                for c in cols
            ],
            self_assessment=SelfAssessment(
                tables_skipped_for_lack_of_signal=[],
                columns_marked_ambiguous=0,
                proposed_entities_with_low_evidence=[],
                requires_steward_attention=[]),
        )


def test_wide_table_is_chunked_and_merged():
    store = _store_with("wide_table", 100)
    client = CountingClient()
    bundles = enrich_graph(store, client, column_batch_size=40)

    assert len(client.calls) == 3                       # 40 + 40 + 20
    assert [c["n_cols"] for c in client.calls] == [40, 40, 20]
    assert client.calls[0]["batch"] == {
        "chunk": 1, "of": 3, "columns_in_chunk": 40, "total_columns": 100}

    merged = bundles["wide_table"]
    assert len(merged.column_observations) == 100       # all chunks merged
    assert merged.table_description_proposal == "desc from first chunk"

    # every column got its llm_generated fact applied to the graph
    node = store.get(canonical_uri("column", "wide_table", "col_099"))
    assert node.properties["ai_generated_description"] == "about col_099"
    assert "llm_generated" in node.provenance.sources


def test_call_budget_skips_tables_in_band_not_silently():
    store = _store_with("t_one", 10)
    t2 = canonical_uri("table", "t_two")
    store.upsert_node("Table", t2, {"table_name": "t_two"}, source="mdm")
    for i in range(90):
        store.upsert_node(
            "Column", canonical_uri("column", "t_two", f"c{i}"),
            {"table_name": "t_two"}, source="mdm")
        store.upsert_edge("CONTAINS", t2,
                          canonical_uri("column", "t_two", f"c{i}"),
                          {}, source="mdm")
    client = CountingClient()
    bundles = enrich_graph(store, client, column_batch_size=40, max_calls=2)

    assert len(client.calls) <= 2
    enriched = set(bundles)
    assert len(enriched) == 1                            # one table fit
    only = bundles[next(iter(enriched))]
    skipped = only.self_assessment.tables_skipped_for_lack_of_signal
    assert any("enrichment budget exhausted" in s for s in skipped)


def test_only_tables_filter_is_case_insensitive():
    store = _store_with("alpha", 3)
    beta = canonical_uri("table", "beta")
    store.upsert_node("Table", beta, {"table_name": "beta"}, source="mdm")
    client = CountingClient()
    bundles = enrich_graph(store, client, only_tables=["ALPHA"])
    assert set(bundles) == {"alpha"}


# ─── tolerant bundle parsing (vertex client) ─────────────────


def test_parse_bundle_strips_fences_and_fills_self_assessment():
    text = """```json
{"table_name": "ignored", "table_description_proposal": "A view.",
 "column_observations": [{"column_name": "x", "candidate_role": "attribute",
                          "self_confidence": 0.9}]}
```"""
    bundle = parse_bundle_text(text, "real_table")
    assert bundle.table_name == "real_table"             # forced, not LLM's
    assert bundle.table_description_proposal == "A view."
    assert bundle.column_observations[0].column_name == "x"


def test_parse_bundle_failures_are_in_band():
    bundle = parse_bundle_text("I'm sorry, I can't do that.", "t")
    assert bundle.column_observations == []
    assert any("no JSON object" in n
               for n in bundle.self_assessment.requires_steward_attention)
    bundle = parse_bundle_text('{"broken": }', "t")
    assert any("unparseable" in n
               for n in bundle.self_assessment.requires_steward_attention)


# ─── session stage scoped by manifest (CLI, offline) ─────────


def test_lumi_session_scoped_to_manifest(tmp_path):
    session = {
        "in_scope_table": {"mdm_columns": [], "columns_referenced": [],
                           "aggregations": [], "queries_using_this": []},
        "out_of_scope_table": {"mdm_columns": [], "columns_referenced": [],
                               "aggregations": [], "queries_using_this": []},
    }
    session_path = tmp_path / "session1_output.json"
    session_path.write_text(json.dumps(session), encoding="utf-8")
    manifest = tmp_path / "tables.yaml"
    manifest.write_text("tables:\n  - name: in_scope_table\n",
                        encoding="utf-8")
    proc = subprocess.run(
        [sys.executable,
         str(REPO_ROOT / "synapse" / "scripts" / "pipeline.py"),
         "--lumi-session", str(session_path),
         "--manifest", str(manifest),
         "--sources-dir", str(tmp_path / "sources"),
         "--out", str(tmp_path / "graph.json")],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "1 of 2 session table(s) selected" in proc.stdout
    staged = tmp_path / "sources" / "mdm_cache"
    assert (staged / "in_scope_table.json").exists()
    assert not (staged / "out_of_scope_table.json").exists()
