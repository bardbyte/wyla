"""Failed-query corrections — the graph learns from what analysts get
WRONG, not just from what they get right.

The original single-table build surfaced known column misnamings
("fico" → ``fico_score``) captured from failed-query logs, and its
agent checked them proactively before naming any column. The corpus
witness only ever ingested successful SQL — this closes the other half
of the loop.

Source artifact: ``corrections.json`` — a list of
``{table, wrong_name, correct_name, evidence_count, last_seen?}``.
Today it is a curated file under ``semantic-graph/config/``; when the
laptop extraction adds failed-job mining (INFORMATION_SCHEMA.JOBS,
error messages parsed for unknown-column names), the same shape flows
through unchanged.

Each entry becomes a ``naming_corrections`` fact on the CORRECT
column's node with source ``bq`` (warehouse-observed telemetry).
Grounding discipline: an entry whose correct column is not in the
graph is SKIPPED and reported — corrections never mint columns.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from synapse.graph.store import GraphStore, canonical_uri


def ingest_corrections_file(store: GraphStore,
                            path: Path) -> dict[str, Any]:
    """Fold a corrections file into Column nodes. No-op when absent."""
    report: dict[str, Any] = {"applied": 0, "skipped_missing_column": []}
    if not path or not path.exists():
        return report
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        report["error"] = f"unreadable corrections file: {path.name}"
        return report
    if not isinstance(entries, list):
        report["error"] = "corrections.json must be a list of entries"
        return report

    for entry in entries:
        table = str(entry.get("table", "")).strip()
        wrong = str(entry.get("wrong_name", "")).strip()
        correct = str(entry.get("correct_name", "")).strip()
        if not (table and wrong and correct):
            continue
        col_uri = canonical_uri("column", table, correct)
        node = store.get(col_uri)
        if node is None:
            report["skipped_missing_column"].append(
                f"{table}.{correct} (for wrong name {wrong!r})")
            continue
        existing = [c for c in
                    (node.properties.get("naming_corrections") or [])
                    if isinstance(c, dict)]
        merged = {c.get("wrong_name"): c for c in existing}
        candidate = {
            "wrong_name": wrong,
            "evidence_count": int(entry.get("evidence_count", 1) or 1),
        }
        if entry.get("last_seen"):
            candidate["last_seen"] = str(entry["last_seen"])
        prior = merged.get(wrong)
        if prior is None or candidate["evidence_count"] >= int(
                prior.get("evidence_count", 0) or 0):
            merged[wrong] = candidate
        store.upsert_node(
            "Column", col_uri,
            {"naming_corrections": sorted(
                merged.values(), key=lambda c: -int(
                    c.get("evidence_count", 0) or 0))},
            source="bq")
        report["applied"] += 1
    return report
