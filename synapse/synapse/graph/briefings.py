"""Per-table steward briefings — the one-table build's quality secret,
made a first-class witness.

The original single-table graph owed its precision to a hand-written
capsule at the top of its enrichment skill: the grain, the VIEW-not-
table gotcha, the account-vs-customer key distinction, known analyst
mistakes. Knowledge only a human could write, injected where the LLM
and the agent would both see it.

This module generalizes that: one markdown file per table under
``semantic-graph/config/briefings/<table>.md``. On compile (or via
``scripts/briefings.py apply`` against an existing snapshot) each file
becomes a ``briefing`` fact on its Table node with source
``human_approval`` — steward-authored context, the strongest witness.
From there it rides automatically into the enrichment context and into
``inspect_table`` for the agent.

Grounding discipline applies here too: a briefing whose filename
matches no table in the graph is SKIPPED and reported, never minted.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from synapse.graph.store import GraphStore, normalize_table_name

# A briefing is context, not a document store — cap what one file may
# inject into every enrichment call and inspect payload.
MAX_BRIEFING_CHARS = 6_000


def ingest_briefings_dir(store: GraphStore,
                         briefings_dir: Path) -> dict[str, Any]:
    """Fold every ``<table>.md`` under ``briefings_dir`` into its Table
    node. No-op (with an empty report) when the directory is absent."""
    report: dict[str, Any] = {"applied": [], "skipped_missing_table": [],
                              "truncated": []}
    if not briefings_dir or not briefings_dir.exists():
        return report

    by_norm = {}
    for node in store.nodes_by_type("Table"):
        name = str(node.properties.get("table_name", ""))
        if name:
            by_norm[normalize_table_name(name)] = node

    for path in sorted(briefings_dir.glob("*.md")):
        if path.stem.lower() == "readme":
            continue                          # the template, not a table
        want = normalize_table_name(path.stem)
        node = by_norm.get(want)
        if node is None:
            report["skipped_missing_table"].append(path.stem)
            continue
        text = path.read_text(encoding="utf-8").strip()
        if len(text) > MAX_BRIEFING_CHARS:
            text = text[:MAX_BRIEFING_CHARS] + "\n\n[briefing truncated]"
            report["truncated"].append(path.stem)
        store.upsert_node(
            "Table", node.canonical_uri,
            {"briefing": text}, source="human_approval")
        report["applied"].append(
            str(node.properties.get("table_name", path.stem)))
    return report
