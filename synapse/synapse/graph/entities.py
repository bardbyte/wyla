"""The entity layer — business things, not strings.

The graph's L3: real business entities (Cardmember, Account, Card
Product …) materialized as Entity nodes with Column —IDENTIFIES→ Entity
edges, so the agent can reason "these four tables all speak about
Accounts" instead of string-matching column names.

Flow (zero LLM calls — a pure reduction over work already done):

    enrichment_memory.json ──propose──► entity_review.yaml   (steward
        (candidate_entity_name             flips approve: true)
         observations, captured
         during --enrich)
    entity_review.yaml ──apply──► Entity nodes + IDENTIFIES edges,
        source="human_approval" (weight 10 → tier human_asserted 0.99)
        + approvals persisted so every future compile re-ingests them
        as witness #6.

Grounding discipline matches the enrichment gate: an IDENTIFIES edge is
only written when the column node actually exists in the graph; ghost
references are counted and reported, never minted.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from synapse.graph.store import GraphStore, canonical_uri


def load_bundles_from_memory(memory_path: Path) -> dict[str, Any]:
    """enrichment_memory.json → {table_name: EnrichmentBundle}.

    The memory file is the steward-audit dump the pipeline writes during
    --enrich; re-reducing it costs nothing."""
    from synapse.enrichment.schemas import EnrichmentBundle

    raw = json.loads(Path(memory_path).read_text(encoding="utf-8"))
    bundles: dict[str, Any] = {}
    for table_name, blob in raw.items():
        try:
            bundles[table_name] = EnrichmentBundle.model_validate(blob)
        except Exception:
            continue  # a malformed table blob must not sink the reduction
    return bundles


def write_review_yaml(
    proposals: list[Any], path: Path, *, meta: dict[str, Any] | None = None,
) -> Path:
    """EntityProposals → a human-editable review file.

    One file, two jobs: readable (comments, evidence inline) and
    parseable (flip ``approve: true`` and run apply). Everything
    defaults to NOT approved — silence never mints an entity."""
    doc: dict[str, Any] = {
        "meta": {
            "what": "Entity review — flip `approve: true` for real "
                    "business entities; leave false to reject.",
            "applied_as": "source=human_approval → tier human_asserted",
            **(meta or {}),
        },
        "proposals": [
            {
                "name": p.proposed_name,
                "approve": False,
                "description": "",
                "identified_by_columns": list(p.identified_by_columns),
                "tables": list(p.materialized_in_tables),
                "aggregate_confidence": p.aggregate_self_confidence,
                "n_observations": p.n_supporting_observations,
                "conflicts": list(p.conflict_signals),
                "evidence": list(p.evidence_packet_refs),
            }
            for p in proposals
        ],
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=88),
        encoding="utf-8")
    return path


def read_approved(review_path: Path) -> list[dict[str, Any]]:
    """Review/approvals YAML → the approved entries only."""
    doc = yaml.safe_load(Path(review_path).read_text(encoding="utf-8")) or {}
    entries = doc.get("proposals") or doc.get("entities") or []
    approved = []
    for e in entries:
        if not isinstance(e, dict) or not e.get("name"):
            continue
        # an approvals file (already-filtered) may omit the flag entirely
        if e.get("approve", True) in (True, "true", "yes", 1):
            approved.append(e)
    return approved


def apply_entities(
    store: GraphStore, approved: list[dict[str, Any]],
) -> dict[str, Any]:
    """Approved entities → Entity nodes + Column—IDENTIFIES→Entity edges.

    source='human_approval' puts every fact at the top of the ladder
    (human_asserted, 0.99). Ghost column references are skipped and
    counted — steward approval is authority over MEANING, not license
    to mint schema."""
    report: dict[str, Any] = {
        "entities_added": 0, "edges_added": 0,
        "edges_skipped_missing_column": 0, "per_entity": {},
    }
    for entry in approved:
        name = str(entry["name"]).strip()
        e_uri = canonical_uri("entity", name)
        store.upsert_node(
            "Entity", e_uri,
            properties={
                "entity_name": name,
                "description": str(entry.get("description") or ""),
                "identified_by_columns": list(
                    entry.get("identified_by_columns") or []),
                "materialized_in_tables": list(entry.get("tables") or []),
                "entry_type": "Steward_Approved",
            },
            source="human_approval",
        )
        report["entities_added"] += 1
        added = skipped = 0
        for ref in entry.get("evidence") or []:
            # evidence refs are "table::column" pairs from the reducer
            if "::" not in str(ref):
                continue
            table, column = str(ref).split("::", 1)
            c_uri = canonical_uri("column", table, column)
            if c_uri not in store.nodes:
                skipped += 1
                continue
            store.upsert_edge(
                "IDENTIFIES", c_uri, e_uri,
                properties={"entity_name": name},
                source="human_approval",
            )
            added += 1
        report["edges_added"] += added
        report["edges_skipped_missing_column"] += skipped
        report["per_entity"][name] = {"edges": added, "skipped": skipped}
    return report


def ingest_entities_file(store: GraphStore, path: Path) -> dict[str, Any]:
    """Builder hook — witness #6. No-op when the file is absent, so
    every compile stays runnable without approvals."""
    path = Path(path)
    if not path.exists():
        return {"entities_added": 0, "edges_added": 0,
                "edges_skipped_missing_column": 0, "per_entity": {}}
    return apply_entities(store, read_approved(path))
