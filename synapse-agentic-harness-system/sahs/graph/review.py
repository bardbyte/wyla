"""ReviewItem + user-variant intake — the schemas land BEFORE the first
real build (E12/A5) so day-one quads are final; the queue tooling
(review_list/get/decide/stats) is Part B and lives behind a separate
trust boundary when it arrives.

A ReviewItem is a node like everything else:

    review:<fp12>  props {kind, subject, evidence[], proposal,
                          agent_recommendation?, priority,
                          status: open|decided|spawned_task,
                          decided_by?, decided_at?, verdict?, correction?}
    + edge (review:<id>, concerns, <subject>)

Five streams eventually feed it (D1–D5 tickets, census conflicts,
enricher suggestions, user variants, witness divergences); Part A emits
only `witness_divergence` plus fixture seeds.

Priority (pinned): support_effective × usage_recency_weight ×
blast_radius, where blast_radius: certified-adjacent conflict 3 ≻
ungoverned-but-used 2 ≻ cosmetic 1.

User-variant intake (the Alice/Bob case, pinned): a confirmed
on-the-fly variant is written AT CREATION as ordinary metric quads —
`witness: user_variant`, `certified_as → status:team_candidate` with
`prov.actor = <user>`, `variant_of` → the nearest certified fingerprint
— plus a ReviewItem(kind=variant). One store, one status lattice;
variants are never "memory". The resolver serves a team_candidate
variant only with disclosure, never as the meridian line.
"""

from __future__ import annotations

import hashlib
from typing import Any

from sahs.graph.quads import (
    REVIEW_KINDS,
    REVIEW_STATUSES,
    GraphDir,
    NodeRecord,
    Prov,
    Quad,
)

BLAST_RADIUS = {"certified_adjacent": 3, "ungoverned_used": 2, "cosmetic": 1}


def review_id(kind: str, subject: str, proposal: str) -> str:
    digest = hashlib.sha256(
        f"{kind}\x1f{subject}\x1f{proposal}".encode("utf-8")).hexdigest()
    return f"review:{digest[:12]}"


def review_priority(support_effective: int, usage_recency_weight: float,
                    blast_radius: int) -> float:
    """Pinned formula — the queue's ranking key (B2 consumes it)."""
    return round(max(support_effective, 1)
                 * max(usage_recency_weight, 0.0)
                 * max(blast_radius, 1), 4)


def emit_review_item(graph: GraphDir, *, kind: str, subject: str,
                     proposal: str, evidence: list[str], run_id: str,
                     source: str, witness: str,
                     support_effective: int = 1,
                     usage_recency_weight: float = 1.0,
                     blast_radius: int = 1,
                     agent_recommendation: str = "") -> str:
    """The one sanctioned writer — every stream funnels through here."""
    assert kind in REVIEW_KINDS, kind
    item = review_id(kind, subject, proposal)
    priority = review_priority(support_effective, usage_recency_weight,
                               blast_radius)
    prov = Prov(source=source, run=run_id, witness=witness,
                evidence=evidence[0] if evidence else "")
    graph.append_node(NodeRecord(id=item, props={
        "kind": kind, "subject": subject, "evidence": evidence,
        "proposal": proposal,
        "agent_recommendation": agent_recommendation,
        "priority": priority, "status": "open",
    }, prov=prov))
    graph.append_edge(Quad(s=item, r="concerns", o=subject, prov=prov))
    return item


def fold_review_items(graph: GraphDir) -> list[dict[str, Any]]:
    """Current open/decided items, priority-ranked — B2's queue reads
    exactly this fold."""
    items = []
    for node_id, record in graph.fold_nodes().items():
        if not node_id.startswith("review:"):
            continue
        props = dict(record.props)
        if props.get("status") not in REVIEW_STATUSES:
            continue
        items.append({"item_id": node_id, **props})
    return sorted(items, key=lambda x: (-x.get("priority", 0),
                                        x["item_id"]))
