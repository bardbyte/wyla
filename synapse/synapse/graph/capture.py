"""capture_knowledge — a trusted human's assertion, at the top tier (P5).

Writes a definition/correction as ``source='human_approval'`` → tier
``human_asserted`` (0.99), credited to the actor. Distinct from on-demand
``explain_column`` (agent-authored, capped at ``inferred``): a human's
assertion is authoritative and outranks everything.

Policy A (MVP): live immediately. It also persists to the overlay so it
survives restarts and is the steward-review trail for the future policy B.
"""

from __future__ import annotations

from typing import Any

from synapse.graph.store import GraphStore, canonical_uri


def resolve_subject_uri(
    subject_type: str, subject_ref: str,
) -> tuple[str | None, str | None]:
    """(canonical_uri, node_type) for a subject, or (None, None) if it can't
    be resolved. Columns take ``table.column``."""
    st = (subject_type or "").strip().lower()
    ref = (subject_ref or "").strip()
    if not ref:
        return None, None
    if st == "table":
        return canonical_uri("table", ref), "Table"
    if st == "column":
        if "." not in ref:
            return None, None            # need table.column
        tbl, col = ref.rsplit(".", 1)
        return canonical_uri("column", tbl, col), "Column"
    if st == "entity":
        return canonical_uri("entity", ref), "Entity"
    return None, None


def capture_assertion(
    store: GraphStore, *, subject_type: str, subject_ref: str,
    statement: str, actor: str = "analyst", overlay: Any = None,
    at: str | None = None,
) -> dict[str, Any]:
    """Record a human assertion about a subject as authoritative
    (``human_asserted``), credited to ``actor``. Persists to ``overlay`` when
    given. Returns the recorded assertion + the new tier."""
    if not (statement or "").strip():
        return {"status": "error", "reason": "the assertion text is empty"}
    uri, _ntype = resolve_subject_uri(subject_type, subject_ref)
    if uri is None:
        return {"status": "error",
                "reason": f"could not resolve {subject_type} '{subject_ref}' "
                          "(for a column use 'table.column')"}
    node = store.get(uri)
    if node is None:
        return {"status": "error",
                "reason": f"{subject_type} '{subject_ref}' is not in the graph"}
    # human_approval (weight 10) → human_asserted, and outranks MDM's value
    store.upsert_node(
        node.node_type, uri,
        {"description": statement.strip(), "asserted_by": actor},
        source="human_approval")
    node = store.get(uri)
    if overlay is not None and hasattr(overlay, "record_assertion"):
        overlay.record_assertion(
            subject_type=subject_type, subject_ref=subject_ref,
            statement=statement.strip(), actor=actor,
            tier=node.provenance.confidence_tier, at=at)
    return {
        "status": "ok",
        "subject_type": subject_type,
        "subject_ref": subject_ref,
        "statement": statement.strip(),
        "asserted_by": actor,
        "tier": node.provenance.confidence_tier,      # human_asserted
        "note": f"Recorded as authoritative and credited to {actor}. "
                "This now outranks the machine's guess for everyone.",
    }
