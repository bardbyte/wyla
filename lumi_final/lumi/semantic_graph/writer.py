"""Dual-write entry point: JSONL audit + AGE materialized view.

This is the only place that knows about both write surfaces. Every
caller that records ontology events should go through ``writer.record()``
instead of ``OntologyStore.record()`` directly so the graph projection
happens automatically.

Contract:
  - JSONL write happens first (it's the source of truth).
  - AGE projection happens after, gated by ``LUMI_AGE_ENABLED``.
  - Returns a ``WriteReceipt`` documenting what landed where.
  - Errors in AGE never roll back JSONL — divergence is detectable
    by ``replay.verify()``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lumi.ontology_store import OntologyStore
from lumi.schemas import OntologyEvent
from lumi.semantic_graph import config as gconfig
from lumi.semantic_graph import projector

logger = logging.getLogger("lumi.semantic_graph.writer")


@dataclass
class WriteReceipt:
    """Outcome of one dual-write."""

    event_content_hash: str
    jsonl_ok: bool
    jsonl_was_duplicate: bool  # idempotent dedup hit
    age_attempted: bool
    age_ok: bool
    age_skip_reason: str | None = None


# Cached store handle — created lazily on first write
_STORE: OntologyStore | None = None


def _get_store() -> OntologyStore:
    global _STORE
    if _STORE is None:
        _STORE = OntologyStore()
    return _STORE


def record(event: OntologyEvent, *, store: OntologyStore | None = None) -> WriteReceipt:
    """Dual-write a single event.

    Args:
        event: the OntologyEvent to record. content_hash auto-set if absent.
        store: optional store override (tests inject a tmp_path store).
    """
    target_store = store or _get_store()
    receipt = WriteReceipt(
        event_content_hash="",
        jsonl_ok=False,
        jsonl_was_duplicate=False,
        age_attempted=False,
        age_ok=False,
    )

    # Step 1: JSONL audit write (source of truth).
    try:
        was_new = target_store.record(event)
        receipt.jsonl_ok = True
        receipt.jsonl_was_duplicate = not was_new
        receipt.event_content_hash = event.content_hash
    except Exception as e:  # noqa: BLE001
        logger.error("JSONL write failed for %s: %s", event.event_type, e)
        return receipt  # nothing more to do

    # Step 2: AGE projection (gated).
    if not gconfig.is_age_enabled():
        receipt.age_skip_reason = "LUMI_AGE_ENABLED not set"
        return receipt

    # Don't re-project duplicates — they'd just be no-op MERGE bumps,
    # but skipping avoids unnecessary DB round-trips.
    if receipt.jsonl_was_duplicate:
        receipt.age_skip_reason = "duplicate event (idempotent skip)"
        return receipt

    receipt.age_attempted = True
    try:
        receipt.age_ok = projector.project(event)
        if not receipt.age_ok:
            receipt.age_skip_reason = "projector returned False (check logs)"
    except Exception as e:  # noqa: BLE001
        logger.error("AGE projection raised for %s: %s", event.event_type, e)
        receipt.age_skip_reason = f"{type(e).__name__}: {e}"

    return receipt


def record_many(
    events: list[OntologyEvent], *, store: OntologyStore | None = None,
) -> list[WriteReceipt]:
    """Dual-write a batch. Returns one receipt per event."""
    return [record(ev, store=store) for ev in events]
