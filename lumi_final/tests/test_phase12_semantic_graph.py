"""Phase 1+2 — semantic graph scaffolding contracts.

These tests don't require Apache AGE — they verify:
- config registry is correct
- projector dispatch is exhaustive vs schemas.OntologyEventType
- writer falls back to JSONL-only when AGE disabled
- schema module imports + bootstrap shape are correct

End-to-end AGE round-trip tests live in the probe script (run on the
work laptop where Postgres+AGE is available).
"""

from __future__ import annotations

import os

from lumi.ontology_store import OntologyStore
from lumi.schemas import OntologyEvent, OntologyEventType
from lumi.semantic_graph import config as gconfig
from lumi.semantic_graph import projector, writer


def test_config_node_and_edge_label_counts():
    """Spec: 16 node labels (12 semantic + Cohort + 3 operational),
    16 edge labels."""
    assert len(gconfig.NODE_LABELS) == 16
    assert len(gconfig.EDGE_LABELS) == 16


def test_config_node_labels_include_required():
    required = {
        "Table", "Column", "Entity", "Metric", "Filter",
        "FilterValue", "TimeGrain", "QuestionPattern", "Explore", "View",
        "Synonym", "Threshold", "Cohort", "Source", "Event", "Approval",
    }
    assert set(gconfig.NODE_LABELS) == required


def test_config_edge_labels_include_required():
    required = {
        "CONTAINS", "IDENTIFIES", "EQUIVALENT_TO", "RELATES_TO",
        "COMPUTED_FROM", "OBSERVED_AT_GRAIN", "COMPANION_OF",
        "HAS_SYNONYM", "REQUIRES_FILTER", "RENDERED_AS_VIEW",
        "RENDERED_AS_EXPLORE", "ANSWERS", "JOIN_PATH",
        "ASSERTS", "LOCKS", "DEPRECATES",
    }
    assert set(gconfig.EDGE_LABELS) == required


def test_source_weights_present_for_all_canonical_sources():
    required_sources = {
        "human_approval", "mdm", "baseline_lookml", "corpus_sql",
        "llm_inferred", "bq_probe_confirm", "bq_probe_contradict",
        "compilation_conformance_confirm", "compilation_conformance_contradict",
    }
    assert required_sources <= set(gconfig.SOURCE_WEIGHTS)


def test_promotion_thresholds_present_for_node_types():
    # Every semantic node type with a non-trivial promotion gate.
    expected_keys = {
        "Table", "Column", "Entity", "Metric", "Filter", "FilterValue",
        "TimeGrain", "QuestionPattern", "Synonym", "Threshold", "Cohort",
    }
    assert expected_keys <= set(gconfig.PROMOTION_THRESHOLDS)


def test_decay_windows_present_for_node_types():
    expected_keys = {
        "Table", "Column", "Entity", "Metric", "Filter", "FilterValue",
        "TimeGrain", "QuestionPattern", "Synonym", "Threshold", "Cohort",
    }
    assert expected_keys <= set(gconfig.DECAY_WINDOWS_DAYS)


def test_confidence_levels_ordered():
    """Higher index = higher confidence."""
    assert gconfig.confidence_rank("deprecated") == 0
    assert gconfig.confidence_rank("guessed") == 1
    assert gconfig.confidence_rank("inferred") == 2
    assert gconfig.confidence_rank("grounded") == 3
    assert gconfig.confidence_rank("human_asserted") == 4
    assert gconfig.confidence_rank("bogus_label") == -1


def test_age_enabled_default_off():
    """Without env var, AGE writes no-op."""
    # Ensure the env var is unset for this test (defensive)
    prior = os.environ.pop("LUMI_AGE_ENABLED", None)
    try:
        assert gconfig.is_age_enabled() is False
    finally:
        if prior is not None:
            os.environ["LUMI_AGE_ENABLED"] = prior


def test_age_enabled_when_env_set(monkeypatch):
    monkeypatch.setenv("LUMI_AGE_ENABLED", "1")
    assert gconfig.is_age_enabled() is True


# ─── Projector exhaustiveness contract ──────────────────────


def test_projector_covers_every_ontology_event_type_in_use():
    """Projector dispatch must handle every event_type the codebase emits.

    OntologyEventType in schemas.py is the source of truth for the
    universe of event types. Every type that's actively emitted should
    have a projector. (Operational types like 'audit_only' could be
    excluded if added later.)
    """
    declared = set(OntologyEventType.__args__)  # Literal members
    covered = set(projector.covered_event_types())
    missing = declared - covered
    # If any are intentionally NOT projected, list them here:
    intentionally_unprojected: set[str] = set()
    assert missing - intentionally_unprojected == set(), (
        f"Projector missing dispatch for event types: {missing}"
    )


# ─── Writer dual-write fallback ─────────────────────────────


def test_writer_falls_back_to_jsonl_only_when_age_disabled(tmp_path, monkeypatch):
    """Without AGE enabled, writer.record() succeeds via JSONL alone."""
    monkeypatch.delenv("LUMI_AGE_ENABLED", raising=False)
    store = OntologyStore(tmp_path)
    ev = OntologyEvent(
        event_type="entity_hint",
        source="fetch_mdm",
        table_name="cardmember_dim",
        column_name="cm11",
        entity_name="cardmember",
    )
    receipt = writer.record(ev, store=store)
    assert receipt.jsonl_ok is True
    assert receipt.age_attempted is False
    assert "LUMI_AGE_ENABLED" in (receipt.age_skip_reason or "")
    # And the event landed in JSONL
    cands = store.candidates()
    assert "cardmember" in cands["entities"]


def test_writer_idempotent_skip_on_duplicate_event(tmp_path, monkeypatch):
    """A second identical event records to JSONL as duplicate; AGE still skipped."""
    monkeypatch.delenv("LUMI_AGE_ENABLED", raising=False)
    store = OntologyStore(tmp_path)
    ev1 = OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    )
    receipt1 = writer.record(ev1, store=store)
    assert receipt1.jsonl_ok is True
    assert receipt1.jsonl_was_duplicate is False

    ev2 = OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    )
    receipt2 = writer.record(ev2, store=store)
    assert receipt2.jsonl_ok is True
    assert receipt2.jsonl_was_duplicate is True


def test_writer_batch_returns_one_receipt_per_event(tmp_path, monkeypatch):
    monkeypatch.delenv("LUMI_AGE_ENABLED", raising=False)
    store = OntologyStore(tmp_path)
    events = [
        OntologyEvent(
            event_type="entity_hint", source="fetch_mdm",
            table_name="t1", column_name=f"col_{i}", entity_name="cardmember",
        )
        for i in range(5)
    ]
    receipts = writer.record_many(events, store=store)
    assert len(receipts) == 5
    assert all(r.jsonl_ok for r in receipts)


# ─── Schema module surface (no DB connection) ───────────────


def test_schema_module_importable_without_postgres():
    """Importing the schema module shouldn't require psycopg/AGE to be
    installed. The lazy import lives inside _connect()."""
    from lumi.semantic_graph import schema as _schema
    # Module-level functions exist
    assert callable(_schema.bootstrap)
    assert callable(_schema.verify)
    assert callable(_schema.create_graph)
    assert callable(_schema.create_labels)
    assert callable(_schema.create_indexes)
