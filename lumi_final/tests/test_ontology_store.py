"""Ontology store tests — event sourcing, candidates, promotion, hooks."""

from __future__ import annotations

from pathlib import Path

from lumi.ontology_store import (
    OntologyStore,
    record_approval_lock,
    record_curated_synonyms_from_baseline,
    record_entity_hints_from_mdm,
    record_equivalences_from_fingerprints,
)
from lumi.schemas import OntologyEvent, PlanApproval, TableContext
from lumi.sql_to_context import parse_sqls


def _ctx(**kw) -> TableContext:
    return TableContext(
        table_name=kw.get("table_name", "t1"),
        columns_referenced=kw.get("columns_referenced", []),
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=[], date_functions=[],
        mdm_columns=kw.get("mdm_columns", []),
        mdm_table_description=kw.get("mdm_table_description"),
        mdm_coverage_pct=kw.get("mdm_coverage_pct", 0.0),
        mdm_dataset_details=kw.get("mdm_dataset_details", {}),
        existing_view_lkml=None,
        baseline_primary_key_column=kw.get("baseline_primary_key_column"),
        baseline_sql_aliases=kw.get("baseline_sql_aliases", {}),
        queries_using_this=kw.get("queries_using_this", []),
    )


def test_record_event_appends_jsonl(tmp_path: Path):
    store = OntologyStore(tmp_path)
    store.record(OntologyEvent(
        event_type="entity_hint",
        source="fetch_mdm",
        table_name="cardmember_dim",
        column_name="cm11",
        entity_name="cardmember",
        confidence=0.7,
        evidence="naming pattern",
    ))
    files = list(store.events_dir.glob("*.jsonl"))
    assert len(files) == 1
    content = files[0].read_text()
    assert "cardmember" in content


def test_candidates_tally_entity_hint(tmp_path: Path):
    store = OntologyStore(tmp_path)
    # Three DIFFERENT events (different columns) — dedup leaves each
    # untouched. Identical re-emissions would dedupe to 1 (idempotency).
    for col in ("cm11", "cm15", "cm_id"):
        store.record(OntologyEvent(
            event_type="entity_hint",
            source="fetch_mdm",
            table_name="cardmember_dim",
            column_name=col,
            entity_name="cardmember",
        ))
    c = store.candidates()
    assert c["entities"]["cardmember"]["evidence_count"] == 3
    assert "cm11" in c["entities"]["cardmember"]["grain_columns"]["cardmember_dim"]


def test_idempotent_record_dedupes_identical_events(tmp_path: Path):
    """Re-emitting the same event from the same source is a no-op —
    that's how we avoid inflating evidence_count on every plan re-run."""
    store = OntologyStore(tmp_path)
    ev = OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    )
    assert store.record(ev) is True
    # Same fact, second emission — must dedupe.
    assert store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    )) is False
    c = store.candidates()
    assert c["entities"]["cardmember"]["evidence_count"] == 1


def test_idempotency_persists_across_store_instances(tmp_path: Path):
    """A second OntologyStore on the same root must remember the
    hashes — otherwise re-running the pipeline doubles every count."""
    store_a = OntologyStore(tmp_path)
    ev = OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    )
    assert store_a.record(ev) is True

    # Simulate fresh process — new store instance reading same root.
    store_b = OntologyStore(tmp_path)
    ev2 = OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    )
    assert store_b.record(ev2) is False
    c = store_b.candidates()
    assert c["entities"]["cardmember"]["evidence_count"] == 1


def test_candidates_tally_equivalence_pairs_dedup(tmp_path: Path):
    store = OntologyStore(tmp_path)
    # Record both directions — should tally to ONE bucket.
    store.record(OntologyEvent(
        event_type="equivalence_observed", source="parse_sqls",
        table_name="t1", column_name="cm11",
        payload={"a_table": "t1", "a_column": "cm11",
                 "b_table": "t2", "b_column": "cust_id"},
    ))
    store.record(OntologyEvent(
        event_type="equivalence_observed", source="parse_sqls",
        table_name="t2", column_name="cust_id",
        payload={"a_table": "t2", "a_column": "cust_id",
                 "b_table": "t1", "b_column": "cm11"},
    ))
    c = store.candidates()
    assert len(c["equivalences"]) == 1
    assert c["equivalences"][0]["count"] == 2


def test_promote_candidates_with_threshold(tmp_path: Path):
    store = OntologyStore(tmp_path)
    # Three DIFFERENT events for cardmember → evidence_count = 3 → promotes
    for col in ("cm11", "cm15", "cm_id"):
        store.record(OntologyEvent(
            event_type="entity_hint", source="fetch_mdm",
            table_name="t1", column_name=col, entity_name="cardmember",
        ))
    # One event for merchant → evidence_count = 1 → does NOT promote at threshold=2
    store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t2", column_name="merch_id", entity_name="merchant",
    ))
    ontology = store.promote_candidates(evidence_threshold=2)
    names = {e.name for e in ontology.entities}
    assert "cardmember" in names
    assert "merchant" not in names


def test_snapshot_writes_versioned_file(tmp_path: Path):
    store = OntologyStore(tmp_path)
    store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    ))
    store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t2", column_name="cm15", entity_name="cardmember",
    ))
    ontology = store.promote_candidates(evidence_threshold=1)
    p1 = store.snapshot(ontology, reason="first")
    p2 = store.snapshot(ontology, reason="second")
    assert p1.name == "v0001.json"
    assert p2.name == "v0002.json"
    assert p1.exists() and p2.exists()


def test_save_and_load_current(tmp_path: Path):
    store = OntologyStore(tmp_path)
    store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    ))
    ontology = store.promote_candidates(evidence_threshold=1)
    store.save_current(ontology)
    loaded = store.current()
    assert loaded is not None
    assert {e.name for e in loaded.entities} == {"cardmember"}


# ─── Hooks ──────────────────────────────────────────────────


def test_hook_equivalences_from_fingerprints(tmp_path: Path):
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT * FROM cardmember a JOIN customer b ON a.cm11 = b.cust_id",
    ])
    n = record_equivalences_from_fingerprints(store, fps)
    assert n >= 1
    c = store.candidates()
    assert c["equivalences"], "should have at least one equivalence"


def test_hook_entity_hints_from_mdm(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cardmember_dim",
        mdm_columns=[
            {"name": "cm11", "business_name": "Cardmember ID", "type": "STRING"},
            {"name": "acct_id", "business_name": "Account ID", "type": "STRING"},
        ],
        mdm_dataset_details={"data_category": "Cardmember Reference"},
    )
    n = record_entity_hints_from_mdm(store, ctx)
    assert n > 0
    c = store.candidates()
    # cardmember entity should have evidence
    assert "cardmember" in c["entities"]


def test_hook_curated_synonyms_from_baseline(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cardmember_dim",
        baseline_sql_aliases={"customer_segment": "bus_seg"},
        baseline_primary_key_column="cm_id",
    )
    n = record_curated_synonyms_from_baseline(store, ctx)
    # one synonym + one PK = 2 events
    assert n == 2
    c = store.candidates()
    assert c["primary_keys"]["cardmember_dim"]["cm_id"] == 1


def test_hook_approval_lock_only_for_approved(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(table_name="cardmember_dim")

    rejected = PlanApproval(
        table_name="cardmember_dim", approved=False, approver="human",
    )
    assert record_approval_lock(store, rejected, ctx) == 0

    approved = PlanApproval(
        table_name="cardmember_dim", approved=True, approver="human",
    )
    n = record_approval_lock(store, approved, ctx)
    assert n == 1
    # vocabulary_lock should boost evidence count by 5
    c = store.candidates()
    assert c["entities"]["cardmember"]["evidence_count"] >= 5
