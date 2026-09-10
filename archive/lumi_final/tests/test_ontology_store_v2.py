"""Ontology store v2 tests — unified refresh, concurrency, plan mining."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from lumi.ontology_store import (
    OntologyStore,
    record_approval_lock,
)
from lumi.schemas import (
    EnrichmentPlan,
    OntologyEvent,
    PlanApproval,
    TableContext,
)


def _ctx(table: str = "cardmember_dim") -> TableContext:
    return TableContext(
        table_name=table, columns_referenced=[],
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=[], date_functions=[], mdm_columns=[],
        mdm_table_description=None, mdm_coverage_pct=0.0,
        existing_view_lkml=None, queries_using_this=[],
    )


# ─── Unified refresh ───────────────────────────────────────


def test_refresh_with_seed_fn_on_cold_start(tmp_path: Path):
    """Seed function fires when current.json is missing."""
    store = OntologyStore(tmp_path)
    seeded = []

    def seed_fn(s):
        seeded.append(True)
        s.record(OntologyEvent(
            event_type="entity_hint", source="llm_seed",
            table_name="t1", column_name="cm11", entity_name="cardmember",
        ))
        s.record(OntologyEvent(
            event_type="entity_hint", source="llm_seed",
            table_name="t1", column_name="cm15", entity_name="cardmember",
        ))
        return 2

    ontology = store.refresh(seed_fn=seed_fn, evidence_threshold=1)
    assert seeded == [True]
    assert {e.name for e in ontology.entities} == {"cardmember"}
    # current.json now exists
    assert store.current_path.exists()


def test_refresh_skips_seed_on_warm_start(tmp_path: Path):
    """Seed should NOT fire when current.json exists."""
    store = OntologyStore(tmp_path)
    # First run: seed fires.
    store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    ))
    store.refresh(evidence_threshold=1)

    seeded = []

    def seed_fn(s):
        seeded.append(True)
        return 0

    store.refresh(seed_fn=seed_fn, evidence_threshold=1)
    assert seeded == []  # warm start — skip seed


def test_refresh_force_seed_overrides_warm_start(tmp_path: Path):
    """force_seed=True fires the seed even if current.json exists."""
    store = OntologyStore(tmp_path)
    store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="cm11", entity_name="cardmember",
    ))
    store.refresh(evidence_threshold=1)

    seeded = []

    def seed_fn(s):
        seeded.append(True)
        return 0

    store.refresh(
        seed_fn=seed_fn, force_seed=True, evidence_threshold=1,
    )
    assert seeded == [True]


def test_refresh_infers_relationships_from_equivalences(tmp_path: Path):
    """Cross-entity equivalences become OntologyRelationships."""
    store = OntologyStore(tmp_path)
    # Build two entities.
    for col in ("cm11", "cm15"):
        store.record(OntologyEvent(
            event_type="entity_hint", source="fetch_mdm",
            table_name="t1", column_name=col, entity_name="cardmember",
        ))
    store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t1", column_name="acct_id", entity_name="account",
    ))
    store.record(OntologyEvent(
        event_type="entity_hint", source="fetch_mdm",
        table_name="t2", column_name="acct_xref", entity_name="account",
    ))
    # Equivalence between cm11 and acct_id
    store.record(OntologyEvent(
        event_type="equivalence_observed", source="parse_sqls",
        table_name="t1", column_name="cm11",
        payload={"a_table": "t1", "a_column": "cm11",
                 "b_table": "t1", "b_column": "acct_id"},
    ))
    ontology = store.refresh(evidence_threshold=1)
    # A relationship between cardmember and account should appear.
    pairs = {(r.from_entity, r.to_entity) for r in ontology.relationships}
    pairs |= {(b, a) for a, b in pairs}
    assert ("cardmember", "account") in pairs or ("account", "cardmember") in pairs


# ─── Concurrency safety ────────────────────────────────────


def test_concurrent_writes_dont_corrupt_candidates(tmp_path: Path):
    """20 threads each record 10 distinct events → all 200 land safely."""
    store = OntologyStore(tmp_path)

    def worker(thread_id: int):
        for col_id in range(10):
            store.record(OntologyEvent(
                event_type="entity_hint", source="fetch_mdm",
                table_name=f"table_{thread_id}",
                column_name=f"col_{col_id}",
                entity_name="cardmember",
            ))

    with ThreadPoolExecutor(max_workers=20) as ex:
        list(ex.map(worker, range(20)))

    c = store.candidates()
    # 20 threads × 10 distinct cols × 1 entity = 200 events
    assert c["entities"]["cardmember"]["evidence_count"] == 200


# ─── Approval mining ───────────────────────────────────────


def test_approval_mines_synonym_from_renamed_dim(tmp_path: Path):
    """Plan dim where name != source_column → curated_synonym event."""
    store = OntologyStore(tmp_path)
    ctx = _ctx()
    approval = PlanApproval(
        table_name="cardmember_dim", approved=True, approver="human",
    )
    plan = EnrichmentPlan(
        table_name="cardmember_dim",
        proposed_dimensions=[{
            "name": "card_member_id",  # renamed
            "source_column": "cm11",
            "type": "string",
        }],
        proposed_measures=[],
        reasoning="cardmember dim",
    )
    n = record_approval_lock(store, approval, ctx, plan=plan)
    assert n >= 2  # vocabulary_lock + curated_synonym
    c = store.candidates()
    assert "cardmember" in (c.get("synonyms") or {})
    assert "card_member_id" in c["synonyms"]["cardmember"]


def test_record_cardinalities_from_fingerprints(tmp_path: Path):
    """Hook 5 — corpus-wide cardinality + path inference flows into store."""
    from lumi.ontology_store import record_cardinalities_from_fingerprints
    from lumi.sql_to_context import parse_sqls

    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ] * 4)
    n = record_cardinalities_from_fingerprints(store, fps)
    assert n >= 2  # at least 1 cardinality + 1 path
    c = store.candidates()
    assert c["cardinalities"], "cardinalities tally must be populated"
    assert c["join_paths"], "join_paths tally must be populated"
    # The cardmember↔transaction pair should be one_to_many
    bucket = next(iter(c["cardinalities"].values()))
    assert "one_to_many" in bucket["votes"]


def test_relationships_inferred_with_cardinality_from_corpus(tmp_path: Path):
    """promote_candidates → _infer_relationships uses cardinality candidates."""
    from lumi.ontology_store import (
        record_cardinalities_from_fingerprints,
        record_equivalences_from_fingerprints,
    )
    from lumi.sql_to_context import parse_sqls

    store = OntologyStore(tmp_path)
    # Seed two entities at the columns used in the join
    for col in ("cm11",):
        store.record(OntologyEvent(
            event_type="entity_hint", source="fetch_mdm",
            table_name="cardmember", column_name=col, entity_name="cardmember",
        ))
    for col in ("cm_id",):
        store.record(OntologyEvent(
            event_type="entity_hint", source="fetch_mdm",
            table_name="transaction", column_name=col, entity_name="transaction",
        ))
    # Record equivalences + cardinalities from real queries
    fps = parse_sqls([
        "SELECT cm.cm11, SUM(t.amount) FROM cardmember cm "
        "JOIN transaction t ON cm.cm11 = t.cm_id GROUP BY cm.cm11",
    ] * 4)
    record_equivalences_from_fingerprints(store, fps)
    record_cardinalities_from_fingerprints(store, fps)

    ontology = store.refresh(evidence_threshold=1)
    # Relationship should be present with cardinality (NOT unknown)
    rel = next(
        (r for r in ontology.relationships
         if {r.from_entity, r.to_entity} == {"cardmember", "transaction"}),
        None,
    )
    assert rel is not None
    assert rel.cardinality in {"one_to_many", "many_to_one"}


def test_approval_mines_curated_pk(tmp_path: Path):
    """primary_key=yes dim → curated_pk event."""
    store = OntologyStore(tmp_path)
    ctx = _ctx()
    approval = PlanApproval(
        table_name="cardmember_dim", approved=True, approver="human",
    )
    plan = EnrichmentPlan(
        table_name="cardmember_dim",
        proposed_dimensions=[{
            "name": "cm11", "source_column": "cm11",
            "type": "string", "primary_key": True,
        }],
        proposed_measures=[],
        reasoning="cardmember dim",
    )
    record_approval_lock(store, approval, ctx, plan=plan)
    c = store.candidates()
    assert c.get("primary_keys", {}).get("cardmember_dim", {}).get("cm11") == 1
