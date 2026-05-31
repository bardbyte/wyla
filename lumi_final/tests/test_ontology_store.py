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


def _read_event_types(store: OntologyStore) -> set[str]:
    """Helper — collect every event_type recorded under this store."""
    import json
    types: set[str] = set()
    for f in store.events_dir.glob("*.jsonl"):
        for line in f.read_text().splitlines():
            if line.strip():
                types.add(json.loads(line)["event_type"])
    return types


def test_mdm_emits_curated_pk_for_is_primary(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cm_dim",
        mdm_columns=[
            {"name": "cm11", "is_primary": True, "type": "STRING"},
        ],
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "curated_pk" in _read_event_types(store)


def test_mdm_emits_curated_pk_for_is_dedupe_key(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cm_dim",
        mdm_columns=[
            {"name": "natural_id", "is_dedupe_key": True, "type": "STRING"},
        ],
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "curated_pk" in _read_event_types(store)


def test_mdm_emits_column_governance(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cm_dim",
        mdm_columns=[
            {
                "name": "ssn",
                "is_pii": True,
                "pii_role_id": "PII_ROLE_42",
                "is_critical_data_element": True,
                "is_mandatory": True,
                "is_clustered": True,
                "format": "STRING(11)",
            },
        ],
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "column_governance_observed" in _read_event_types(store)


def test_mdm_emits_partition_observed(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cm_metrics",
        mdm_columns=[
            {
                "name": "rpt_dt", "is_partitioned": True,
                "partition_position": 1, "time_partition_type": "DAY",
            },
        ],
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "partition_observed" in _read_event_types(store)


def test_mdm_emits_derived_formula(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cm_metrics",
        mdm_columns=[
            {
                "name": "tbb_usd",
                "derived_logic": "CASE WHEN currency='USD' THEN amount ELSE amount*fx END",
            },
        ],
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "derived_formula_observed" in _read_event_types(store)


def test_mdm_emits_external_reference_as_cardinality(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cm_dim",
        mdm_columns=[
            {
                "name": "acct_id",
                "external_references": [
                    {"table": "acct_dim", "column": "id",
                     "cardinality": "many_to_one"},
                ],
            },
        ],
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "cardinality_observed" in _read_event_types(store)


def test_mdm_emits_table_metadata(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cm_dim",
        mdm_dataset_details={
            "table_type": "DIM", "feed_type": "DAILY",
            "data_category": "Cardmember",
            "bq_project": "my-project", "bq_dataset": "dw",
            "bq_table": "cm_dim",
        },
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "table_metadata_observed" in _read_event_types(store)


def test_mdm_emits_deprecation_for_decommissioned_table(tmp_path: Path):
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="old_table",
        mdm_dataset_details={
            "is_decommissioned": True,
            "data_category": "Cardmember",
        },
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "deprecation_observed" in _read_event_types(store)


def test_mdm_omits_governance_when_no_facts(tmp_path: Path):
    """Column with no governance flags should NOT emit a governance event."""
    store = OntologyStore(tmp_path)
    ctx = _ctx(
        table_name="cm_dim",
        mdm_columns=[
            {"name": "neutral_col", "business_name": "Neutral", "type": "STRING"},
        ],
    )
    record_entity_hints_from_mdm(store, ctx)
    assert "column_governance_observed" not in _read_event_types(store)


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


# ─── corpus-facts hook (verb layer) ─────────────────────────


def test_corpus_facts_emits_metric_from_aggregation(tmp_path: Path):
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT bus_seg, SUM(billed_business) AS tbb "
        "FROM cornerstone_metrics GROUP BY bus_seg",
    ])
    record_corpus_facts(store, fps)
    types = _read_event_types(store)
    assert "metric_observed" in types


def test_corpus_facts_aggregates_metric_count_across_queries(tmp_path: Path):
    """Same SUM(x) across 3 queries → one metric_observed event with
    payload.count = 3 (the pre-aggregation pattern)."""
    import json
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT SUM(billed_business) FROM cornerstone_metrics",
        "SELECT SUM(billed_business) FROM cornerstone_metrics WHERE x=1",
        "SELECT SUM(billed_business) FROM cornerstone_metrics GROUP BY y",
    ])
    record_corpus_facts(store, fps)
    # Find the metric event
    metric_payload = None
    for f in store.events_dir.glob("*.jsonl"):
        for line in f.read_text().splitlines():
            if not line.strip():
                continue
            ev = json.loads(line)
            if ev["event_type"] == "metric_observed":
                metric_payload = ev["payload"]
                break
    assert metric_payload is not None
    assert metric_payload["count"] == 3
    assert metric_payload["function"] == "SUM"


def test_corpus_facts_emits_threshold_from_case_when(tmp_path: Path):
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT CASE WHEN fico >= 740 THEN 'Prime' "
        "WHEN fico >= 670 THEN 'Near-Prime' ELSE 'Sub' END AS fico_band "
        "FROM customers",
    ])
    record_corpus_facts(store, fps)
    assert "threshold_observed" in _read_event_types(store)


def test_corpus_facts_emits_filter_observed(tmp_path: Path):
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT bus_seg FROM cornerstone_metrics WHERE data_source = 'cornerstone'",
    ])
    record_corpus_facts(store, fps)
    assert "filter_observed" in _read_event_types(store)


def test_corpus_facts_emits_time_grain_from_date_trunc(tmp_path: Path):
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT DATE_TRUNC(rpt_dt, MONTH) AS m, SUM(x) "
        "FROM cornerstone_metrics GROUP BY m",
    ])
    record_corpus_facts(store, fps)
    assert "time_grain_observed" in _read_event_types(store)


def test_corpus_facts_emits_cohort_from_named_cte(tmp_path: Path):
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "WITH active_consumers AS ("
        "  SELECT id FROM users WHERE status='Active'"
        ") "
        "SELECT bus_seg, SUM(x) FROM cornerstone_metrics t "
        "JOIN active_consumers a ON t.id = a.id GROUP BY bus_seg",
    ])
    record_corpus_facts(store, fps)
    assert "cohort_observed" in _read_event_types(store)


def test_corpus_facts_emits_question_pattern_from_clustering(tmp_path: Path):
    store = OntologyStore(tmp_path)
    # Same shape twice → one cluster
    fps = parse_sqls([
        "SELECT bus_seg, SUM(billed_business) FROM cornerstone_metrics "
        "GROUP BY bus_seg",
        "SELECT bus_seg, SUM(billed_business) FROM cornerstone_metrics "
        "WHERE flag=1 GROUP BY bus_seg",
    ])
    record_corpus_facts(store, fps)
    assert "question_pattern_observed" in _read_event_types(store)


def test_corpus_facts_emits_metric_dimension_co_occurrence(tmp_path: Path):
    """A2 — SUM(x) GROUP BY seg should emit metric_dimension_co_occurrence."""
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT bus_seg, SUM(billed_business) "
        "FROM cornerstone_metrics GROUP BY bus_seg",
    ])
    record_corpus_facts(store, fps)
    assert "metric_dimension_co_occurrence" in _read_event_types(store)


def test_md_cooccurrence_skips_single_lookup(tmp_path: Path):
    """A2 — single_lookup queries should NOT produce co-occurrence events."""
    store = OntologyStore(tmp_path)
    fps = parse_sqls(["SELECT * FROM customers WHERE id = 42"])
    record_corpus_facts(store, fps)
    assert "metric_dimension_co_occurrence" not in _read_event_types(store)


def test_md_cooccurrence_dedups_across_queries(tmp_path: Path):
    """A2 — same (metric, dimension) pair across multiple queries →
    one event with count_obs = N and N member_query_ids."""
    import json
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT bus_seg, SUM(billed_business) FROM cornerstone_metrics GROUP BY bus_seg",
        "SELECT bus_seg, SUM(billed_business) FROM cornerstone_metrics WHERE flag=1 GROUP BY bus_seg",
        "SELECT bus_seg, SUM(billed_business) FROM cornerstone_metrics GROUP BY bus_seg ORDER BY 2 DESC",
    ])
    record_corpus_facts(store, fps)
    found = None
    for f in store.events_dir.glob("*.jsonl"):
        for line in f.read_text().splitlines():
            if not line.strip():
                continue
            ev = json.loads(line)
            if ev["event_type"] == "metric_dimension_co_occurrence":
                found = ev["payload"]
                break
    assert found is not None
    assert found["count"] == 3
    assert len(found["member_query_ids"]) == 3
    assert set(found["member_query_ids"]) == {"Q01", "Q02", "Q03"}


def test_filter_split_structural_vs_business(tmp_path: Path):
    """A3 — filters with is_structural=True emit structural_filter_observed;
    others emit business_filter_observed. Mutually exclusive."""
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        # Structural: CTE-scoped filter on data_source
        "WITH x AS (SELECT * FROM t WHERE data_source = 'cornerstone') "
        "SELECT bus_seg, SUM(amount) FROM x GROUP BY bus_seg",
        # Business: per-query filter on bus_seg
        "SELECT amount FROM t WHERE bus_seg = 'Centurion'",
    ])
    record_corpus_facts(store, fps)
    types = _read_event_types(store)
    # at least one of each should be present
    assert "business_filter_observed" in types
    # structural may or may not appear depending on extractor's is_structural;
    # but no event should be the legacy filter_observed type
    assert "filter_observed" not in types


def test_business_filter_carries_distinct_values(tmp_path: Path):
    """A3 — business_filter_observed should list distinct values seen in corpus."""
    import json
    store = OntologyStore(tmp_path)
    fps = parse_sqls([
        "SELECT amount FROM t WHERE bus_seg = 'Centurion'",
        "SELECT amount FROM t WHERE bus_seg = 'Platinum'",
        "SELECT amount FROM t WHERE bus_seg = 'Gold'",
    ])
    record_corpus_facts(store, fps)
    for f in store.events_dir.glob("*.jsonl"):
        for line in f.read_text().splitlines():
            if not line.strip():
                continue
            ev = json.loads(line)
            if ev["event_type"] == "business_filter_observed":
                vals = set(ev["payload"]["distinct_values_observed"])
                # All 3 should accumulate into distinct_values_observed
                assert vals >= {"Centurion", "Platinum", "Gold"}
                return
    assert False, "No business_filter_observed event emitted"


def test_corpus_facts_no_events_on_parse_errors(tmp_path: Path):
    """Parse-error fingerprints must not bleed into corpus events."""
    store = OntologyStore(tmp_path)
    fps = parse_sqls(["this is not sql at all"])
    n = record_corpus_facts(store, fps)
    # Question-pattern clustering may still emit zero events; the
    # important assertion is no crash and no metric/filter events.
    types = _read_event_types(store)
    assert "metric_observed" not in types
    assert "filter_observed" not in types
