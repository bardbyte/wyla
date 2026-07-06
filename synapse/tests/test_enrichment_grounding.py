"""The grounding gate — skill.md's anti-patterns enforced as code.

Every LLM claim is validated against what the graph actually knows
before it is written. These tests pin the gate's behavior: imagined
columns dropped, unevidenced/low-confidence descriptions held,
ungrounded synonyms and code resolutions dropped — and every verdict
counted in the per-table grounding report.
"""

from __future__ import annotations

from synapse.enrichment.enricher import (
    MockLLMClient, _apply_bundle, _grounding_index, enrich_graph,
)
from synapse.enrichment.schemas import (
    CandidateSynonym, CodeResolution, ColumnObservation,
    EnrichmentBundle, SelfAssessment,
)
from synapse.graph.store import GraphStore, canonical_uri


def _sa() -> SelfAssessment:
    return SelfAssessment(
        tables_skipped_for_lack_of_signal=[],
        columns_marked_ambiguous=0,
        proposed_entities_with_low_evidence=[],
        requires_steward_attention=[])


def _store() -> GraphStore:
    """accounts(acct_id [business_name: Account Identifier], status_code)."""
    store = GraphStore()
    t_uri = canonical_uri("table", "accounts")
    store.upsert_node("Table", t_uri, {"table_name": "accounts"}, source="mdm")
    for col, props in [
        ("acct_id", {"business_name": "Account Identifier"}),
        ("status_code", {}),
    ]:
        c_uri = canonical_uri("column", "accounts", col)
        store.upsert_node(
            "Column", c_uri,
            {"table_name": "accounts", **props}, source="mdm")
        store.upsert_edge("CONTAINS", t_uri, c_uri, {}, source="mdm")
    return store


def _obs(**overrides) -> ColumnObservation:
    base = dict(column_name="acct_id", proposed_description="An account id.",
                candidate_role="identifier", self_confidence=0.9,
                evidence_used=["mdm"])
    base.update(overrides)
    return ColumnObservation(**base)


# ─── the reference set ───────────────────────────────────────


def test_grounding_index_normalizes_names_and_properties():
    known = _grounding_index(_store())
    assert "accounts" in known            # table uri tail
    assert "acctid" in known              # column uri tail, normalized
    assert "accountidentifier" in known   # business_name property
    assert "ghostcolumn" not in known


# ─── column observations ─────────────────────────────────────


def test_imagined_column_is_dropped_not_minted():
    store = _store()
    n_before = len(store.nodes)
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[_obs(column_name="hallucinated_col")],
        self_assessment=_sa()))
    assert report["dropped_imagined_columns"] == 1
    assert report["applied_descriptions"] == 0
    assert len(store.nodes) == n_before   # nothing minted

def test_grounded_description_is_applied():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts", column_observations=[_obs()],
        self_assessment=_sa()))
    assert report["applied_descriptions"] == 1
    node = store.get(canonical_uri("column", "accounts", "acct_id"))
    assert node.properties["ai_generated_description"] == "An account id."
    assert "llm_generated" in node.provenance.sources


def test_description_without_evidence_is_held_but_role_lands():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[_obs(evidence_used=[])],
        self_assessment=_sa()))
    assert report["held_no_evidence"] == 1
    assert report["applied_descriptions"] == 0
    node = store.get(canonical_uri("column", "accounts", "acct_id"))
    assert "ai_generated_description" not in node.properties  # text held
    assert node.properties["candidate_role"] == "identifier"  # audit trail lands


def test_low_confidence_description_is_held():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[_obs(self_confidence=0.2)],
        self_assessment=_sa()))
    assert report["held_low_confidence"] == 1
    node = store.get(canonical_uri("column", "accounts", "acct_id"))
    assert "ai_generated_description" not in node.properties
    assert node.properties["llm_self_confidence"] == 0.2      # still auditable


def test_ambiguity_flags_are_counted():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[
            _obs(ambiguity_flag="MDM says nullable, BQ shows 100% non-null")],
        self_assessment=_sa()))
    assert report["ambiguity_flags"] == 1
    node = store.get(canonical_uri("column", "accounts", "acct_id"))
    assert "non-null" in node.properties["ambiguity_flag"]


# ─── synonyms ────────────────────────────────────────────────


def test_ungrounded_synonym_is_dropped():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        candidate_synonyms=[CandidateSynonym(
            surface_form="BNM", canonical_form="Blorp Nonsense Metric",
            evidence_source="corpus", rationale="seen once")],
        self_assessment=_sa()))
    assert report["dropped_ungrounded_synonyms"] == 1
    assert store.nodes_by_type("Synonym") == []


def test_grounded_synonym_is_kept():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        candidate_synonyms=[CandidateSynonym(
            surface_form="AID", canonical_form="Account Identifier",
            evidence_source="corpus", rationale="alias in corpus queries")],
        self_assessment=_sa()))
    assert report["dropped_ungrounded_synonyms"] == 0
    synonyms = store.nodes_by_type("Synonym")
    assert len(synonyms) == 1
    assert synonyms[0].properties["surface_form"] == "AID"
    assert synonyms[0].properties["entry_type"] == "LLM_Inferred"


def test_synonym_grounding_is_case_and_punctuation_insensitive():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        candidate_synonyms=[CandidateSynonym(
            surface_form="acct", canonical_form="ACCT-ID",  # → acctid
            evidence_source="table_name", rationale="column name")],
        self_assessment=_sa()))
    assert report["dropped_ungrounded_synonyms"] == 0
    assert len(store.nodes_by_type("Synonym")) == 1


# ─── code resolutions ────────────────────────────────────────


def test_code_resolution_on_ghost_column_is_dropped():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        candidate_code_resolutions=[CodeResolution(
            column="ghost_column", raw_value="005",
            proposed_meaning="Platinum", evidence="none really",
            confidence=0.9)],
        self_assessment=_sa()))
    assert report["dropped_ungrounded_code_resolutions"] == 1
    assert store.nodes_by_type("CodeMapping") == []


def test_code_resolution_on_real_column_is_kept():
    store = _store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        candidate_code_resolutions=[CodeResolution(
            column="status_code", raw_value="A",
            proposed_meaning="Active", evidence="CASE WHEN in corpus",
            confidence=0.8)],
        self_assessment=_sa()))
    assert report["dropped_ungrounded_code_resolutions"] == 0
    mappings = store.nodes_by_type("CodeMapping")
    assert len(mappings) == 1
    assert mappings[0].properties["human_meaning"] == "Active"
    assert mappings[0].properties["source"] == "llm_inferred"


# ─── end to end through enrich_graph ─────────────────────────


def test_grounding_reports_out_param_is_filled_per_table():
    store = _store()

    def respond(table_name: str, context: dict) -> EnrichmentBundle:
        return EnrichmentBundle(
            table_name=table_name,
            column_observations=[
                _obs(),                                    # applied
                _obs(column_name="made_up_col"),           # dropped
                _obs(column_name="status_code",
                     proposed_description="A code.",
                     candidate_role="code", evidence_used=[]),  # held
            ],
            candidate_synonyms=[CandidateSynonym(
                surface_form="XYZ", canonical_form="Not A Real Thing",
                evidence_source="corpus", rationale="?")],  # dropped
            self_assessment=_sa())

    reports: dict[str, dict] = {}
    enrich_graph(store, MockLLMClient(respond), grounding_reports=reports)

    assert set(reports) == {"accounts"}
    r = reports["accounts"]
    assert r["applied_descriptions"] == 1
    assert r["dropped_imagined_columns"] == 1
    assert r["held_no_evidence"] == 1
    assert r["dropped_ungrounded_synonyms"] == 1
