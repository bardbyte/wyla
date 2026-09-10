"""The grounding gate — skill.md's anti-patterns enforced as code.

Every LLM claim is validated against what the graph actually knows
before it is written. These tests pin the gate's behavior: imagined
columns dropped, unevidenced/low-confidence descriptions held,
ungrounded synonyms and code resolutions dropped — and every verdict
counted in the per-table grounding report.
"""

from __future__ import annotations

import json

from synapse.enrichment.enricher import (
    MockLLMClient, _apply_bundle, _grounding_index, enrich_graph,
)
from synapse.enrichment.schemas import (
    CandidateSynonym, CodeResolution, ColumnObservation,
    EnrichmentBundle, RelationProposal, SelfAssessment,
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


# ─── relates_to → cross-table edges, gap-fill only ───────────


def _two_table_store() -> GraphStore:
    store = _store()
    t2 = canonical_uri("table", "customers")
    store.upsert_node("Table", t2, {"table_name": "customers"}, source="mdm")
    c2 = canonical_uri("column", "customers", "cust_id")
    store.upsert_node("Column", c2, {"table_name": "customers"}, source="mdm")
    store.upsert_edge("CONTAINS", t2, c2, {}, source="mdm")
    return store


def _rel(**overrides) -> RelationProposal:
    base = dict(target_table="customers", target_column="cust_id",
                verb="joins to", evidence_count=3)
    base.update(overrides)
    return RelationProposal(**base)


def test_grounded_relation_becomes_equivalent_to_edge():
    store = _two_table_store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[_obs(relates_to=[_rel()])],
        self_assessment=_sa()))
    assert report["applied_relations"] == 1
    edge_uri = (f"{canonical_uri('column', 'accounts', 'acct_id')}"
                f"::EQUIVALENT_TO::"
                f"{canonical_uri('column', 'customers', 'cust_id')}")
    edge = store.edges[edge_uri]
    assert edge.properties["verb"] == "joins to"
    assert "llm_generated" in edge.provenance.sources


def test_relation_to_ghost_target_is_dropped():
    store = _two_table_store()
    n_edges = len(store.edges)
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[
            _obs(relates_to=[_rel(target_table="ghost_table")])],
        self_assessment=_sa()))
    assert report["dropped_ungrounded_relations"] == 1
    assert report["applied_relations"] == 0
    assert len(store.edges) == n_edges


def test_corpus_witnessed_relation_is_skipped_not_double_counted():
    store = _two_table_store()
    a_uri = canonical_uri("column", "accounts", "acct_id")
    c_uri = canonical_uri("column", "customers", "cust_id")
    store.upsert_edge("EQUIVALENT_TO", a_uri, c_uri,
                      {"join_type": "INNER"}, source="corpus")
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[_obs(relates_to=[_rel()])],
        self_assessment=_sa()))
    assert report["skipped_existing_relations"] == 1
    assert report["applied_relations"] == 0
    # the LLM echoing the corpus back must NOT count as a second witness
    edge = store.edges[f"{a_uri}::EQUIVALENT_TO::{c_uri}"]
    assert "llm_generated" not in edge.provenance.sources


def test_reverse_direction_existing_edge_also_skips():
    store = _two_table_store()
    a_uri = canonical_uri("column", "accounts", "acct_id")
    c_uri = canonical_uri("column", "customers", "cust_id")
    store.upsert_edge("EQUIVALENT_TO", c_uri, a_uri, {}, source="corpus")
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[_obs(relates_to=[_rel()])],
        self_assessment=_sa()))
    assert report["skipped_existing_relations"] == 1
    assert report["applied_relations"] == 0


def test_self_relation_is_dropped():
    store = _two_table_store()
    report = _apply_bundle(store, EnrichmentBundle(
        table_name="accounts",
        column_observations=[_obs(relates_to=[
            _rel(target_table="accounts", target_column="acct_id")])],
        self_assessment=_sa()))
    assert report["dropped_ungrounded_relations"] == 1


# ─── evidence-rich context: SQL, skills, scope digest ────────


def test_context_carries_session_sql_and_scope_digest(tmp_path):
    store = _two_table_store()
    (tmp_path / "accounts.json").write_text(json.dumps({
        "queries_using_this": [
            {"sql": "SELECT a.acct_id FROM accounts a "
                    "JOIN customers c ON a.acct_id = c.cust_id"},
            "SELECT COUNT(*) FROM accounts",     # bare-string entry tolerated
        ],
        "aggregations": [{"function": "COUNT", "column": "acct_id"}],
    }), encoding="utf-8")

    captured: dict[str, dict] = {}

    def respond(table_name: str, context: dict) -> EnrichmentBundle:
        captured[table_name] = context
        return EnrichmentBundle(table_name=table_name, self_assessment=_sa())

    enrich_graph(store, MockLLMClient(respond),
                 only_tables=["accounts"], evidence_dir=tmp_path)

    ctx = captured["accounts"]
    sql = ctx["corpus_sql_evidence"]
    assert "JOIN customers" in sql["queries"][0]
    assert sql["queries"][1] == "SELECT COUNT(*) FROM accounts"
    assert sql["n_queries_total"] == 2
    # scope digest lists ALL graph tables (even outside the enrich scope)
    scope = {t["table"]: t for t in ctx["tables_in_scope"]}
    assert {"accounts", "customers"} <= set(scope)
    assert "cust_id" in scope["customers"]["columns"]


def test_context_carries_skill_knowledge_for_applied_tables():
    store = _store()
    s_uri = canonical_uri("skill", "roll_rate_analysis")
    store.upsert_node("Skill", s_uri, {
        "skill_id": "roll_rate_analysis", "domain": "portfolio_analytics",
        "description": "Roll rate methodology",
        "knowledge_excerpt": "Never apply LAG() to pre-lagged tables.",
        "metrics_defined": ["roll_rate"],
    }, source="skills")
    store.upsert_edge("APPLIES_TO", s_uri,
                      canonical_uri("table", "accounts"), {}, source="skills")

    captured: dict[str, dict] = {}

    def respond(table_name: str, context: dict) -> EnrichmentBundle:
        captured[table_name] = context
        return EnrichmentBundle(table_name=table_name, self_assessment=_sa())

    enrich_graph(store, MockLLMClient(respond))
    skills = captured["accounts"]["skills_evidence"]
    assert len(skills) == 1
    assert skills[0]["skill_id"] == "roll_rate_analysis"
    assert "pre-lagged" in skills[0]["knowledge_excerpt"]


def test_missing_evidence_dir_yields_empty_sql_evidence():
    store = _store()
    captured: dict[str, dict] = {}

    def respond(table_name: str, context: dict) -> EnrichmentBundle:
        captured[table_name] = context
        return EnrichmentBundle(table_name=table_name, self_assessment=_sa())

    enrich_graph(store, MockLLMClient(respond))    # no evidence_dir at all
    assert captured["accounts"]["corpus_sql_evidence"] == {}


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
