"""Demo-question generation — gated, verified-answerable, rendered.

A DemoQuestion reaches demo_questions.md only when (1) its grounding
names something the graph knows AND (2) every capability its answer
claims (lineage, governance, metrics, …) is actually present in the
built graph for that table. The demo script must never contain a
question the graph can't answer live.
"""

from __future__ import annotations

import json

from synapse.enrichment.enricher import (
    MockLLMClient, _apply_bundle, _capabilities_present,
    _render_demo_script, enrich_graph,
)
from synapse.enrichment.schemas import (
    DemoQuestion, EnrichmentBundle, SelfAssessment,
)
from synapse.graph.store import GraphStore, canonical_uri


def _sa() -> SelfAssessment:
    return SelfAssessment(
        tables_skipped_for_lack_of_signal=[],
        columns_marked_ambiguous=0,
        proposed_entities_with_low_evidence=[],
        requires_steward_attention=[])


def _store(*, with_lineage: bool = False,
           with_guardrail: bool = False) -> GraphStore:
    store = GraphStore()
    t_uri = canonical_uri("table", "accounts")
    store.upsert_node("Table", t_uri, {"table_name": "accounts"},
                      source="mdm")
    c_uri = canonical_uri("column", "accounts", "acct_id")
    store.upsert_node("Column", c_uri, {"table_name": "accounts"},
                      source="mdm")
    store.upsert_edge("CONTAINS", t_uri, c_uri, {}, source="mdm")
    if with_lineage:
        up = canonical_uri("table", "raw_accounts")
        store.upsert_node("Table", up, {"table_name": "raw_accounts"},
                          source="mdm")
        store.upsert_edge("UPSTREAM_OF", up, t_uri, {}, source="mdm")
    if with_guardrail:
        g_uri = canonical_uri("guardrail", "skill_x", "0")
        store.upsert_node("Guardrail", g_uri,
                          {"rule": "never expose acct_id raw"},
                          source="skills")
        store.upsert_edge("CONSTRAINS", g_uri, c_uri, {}, source="skills")
    return store


def _q(**overrides) -> DemoQuestion:
    base = dict(
        question="What does acct_id represent and who can see it?",
        audience="vp",
        answered_by=["column_semantics"],
        grounding=["acct_id"],
        expected_answer_sketch="The account identifier; visibility is "
                               "constrained by a skill guardrail.",
        wow_factor="Surfaces a rule the team knows tribally.")
    base.update(overrides)
    return DemoQuestion(**base)


def _bundle(*questions: DemoQuestion) -> EnrichmentBundle:
    return EnrichmentBundle(
        table_name="accounts",
        candidate_demo_questions=list(questions),
        self_assessment=_sa())


# ─── capability verification against the built graph ────────


def test_capabilities_reflect_actual_graph_content():
    caps = _capabilities_present(_store(), "accounts")
    assert caps["column_semantics"] is True       # columns exist
    assert caps["lineage"] is False               # no edges either way
    assert caps["guardrails"] is False
    assert caps["metrics"] is False
    caps2 = _capabilities_present(
        _store(with_lineage=True, with_guardrail=True), "accounts")
    assert caps2["lineage"] is True
    assert caps2["guardrails"] is True


# ─── the gate ────────────────────────────────────────────────


def test_verified_question_survives_into_demo_pack():
    report = _apply_bundle(_store(), _bundle(_q()))
    assert report["applied_demo_questions"] == 1
    assert len(report["demo_pack"]) == 1
    assert report["demo_pack"][0]["table"] == "accounts"


def test_ungrounded_question_is_dropped():
    report = _apply_bundle(_store(), _bundle(
        _q(grounding=["flux_capacitor", "warp_core"])))
    assert report["dropped_ungrounded_demo_questions"] == 1
    assert report["demo_pack"] == []


def test_question_claiming_missing_capability_is_held():
    report = _apply_bundle(_store(), _bundle(
        _q(answered_by=["column_semantics", "lineage"])))   # no lineage
    assert report["held_unanswerable_demo_questions"] == 1
    assert report["demo_pack"] == []
    held = report["demo_pack_held"][0]
    assert held["missing_capabilities"] == ["lineage"]


def test_same_question_answerable_once_graph_has_lineage():
    report = _apply_bundle(_store(with_lineage=True), _bundle(
        _q(answered_by=["column_semantics", "lineage"])))
    assert report["applied_demo_questions"] == 1


def test_guardrail_save_question_needs_constrains_edge():
    q = _q(question="Show me spend by raw acct_id",
           answered_by=["guardrails"])
    assert _apply_bundle(_store(), _bundle(q))[
        "held_unanswerable_demo_questions"] == 1
    assert _apply_bundle(_store(with_guardrail=True), _bundle(q))[
        "applied_demo_questions"] == 1


def test_question_claiming_nothing_is_held_not_verified():
    report = _apply_bundle(_store(), _bundle(_q(answered_by=[])))
    assert report["held_unanswerable_demo_questions"] == 1
    assert report["demo_pack_held"][0]["missing_capabilities"] == [
        "(none claimed)"]


# ─── end to end: files + rendering ───────────────────────────


def test_enrich_graph_writes_demo_json_and_md(tmp_path):
    store = _store(with_lineage=True)

    def respond(table_name: str, context: dict) -> EnrichmentBundle:
        return EnrichmentBundle(
            table_name=table_name,
            candidate_demo_questions=[
                _q(audience="c_suite"),
                _q(question="Where does accounts data come from?",
                   audience="vp",
                   answered_by=["lineage"],
                   grounding=["accounts"]),
                _q(question="Ungrounded?", grounding=["nonsense_xyz"]),
            ],
            self_assessment=_sa())

    demo_json = tmp_path / "demo_questions.json"
    enrich_graph(store, MockLLMClient(respond), demo_out=demo_json,
                 only_tables=["accounts"])

    pack = json.loads(demo_json.read_text(encoding="utf-8"))
    assert len(pack["verified"]) == 2
    assert pack["held"] == []
    md = (tmp_path / "demo_questions.md").read_text(encoding="utf-8")
    assert "C-Suite" in md and "VP" in md
    assert "Where does accounts data come from?" in md
    assert "Ungrounded?" not in md                 # dropped, not rendered


def test_render_marks_live_warehouse_questions():
    md = _render_demo_script([{
        "question": "How many accounts opened last month?",
        "audience": "c_suite", "table": "accounts",
        "answered_by": ["metrics", "warehouse_sql"],
        "grounding": ["accounts"],
        "expected_answer_sketch": "A gated dry-run + row-capped query.",
        "wow_factor": "Live number with full audit trail.",
    }], n_held=1)
    assert "live gated BigQuery" in md
    assert "1 held back" in md


def test_merge_dedupes_demo_questions_across_chunks():
    from synapse.enrichment.enricher import _merge_bundles
    q = _q()
    merged = _merge_bundles("accounts", [_bundle(q), _bundle(q)])
    assert len(merged.candidate_demo_questions) == 1
