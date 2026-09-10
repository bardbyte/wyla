"""Critic v2 tests — equivalence enforcement, verdict re-derivation,
refinement events, reject pathway.

These tests cover the architectural fixes layered on top of the original
critic: the things that prevent silent semantic drift across tables.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from lumi.critic import _check_equivalence_preservation, critique_plan
from lumi.ontology import compute_equivalence_classes
from lumi.schemas import (
    CritiqueIssue,
    CritiqueReport,
    DomainOntology,
    EnrichmentPlan,
    OntologyEntity,
    TableContext,
)
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
        mdm_coverage_pct=0.5,
        mdm_dataset_details={},
        existing_view_lkml="view: t1 {}",
        queries_using_this=["q1", "q2"],
    )


def _plan(**kw) -> EnrichmentPlan:
    defaults = dict(
        table_name="t1",
        proposed_dimensions=[],
        proposed_measures=[],
        reasoning=(
            "This table is the cardmember dimension at point-in-time grain. "
            "Plan adds measures and preserves baseline."
        ),
    )
    defaults.update(kw)
    return EnrichmentPlan(**defaults)


# ─── Equivalence preservation ───────────────────────────────


def test_eq_preservation_blocks_when_class_spans_two_entities(tmp_path: Path):
    """Proven-equivalent columns split across entities = ontology contradiction."""
    fps = parse_sqls([
        "SELECT * FROM cardmember a JOIN customer_master b "
        "ON a.cm11 = b.cust_xref_id",
    ])
    eq_map = compute_equivalence_classes(fps)
    # Build an ontology that splits the class.
    ontology = DomainOntology(
        entities=[
            OntologyEntity(
                name="cardmember", grain_columns={"cardmember": ["cm11"]},
            ),
            OntologyEntity(
                name="customer", grain_columns={"customer_master": ["cust_xref_id"]},
            ),
        ],
        table_to_primary_entity={
            "cardmember": "cardmember",
            "customer_master": "customer",
        },
    )
    ctx = _ctx(table_name="cardmember")
    plan = _plan(
        proposed_dimensions=[
            {"name": "cm11", "source_column": "cm11", "type": "string"},
        ],
    )
    issues = _check_equivalence_preservation(ctx, plan, ontology, eq_map)
    blocks = [i for i in issues if i.severity == "block"]
    assert blocks, "splitting an equivalence class must be blocking"
    assert blocks[0].category == "equivalence_preservation"


def test_eq_preservation_passes_when_class_in_single_entity(tmp_path: Path):
    """When the class lives in one entity, no contradiction."""
    fps = parse_sqls([
        "SELECT * FROM cardmember a JOIN customer_master b "
        "ON a.cm11 = b.cust_xref_id",
    ])
    eq_map = compute_equivalence_classes(fps)
    # Both columns belong to ONE entity — equivalence preserved.
    ontology = DomainOntology(
        entities=[OntologyEntity(
            name="cardmember",
            grain_columns={
                "cardmember": ["cm11"],
                "customer_master": ["cust_xref_id"],
            },
        )],
    )
    ctx = _ctx(table_name="cardmember")
    plan = _plan(
        proposed_dimensions=[
            {"name": "cm11", "source_column": "cm11", "type": "string"},
        ],
    )
    issues = _check_equivalence_preservation(ctx, plan, ontology, eq_map)
    assert not [i for i in issues if i.severity == "block"]


def test_eq_preservation_info_when_column_unclassified():
    """Column on this table is in an eq class but ontology hasn't
    classified it yet — info-level so the next promotion picks up."""
    fps = parse_sqls([
        "SELECT * FROM t1 a JOIN t2 b ON a.cm = b.cust",
    ])
    eq_map = compute_equivalence_classes(fps)
    ontology = DomainOntology(entities=[])  # nothing classified yet
    ctx = _ctx(table_name="t1")
    plan = _plan(proposed_dimensions=[
        {"name": "cm", "source_column": "cm", "type": "string"},
    ])
    issues = _check_equivalence_preservation(ctx, plan, ontology, eq_map)
    assert any(
        i.severity == "info" and i.category == "equivalence_preservation"
        for i in issues
    )


# ─── Verdict re-derivation ─────────────────────────────────


def test_verdict_overrides_llm_lie_about_blocks(monkeypatch, tmp_path: Path):
    """LLM claims approve; deterministic finds blocks → verdict becomes retry."""
    monkeypatch.chdir(tmp_path)  # isolate ontology store

    ctx = _ctx()
    # Plan with placeholder name — deterministic will block.
    plan = _plan(proposed_dimensions=[
        {"name": "?", "source_column": "x", "type": "string"},
    ])
    fake_llm = CritiqueReport(
        table_name="t1",
        issues=[],  # LLM claims clean
        overall_verdict="approve",  # LIE
        radix_retrieval_score=10,
        summary="all good",
    )
    with patch("lumi.critic._llm_critique", return_value=fake_llm):
        report = critique_plan(ctx, plan, with_llm=True)
    # We must override the LLM's lie.
    assert report.overall_verdict in {"retry", "reject"}
    assert report.block_count >= 1


# ─── Refinement events ─────────────────────────────────────


def test_critique_emits_entity_refinement_events(monkeypatch, tmp_path: Path):
    """A vocabulary_completeness issue mentioning `cardmember` should
    emit an entity_refinement event into the store."""
    monkeypatch.chdir(tmp_path)

    ctx = _ctx(
        table_name="cardmember_dim",
        # Plan has reasoning that doesn't mention the entity → triggers
        # vocabulary_completeness with `cardmember` in the recommendation.
    )
    plan = _plan(
        reasoning=(
            "This is a generic dimension table that adds five new fields "
            "to cover the recent query usage observed in the corpus."
        ),
        proposed_dimensions=[
            {"name": "id", "source_column": "id", "type": "string"},
        ],
    )
    ontology = DomainOntology(
        entities=[OntologyEntity(name="cardmember", synonyms=["cm"])],
        table_to_primary_entity={"cardmember_dim": "cardmember"},
    )
    critique_plan(ctx, plan, ontology=ontology, with_llm=False)

    # The store should now have at least one entity_refinement event
    # mining `cardmember` from the recommendation backticks.
    from lumi.ontology_store import OntologyStore
    store = OntologyStore()
    cands = store.candidates()
    assert "cardmember" in (cands.get("entities") or {})


# ─── Reject bubbling ───────────────────────────────────────


def test_critic_rejected_plan_marks_authoring_reason():
    """When critic returns reject, the plan should carry the reason
    in authoring.reason for the execute phase to detect."""
    from unittest.mock import patch as _patch

    from lumi.config import LumiConfig
    from lumi.plan_builder import build_enrichment_plan

    cfg = LumiConfig()
    cfg.plan_repair_max_rounds = 0  # zero retries — critic verdict is final

    ctx = TableContext(
        table_name="t1",
        columns_referenced=["x"],
        aggregations=[], case_whens=[], ctes_referencing_this=[],
        temp_tables_referencing_this=[], joins_involving_this=[],
        filters_on_this=[], date_functions=[], mdm_columns=[],
        mdm_table_description=None, mdm_coverage_pct=0.5,
        existing_view_lkml="view: t1 {}",
        queries_using_this=["q1", "q2"],
    )
    fps = parse_sqls(["SELECT x FROM t1"])

    refined = EnrichmentPlan(
        table_name="t1",
        proposed_dimensions=[
            {"name": "x", "type": "string", "source_column": "x"},
        ],
        proposed_measures=[],
        reasoning="Cardmember table at one row per cardmember per day.",
        complexity="simple",
    )
    rejection = CritiqueReport(
        table_name="t1",
        issues=[CritiqueIssue(
            category="reasoning_grounding", severity="block",
            locus="reasoning", finding="bad", recommendation="fix",
        )],
        overall_verdict="reject",
        radix_retrieval_score=0,
        summary="rejected",
    )
    with _patch("lumi.plan_builder._invoke_plan_agent", return_value=refined), \
         _patch("lumi.critic.critique_plan", return_value=rejection):
        plan = build_enrichment_plan(
            ctx, all_fingerprints=fps, with_llm=True, with_critic=True, config=cfg,
        )
    assert plan.authoring.get("reason") == "rejected_by_critic"
