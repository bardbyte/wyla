"""Disambiguating descriptions (B) — schemas, critic, render."""

from __future__ import annotations

from pathlib import Path

from lumi.critic import _check_disambiguation_completeness
from lumi.publish import _apply_explore_description, _apply_view_description
from lumi.schemas import (
    DomainOntology,
    EnrichmentPlan,
    ExploreDescription,
    OntologyEntity,
    TableContext,
    ViewDescription,
)


def _ctx(name: str = "cardmember_dim") -> TableContext:
    return TableContext(
        table_name=name,
        columns_referenced=["cm11"], aggregations=[], case_whens=[],
        ctes_referencing_this=[], temp_tables_referencing_this=[],
        joins_involving_this=[], filters_on_this=[], date_functions=[],
        mdm_columns=[], mdm_table_description=None, mdm_coverage_pct=0.5,
        existing_view_lkml="view: " + name + " {}",
        queries_using_this=["q1", "q2"],
    )


def _plan(**kw) -> EnrichmentPlan:
    defaults = dict(
        table_name="cardmember_dim",
        proposed_dimensions=[
            {"name": "cm11", "source_column": "cm11", "type": "string"},
        ],
        proposed_measures=[],
        reasoning=(
            "Cardmember dim at point-in-time grain. Adds basic dimensions."
        ),
    )
    defaults.update(kw)
    return EnrichmentPlan(**defaults)


# ─── Critic ─────────────────────────────────────────────────


def test_blocks_when_view_description_missing():
    issues = _check_disambiguation_completeness(_ctx(), _plan(), ontology=None)
    blocks = [i for i in issues if i.severity == "block"]
    assert blocks
    assert blocks[0].locus == "proposed_view_description"


def test_blocks_when_distinguishes_from_empty_with_siblings():
    """ontology has 2 sibling tables; distinguishes_from is empty → BLOCK."""
    ontology = DomainOntology(
        entities=[OntologyEntity(
            name="cardmember",
            grain_columns={
                "cardmember_dim": ["cm11"],
                "cardmember_snapshot": ["cm11"],
                "cardmember_monthly": ["cm11"],
            },
        )],
        table_to_primary_entity={
            "cardmember_dim": "cardmember",
            "cardmember_snapshot": "cardmember",
            "cardmember_monthly": "cardmember",
        },
    )
    plan = _plan(proposed_view_description=ViewDescription(
        one_liner="cm dim", grain="one row per cm", scope="all",
        when_to_use="cm questions",
        distinguishes_from=[],  # empty!
    ))
    issues = _check_disambiguation_completeness(_ctx(), plan, ontology)
    assert any(
        i.severity == "block" and "distinguishes_from" in i.locus
        for i in issues
    )


def test_warns_when_one_liner_missing():
    plan = _plan(proposed_view_description=ViewDescription(
        one_liner="",
        grain="one row per cm",
        when_to_use="cm questions",
    ))
    issues = _check_disambiguation_completeness(_ctx(), plan, ontology=None)
    warns = [
        i for i in issues
        if i.severity == "warn" and "one_liner" in i.locus
    ]
    assert warns


def test_warns_when_one_liner_too_long():
    long_liner = "x" * 250
    plan = _plan(proposed_view_description=ViewDescription(
        one_liner=long_liner,
        grain="one row per cm",
        when_to_use="cm questions",
    ))
    issues = _check_disambiguation_completeness(_ctx(), plan, ontology=None)
    warns = [
        i for i in issues
        if i.severity == "warn" and "one_liner" in i.locus
    ]
    assert warns


def test_passes_when_complete_and_no_siblings():
    plan = _plan(
        proposed_view_description=ViewDescription(
            one_liner="cm dim",
            grain="one row per cm per day",
            scope="active US cm",
            when_to_use="cm attribute questions",
            when_not_to_use="don't use for txn-grain spend",
        ),
        proposed_explore_description=ExploreDescription(
            one_liner="cm explore",
            primary_questions=["how many active cm?", "cm by region?", "cm by segment?"],
            anti_questions=["use cm_txn for spend"],
        ),
    )
    issues = _check_disambiguation_completeness(_ctx(), plan, ontology=None)
    assert not [i for i in issues if i.severity == "block"]


def test_passes_when_distinguishes_from_covers_siblings():
    ontology = DomainOntology(
        entities=[OntologyEntity(
            name="cardmember",
            grain_columns={
                "cardmember_dim": ["cm11"],
                "cardmember_snapshot": ["cm11"],
            },
        )],
        table_to_primary_entity={
            "cardmember_dim": "cardmember",
            "cardmember_snapshot": "cardmember",
        },
    )
    plan = _plan(
        proposed_view_description=ViewDescription(
            one_liner="cm dim PIT",
            grain="one row per cm per day",
            when_to_use="point-in-time cm attribute questions",
            distinguishes_from=[{
                "view_name": "cardmember_snapshot",
                "how_it_differs": "monthly vs daily snapshot",
            }],
        ),
        proposed_explore_description=ExploreDescription(
            one_liner="cm explore",
            primary_questions=["q1", "q2", "q3"],
            anti_questions=["use cardmember_snapshot for monthly"],
        ),
    )
    issues = _check_disambiguation_completeness(_ctx(), plan, ontology)
    assert not [i for i in issues if i.severity == "block"]


def test_warns_when_explore_missing_anti_questions_with_siblings():
    ontology = DomainOntology(
        entities=[OntologyEntity(
            name="cardmember",
            grain_columns={
                "cardmember_dim": ["cm11"],
                "cardmember_snapshot": ["cm11"],
            },
        )],
        table_to_primary_entity={
            "cardmember_dim": "cardmember",
            "cardmember_snapshot": "cardmember",
        },
    )
    plan = _plan(
        proposed_view_description=ViewDescription(
            one_liner="cm dim", grain="one row per cm",
            when_to_use="cm questions",
            distinguishes_from=[{
                "view_name": "cardmember_snapshot",
                "how_it_differs": "monthly snapshot",
            }],
        ),
        proposed_explore_description=ExploreDescription(
            one_liner="cm explore",
            primary_questions=["q1", "q2", "q3"],
            anti_questions=[],  # missing!
        ),
    )
    issues = _check_disambiguation_completeness(_ctx(), plan, ontology)
    assert any(
        "anti_questions" in i.locus and i.severity == "warn"
        for i in issues
    )


# ─── Render ─────────────────────────────────────────────────


def test_view_description_renders_comment_block():
    view_lkml = "view: cm_dim {\n  sql_table_name: dw.cm_dim ;;\n}\n"
    vd = ViewDescription(
        one_liner="Cardmember dimension at PIT",
        grain="one row per cm per day",
        scope="active US cm",
        when_to_use="cm attribute questions",
        when_not_to_use="don't use for spend",
        distinguishes_from=[
            {"view_name": "cm_snapshot", "how_it_differs": "monthly snapshot"},
        ],
    )
    result = _apply_view_description(view_lkml, vd)
    assert "# === DISAMBIGUATION ===" in result
    assert "# summary: Cardmember dimension at PIT" in result
    assert "# grain: one row per cm per day" in result
    assert "cm_snapshot" in result
    # Comment block precedes view: keyword
    assert result.index("# === DISAMBIGUATION ===") < result.index("view:")


def test_view_description_injects_description_param():
    view_lkml = "view: cm_dim {\n  sql_table_name: dw.cm_dim ;;\n}\n"
    vd = ViewDescription(
        one_liner="Cardmember dim",
        grain="one row per cm per day",
        scope="active US cm",
    )
    result = _apply_view_description(view_lkml, vd)
    # description: parameter ends up in the view body
    assert "description:" in result
    assert "Cardmember dim" in result


def test_view_description_idempotent():
    """Re-applying replaces the prior block, doesn't accumulate."""
    view_lkml = "view: cm_dim {}\n"
    vd1 = ViewDescription(one_liner="first version", grain="g")
    vd2 = ViewDescription(one_liner="second version", grain="g")
    once = _apply_view_description(view_lkml, vd1)
    twice = _apply_view_description(once, vd2)
    assert twice.count("# === DISAMBIGUATION ===") == 1
    assert "second version" in twice
    assert "first version" not in twice


def test_view_description_noop_when_empty():
    view_lkml = "view: x {}"
    assert _apply_view_description(view_lkml, None) == view_lkml
    assert _apply_view_description(view_lkml, ViewDescription()) == view_lkml


def test_explore_description_renders_block_and_param():
    explore_lkml = "explore: cm_dim {}\n"
    ed = ExploreDescription(
        one_liner="Explore for cm questions",
        primary_questions=[
            "how many active cardmembers?",
            "cardmembers by segment?",
        ],
        anti_questions=["spend questions go to cm_txn explore"],
        canonical_filters={"data_source": "cornerstone"},
        join_paths=["cardmember → account → transaction"],
    )
    result = _apply_explore_description(explore_lkml, ed)
    assert "# === EXPLORE DISAMBIGUATION ===" in result
    assert "# summary: Explore for cm questions" in result
    assert "# primary_questions" in result
    assert "how many active cardmembers" in result
    assert "# anti_questions" in result
    assert "data_source: cornerstone" in result
    assert "cardmember → account → transaction" in result
    # description: parameter present
    assert "description:" in result


def test_explore_description_idempotent():
    explore_lkml = "explore: x {}\n"
    ed1 = ExploreDescription(one_liner="v1", primary_questions=["q"])
    ed2 = ExploreDescription(one_liner="v2", primary_questions=["q"])
    once = _apply_explore_description(explore_lkml, ed1)
    twice = _apply_explore_description(once, ed2)
    assert twice.count("# === EXPLORE DISAMBIGUATION ===") == 1
    assert "v2" in twice and "v1" not in twice


# ─── End-to-end: plan → enrich → publish ────────────────────


def test_publish_includes_descriptions_for_enriched_output(tmp_path: Path):
    """Full publish path renders descriptions from EnrichedOutput."""
    from lumi.publish import publish_to_disk
    from lumi.schemas import EnrichedOutput

    enriched = {
        "cm_dim": EnrichedOutput(
            view_lkml="view: cm_dim { sql_table_name: dw.cm_dim ;; }\n",
            explore_lkml="explore: cm_dim {}\n",
            view_description=ViewDescription(
                one_liner="cm dim",
                grain="one row per cm per day",
                scope="active",
                when_to_use="cm questions",
            ),
            explore_description=ExploreDescription(
                one_liner="cm explore",
                primary_questions=["q1", "q2", "q3"],
            ),
        ),
    }
    baseline_dir = tmp_path / "baseline"
    output_dir = tmp_path / "out"
    baseline_dir.mkdir()
    res = publish_to_disk(
        enriched, baseline_dir=baseline_dir, output_dir=output_dir,
        coverage=None,
    )
    assert res.get("status") == "ok"
    view_text = (output_dir / "views" / "cm_dim.view.lkml").read_text()
    assert "# === DISAMBIGUATION ===" in view_text
    assert "cm dim" in view_text
    model_text = (output_dir / "models" / "lumi_enriched.model.lkml").read_text()
    assert "# === EXPLORE DISAMBIGUATION ===" in model_text
    assert "cm explore" in model_text
