"""Tests for the review-markdown renderer."""

from __future__ import annotations

from pathlib import Path

from synapse.curation.parser import parse_llm_output
from synapse.curation.review import render_review_markdown


def _proposal(llm_response_yaml: Path):
    return parse_llm_output(
        llm_response_yaml.read_text(encoding="utf-8"),
        model_used="test-model",
        prompt_sha="0" * 64,
    )


def test_render_contains_header_and_per_entity_sections(llm_response_yaml: Path):
    md = render_review_markdown(_proposal(llm_response_yaml))
    assert md.startswith("# Synapse — Entity Registry Proposal")
    # One H3 per entity (Cardmember + CardProduct)
    assert "### 1. `Cardmember`" in md
    assert "### 2. `CardProduct`" in md


def test_render_includes_decision_checkboxes_per_entity(llm_response_yaml: Path):
    md = render_review_markdown(_proposal(llm_response_yaml))
    # Two entities → two decision lines
    assert md.count("[ ] approve") == 2
    assert md.count("[ ] modify") == 2
    assert md.count("[ ] reject") == 2


def test_render_includes_ambiguities_section(llm_response_yaml: Path):
    md = render_review_markdown(_proposal(llm_response_yaml))
    assert "## ⚠ Ambiguities to resolve" in md
    # Both ambiguities from fixture should appear
    assert "CM acronym means both" in md
    assert "Account vs CardAccount" in md


def test_render_includes_scope_observations(llm_response_yaml: Path):
    md = render_review_markdown(_proposal(llm_response_yaml))
    assert "## Scope observations" in md
    assert "Loyalty domain" in md


def test_render_includes_source_evidence_per_entity(llm_response_yaml: Path):
    md = render_review_markdown(_proposal(llm_response_yaml))
    # The Cardmember entity references these tables/columns/metrics
    assert "`custins_customer_insights_cardmember`" in md
    assert "`cm11`" in md
    assert "`total_billed_business`" in md
    assert "`CM`" in md  # acronym


def test_render_confidence_pill_reflects_score(llm_response_yaml: Path):
    md = render_review_markdown(_proposal(llm_response_yaml))
    # Cardmember has 0.95 → high; CardProduct has 0.82 → medium
    assert "🟢 high" in md
    assert "🟡 medium" in md


def test_render_omits_relationship_block_when_empty():
    """An entity with no relationships shouldn't show an empty block."""
    from synapse.registry import CurationProposal, ProposedEntity
    proposal = CurationProposal(
        proposed_entities=[
            ProposedEntity(
                canonical_name="Solo",
                description="lonely entity",
                source_evidence={"tables": [], "columns": [], "metrics": [],
                                  "acronyms": [], "data_categories": []},
                properties={},
                parent_entity=None,
                relationships=[],
                llm_confidence=0.7,
                human_review_notes="",
            ),
        ],
    )
    md = render_review_markdown(proposal)
    assert "**Relationships**" not in md
