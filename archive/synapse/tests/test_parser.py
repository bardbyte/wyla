"""Tests for the LLM-output parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from synapse.curation.parser import parse_llm_output


def test_parses_clean_fixture_response(llm_response_yaml: Path):
    raw = llm_response_yaml.read_text(encoding="utf-8")
    proposal = parse_llm_output(raw, model_used="test-model", prompt_sha="abc")
    assert len(proposal.proposed_entities) == 2
    e0 = proposal.proposed_entities[0]
    assert e0.canonical_name == "Cardmember"
    assert e0.llm_confidence == 0.95
    assert "CM" in e0.source_evidence.get("acronyms", [])
    assert len(proposal.ambiguities_flagged) == 2
    assert proposal.model_used == "test-model"
    assert proposal.prompt_sha256 == "abc"
    assert proposal.generated_at  # auto-stamped


def test_strips_markdown_fences():
    raw = """```yaml
proposed_entities:
  - canonical_name: Foo
    description: a test entity
    source_evidence:
      tables: [t1]
      columns: []
      metrics: []
      acronyms: []
      data_categories: []
    properties: {}
    parent_entity: null
    relationships: []
    llm_confidence: 0.5
    human_review_notes: ""
ambiguities_flagged: []
scope_observations: []
```
"""
    proposal = parse_llm_output(raw)
    assert len(proposal.proposed_entities) == 1
    assert proposal.proposed_entities[0].canonical_name == "Foo"


def test_strips_preamble_prose():
    """Tolerate a model that adds prose before the YAML."""
    raw = """\
Sure, here is the proposed entity registry based on your evidence:

proposed_entities:
  - canonical_name: Bar
    description: another test
    source_evidence: {tables: [], columns: [], metrics: [], acronyms: [], data_categories: []}
    properties: {}
    parent_entity: null
    relationships: []
    llm_confidence: 0.6
    human_review_notes: ""
ambiguities_flagged: []
scope_observations: []
"""
    proposal = parse_llm_output(raw)
    assert proposal.proposed_entities[0].canonical_name == "Bar"


def test_invalid_yaml_raises_with_context():
    with pytest.raises(ValueError, match="not valid YAML"):
        parse_llm_output("not: valid: yaml: with: colons: everywhere")


def test_missing_top_level_key_raises():
    raw = "ambiguities_flagged: []\nscope_observations: []\n"
    with pytest.raises(ValueError, match="missing required 'proposed_entities'"):
        parse_llm_output(raw)


def test_schema_violation_raises_with_offender():
    """An entity missing required fields surfaces a Pydantic violation."""
    raw = """\
proposed_entities:
  - canonical_name: Bad
    # missing description, source_evidence, llm_confidence
ambiguities_flagged: []
scope_observations: []
"""
    with pytest.raises(ValueError, match="does not match"):
        parse_llm_output(raw)


def test_relationship_type_enum_enforced():
    raw = """\
proposed_entities:
  - canonical_name: X
    description: x
    source_evidence: {tables: [], columns: [], metrics: [], acronyms: [], data_categories: []}
    properties: {}
    parent_entity: null
    relationships:
      - type: bogus_type    # invalid
        target_entity: Y
        via_column: null
        cardinality_evidence: null
    llm_confidence: 0.5
    human_review_notes: ""
ambiguities_flagged: []
scope_observations: []
"""
    with pytest.raises(ValueError):
        parse_llm_output(raw)


def test_llm_confidence_bounds_enforced():
    raw = """\
proposed_entities:
  - canonical_name: X
    description: x
    source_evidence: {tables: [], columns: [], metrics: [], acronyms: [], data_categories: []}
    properties: {}
    parent_entity: null
    relationships: []
    llm_confidence: 1.5      # invalid: must be <= 1.0
    human_review_notes: ""
ambiguities_flagged: []
scope_observations: []
"""
    with pytest.raises(ValueError):
        parse_llm_output(raw)
