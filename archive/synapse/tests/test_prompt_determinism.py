"""Prompt determinism — same EvidenceBundle → same bytes → same SHA."""

from __future__ import annotations

from pathlib import Path

from synapse.curation.prompt import build_prompt, prompt_sha256
from synapse.registry import EvidenceBundle
from synapse.registry.glossary import load_glossary
from synapse.registry.metric_catalog import load_metric_catalog
from synapse.registry.table_catalog import load_table_catalog


def _bundle(
    glossary_csv: Path,
    metric_catalog_csv: Path,
    table_catalog_csv: Path,
) -> EvidenceBundle:
    return EvidenceBundle(
        scope_description="unit test scope",
        table_catalog=load_table_catalog(table_catalog_csv),
        glossary=load_glossary(glossary_csv),
        metric_catalog=load_metric_catalog(metric_catalog_csv),
        mdm_digests=[],
        corpus_signals=[],
        n_queries_analyzed=0,
    )


def test_prompt_is_byte_identical_across_builds(
    glossary_csv, metric_catalog_csv, table_catalog_csv,
):
    bundle = _bundle(glossary_csv, metric_catalog_csv, table_catalog_csv)
    p1 = build_prompt(bundle)
    p2 = build_prompt(bundle)
    assert p1 == p2


def test_prompt_sha256_is_stable(
    glossary_csv, metric_catalog_csv, table_catalog_csv,
):
    bundle = _bundle(glossary_csv, metric_catalog_csv, table_catalog_csv)
    s1 = prompt_sha256(build_prompt(bundle))
    s2 = prompt_sha256(build_prompt(bundle))
    assert s1 == s2
    assert len(s1) == 64  # SHA-256 hex


def test_prompt_includes_required_sections(
    glossary_csv, metric_catalog_csv, table_catalog_csv,
):
    bundle = _bundle(glossary_csv, metric_catalog_csv, table_catalog_csv)
    p = build_prompt(bundle)
    # Must include all five evidence sections + output contract
    for required in (
        "Section A — Table catalog",
        "Section B — MDM table digests",
        "Section C — Glossary",
        "Section D — Metric catalog",
        "Section E — Corpus noun-frequency",
        "OUTPUT CONTRACT",
        "proposed_entities:",
    ):
        assert required in p, f"missing required marker: {required!r}"


def test_prompt_changes_when_bundle_changes(
    glossary_csv, metric_catalog_csv, table_catalog_csv,
):
    """A different scope_description must produce a different SHA."""
    b1 = _bundle(glossary_csv, metric_catalog_csv, table_catalog_csv)
    b2 = b1.model_copy(update={"scope_description": "different scope"})
    assert prompt_sha256(build_prompt(b1)) != prompt_sha256(build_prompt(b2))


def test_prompt_highlights_ambiguous_symbols(
    glossary_csv, metric_catalog_csv, table_catalog_csv,
):
    """The prompt must explicitly call out ambiguous glossary entries."""
    bundle = _bundle(glossary_csv, metric_catalog_csv, table_catalog_csv)
    p = build_prompt(bundle)
    # cm and aa both have multiple definitions in the fixture
    assert "Ambiguous symbols" in p
    assert "`cm`" in p
    assert "`aa`" in p
