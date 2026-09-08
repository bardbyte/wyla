"""Tier 1: field_searchability critic tests."""

from __future__ import annotations

from lumi.critic import _check_field_searchability
from lumi.schemas import EnrichmentPlan


def _plan(dims=None, measures=None) -> EnrichmentPlan:
    return EnrichmentPlan(
        table_name="t1",
        proposed_dimensions=dims or [],
        proposed_measures=measures or [],
        reasoning=(
            "Cardmember dim at point-in-time grain. Adds dims and measures."
        ),
    )


def test_blocks_when_majority_of_fields_missing_hint():
    """4 of 5 dims missing hint → BLOCK."""
    dims = [
        {"name": "a", "source_column": "a", "type": "string", "label": "A",
         "description": "x", "tags": ["a"]},  # no hint
        {"name": "b", "source_column": "b", "type": "string", "label": "B",
         "description": "x", "tags": ["b"]},  # no hint
        {"name": "c", "source_column": "c", "type": "string", "label": "C",
         "description": "x", "tags": ["c"]},  # no hint
        {"name": "d", "source_column": "d", "type": "string", "label": "D",
         "description": "x", "tags": ["d"]},  # no hint
        {"name": "e", "source_column": "e", "type": "string", "label": "E",
         "description": "x", "hint": "alt names", "tags": ["e"]},  # has hint
    ]
    issues = _check_field_searchability(_plan(dims=dims))
    blocks = [i for i in issues if i.severity == "block"]
    assert blocks
    assert blocks[0].category == "field_searchability"


def test_no_block_when_all_fields_have_hint():
    dims = [
        {"name": "a", "source_column": "a", "type": "string", "label": "A",
         "description": "x", "hint": "alt names", "tags": ["a"]},
        {"name": "b", "source_column": "b", "type": "string", "label": "B",
         "description": "x", "hint": "alt names", "tags": ["b"]},
    ]
    issues = _check_field_searchability(_plan(dims=dims))
    assert not [i for i in issues if i.severity == "block"]


def test_warn_per_field_missing_hint():
    dims = [
        {"name": "a", "source_column": "a", "type": "string", "label": "A",
         "description": "x", "hint": "alt", "tags": ["a"]},
        {"name": "b", "source_column": "b", "type": "string", "label": "B",
         "description": "x", "tags": ["b"]},  # missing hint
    ]
    issues = _check_field_searchability(_plan(dims=dims))
    warns = [
        i for i in issues
        if i.severity == "warn" and "hint" in i.locus and "name=b" in i.locus
    ]
    assert warns


def test_warn_per_field_missing_tags():
    dims = [
        {"name": "a", "source_column": "a", "type": "string", "label": "A",
         "description": "x", "hint": "alt"},  # tags missing
    ]
    issues = _check_field_searchability(_plan(dims=dims))
    warns = [
        i for i in issues
        if i.severity == "warn" and "tags" in i.locus
    ]
    assert warns


def test_info_when_label_equals_column_name():
    dims = [
        {"name": "card_member_id", "source_column": "cm11",
         "type": "string", "label": "card_member_id",  # auto-gen-looking
         "description": "x", "hint": "alt", "tags": ["cm11"]},
    ]
    issues = _check_field_searchability(_plan(dims=dims))
    infos = [
        i for i in issues
        if i.severity == "info" and "label" in i.locus
    ]
    assert infos


def test_info_when_measure_description_lacks_aggregation_verb():
    measures = [
        {"name": "total_billed", "source_column": "billed", "type": "sum",
         "label": "Total Billed",
         "description": "Revenue figure for the reporting period.",
         "hint": "x", "tags": ["t"]},
    ]
    issues = _check_field_searchability(_plan(measures=measures))
    infos = [
        i for i in issues
        if i.severity == "info" and "description" in i.locus
    ]
    assert infos


def test_no_issues_for_clean_measure_with_aggregation_verb():
    measures = [
        {"name": "total_billed", "source_column": "billed", "type": "sum",
         "label": "Total Billed", "description": "Sum of billed amount in USD",
         "hint": "alt names", "tags": ["billings"]},
    ]
    issues = _check_field_searchability(_plan(measures=measures))
    # No block, no warn — possibly an info if label-name match.
    assert not [i for i in issues if i.severity in {"block", "warn"}]


def test_empty_plan_no_issues():
    issues = _check_field_searchability(_plan())
    assert issues == []
