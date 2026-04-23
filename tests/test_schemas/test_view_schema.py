from __future__ import annotations

import pytest
from pydantic import ValidationError

from lumi.schemas import EnrichedField, EnrichedView, ParsedField, ParsedView


def test_parsed_view_field_count_and_lookup() -> None:
    v = ParsedView(
        view_name="v",
        source_path="x.view.lkml",
        fields=[
            ParsedField(name="a", kind="dimension"),
            ParsedField(name="b", kind="measure"),
        ],
    )
    assert v.field_count == 2
    assert v.field_by_name("a") is not None
    assert v.field_by_name("missing") is None


def test_enriched_view_requires_fields() -> None:
    with pytest.raises(ValidationError):
        EnrichedView(
            view_name="v",
            view_label="V",
            view_description="desc",
            fields=[],
        )


def test_enriched_field_requires_non_empty_description() -> None:
    with pytest.raises(ValidationError):
        EnrichedField(
            name="f",
            kind="dimension",
            label="F",
            description="",
            origin="gold_query",
        )
