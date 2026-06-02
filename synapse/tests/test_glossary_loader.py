"""Tests for the glossary loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from synapse.registry.glossary import (
    ambiguous_symbols,
    index_by_symbol,
    load_glossary,
)


def test_load_glossary_returns_typed_entries(glossary_csv: Path):
    entries = load_glossary(glossary_csv)
    assert len(entries) == 10  # rows in fixture
    e = entries[0]
    assert e.symbol == "CM"
    assert e.definition == "Cardmember"
    assert e.business_unit == "Finance"
    assert e.region == "US"
    assert e.entry_type == "Acronym"
    assert e.raw_row  # forward-compat field preserved


def test_load_glossary_missing_file_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_glossary(tmp_path / "does_not_exist.csv")


def test_load_glossary_skips_rows_missing_required(tmp_path: Path):
    """Rows lacking either symbol or definition should be silently skipped."""
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "Symbol,Definition\n"
        "VALID,A real entry\n"
        ",Missing symbol\n"
        "MISSING_DEF,\n",
    )
    entries = load_glossary(bad_csv)
    assert len(entries) == 1
    assert entries[0].symbol == "VALID"


def test_index_by_symbol_groups_correctly(glossary_csv: Path):
    entries = load_glossary(glossary_csv)
    grouped = index_by_symbol(entries)
    # 'cm' appears twice with different definitions
    assert "cm" in grouped
    assert len(grouped["cm"]) == 2
    defs = {e.definition for e in grouped["cm"]}
    assert "Cardmember" in defs
    assert "Communication Module" in defs


def test_ambiguous_symbols_detects_multi_def(glossary_csv: Path):
    entries = load_glossary(glossary_csv)
    ambig = ambiguous_symbols(entries)
    # 'cm' and 'aa' both have multiple definitions
    assert "cm" in ambig
    assert "aa" in ambig
    # Unique single-def entries are NOT in ambig
    assert "cmid" not in ambig
    assert "tbb" not in ambig
    # 'aa' has 3 distinct meanings
    assert len({e.definition for e in ambig["aa"]}) == 3


def test_glossary_loader_tolerates_column_aliases(tmp_path: Path):
    """Loader should match Symbol/Acronym/Abbreviation interchangeably."""
    alt_csv = tmp_path / "alt.csv"
    alt_csv.write_text(
        "Acronym,Meaning,BU,Geo\n"
        "PCL,Premium Card Loyalty,Loyalty,US\n",
    )
    entries = load_glossary(alt_csv)
    assert len(entries) == 1
    assert entries[0].symbol == "PCL"
    assert entries[0].definition == "Premium Card Loyalty"
    assert entries[0].business_unit == "Loyalty"
    assert entries[0].region == "US"
