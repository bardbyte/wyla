"""Tests for the metric catalog loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from synapse.registry.metric_catalog import index_by_domain, load_metric_catalog


def test_load_metric_catalog_returns_typed_entries(metric_catalog_csv: Path):
    entries = load_metric_catalog(metric_catalog_csv)
    assert len(entries) == 4
    e = entries[0]
    assert e.technical_name == "total_billed_business"
    assert e.business_name == "Total Billed Business"
    assert "SUM" in (e.calculation_logic or "")
    assert e.primary_data_product == "cornerstone_metrics"
    assert e.associated_domain == "Finance"
    assert e.metric_grain == "aggregated"
    # business_synonyms is semicolon-separated in the fixture
    assert "TBB" in e.business_synonyms
    assert "Total BB" in e.business_synonyms


def test_load_metric_catalog_missing_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_metric_catalog(tmp_path / "nope.csv")


def test_load_metric_catalog_handles_pipe_and_semicolon_separators(tmp_path: Path):
    csv_path = tmp_path / "mc.csv"
    csv_path.write_text(
        "technical_name,business_synonyms\n"
        "metric_a,Foo|Bar|Baz\n"
        "metric_b,X;Y;Z\n"
        'metric_c,"P,Q,R"\n',  # comma INSIDE cell must be quoted in CSV
    )
    entries = load_metric_catalog(csv_path)
    assert len(entries) == 3
    assert entries[0].business_synonyms == ["Foo", "Bar", "Baz"]
    assert entries[1].business_synonyms == ["X", "Y", "Z"]
    # Quoted comma cell — _split_csv falls back to comma-split when no
    # pipe/semicolon present
    assert entries[2].business_synonyms == ["P", "Q", "R"]


def test_load_metric_catalog_skips_rows_with_no_name(tmp_path: Path):
    csv_path = tmp_path / "blank.csv"
    csv_path.write_text(
        "technical_name,business_name\n"
        "ok_metric,OK\n"
        ",only_business\n"
        ",\n",
    )
    entries = load_metric_catalog(csv_path)
    # ok_metric kept; only_business kept (business name acts as tech);
    # blank-blank dropped
    assert len(entries) == 2


def test_index_by_domain(metric_catalog_csv: Path):
    entries = load_metric_catalog(metric_catalog_csv)
    grouped = index_by_domain(entries)
    assert "finance" in grouped
    assert "risk" in grouped
    assert len(grouped["finance"]) >= 1
