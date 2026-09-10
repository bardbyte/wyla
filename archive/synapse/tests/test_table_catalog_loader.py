"""Tests for the table catalog loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from synapse.registry.table_catalog import (
    in_scope,
    index_by_domain,
    load_table_catalog,
)


def test_load_table_catalog_returns_typed_entries(table_catalog_csv: Path):
    entries = load_table_catalog(table_catalog_csv)
    assert len(entries) == 10
    in_dmp = [t for t in entries if t.is_in_dmp]
    assert len(in_dmp) == 8  # per fixture


def test_in_scope_filters_to_dmp_only(table_catalog_csv: Path):
    entries = load_table_catalog(table_catalog_csv)
    dmp_only = in_scope(entries, require_dmp=True)
    assert all(t.is_in_dmp for t in dmp_only)
    assert len(dmp_only) == 8


def test_in_scope_filters_by_company_domain(table_catalog_csv: Path):
    entries = load_table_catalog(table_catalog_csv)
    finance = in_scope(
        entries, require_dmp=True, company_domains={"Finance"},
    )
    assert all(t.company_domain == "Finance" for t in finance)
    assert len(finance) == 4  # custins_*_cardmember, *_product, fin_*, pmdl_*


def test_in_scope_case_insensitive_domain(table_catalog_csv: Path):
    entries = load_table_catalog(table_catalog_csv)
    finance_lower = in_scope(
        entries, require_dmp=True, company_domains={"finance"},
    )
    finance_upper = in_scope(
        entries, require_dmp=True, company_domains={"FINANCE"},
    )
    assert len(finance_lower) == len(finance_upper) == 4


def test_normalize_not_found_to_none(table_catalog_csv: Path):
    entries = load_table_catalog(table_catalog_csv)
    crt = next(t for t in entries if t.table_name == "crt_currency")
    # 'Not Found' literal should normalize to None
    assert crt.company_domain is None
    assert crt.data_domain is None
    assert crt.is_in_dmp is False


def test_load_missing_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_table_catalog(tmp_path / "missing.csv")


def test_index_by_domain(table_catalog_csv: Path):
    entries = load_table_catalog(table_catalog_csv)
    grouped = index_by_domain(entries)
    assert "Finance" in grouped
    assert "Risk" in grouped
    assert len(grouped["Finance"]) >= 4


def test_parse_bool_variants(tmp_path: Path):
    csv_path = tmp_path / "bool.csv"
    csv_path.write_text(
        "table_name,IS IN DMP\n"
        "a,Yes\n"
        "b,No\n"
        "c,YES\n"
        "d,y\n"
        "e,true\n"
        "f,1\n"
        "g,\n",
    )
    entries = load_table_catalog(csv_path)
    name_to_dmp = {e.table_name: e.is_in_dmp for e in entries}
    assert name_to_dmp == {
        "a": True, "b": False, "c": True, "d": True,
        "e": True, "f": True, "g": False,
    }
