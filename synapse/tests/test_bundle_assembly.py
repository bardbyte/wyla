"""Tests for evidence-bundle assembly.

These run against the fixture CSVs only (no MDM, no SQL corpus) — the
corpus and MDM paths are tested separately where the dependencies are
available."""

from __future__ import annotations

from pathlib import Path

import pytest

from synapse.curation.bundle import assemble_evidence_bundle, load_mdm_digests


def test_assemble_with_fixture_csvs_only(
    tmp_path: Path,
    glossary_csv: Path,
    metric_catalog_csv: Path,
    table_catalog_csv: Path,
):
    """Bundle assembles cleanly when MDM + corpus dirs don't exist."""
    bundle = assemble_evidence_bundle(
        glossary_path=glossary_csv,
        metric_catalog_path=metric_catalog_csv,
        table_catalog_path=table_catalog_csv,
        mdm_cache_dir=tmp_path / "no_mdm",   # missing on purpose
        sql_corpus_dir=tmp_path / "no_sql",  # missing on purpose
        scope_description="bundle test",
    )
    assert bundle.scope_description == "bundle test"
    assert len(bundle.glossary) > 0
    assert len(bundle.metric_catalog) > 0
    assert len(bundle.table_catalog) > 0
    assert bundle.mdm_digests == []
    assert bundle.corpus_signals == []
    assert bundle.n_queries_analyzed == 0


def test_assemble_missing_required_csv_raises(
    tmp_path: Path,
    metric_catalog_csv: Path,
    table_catalog_csv: Path,
):
    with pytest.raises(FileNotFoundError):
        assemble_evidence_bundle(
            glossary_path=tmp_path / "missing_glossary.csv",
            metric_catalog_path=metric_catalog_csv,
            table_catalog_path=table_catalog_csv,
            mdm_cache_dir=tmp_path,
            sql_corpus_dir=tmp_path,
        )


def test_load_mdm_digests_handles_missing_dir(tmp_path: Path):
    """Returns empty list, doesn't raise."""
    out = load_mdm_digests(tmp_path / "nope")
    assert out == []


def test_load_mdm_digests_distills_minimal_shape(tmp_path: Path):
    """Verifies the digest projection respects key fields."""
    import json
    cache = tmp_path / "mdm_cache"
    cache.mkdir()
    (cache / "demo_table.json").write_text(json.dumps({
        "table_name": "demo_table",
        "table_business_name": "Demo Table",
        "table_description": "A demo table for unit tests",
        "data_category": "Demo",
        "data_sub_category": "Unit",
        "bq_project": "p", "bq_dataset": "d", "bq_table": "demo_table",
        "columns": [
            {"name": "id", "business_name": "ID", "description": "Primary key",
             "type": "STRING", "is_primary": True},
            {"name": "ssn", "business_name": "SSN", "description": "Sensitive",
             "type": "STRING", "is_pii": True, "pii_role_id": "PII_42"},
            {"name": "name", "business_name": "Name", "description": "Display",
             "type": "STRING"},
        ],
    }))
    out = load_mdm_digests(cache)
    assert len(out) == 1
    d = out[0]
    assert d.table_name == "demo_table"
    assert d.bq_fqn == "p.d.demo_table"
    assert d.n_columns == 3
    assert len(d.key_columns) == 1
    assert d.key_columns[0]["name"] == "id"
    assert d.pii_columns == ["ssn"]
    assert {c["name"] for c in d.sample_columns} == {"id", "ssn", "name"}


def test_load_mdm_digests_skips_malformed_files(tmp_path: Path):
    cache = tmp_path / "mdm_cache"
    cache.mkdir()
    (cache / "bad_json.json").write_text("{not valid json")
    (cache / "non_dict.json").write_text('"a string"')
    (cache / "good.json").write_text('{"table_name":"good","columns":[]}')
    out = load_mdm_digests(cache)
    names = {d.table_name for d in out}
    assert names == {"good"}


def test_load_mdm_digests_filters_by_table_names(tmp_path: Path):
    cache = tmp_path / "mdm_cache"
    cache.mkdir()
    for name in ("a", "b", "c"):
        (cache / f"{name}.json").write_text(
            f'{{"table_name":"{name}","columns":[]}}',
        )
    out = load_mdm_digests(cache, table_names=["a", "c"])
    assert {d.table_name for d in out} == {"a", "c"}
