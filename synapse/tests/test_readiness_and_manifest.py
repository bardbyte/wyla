"""Manifest parsing, FQN lineage retry, and the BQ readiness probe."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from synapse.loaders.mdm_crawler import crawl_mdm_for_table
from synapse.utils.manifest import read_tables_manifest

REPO_ROOT = Path(__file__).resolve().parents[2]
MDM_FIXTURES = Path(__file__).parent / "fixtures" / "mdm"


# ─── manifest ────────────────────────────────────────────────


def test_real_manifest_contains_skills_tables():
    tables = read_tables_manifest(
        REPO_ROOT / "semantic-graph" / "config" / "tables.yaml")
    by_name = {t["name"]: t for t in tables}
    assert {"sbs_new_accounts", "us_daily_rr_smry_data",
            "roll_rate_calc"} <= set(by_name)
    assert by_name["roll_rate_calc"]["bq_dataset"] == "common"
    assert by_name["sbs_new_accounts"]["bq_dataset"] == "dw"  # default


def test_manifest_tolerates_bare_strings(tmp_path):
    yaml_path = tmp_path / "t.yaml"
    yaml_path.write_text(
        "defaults:\n  bq_dataset: dw\n"
        "tables:\n  - plain_name\n  - name: dict_name\n"
        "    bq_dataset: other\n  - {}\n",
        encoding="utf-8")
    tables = read_tables_manifest(yaml_path)
    assert [t["name"] for t in tables] == ["plain_name", "dict_name"]
    assert tables[0]["bq_dataset"] == "dw"
    assert tables[1]["bq_dataset"] == "other"


# ─── FQN lineage retry ───────────────────────────────────────


def test_lineage_retries_with_dataset_qualified_name(tmp_path):
    result = crawl_mdm_for_table(
        "fqn_case", out_dir=tmp_path, base_url="", cache_dir=MDM_FIXTURES)
    report = result.metadata["fetch_report"]
    assert report["lineage_up"] == "cached"          # bare name: 200, empty
    assert report["lineage_up_fqn"] == "cached"      # retry fired
    blob = json.loads((tmp_path / "mdm_cache" / "fqn_case.json").read_text())
    assert blob["lineage_upstream"] == ["dw.raw_events"]
    assert result.metadata["n_upstream"] == 1


def test_no_fqn_retry_when_bare_name_had_lineage(tmp_path):
    result = crawl_mdm_for_table(
        "roll_rate_calc", out_dir=tmp_path,
        base_url="", cache_dir=MDM_FIXTURES)
    assert "lineage_up_fqn" not in result.metadata["fetch_report"]


# ─── BQ readiness probe (CLI, offline) ───────────────────────


def _make_extraction(root: Path) -> None:
    good = root / "good_table"
    good.mkdir(parents=True)
    (good / "1_1__columns.csv").write_text(
        "column_name,data_type,is_nullable,is_partitioning_column,"
        "clustering_ordinal_position\n"
        "rpt_month,DATE,NO,YES,\nbal,NUMERIC,YES,NO,\n", encoding="utf-8")
    (good / "1_3__table_meta.json").write_text(json.dumps(
        {"table_name": "good_table", "table_type": "VIEW",
         "ddl": "CREATE VIEW x AS SELECT 1"}), encoding="utf-8")
    (good / "3_1__cardinality_nulls.csv").write_text(
        "column_name,approx_distinct,null_fraction\nbal,42,0.1\n",
        encoding="utf-8")
    broken = root / "broken_table"
    broken.mkdir()
    (broken / "note.txt").write_text("no schema here", encoding="utf-8")


def test_readiness_probe_buckets_and_exit_code(tmp_path):
    _make_extraction(tmp_path)
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "probe_bq_readiness.py"),
         "--extract-dir", str(tmp_path)],
        capture_output=True, text=True,
    )
    assert proc.returncode == 1, proc.stdout + proc.stderr  # 1 broken table
    assert "good_table" in proc.stdout
    assert "partially_usable" in proc.stdout
    assert "genuinely_failed" in proc.stdout
    report = json.loads((tmp_path / "_readiness_report.json").read_text())
    assert report["buckets"] == {"fully_usable": 0, "partially_usable": 1,
                                 "genuinely_failed": 1}
    good = next(t for t in report["tables"] if t["table"] == "good_table")
    assert good["loader"]["status"] == "ok"          # real loader converted it
    assert good["n_columns"] == 2


def test_readiness_probe_flags_unextracted_manifest_tables(tmp_path):
    _make_extraction(tmp_path)
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "probe_bq_readiness.py"),
         "--extract-dir", str(tmp_path),
         "--tables-yaml",
         str(REPO_ROOT / "semantic-graph" / "config" / "tables.yaml")],
        capture_output=True, text=True,
    )
    assert "roll_rate_calc" in proc.stdout
    assert "not_extracted" in proc.stdout


# ─── bq_loader tolerates every meta JSON shape (crash repro) ──


def test_bq_loader_reads_pretty_printed_table_meta(tmp_path):
    """Repro of the laptop crash: bq_batch_extract writes 1_3__table_meta
    .json with json.dump(indent=2); the reader used to grab the first
    line ('{') and crash with JSONDecodeError."""
    from synapse.loaders.bq_loader import _read_json_first_row, load_bq_for_table

    # exactly what json.dump(..., indent=2) produces — starts with a bare '{'
    pretty = ('{\n  "table_name": "custins_customer_insights_cardmember",\n'
              '  "table_type": "VIEW",\n  "ddl": "CREATE VIEW x AS SELECT 1"\n}\n')
    p = tmp_path / "1_3__table_meta.json"
    p.write_text(pretty, encoding="utf-8")
    meta = _read_json_first_row(p)                 # must NOT raise
    assert meta["table_type"] == "VIEW"

    # array form and JSONL form both still work
    (tmp_path / "arr.json").write_text('[{"table_type": "BASE TABLE"}]')
    assert _read_json_first_row(tmp_path / "arr.json")["table_type"] == "BASE TABLE"
    (tmp_path / "jsonl.json").write_text(
        '{"table_type": "VIEW"}\n{"table_type": "IGNORED"}\n')
    assert _read_json_first_row(tmp_path / "jsonl.json")["table_type"] == "VIEW"

    # end-to-end: a cardmember-shaped folder now loads without crashing
    tdir = tmp_path / "custins_customer_insights_cardmember"
    tdir.mkdir()
    (tdir / "1_1__columns.csv").write_text(
        "column_name,data_type,is_nullable,is_partitioning_column,"
        "clustering_ordinal_position\ncm11,STRING,NO,NO,\n", encoding="utf-8")
    (tdir / "1_3__table_meta.json").write_text(pretty, encoding="utf-8")
    result = load_bq_for_table(
        "custins_customer_insights_cardmember",
        source_dir=tmp_path, out_dir=tmp_path / "out")
    assert result.status in ("ok", "partial")
    blob = json.loads(
        (tmp_path / "out" / "bq_cache"
         / "custins_customer_insights_cardmember.json").read_text())
    assert blob["asset_kind"] == "View"
