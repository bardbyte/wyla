"""Gold-SQL corpus loader — sqlglot signals from plain .sql folders."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from synapse.loaders.gold_sql_loader import load_gold_sql_corpus

pytest.importorskip("sqlglot")


CORPUS = {
    "Q01.sql": """
        SELECT DATE_TRUNC(decision_dt, MONTH) AS m,
               COUNT(DISTINCT na_pcn_no) AS apps
        FROM demo_ds.sbs_new_accounts
        WHERE decision_cd = 'A' AND bus_seg = 'Consumer'
        GROUP BY m
    """,
    "Q02.sql": """
        WITH a AS (
          SELECT acct_id, cust_id, bal FROM demo_ds.accounts
          WHERE rpt_dt = DATE('2026-01-01')
        ),
        c AS (
          SELECT cust_id, fico_score FROM demo_ds.customers
        )
        SELECT a.acct_id,
               CASE WHEN c.status_cd = 'A' THEN 'Active'
                    WHEN c.status_cd = 'C' THEN 'Cancelled'
                    ELSE 'Other' END AS status_band,
               SUM(a.bal) AS total_bal
        FROM a LEFT JOIN c ON a.cust_id = c.cust_id
        GROUP BY 1, 2
    """,
}


@pytest.fixture
def corpus_dir(tmp_path: Path) -> Path:
    src = tmp_path / "sqls"
    src.mkdir()
    for name, sql in CORPUS.items():
        (src / name).write_text(sql, encoding="utf-8")
    return src


def test_parses_all_and_writes_canonical_layout(corpus_dir, tmp_path):
    out = tmp_path / "out"
    result = load_gold_sql_corpus(corpus_dir, out_dir=out)
    assert result.status == "ok"
    assert result.records_count == 2
    assert (out / "lumi_signals").is_dir()
    assert (out / "gold_queries" / "Q01.sql").exists()
    assert set(result.metadata["tables_discovered"]) == {
        "demo_ds.sbs_new_accounts", "demo_ds.accounts", "demo_ds.customers",
    }


def test_signal_extraction(corpus_dir, tmp_path):
    out = tmp_path / "out"
    load_gold_sql_corpus(corpus_dir, out_dir=out)
    na = json.loads(
        (out / "lumi_signals" / "demo_ds__sbs_new_accounts.json").read_text())
    assert {"function": "COUNT", "column": "na_pcn_no",
            "alias": "apps", "query_id": "Q01"} in na["aggregations"]
    filter_cols = {f["column"] for f in na["filters"]}
    assert {"decision_cd", "bus_seg"} <= filter_cols
    assert {"column": "decision_dt", "granularity": "MONTH"} \
        in na["date_functions"]
    assert "na_pcn_no" in na["columns_referenced"]


def test_cte_joins_resolve_to_underlying_tables(corpus_dir, tmp_path):
    out = tmp_path / "out"
    load_gold_sql_corpus(corpus_dir, out_dir=out)
    accounts = json.loads(
        (out / "lumi_signals" / "demo_ds__accounts.json").read_text())
    joins = accounts["joins"]
    assert joins, "CTE-wrapped join must surface on the real table"
    assert joins[0]["other_table"] == "demo_ds.customers"
    assert joins[0]["left_column"] == "cust_id"
    assert joins[0]["join_type"] == "LEFT"
    # case-when code mapping mined from the joined query
    customers = json.loads(
        (out / "lumi_signals" / "demo_ds__customers.json").read_text())
    mappings = {(cw["raw_value"], cw["human_meaning"])
                for cw in customers["case_whens"]
                if cw["column"] == "status_cd"}
    assert {("A", "Active"), ("C", "Cancelled")} <= mappings


def test_unparseable_sql_is_partial_not_fatal(tmp_path):
    src = tmp_path / "sqls"
    src.mkdir()
    (src / "good.sql").write_text("SELECT a FROM demo_ds.t1", encoding="utf-8")
    (src / "bad.sql").write_text("SELEC oops FROM FROM", encoding="utf-8")
    result = load_gold_sql_corpus(src, out_dir=tmp_path / "out")
    assert result.status == "partial"
    assert result.records_count == 1
    assert result.warnings and "bad.sql" in result.warnings[0]


def test_empty_dir_skipped(tmp_path):
    src = tmp_path / "none"
    src.mkdir()
    assert load_gold_sql_corpus(src, out_dir=tmp_path).status == "skipped"
