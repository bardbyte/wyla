"""Taught warehouse errors: a BigQuery failure comes back classified
— the model's to fix (sql, cost) with the closest real names, or
configuration to report (environment, access) with the exact .env
change. Pure: the build, the SQL, and the message in; a dict out."""

from __future__ import annotations

import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.tools.api import Build                              # noqa: E402
from sahs.tools.warehouse_errors import (KINDS,               # noqa: E402
                                         teach_warehouse_error)


class Stub:
    schema = {"dw.gms_transaction": {"part_dt": {}, "trans_usd_am": {},
                                     "country_cd": {}, "merchant_id": {}},
              "dw.wwcas_authorization": {"approval_cd": {},
                                         "part_dt": {}}}
    physical_of = Build.physical_of
    short_table = Build.short_table


SQL = ("SELECT country_cd, sum(trans_usd_amt) AS spend "
       "FROM dw.gms_transaction GROUP BY country_cd")


def teach(message, sql=SQL, **kw):
    return teach_warehouse_error(Stub(), sql, message, **kw)


def test_a_known_table_in_the_wrong_project_is_configuration():
    got = teach("Not found: Table prj-p-lumi-gpt:dw.gms_transaction was "
                "not found in location US", data_project="", location="US")
    assert got["kind"] == "environment" and got["yours_to_fix"] is False
    assert "prj-p-lumi-gpt" in got["hint"] and "not your SQL" in got["hint"]
    assert "LUMI_BQ_DATA_PROJECT" in got["hint"]
    assert got["fix_env"]["BQ_LOCATION"] == "US"
    assert got["smoke"] == "python scripts/bq_check.py --table dw.gms_transaction"
    assert "Do not retry" in got["hint"]
    # a dataset the build uses, missing → the same verdict
    got = teach("Not found: Dataset prj-p-lumi-gpt:dw was not found in "
                "location US")
    assert got["kind"] == "environment" and got["yours_to_fix"] is False


def test_an_unknown_table_or_dataset_is_the_models_slip():
    got = teach("Not found: Table prj-p-lumi-gpt:dw.gms_transactions was "
                "not found in location US")
    assert got["kind"] == "sql" and got["yours_to_fix"] is True
    assert got["closest"] == ["dw.gms_transaction"]
    assert "did you mean dw.gms_transaction" in got["hint"]
    got = teach("Not found: Dataset prj-p-lumi-gpt:warehouse")
    assert got["kind"] == "sql" and "dw" in got["closest"]


def test_an_unrecognized_column_names_the_closest_real_ones():
    got = teach("Unrecognized name: trans_usd_amt at [1:26]")
    assert got["kind"] == "sql" and got["yours_to_fix"] is True
    assert got["closest"] == ["trans_usd_am"]
    assert "trans_usd_am (dw.gms_transaction)" in got["hint"]
    assert "dw.gms_transaction" in got["hint"]
    # BigQuery's own suggestion, when it offers one, comes first
    got = teach("Unrecognized name: cntry; Did you mean country_cd? "
                "at [1:8]")
    assert got["closest"][0] == "country_cd"
    got = teach("Name approval_cdx not found inside w at [1:8]",
                sql="SELECT w.approval_cdx FROM dw.wwcas_authorization w")
    assert got["kind"] == "sql" and got["closest"] == ["approval_cd"]


def test_partition_filter_and_types_are_the_models_to_fix():
    got = teach("Cannot query over table 'axp-lumi.dw.gms_transaction' "
                "without a filter over column(s) 'part_dt' that can be "
                "used for partition elimination")
    assert got["kind"] == "sql" and got["closest"] == ["part_dt"]
    assert "WHERE part_dt BETWEEN" in got["hint"]
    got = teach("No matching signature for operator = for argument "
                "types: STRING, INT64. Supported signature: ANY = ANY "
                "at [1:78]")
    assert got["kind"] == "sql" and "type mismatch calling =" in got["hint"]
    assert "CAST" in got["hint"]


def test_a_syntax_error_shows_the_spot():
    got = teach("Syntax error: Expected end of input but got keyword "
                "GROUP at [1:52]")
    assert got["kind"] == "sql"
    assert "line 1: SELECT country_cd" in got["hint"]
    assert "^" in got["hint"]
    caret_line = [ln for ln in got["hint"].splitlines() if "^" in ln][0]
    assert caret_line.index("^") - len("        ") == 51    # column 52


def test_access_quota_transport_are_not_the_models():
    got = teach("Access Denied: Table axp-lumi:dw.gms_transaction: User "
                "does not have permission to query table")
    assert got["kind"] == "access" and got["yours_to_fix"] is False
    assert got["tables"] == ["dw.gms_transaction"]
    assert "bigquery.tables.getData" in got["hint"]
    got = teach("Quota exceeded: Your project exceeded quota for free "
                "query bytes scanned")
    assert got["kind"] == "environment" and got["yours_to_fix"] is False
    got = teach("transport: <urlopen error [Errno 111] Connection refused>")
    assert got["kind"] == "environment" and got["yours_to_fix"] is False
    assert got["smoke"] == "python scripts/bq_check.py"
    got = teach("Query exceeded limit for bytes billed: 1000000")
    assert got["kind"] == "cost" and got["yours_to_fix"] is True


def test_the_unknown_still_teaches_and_the_kinds_are_pinned():
    got = teach("Something odd happened at [1:3]")
    assert got["kind"] == "unknown" and got["yours_to_fix"] is True
    assert "configuration" in got["hint"] and "line 1:" in got["hint"]
    assert KINDS == ("sql", "cost", "environment", "access", "unknown")
    assert got["source"] == "warehouse"
