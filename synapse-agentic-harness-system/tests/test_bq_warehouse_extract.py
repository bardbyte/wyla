"""The warehouse extractor against a fake BigQuery: the archive layout
the loader consumes, the status semantics, the budget-planned profile,
the three-source history and its local indexer, resumability, and the
probe. No network anywhere."""

from __future__ import annotations

import csv
import datetime as _dt
import gzip
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.extract import bq_sql as S                          # noqa: E402
from sahs.extract.bq_rest import (                            # noqa: E402
    BQError, Budget, DENIED, EMPTY, FETCHED, Ledger, NOT_FOUND, QueryResult,
    classify, decode_rows)
from sahs.extract.config import ConfigError, load_config      # noqa: E402
from sahs.extract.history import (                            # noqa: E402
    fingerprint, match_tables, normalize_record, normalize_sql)
from sahs.extract.warehouse import (                          # noqa: E402
    WarehouseExtractor, _partition_bounds)

GIB = 1 << 30
NOW = _dt.datetime(2026, 8, 23, 14, 30, tzinfo=_dt.timezone.utc)
SA = "svc-p-lumi-gpt-hyd@prj-p-lumi-gpt.iam.gserviceaccount.com"

CONFIG = """
version: 2
defaults:
  data_project: axp-lumi
  logical_dataset: dw
  physical_dataset: data
  billing_project: prj-p-lumi-gpt
  region: us
  job_projects: [axp-lumi, prj-p-lumi-gpt]
  scan_organization_jobs: true
  audit_log_table: axp-lumi.data_backup.cloudaudit_googleapis_com_data_access
  history_days: 3
  concurrency: 2
  history_concurrency: 2
  run_budget: 2 TiB
  profile_budget: 200 GiB
  domain_budget: 100 GiB
  history_day_budget: 50 GiB
  profile_chunk_columns: 3
  domain_group_columns: 2
  low_card_threshold: 10
tables:
  - name: gms_transaction
  - name: risk_pers_acct
"""


# ── the fake warehouse ──

def _col(table, name, pos, dtype, part="NO"):
    return {"table_catalog": "axp-lumi", "table_schema": "dw",
            "table_name": table, "column_name": name,
            "ordinal_position": pos, "is_nullable": "YES",
            "data_type": dtype, "is_partitioning_column": part,
            "clustering_ordinal_position": None}


GMS_COLUMNS = [_col("gms_transaction", "part_dt", 1, "DATE", "YES"),
               _col("gms_transaction", "country_cd", 2, "STRING"),
               _col("gms_transaction", "trans_usd_am", 3, "FLOAT64"),
               _col("gms_transaction", "se_no", 4, "STRING"),
               _col("gms_transaction", "payment_detail", 5,
                    "STRUCT<card STRUCT<network STRING>>")]
RISK_COLUMNS = [_col("risk_pers_acct", "acct_id", 1, "STRING"),
                _col("risk_pers_acct", "acct_status_cd", 2, "STRING")]
PARTITIONS = [{"table_catalog": "axp-lumi", "table_schema": "data",
               "table_name": "gms_transaction", "partition_id": pid,
               "total_rows": 500, "total_logical_bytes": 100 * GIB,
               "last_modified_time": "2026-08-22T00:00:00Z"}
              for pid in ("20260820", "20260821", "20260822")]


def _job(job_id, user, day, query, tables, project="axp-lumi", failed=False,
         billed=400_000_000):
    minute = int(job_id[-1])
    job_id = f"{job_id}_{day.replace('-', '')}"      # unique per day
    return {"job_id": job_id, "project_id": project, "user_email": user,
            "creation_time": f"{day}T10:{minute:02d}:00Z",
            "start_time": None, "end_time": f"{day}T10:{minute:02d}:05Z",
            "job_type": "QUERY", "statement_type": "SELECT",
            "state": "DONE", "priority": "INTERACTIVE", "cache_hit": False,
            "error_reason": "invalidQuery" if failed else None,
            "error_message": "Unrecognized name: contry_cd" if failed else None,
            "total_bytes_processed": billed, "total_bytes_billed": billed,
            "total_slot_ms": 1200, "destination_table": None,
            "referenced_tables": [{"project_id": "axp-lumi",
                                   "dataset_id": d, "table_id": t}
                                  for d, t in tables],
            "labels": [], "parent_job_id": None, "query": query}


class FakeBQ:
    """Routes statements on their text — enough BigQuery to prove the
    extractor's decisions, none of its cost."""

    project = "prj-p-lumi-gpt"

    def __init__(self, *, org_denied=True, audit=True,
                 refuse_not_null=False) -> None:
        self.connection = SimpleNamespace(key_path=None)
        self.ledger = Ledger()
        self.calls: list[tuple[str, str]] = []
        self.org_denied = org_denied
        self.audit = audit
        # BigQuery accepts `part_dt IS NOT NULL` as a partition filter;
        # this flag models a stricter policy (or a view that hides the
        # column) to prove the inventory-priced fallback
        self.refuse_not_null = refuse_not_null
        self._job = 0

    # -- resources --
    def get_dataset(self, project, dataset):
        if dataset in ("dw", "data", "data_backup"):
            return {"datasetReference": {"datasetId": dataset},
                    "location": "US"}
        raise BQError(NOT_FOUND, f"Not found: Dataset {project}:{dataset}",
                      code=404, reason="notFound")

    def get_table(self, project, dataset, table):
        key = f"{dataset}.{table}"
        if key == "dw.gms_transaction":
            return {"tableReference": {"projectId": project,
                                       "datasetId": dataset,
                                       "tableId": table},
                    "type": "VIEW", "description": "merchant transactions"}
        if key == "data.gms_transaction":
            return {"tableReference": {"projectId": project,
                                       "datasetId": dataset,
                                       "tableId": table},
                    "type": "TABLE", "numRows": "1500",
                    "numBytes": str(300 * GIB),
                    "timePartitioning": {"type": "DAY", "field": "part_dt"},
                    "requirePartitionFilter": True,
                    "lastModifiedTime": "1755820800000"}
        if key == "dw.risk_pers_acct":
            return {"tableReference": {"projectId": project,
                                       "datasetId": dataset,
                                       "tableId": table}, "type": "VIEW"}
        if key == "data.risk_pers_acct_hist":
            return {"tableReference": {"projectId": project,
                                       "datasetId": dataset,
                                       "tableId": table}, "type": "TABLE",
                    "numRows": "42", "numBytes": str(GIB)}
        if key == "data_backup.cloudaudit_googleapis_com_data_access":
            if not self.audit:
                raise BQError(DENIED, "Access Denied: Table", code=403,
                              reason="accessDenied")
            return {"tableReference": {"tableId": table},
                    "type": "TABLE",
                    "timePartitioning": {"type": "DAY", "field": "timestamp"},
                    "schema": {"fields": [
                        {"name": "timestamp", "type": "TIMESTAMP"},
                        {"name": "protopayload_auditlog", "type": "RECORD",
                         "fields": [{"name": "metadataJson",
                                     "type": "STRING"}]}]}}
        raise BQError(NOT_FOUND, f"Not found: Table {project}:{key}",
                      code=404, reason="notFound")

    def list_row_access_policies(self, project, dataset, table):
        if table == "risk_pers_acct_hist":
            raise BQError(DENIED, "Permission bigquery.rowAccessPolicies.list "
                          "denied", code=403, reason="accessDenied")
        return []

    def test_iam_permissions(self, project, dataset, table, permissions):
        return [p for p in permissions if "getData" in p or "tables.get" == p]

    # -- statements --
    def _bytes_for(self, sql: str) -> int:
        if "INFORMATION_SCHEMA" in sql or "get_table_metrics" in sql:
            return 0
        if "cloudaudit" in sql:
            return 3 * GIB
        if "data`.`gms_transaction`" in sql or "dw`.`gms_transaction`" in sql:
            narrow = "AS column_name" in sql          # value-domain branches
            full = 6 * GIB if narrow else 300 * GIB
            if "TABLESAMPLE" in sql:
                pct = float(re.search(r"\(([\d.]+) PERCENT\)", sql).group(1))
                return int(full * pct / 100)
            m = re.search(r"part_dt` >= DATE\('(\S+)'\) AND `part_dt` <= "
                          r"DATE\('(\S+)'\)", sql)
            if m:
                d0 = _dt.date.fromisoformat(m.group(1))
                d1 = _dt.date.fromisoformat(m.group(2))
                return int(full / 3 * ((d1 - d0).days + 1))
            if "IS NOT NULL" in sql and self.refuse_not_null:
                if "data`.`gms_transaction`" in sql:
                    raise BQError("INVALID", "Cannot query over table "
                                  "'axp-lumi.data.gms_transaction' without a "
                                  "filter over column(s) 'part_dt' that can "
                                  "be used for partition elimination",
                                  reason="invalidQuery")
            return full
        if "risk_pers_acct" in sql:
            return GIB
        return 10_000_000

    def dry_run(self, sql, params=None, *, scope="", operation=""):
        self.calls.append(("dry_run", operation))
        if "dw`.`risk_pers_acct`" in sql and "LIMIT 1" in sql:
            return QueryResult(rows=[], schema={}, dry_run=True,
                               bytes_processed=GIB, referenced_tables=[
                                   {"projectId": "axp-lumi", "datasetId": "dw",
                                    "tableId": "risk_pers_acct"},
                                   {"projectId": "axp-lumi",
                                    "datasetId": "data",
                                    "tableId": "risk_pers_acct_hist"}])
        return QueryResult(rows=[], schema={}, dry_run=True,
                           bytes_processed=self._bytes_for(sql))

    def query(self, sql, params=None, *, scope="", operation="",
              timeout_s=0, max_bytes_billed=None, use_cache=True,
              labels=None, page_size=0):
        self.calls.append(("query", operation))
        self._job += 1
        job_id = f"job_{self._job:04d}"
        p = {x["name"]: x for x in (params or [])}
        tables = [v["value"] for v in p.get("tables", {}).get(
            "parameterValue", {}).get("arrayValues", [])]
        rows = self._route(sql, p, tables)
        billed = self._bytes_for(sql) if "INFORMATION_SCHEMA" not in sql \
            else 0
        return QueryResult(rows=rows, schema={}, job_id=job_id,
                           bytes_processed=billed, bytes_billed=billed)

    def _route(self, sql, p, tables):                 # noqa: C901
        if sql.strip() == "SELECT 1 AS ok":
            return [{"ok": 1}]
        if "INFORMATION_SCHEMA.TABLES" in sql:
            if "`dw`" in sql:
                return [{"table_catalog": "axp-lumi", "table_schema": "dw",
                         "table_name": t, "table_type": "VIEW",
                         "creation_time": "2025-01-01T00:00:00Z",
                         "ddl": f"CREATE VIEW dw.{t} AS SELECT * FROM data.{t}"}
                        for t in tables if t in ("gms_transaction",
                                                 "risk_pers_acct")]
            return [{"table_catalog": "axp-lumi", "table_schema": "data",
                     "table_name": "gms_transaction", "table_type": "BASE TABLE",
                     "ddl": "CREATE TABLE data.gms_transaction (...)"}]
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            rows = []
            if "gms_transaction" in tables:
                rows += GMS_COLUMNS
            if "risk_pers_acct" in tables and "`dw`" in sql:
                rows += RISK_COLUMNS
            if "`data`" in sql:
                rows = [dict(r, table_schema="data") for r in rows
                        if r["table_name"] == "gms_transaction"]
            return rows
        if "COLUMN_FIELD_PATHS" in sql:
            return [{"table_name": "gms_transaction", "column_name":
                     "payment_detail", "field_path":
                     "payment_detail.card.network", "data_type": "STRING",
                     "description": "Card network"}]
        if "TABLE_OPTIONS" in sql:
            return [{"table_name": "gms_transaction", "option_name":
                     "labels", "option_type": "ARRAY<STRUCT<STRING, STRING>>",
                     "option_value": '[STRUCT("env", "prod")]'}]
        if "INFORMATION_SCHEMA.VIEWS" in sql:
            return [{"table_name": t, "view_definition":
                     f"SELECT * FROM `axp-lumi.data.{t}` WHERE oncop(1)",
                     "use_standard_sql": "YES"}
                    for t in tables if t in ("gms_transaction",
                                             "risk_pers_acct")]
        if "INFORMATION_SCHEMA.PARTITIONS" in sql:
            return list(PARTITIONS) if "`data`" in sql else []
        if "TABLE_CONSTRAINTS" in sql:
            return [{"table_name": "gms_transaction", "constraint_name":
                     "pk", "constraint_type": "PRIMARY KEY",
                     "column_name": "se_no", "ordinal_position": 1}] \
                if "`data`" in sql else []
        if "INFORMATION_SCHEMA.ROUTINES" in sql:
            return [{"routine_name": "get_table_metrics",
                     "routine_type": "TABLE FUNCTION"}]
        if "TABLE_STORAGE" in sql:
            raise BQError(DENIED, "Access Denied: TABLE_STORAGE", code=403,
                          reason="accessDenied")
        if "get_table_metrics" in sql:
            if "gms_transaction" in sql:
                return [{"table_name": "gms_transaction", "total_rows": 1500,
                         "table_size_bytes": 300 * GIB}]
            return []
        if "JOBS_BY_ORGANIZATION" in sql:
            if self.org_denied:
                raise BQError(DENIED, "Access Denied: bigquery.jobs.listAll "
                              "on organization", code=403,
                              reason="accessDenied")
            return []
        if "JOBS_BY_PROJECT" in sql:
            day = p["day_start"]["parameterValue"]["value"][:10]
            if "`prj-p-lumi-gpt`" in sql:
                return [_job("s1", SA, day, "SELECT COUNT(*) FROM "
                             "`axp-lumi.dw.gms_transaction`",
                             [("dw", "gms_transaction"),
                              ("data", "gms_transaction")],
                             project="prj-p-lumi-gpt")]
            return [
                _job("a1", "analyst1@corp", day,
                     "SELECT SUM(trans_usd_am) FROM dw.gms_transaction WHERE "
                     "part_dt = '2026-08-20' AND country_cd = 'US'",
                     [("dw", "gms_transaction"), ("data", "gms_transaction")]),
                _job("a2", "analyst2@corp", day,
                     "SELECT SUM(trans_usd_am) FROM dw.gms_transaction WHERE "
                     "part_dt = '2026-08-19' AND country_cd = 'GB'",
                     [("dw", "gms_transaction"), ("data", "gms_transaction")]),
                _job("a3", "analyst1@corp", day,
                     "SELECT a.acct_id FROM dw.risk_pers_acct a JOIN "
                     "dw.gms_transaction t ON a.se_no = t.se_no",
                     [("dw", "risk_pers_acct"), ("dw", "gms_transaction")]),
                _job("a4", "analyst3@corp", day,
                     "SELECT contry_cd FROM dw.gms_transaction",
                     [], failed=True, billed=0),
                _job("a5", SA, day, "SELECT COUNT(*) FROM "
                     "`axp-lumi.data.gms_transaction`",
                     [("data", "gms_transaction")]),
            ]
        if "cloudaudit" in sql:
            day = p["day_start"]["parameterValue"]["value"][:10]
            return [{"log_time": f"{day}T11:00:00Z",
                     "user_email": "looker-sa@other-project.iam",
                     "method_name": "google.cloud.bigquery.v2.JobService.Query",
                     "job_name": f"projects/prj-looker/jobs/lk_{day}",
                     "query": "SELECT country_cd, COUNT(1) FROM "
                              "`axp-lumi.dw.gms_transaction` GROUP BY 1",
                     "statement_type": "SELECT", "creation_time":
                     f"{day}T11:00:00Z", "end_time": f"{day}T11:00:03Z",
                     "total_bytes_processed": "123", "total_bytes_billed":
                     "10485760", "error_message": None,
                     "referenced_tables": [
                         '"projects/axp-lumi/datasets/dw/tables/'
                         'gms_transaction"',
                         '"projects/axp-lumi/datasets/data/tables/'
                         'gms_transaction"'],
                     "referenced_views": [
                         '"projects/axp-lumi/datasets/dw/tables/'
                         'gms_transaction"']}]
        if "APPROX_COUNT_DISTINCT" in sql:
            n = len(re.findall(r"AS c\d+_distinct", sql))
            row = {"total_rows": 1000}
            names = re.findall(r"APPROX_COUNT_DISTINCT\(`([^`]+)`\)", sql)
            for i, name in enumerate(names[:n]):
                distinct = {"country_cd": 4, "part_dt": 3,
                            "acct_status_cd": 5}.get(name, 900)
                row[f"c{i}_nulls"] = 10
                row[f"c{i}_distinct"] = distinct
                row[f"c{i}_min"] = "A"
                row[f"c{i}_max"] = "Z"
            return [row]
        if "AS column_name" in sql:
            cols = re.findall(r"'([^']+)' AS column_name", sql)
            rows = []
            for col in cols:
                if col == "se_no":                       # over the cap
                    rows += [{"column_name": col, "value": str(i),
                              "is_null": False, "value_count": 1}
                             for i in range(11)]
                else:
                    rows += [{"column_name": col, "value": v,
                              "is_null": False, "value_count": c}
                             for v, c in (("US", 720), ("GB", 63),
                                          ("CA", 40))]
                    rows.append({"column_name": col, "value": None,
                                 "is_null": True, "value_count": 7})
            return rows
        raise AssertionError(f"unrouted statement: {sql[:200]}")


@pytest.fixture
def cfg(tmp_path):
    path = tmp_path / "tables.yaml"
    path.write_text(CONFIG)
    c = load_config(path, output_dir=tmp_path / "out")
    return c


def _run(cfg, fake, **kw):
    ext = WarehouseExtractor(cfg, fake, quiet=True, now=lambda: NOW, **kw)
    ext.sa_email = SA
    report = ext.run()
    return ext, report


def _csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


# ── the archive layout ──

def test_run_produces_the_loader_layout(cfg):
    fake = FakeBQ()
    ext, report = _run(cfg, fake)
    root = cfg.output_dir
    t = root / "gms_transaction"
    for name in ("00_logical_table_resource.json",
                 "00_physical_table_resource.json",
                 "01_logical_table_meta.csv", "02_logical_columns.csv",
                 "03_logical_column_field_paths.csv",
                 "04_logical_table_options.csv", "05_view_definition.sql",
                 "05_view_definition.csv", "06_physical_table_meta.csv",
                 "07_physical_columns.csv", "10_physical_partitions.csv",
                 "11_logical_constraints.json", "12_physical_constraints.json",
                 "13_table_metrics.csv", "13_table_metrics.json",
                 "14_profile_plan.json", "14_profile_coverage.json",
                 "14_column_profile.csv", "15_low_cardinality_manifest.csv",
                 "15_low_cardinality_values/country_cd.csv",
                 "16_row_access_policies.json",
                 "17_queries_30d/jobs_30d.jsonl.gz",
                 "17_queries_30d/jobs_top_users.csv",
                 "17_queries_30d/jobs_co_queried_tables.csv",
                 "17_queries_30d/jobs_query_templates.csv",
                 "17_queries_30d/jobs_daily_usage_cost.csv",
                 "17_queries_30d/jobs_failed_queries.json",
                 "17_queries_30d/audit_top_users.csv",
                 "17_queries_30d/summary.json",
                 "_summary.json", "_profile_summary.json"):
        assert (t / name).exists(), name
    for name in ("_state.json", "_run_report.json", "_run_report.md",
                 "_batch_summary.json", "_batch_summary.csv",
                 "_shared/logical_columns.csv", "_shared/dataset_dw.json",
                 "_history/per_table_summary.json"):
        assert (root / name).exists(), name
    assert list((root / "_run_logs").glob("*.events.jsonl"))

    # the columns the loader reads, by name
    cols = _csv(t / "02_logical_columns.csv")
    assert {"column_name", "ordinal_position", "data_type", "is_nullable",
            "is_partitioning_column"} <= set(cols[0])
    assert [c["column_name"] for c in cols] == [
        "part_dt", "country_cd", "trans_usd_am", "se_no", "payment_detail"]
    metrics = _csv(t / "13_table_metrics.csv")
    assert metrics[0]["total_rows"] == "1500"
    assert metrics[0]["metrics_source"] == "get_table_metrics"
    parts = _csv(t / "10_physical_partitions.csv")
    assert [p["partition_id"] for p in parts] == ["20260820", "20260821",
                                                  "20260822"]
    constraints = json.loads((t / "12_physical_constraints.json").read_text())
    assert constraints["primary_key"]["columns"] == ["se_no"]
    logical_constraints = json.loads(
        (t / "11_logical_constraints.json").read_text())
    assert logical_constraints["primary_key"]["columns"] == []   # confirmed
    assert "oncop" in (t / "05_view_definition.sql").read_text()
    assert json.loads((t / "16_row_access_policies.json").read_text()) == []
    summary = json.loads((t / "_summary.json").read_text())
    assert summary["logical_type"] == "VIEW" and summary["physical_exists"]
    assert summary["physical_resolution"] == "same_name"
    assert report["status"] == "complete"


def test_profile_is_budget_planned_on_the_physical_table(cfg):
    fake = FakeBQ()
    _run(cfg, fake)
    t = cfg.output_dir / "gms_transaction"
    plan = json.loads((t / "14_profile_plan.json").read_text())
    assert plan["target"] == "axp-lumi.data.gms_transaction"
    assert plan["target_layer"] == "physical"
    assert plan["partition_column"] == "part_dt"
    # full history (every non-NULL partition) is 300 GiB: over budget
    assert plan["full_history_bytes"] == 300 * GIB
    assert plan["candidates"][0]["coverage_mode"] == \
        "full_non_null_partition_history"
    assert plan["chosen"] == "recent_partitions_budgeted"
    assert plan["planned_bytes"] <= 200 * GIB
    # 4 scalar columns in chunks of 3 → 2 chunks; STRUCT listed, not dropped
    assert plan["profile_chunks"] == 2
    assert [u["column_name"] for u in plan["unsupported_columns"]] == [
        "payment_detail"]
    coverage = json.loads((t / "14_profile_coverage.json").read_text())
    assert coverage["coverage_mode"] == "recent_partitions_budgeted"
    assert "part_dt" in coverage["partition_predicate"]
    profile = _csv(t / "14_column_profile.csv")
    assert {r["column_name"] for r in profile} == {"part_dt", "country_cd",
                                                   "trans_usd_am", "se_no"}
    by = {r["column_name"]: r for r in profile}
    assert by["country_cd"]["approx_distinct"] == "4"
    assert by["country_cd"]["null_count"] == "10"
    assert by["country_cd"]["coverage_mode"] == "recent_partitions_budgeted"
    assert list((t / "_profile_chunks").glob("chunk_*.json"))


def test_full_history_priced_from_the_inventory_when_refused(cfg):
    fake = FakeBQ(refuse_not_null=True)
    _run(cfg, fake)
    t = cfg.output_dir / "gms_transaction"
    plan = json.loads((t / "14_profile_plan.json").read_text())
    assert "error" in plan["candidates"][0]
    assert plan["full_history_bytes"] == 300 * GIB      # 3 × 100 GiB
    assert plan["chosen"] == "recent_partitions_budgeted"
    manifest = {r["column_name"]: r
                for r in _csv(t / "15_low_cardinality_manifest.csv")}
    # the full-history domain scan was refused too: the window's domain
    # is what we have, and it says so
    assert manifest["country_cd"]["coverage_mode"] == \
        "recent_partitions_budgeted"


def test_value_domains_confirm_exactly_and_prefer_full_history(cfg):
    fake = FakeBQ()
    _run(cfg, fake)
    t = cfg.output_dir / "gms_transaction"
    manifest = {r["column_name"]: r
                for r in _csv(t / "15_low_cardinality_manifest.csv")}
    # candidates: approx ≤ threshold*1.2 → country_cd (4), part_dt (3);
    # se_no (900) never enters
    assert set(manifest) == {"country_cd", "part_dt"}
    assert manifest["country_cd"]["low_cardinality"] == "YES"
    assert manifest["country_cd"]["profiled"] == "YES"
    assert manifest["country_cd"]["exact_distinct_non_null"] == "3"
    assert manifest["country_cd"]["distinct_estimate"] == "3"
    # narrow columns over every partition fit the domain budget, so the
    # domain is full history even though the profile was windowed
    assert manifest["country_cd"]["coverage_mode"] == \
        "full_non_null_partition_history_value_domain"
    values = _csv(t / "15_low_cardinality_values/country_cd.csv")
    assert {"column_name", "value", "value_count", "pct_of_rows", "rank"} \
        <= set(values[0])
    assert values[0]["value"] == "US" and values[0]["rank"] == "1"
    assert values[0]["pct_of_rows"] == str(round(720 / 830 * 100, 4))
    assert values[-1]["is_null"] == "YES"
    assert not (t / "15_low_cardinality_values/se_no.csv").exists()


def test_history_comes_from_the_data_project_not_the_billing_one(cfg):
    fake = FakeBQ()
    ext, report = _run(cfg, fake)
    q = cfg.output_dir / "gms_transaction" / "17_queries_30d"
    users = _csv(q / "jobs_top_users.csv")
    # the service account's own jobs (both projects) never enter a digest
    assert [u["user_email"] for u in users] == ["analyst1@corp",
                                                "analyst2@corp",
                                                "analyst3@corp"]
    assert users[0]["query_count"] == "6"           # 2 jobs × 3 days
    raw = [json.loads(l) for l in gzip.open(q / "jobs_30d.jsonl.gz", "rt")]
    ours = [r for r in raw if r["user_email"] == SA]
    assert ours and all(r["excluded_reason"] == "extractor_identity"
                        for r in ours)
    assert {r["project_id"] for r in raw} == {"axp-lumi", "prj-p-lumi-gpt"}
    co = {r["other_table"]: int(r["co_query_count"])
          for r in _csv(q / "jobs_co_queried_tables.csv")}
    assert co == {"risk_pers_acct": 3}
    templates = _csv(q / "jobs_query_templates.csv")
    assert templates[0]["occurrences"] == "6"
    assert "part_dt = ?" in templates[0]["normalized_sql"]
    assert "country_cd = ?" in templates[0]["normalized_sql"]
    assert templates[0]["distinct_users"] == "2"
    failed = json.loads((q / "jobs_failed_queries.json").read_text())
    assert len(failed) == 3 and "contry_cd" in failed[0]["error_message"]
    daily = _csv(q / "jobs_daily_usage_cost.csv")
    assert len(daily) == 3 and daily[0]["failed_count"] == "1"
    # the audit sink saw the Looker project's query — a project no
    # JOBS_BY_PROJECT scan covers
    audit_users = _csv(q / "audit_top_users.csv")
    assert audit_users[0]["user_email"] == "looker-sa@other-project.iam"
    summary = json.loads((q / "summary.json").read_text())
    assert summary["jobs_rows"] == 12 and summary["audit_rows"] == 3
    assert summary["jobs"]["by_job_project"] == {"axp-lumi": 12}
    assert summary["distinct_users"] == 4
    assert summary["history_days"] == 3
    # JOBS_BY_ORGANIZATION was denied ONCE, then left alone for the run
    org = [t for t in report["tasks"]
           if t["operation"].startswith("jobs_by_organization")]
    assert len(org) == 1 and org[0]["status"] == DENIED
    assert report["history_window"] == ["2026-08-21T00:00:00Z",
                                        "2026-08-24T00:00:00Z"]
    corpus = json.loads((cfg.output_dir / "_history"
                         / "per_table_summary.json").read_text())["corpus"]
    assert corpus["jobs_by_project/axp-lumi"]["files"] == 3
    assert corpus["audit_log"]["matched"] == 3


def test_physical_table_is_resolved_from_the_views_dry_run(cfg):
    fake = FakeBQ()
    _run(cfg, fake)
    t = cfg.output_dir / "risk_pers_acct"
    summary = json.loads((t / "_summary.json").read_text())
    assert summary["physical_exists"]
    assert summary["physical_resolution"] == "view_reference"
    assert summary["physical_candidates"] == ["axp-lumi.data.risk_pers_acct_hist"]
    res = json.loads((t / "00_physical_table_resource.json").read_text())
    assert res["tableReference"]["tableId"] == "risk_pers_acct_hist"
    assert summary["physical"] == "axp-lumi.data.risk_pers_acct_hist"
    # every physical read — row policies, the profile — uses the
    # RESOLVED table, not the configured name
    plan = json.loads((t / "14_profile_plan.json").read_text())
    assert plan["target"] == "axp-lumi.data.risk_pers_acct_hist"
    # tables.get metrics fallback when the routine answers nothing
    metrics = _csv(t / "13_table_metrics.csv")
    assert metrics[0]["total_rows"] == "42"
    assert metrics[0]["metrics_source"] == "tables.get physical"


def test_denied_is_recorded_as_unknown_never_as_absent(cfg):
    fake = FakeBQ()
    _, report = _run(cfg, fake)
    denied = {(d["table"], d["operation"]) for d in report["denied_operations"]}
    assert ("risk_pers_acct", "rowAccessPolicies.list") in denied
    assert not (cfg.output_dir / "risk_pers_acct"
                / "16_row_access_policies.json").exists()
    assert ("_shared", "table_storage") in denied
    assert report["status_counts"][DENIED] >= 3
    md = (cfg.output_dir / "_run_report.md").read_text()
    assert "Denied (unknown, NOT absent)" in md
    assert "rowAccessPolicies.list" in md


def test_resume_reuses_finished_work(cfg):
    fake = FakeBQ()
    _run(cfg, fake)
    profile_jobs = [c for c in fake.calls if c[1].startswith("profile chunk")
                    and c[0] == "query"]
    assert len(profile_jobs) == 3            # gms: 2 chunks · risk: 1
    fake.calls.clear()
    _, report = _run(cfg, fake)
    assert not [c for c in fake.calls if c[1].startswith("profile chunk")]
    assert not [c for c in fake.calls if c[1].startswith("domain group")]
    assert not [c for c in fake.calls
                if c[1].startswith("jobs_by_project") and "2026-08-21" in c[1]]
    assert report["status_counts"]["CACHED"] > 10
    # --force re-extracts one table only
    fake.calls.clear()
    _run(cfg, fake, force_tables=["gms_transaction"])
    assert [c for c in fake.calls if c[1].startswith("profile chunk")]


def test_plan_mode_never_bills(cfg):
    fake = FakeBQ()
    _, report = _run(cfg, fake, plan_only=True)
    assert report["total_bytes_billed"] == 0
    assert not [c for c in fake.calls if c[1].startswith("profile chunk")
                and c[0] == "query"]
    plan = json.loads((cfg.output_dir / "gms_transaction"
                       / "14_profile_plan.json").read_text())
    assert plan["chosen"] == "recent_partitions_budgeted"
    assert report["status_counts"].get("PLANNED", 0) >= 3


def test_budget_skips_are_explicit(cfg):
    cfg.profile_budget_bytes = 10 * GIB          # under even one partition
    fake = FakeBQ()
    _, report = _run(cfg, fake)
    t = cfg.output_dir / "gms_transaction"
    plan = json.loads((t / "14_profile_plan.json").read_text())
    assert plan["chosen"] == "single_partition_system_sample_budgeted"
    assert 0 < plan["candidates"][-1]["sample_percent"] < 100
    coverage = json.loads((t / "14_profile_coverage.json").read_text())
    assert coverage["sample_percent"] < 100
    cfg2 = cfg
    cfg2.run_budget_bytes = 1                     # nothing may run
    fake2 = FakeBQ()
    _, report2 = _run(cfg2, fake2, fresh=True)
    assert report2["status_counts"]["BUDGET_SKIPPED"] >= 1
    assert report2["total_bytes_billed"] == 0


def test_the_loader_consumes_the_archive(cfg, tmp_path):
    from sahs.graph.crosswalk import Crosswalk
    from sahs.graph.quads import GraphDir
    from sahs.loaders.archives.bq_extraction import load_bq_archive
    fake = FakeBQ()
    _run(cfg, fake)
    cw = tmp_path / "crosswalk.jsonl"
    cw.write_text("\n".join(json.dumps({
        "physical": f"dw.{t}", "lumi_asset_id": f"lumi-{i}",
        "atlas_entity_id": t, "verified_by": "test",
        "verified_on": "2026-08-23"})
        for i, t in enumerate(("gms_transaction", "risk_pers_acct"))) + "\n")
    graph = GraphDir(tmp_path / "graph")
    report, blocking = load_bq_archive(cfg.output_dir, graph,
                                       Crosswalk.load(cw), "test-run")
    assert blocking == []
    assert report["tables"] == 2
    assert report["columns"] == 7
    assert report["nested_columns"] == 1          # payment_detail.card.network
    assert report["domains"] == 3     # country_cd, part_dt, acct_status_cd
    assert report["domains_with_estimate"] == 3
    assert report["templates"] >= 2
    assert report["co_query_edges"] == 2
    assert report["pk_columns"] == 0              # the logical layer declares none
    assert report["policies_unknown"] == 1        # risk_pers_acct: DENIED
    assert report["template_rows_skipped"] == 0
    assert report["field_path_rows_skipped"] == 0


# ── the pieces ──

def test_normalize_sql_is_literal_blind():
    a = normalize_sql("SELECT x FROM t WHERE id = 12345 AND d = '2026-01-01'"
                      " -- note\n AND k IN (1, 2, 3)")
    b = normalize_sql("SELECT x FROM t WHERE id = 98765 AND d = '2025-12-31'"
                      " AND k IN (7, 8)")
    assert a == b == ("SELECT x FROM t WHERE id = ? AND d = ? AND k IN (?)")
    assert fingerprint(a) == fingerprint(b) and len(fingerprint(a)) == 12
    assert normalize_sql("SELECT col_1 FROM t2 WHERE ts > DATE '2026-01-01'") \
        == "SELECT col_1 FROM t2 WHERE ts > DATE ?"


def test_match_tables_by_reference_then_by_text(cfg):
    rec = normalize_record({"job_id": "j", "project_id": "axp-lumi",
                            "referenced_tables": [{"projectId": "other",
                                                   "datasetId": "dw",
                                                   "tableId":
                                                   "gms_transaction"}],
                            "query": "select 1 from gms_transaction_v2"},
                           "jobs_by_project")
    assert match_tables(rec, cfg.tables, "axp-lumi") == ([], "")
    rec.query = "select * from dw.gms_transaction"
    assert match_tables(rec, cfg.tables, "axp-lumi") == (["gms_transaction"],
                                                          "query_text")
    rec.referenced_tables = [{"project_id": "", "dataset_id": "",
                              "table_id": "RISK_PERS_ACCT"}]
    assert match_tables(rec, cfg.tables, "axp-lumi") == (["risk_pers_acct"],
                                                          "referenced")


def test_audit_record_paths_decode():
    rec = normalize_record({
        "job_name": "projects/prj-looker/jobs/lk1",
        "referenced_tables": ['"projects/axp-lumi/datasets/dw/tables/x"'],
        "destination_table": "projects/p/datasets/d/tables/t",
        "cache_hit": "true", "total_bytes_billed": "10485760"},
        "audit_log")
    assert rec.project_id == "prj-looker" and rec.job_id == "lk1"
    assert rec.referenced_tables == [{"project_id": "axp-lumi",
                                      "dataset_id": "dw", "table_id": "x"}]
    assert rec.destination_table == {"project_id": "p", "dataset_id": "d",
                                     "table_id": "t"}
    assert rec.cache_hit is True and rec.total_bytes_billed == 10485760


def test_classify_and_decode():
    assert classify(403, "accessDenied", "Access Denied") == DENIED
    assert classify(404, "notFound", "Not found: Table") == NOT_FOUND
    assert classify(0, "", "Not found: Table axp-lumi:dw.x") == NOT_FOUND
    assert classify(0, "invalidQuery", "Cannot query over table") == "INVALID"
    assert classify(0, "", "Query exceeded limit for bytes billed") \
        == "BUDGET_SKIPPED"
    assert classify(503, "backendError", "x") == "TRANSPORT"
    schema = {"fields": [
        {"name": "n", "type": "INTEGER"},
        {"name": "ts", "type": "TIMESTAMP"},
        {"name": "refs", "type": "RECORD", "mode": "REPEATED",
         "fields": [{"name": "table_id", "type": "STRING"}]},
        {"name": "top", "type": "RECORD", "mode": "REPEATED",
         "fields": [{"name": "value", "type": "STRING"},
                    {"name": "count", "type": "INTEGER"}]}]}
    rows = decode_rows(schema, [{"f": [
        {"v": "3"}, {"v": "1.7558208E9"},
        {"v": [{"v": {"f": [{"v": "gms_transaction"}]}}]},
        {"v": [{"v": {"f": [{"v": "US"}, {"v": "7"}]}}]}]}])
    assert rows == [{"n": 3, "ts": "2025-08-22T00:00:00.000000Z",
                     "refs": [{"table_id": "gms_transaction"}],
                     "top": [{"value": "US", "count": 7}]}]


def test_budget_reserves_and_settles():
    b = Budget(100)
    assert b.try_reserve(60) and not b.try_reserve(50)
    b.settle(60, 30)
    assert b.spent == 30 and b.remaining == 70
    assert b.try_reserve(70)
    assert Budget(0).try_reserve(10 ** 15)        # 0 = unlimited


def test_sql_is_parameterized_and_qualified():
    sql, params = S.jobs_by_project_day("axp-lumi", "us", "a", "b",
                                        ["gms_transaction", "x"])
    assert "`axp-lumi`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT" in sql
    assert "@day_start" in sql and "UNNEST(@tables)" in sql
    assert {p["name"] for p in params} == {"day_start", "day_end", "tables",
                                           "name_regex"}
    assert S.name_regex(["gms_transaction", "risk_pers_acct"]) == \
        r"(?i)\b(gms_transaction|risk_pers_acct)\b"
    org, _ = S.jobs_by_organization_day("axp-lumi", "us", "a", "b", ["t"])
    assert "CAST(NULL AS STRING) AS query" in org
    audit, _ = S.audit_log_day("p.d.t_*", S.AUDIT_FORMAT_METADATA_JSON,
                               "timestamp", "a", "b", "20260822", ["t"])
    assert "_TABLE_SUFFIX = @day_suffix" in audit
    assert "jobChange.after') = 'DONE'" in audit
    target = S.ProfileTarget("axp-lumi", "data", "t", partition_column="dt",
                             partition_predicate="`dt` >= DATE('2026-01-01')")
    chunk, prefixes = S.profile_chunk(target, [("a", "STRING"),
                                               ("b", "NUMERIC(10,2)")])
    assert prefixes == ["c0", "c1"] and "AS c1_avg" in chunk
    assert "SUBSTR(MIN(`a`), 1, 256)" in chunk
    assert chunk.endswith("WHERE `dt` >= DATE('2026-01-01')")
    sampled = S.ProfileTarget("p", "d", "t", sample_percent=0.5)
    assert "TABLESAMPLE SYSTEM (0.5 PERCENT)" in sampled.from_clause()
    domain = S.domain_group(target, ["a", "it's"], 11)
    assert domain.count("UNION ALL") == 1 and "LIMIT 11" in domain
    assert "'it\\'s' AS column_name" in domain
    assert S.partition_predicate("dt", "DAY", "2026-01-01", "2026-01-31",
                                 "TIMESTAMP").startswith("`dt` >= TIMESTAMP(")
    assert S.partition_predicate("_PARTITIONTIME", "DAY", "2026-01-01",
                                 "2026-01-31", "") \
        .startswith("_PARTITIONTIME >= TIMESTAMP(")
    assert S.partition_predicate("bucket", "RANGE", "10", "20", "INT64") \
        == "`bucket` >= 10 AND `bucket` <= 20"
    assert _partition_bounds(["202601", "202603"], "MONTH") == \
        ("2026-01-01", "2026-03-31")
    assert _partition_bounds(["2024", "2026"], "YEAR") == \
        ("2024-01-01", "2026-12-31")


def test_config_rejects_what_would_hurt(tmp_path):
    path = tmp_path / "t.yaml"
    path.write_text(CONFIG.replace("  - name: risk_pers_acct",
                                   "  - name: gms_transaction"))
    with pytest.raises(ConfigError, match="listed twice"):
        load_config(path)
    path.write_text(CONFIG + "    bq_datset: x\n")
    with pytest.raises(ConfigError, match="unknown keys"):
        load_config(path)
    path.write_text(CONFIG.replace("history_days: 3", "history_days: 400"))
    with pytest.raises(ConfigError, match="history_days"):
        load_config(path)
    path.write_text(CONFIG)
    with pytest.raises(ConfigError, match="not in config"):
        load_config(path, only=["nope"])
    cfg = load_config(path, extra_tables=["new_table_x"],
                      env={"BQ_PROJECT_ID": "ignored"})
    assert cfg.table("new_table_x").physical_fqn == "axp-lumi.data.new_table_x"
    assert cfg.job_projects == ["axp-lumi", "prj-p-lumi-gpt"]
    assert cfg.run_budget_bytes == 2 << 40
    # the legacy semantic-graph tables.yaml keys still load
    legacy = tmp_path / "legacy.yaml"
    legacy.write_text("defaults:\n  bq_project: axp-lumi\n  bq_dataset: dw\n"
                      "  billing_project: prj-p-lumi-gpt\n"
                      "tables:\n  - name: a\n  - name: b\n    bq_dataset: common\n")
    cfg = load_config(legacy)
    assert cfg.table("b").logical_fqn == "axp-lumi.common.b"
    assert cfg.table("a").physical_fqn == "axp-lumi.data.a"


def test_probe_reports_where_the_history_is(cfg):
    from sahs.extract.probe import render_probe, run_probe
    fake = FakeBQ()
    rows = run_probe(cfg, fake, cfg.tables[0], self_email=SA, now=NOW)
    by = {r.surface: r for r in rows}
    assert by["tables.get physical data.gms_transaction"].status == FETCHED
    assert "requirePartitionFilter=True" in \
        by["tables.get physical data.gms_transaction"].detail
    assert by["rowAccessPolicies.list physical"].status == FETCHED
    assert by["INFORMATION_SCHEMA.TABLE_STORAGE (region)"].status == DENIED
    assert by["JOBS_BY_ORGANIZATION in axp-lumi (7d)"].status == DENIED
    assert by["audit_log sink"].status == FETCHED
    assert "metadata_json" in by["audit_log sink"].detail
    text = render_probe(rows)
    assert "DENIED" in text and "summary:" in text


def test_index_is_reproducible_offline(cfg):
    from sahs.extract.history import index_history
    fake = FakeBQ()
    _run(cfg, fake)
    q = cfg.output_dir / "gms_transaction" / "17_queries_30d"
    before = (q / "jobs_query_templates.csv").read_text()
    (q / "jobs_query_templates.csv").unlink()
    index_history(cfg.output_dir / "_history", cfg.tables, cfg.output_dir,
                  data_project="axp-lumi", exclude_users=[SA],
                  history_days=3)
    assert (q / "jobs_query_templates.csv").read_text() == before
