"""Every SQL statement the extractor runs, in one place, parameterized.

Rules that hold for every statement here:

  * table names, dates and thresholds travel as NAMED PARAMETERS
    (``@tables``, ``@day_start`` …) — nothing user-supplied is spliced
    into SQL text. Identifiers that MUST be spliced (projects, datasets,
    column names) are validated by the config layer and back-quoted.
  * INFORMATION_SCHEMA statements are project-qualified
    (``\`axp-lumi\`.dw.INFORMATION_SCHEMA.COLUMNS``) so they resolve where
    the data lives even though the job runs and bills elsewhere.
  * job-history statements are qualified with the project WHOSE history
    is wanted (``\`axp-lumi\`.\`region-us\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT``):
    that is the whole fix for "the 30-day queries were invisible" — the
    billing project's view only ever held our own jobs.
  * every statement that reads DATA (profiling, value domains) is built
    from a ProfileTarget carrying the coverage predicate / sample clause
    the planner chose, so what was scanned is always on record.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from sahs.extract.bq_rest import p_array_str, p_int, p_str, p_ts

# INFORMATION_SCHEMA data types the profiler aggregates over. Everything
# else (STRUCT, ARRAY, JSON, GEOGRAPHY, INTERVAL, BYTES, RANGE) is listed
# as unsupported in 14_profile_plan.json rather than silently dropped.
PROFILE_TYPES = {"STRING", "INT64", "INTEGER", "FLOAT64", "FLOAT", "NUMERIC",
                 "BIGNUMERIC", "DECIMAL", "BIGDECIMAL", "BOOL", "BOOLEAN",
                 "DATE", "DATETIME", "TIMESTAMP", "TIME"}
NUMERIC_TYPES = {"INT64", "INTEGER", "FLOAT64", "FLOAT", "NUMERIC",
                 "BIGNUMERIC", "DECIMAL", "BIGDECIMAL"}


def q(ident: str) -> str:
    """Back-quote one identifier segment (a project may carry dashes)."""
    ident = str(ident).replace("`", "")
    return f"`{ident}`"


def fqn(project: str, dataset: str, table: str) -> str:
    return f"{q(project)}.{q(dataset)}.{q(table)}"


def info_schema(project: str, dataset: str, view: str) -> str:
    return f"{q(project)}.{q(dataset)}.INFORMATION_SCHEMA.{view}"


def region_schema(project: str, region: str, view: str) -> str:
    return f"{q(project)}.{q('region-' + region.lower())}." \
           f"INFORMATION_SCHEMA.{view}"


def base_type(data_type: str) -> str:
    """``NUMERIC(10,2)`` → ``NUMERIC``; ``STRING(20)`` → ``STRING``."""
    return re.split(r"[(<]", (data_type or "").strip().upper(), 1)[0]


# ── phase 1: shared metadata (one statement per dataset, all tables) ──

def shared_tables(project: str, dataset: str, tables: list[str]
                  ) -> tuple[str, list[dict]]:
    return (f"SELECT * FROM {info_schema(project, dataset, 'TABLES')}\n"
            f"WHERE table_name IN UNNEST(@tables)\nORDER BY table_name",
            [p_array_str("tables", tables)])


def shared_columns(project: str, dataset: str, tables: list[str]
                   ) -> tuple[str, list[dict]]:
    return (f"SELECT * FROM {info_schema(project, dataset, 'COLUMNS')}\n"
            f"WHERE table_name IN UNNEST(@tables)\n"
            f"ORDER BY table_name, ordinal_position",
            [p_array_str("tables", tables)])


def shared_field_paths(project: str, dataset: str, tables: list[str]
                       ) -> tuple[str, list[dict]]:
    return (f"SELECT * FROM "
            f"{info_schema(project, dataset, 'COLUMN_FIELD_PATHS')}\n"
            f"WHERE table_name IN UNNEST(@tables)\n"
            f"ORDER BY table_name, field_path",
            [p_array_str("tables", tables)])


def shared_table_options(project: str, dataset: str, tables: list[str]
                         ) -> tuple[str, list[dict]]:
    return (f"SELECT * FROM {info_schema(project, dataset, 'TABLE_OPTIONS')}\n"
            f"WHERE table_name IN UNNEST(@tables)\n"
            f"ORDER BY table_name, option_name",
            [p_array_str("tables", tables)])


def shared_views(project: str, dataset: str, tables: list[str]
                 ) -> tuple[str, list[dict]]:
    return (f"SELECT * FROM {info_schema(project, dataset, 'VIEWS')}\n"
            f"WHERE table_name IN UNNEST(@tables)\nORDER BY table_name",
            [p_array_str("tables", tables)])


def shared_partitions(project: str, dataset: str, tables: list[str]
                      ) -> tuple[str, list[dict]]:
    return (f"SELECT * FROM {info_schema(project, dataset, 'PARTITIONS')}\n"
            f"WHERE table_name IN UNNEST(@tables)\n"
            f"ORDER BY table_name, partition_id",
            [p_array_str("tables", tables)])


def shared_constraints(project: str, dataset: str, tables: list[str]
                       ) -> tuple[str, list[dict]]:
    tc = info_schema(project, dataset, "TABLE_CONSTRAINTS")
    kcu = info_schema(project, dataset, "KEY_COLUMN_USAGE")
    ccu = info_schema(project, dataset, "CONSTRAINT_COLUMN_USAGE")
    return (
        "SELECT tc.table_name, tc.constraint_name, tc.constraint_type,\n"
        "       tc.enforced, kcu.column_name, kcu.ordinal_position,\n"
        "       kcu.position_in_unique_constraint,\n"
        "       ccu.table_schema AS referenced_schema,\n"
        "       ccu.table_name AS referenced_table,\n"
        "       ccu.column_name AS referenced_column\n"
        f"FROM {tc} tc\n"
        f"LEFT JOIN {kcu} kcu\n"
        "  ON kcu.constraint_name = tc.constraint_name\n"
        " AND kcu.table_name = tc.table_name\n"
        f"LEFT JOIN {ccu} ccu\n"
        "  ON ccu.constraint_name = tc.constraint_name\n"
        " AND tc.constraint_type = 'FOREIGN KEY'\n"
        "WHERE tc.table_name IN UNNEST(@tables)\n"
        "ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position",
        [p_array_str("tables", tables)])


def shared_routines(project: str, dataset: str) -> tuple[str, list[dict]]:
    return (f"SELECT * FROM {info_schema(project, dataset, 'ROUTINES')}\n"
            f"ORDER BY routine_name", [])


def shared_table_storage(project: str, region: str, datasets: list[str],
                         tables: list[str]) -> tuple[str, list[dict]]:
    return (f"SELECT * FROM "
            f"{region_schema(project, region, 'TABLE_STORAGE')}\n"
            f"WHERE table_schema IN UNNEST(@datasets)\n"
            f"  AND table_name IN UNNEST(@tables)\n"
            f"ORDER BY table_schema, table_name",
            [p_array_str("datasets", datasets),
             p_array_str("tables", tables)])


# ── phase 2: per-table resources ──

def table_metrics_routine(project: str, dataset: str, routine: str,
                          table: str) -> tuple[str, list[dict]]:
    """The proven ``dw.get_table_metrics('<table>')`` table function. A
    TVF argument cannot be a query parameter, so the (config-validated)
    name is escaped as a string literal."""
    literal = table.replace("\\", "\\\\").replace("'", "\\'")
    return (f"SELECT * FROM {q(project)}.{q(dataset)}.{q(routine)}"
            f"('{literal}')", [])


def probe_select(project: str, dataset: str, table: str
                 ) -> tuple[str, list[dict]]:
    return f"SELECT * FROM {fqn(project, dataset, table)} LIMIT 1", []


# ── phase 3: profiling ──

@dataclass
class ProfileTarget:
    """What the profiler reads and how much of it — decided by the
    planner, recorded verbatim in 14_profile_coverage.json."""

    project: str
    dataset: str
    table: str
    coverage_mode: str = "unpartitioned"
    partition_column: str = ""
    partition_predicate: str = ""      # SQL text after WHERE, or ""
    sample_percent: float = 100.0      # <100 → TABLESAMPLE SYSTEM
    partition_ids: list[str] = field(default_factory=list)
    partition_type: str = ""           # DAY / MONTH / HOUR / YEAR / RANGE
    partition_low: str = ""
    partition_high: str = ""

    @property
    def fqn(self) -> str:
        return fqn(self.project, self.dataset, self.table)

    def from_clause(self) -> str:
        clause = f"FROM {self.fqn}"
        if self.sample_percent < 100:
            pct = max(0.001, min(99.999, float(self.sample_percent)))
            clause += f" TABLESAMPLE SYSTEM ({pct:g} PERCENT)"
        if self.partition_predicate:
            clause += f"\nWHERE {self.partition_predicate}"
        return clause

    def describe(self) -> dict:
        return {"target": f"{self.project}.{self.dataset}.{self.table}",
                "coverage_mode": self.coverage_mode,
                "partition_column": self.partition_column or None,
                "partition_type": self.partition_type or None,
                "partition_predicate": self.partition_predicate or None,
                "partition_low": self.partition_low or None,
                "partition_high": self.partition_high or None,
                "partitions_in_window": len(self.partition_ids) or None,
                "sample_percent": self.sample_percent}


def partition_predicate(column: str, partition_type: str, low: str,
                        high: str, column_type: str) -> str:
    """Inclusive window on a partition column. Time partitions compare
    against the column's own type (DATE / TIMESTAMP / DATETIME); range
    partitions compare integers. ``_PARTITIONTIME`` (ingestion-time)
    is handled by name."""
    col = q(column) if column not in ("_PARTITIONTIME", "_PARTITIONDATE") \
        else column
    ctype = base_type(column_type)
    if partition_type == "RANGE":
        return f"{col} >= {int(low)} AND {col} <= {int(high)}"
    if column == "_PARTITIONTIME" or ctype == "TIMESTAMP":
        return (f"{col} >= TIMESTAMP('{low}') AND "
                f"{col} < TIMESTAMP_ADD(TIMESTAMP('{high}'), INTERVAL 1 DAY)")
    if ctype == "DATETIME":
        return (f"{col} >= DATETIME('{low}') AND "
                f"{col} < DATETIME_ADD(DATETIME('{high}'), INTERVAL 1 DAY)")
    return f"{col} >= DATE('{low}') AND {col} <= DATE('{high}')"


def partition_not_null_predicate(column: str) -> str:
    """Full-history coverage on a partitioned table: every non-NULL
    partition (the NULL partition is where unparseable dates land)."""
    col = q(column) if column not in ("_PARTITIONTIME", "_PARTITIONDATE") \
        else column
    return f"{col} IS NOT NULL"


def profile_chunk(target: ProfileTarget, columns: list[tuple[str, str]]
                  ) -> tuple[str, list[str]]:
    """One aggregate statement over up to N scalar columns.

    Returns (sql, alias_prefixes): column i is exposed as ``c{i}_nulls``,
    ``c{i}_distinct``, ``c{i}_min``, ``c{i}_max`` and (numeric only)
    ``c{i}_avg`` — positional aliases keep the statement valid for the
    1,400-column, 300-char-name tables this warehouse has."""
    selects = ["COUNT(*) AS total_rows"]
    prefixes: list[str] = []
    for i, (name, data_type) in enumerate(columns):
        col = q(name)
        p = f"c{i}"
        prefixes.append(p)
        btype = base_type(data_type)
        selects.append(f"COUNTIF({col} IS NULL) AS {p}_nulls")
        selects.append(f"APPROX_COUNT_DISTINCT({col}) AS {p}_distinct")
        if btype == "STRING":
            selects.append(f"SUBSTR(MIN({col}), 1, 256) AS {p}_min")
            selects.append(f"SUBSTR(MAX({col}), 1, 256) AS {p}_max")
        else:
            selects.append(f"CAST(MIN({col}) AS STRING) AS {p}_min")
            selects.append(f"CAST(MAX({col}) AS STRING) AS {p}_max")
        if btype in NUMERIC_TYPES:
            selects.append(f"CAST(AVG(CAST({col} AS FLOAT64)) AS STRING) "
                           f"AS {p}_avg")
    sql = "SELECT\n  " + ",\n  ".join(selects) + "\n" + target.from_clause()
    return sql, prefixes


def domain_group(target: ProfileTarget, columns: list[str], limit: int
                 ) -> str:
    """Exact value distributions for a group of candidate columns — one
    narrow projection per column, UNION ALL'd, each capped at
    ``limit`` (= threshold + 1: one row over the cap proves the column
    is NOT low-cardinality without paying for the whole domain)."""
    branches = []
    for name in columns:
        col = q(name)
        literal = name.replace("\\", "\\\\").replace("'", "\\'")
        branches.append(
            "(SELECT "
            f"'{literal}' AS column_name, "
            f"CAST({col} AS STRING) AS value, "
            f"{col} IS NULL AS is_null, "
            "COUNT(*) AS value_count\n "
            f"{target.from_clause()}\n "
            "GROUP BY 1, 2, 3\n "
            f"ORDER BY value_count DESC\n LIMIT {int(limit)})")
    return "\nUNION ALL\n".join(branches)


# ── phase 4: query history (one statement per source per UTC day) ──

_JOB_COLUMNS = (
    "job_id, project_id, user_email, creation_time, start_time, end_time,\n"
    "  job_type, statement_type, state, priority, cache_hit,\n"
    "  error_result.reason AS error_reason,\n"
    "  error_result.message AS error_message,\n"
    "  total_bytes_processed, total_bytes_billed, total_slot_ms,\n"
    "  destination_table, referenced_tables, labels, parent_job_id")


def name_regex(names: list[str]) -> str:
    """Word-bounded, case-insensitive alternation of the short names —
    the textual net that catches jobs whose referenced_tables is empty
    (failed jobs, scripts) while never matching ``gms_transaction_v2``."""
    alts = "|".join(re.escape(n.lower()) for n in sorted(set(names)))
    return rf"(?i)\b({alts})\b"


def jobs_by_project_day(project: str, region: str, day_start: str,
                        day_end: str, names: list[str]
                        ) -> tuple[str, list[dict]]:
    """Jobs run IN ``project`` that touched any in-scope table. The
    creation_time window prunes the partitioned view; the match is by
    referenced table id (views AND the base tables they expand to
    appear there) or, for jobs without references, by query text."""
    return (
        f"SELECT\n  {_JOB_COLUMNS},\n  query\n"
        f"FROM {region_schema(project, region, 'JOBS_BY_PROJECT')}\n"
        "WHERE creation_time >= @day_start AND creation_time < @day_end\n"
        "  AND job_type = 'QUERY'\n"
        "  AND (EXISTS (SELECT 1 FROM UNNEST(referenced_tables) AS rt\n"
        "               WHERE LOWER(rt.table_id) IN UNNEST(@tables))\n"
        "       OR REGEXP_CONTAINS(query, @name_regex))",
        [p_ts("day_start", day_start), p_ts("day_end", day_end),
         p_array_str("tables", sorted({n.lower() for n in names})),
         p_str("name_regex", name_regex(names))])


def jobs_by_organization_day(project: str, region: str, day_start: str,
                             day_end: str, names: list[str]
                             ) -> tuple[str, list[dict]]:
    """Every project in the organization — needs org-level
    bigquery.jobs.listAll. This view carries no query text, so the
    match is by referenced table only."""
    return (
        f"SELECT\n  {_JOB_COLUMNS},\n  CAST(NULL AS STRING) AS query\n"
        f"FROM {region_schema(project, region, 'JOBS_BY_ORGANIZATION')}\n"
        "WHERE creation_time >= @day_start AND creation_time < @day_end\n"
        "  AND job_type = 'QUERY'\n"
        "  AND EXISTS (SELECT 1 FROM UNNEST(referenced_tables) AS rt\n"
        "              WHERE LOWER(rt.table_id) IN UNNEST(@tables))",
        [p_ts("day_start", day_start), p_ts("day_end", day_end),
         p_array_str("tables", sorted({n.lower() for n in names}))])


AUDIT_FORMAT_METADATA_JSON = "metadata_json"      # BigQueryAuditMetadata
AUDIT_FORMAT_SERVICEDATA_V1 = "servicedata_v1"    # legacy AuditData


def audit_log_day(table_fqn: str, audit_format: str, partition_column: str,
                  day_start: str, day_end: str, day_suffix: str,
                  names: list[str]) -> tuple[str, list[dict]]:
    """Completed query jobs from the data-access audit sink in the DATA
    project — the source that records a query from ANY project, because
    data-access logs land on the project that OWNS the table.

    Two sink formats exist in the wild; the schema probe picks one.
    A wildcard ``table_fqn`` (``…_data_access_*``) filters by
    _TABLE_SUFFIX, a partitioned sink by its timestamp column."""
    project, dataset, table = table_fqn.split(".")
    wildcard = table.endswith("*")
    source = fqn(project, dataset, table)
    if wildcard:
        window = "_TABLE_SUFFIX = @day_suffix"
    else:
        window = (f"{q(partition_column)} >= @day_start AND "
                  f"{q(partition_column)} < @day_end")
    params = [p_ts("day_start", day_start), p_ts("day_end", day_end),
              p_str("day_suffix", day_suffix),
              p_str("name_regex", name_regex(names))]
    if audit_format == AUDIT_FORMAT_SERVICEDATA_V1:
        job = ("protopayload_auditlog.servicedata_v1_bigquery."
               "jobCompletedEvent.job")
        sql = (
            "SELECT\n"
            "  timestamp AS log_time,\n"
            "  protopayload_auditlog.authenticationInfo.principalEmail "
            "AS user_email,\n"
            "  protopayload_auditlog.methodName AS method_name,\n"
            "  protopayload_auditlog.resourceName AS resource_name,\n"
            f"  {job}.jobName.projectId AS project_id,\n"
            f"  {job}.jobName.jobId AS job_id,\n"
            f"  {job}.jobConfiguration.query.query AS query,\n"
            f"  {job}.jobConfiguration.query.statementType "
            "AS statement_type,\n"
            f"  {job}.jobStatistics.createTime AS creation_time,\n"
            f"  {job}.jobStatistics.endTime AS end_time,\n"
            f"  {job}.jobStatistics.totalProcessedBytes "
            "AS total_bytes_processed,\n"
            f"  {job}.jobStatistics.totalBilledBytes AS total_bytes_billed,\n"
            f"  {job}.jobStatistics.totalSlotMs AS total_slot_ms,\n"
            f"  {job}.jobStatus.error.message AS error_message,\n"
            f"  {job}.jobStatus.error.code AS error_reason,\n"
            f"  {job}.jobStatistics.referencedTables AS referenced_tables,\n"
            f"  {job}.jobStatistics.referencedViews AS referenced_views,\n"
            f"  {job}.jobConfiguration.query.destinationTable "
            "AS destination_table\n"
            f"FROM {source}\n"
            f"WHERE {window}\n"
            "  AND protopayload_auditlog.servicedata_v1_bigquery."
            "jobCompletedEvent.eventName = 'query_job_completed'\n"
            f"  AND REGEXP_CONTAINS(TO_JSON_STRING({job}), @name_regex)")
        return sql, params
    meta = "protopayload_auditlog.metadataJson"
    sql = (
        "SELECT\n"
        "  timestamp AS log_time,\n"
        "  protopayload_auditlog.authenticationInfo.principalEmail "
        "AS user_email,\n"
        "  protopayload_auditlog.methodName AS method_name,\n"
        "  protopayload_auditlog.resourceName AS resource_name,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobName') AS job_name,\n"
        f"  JSON_VALUE({meta}, "
        "'$.jobChange.job.jobConfig.queryConfig.query') AS query,\n"
        f"  JSON_VALUE({meta}, "
        "'$.jobChange.job.jobConfig.queryConfig.statementType') "
        "AS statement_type,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobConfig.type') "
        "AS job_config_type,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobStats.createTime') "
        "AS creation_time,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobStats.endTime') "
        "AS end_time,\n"
        f"  JSON_VALUE({meta}, "
        "'$.jobChange.job.jobStats.queryStats.totalProcessedBytes') "
        "AS total_bytes_processed,\n"
        f"  JSON_VALUE({meta}, "
        "'$.jobChange.job.jobStats.queryStats.totalBilledBytes') "
        "AS total_bytes_billed,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobStats.totalSlotMs') "
        "AS total_slot_ms,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobStats.queryStats."
        "cacheHit') AS cache_hit,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobStatus.errorResult."
        "message') AS error_message,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobStatus.errorResult."
        "code') AS error_reason,\n"
        f"  JSON_QUERY_ARRAY({meta}, '$.jobChange.job.jobStats.queryStats."
        "referencedTables') AS referenced_tables,\n"
        f"  JSON_QUERY_ARRAY({meta}, '$.jobChange.job.jobStats.queryStats."
        "referencedViews') AS referenced_views,\n"
        f"  JSON_VALUE({meta}, '$.jobChange.job.jobConfig.queryConfig."
        "destinationTable') AS destination_table\n"
        f"FROM {source}\n"
        f"WHERE {window}\n"
        f"  AND JSON_VALUE({meta}, '$.jobChange.after') = 'DONE'\n"
        f"  AND JSON_VALUE({meta}, '$.jobChange.job.jobConfig.type') "
        "= 'QUERY'\n"
        f"  AND REGEXP_CONTAINS({meta}, @name_regex)")
    return sql, params


# ── probes (capability matrix; all cheap or metadata-only) ──

def probe_jobs_visible(project: str, region: str, view: str, days: int
                       ) -> tuple[str, list[dict]]:
    """How many query jobs the SA can SEE in a project's history, and
    how many of them are not its own — the number that decides whether
    a history source is worth scanning at all."""
    return (
        "SELECT COUNT(*) AS jobs,\n"
        "       COUNT(DISTINCT user_email) AS users,\n"
        "       COUNTIF(user_email != @self) AS jobs_by_others,\n"
        "       MIN(creation_time) AS first_seen,\n"
        "       MAX(creation_time) AS last_seen\n"
        f"FROM {region_schema(project, region, view)}\n"
        "WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), "
        "INTERVAL @days DAY)\n"
        "  AND job_type = 'QUERY'",
        [p_int("days", days), p_str("self", "")])


def probe_jobs_touching(project: str, region: str, view: str, days: int,
                        names: list[str]) -> tuple[str, list[dict]]:
    return (
        "SELECT LOWER(rt.dataset_id) AS dataset_id,\n"
        "       LOWER(rt.table_id) AS table_id,\n"
        "       COUNT(DISTINCT j.job_id) AS jobs,\n"
        "       COUNT(DISTINCT j.user_email) AS users\n"
        f"FROM {region_schema(project, region, view)} j,\n"
        "     UNNEST(j.referenced_tables) AS rt\n"
        "WHERE j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), "
        "INTERVAL @days DAY)\n"
        "  AND j.job_type = 'QUERY'\n"
        "  AND LOWER(rt.table_id) IN UNNEST(@tables)\n"
        "GROUP BY 1, 2\nORDER BY jobs DESC\nLIMIT 200",
        [p_int("days", days),
         p_array_str("tables", sorted({n.lower() for n in names}))])
