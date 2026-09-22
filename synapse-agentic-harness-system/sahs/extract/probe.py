"""The capability probe — what THIS identity can see, before spending.

One table, every surface the extractor will touch, each answered with
the archive's status vocabulary and the bytes a real run would scan.
Metadata reads only, plus dry runs; the single data read is a COUNT(*)
capped at 1 GiB billed. Run it on the laptop first:

    python scripts/bq_warehouse_extract.py probe --table gms_transaction

The history rows are the ones that matter for the 30-day question: for
every job project they report how many query jobs are visible and how
many were run by someone OTHER than this identity. A project whose
history shows only our own jobs is the billing project's view — the
symptom the earlier runs hit — and the row for the data project (or
the audit sink) is where the analysts' queries actually are.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass, field
from typing import Any, Callable

from sahs.extract import bq_sql as S
from sahs.extract.bq_rest import BQError, BQRest, EMPTY, FETCHED, p_str
from sahs.extract.config import RunConfig, TableSpec, fmt_bytes


@dataclass
class ProbeRow:
    surface: str
    status: str
    detail: str = ""
    bytes_estimate: int | None = None
    data: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        return {"surface": self.surface, "status": self.status,
                "detail": self.detail, "bytes_estimate": self.bytes_estimate,
                **({"data": self.data} if self.data else {})}


def _attempt(surface: str, fn: Callable[[], ProbeRow]) -> ProbeRow:
    try:
        return fn()
    except BQError as e:
        return ProbeRow(surface, e.status, e.message[:300])
    except Exception as e:                                   # noqa: BLE001
        return ProbeRow(surface, "ERROR", f"{type(e).__name__}: {e}"[:300])


def run_probe(cfg: RunConfig, client: BQRest, spec: TableSpec, *,
              self_email: str = "", history_days: int = 7,
              now: _dt.datetime | None = None) -> list[ProbeRow]:
    rows: list[ProbeRow] = []
    P, D, L, PH = (spec.data_project, spec.physical_dataset,
                   spec.logical_dataset, spec.physical_name)
    names = sorted({n for t in cfg.tables for n in t.match_names})

    def q(surface: str, sql: str, params: list[dict], *, data_read: bool
          = False, cap: int = 1 << 30) -> ProbeRow:
        estimate = None
        if data_read:
            estimate = client.dry_run(sql, params, scope="probe",
                                      operation=surface).bytes_processed
            if estimate > cap:
                return ProbeRow(surface, "BUDGET_SKIPPED",
                                f"would scan {fmt_bytes(estimate)}", estimate)
        r = client.query(sql, params, scope="probe", operation=surface,
                         timeout_s=300, max_bytes_billed=cap if data_read
                         else None)
        first = r.rows[0] if r.rows else {}
        return ProbeRow(surface, FETCHED if r.rows else EMPTY,
                        f"{len(r.rows)} rows" + (f" · {json.dumps(first, default=str)[:200]}"
                                                 if first else ""),
                        estimate, {"job_id": r.job_id, "rows": len(r.rows)})

    rows.append(_attempt("connect · SELECT 1", lambda: q(
        "connect · SELECT 1", "SELECT 1 AS ok", [])))
    for dataset in (L, D):
        rows.append(_attempt(f"datasets.get {P}.{dataset}", lambda d=dataset:
                             ProbeRow(f"datasets.get {P}.{d}", FETCHED,
                                      (client.get_dataset(P, d).get("location")
                                       or "?"))))
    for layer, dataset, table in (("logical", L, spec.name),
                                  ("physical", D, PH)):
        def get(d=dataset, t=table, layer=layer) -> ProbeRow:
            payload = client.get_table(P, d, t)
            tp = payload.get("timePartitioning") or {}
            return ProbeRow(
                f"tables.get {layer} {d}.{t}", FETCHED,
                f"{payload.get('type')} · rows {payload.get('numRows', '?')}"
                f" · bytes {payload.get('numBytes', '?')} · partition "
                f"{tp.get('field') or tp.get('type') or 'none'} · "
                f"requirePartitionFilter="
                f"{payload.get('requirePartitionFilter', False)}",
                data={"type": payload.get("type"),
                      "timePartitioning": tp,
                      "clustering": payload.get("clustering"),
                      "requirePartitionFilter":
                          payload.get("requirePartitionFilter")})
        rows.append(_attempt(f"tables.get {layer} {dataset}.{table}", get))
    for layer, dataset, table in (("logical", L, spec.name),
                                  ("physical", D, PH)):
        sql, params = S.shared_columns(P, dataset, [table])
        rows.append(_attempt(f"INFORMATION_SCHEMA.COLUMNS {dataset}",
                             lambda s=sql, p=params, d=dataset: q(
                                 f"INFORMATION_SCHEMA.COLUMNS {d}", s, p)))
    sql, params = S.shared_partitions(P, D, [PH])
    rows.append(_attempt("INFORMATION_SCHEMA.PARTITIONS physical",
                         lambda: q("INFORMATION_SCHEMA.PARTITIONS physical",
                                   sql, params)))
    sql, params = S.shared_views(P, L, [spec.name])
    rows.append(_attempt("INFORMATION_SCHEMA.VIEWS logical",
                         lambda: q("INFORMATION_SCHEMA.VIEWS logical", sql,
                                   params)))
    sql, params = S.shared_constraints(P, D, [PH])
    rows.append(_attempt("INFORMATION_SCHEMA.TABLE_CONSTRAINTS physical",
                         lambda: q("INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
                                   "physical", sql, params)))
    sql, params = S.shared_table_storage(P, cfg.region, [L, D],
                                         [spec.name, PH])
    rows.append(_attempt("INFORMATION_SCHEMA.TABLE_STORAGE (region)",
                         lambda: q("INFORMATION_SCHEMA.TABLE_STORAGE (region)",
                                   sql, params)))
    sql, params = S.table_metrics_routine(P, L, cfg.table_metrics_routine,
                                          spec.name)
    rows.append(_attempt(f"routine {L}.{cfg.table_metrics_routine}",
                         lambda: q(f"routine {L}.{cfg.table_metrics_routine}",
                                   sql, params)))
    for layer, dataset, table in (("logical", L, spec.name),
                                  ("physical", D, PH)):
        rows.append(_attempt(
            f"rowAccessPolicies.list {layer}",
            lambda d=dataset, t=table, layer=layer: ProbeRow(
                f"rowAccessPolicies.list {layer}", FETCHED,
                f"{len(client.list_row_access_policies(P, d, t))} policies")))
    perms = ["bigquery.tables.get", "bigquery.tables.getData",
             "bigquery.tables.list", "bigquery.rowAccessPolicies.list",
             "bigquery.tables.getIamPolicy"]
    rows.append(_attempt(
        "tables.testIamPermissions physical",
        lambda: ProbeRow("tables.testIamPermissions physical", FETCHED,
                         ", ".join(client.test_iam_permissions(
                             P, D, PH, perms)) or "(none granted)")))

    # dry runs: what the view reads, what the base table costs
    def view_dry() -> ProbeRow:
        sql, params = S.probe_select(P, L, spec.name)
        r = client.dry_run(sql, params, scope="probe",
                           operation="dry run view")
        refs = [".".join(str(x.get(k, "")) for k in
                         ("projectId", "datasetId", "tableId"))
                for x in r.referenced_tables]
        return ProbeRow("dry run SELECT * FROM logical view", FETCHED,
                        f"{fmt_bytes(r.bytes_processed)} full · reads "
                        f"{refs or '(itself)'}", r.bytes_processed,
                        {"referenced_tables": refs})
    rows.append(_attempt("dry run SELECT * FROM logical view", view_dry))

    def phys_dry() -> ProbeRow:
        sql, params = S.probe_select(P, D, PH)
        r = client.dry_run(sql, params, scope="probe",
                           operation="dry run physical")
        return ProbeRow("dry run SELECT * FROM physical table", FETCHED,
                        f"{fmt_bytes(r.bytes_processed)} full scan",
                        r.bytes_processed)
    rows.append(_attempt("dry run SELECT * FROM physical table", phys_dry))
    rows.append(_attempt("data read · COUNT(*) logical view", lambda: q(
        "data read · COUNT(*) logical view",
        f"SELECT COUNT(*) AS n FROM {S.fqn(P, L, spec.name)}", [],
        data_read=True)))

    # history: where the analysts' queries actually are
    views = [(p, "JOBS_BY_PROJECT") for p in cfg.job_projects]
    if cfg.scan_organization_jobs:
        views.append((cfg.data_project, "JOBS_BY_ORGANIZATION"))
    for project, view in views:
        surface = f"{view} in {project} ({history_days}d)"
        sql, params = S.probe_jobs_visible(project, cfg.region, view,
                                           history_days)
        params = [p for p in params if p["name"] != "self"] + [
            p_str("self", self_email or "")]

        def visible(s=sql, p=params, surface=surface) -> ProbeRow:
            est = client.dry_run(s, p, scope="probe",
                                 operation=surface).bytes_processed
            r = client.query(s, p, scope="probe", operation=surface,
                             timeout_s=300)
            row = r.rows[0] if r.rows else {}
            jobs = int(row.get("jobs") or 0)
            others = int(row.get("jobs_by_others") or 0)
            status = FETCHED if jobs else EMPTY
            verdict = ("only OUR jobs — this is the billing project's view"
                       if jobs and not others else
                       f"{others} jobs by others · "
                       f"{row.get('users')} users")
            return ProbeRow(surface, status,
                            f"{jobs} query jobs · {verdict} · "
                            f"{fmt_bytes(est)}/query", est, dict(row))
        rows.append(_attempt(surface, visible))
        surface2 = f"{view} in {project} · jobs touching in-scope tables"
        sql2, params2 = S.probe_jobs_touching(project, cfg.region, view,
                                              max(history_days, 30), names)

        def touching(s=sql2, p=params2, surface=surface2) -> ProbeRow:
            r = client.query(s, p, scope="probe", operation=surface,
                             timeout_s=300)
            top = [f"{x.get('dataset_id')}.{x.get('table_id')}="
                   f"{x.get('jobs')}" for x in r.rows[:8]]
            return ProbeRow(surface, FETCHED if r.rows else EMPTY,
                            ", ".join(top) or "none in 30d",
                            data={"rows": r.rows[:50]})
        rows.append(_attempt(surface2, touching))

    if cfg.audit_log_table:
        project, dataset, table = cfg.audit_log_table.split(".")
        day = ((now or _dt.datetime.now(_dt.timezone.utc))
               - _dt.timedelta(days=1))
        probe_table = (table[:-1] + day.strftime("%Y%m%d")
                       if table.endswith("*") else table)

        def audit() -> ProbeRow:
            resource = client.get_table(project, dataset, probe_table)
            fields = {f.get("name"): f for f in
                      (resource.get("schema") or {}).get("fields") or []}
            sub = {f.get("name") for f in
                   (fields.get("protopayload_auditlog") or {}).get("fields")
                   or []}
            fmt = (S.AUDIT_FORMAT_METADATA_JSON if "metadataJson" in sub
                   else S.AUDIT_FORMAT_SERVICEDATA_V1
                   if "servicedata_v1_bigquery" in sub else "")
            if not fmt:
                return ProbeRow("audit_log sink", "ERROR",
                                "no BigQuery audit payload in the sink schema")
            d0 = day.strftime("%Y-%m-%d 00:00:00+00")
            d1 = (day + _dt.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00+00")
            sql, params = S.audit_log_day(
                cfg.audit_log_table, fmt, cfg.audit_log_partition_column,
                d0, d1, day.strftime("%Y%m%d"), names)
            est = client.dry_run(sql, params, scope="probe",
                                 operation="audit_log day").bytes_processed
            tp = resource.get("timePartitioning") or {}
            return ProbeRow("audit_log sink", FETCHED,
                            f"format {fmt} · partitioned on "
                            f"{tp.get('field') or tp.get('type') or 'none'}"
                            f" · one day ≈ {fmt_bytes(est)} · "
                            f"{cfg.history_days} days ≈ "
                            f"{fmt_bytes(est * cfg.history_days)}",
                            est, {"format": fmt, "day_bytes": est})
        rows.append(_attempt("audit_log sink", audit))
    else:
        rows.append(ProbeRow("audit_log sink", "SKIPPED",
                             "defaults.audit_log_table not set"))
    return rows


def render_probe(rows: list[ProbeRow]) -> str:
    width = max(len(r.surface) for r in rows) + 2
    glyph = {"FETCHED": "✅", "EMPTY": "◦ ", "DENIED": "⛔", "NOT_FOUND": "∅ ",
             "BUDGET_SKIPPED": "$ ", "INVALID": "✗ ", "ERROR": "❌",
             "TRANSPORT": "⚡", "QUOTA": "⏳", "SKIPPED": "— "}
    out = []
    for r in rows:
        out.append(f"{glyph.get(r.status, '? ')} {r.status:<15} "
                   f"{r.surface:<{width}} {r.detail}")
    counts: dict[str, int] = {}
    for r in rows:
        counts[r.status] = counts.get(r.status, 0) + 1
    out.append("")
    out.append("summary: " + " · ".join(f"{k} {v}"
                                        for k, v in sorted(counts.items())))
    return "\n".join(out)
