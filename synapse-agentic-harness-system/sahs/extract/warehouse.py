"""The orchestrator — phases, checkpoints, writers, reports.

    phase 0  connect      BQConnection.from_env() → token → SELECT 1
    phase 1  shared       dataset-level INFORMATION_SCHEMA for ALL tables
                          in one statement per view, split per table
    phase 2  resources    tables.get on both layers, the physical table
                          behind each view, row policies, table metrics
    phase 3  history      one statement per source per UTC day into
                          _history/, then the local indexer routes the
                          corpus into every table's 17_queries_30d/
                          (before profiling: it must never be starved)
    phase 4  profile      budget-planned column statistics on the physical
                          table (or the view with a partition window),
                          then exact value domains for low-cardinality
                          columns — full history where it is affordable
    phase 5  report       _summary.json per table, _batch_summary.*,
                          _run_report.json/.md

Every operation ends in one archive status — FETCHED · CACHED · EMPTY ·
DENIED · NOT_FOUND · BUDGET_SKIPPED · ERROR — logged to the run's
events.jsonl and counted in the run report. ``_state.json`` remembers
finished tasks so a restart reuses them (``--fresh`` forgets).
"""

from __future__ import annotations

import csv
import datetime as _dt
import json
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from sahs.extract import bq_sql as S
from sahs.extract.bq_rest import (
    BQError, BQRest, BUDGET_SKIPPED, Budget, CACHED, DENIED, EMPTY, ERROR,
    FETCHED, INVALID, NOT_FOUND, QUOTA, QueryResult, TRANSPORT)
from sahs.extract.config import RunConfig, TableSpec, fmt_bytes, usd_of
from sahs.extract.history import (
    DigestLimits, SOURCE_AUDIT, SOURCE_JOBS_BY_ORG, SOURCE_JOBS_BY_PROJECT,
    index_history, write_jsonl_gz)

SCRIPT_VERSION = "bq_warehouse_extract/1.0"

_PARTITION_ERR = re.compile(
    r"Cannot query over table '([\w\-.$]+)' without a filter over "
    r"column\(s\) '([^']+)'", re.I)
_PARTITION_TYPES = {"DAY", "MONTH", "YEAR", "HOUR"}

EXIT_OK = 0
EXIT_GATE_FAILURE = 1
EXIT_VALIDATION_ERROR = 2
EXIT_ENV_AUTH = 3
EXIT_INTERRUPTED = 4


def utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def iso(ts: _dt.datetime) -> str:
    return ts.isoformat(timespec="seconds").replace("+00:00", "Z")


# ── atomic writers ──

def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str,
                              ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def write_csv(path: Path, rows: list[dict],
              fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        seen: dict[str, None] = {}
        for row in rows:
            for k in row:
                seen.setdefault(k, None)
        fieldnames = list(seen)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: _cell(v) for k, v in row.items()})
    os.replace(tmp, path)


def _cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, ensure_ascii=False)
    if isinstance(value, bool):
        return "YES" if value else "NO"
    return value


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


# ── the event log ──

class EventLog:
    """One line on the terminal, one record in events.jsonl — the same
    vocabulary as the archive contract."""

    def __init__(self, run_dir: Path, run_id: str, *, quiet: bool = False,
                 stream=None) -> None:
        self.run_id = run_id
        self.quiet = quiet
        self.stream = stream or sys.stderr
        self._lock = threading.Lock()
        logs = run_dir / "_run_logs"
        logs.mkdir(parents=True, exist_ok=True)
        self.log_path = logs / f"{run_id}.log"
        self.events_path = logs / f"{run_id}.events.jsonl"
        self._log = self.log_path.open("a", encoding="utf-8")
        self._events = self.events_path.open("a", encoding="utf-8")
        self.counts: dict[str, int] = {}

    def emit(self, state: str, scope: str, operation: str, message: str = "",
             *, job_id: str = "", bytes_processed: int | None = None,
             bytes_billed: int | None = None, count: bool = True) -> None:
        record = {"ts": iso(utc_now()), "state": state, "scope": scope,
                  "operation": operation, "message": message}
        if job_id:
            record["job_id"] = job_id
        if bytes_processed is not None:
            record["bytes_processed"] = bytes_processed
        if bytes_billed is not None:
            record["bytes_billed"] = bytes_billed
        line = f"[{state}] {scope} · {operation}"
        if bytes_billed:
            line += f" · {fmt_bytes(bytes_billed)}"
        if message:
            line += f" · {message}"
        if job_id:
            line += f" · {job_id}"
        with self._lock:
            if count:
                self.counts[state] = self.counts.get(state, 0) + 1
            self._log.write(line + "\n")
            self._log.flush()
            self._events.write(json.dumps(record, ensure_ascii=False) + "\n")
            self._events.flush()
            if not self.quiet:
                print(line, file=self.stream, flush=True)

    def close(self) -> None:
        with self._lock:
            self._log.close()
            self._events.close()


STATUS_ORDER = (FETCHED, CACHED, EMPTY, DENIED, NOT_FOUND, BUDGET_SKIPPED,
                ERROR, INVALID, QUOTA, TRANSPORT)


# ── checkpoint state ──

class State:
    """``_state.json``: finished tasks keyed ``<scope>:<operation>``
    with the job id and artifact that proved them. Thread-safe;
    written through on every change (a crash loses nothing)."""

    def __init__(self, path: Path, *, fresh: bool = False) -> None:
        self.path = path
        self._lock = threading.Lock()
        self.data: dict[str, Any] = {"schema": "bq_warehouse_state/1",
                                     "tasks": {}, "completed": [],
                                     "phase": "start"}
        if path.exists() and not fresh:
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict) and "tasks" in loaded:
                    self.data = loaded
            except (ValueError, OSError):
                pass

    def done(self, key: str, signature: str = "") -> dict | None:
        """The finished task, or None — also None when the caller's
        ``signature`` (what produced the artifact: the table set a
        history day was filtered by, a profile's columns and budget, a
        domain's threshold) differs from the recorded one."""
        with self._lock:
            info = self.data["tasks"].get(key)
            if info is None:
                return None
            if signature and info.get("signature", "") != signature:
                return None
            return info

    def mark(self, key: str, **info: Any) -> None:
        with self._lock:
            self.data["tasks"][key] = {"ts": iso(utc_now()), **info}
            self._flush()

    def forget_prefix(self, prefix: str) -> int:
        with self._lock:
            keys = [k for k in self.data["tasks"] if k.startswith(prefix)]
            for k in keys:
                del self.data["tasks"][k]
            self.data["completed"] = [c for c in self.data["completed"]
                                      if not c.startswith(prefix.rstrip(":"))]
            self._flush()
            return len(keys)

    def complete_table(self, table: str) -> None:
        with self._lock:
            if table not in self.data["completed"]:
                self.data["completed"].append(table)
            self._flush()

    def set_phase(self, phase: str) -> None:
        with self._lock:
            self.data["phase"] = phase
            self._flush()

    def _flush(self) -> None:
        write_json(self.path, self.data)


# ── outcomes ──

@dataclass
class Outcome:
    status: str
    scope: str
    operation: str
    message: str = ""
    job_id: str = ""
    rows: int = 0
    bytes_processed: int = 0
    bytes_billed: int = 0
    artifact: str = ""
    error: str = ""

    def to_json(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v not in ("", 0)
                or k in ("status", "scope", "operation")}


@dataclass
class TableContext:
    spec: TableSpec
    dir: Path
    logical_resource: dict | None = None
    physical_resource: dict | None = None
    physical_status: str = ""
    physical_resolution: str = ""
    physical_candidates: list[str] = field(default_factory=list)
    logical_columns: list[dict] = field(default_factory=list)
    physical_columns: list[dict] = field(default_factory=list)
    physical_partitions: list[dict] = field(default_factory=list)
    metrics: list[dict] = field(default_factory=list)
    outcomes: list[Outcome] = field(default_factory=list)
    profile_summary: dict = field(default_factory=dict)
    history_summary: dict = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def logical_exists(self) -> bool:
        return self.logical_resource is not None

    @property
    def physical_exists(self) -> bool:
        return self.physical_resource is not None

    @property
    def logical_type(self) -> str:
        return str((self.logical_resource or {}).get("type") or "")

    @property
    def physical_ref(self) -> tuple[str, str, str]:
        """(project, dataset, table) of the storage object actually
        found — the configured name, or the one the view's dry run
        resolved to."""
        ref = (self.physical_resource or {}).get("tableReference") or {}
        return (str(ref.get("projectId") or self.spec.data_project),
                str(ref.get("datasetId") or self.spec.physical_dataset),
                str(ref.get("tableId") or self.spec.physical_name))

    @property
    def physical_fqn(self) -> str:
        return ".".join(self.physical_ref)

    def status_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for o in self.outcomes:
            counts[o.status] = counts.get(o.status, 0) + 1
        return counts


# ── the extractor ──

class WarehouseExtractor:
    def __init__(self, cfg: RunConfig, client: BQRest, *,
                 run_id: str = "", fresh: bool = False,
                 force_tables: list[str] | None = None,
                 skip_profile: bool = False, skip_history: bool = False,
                 skip_shared: bool = False, plan_only: bool = False,
                 quiet: bool = False, stream=None,
                 now: Callable[[], _dt.datetime] = utc_now) -> None:
        self.cfg = cfg
        self.client = client
        self.now = now
        self.run_id = run_id or (self.now().strftime("%Y%m%dT%H%M%SZ")
                                 + "_" + uuid.uuid4().hex[:8])
        self.root = Path(cfg.output_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.state = State(self.root / "_state.json", fresh=fresh)
        for name in force_tables or []:
            self.state.forget_prefix(f"{name}:")
        self.log = EventLog(self.root, self.run_id, quiet=quiet,
                            stream=stream)
        self.budget = Budget(cfg.run_budget_bytes)
        self.skip_profile = skip_profile
        self.skip_history = skip_history
        self.skip_shared = skip_shared
        self.plan_only = plan_only
        self.started = self.now()
        self.tables: dict[str, TableContext] = {
            t.name: TableContext(spec=t, dir=self.root / t.name)
            for t in cfg.tables}
        self.shared_dir = self.root / "_shared"
        self.history_dir = self.root / "_history"
        self.denied: list[dict] = []
        self.not_found: list[dict] = []
        self.failures: list[dict] = []
        self.tasks: list[dict] = []
        self.sa_email = ""
        self.interrupted = False
        self._lock = threading.Lock()
        # what a shared pull or a history day was filtered by: a changed
        # table list invalidates them (a new table must get its history)
        self.scope_signature = _signature(sorted(
            n for t in cfg.tables for n in t.match_names))

    # ── helpers ──
    def _record(self, ctx: TableContext | None, outcome: Outcome) -> Outcome:
        with self._lock:
            if ctx is not None:
                ctx.outcomes.append(outcome)
            self.tasks.append(outcome.to_json())
            entry = {"table": outcome.scope, "operation": outcome.operation,
                     "error": outcome.error or outcome.message}
            if outcome.status == DENIED:
                self.denied.append(entry)
            elif outcome.status == NOT_FOUND:
                self.not_found.append(entry)
            elif outcome.status in (ERROR, INVALID, QUOTA, TRANSPORT):
                self.failures.append(entry)
        self.log.emit(outcome.status, outcome.scope, outcome.operation,
                      outcome.message, job_id=outcome.job_id,
                      bytes_processed=outcome.bytes_processed or None,
                      bytes_billed=outcome.bytes_billed or None)
        return outcome

    def _run_query(self, sql: str, params: list[dict], *, scope: str,
                   operation: str, costs_bytes: bool = False,
                   budget_bytes: int = 0, timeout_s: int = 0
                   ) -> tuple[QueryResult | None, Outcome]:
        """Dry run (when the statement reads data), reserve against the
        run budget, run, settle. Metadata statements skip the dry run."""
        estimate = 0
        if costs_bytes:
            try:
                dry = self.client.dry_run(sql, params, scope=scope,
                                          operation=operation)
                estimate = dry.bytes_processed
            except BQError as e:
                return None, Outcome(e.status, scope, operation,
                                     f"dry run: {e.message[:300]}",
                                     error=e.message)
            if budget_bytes and estimate > budget_bytes:
                return None, Outcome(
                    BUDGET_SKIPPED, scope, operation,
                    f"estimate {fmt_bytes(estimate)} exceeds budget "
                    f"{fmt_bytes(budget_bytes)}", bytes_processed=estimate)
            if self.plan_only:
                return None, Outcome(
                    "PLANNED", scope, operation,
                    f"would scan {fmt_bytes(estimate)}",
                    bytes_processed=estimate)
            if not self.budget.try_reserve(estimate):
                return None, Outcome(
                    BUDGET_SKIPPED, scope, operation,
                    f"estimate {fmt_bytes(estimate)} exceeds remaining run "
                    f"budget {fmt_bytes(self.budget.remaining)}",
                    bytes_processed=estimate)
        elif self.plan_only:
            # metadata reads are free: run them even in plan mode, so
            # the plan knows the schemas, partitions and sizes it needs
            pass
        try:
            result = self.client.query(
                sql, params, scope=scope, operation=operation,
                timeout_s=timeout_s or self.cfg.query_timeout_s,
                max_bytes_billed=(max(budget_bytes, estimate * 2, 1 << 30)
                                  if costs_bytes else None))
        except BQError as e:
            if costs_bytes:
                self.budget.release(estimate)
            return None, Outcome(e.status, scope, operation,
                                 e.message[:300], job_id=e.job_id,
                                 error=e.message)
        if costs_bytes:
            self.budget.settle(estimate, result.bytes_billed)
        status = FETCHED if result.rows else EMPTY
        return result, Outcome(status, scope, operation,
                               f"{len(result.rows)} rows"
                               + (" (cache)" if result.cache_hit else ""),
                               job_id=result.job_id, rows=len(result.rows),
                               bytes_processed=result.bytes_processed,
                               bytes_billed=result.bytes_billed)

    def _cached(self, ctx: TableContext | None, key: str, operation: str,
                artifact: Path | None, signature: str = "") -> bool:
        done = self.state.done(key, signature)
        if done and (artifact is None or artifact.exists()):
            self._record(ctx, Outcome(CACHED, key.split(":")[0], operation,
                                      done.get("message", ""),
                                      job_id=done.get("job_id", "")))
            return True
        return False

    # ── phase 0 ──
    def connect(self) -> None:
        self.log.emit("START", "run", "connect",
                      f"{SCRIPT_VERSION} · billing {self.client.project} · "
                      f"data {self.cfg.data_project} · "
                      f"{len(self.cfg.tables)} tables · budget "
                      f"{fmt_bytes(self.cfg.run_budget_bytes)}",
                      count=False)
        key = self.client.connection.key_path
        if key is not None:
            try:
                info = json.loads(Path(key).read_text(encoding="utf-8"))
                self.sa_email = str(info.get("client_email") or "").lower()
            except (OSError, ValueError):
                self.sa_email = ""
        result = self.client.query("SELECT 1 AS ok", scope="run",
                                   operation="connect", timeout_s=120)
        self.log.emit(FETCHED, "run", "connect",
                      f"job {result.job_id} · identity "
                      f"{self.sa_email or '(unknown)'}", count=False)
        self.state.set_phase("connected")

    # ── phase 1 ──
    def phase_shared(self) -> None:
        self.state.set_phase("shared")
        names = sorted({t.name for t in self.cfg.tables}
                       | {t.physical_name for t in self.cfg.tables})
        project = self.cfg.data_project
        layers = (("logical", self.cfg.logical_dataset),
                  ("physical", self.cfg.physical_dataset))
        for layer, dataset in layers:
            self._shared_dataset_resource(layer, dataset)
            for view, builder in (("tables", S.shared_tables),
                                  ("columns", S.shared_columns),
                                  ("column_field_paths", S.shared_field_paths),
                                  ("table_options", S.shared_table_options),
                                  ("views", S.shared_views),
                                  ("partitions", S.shared_partitions),
                                  ("constraints", S.shared_constraints)):
                if layer == "logical" and view == "partitions":
                    continue           # views carry no partitions
                if layer == "physical" and view == "views":
                    continue
                sql, params = builder(project, dataset, names)
                self._shared_pull(f"{layer}_{view}", sql, params)
        sql, params = S.shared_routines(project, self.cfg.logical_dataset)
        self._shared_pull("routines", sql, params)
        sql, params = S.shared_table_storage(
            project, self.cfg.region,
            [self.cfg.logical_dataset, self.cfg.physical_dataset], names)
        self._shared_pull("table_storage", sql, params)
        self._split_shared()

    def _shared_dataset_resource(self, layer: str, dataset: str) -> None:
        key = f"_shared:dataset_{dataset}"
        artifact = self.shared_dir / f"dataset_{dataset}.json"
        if self._cached(None, key, f"datasets.get {dataset}", artifact):
            return
        try:
            payload = self.client.get_dataset(self.cfg.data_project, dataset)
        except BQError as e:
            self._record(None, Outcome(e.status, "_shared",
                                       f"datasets.get {dataset}",
                                       e.message[:300], error=e.message))
            return
        write_json(artifact, payload)
        self.state.mark(key, message=f"{layer} dataset")
        self._record(None, Outcome(FETCHED, "_shared",
                                   f"datasets.get {dataset}", layer,
                                   artifact=str(artifact)))

    def _shared_pull(self, name: str, sql: str, params: list[dict]) -> None:
        key = f"_shared:{name}"
        artifact = self.shared_dir / f"{name}.json"
        if self._cached(None, key, name, artifact, self.scope_signature):
            return
        result, outcome = self._run_query(sql, params, scope="_shared",
                                          operation=name)
        if result is not None:
            write_json(artifact, result.rows)
            write_csv(self.shared_dir / f"{name}.csv", result.rows)
            outcome.artifact = str(artifact)
            self.state.mark(key, job_id=result.job_id,
                            message=f"{len(result.rows)} rows",
                            signature=self.scope_signature)
        self._record(None, outcome)

    def _shared_rows(self, name: str) -> list[dict]:
        path = self.shared_dir / f"{name}.json"
        if not path.exists():
            return []
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            return []

    def _split_shared(self) -> None:
        """Distribute the shared pulls into every table's 01-12 files."""
        splits = {
            "logical_tables": ("01_logical_table_meta", "table_name"),
            "logical_columns": ("02_logical_columns", "table_name"),
            "logical_column_field_paths": ("03_logical_column_field_paths",
                                           "table_name"),
            "logical_table_options": ("04_logical_table_options",
                                      "table_name"),
            "physical_tables": ("06_physical_table_meta", "table_name"),
            "physical_columns": ("07_physical_columns", "table_name"),
            "physical_column_field_paths": ("08_physical_column_field_paths",
                                            "table_name"),
            "physical_table_options": ("09_physical_table_options",
                                       "table_name"),
            "physical_partitions": ("10_physical_partitions", "table_name"),
        }
        shared = {name: self._shared_rows(name) for name in splits}
        shared["logical_views"] = self._shared_rows("logical_views")
        shared["logical_constraints"] = self._shared_rows(
            "logical_constraints")
        shared["physical_constraints"] = self._shared_rows(
            "physical_constraints")
        for ctx in self.tables.values():
            ctx.dir.mkdir(parents=True, exist_ok=True)
            for source, (stem, key) in splits.items():
                physical = source.startswith("physical")
                wanted = (ctx.spec.physical_name if physical
                          else ctx.spec.name).lower()
                rows = [r for r in shared[source]
                        if str(r.get(key, "")).lower() == wanted]
                if source == "logical_columns":
                    ctx.logical_columns = rows
                elif source == "physical_columns":
                    ctx.physical_columns = rows
                elif source == "physical_partitions":
                    ctx.physical_partitions = rows
                if rows or (self.shared_dir / f"{source}.json").exists():
                    write_csv(ctx.dir / f"{stem}.csv", rows)
                    write_json(ctx.dir / f"{stem}.json", rows)
            views = [r for r in shared["logical_views"]
                     if str(r.get("table_name", "")).lower()
                     == ctx.spec.name.lower()]
            if views:
                write_csv(ctx.dir / "05_view_definition.csv", views)
                write_json(ctx.dir / "05_view_definition.json", views[0])
                ddl = views[0].get("view_definition") or ""
                if ddl:
                    write_text(ctx.dir / "05_view_definition.sql", str(ddl))
            for layer, stem in (("logical", "11_logical_constraints"),
                                ("physical", "12_physical_constraints")):
                wanted = (ctx.spec.physical_name if layer == "physical"
                          else ctx.spec.name).lower()
                rows = [r for r in shared[f"{layer}_constraints"]
                        if str(r.get("table_name", "")).lower() == wanted]
                if (self.shared_dir / f"{layer}_constraints.json").exists():
                    write_json(ctx.dir / f"{stem}.json",
                               _constraints_payload(rows))
                    write_csv(ctx.dir / f"{stem}.csv", rows)

    # ── phase 2 ──
    def phase_resources(self) -> None:
        self.state.set_phase("resources")
        self._parallel(self._table_resources, "resources")

    def _table_resources(self, ctx: TableContext) -> None:
        spec = ctx.spec
        ctx.logical_resource = self._table_get(
            ctx, "logical", spec.data_project, spec.logical_dataset,
            spec.name, ctx.dir / "00_logical_table_resource.json")
        self._resolve_physical(ctx)
        if ctx.physical_exists:
            self._row_policies(ctx, "physical", *ctx.physical_ref)
        elif ctx.logical_exists and ctx.logical_type != "VIEW":
            self._row_policies(ctx, "logical", spec.data_project,
                               spec.logical_dataset, spec.name)
        self._table_metrics(ctx)

    def _table_get(self, ctx: TableContext, layer: str, project: str,
                   dataset: str, table: str, artifact: Path) -> dict | None:
        key = f"{ctx.name}:tables.get:{layer}"
        operation = f"tables.get {layer}"
        done = self.state.done(key)
        if done and artifact.exists():
            self._record(ctx, Outcome(CACHED, ctx.name, operation,
                                      done.get("message", "")))
            try:
                return json.loads(artifact.read_text(encoding="utf-8"))
            except ValueError:
                pass
        if done and done.get("status") in (NOT_FOUND, DENIED):
            self._record(ctx, Outcome(done["status"], ctx.name, operation,
                                      done.get("message", "")))
            return None
        try:
            payload = self.client.get_table(project, dataset, table)
        except BQError as e:
            self._record(ctx, Outcome(e.status, ctx.name, operation,
                                      e.message[:300], error=e.message))
            if e.status in (NOT_FOUND, DENIED):
                self.state.mark(key, status=e.status,
                                message=e.message[:300])
            return None
        write_json(artifact, payload)
        self.state.mark(key, status=FETCHED,
                        message=f"{payload.get('type', '?')} "
                                f"{project}.{dataset}.{table}")
        self._record(ctx, Outcome(FETCHED, ctx.name, operation,
                                  f"{payload.get('type', '?')} "
                                  f"{dataset}.{table}",
                                  artifact=str(artifact)))
        return payload

    def _resolve_physical(self, ctx: TableContext) -> None:
        """The storage object behind the logical name. Same name in the
        physical dataset first; otherwise the view's own dry run names
        the tables it reads (referencedTables), and a partition-filter
        refusal names the base table in its message."""
        spec = ctx.spec
        artifact = ctx.dir / "00_physical_table_resource.json"
        ctx.physical_resource = self._table_get(
            ctx, "physical", spec.data_project, spec.physical_dataset,
            spec.physical_name, artifact)
        if ctx.physical_resource is not None:
            ctx.physical_resolution = "same_name"
            ctx.physical_status = FETCHED
            return
        last = next((o for o in reversed(ctx.outcomes)
                     if o.operation == "tables.get physical"), None)
        ctx.physical_status = last.status if last else ERROR
        if not ctx.logical_exists or ctx.physical_status == DENIED:
            ctx.physical_resolution = "unresolved"
            return
        sql, params = S.probe_select(spec.data_project, spec.logical_dataset,
                                     spec.name)
        candidates: list[str] = []
        resolution = "unresolved"
        try:
            dry = self.client.dry_run(sql, params, scope=ctx.name,
                                      operation="view dry run")
            for ref in dry.referenced_tables:
                fq = ".".join(str(ref.get(k, "")) for k in
                              ("projectId", "datasetId", "tableId"))
                if fq.lower() != spec.logical_fqn.lower():
                    candidates.append(fq)
            resolution = "view_reference" if candidates else "no_reference"
        except BQError as e:
            m = _PARTITION_ERR.search(e.message)
            if m:
                candidates.append(m.group(1).replace(":", "."))
                resolution = "partition_error"
            else:
                resolution = f"dry_run_{e.status.lower()}"
        ctx.physical_candidates = candidates
        ctx.physical_resolution = resolution
        # a single candidate in the data project is the physical table
        same_project = [c for c in candidates
                        if c.lower().startswith(spec.data_project.lower()
                                                + ".")]
        if len(same_project) == 1:
            project, dataset, table = same_project[0].split(".")
            payload = None
            try:
                payload = self.client.get_table(project, dataset, table)
            except BQError as e:
                self._record(ctx, Outcome(e.status, ctx.name,
                                          "tables.get physical(resolved)",
                                          e.message[:300], error=e.message))
            if payload is not None:
                write_json(artifact, payload)
                ctx.physical_resource = payload
                ctx.physical_status = FETCHED
                self._record(ctx, Outcome(
                    FETCHED, ctx.name, "tables.get physical(resolved)",
                    f"{payload.get('type', '?')} {dataset}.{table} via "
                    f"{resolution}", artifact=str(artifact)))
                self.state.mark(f"{ctx.name}:tables.get:physical",
                                status=FETCHED,
                                message=f"resolved {same_project[0]}")
        if candidates:
            write_json(ctx.dir / "00_physical_candidates.json",
                       {"resolution": resolution, "candidates": candidates})
        self.log.emit("INFO", ctx.name, "physical resolution",
                      f"{resolution}: {candidates or 'none'}", count=False)

    def _row_policies(self, ctx: TableContext, layer: str, project: str,
                      dataset: str, table: str) -> None:
        key = f"{ctx.name}:rowAccessPolicies.list"
        artifact = ctx.dir / "16_row_access_policies.json"
        operation = "rowAccessPolicies.list"
        done = self.state.done(key)
        if done and done.get("status") == DENIED:
            self._record(ctx, Outcome(DENIED, ctx.name, operation,
                                      done.get("message", "")))
            return
        if done and artifact.exists():
            self._record(ctx, Outcome(CACHED, ctx.name, operation,
                                      done.get("message", "")))
            return
        try:
            policies = self.client.list_row_access_policies(project, dataset,
                                                            table)
        except BQError as e:
            self._record(ctx, Outcome(e.status, ctx.name, operation,
                                      f"{layer}: {e.message[:300]}",
                                      error=e.message))
            if e.status == DENIED:
                self.state.mark(key, status=DENIED, message=e.message[:300])
            return
        write_json(artifact, policies)
        self.state.mark(key, status=FETCHED if policies else EMPTY,
                        message=f"{len(policies)} policies ({layer})")
        self._record(ctx, Outcome(FETCHED if policies else EMPTY, ctx.name,
                                  operation, f"{len(policies)} policies "
                                  f"({layer})", artifact=str(artifact)))

    def _table_metrics(self, ctx: TableContext) -> None:
        spec = ctx.spec
        key = f"{ctx.name}:table_metrics"
        artifact = ctx.dir / "13_table_metrics.json"
        if self._cached(ctx, key, "table metrics", artifact):
            try:
                ctx.metrics = json.loads(artifact.read_text(encoding="utf-8"))
            except ValueError:
                ctx.metrics = []
            return
        rows: list[dict] = []
        source = ""
        sql, params = S.table_metrics_routine(
            spec.data_project, spec.logical_dataset,
            self.cfg.table_metrics_routine, spec.name)
        result, outcome = self._run_query(sql, params, scope=ctx.name,
                                          operation="table metrics routine")
        self._record(ctx, outcome)
        if result is not None and result.rows:
            rows, source = result.rows, self.cfg.table_metrics_routine
        if not rows:
            phys_names = {spec.physical_name.lower(),
                          ctx.physical_ref[2].lower()}
            storage = [r for r in self._shared_rows("table_storage")
                       if str(r.get("table_name", "")).lower()
                       in phys_names | {spec.name.lower()}]
            storage.sort(key=lambda r: 0 if r.get("table_schema")
                         == spec.physical_dataset else 1)
            if storage:
                r = storage[0]
                rows = [{"table_name": spec.name,
                         "total_rows": r.get("total_rows"),
                         "table_size_bytes": r.get("total_logical_bytes"),
                         "total_physical_bytes": r.get("total_physical_bytes"),
                         "storage_last_modified":
                             r.get("storage_last_modified_time"),
                         "table_schema": r.get("table_schema")}]
                source = "INFORMATION_SCHEMA.TABLE_STORAGE"
        if not rows and ctx.physical_resource:
            p = ctx.physical_resource
            if p.get("numRows") is not None:
                rows = [{"table_name": spec.name,
                         "total_rows": int(p.get("numRows") or 0),
                         "table_size_bytes": int(p.get("numBytes") or 0),
                         "last_modified":
                             _ms_iso(p.get("lastModifiedTime"))}]
                source = "tables.get physical"
        if rows:
            for r in rows:
                r.setdefault("table_name", spec.name)
                r["metrics_source"] = source
            write_json(artifact, rows)
            write_csv(ctx.dir / "13_table_metrics.csv", rows)
            ctx.metrics = rows
            self.state.mark(key, message=f"via {source}")
            self._record(ctx, Outcome(FETCHED, ctx.name, "table metrics",
                                      f"via {source}", artifact=str(artifact)))
        else:
            self._record(ctx, Outcome(EMPTY, ctx.name, "table metrics",
                                      "no source answered"))

    # ── phase 3 ──
    def phase_profile(self) -> None:
        self.state.set_phase("profile")
        self._parallel(self._table_profile, "profile")

    def _profile_columns(self, ctx: TableContext) -> tuple[list[dict], str]:
        """Prefer the physical schema (what the profiler reads); the
        logical one when the view is the target."""
        if ctx.physical_exists and ctx.physical_columns:
            return ctx.physical_columns, "physical"
        return ctx.logical_columns, "logical"

    def _plan_target(self, ctx: TableContext, columns: list[dict],
                     budget: int) -> tuple[S.ProfileTarget | None, dict]:
        """Decide what to scan. Returns (target, plan)."""
        spec = ctx.spec
        physical = ctx.physical_resource or {}
        use_physical = ctx.physical_exists and physical.get("type") == "TABLE"
        p_project, p_dataset, p_table = ctx.physical_ref
        target = S.ProfileTarget(
            project=p_project if use_physical else spec.data_project,
            dataset=p_dataset if use_physical else spec.logical_dataset,
            table=p_table if use_physical else spec.name)
        part_col, part_type, col_type = _partitioning(physical, columns)
        available = {str(c.get("column_name", "")).lower() for c in columns}
        if part_col and part_col.lower() not in available \
                and part_col not in ("_PARTITIONTIME", "_PARTITIONDATE"):
            part_col = ""                # the view hides the column
        target.partition_column = part_col
        target.partition_type = part_type
        plan: dict[str, Any] = {
            "target": target.fqn.replace("`", ""),
            "target_layer": "physical" if use_physical else "logical",
            "partition_column": part_col or None,
            "partition_type": part_type or None,
            "profile_budget_bytes": budget,
            "candidates": []}
        sample = columns[:min(len(columns), self.cfg.profile_chunk_columns)]
        pairs = [(str(c["column_name"]), str(c.get("data_type", "STRING")))
                 for c in sample]

        def estimate(t: S.ProfileTarget) -> int | None:
            sql, _ = S.profile_chunk(t, pairs)
            try:
                return self.client.dry_run(sql, scope=ctx.name,
                                           operation="profile plan"
                                           ).bytes_processed
            except BQError as e:
                plan["candidates"].append({"coverage_mode": t.coverage_mode,
                                           "error": e.message[:300]})
                return None

        # 1. the whole object (unpartitioned or every non-NULL partition)
        full = S.ProfileTarget(**{**target.__dict__})
        if part_col:
            full.coverage_mode = "full_non_null_partition_history"
            full.partition_predicate = S.partition_not_null_predicate(part_col)
        else:
            full.coverage_mode = "unpartitioned"
        full_bytes = estimate(full)
        if full_bytes is None and part_col:
            # require_partition_filter refuses IS NOT NULL: estimate from
            # the partition inventory instead
            full_bytes = sum(int(r.get("total_logical_bytes") or 0)
                             for r in ctx.physical_partitions) or None
        plan["full_history_bytes"] = full_bytes
        plan["full_history_usd"] = usd_of(full_bytes)
        chunk_count = max(1, -(-len(columns) // self.cfg.profile_chunk_columns))
        plan["profile_chunks"] = chunk_count
        plan["candidates"].append({"coverage_mode": full.coverage_mode,
                                   "bytes": full_bytes})
        if full_bytes is not None and full_bytes * chunk_count <= budget:
            plan["chosen"] = full.coverage_mode
            plan["planned_bytes"] = full_bytes * chunk_count
            return full, plan

        # 2. the most recent partitions that fit
        if part_col:
            ids = _partition_window_ids(ctx.physical_partitions, part_type)
            # the rungs: recent-N windows that exist; the whole inventory
            # only when the full-history dry run could not be made
            rungs = sorted({min(n, len(ids)) for n in (365, 180, 90, 30, 7, 1)
                            if ids}, reverse=True)
            if full_bytes is not None and ids:
                rungs = [n for n in rungs if n < len(ids)] or rungs[-1:]
            for n in rungs:
                window = ids[-n:]
                low, high = _partition_bounds(window, part_type)
                cand = S.ProfileTarget(**{**target.__dict__})
                cand.coverage_mode = "recent_partitions_budgeted"
                cand.partition_ids = window
                cand.partition_low, cand.partition_high = low, high
                cand.partition_predicate = S.partition_predicate(
                    part_col, part_type, low, high, col_type)
                b = estimate(cand)
                plan["candidates"].append({"coverage_mode": cand.coverage_mode,
                                           "partitions": n, "bytes": b})
                if b is not None and b * chunk_count <= budget:
                    plan["chosen"] = cand.coverage_mode
                    plan["planned_bytes"] = b * chunk_count
                    return cand, plan
                if b is not None and n == 1 and use_physical:
                    pct = max(0.01, min(99.0, budget / max(b * chunk_count, 1)
                                        * 100))
                    cand.coverage_mode = \
                        "single_partition_system_sample_budgeted"
                    cand.sample_percent = round(pct, 3)
                    b2 = estimate(cand)
                    plan["candidates"].append(
                        {"coverage_mode": cand.coverage_mode,
                         "sample_percent": cand.sample_percent, "bytes": b2})
                    plan["chosen"] = cand.coverage_mode
                    plan["planned_bytes"] = (b2 or 0) * chunk_count
                    return cand, plan
        # 3. unpartitioned but over budget: sample the physical table
        if use_physical and full_bytes:
            pct = max(0.01, min(99.0, budget / max(full_bytes * chunk_count, 1)
                                * 100))
            cand = S.ProfileTarget(**{**target.__dict__})
            cand.coverage_mode = "system_sample_budgeted"
            cand.sample_percent = round(pct, 3)
            b = estimate(cand)
            plan["candidates"].append({"coverage_mode": cand.coverage_mode,
                                       "sample_percent": cand.sample_percent,
                                       "bytes": b})
            plan["chosen"] = cand.coverage_mode
            plan["planned_bytes"] = (b or 0) * chunk_count
            return cand, plan
        # 4. a view we cannot bound: refuse rather than scan blind
        plan["chosen"] = None
        plan["planned_bytes"] = None
        plan["reason"] = ("no partition column exposed and TABLESAMPLE is "
                          "not allowed on a view; raise profile_budget or "
                          "set physical_dataset/physical_name")
        return None, plan

    def _table_profile(self, ctx: TableContext) -> None:
        spec = ctx.spec
        if not spec.profile or self.skip_profile:
            self._record(ctx, Outcome("SKIPPED", ctx.name, "profile",
                                      "profiling disabled"))
            return
        if not (ctx.logical_exists or ctx.physical_exists):
            self._record(ctx, Outcome(NOT_FOUND, ctx.name, "profile",
                                      "no logical or physical object"))
            return
        columns, layer = self._profile_columns(ctx)
        supported = [c for c in columns
                     if S.base_type(str(c.get("data_type", ""))) in
                     S.PROFILE_TYPES]
        unsupported = [{"column_name": c.get("column_name"),
                        "data_type": c.get("data_type")}
                       for c in columns if c not in supported]
        if not supported:
            self._record(ctx, Outcome(EMPTY, ctx.name, "profile",
                                      "no supported scalar columns"))
            return
        budget = spec.profile_budget_bytes or self.cfg.profile_budget_bytes
        key = f"{ctx.name}:profile"
        profile_csv = ctx.dir / "14_column_profile.csv"
        t0 = time.monotonic()
        profile_sig = _signature([c.get("column_name") for c in supported],
                                 budget, self.cfg.profile_chunk_columns)
        if self._cached(ctx, key, "profile", profile_csv, profile_sig):
            summary_path = ctx.dir / "_profile_summary.json"
            if summary_path.exists():
                try:
                    ctx.profile_summary = json.loads(
                        summary_path.read_text(encoding="utf-8"))
                except ValueError:
                    pass
            self._table_domains(ctx, supported, layer)
            return
        target, plan = self._plan_target(ctx, supported, budget)
        plan.update({"schema_layer": layer,
                     "supported_columns": len(supported),
                     "unsupported_columns": unsupported,
                     "planned_usd": usd_of(plan.get("planned_bytes")),
                     "budget_snapshot": self.budget.snapshot()})
        write_json(ctx.dir / "14_profile_plan.json", plan)
        if target is None:
            self._record(ctx, Outcome(BUDGET_SKIPPED, ctx.name, "profile",
                                      plan.get("reason", "no target")))
            return
        write_json(ctx.dir / "14_profile_coverage.json", target.describe())
        self.log.emit("COST_PLAN", ctx.name, "profile",
                      f"{target.coverage_mode} · planned "
                      f"{fmt_bytes(plan.get('planned_bytes'))} of full "
                      f"{fmt_bytes(plan.get('full_history_bytes'))} · "
                      f"{plan['profile_chunks']} chunks", count=False)
        if self.plan_only:
            self._record(ctx, Outcome("PLANNED", ctx.name, "profile",
                                      f"{target.coverage_mode} "
                                      f"{fmt_bytes(plan.get('planned_bytes'))}"))
            return
        chunks_dir = ctx.dir / "_profile_chunks"
        profile_rows: list[dict] = []
        chunk_results: list[dict] = []
        total_rows = None
        size = self.cfg.profile_chunk_columns
        for i in range(0, len(supported), size):
            chunk = supported[i:i + size]
            pairs = [(str(c["column_name"]), str(c.get("data_type", "STRING")))
                     for c in chunk]
            sql, prefixes = S.profile_chunk(target, pairs)
            n = i // size + 1
            result, outcome = self._run_query(
                sql, [], scope=ctx.name, operation=f"profile chunk {n}",
                costs_bytes=True, budget_bytes=budget)
            self._record(ctx, outcome)
            chunk_results.append({"chunk": n, "columns": len(chunk),
                                  **outcome.to_json()})
            if result is None or not result.rows:
                if outcome.status == BUDGET_SKIPPED:
                    break
                continue
            row = result.rows[0]
            write_json(chunks_dir / f"chunk_{n:04d}.json",
                       {"sql": sql, "row": row, "job_id": result.job_id,
                        "bytes_billed": result.bytes_billed})
            total_rows = int(row.get("total_rows") or 0)
            for (name, dtype), p in zip(pairs, prefixes):
                nulls = int(row.get(f"{p}_nulls") or 0)
                profile_rows.append({
                    "column_name": name, "data_type": dtype,
                    "total_rows": total_rows, "null_count": nulls,
                    "non_null_count": total_rows - nulls,
                    "null_pct": (round(nulls / total_rows * 100, 4)
                                 if total_rows else None),
                    "approx_distinct": int(row.get(f"{p}_distinct") or 0),
                    "min_value": row.get(f"{p}_min"),
                    "max_value": row.get(f"{p}_max"),
                    "avg_value": row.get(f"{p}_avg"),
                    "coverage_mode": target.coverage_mode,
                    "partition_predicate": target.partition_predicate or None,
                    "sample_percent": target.sample_percent,
                    "profile_job_id": result.job_id})
        if profile_rows:
            write_csv(profile_csv, profile_rows)
            write_json(ctx.dir / "14_column_profile.json", profile_rows)
        summary = {
            "table": ctx.name, "source": target.fqn.replace("`", ""),
            "coverage": target.describe(), "supported_columns": len(supported),
            "profiled_columns": len(profile_rows),
            "total_rows_in_coverage": total_rows,
            "chunks": chunk_results,
            "full_history_bytes": plan.get("full_history_bytes"),
            "planned_bytes": plan.get("planned_bytes"),
            "elapsed_s": round(time.monotonic() - t0, 2),
            "budget_snapshot": self.budget.snapshot()}
        ctx.profile_summary = summary
        write_json(ctx.dir / "_profile_summary.json", summary)
        if profile_rows and len(profile_rows) == len(supported):
            self.state.mark(key, message=f"{len(profile_rows)} columns "
                                         f"{target.coverage_mode}",
                            signature=profile_sig)
        self._table_domains(ctx, supported, layer, target=target)

    def _table_domains(self, ctx: TableContext, supported: list[dict],
                       layer: str, target: S.ProfileTarget | None = None
                       ) -> None:
        """Exact value domains for the low-cardinality candidates."""
        spec = ctx.spec
        profile_csv = ctx.dir / "14_column_profile.csv"
        if not profile_csv.exists():
            return
        with profile_csv.open(encoding="utf-8", newline="") as f:
            profile = list(csv.DictReader(f))
        threshold = spec.low_card_threshold
        candidates = [r for r in profile
                      if 0 < int(float(r.get("approx_distinct") or 0))
                      <= threshold * 1.2]      # HLL++ error margin
        manifest_path = ctx.dir / "15_low_cardinality_manifest.csv"
        key = f"{ctx.name}:domains"
        budget = spec.domain_budget_bytes or self.cfg.domain_budget_bytes
        domain_sig = _signature([r["column_name"] for r in candidates],
                                threshold, budget)
        if self._cached(ctx, key, "value domains", manifest_path,
                        domain_sig):
            return
        if not candidates:
            write_csv(manifest_path, [], _MANIFEST_FIELDS)
            write_json(ctx.dir / "15_low_cardinality_manifest.json", [])
            self.state.mark(key, message="no candidates",
                            signature=domain_sig)
            self._record(ctx, Outcome(EMPTY, ctx.name, "value domains",
                                      "no low-cardinality candidates"))
            return
        if target is None:
            coverage = ctx.dir / "14_profile_coverage.json"
            try:
                d = json.loads(coverage.read_text(encoding="utf-8"))
                project, dataset, table = d["target"].split(".")
                target = S.ProfileTarget(
                    project=project, dataset=dataset, table=table,
                    coverage_mode=d.get("coverage_mode", ""),
                    partition_column=d.get("partition_column") or "",
                    partition_predicate=d.get("partition_predicate") or "",
                    sample_percent=float(d.get("sample_percent") or 100))
            except (OSError, ValueError, KeyError):
                self._record(ctx, Outcome(ERROR, ctx.name, "value domains",
                                          "no profile coverage on disk"))
                return
        # full history for the narrow columns when it fits (section 22
        # of the contract): a categorical domain observed over every
        # partition, without paying for the wide profile at that depth
        full = S.ProfileTarget(**{**target.__dict__})
        if target.partition_column and target.coverage_mode not in (
                "unpartitioned", "full_non_null_partition_history"):
            full.coverage_mode = "full_non_null_partition_history_value_domain"
            full.partition_predicate = S.partition_not_null_predicate(
                target.partition_column)
            full.sample_percent = 100.0
            full.partition_ids = []
        elif target.sample_percent < 100:
            full.coverage_mode = "full_value_domain"
            full.sample_percent = 100.0
        values_dir = ctx.dir / "15_low_cardinality_values"
        manifest: list[dict] = []
        names = [r["column_name"] for r in candidates]
        types = {r["column_name"]: r.get("data_type", "") for r in candidates}
        approx = {r["column_name"]: int(float(r.get("approx_distinct") or 0))
                  for r in candidates}
        size = self.cfg.domain_group_columns
        limit = threshold + 1
        for g in range(0, len(names), size):
            group = names[g:g + size]
            n = g // size + 1
            chosen = target
            sql = S.domain_group(full, group, limit)
            if full.coverage_mode != target.coverage_mode:
                try:
                    est = self.client.dry_run(sql, scope=ctx.name,
                                              operation=f"domain plan {n}"
                                              ).bytes_processed
                    if est <= budget:
                        chosen = full
                except BQError:
                    pass
            if chosen is target:
                sql = S.domain_group(target, group, limit)
            result, outcome = self._run_query(
                sql, [], scope=ctx.name, operation=f"domain group {n}",
                costs_bytes=True, budget_bytes=budget)
            self._record(ctx, outcome)
            write_json(values_dir / f"_group_{n:04d}.json",
                       {"columns": group, "coverage": chosen.describe(),
                        **outcome.to_json()})
            if result is None:
                for col in group:
                    manifest.append(_manifest_row(
                        col, types[col], approx[col], None, False,
                        threshold, 0, "", outcome.job_id, chosen,
                        outcome.status))
                continue
            by_col: dict[str, list[dict]] = {c: [] for c in group}
            for row in result.rows:
                by_col.setdefault(str(row.get("column_name")), []).append(row)
            for col in group:
                rows = sorted(by_col.get(col, []),
                              key=lambda r: -int(r.get("value_count") or 0))
                distinct_non_null = sum(1 for r in rows if not r.get("is_null"))
                if len(rows) > threshold:
                    manifest.append(_manifest_row(
                        col, types[col], approx[col], None, False,
                        threshold, 0, "", result.job_id, chosen, FETCHED))
                    continue
                total = sum(int(r.get("value_count") or 0) for r in rows)
                values = []
                for rank, r in enumerate(rows, 1):
                    count = int(r.get("value_count") or 0)
                    values.append({
                        "column_name": col, "data_type": types[col],
                        "value": r.get("value"),
                        "is_null": bool(r.get("is_null")),
                        "value_count": count,
                        "pct_of_rows": (round(count / total * 100, 4)
                                        if total else 0.0),
                        "rank": rank,
                        "exact_distinct_non_null": distinct_non_null,
                        "total_rows": total,
                        "coverage_mode": chosen.coverage_mode,
                        "partition_predicate":
                            chosen.partition_predicate or None,
                        "sample_percent": chosen.sample_percent})
                safe = re.sub(r"[^A-Za-z0-9_.\-]", "_", col)
                write_csv(values_dir / f"{safe}.csv", values,
                          _VALUE_FIELDS)
                write_json(values_dir / f"{safe}.json", values)
                manifest.append(_manifest_row(
                    col, types[col], approx[col], distinct_non_null, True,
                    threshold, len(values),
                    f"15_low_cardinality_values/{safe}.csv", result.job_id,
                    chosen, FETCHED))
        write_csv(manifest_path, manifest, _MANIFEST_FIELDS)
        write_json(ctx.dir / "15_low_cardinality_manifest.json", manifest)
        confirmed = sum(1 for m in manifest if m["low_cardinality"] == "YES")
        if ctx.profile_summary:
            ctx.profile_summary.update({
                "low_cardinality_candidates": len(candidates),
                "low_cardinality_confirmed": confirmed})
            write_json(ctx.dir / "_profile_summary.json", ctx.profile_summary)
        if all(m["status"] in (FETCHED, EMPTY) for m in manifest):
            self.state.mark(key, message=f"{confirmed}/{len(candidates)} "
                                         "confirmed", signature=domain_sig)
        self._record(ctx, Outcome(FETCHED if confirmed else EMPTY, ctx.name,
                                  "value domains",
                                  f"{confirmed} of {len(candidates)} "
                                  "candidates confirmed"))

    # ── phase 4 ──
    def history_window(self) -> tuple[_dt.datetime, _dt.datetime]:
        end = self.started.replace(hour=0, minute=0, second=0, microsecond=0) \
            + _dt.timedelta(days=1)
        start = end - _dt.timedelta(days=self.cfg.history_days)
        return start, end

    def phase_history(self) -> None:
        self.state.set_phase("history")
        if self.skip_history:
            self.log.emit("SKIPPED", "_history", "history", "disabled",
                          count=False)
            return
        start, end = self.history_window()
        names = sorted({n for t in self.cfg.tables for n in t.match_names})
        days = [start + _dt.timedelta(days=i)
                for i in range(self.cfg.history_days)]
        today = self.started.strftime("%Y-%m-%d")
        sources: list[tuple[str, str, Callable]] = []
        for project in self.cfg.job_projects:
            sources.append((
                f"jobs_by_project/{project}", project,
                lambda d0, d1, p=project: S.jobs_by_project_day(
                    p, self.cfg.region, d0, d1, names)))
        if self.cfg.scan_organization_jobs:
            sources.append((
                "jobs_by_organization", self.cfg.data_project,
                lambda d0, d1: S.jobs_by_organization_day(
                    self.cfg.data_project, self.cfg.region, d0, d1, names)))
        audit_format = self._audit_format() if self.cfg.audit_log_table \
            else ""
        if audit_format:
            sources.append((
                "audit_log", self.cfg.audit_log_table,
                lambda d0, d1, fmt=audit_format: S.audit_log_day(
                    self.cfg.audit_log_table, fmt,
                    self.cfg.audit_log_partition_column, d0, d1,
                    d0[:10].replace("-", ""), names)))
        # a source that is DENIED on its first day is not asked 29 more
        # times: the first day of every source runs alone, so one status
        # is recorded once and the fan-out never touches a dead source
        dead: set[str] = set()
        jobs: list[tuple[str, str, Callable, _dt.datetime]] = []
        for key, label, builder in sources:
            self._history_day(key, label, builder, days[0], today, dead)
            if self.interrupted:
                return
            for day in days[1:]:
                jobs.append((key, label, builder, day))
        pool = ThreadPoolExecutor(max_workers=self.cfg.history_concurrency)
        try:
            futures = {}
            for key, label, builder, day in jobs:
                futures[pool.submit(self._history_day, key, label, builder,
                                    day, today, dead)] = (key, day)
            for fut in as_completed(futures):
                if self.interrupted:
                    break
                fut.result()
        except KeyboardInterrupt:
            self.interrupted = True
            pool.shutdown(wait=False, cancel_futures=True)
            raise
        pool.shutdown(wait=True)
        self._index_history()

    def _audit_format(self) -> str:
        project, dataset, table = self.cfg.audit_log_table.split(".")
        probe_table = table
        if table.endswith("*"):
            probe_table = table[:-1] + (self.started
                                        - _dt.timedelta(days=1)
                                        ).strftime("%Y%m%d")
        key = "_history:audit_format"
        done = self.state.done(key)
        if done and done.get("format"):
            return str(done["format"])
        try:
            resource = self.client.get_table(project, dataset, probe_table)
        except BQError as e:
            self._record(None, Outcome(e.status, "_history",
                                       "audit_log tables.get",
                                       e.message[:300], error=e.message))
            return ""
        fields = {f.get("name"): f for f in
                  (resource.get("schema") or {}).get("fields") or []}
        proto = fields.get("protopayload_auditlog") or {}
        sub = {f.get("name") for f in proto.get("fields") or []}
        if "metadataJson" in sub:
            fmt = S.AUDIT_FORMAT_METADATA_JSON
        elif "servicedata_v1_bigquery" in sub:
            fmt = S.AUDIT_FORMAT_SERVICEDATA_V1
        else:
            self._record(None, Outcome(ERROR, "_history", "audit_log schema",
                                       "no BigQuery audit payload fields "
                                       "in the sink table"))
            return ""
        write_json(self.history_dir / "audit_log" / "_schema.json", resource)
        self.state.mark(key, format=fmt)
        self.log.emit("INFO", "_history", "audit_log format", fmt, count=False)
        return fmt

    def _history_day(self, key: str, label: str, builder: Callable,
                     day: _dt.datetime, today: str, dead: set[str]) -> None:
        day_str = day.strftime("%Y-%m-%d")
        scope = "_history"
        operation = f"{key} {day_str}"
        path = self.history_dir / key / f"{day_str}.jsonl.gz"
        state_key = f"_history:{key}:{day_str}"
        if key in dead:
            return
        done = self.state.done(state_key, self.scope_signature)
        if done and path.exists() and not done.get("partial"):
            self._record(None, Outcome(CACHED, scope, operation,
                                       done.get("message", "")))
            return
        d0 = day.strftime("%Y-%m-%d %H:%M:%S+00")
        d1 = (day + _dt.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S+00")
        sql, params = builder(d0, d1)
        result, outcome = self._run_query(
            sql, params, scope=scope, operation=operation, costs_bytes=True,
            budget_bytes=self.cfg.history_day_budget_bytes)
        if outcome.status in (DENIED, NOT_FOUND, INVALID):
            dead.add(key)
            outcome.message = (f"{outcome.message} — source {key} "
                               "disabled for the run")
        self._record(None, outcome)
        if result is None:
            return
        n = write_jsonl_gz(path, result.rows)
        self.state.mark(state_key, job_id=result.job_id, rows=n,
                        message=f"{n} rows", partial=(day_str == today),
                        signature=self.scope_signature)

    def _index_history(self) -> None:
        start, end = self.history_window()
        limits = DigestLimits(top_users=self.cfg.top_users_retained,
                              co_queried=self.cfg.co_queried_retained,
                              failed_queries=self.cfg.failed_queries_retained,
                              templates=self.cfg.templates_retained)
        excluded = list(self.cfg.exclude_users)
        if self.sa_email:
            excluded.append(self.sa_email)
        report = index_history(
            self.history_dir, self.cfg.tables, self.root,
            data_project=self.cfg.data_project, exclude_users=excluded,
            history_days=self.cfg.history_days, limits=limits,
            window_start=iso(start), window_end=iso(end))
        for name, summary in report["tables"].items():
            ctx = self.tables.get(name)
            if ctx is None:
                continue
            ctx.history_summary = summary
            status = FETCHED if (summary["jobs_rows"] or summary["audit_rows"]) \
                else EMPTY
            self._record(ctx, Outcome(
                status, name, "history index",
                f"jobs {summary['jobs_rows']} · audit {summary['audit_rows']}"
                f" · users {summary['distinct_users']}"))
        self.log.emit("INFO", "_history", "corpus",
                      json.dumps(report["corpus"], default=str), count=False)

    # ── phase 5 ──
    def phase_report(self, status: str = "complete") -> dict:
        self.state.set_phase("report")
        rows = []
        for ctx in self.tables.values():
            summary = self._table_summary(ctx)
            rows.append(summary)
            write_json(ctx.dir / "_summary.json", summary)
            if status == "complete":
                self.state.complete_table(ctx.name)
        batch = [{
            "table": r["table"],
            "logical_exists": r["logical_exists"],
            "logical_type": r["logical_type"],
            "physical_exists": r["physical_exists"],
            "physical_resolution": r["physical_resolution"],
            "columns": r["metadata"]["logical_columns"],
            "profiled_columns": r["profile"].get("profiled_columns", 0),
            "profile_coverage": r["profile"].get("coverage_mode"),
            "low_cardinality_columns":
                r["profile"].get("low_cardinality_confirmed", 0),
            "jobs_30d": r["queries_30d"].get("jobs_rows", 0),
            "audit_30d": r["queries_30d"].get("audit_rows", 0),
            "distinct_query_users_30d":
                r["queries_30d"].get("distinct_users", 0),
            "bytes_billed": r["bytes_billed"],
            "statuses": json.dumps(r["status_counts"], sort_keys=True),
        } for r in rows]
        write_json(self.root / "_batch_summary.json", batch)
        write_csv(self.root / "_batch_summary.csv", batch)
        ended = self.now()
        counts = dict(self.log.counts)
        report = {
            "run_id": self.run_id,
            "script_version": SCRIPT_VERSION,
            "status": status,
            "started": iso(self.started),
            "ended": iso(ended),
            "elapsed_s": round((ended - self.started).total_seconds(), 1),
            "identity": self.sa_email or None,
            "billing_project": self.client.project,
            "data_project": self.cfg.data_project,
            "logical_dataset": self.cfg.logical_dataset,
            "physical_dataset": self.cfg.physical_dataset,
            "region": self.cfg.region,
            "job_projects": self.cfg.job_projects,
            "audit_log_table": self.cfg.audit_log_table or None,
            "history_days": self.cfg.history_days,
            "history_window": [iso(x) for x in self.history_window()],
            "configured_tables": len(self.cfg.tables),
            "tables": [t.name for t in self.cfg.tables],
            "status_counts": counts,
            "total_bytes_processed": self.client.ledger.bytes_processed,
            "total_bytes_billed": self.client.ledger.bytes_billed,
            "on_demand_equivalent_usd": usd_of(
                self.client.ledger.bytes_billed),
            "budget": self.budget.snapshot(),
            "bytes_by_scope": self.client.ledger.by_scope(),
            "denied_operations": self.denied,
            "missing_objects": self.not_found,
            "failures": self.failures,
            "tasks": self.tasks,
            "jobs": self.client.ledger.snapshot(),
        }
        write_json(self.root / "_run_report.json", report)
        write_text(self.root / "_run_report.md", render_report_md(report,
                                                                  batch))
        self.log.emit("COMPLETE" if status == "complete" else status.upper(),
                      "run", "report",
                      f"{fmt_bytes(report['total_bytes_billed'])} billed · "
                      f"${report['on_demand_equivalent_usd']:.2f} on-demand "
                      f"equivalent · "
                      + " ".join(f"{k}={v}" for k, v in sorted(counts.items())),
                      count=False)
        return report

    def _table_summary(self, ctx: TableContext) -> dict:
        profile = dict(ctx.profile_summary or {})
        coverage = (profile.get("coverage") or {})
        by_scope = self.client.ledger.by_scope().get(ctx.name, {})
        return {
            "table": ctx.name,
            "logical": ctx.spec.logical_fqn,
            "physical": ctx.physical_fqn if ctx.physical_exists else None,
            "logical_exists": ctx.logical_exists,
            "logical_type": ctx.logical_type or None,
            "physical_exists": ctx.physical_exists,
            "physical_resolution": ctx.physical_resolution or None,
            "physical_candidates": ctx.physical_candidates,
            "metadata": {
                "logical_columns": len(ctx.logical_columns),
                "physical_columns": len(ctx.physical_columns),
                "physical_partitions": len(ctx.physical_partitions),
                "table_metrics": ctx.metrics[0] if ctx.metrics else None,
            },
            "profile": {
                "profiled_columns": profile.get("profiled_columns", 0),
                "supported_columns": profile.get("supported_columns", 0),
                "coverage_mode": coverage.get("coverage_mode"),
                "partition_predicate": coverage.get("partition_predicate"),
                "low_cardinality_candidates":
                    profile.get("low_cardinality_candidates", 0),
                "low_cardinality_confirmed":
                    profile.get("low_cardinality_confirmed", 0),
                "planned_bytes": profile.get("planned_bytes"),
                "full_history_bytes": profile.get("full_history_bytes"),
            },
            "queries_30d": {k: ctx.history_summary.get(k)
                            for k in ("jobs_rows", "audit_rows",
                                      "distinct_users", "first_seen",
                                      "last_seen", "history_days")},
            "bytes_billed": by_scope.get("bytes_billed", 0),
            "jobs_run": by_scope.get("jobs", 0),
            "status_counts": ctx.status_counts(),
            "operations": [o.to_json() for o in ctx.outcomes],
        }

    # ── driver ──
    def _parallel(self, fn: Callable[[TableContext], None], phase: str
                  ) -> None:
        pool = ThreadPoolExecutor(max_workers=self.cfg.concurrency)
        try:
            futures = {pool.submit(self._guard, fn, ctx): ctx
                       for ctx in self.tables.values()}
            for fut in as_completed(futures):
                if self.interrupted:
                    break
                fut.result()
        except KeyboardInterrupt:
            # Ctrl-C: nothing queued starts, running jobs finish their
            # (bounded) statement, the checkpoint keeps what landed
            self.interrupted = True
            pool.shutdown(wait=False, cancel_futures=True)
            raise
        pool.shutdown(wait=True)

    def _guard(self, fn: Callable[[TableContext], None],
               ctx: TableContext) -> None:
        if self.interrupted:
            return
        try:
            fn(ctx)
        except BQError as e:                       # a leak = a bug, logged
            self._record(ctx, Outcome(e.status, ctx.name, fn.__name__,
                                      e.message[:300], error=e.message))
        except Exception as e:                     # noqa: BLE001
            self._record(ctx, Outcome(ERROR, ctx.name, fn.__name__,
                                      f"{type(e).__name__}: {e}"[:300],
                                      error=repr(e)))

    def run(self) -> dict:
        try:
            self.connect()
            if not self.skip_shared:
                self.phase_shared()
            else:
                self._split_shared()
            self.phase_resources()
            # history BEFORE profiling: the JOBS views are metadata and the
            # audit sink is cheap per day; the profile is the one real
            # scan and must never starve the 30-day corpus of budget
            self.phase_history()
            self.phase_profile()
            return self.phase_report("complete")
        except KeyboardInterrupt:
            self.interrupted = True
            self.log.emit("INTERRUPTED", "run", "signal",
                          "checkpoint kept; rerun resumes", count=False)
            return self.phase_report("interrupted")
        finally:
            self.log.close()


# ── module helpers ──

_MANIFEST_FIELDS = ["column_name", "data_type", "approx_distinct",
                    "exact_distinct_non_null", "distinct_estimate",
                    "low_cardinality", "profiled", "threshold", "value_rows",
                    "artifact", "job_id", "coverage_mode",
                    "partition_predicate", "sample_percent", "status"]
_VALUE_FIELDS = ["column_name", "data_type", "value", "is_null",
                 "value_count", "pct_of_rows", "rank",
                 "exact_distinct_non_null", "total_rows", "coverage_mode",
                 "partition_predicate", "sample_percent"]


def _manifest_row(col: str, dtype: str, approx: int, exact: int | None,
                  low: bool, threshold: int, value_rows: int, artifact: str,
                  job_id: str, target: S.ProfileTarget, status: str) -> dict:
    return {"column_name": col, "data_type": dtype, "approx_distinct": approx,
            "exact_distinct_non_null": exact,
            "distinct_estimate": exact if exact is not None else approx,
            "low_cardinality": "YES" if low else "NO",
            "profiled": "YES" if status == FETCHED else "NO",
            "threshold": threshold, "value_rows": value_rows,
            "artifact": artifact, "job_id": job_id,
            "coverage_mode": target.coverage_mode,
            "partition_predicate": target.partition_predicate or None,
            "sample_percent": target.sample_percent, "status": status}


def _constraints_payload(rows: list[dict]) -> dict:
    """INFORMATION_SCHEMA constraint rows → the shape the loader parses
    (primary_key.columns / foreign_keys[]); confirmed-empty stays a
    dict with empty members, never a missing file."""
    pk: list[str] = []
    fks: dict[str, dict] = {}
    for r in rows:
        ctype = str(r.get("constraint_type") or "").upper()
        col = r.get("column_name")
        if ctype == "PRIMARY KEY" and col:
            pk.append(str(col))
        elif ctype == "FOREIGN KEY":
            name = str(r.get("constraint_name") or "")
            fk = fks.setdefault(name, {"name": name, "columns": [],
                                       "referenced_table": "",
                                       "referenced_columns": []})
            if col:
                fk["columns"].append(str(col))
            if r.get("referenced_table"):
                fk["referenced_table"] = ".".join(
                    p for p in (r.get("referenced_schema"),
                                r.get("referenced_table")) if p)
            if r.get("referenced_column"):
                fk["referenced_columns"].append(str(r["referenced_column"]))
    return {"primary_key": {"columns": pk},
            "foreign_keys": list(fks.values()),
            "rows": rows}


def _signature(*parts: Any) -> str:
    """A short stable hash of what produced an artifact."""
    import hashlib
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str)
                          .encode("utf-8")).hexdigest()[:16]


def _ms_iso(value: Any) -> str | None:
    try:
        return iso(_dt.datetime.fromtimestamp(int(value) / 1000,
                                              tz=_dt.timezone.utc))
    except (TypeError, ValueError):
        return None


def _partitioning(resource: dict, columns: list[dict]
                  ) -> tuple[str, str, str]:
    """(partition column, partition type, column data type) from the
    physical resource, else from the schema's is_partitioning_column."""
    tp = resource.get("timePartitioning") or {}
    rp = resource.get("rangePartitioning") or {}
    col = ""
    ptype = ""
    if tp:
        col = tp.get("field") or "_PARTITIONTIME"
        ptype = str(tp.get("type") or "DAY").upper()
    elif rp:
        col = rp.get("field") or ""
        ptype = "RANGE"
    if not col:
        for c in columns:
            if str(c.get("is_partitioning_column", "")).upper() \
                    in ("YES", "TRUE"):
                col = str(c.get("column_name"))
                ptype = ptype or "DAY"
                break
    col_type = next((str(c.get("data_type", "")) for c in columns
                     if str(c.get("column_name", "")).lower() == col.lower()),
                    "TIMESTAMP" if col == "_PARTITIONTIME" else "DATE")
    return col, ptype, col_type


def _partition_window_ids(partitions: list[dict], ptype: str) -> list[str]:
    ids = sorted({str(r.get("partition_id")) for r in partitions
                  if r.get("partition_id") not in (None, "", "__NULL__",
                                                   "__UNPARTITIONED__")})
    return ids


def _partition_bounds(ids: list[str], ptype: str) -> tuple[str, str]:
    """partition ids → inclusive [low, high] literals for the predicate."""
    def as_date(pid: str) -> str:
        if ptype == "RANGE":
            return pid
        if len(pid) == 4:                                  # YEAR
            return f"{pid}-01-01"
        if len(pid) == 6:                                  # MONTH
            return f"{pid[:4]}-{pid[4:6]}-01"
        if len(pid) >= 8:                                  # DAY / HOUR
            return f"{pid[:4]}-{pid[4:6]}-{pid[6:8]}"
        return pid
    low, high = as_date(ids[0]), as_date(ids[-1])
    if ptype == "MONTH" and len(ids[-1]) == 6:
        y, m = int(ids[-1][:4]), int(ids[-1][4:6])
        nxt = _dt.date(y + (m // 12), (m % 12) + 1, 1) - _dt.timedelta(days=1)
        high = nxt.isoformat()
    elif ptype == "YEAR" and len(ids[-1]) == 4:
        high = f"{ids[-1]}-12-31"
    return low, high


def render_report_md(report: dict, batch: list[dict]) -> str:
    lines = [f"# Warehouse extraction run `{report['run_id']}`", "",
             f"- status: **{report['status']}** · started {report['started']}"
             f" · elapsed {report['elapsed_s']}s",
             f"- identity: `{report.get('identity') or '?'}` · billing "
             f"`{report['billing_project']}` · data `{report['data_project']}`"
             f" (`{report['logical_dataset']}` → "
             f"`{report['physical_dataset']}`)",
             f"- history: {report['history_days']} days "
             f"{report['history_window'][0]} → {report['history_window'][1]}"
             f" · job projects {', '.join(report['job_projects'])}"
             + (f" · audit `{report['audit_log_table']}`"
                if report.get('audit_log_table') else " · audit: off"),
             f"- scanned: {fmt_bytes(report['total_bytes_processed'])} "
             f"processed · {fmt_bytes(report['total_bytes_billed'])} billed"
             f" · ${report['on_demand_equivalent_usd']:.2f} on-demand "
             f"equivalent (informational, $6.25/TiB) · budget "
             f"{fmt_bytes(report['budget']['total_bytes'])}",
             "- statuses: " + ", ".join(
                 f"{k} {v}" for k, v in sorted(
                     report["status_counts"].items())),
             ""]
    lines += ["| Table | Logical | Physical | Cols | Profiled | Coverage | "
              "Low-card | Jobs 30d | Audit 30d | Users | Billed |",
              "|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|"]
    for r in batch:
        lines.append(
            f"| {r['table']} | {r['logical_type'] or '—'} | "
            f"{'yes' if r['physical_exists'] else r['physical_resolution'] or 'no'}"
            f" | {r['columns']} | {r['profiled_columns']} | "
            f"{r['profile_coverage'] or '—'} | {r['low_cardinality_columns']}"
            f" | {r['jobs_30d']} | {r['audit_30d']} | "
            f"{r['distinct_query_users_30d']} | {fmt_bytes(r['bytes_billed'])} |")
    if report["denied_operations"]:
        lines += ["", "## Denied (unknown, NOT absent)", ""]
        for d in report["denied_operations"]:
            lines.append(f"- `{d['table']}` · {d['operation']} — "
                         f"{d['error'][:160]}")
    if report["missing_objects"]:
        lines += ["", "## Not found", ""]
        for d in report["missing_objects"]:
            lines.append(f"- `{d['table']}` · {d['operation']} — "
                         f"{d['error'][:160]}")
    if report["failures"]:
        lines += ["", "## Failures", ""]
        for d in report["failures"]:
            lines.append(f"- `{d['table']}` · {d['operation']} — "
                         f"{d['error'][:200]}")
    return "\n".join(lines) + "\n"
