"""Extraction configuration — tables.yaml → typed run + table specs.

Three identities per table, kept apart on purpose (the same short name
exists in several layers):

    billing project   prj-p-lumi-gpt   runs and bills every job; its
                                       JOBS_BY_PROJECT only holds OUR jobs
    logical dataset   axp-lumi.dw      the curated view layer analysts see
    physical dataset  axp-lumi.data    the storage layer the views wrap

Analysts' queries against the ``dw`` views are recorded in the projects
that RAN them — so ``job_projects`` lists every project whose
JOBS_BY_PROJECT we scan (``axp-lumi`` first), and the audit-log sink in
the data project is the source that sees a query from ANY project.

Only pyyaml beyond the stdlib. Validation is strict and typed: a bad
config is exit 2 (validation), never a stack trace mid-run.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_PROJECT_RE = re.compile(r"^[a-z][a-z0-9\-]{4,28}[a-z0-9]$")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
_TABLE_RE = re.compile(r"^[A-Za-z0-9_\-$]{1,1024}$")

TIB = float(1 << 40)
ON_DEMAND_USD_PER_TIB = 6.25


class ConfigError(ValueError):
    """Configuration problem — maps to exit code 2 (validation)."""


def _require_ident(value: str, what: str, pattern: re.Pattern = _IDENT_RE
                   ) -> str:
    value = str(value or "").strip()
    if not value or not pattern.match(value):
        raise ConfigError(f"{what} {value!r} is not a valid BigQuery "
                          "identifier")
    return value


def _bytes_of(value: Any, what: str) -> int:
    """Human byte sizes in yaml: ``2 TiB``, ``500 GiB``, ``100MB``, or
    a bare integer of bytes."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().replace("_", "")
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([KMGTP]?i?B?)$", text, re.I)
    if not m:
        raise ConfigError(f"{what}: cannot parse byte size {value!r}")
    number, unit = float(m.group(1)), m.group(2).upper()
    scale = {"": 1, "B": 1,
             "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12,
             "PB": 10**15,
             "KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30,
             "TIB": 1 << 40, "PIB": 1 << 50}
    key = unit if unit in scale else unit + "B"
    if key not in scale:
        raise ConfigError(f"{what}: unknown unit in {value!r}")
    return int(number * scale[key])


def fmt_bytes(n: int | float | None) -> str:
    if n is None:
        return "?"
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024 or unit == "PiB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.2f} {unit}"
        n /= 1024
    return f"{n:,.2f} PiB"


def usd_of(bytes_billed: int | float | None) -> float:
    """Informational on-demand equivalent at $6.25/TiB — NOT the bill
    when the project sits under a reservation."""
    return round(float(bytes_billed or 0) / TIB * ON_DEMAND_USD_PER_TIB, 4)


@dataclass(frozen=True)
class TableSpec:
    """One in-scope warehouse object, fully qualified on both layers."""

    name: str                       # short name, e.g. gms_transaction
    data_project: str               # axp-lumi
    logical_dataset: str            # dw
    physical_dataset: str           # data
    physical_name: str              # usually == name; overridable
    profile: bool = True
    low_card_threshold: int = 1000
    profile_budget_bytes: int = 0   # 0 → run default
    domain_budget_bytes: int = 0    # 0 → run default
    notes: str = ""

    @property
    def logical_fqn(self) -> str:
        return f"{self.data_project}.{self.logical_dataset}.{self.name}"

    @property
    def physical_fqn(self) -> str:
        return (f"{self.data_project}.{self.physical_dataset}."
                f"{self.physical_name}")

    @property
    def match_names(self) -> tuple[str, ...]:
        """Short names a history record may reference this object by."""
        names = {self.name.lower(), self.physical_name.lower()}
        return tuple(sorted(names))


@dataclass
class RunConfig:
    tables: list[TableSpec]
    data_project: str
    logical_dataset: str
    physical_dataset: str
    billing_project: str            # "" → the connection's project
    region: str = "us"
    job_projects: list[str] = field(default_factory=list)
    scan_organization_jobs: bool = True
    audit_log_table: str = ""       # "" → audit source disabled
    audit_log_partition_column: str = "timestamp"
    history_days: int = 30
    output_dir: Path = Path("data/real_extractions_production")
    concurrency: int = 6
    history_concurrency: int = 3
    run_budget_bytes: int = _bytes_of("5 TiB", "run_budget")
    profile_budget_bytes: int = _bytes_of("200 GiB", "profile_budget")
    domain_budget_bytes: int = _bytes_of("100 GiB", "domain_budget")
    history_day_budget_bytes: int = _bytes_of("50 GiB", "history_day")
    profile_chunk_columns: int = 80
    domain_group_columns: int = 16
    low_card_threshold: int = 1000
    failed_queries_retained: int = 200
    templates_retained: int = 500
    top_users_retained: int = 50
    co_queried_retained: int = 100
    exclude_users: list[str] = field(default_factory=list)
    query_timeout_s: int = 900
    labels: dict[str, str] = field(default_factory=lambda: {
        "lumi-extract": "warehouse-snapshot"})
    table_metrics_routine: str = "get_table_metrics"

    @property
    def datasets(self) -> tuple[str, str]:
        return self.logical_dataset, self.physical_dataset

    def table(self, name: str) -> TableSpec | None:
        for t in self.tables:
            if t.name.lower() == name.lower():
                return t
        return None


_TABLE_KEYS = {"name", "logical_dataset", "physical_dataset",
               "physical_name", "profile", "low_card_threshold",
               "profile_budget", "domain_budget", "notes", "data_project",
               # legacy keys from semantic-graph/config/tables.yaml
               "bq_dataset", "bq_project"}


def _table_spec(entry: Any, defaults: dict, run_low_card: int) -> TableSpec:
    if isinstance(entry, str):
        entry = {"name": entry}
    if not isinstance(entry, dict) or not entry.get("name"):
        raise ConfigError(f"tables: entry {entry!r} needs a name")
    unknown = set(entry) - _TABLE_KEYS
    if unknown:
        raise ConfigError(f"tables[{entry['name']}]: unknown keys "
                          f"{sorted(unknown)}")
    name = _require_ident(entry["name"], "table name", _TABLE_RE)
    logical = entry.get("logical_dataset") or entry.get("bq_dataset") \
        or defaults["logical_dataset"]
    physical = entry.get("physical_dataset") or defaults["physical_dataset"]
    project = entry.get("data_project") or entry.get("bq_project") \
        or defaults["data_project"]
    return TableSpec(
        name=name,
        data_project=_require_ident(project, "data_project", _PROJECT_RE),
        logical_dataset=_require_ident(logical, "logical_dataset"),
        physical_dataset=_require_ident(physical, "physical_dataset"),
        physical_name=_require_ident(entry.get("physical_name") or name,
                                     "physical_name", _TABLE_RE),
        profile=bool(entry.get("profile", True)),
        low_card_threshold=int(entry.get("low_card_threshold",
                                         run_low_card)),
        profile_budget_bytes=_bytes_of(entry.get("profile_budget"),
                                       f"tables[{name}].profile_budget"),
        domain_budget_bytes=_bytes_of(entry.get("domain_budget"),
                                      f"tables[{name}].domain_budget"),
        notes=str(entry.get("notes") or ""))


def load_config(path: Path, *, only: list[str] | None = None,
                extra_tables: list[str] | None = None,
                output_dir: Path | None = None,
                env: dict[str, str] | None = None) -> RunConfig:
    """Read tables.yaml. ``only`` restricts to named tables; ``extra_tables``
    appends bare names (the "new list of tables, instantly" path — every
    default applies). Environment variables fill what the yaml leaves
    blank: BQ_PROJECT_ID (billing), LUMI_BQ_DATA_PROJECT (data)."""
    env = dict(os.environ if env is None else env)
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"config not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"{path}: yaml error: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: top level must be a mapping")
    d = dict(raw.get("defaults") or {})

    data_project = (d.get("data_project") or d.get("bq_project")
                    or env.get("BQ_DATA_PROJECT")
                    or env.get("LUMI_BQ_DATA_PROJECT") or "")
    if not data_project:
        raise ConfigError("defaults.data_project (the project that HOSTS "
                          "the tables, e.g. axp-lumi) is required — or "
                          "set LUMI_BQ_DATA_PROJECT")
    logical_dataset = d.get("logical_dataset") or d.get("bq_dataset") \
        or "dw"
    physical_dataset = d.get("physical_dataset") or "data"
    billing = (d.get("billing_project") or env.get("BQ_PROJECT_ID")
               or env.get("LUMI_BQ_PROJECT") or "")
    defaults = {"data_project": data_project,
                "logical_dataset": logical_dataset,
                "physical_dataset": physical_dataset}
    low_card = int(d.get("low_card_threshold", 1000))

    entries = list(raw.get("tables") or [])
    for name in extra_tables or []:
        entries.append({"name": name})
    specs = [_table_spec(e, defaults, low_card) for e in entries]
    if not specs:
        raise ConfigError("no tables configured (tables: [] and no "
                          "--tables given)")
    seen: dict[str, str] = {}
    for s in specs:
        key = s.name.lower()
        if key in seen:
            raise ConfigError(f"tables: {s.name} listed twice")
        seen[key] = s.name
    if only:
        wanted = {n.strip().lower() for n in only if n.strip()}
        missing = wanted - set(seen)
        if missing:
            raise ConfigError(f"--tables names not in config: "
                              f"{sorted(missing)}")
        specs = [s for s in specs if s.name.lower() in wanted]

    job_projects = list(d.get("job_projects") or [])
    if not job_projects:
        job_projects = [data_project]
    job_projects = [_require_ident(p, "job_projects entry", _PROJECT_RE)
                    for p in job_projects]
    # the billing project's own history is always worth one look: it
    # tells us what OUR identity has been doing (cheap, and it is what
    # the earlier runs saw exclusively)
    if billing and billing not in job_projects:
        job_projects.append(billing)

    audit = str(d.get("audit_log_table") or "").strip()
    if audit and audit.count(".") != 2:
        raise ConfigError("defaults.audit_log_table must be "
                          "project.dataset.table (a trailing * allowed)")

    cfg = RunConfig(
        tables=specs,
        data_project=_require_ident(data_project, "data_project",
                                    _PROJECT_RE),
        logical_dataset=_require_ident(logical_dataset, "logical_dataset"),
        physical_dataset=_require_ident(physical_dataset,
                                        "physical_dataset"),
        billing_project=(_require_ident(billing, "billing_project",
                                        _PROJECT_RE) if billing else ""),
        region=str(d.get("region") or "us").lower().removeprefix("region-"),
        job_projects=job_projects,
        scan_organization_jobs=bool(d.get("scan_organization_jobs", True)),
        audit_log_table=audit,
        audit_log_partition_column=str(d.get("audit_log_partition_column")
                                       or "timestamp"),
        history_days=int(d.get("history_days", 30)),
        output_dir=Path(output_dir or d.get("output_dir")
                        or "data/real_extractions_production"),
        concurrency=max(1, int(d.get("concurrency", 6))),
        history_concurrency=max(1, int(d.get("history_concurrency", 3))),
        run_budget_bytes=_bytes_of(d.get("run_budget", "5 TiB"),
                                   "run_budget"),
        profile_budget_bytes=_bytes_of(d.get("profile_budget", "200 GiB"),
                                       "profile_budget"),
        domain_budget_bytes=_bytes_of(d.get("domain_budget", "100 GiB"),
                                      "domain_budget"),
        history_day_budget_bytes=_bytes_of(
            d.get("history_day_budget", "50 GiB"), "history_day_budget"),
        profile_chunk_columns=max(1, int(d.get("profile_chunk_columns",
                                               80))),
        domain_group_columns=max(1, int(d.get("domain_group_columns", 16))),
        low_card_threshold=low_card,
        failed_queries_retained=int(d.get("failed_queries_retained", 200)),
        templates_retained=int(d.get("templates_retained", 500)),
        top_users_retained=int(d.get("top_users_retained", 50)),
        co_queried_retained=int(d.get("co_queried_retained", 100)),
        exclude_users=[str(u).lower() for u in (d.get("exclude_users")
                                                 or [])],
        query_timeout_s=int(d.get("query_timeout_s", 900)),
        table_metrics_routine=str(d.get("table_metrics_routine")
                                  or "get_table_metrics"),
    )
    if cfg.history_days < 1 or cfg.history_days > 180:
        raise ConfigError("defaults.history_days must be 1..180 "
                          "(JOBS_BY_PROJECT retains 180 days)")
    return cfg
