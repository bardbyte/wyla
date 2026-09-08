"""The local history indexer — 30 daily corpus files → per-table digests.

The extractor pulls each UTC day of history ONCE per source into
``_history/`` (jobs_by_project per job project, jobs_by_organization,
audit_log) and this module routes the records into every in-scope
table's ``17_queries_30d/`` folder: raw matches, top users, peak hours,
co-queried tables, daily usage and cost, failed queries, and query
templates with literals normalized away. No network here — it re-runs
offline on the same corpus (``bq_warehouse_extract.py index``).

Two families are kept apart, as the archive contract says: ``jobs_*``
(INFORMATION_SCHEMA.JOBS_BY_PROJECT across every scanned project plus
JOBS_BY_ORGANIZATION, de-duplicated on project+job id) and ``audit_*``
(the data-access audit sink). They are different evidence; a consumer
can compare them, and the summary says which projects supplied what.

The extractor's OWN jobs (the service account's profiling, the
metadata pulls) stay in the raw files flagged ``excluded_reason`` and
never enter a digest — otherwise every table's top user would be us.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from sahs.extract.config import TableSpec, usd_of

JOBS_FAMILY = "jobs"
AUDIT_FAMILY = "audit"
SOURCE_JOBS_BY_PROJECT = "jobs_by_project"
SOURCE_JOBS_BY_ORG = "jobs_by_organization"
SOURCE_AUDIT = "audit_log"

_META_QUERY = re.compile(r"INFORMATION_SCHEMA|__TABLES__", re.I)


# ── SQL normalization ──

_COMMENT_LINE = re.compile(r"--[^\n]*")
_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.S)
_STRING = re.compile(r"'(?:[^'\\]|\\.|'')*'|\"(?:[^\"\\]|\\.)*\"")
_TYPED_LITERAL = re.compile(
    r"\b(DATE|DATETIME|TIMESTAMP|TIME|NUMERIC|BIGNUMERIC|INTERVAL)\s+\?",
    re.I)
_NUMBER = re.compile(r"(?<![\w.`])[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
                     r"(?![\w.`])")
_IN_LIST = re.compile(r"\(\s*\?(?:\s*,\s*\?)+\s*\)")
_WS = re.compile(r"\s+")


def normalize_sql(text: str) -> str:
    """Literal-blind SQL: strings, numbers and IN-lists become ``?``,
    comments go, whitespace collapses. Identifiers and keywords keep
    their case, so two analysts' spellings of one query still match
    only when the SQL is the same shape."""
    if not text:
        return ""
    sql = _COMMENT_BLOCK.sub(" ", text)
    sql = _COMMENT_LINE.sub(" ", sql)
    sql = _STRING.sub("?", sql)
    sql = _NUMBER.sub("?", sql)
    sql = _TYPED_LITERAL.sub(lambda m: f"{m.group(1).upper()} ?", sql)
    sql = _IN_LIST.sub("(?)", sql)
    sql = _WS.sub(" ", sql).strip()
    return sql


def fingerprint(template: str) -> str:
    return hashlib.sha256(template.lower().encode("utf-8")).hexdigest()[:12]


# ── record normalization ──

_TABLE_PATH = re.compile(
    r"projects/([^/]+)/datasets/([^/]+)/tables/([^/\"]+)")


def _ref(project: str, dataset: str, table: str) -> dict:
    return {"project_id": str(project or ""), "dataset_id": str(dataset or ""),
            "table_id": str(table or "")}


def _refs_of(value: Any) -> list[dict]:
    """referenced_tables in any of the three wire shapes → uniform."""
    out: list[dict] = []
    if not value:
        return out
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            value = [value]
    if isinstance(value, dict):
        value = [value]
    for item in value:
        if isinstance(item, dict):
            out.append(_ref(item.get("project_id") or item.get("projectId"),
                            item.get("dataset_id") or item.get("datasetId"),
                            item.get("table_id") or item.get("tableId")))
        elif isinstance(item, str):
            text = item.strip().strip('"')
            m = _TABLE_PATH.search(text)
            if m:
                out.append(_ref(*m.groups()))
            elif text.count(".") == 2:
                out.append(_ref(*text.split(".")))
    return out


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _iso(value: Any) -> str:
    return str(value) if value not in (None, "") else ""


@dataclass
class JobRecord:
    source: str
    job_id: str
    project_id: str
    user_email: str
    creation_time: str
    end_time: str
    statement_type: str
    state: str
    error_reason: str
    error_message: str
    total_bytes_processed: int | None
    total_bytes_billed: int | None
    total_slot_ms: int | None
    cache_hit: bool | None
    destination_table: dict | None
    referenced_tables: list[dict]
    referenced_views: list[dict]
    query: str
    labels: Any = None
    matched_tables: list[str] = field(default_factory=list)
    match_via: str = ""
    excluded_reason: str = ""

    @property
    def key(self) -> str:
        return f"{self.project_id}:{self.job_id}".lower()

    @property
    def failed(self) -> bool:
        return bool(self.error_message or self.error_reason)

    @property
    def day(self) -> str:
        return (self.creation_time or "")[:10]

    @property
    def hour_utc(self) -> int | None:
        t = self.creation_time or ""
        if len(t) >= 13 and t[10] in "T ":
            try:
                return int(t[11:13])
            except ValueError:
                return None
        return None

    def to_json(self) -> dict:
        return dict(self.__dict__)


def normalize_record(raw: dict, source: str) -> JobRecord:
    """A row from any history source → JobRecord."""
    project_id = str(raw.get("project_id") or "")
    job_id = str(raw.get("job_id") or "")
    if not job_id and raw.get("job_name"):
        m = re.match(r"projects/([^/]+)/jobs/(.+)", str(raw["job_name"]))
        if m:
            project_id, job_id = project_id or m.group(1), m.group(2)
    dest = raw.get("destination_table")
    if isinstance(dest, str):
        refs = _refs_of(dest)
        dest = refs[0] if refs else None
    elif isinstance(dest, dict):
        dest = _ref(dest.get("project_id") or dest.get("projectId"),
                    dest.get("dataset_id") or dest.get("datasetId"),
                    dest.get("table_id") or dest.get("tableId"))
    cache = raw.get("cache_hit")
    if isinstance(cache, str):
        cache = cache.lower() == "true"
    error_message = str(raw.get("error_message") or "")
    error_reason = str(raw.get("error_reason") or "")
    state = str(raw.get("state") or "DONE")
    return JobRecord(
        source=source, job_id=job_id, project_id=project_id,
        user_email=str(raw.get("user_email") or "").lower(),
        creation_time=_iso(raw.get("creation_time")),
        end_time=_iso(raw.get("end_time")),
        statement_type=str(raw.get("statement_type") or ""),
        state=state, error_reason=error_reason,
        error_message=error_message,
        total_bytes_processed=_int(raw.get("total_bytes_processed")),
        total_bytes_billed=_int(raw.get("total_bytes_billed")),
        total_slot_ms=_int(raw.get("total_slot_ms")),
        cache_hit=cache if isinstance(cache, bool) else None,
        destination_table=dest,
        referenced_tables=_refs_of(raw.get("referenced_tables")),
        referenced_views=_refs_of(raw.get("referenced_views")),
        query=str(raw.get("query") or ""),
        labels=raw.get("labels"))


def match_tables(record: JobRecord, specs: list[TableSpec],
                 data_project: str) -> tuple[list[str], str]:
    """Which in-scope tables a record touched, and how it was decided:
    ``referenced`` (the job's referenced tables/views name it, in the
    data project or unqualified) or ``query_text`` (word-bounded name
    in the SQL — the net for failed jobs, which reference nothing)."""
    by_name: dict[str, str] = {}
    for spec in specs:
        for n in spec.match_names:
            by_name[n] = spec.name
    hits: set[str] = set()
    for ref in record.referenced_tables + record.referenced_views:
        tid = ref["table_id"].lower()
        project = ref["project_id"].lower()
        if tid in by_name and (not project or project == data_project.lower()):
            hits.add(by_name[tid])
    if hits:
        return sorted(hits), "referenced"
    if record.query:
        lowered = record.query.lower()
        for name, table in by_name.items():
            if name in lowered and re.search(rf"\b{re.escape(name)}\b",
                                             lowered):
                hits.add(table)
        if hits:
            return sorted(hits), "query_text"
    return [], ""


# ── corpus files ──

def write_jsonl_gz(path: Path, rows: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    n = 0
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, default=str, ensure_ascii=False) + "\n")
            n += 1
    os.replace(tmp, path)
    return n


def read_jsonl_gz(path: Path) -> Iterable[dict]:
    if not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def corpus_files(history_dir: Path) -> list[tuple[str, str, Path]]:
    """Every daily corpus file → (family, source_key, path)."""
    out: list[tuple[str, str, Path]] = []
    jobs_root = history_dir / "jobs_by_project"
    if jobs_root.exists():
        for project_dir in sorted(p for p in jobs_root.iterdir()
                                  if p.is_dir()):
            for f in sorted(project_dir.glob("*.jsonl.gz")):
                out.append((JOBS_FAMILY,
                            f"{SOURCE_JOBS_BY_PROJECT}/{project_dir.name}",
                            f))
    org_root = history_dir / "jobs_by_organization"
    if org_root.exists():
        for f in sorted(org_root.glob("*.jsonl.gz")):
            out.append((JOBS_FAMILY, SOURCE_JOBS_BY_ORG, f))
    audit_root = history_dir / "audit_log"
    if audit_root.exists():
        for f in sorted(audit_root.glob("*.jsonl.gz")):
            out.append((AUDIT_FAMILY, SOURCE_AUDIT, f))
    return out


# ── digests ──

def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)
    os.replace(tmp, path)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str,
                              ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class DigestLimits:
    top_users: int = 50
    co_queried: int = 100
    failed_queries: int = 200
    templates: int = 500


def build_digests(records: list[JobRecord], table: str, family: str,
                  out_dir: Path, limits: DigestLimits,
                  history_days: int) -> dict:
    """One family's digest files for one table. ``records`` are the
    matches for this table, already de-duplicated; excluded ones ride
    into the raw file only."""
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / f"{family}_30d.jsonl.gz"
    write_jsonl_gz(raw_path, (r.to_json() for r in records))
    live = [r for r in records if not r.excluded_reason]

    users: dict[str, dict] = {}
    for r in live:
        u = users.setdefault(r.user_email or "(unknown)", {
            "user_email": r.user_email or "(unknown)", "query_count": 0,
            "failed_count": 0, "bytes_billed": 0, "first_seen": r.creation_time,
            "last_seen": r.creation_time, "days": set()})
        u["query_count"] += 1
        u["failed_count"] += 1 if r.failed else 0
        u["bytes_billed"] += r.total_bytes_billed or 0
        u["first_seen"] = min(u["first_seen"], r.creation_time or
                              u["first_seen"])
        u["last_seen"] = max(u["last_seen"], r.creation_time or
                             u["last_seen"])
        if r.day:
            u["days"].add(r.day)
    top_users = sorted(users.values(), key=lambda u: (-u["query_count"],
                                                      u["user_email"]))
    for u in top_users:
        u["active_days"] = len(u.pop("days"))
    _write_csv(out_dir / f"{family}_top_users.csv",
               top_users[:limits.top_users],
               ["user_email", "query_count", "failed_count", "bytes_billed",
                "first_seen", "last_seen", "active_days"])

    hours = Counter(r.hour_utc for r in live if r.hour_utc is not None)
    _write_csv(out_dir / f"{family}_peak_hours.csv",
               [{"hour_utc": h, "query_count": n}
                for h, n in sorted(hours.items())],
               ["hour_utc", "query_count"])

    co: Counter = Counter()
    co_fqn: dict[str, str] = {}
    for r in live:
        seen: set[str] = set()
        for ref in r.referenced_tables + r.referenced_views:
            tid = ref["table_id"].lower()
            if not tid or tid == table.lower() or tid in seen:
                continue
            seen.add(tid)
            co[tid] += 1
            co_fqn.setdefault(tid, ".".join(
                p for p in (ref["project_id"], ref["dataset_id"], tid)
                if p))
    _write_csv(out_dir / f"{family}_co_queried_tables.csv",
               [{"other_table": t, "other_table_fqn": co_fqn.get(t, t),
                 "co_query_count": n}
                for t, n in co.most_common(limits.co_queried)],
               ["other_table", "other_table_fqn", "co_query_count"])

    daily: dict[str, dict] = {}
    for r in live:
        if not r.day:
            continue
        d = daily.setdefault(r.day, {
            "date": r.day, "query_count": 0, "distinct_users": set(),
            "bytes_processed": 0, "bytes_billed": 0, "failed_count": 0})
        d["query_count"] += 1
        d["distinct_users"].add(r.user_email)
        d["bytes_processed"] += r.total_bytes_processed or 0
        d["bytes_billed"] += r.total_bytes_billed or 0
        d["failed_count"] += 1 if r.failed else 0
    daily_rows = []
    for d in sorted(daily.values(), key=lambda d: d["date"]):
        d["distinct_users"] = len(d["distinct_users"])
        d["on_demand_usd"] = usd_of(d["bytes_billed"])
        daily_rows.append(d)
    _write_csv(out_dir / f"{family}_daily_usage_cost.csv", daily_rows,
               ["date", "query_count", "distinct_users", "bytes_processed",
                "bytes_billed", "failed_count", "on_demand_usd"])

    failed = [r for r in live if r.failed]
    failed.sort(key=lambda r: r.creation_time, reverse=True)
    _write_json(out_dir / f"{family}_failed_queries.json",
                [{"job_id": r.job_id, "project_id": r.project_id,
                  "user_email": r.user_email,
                  "creation_time": r.creation_time,
                  "error_reason": r.error_reason,
                  "error_message": r.error_message,
                  "statement_type": r.statement_type,
                  "query": r.query[:20000]}
                 for r in failed[:limits.failed_queries]])

    templates: dict[str, dict] = {}
    for r in live:
        if not r.query or r.failed:
            continue
        template = normalize_sql(r.query)
        if not template:
            continue
        fp = fingerprint(template)
        t = templates.setdefault(fp, {
            "fingerprint": fp, "normalized_sql": template[:16000],
            "sample_sql": r.query[:16000], "occurrences": 0,
            "users": set(), "first_seen": r.creation_time,
            "last_seen": r.creation_time, "bytes_billed_total": 0,
            "statement_type": r.statement_type})
        t["occurrences"] += 1
        t["users"].add(r.user_email)
        t["first_seen"] = min(t["first_seen"], r.creation_time or
                              t["first_seen"])
        t["last_seen"] = max(t["last_seen"], r.creation_time or
                             t["last_seen"])
        t["bytes_billed_total"] += r.total_bytes_billed or 0
    template_rows = sorted(templates.values(),
                           key=lambda t: (-t["occurrences"], t["fingerprint"]))
    for t in template_rows:
        t["distinct_users"] = len(t.pop("users"))
    _write_csv(out_dir / f"{family}_query_templates.csv",
               template_rows[:limits.templates],
               ["fingerprint", "normalized_sql", "sample_sql", "occurrences",
                "distinct_users", "first_seen", "last_seen",
                "bytes_billed_total", "statement_type"])

    times = [r.creation_time for r in live if r.creation_time]
    by_source = Counter(r.source for r in records)
    by_project = Counter(r.project_id for r in live)
    return {
        "family": family,
        "rows": len(records),
        "rows_excluded": len(records) - len(live),
        "query_count": len(live),
        "distinct_users": len({r.user_email for r in live}),
        "first_seen": min(times) if times else None,
        "last_seen": max(times) if times else None,
        "bytes_processed": sum(r.total_bytes_processed or 0 for r in live),
        "bytes_billed": sum(r.total_bytes_billed or 0 for r in live),
        "failed_queries": len(failed),
        "failed_queries_retained": min(len(failed), limits.failed_queries),
        "query_templates": len(template_rows),
        "co_queried_tables": len(co),
        "peak_hour_utc": (hours.most_common(1)[0][0] if hours else None),
        "history_days": history_days,
        "by_source": dict(by_source),
        "by_job_project": dict(by_project.most_common()),
        "raw_file": raw_path.name,
    }


def index_history(history_dir: Path, specs: list[TableSpec],
                  tables_root: Path, *, data_project: str,
                  exclude_users: list[str], history_days: int,
                  limits: DigestLimits | None = None,
                  window_start: str = "", window_end: str = "") -> dict:
    """Route the corpus into every table's 17_queries_30d/ and return
    the per-table summary (also written to _history/per_table_summary.json).

    ``window_start``/``window_end`` (ISO UTC) bound the records that
    count, so a corpus holding more days than this run asked for still
    yields exactly the requested window."""
    limits = limits or DigestLimits()
    excluded = {u.lower() for u in exclude_users}
    per_table: dict[str, dict[str, dict[str, JobRecord]]] = {
        s.name: {JOBS_FAMILY: {}, AUDIT_FAMILY: {}} for s in specs}
    corpus_stats: dict[str, dict] = {}
    for family, source_key, path in corpus_files(history_dir):
        stats = corpus_stats.setdefault(source_key, {
            "family": family, "files": 0, "records": 0, "matched": 0})
        stats["files"] += 1
        source = source_key.split("/")[0]
        for raw in read_jsonl_gz(path):
            stats["records"] += 1
            rec = normalize_record(raw, source)
            if window_start and rec.creation_time \
                    and rec.creation_time < window_start:
                continue
            if window_end and rec.creation_time \
                    and rec.creation_time >= window_end:
                continue
            matched, via = match_tables(rec, specs, data_project)
            if not matched:
                continue
            stats["matched"] += 1
            rec.matched_tables, rec.match_via = matched, via
            if rec.user_email in excluded:
                rec.excluded_reason = "extractor_identity"
            elif rec.query and _META_QUERY.search(rec.query):
                rec.excluded_reason = "metadata_query"
            for name in matched:
                bucket = per_table[name][family]
                # one job can surface from two sources (axp-lumi's
                # JOBS_BY_PROJECT and JOBS_BY_ORGANIZATION): keep the
                # richer record (query text wins)
                prior = bucket.get(rec.key)
                if prior is None or (not prior.query and rec.query):
                    bucket[rec.key] = rec

    summary: dict[str, dict] = {}
    for spec in specs:
        out_dir = tables_root / spec.name / "17_queries_30d"
        families = {}
        for family in (JOBS_FAMILY, AUDIT_FAMILY):
            records = sorted(per_table[spec.name][family].values(),
                             key=lambda r: (r.creation_time, r.key))
            families[family] = build_digests(records, spec.name, family,
                                             out_dir, limits, history_days)
        jobs, audit = families[JOBS_FAMILY], families[AUDIT_FAMILY]
        all_users = set()
        for r in per_table[spec.name][JOBS_FAMILY].values():
            if not r.excluded_reason:
                all_users.add(r.user_email)
        for r in per_table[spec.name][AUDIT_FAMILY].values():
            if not r.excluded_reason:
                all_users.add(r.user_email)
        first = [x for x in (jobs["first_seen"], audit["first_seen"]) if x]
        last = [x for x in (jobs["last_seen"], audit["last_seen"]) if x]
        table_summary = {
            "table": spec.name,
            "history_days": history_days,
            "window_start": window_start or None,
            "window_end": window_end or None,
            "jobs_rows": jobs["query_count"],
            "audit_rows": audit["query_count"],
            "jobs_rows_excluded": jobs["rows_excluded"],
            "audit_rows_excluded": audit["rows_excluded"],
            "distinct_users": len(all_users),
            "first_seen": min(first) if first else None,
            "last_seen": max(last) if last else None,
            "bytes_processed": jobs["bytes_processed"]
            or audit["bytes_processed"],
            "bytes_billed": jobs["bytes_billed"] or audit["bytes_billed"],
            "jobs": jobs,
            "audit": audit,
        }
        _write_json(out_dir / "summary.json", table_summary)
        summary[spec.name] = table_summary
    _write_json(history_dir / "per_table_summary.json",
                {"corpus": corpus_stats, "tables": summary})
    return {"corpus": corpus_stats, "tables": summary}
