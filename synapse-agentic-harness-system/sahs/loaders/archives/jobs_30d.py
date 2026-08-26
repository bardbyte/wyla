"""jobs_30d — the in-silo witness over the raw 30-day query history
(E12/A3).

Design sentence, verbatim so nobody "fixes" it later: **the catalog
winning the max is the design working — the jobs witness was never
hired to out-count a longer-horizon miner; it was hired for true
recency, corroboration, and discovery of what the catalog missed.**

Per table, `17_queries_30d/jobs_30d.jsonl.gz` is canonicalized job by
job and mined with a **top-level-only v1 extractor**: aggregates from
the OUTERMOST select list become measure fragments, conjuncts from the
OUTERMOST where become predicate fragments (wrapped exactly as the
blue-insights pipeline wraps, so fingerprints fuse across witnesses).
Anything nested — correlated subqueries, EXISTS, derived tables — goes
to the `nested` quarantine category, counted, never silently skipped:
a fragment lifted out of nested context changes meaning, and a wrong
fragment with support 200 is worse than none.

Pins: support = **distinct job_id count** per fingerprint (repeats →
``run_count``); recency = job ``creation_time`` (the only true
timestamps in the corpus); JOIN ON equalities between resolvable
tables → ``joins_via`` edges with jobs-witness support; per-table
``cost_prior`` (p50/p95 bytes per job, n_jobs) + ``usage_rhythm``
props; ``audit_*`` digests corroborate ONLY — a jobs-vs-audit
divergence emits a ReviewItem(kind=witness_divergence), never a
feature. Gate: ≥90% of a table's jobs canonicalized-or-understood
(nested counts as understood; parse/dialect breakage does not).
"""

from __future__ import annotations

import csv
import gzip
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from sahs.canon.authority import Authority
from sahs.canon.canonical import try_canon, wrap_predicate
from sahs.canon.census import canonicalize_records
from sahs.graph.crosswalk import Crosswalk
from sahs.graph.ids import table_id
from sahs.graph.quads import GraphDir, NodeRecord, Prov, Quad
from sahs.graph.review import emit_review_item
from sahs.loaders.quads_emit import emit_expressions
from sahs.loaders.records import ExpressionRecord

SOURCE = "jobs_30d"
WITNESS = "jobs_30d"
GATE_RATE = 0.90
_AGG_NAMES = {"sum", "count", "avg", "min", "max", "countif",
              "safe_divide", "count_distinct", "approx_count_distinct"}


def _read_gz_jsonl(path: Path) -> list[dict]:
    out = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                out.append(json.loads(line))
    return out


def _csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _is_nested(tree: Any) -> bool:
    from sqlglot import expressions as exp
    if tree.args.get("with") is not None:
        return True
    for node in tree.walk():
        if node is tree:
            continue
        if isinstance(node, (exp.Subquery, exp.Exists, exp.Select)):
            return True
    return False


def _extract(tree: Any, physical_short: str
             ) -> tuple[list[tuple[str, str]], list[str],
                        list[tuple[str, str, str]]]:
    """→ (measures [(label, expr_sql)], predicates [conjunct_sql],
    join_eqs [(left_table, right_table, on_sql)]) — TOP LEVEL ONLY."""
    from sqlglot import expressions as exp
    measures: list[tuple[str, str]] = []
    predicates: list[str] = []
    join_eqs: list[tuple[str, str, str]] = []

    for item in tree.expressions:
        label = item.alias if isinstance(item, exp.Alias) else ""
        body = item.this if isinstance(item, exp.Alias) else item
        has_agg = any(
            isinstance(n, exp.AggFunc)
            or (isinstance(n, exp.Anonymous)
                and n.name.lower() in _AGG_NAMES)
            for n in body.walk())
        if has_agg:
            measures.append((label, body.sql(dialect="bigquery")))

    where = tree.args.get("where")
    if where is not None:
        def conjuncts(node: Any):
            if isinstance(node, exp.And):
                yield from conjuncts(node.this)
                yield from conjuncts(node.expression)
            else:
                yield node
        for conjunct in conjuncts(where.this):
            if conjunct.find(exp.Column) is not None:
                predicates.append(conjunct.sql(dialect="bigquery"))

    alias_to_table: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        for key in (table.alias, name):
            if key:
                alias_to_table[key.lower()] = name
    for join in tree.find_all(exp.Join):
        on = join.args.get("on")
        if on is None:
            continue
        for eq in on.find_all(exp.EQ):
            left, right = eq.this, eq.expression
            if not (isinstance(left, exp.Column)
                    and isinstance(right, exp.Column)):
                continue
            lt = alias_to_table.get((left.table or "").lower(), "")
            rt = alias_to_table.get((right.table or "").lower(), "")
            if lt and rt and lt != rt:
                join_eqs.append((lt, rt, eq.sql(dialect="bigquery")))
    return measures, predicates, join_eqs


def load_jobs_30d(root: Path, graph: GraphDir, crosswalk: Crosswalk,
                  run_id: str, ledger=None) -> tuple[dict, list[str]]:
    """→ (report, gate_failures). Report carries the per-table
    canonicalization accounting the runbook commits."""
    root = Path(root)
    report: dict[str, Any] = {"tables": {}, "measures": 0,
                              "predicates": 0, "joins_via": 0,
                              "divergences": 0, "quarantine": {}}
    gate_failures: list[str] = []

    def track(path: Path) -> Path:
        if ledger is not None:
            ledger.consumed(path)
        return path

    table_dirs = sorted(
        d for d in root.iterdir()
        if d.is_dir() and not d.name.startswith("_"))
    for d in table_dirs:
        physical = (crosswalk.physical_for_atlas(d.name)
                    or crosswalk.physical_for_lumi(d.name))
        if physical is None:
            gate_failures.append(f"jobs_30d: unresolvable table dir "
                                 f"{d.name!r}")
            continue
        q17 = d / "17_queries_30d"
        jobs_path = q17 / "jobs_30d.jsonl.gz"
        if not jobs_path.exists():
            report["tables"][physical] = {"jobs": 0, "note": "no jobs file"}
            continue
        jobs = _read_gz_jsonl(track(jobs_path))
        short = physical.split(".")[-1]

        understood = 0
        quarantine: dict[str, int] = defaultdict(int)
        # aggregation keys → distinct job ids + run counts + dates
        mined: dict[tuple[str, str, str], dict[str, Any]] = {}
        joins: dict[tuple[str, str], dict[str, Any]] = {}

        def _seen(key: tuple[str, str, str], raw_sql: str, kind: str,
                  label: str, job: dict) -> None:
            entry = mined.setdefault(key, {
                "raw_sql": raw_sql, "kind": kind, "label": label,
                "job_ids": set(), "runs": 0,
                "first_seen": "", "last_seen": ""})
            # deterministic label pick: smallest non-empty alias — the
            # label decorates; the FINGERPRINT aggregates
            if label and (not entry["label"] or label < entry["label"]):
                entry["label"] = label
            entry["job_ids"].add(str(job.get("job_id") or ""))
            entry["runs"] += 1
            stamp = str(job.get("creation_time") or "")[:10]
            if stamp:
                if not entry["first_seen"] or stamp < entry["first_seen"]:
                    entry["first_seen"] = stamp
                if stamp > entry["last_seen"]:
                    entry["last_seen"] = stamp

        for job in jobs:
            sql = str(job.get("query") or "")
            result, err = try_canon(sql)
            if err is not None:
                quarantine[err.category] += 1
                continue
            if result.kind not in ("select", "union"):
                quarantine["not_a_query"] += 1
                continue
            tree = result.ast
            if _is_nested(tree):
                quarantine["nested"] += 1     # counted, never silent
                understood += 1
                continue
            understood += 1
            measures, predicates, join_eqs = _extract(tree, short)
            for label, expr_sql in measures:
                # support is per FINGERPRINT (pinned) — the alias never
                # splits the count; two queries computing the same
                # expression are one measure seen twice
                _seen(("measure", "", expr_sql), expr_sql,
                      "metric_expr", label, job)
            for conjunct in predicates:
                from sqlglot import expressions as exp, parse_one
                first_col = parse_one(
                    conjunct, read="bigquery").find(exp.Column)
                concept = first_col.name.lower() if first_col else ""
                if not concept:
                    continue
                _seen(("predicate", concept, conjunct),
                      wrap_predicate(conjunct, short), "predicate",
                      concept, job)
            for lt, rt, on_sql in join_eqs:
                left = (crosswalk.physical_for_atlas(lt)
                        or crosswalk.physical_for_lumi(lt))
                right = (crosswalk.physical_for_atlas(rt)
                         or crosswalk.physical_for_lumi(rt))
                if not left or not right:
                    quarantine["ambiguous_attribution"] += 1
                    continue
                a, b = sorted((left, right))
                edge = joins.setdefault((a, b), {
                    "on": on_sql, "job_ids": set()})
                edge["job_ids"].add(str(job.get("job_id") or ""))

        total = len(jobs)
        rate = (understood / total) if total else 1.0
        report["tables"][physical] = {
            "jobs": total, "understood": understood,
            "rate": round(rate, 4),
            "quarantine": dict(sorted(quarantine.items()))}
        for category, n in quarantine.items():
            report["quarantine"][category] = (
                report["quarantine"].get(category, 0) + n)
        if total and rate < GATE_RATE:
            gate_failures.append(
                f"jobs_30d: {physical} canon rate {rate:.1%} < "
                f"{GATE_RATE:.0%}")

        # mined fragments → the ONE pipeline (fingerprints fuse with
        # catalog/blue witnesses because the wrapping is identical)
        records = []
        for (kind, _label_key, _sql), entry in sorted(mined.items()):
            support = len({j for j in entry["job_ids"] if j})
            records.append(ExpressionRecord(
                raw_sql=entry["raw_sql"], kind=entry["kind"],
                source=SOURCE, authority=Authority.MINED,
                concept_label=entry["label"] or None,
                table_hint=short,
                support=max(support, 1),
                first_seen=entry["first_seen"],
                last_seen=entry["last_seen"],
                evidence_ref=f"{d.name}/17_queries_30d/jobs_30d.jsonl.gz",
                witness=WITNESS,
                extra={"run_count": entry["runs"],
                       # SELECT aliases group usage but never name a
                       # metric — the catalog owns names
                       "label_is_weak": True}))
        if records:
            pairs, _quar = canonicalize_records(records)
            sub = emit_expressions(pairs, graph, crosswalk, run_id)
            report["measures"] += sub.get("metrics", 0)
            report["predicates"] += sub.get("predicates", 0)

        for (a, b), edge in sorted(joins.items()):
            graph.append_edge(Quad(
                s=table_id(a), r="joins_via", o=table_id(b),
                props={"on": edge["on"]},
                prov=Prov(source=SOURCE, run=run_id, witness=WITNESS,
                          support=max(len(edge["job_ids"]), 1),
                          evidence=f"{d.name}/17_queries_30d/"
                                   "jobs_30d.jsonl.gz")))
            report["joins_via"] += 1

        # cost prior (per-job bytes, honest percentiles) + usage rhythm
        bytes_seen = sorted(int(j.get("total_bytes_processed") or 0)
                            for j in jobs
                            if j.get("total_bytes_processed"))
        daily = _csv_rows(track(q17 / "jobs_daily_usage_cost.csv"))
        peaks = _csv_rows(track(q17 / "jobs_peak_hours.csv"))
        props: dict[str, Any] = {}
        if bytes_seen:
            props["cost_prior"] = {
                "n_jobs": len(bytes_seen),
                "p50_bytes": bytes_seen[len(bytes_seen) // 2],
                "p95_bytes": bytes_seen[
                    min(len(bytes_seen) - 1,
                        int(len(bytes_seen) * 0.95))],
                "daily_days": len(daily)}
        if peaks:
            top = sorted(peaks, key=lambda r: -int(
                r.get("query_count") or 0))[:3]
            props["usage_rhythm"] = [
                f"{r.get('hour_utc')}:00 UTC × {r.get('query_count')}"
                for r in top]
        if props:
            graph.append_node(NodeRecord(
                id=table_id(physical), props=props,
                prov=Prov(source=SOURCE, run=run_id, witness=WITNESS,
                          evidence=f"{d.name}/17_queries_30d/")))

        failed_path = q17 / "jobs_failed_queries.json"
        if failed_path.exists():
            failures = json.loads(
                track(failed_path).read_text(encoding="utf-8")) or []
            report["tables"][physical]["failed_queries_recorded"] = \
                len(failures)

        # audit corroboration ONLY — divergence is a review item,
        # never a feature (two witnesses of one event don't vote twice)
        jobs_co = {r.get("other_table", "").lower()
                   for r in _csv_rows(
                       q17 / "jobs_co_queried_tables.csv")}
        audit_co = {r.get("other_table", "").lower()
                    for r in _csv_rows(
                        track(q17 / "audit_co_queried_tables.csv"))}
        track(q17 / "audit_top_users.csv")
        if audit_co and jobs_co != audit_co:
            emit_review_item(
                graph, kind="witness_divergence",
                subject=table_id(physical),
                proposal=("jobs vs audit co-query digests disagree: "
                          f"jobs={sorted(jobs_co)} "
                          f"audit={sorted(audit_co)}"),
                evidence=[f"{d.name}/17_queries_30d/"
                          "jobs_co_queried_tables.csv",
                          f"{d.name}/17_queries_30d/"
                          "audit_co_queried_tables.csv"],
                run_id=run_id, source=SOURCE, witness="audit_30d",
                blast_radius=2)
            report["divergences"] += 1

    return report, gate_failures
