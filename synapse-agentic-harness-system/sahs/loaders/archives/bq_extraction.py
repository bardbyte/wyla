"""BQ archive loader — the real `real_extractions_production/` layout.

Reads the documented per-table artifacts (00 resource, 02 logical
columns, 05 view SQL, 10 partitions, 13 metrics, 14 profile, 15
low-cardinality domains, 16 row policies, 17 query history) plus the
run-level `_run_report.json`, and emits quads under the `bq` source.

Honesty rules carried from the extraction's own status semantics:
DENIED ≠ "no policies" — a denied rowAccessPolicies listing becomes
``has_policy → policy:unknown_denied`` (E3 will fail closed on it);
a present-but-empty 16 file is *confirmed none* and emits nothing.
Every table subject resolves through the E1 crosswalk or the load
BLOCKS (identity errors never quarantine).
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from sahs.graph.crosswalk import Crosswalk
from sahs.graph.ids import col_id, table_id
from sahs.graph.quads import GraphDir, NodeRecord, Prov, Quad

SOURCE = "bq"


def _csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


_SQLISH = ("select", "from", "insert", "merge", "with ", "create")


def _sql_column(rows: list[dict]) -> str | None:
    """jobs_query_templates.csv names its SQL column differently across
    extractor versions — the loader adapts to the file. Known names are
    tried first (pinned priority), then the VALUE signature decides: the
    first column, in header order, whose text looks like SQL. Both rules
    are deterministic for a given file."""
    if not rows:
        return None
    names = [n for n in rows[0] if n]
    for cand in ("normalized_sql", "query_template", "template_sql",
                 "sql_template", "normalized_query", "sql", "query"):
        if cand in names:
            return cand
    for n in names:
        sample = next((str(r.get(n) or "") for r in rows if r.get(n)), "")
        if any(k in sample.lower() for k in _SQLISH):
            return n
    return None


def load_bq_archive(root: Path, graph: GraphDir, crosswalk: Crosswalk,
                    run_id: str, ledger=None,
                    include_jobs_digests: bool = True
                    ) -> tuple[dict, list[str]]:
    """→ (report, blocking_errors).

    ``include_jobs_digests=False`` (A8) skips every fact derived from the
    30-day query history — top_users, co_queried_with, query templates —
    because an incorrect history must not witness anything; the caller
    defers the ``17_queries_30d`` files in the ledger."""
    root = Path(root)
    blocking: list[str] = []
    def track(path: Path):
        if ledger is not None:
            ledger.consumed(path)
        return path

    report = {"tables": 0, "columns": 0, "columns_from_profile_only": 0,
              "domains": 0, "templates": 0, "template_rows_skipped": 0,
              "co_query_edges": 0, "policies_unknown": 0}
    run_report = _json(track(root / "_run_report.json")) or {}
    denied = {(d.get("table"), d.get("operation"))
              for d in run_report.get("denied_operations", [])}

    def prov(**kw) -> Prov:
        return Prov(source=SOURCE, run=run_id, **kw)

    table_dirs = sorted(
        d for d in root.iterdir()
        if d.is_dir() and not d.name.startswith("_"))
    for d in table_dirs:
        resource = _json(track(d / "00_logical_table_resource.json")) or {}
        ref = resource.get("tableReference", {})
        dataset, name = ref.get("datasetId", ""), ref.get("tableId", d.name)
        physical = crosswalk.physical_for_bq(dataset, name)
        if physical is None and not dataset:
            # a dir missing its 00 resource still names its table — the
            # crosswalk's UNIQUE short name is identity enough (views
            # and denied resource calls ship without the 00 file);
            # ambiguity still blocks below
            physical = crosswalk.physical_for_short(name)
        if physical is None:
            blocking.append(
                f"crosswalk: no row for bq table {dataset}.{name} "
                f"(archive dir {d.name})"
                + ("" if dataset else " — 00 resource absent and short "
                   "name not uniquely in crosswalk"))
            continue
        tid = table_id(physical)
        evidence = f"{d.name}/00_logical_table_resource.json"

        columns = _csv_rows(track(d / "02_logical_columns.csv"))
        column_set = ",".join(sorted(
            f"{r['column_name']}:{r['data_type']}" for r in columns))
        schema_version = ("v1_" + hashlib.sha256(
            column_set.encode()).hexdigest()[:8])
        schema_node = f"schema:{physical}@v1"

        metrics = _csv_rows(track(d / "13_table_metrics.csv"))
        total_rows = int(metrics[0]["total_rows"]) if metrics else None
        partitions = _csv_rows(track(d / "10_physical_partitions.csv"))
        users = (_csv_rows(track(d / "17_queries_30d"
                                 / "jobs_top_users.csv"))
                 if include_jobs_digests else [])
        graph.append_node(NodeRecord(
            id=tid,
            props={
                "project": ref.get("projectId", ""),
                "object_type": resource.get("type", "TABLE"),
                "description_bq": resource.get("description", ""),
                "total_rows": total_rows,
                "n_partitions": len(partitions) or None,
                "partition_latest": max(
                    (r["partition_id"] for r in partitions), default=None),
                "top_users": [{"user": r["user_email"],
                               "queries": int(r["query_count"])}
                              for r in users[:5]],
                "schema_fingerprint": schema_version,
            },
            prov=prov(evidence=evidence)))
        graph.append_node(NodeRecord(
            id=schema_node, props={"fingerprint": schema_version,
                                   "n_columns": len(columns)},
            prov=prov(evidence=f"{d.name}/02_logical_columns.csv")))
        graph.append_edge(Quad(s=tid, r="has_schema", o=schema_node,
                               prov=prov()))
        report["tables"] += 1

        view_sql = d / "05_view_definition.sql"
        if view_sql.exists():
            doc = f"doc:view_sql_{physical.replace('.', '_')}"
            graph.append_node(NodeRecord(
                id=doc, props={"kind": "view_sql",
                               "sql": track(view_sql).read_text(encoding="utf-8")},
                prov=prov(evidence=f"{d.name}/05_view_definition.sql")))
            graph.append_edge(Quad(s=tid, r="described_by", o=doc,
                                   prov=prov()))

        profile = {r["column_name"]: r
                   for r in _csv_rows(track(d / "14_column_profile.csv"))}
        for row in columns:
            cid = col_id(physical, row["column_name"])
            p = profile.get(row["column_name"], {})
            graph.append_node(NodeRecord(
                id=cid,
                props={
                    "data_type": row["data_type"],
                    "ordinal": int(row.get("ordinal_position") or 0),
                    "is_partitioning":
                        row.get("is_partitioning_column") == "YES",
                    "null_count": int(p["null_count"]) if p else None,
                    "approx_distinct":
                        int(p["approx_distinct"]) if p else None,
                    "profile_coverage": p.get("coverage_mode", ""),
                },
                prov=prov(valid_for=[schema_node],
                          evidence=f"{d.name}/02_logical_columns.csv")))
            graph.append_edge(Quad(s=tid, r="has_column", o=cid,
                                   prov=prov(valid_for=[schema_node])))
            report["columns"] += 1

        lc_dir = d / "15_low_cardinality_values"
        if lc_dir.exists():
            known_cols = {r["column_name"].lower() for r in columns}
            for value_file in map(track, sorted(lc_dir.glob("*.csv"))):
                rows = _csv_rows(value_file)
                if not rows:
                    continue
                column = rows[0]["column_name"]
                if column.lower() not in known_cols:
                    # the profiler saw a column the 02 schema listing
                    # didn't carry (a truncated export on wide tables).
                    # Observed VALUES attest the column exists — mint
                    # the endpoint from the profile evidence, counted
                    # so the 02-vs-15 drift stays visible
                    known_cols.add(column.lower())
                    ev15 = (f"{d.name}/15_low_cardinality_values/"
                            f"{value_file.name}")
                    graph.append_node(NodeRecord(
                        id=col_id(physical, column),
                        props={"observed_via": "low_cardinality_profile"},
                        prov=prov(evidence=ev15)))
                    graph.append_edge(Quad(
                        s=tid, r="has_column",
                        o=col_id(physical, column),
                        prov=prov(evidence=ev15)))
                    report["columns_from_profile_only"] += 1
                domain = f"domain:{physical}.{column.lower()}"
                graph.append_node(NodeRecord(
                    id=domain,
                    props={"values": [
                        {"value": r["value"],
                         "count": int(r["value_count"]),
                         "pct": float(r["pct_of_rows"])}
                        for r in rows[:1000]]},
                    prov=prov(
                        evidence=f"{d.name}/15_low_cardinality_values/"
                                 f"{value_file.name}")))
                graph.append_edge(Quad(
                    s=col_id(physical, column), r="has_domain", o=domain,
                    prov=prov()))
                report["domains"] += 1

        policies = _json(track(d / "16_row_access_policies.json"))
        if policies is None:
            if (name, "rowAccessPolicies.list") in denied \
                    or any(t == name for t, _ in denied):
                graph.append_edge(Quad(
                    s=tid, r="has_policy", o="policy:unknown_denied",
                    prov=prov(evidence="_run_report.json")))
                report["policies_unknown"] += 1
        elif policies:
            for i, _pol in enumerate(policies):
                graph.append_edge(Quad(
                    s=tid, r="has_policy", o=f"policy:row_access_{i}",
                    prov=prov(
                        evidence=f"{d.name}/16_row_access_policies.json")))

        for row in (_csv_rows(track(
                d / "17_queries_30d" / "jobs_co_queried_tables.csv"))
                if include_jobs_digests else []):
            other_physical = crosswalk.physical_for_lumi(row["other_table"])
            if other_physical is None:
                continue                 # co-query partner outside scope
            graph.append_edge(Quad(
                s=tid, r="co_queried_with", o=table_id(other_physical),
                prov=prov(support=max(int(row["co_query_count"]), 1),
                          evidence=f"{d.name}/17_queries_30d/"
                                   "jobs_co_queried_tables.csv")))
            report["co_query_edges"] += 1

        t_rows = (_csv_rows(track(
            d / "17_queries_30d" / "jobs_query_templates.csv"))
            if include_jobs_digests else [])
        sql_col = _sql_column(t_rows)
        if t_rows and sql_col is None:
            # no column looks like SQL — counted, never a dead run
            report["template_rows_skipped"] += len(t_rows)
        for row in (t_rows if sql_col else []):
            sql_text = str(row.get(sql_col) or "").strip()
            if not sql_text:
                report["template_rows_skipped"] += 1
                continue
            occurrences = int(row.get("occurrences")
                              or row.get("count")
                              or row.get("query_count") or 1)
            tmpl = ("tmpl:" + hashlib.sha256(
                sql_text.encode()).hexdigest()[:12])
            graph.append_node(NodeRecord(
                id=tmpl,
                props={"normalized_sql": sql_text,
                       "occurrences": occurrences,
                       "table": physical},
                prov=prov(support=max(occurrences, 1),
                          evidence=f"{d.name}/17_queries_30d/"
                                   "jobs_query_templates.csv")))
            report["templates"] += 1
    return report, blocking
