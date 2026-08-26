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


def load_bq_archive(root: Path, graph: GraphDir, crosswalk: Crosswalk,
                    run_id: str, ledger=None) -> tuple[dict, list[str]]:
    """→ (report, blocking_errors)."""
    root = Path(root)
    blocking: list[str] = []
    def track(path: Path):
        if ledger is not None:
            ledger.consumed(path)
        return path

    report = {"tables": 0, "columns": 0, "domains": 0, "templates": 0,
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
        if physical is None:
            blocking.append(
                f"crosswalk: no row for bq table {dataset}.{name} "
                f"(archive dir {d.name})")
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
        users = _csv_rows(track(d / "17_queries_30d" / "jobs_top_users.csv"))
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
            for value_file in map(track, sorted(lc_dir.glob("*.csv"))):
                rows = _csv_rows(value_file)
                if not rows:
                    continue
                column = rows[0]["column_name"]
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

        for row in _csv_rows(
                track(d / "17_queries_30d" / "jobs_co_queried_tables.csv")):
            other_physical = crosswalk.physical_for_lumi(row["other_table"])
            if other_physical is None:
                continue                 # co-query partner outside scope
            graph.append_edge(Quad(
                s=tid, r="co_queried_with", o=table_id(other_physical),
                prov=prov(support=max(int(row["co_query_count"]), 1),
                          evidence=f"{d.name}/17_queries_30d/"
                                   "jobs_co_queried_tables.csv")))
            report["co_query_edges"] += 1

        for row in _csv_rows(
                track(d / "17_queries_30d" / "jobs_query_templates.csv")):
            tmpl = ("tmpl:" + hashlib.sha256(
                row["normalized_sql"].encode()).hexdigest()[:12])
            graph.append_node(NodeRecord(
                id=tmpl,
                props={"normalized_sql": row["normalized_sql"],
                       "occurrences": int(row.get("occurrences") or 1),
                       "table": physical},
                prov=prov(support=max(int(row.get("occurrences") or 1), 1),
                          evidence=f"{d.name}/17_queries_30d/"
                                   "jobs_query_templates.csv")))
            report["templates"] += 1
    return report, blocking
