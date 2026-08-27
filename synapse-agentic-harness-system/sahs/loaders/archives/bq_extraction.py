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
import re
import sys
from pathlib import Path

from sahs.graph.crosswalk import Crosswalk
from sahs.graph.ids import col_id, kind_of, table_id
from sahs.graph.quads import GraphDir, NodeRecord, Prov, Quad

SOURCE = "bq"

# Python's csv module refuses any single field over 128KB ("field
# larger than field limit") — real 01/04 exports carry multi-hundred-KB
# cells (labels/options blobs, embedded DDL). The loader adapts to the
# file: raise the cap to the platform maximum, halving on the rare
# platform where maxsize overflows the underlying C long.
_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_limit)
        break
    except OverflowError:
        _limit //= 2


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


_PROP_FIELD_CAP = 8192


def _first_row_props(rows: list[dict]) -> dict:
    """Whole-first-row harvest (01 table meta, full 13 metrics): the
    loader adapts to whatever columns the extractor wrote — every
    non-empty cell of row one rides along. A cell past 8KB (real 01
    exports carry multi-hundred-KB option blobs) is truncated with its
    byte count and content hash — the store stays lean, the evidence
    stays checkable against the archive."""
    out: dict = {}
    for k, v in (rows[0] if rows else {}).items():
        if not k or v in ("", None):
            continue
        if isinstance(v, str) and len(v) > _PROP_FIELD_CAP:
            digest = hashlib.sha256(v.encode()).hexdigest()[:12]
            v = (v[:2048] + f" …[truncated: {len(v)} bytes, "
                 f"sha256_12={digest} — full text in the archive]")
        out[k] = v
    return out


_PATH_RE = re.compile(r"^[A-Za-z_][\w]*(\.[A-Za-z_][\w]*)+$")


def _field_path_rows(d: Path, track) -> tuple[list[dict], str]:
    """03_logical_column_field_paths ships as csv or json — read
    whichever exists (csv preferred); → (rows, evidence_name)."""
    path = d / "03_logical_column_field_paths.csv"
    rows = _csv_rows(track(path))
    if rows:
        return rows, path.name
    path = d / "03_logical_column_field_paths.json"
    payload = _json(track(path))
    if isinstance(payload, list) and payload \
            and isinstance(payload[0], dict):
        return payload, path.name
    return [], path.name


def _field_path_column(rows: list[dict]) -> str | None:
    """Which column holds the dotted field path? Names containing
    "path" first (pinned priority), then the VALUE signature: the first
    column whose text looks like a dotted identifier path."""
    if not rows:
        return None
    names = [n for n in rows[0] if n]
    for cand in ("field_path", "column_field_path", "field_paths",
                 "path"):
        if cand in names:
            return cand
    for n in names:
        if "path" in n.lower():
            return n
    for n in names:
        if any(_PATH_RE.match(str(r.get(n) or "")) and "."
               in str(r.get(n) or "") for r in rows[:50]):
            return n
    return None


def _as_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [v.strip() for v in re.split(r"[,;]\s*", value.strip())
                if v.strip()]
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    return [str(value)]


def _cols_of(block: dict) -> list[str]:
    for key, value in block.items():
        k = key.lower()
        if "column" in k and "ref" not in k and "target" not in k:
            return _as_list(value)
    return []


def _fk_of(block: dict) -> dict | None:
    columns: list[str] = []
    ref_table = ""
    ref_columns: list[str] = []
    name = str(block.get("name") or block.get("constraint_name") or "")
    ref_block = block.get("referenced") or block.get("references")
    if isinstance(ref_block, dict):
        ref_table = next((str(v) for k, v in ref_block.items()
                          if "table" in k.lower() and v), "")
        ref_columns = _cols_of(ref_block)
    for key, value in block.items():
        k = key.lower()
        if "column" in k and ("ref" in k or "target" in k):
            ref_columns = ref_columns or _as_list(value)
        elif "column" in k:
            columns = columns or _as_list(value)
        elif "table" in k and ("ref" in k or "target" in k):
            ref_table = ref_table or str(value or "")
    if columns and ref_table and ref_columns:
        return {"columns": columns, "ref_table": ref_table,
                "ref_columns": ref_columns, "name": name}
    return None


def _constraint_units(payload):
    """Whatever shape 11_logical_constraints ships — a wrapper dict, a
    row list, nested blocks — yield the candidate constraint dicts."""
    if isinstance(payload, dict):
        for key in ("constraints", "table_constraints",
                    "tableConstraints"):
            if isinstance(payload.get(key), (list, dict)):
                yield from _constraint_units(payload[key])
                return
        yield payload
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, (dict, list)):
                yield from _constraint_units(item)


def _parse_constraints(payload) -> tuple[list[str], list[dict], int]:
    """→ (pk_columns, fks, unrecognized_units). Defensive by design:
    an unrecognized shape is COUNTED and skipped, never a crash — the
    real file's shape gets learned from the first run's report."""
    pks: list[str] = []
    fks: list[dict] = []
    unrecognized = 0
    for unit in _constraint_units(payload):
        handled = False
        for key in ("primary_key", "primaryKey"):
            block = unit.get(key)
            if isinstance(block, dict):
                pks += _cols_of(block)
                handled = True
            elif isinstance(block, list):
                pks += _as_list(block)
                handled = True
        for key in ("foreign_keys", "foreignKeys"):
            block = unit.get(key)
            if isinstance(block, list):
                for fk in block:
                    parsed = _fk_of(fk) if isinstance(fk, dict) else None
                    if parsed:
                        fks.append(parsed)
                        handled = True
                    else:
                        unrecognized += 1
        if handled:
            continue
        # row-per-constraint shapes: the type lives in a string value
        text = " ".join(str(v) for v in unit.values()
                        if isinstance(v, str)).upper()
        if "PRIMARY" in text:
            cols = _cols_of(unit)
            if cols:
                pks += cols
                handled = True
        elif "FOREIGN" in text:
            parsed = _fk_of(unit)
            if parsed:
                fks.append(parsed)
                handled = True
        if not handled:
            unrecognized += 1
    return pks, fks, unrecognized


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
              "co_query_edges": 0, "policies_unknown": 0,
              "nested_columns": 0, "field_path_rows_skipped": 0,
              "field_paths_unmintable": 0, "pk_columns": 0,
              "fk_edges": 0, "fk_out_of_scope": 0,
              "cols_minted_from_constraints": 0,
              "constraints_unrecognized": 0}
    # constraint-declared FKs resolve AFTER the walk — the referenced
    # table's own columns must have minted first, whatever dir order
    minted_cols: set[str] = set()
    fk_pending: list[tuple[str, str, str, str, str, str]] = []
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
        table_meta = _first_row_props(
            _csv_rows(track(d / "01_logical_table_meta.csv")))
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
                # the rest of row one — size/modified/freshness context
                # the old loader discarded past total_rows
                "table_metrics": _first_row_props(metrics) or None,
                "table_meta_logical": table_meta or None,
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
            minted_cols.add(cid)
            report["columns"] += 1

        # 03 — nested field paths: STRUCT members the flat 02 listing
        # can't carry become col nodes of their own (dotted ids); top-
        # level rows are skipped — 02 is authoritative for those
        fp_rows, fp_name = _field_path_rows(d, track)
        path_col = _field_path_column(fp_rows)
        if fp_rows and path_col is None:
            report["field_path_rows_skipped"] += len(fp_rows)
        if path_col:
            names = [n for n in fp_rows[0] if n]
            desc_key = next((n for n in names
                             if "description" in n.lower()), None)
            type_key = next((n for n in names if n != path_col
                             and "type" in n.lower()), None)
            seen_paths: set[str] = set()
            ev03 = f"{d.name}/{fp_name}"
            for row in fp_rows:
                path_value = str(row.get(path_col) or "").strip()
                if "." not in path_value or path_value in seen_paths:
                    continue
                seen_paths.add(path_value)
                cid = col_id(physical, path_value)
                if kind_of(cid) != "col":
                    report["field_paths_unmintable"] += 1
                    continue
                nested_props: dict = {"nested_path": True}
                if type_key and row.get(type_key):
                    nested_props["data_type"] = str(row[type_key])
                if desc_key and row.get(desc_key):
                    nested_props["description_bq"] = str(row[desc_key])
                graph.append_node(NodeRecord(
                    id=cid, props=nested_props,
                    prov=prov(valid_for=[schema_node], evidence=ev03)))
                graph.append_edge(Quad(
                    s=tid, r="has_column", o=cid,
                    prov=prov(valid_for=[schema_node], evidence=ev03)))
                minted_cols.add(cid)
                report["nested_columns"] += 1

        # 11 — declared constraints: PRIMARY KEY lands on the col node,
        # FOREIGN KEYs queue for post-walk resolution (both endpoint
        # tables must have walked). A constraint naming a column the 02
        # export missed still attests it exists — minted, counted.
        constraints = _json(track(d / "11_logical_constraints.json"))
        if constraints is not None:
            pks, fks, unrecognized = _parse_constraints(constraints)
            report["constraints_unrecognized"] += unrecognized
            ev11 = f"{d.name}/11_logical_constraints.json"
            for pk in dict.fromkeys(pks):
                cid = col_id(physical, pk)
                if kind_of(cid) != "col":
                    report["constraints_unrecognized"] += 1
                    continue
                pk_props: dict = {"is_primary_key": True}
                if cid not in minted_cols:
                    minted_cols.add(cid)
                    pk_props["observed_via"] = "constraint_declaration"
                    report["cols_minted_from_constraints"] += 1
                    graph.append_edge(Quad(s=tid, r="has_column", o=cid,
                                           prov=prov(evidence=ev11)))
                graph.append_node(NodeRecord(
                    id=cid, props=pk_props, prov=prov(evidence=ev11)))
                report["pk_columns"] += 1
            for fk in fks:
                for src_col, ref_col in zip(fk["columns"],
                                            fk["ref_columns"]):
                    fk_pending.append((physical, src_col,
                                       fk["ref_table"], ref_col,
                                       fk["name"], ev11))

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
                    minted_cols.add(col_id(physical, column))
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

    # ── post-walk: declared FKs → fk_references edges ──
    # The referenced table resolves through the crosswalk or the edge is
    # a counted skip (identity never guessed); a referenced column the
    # 02 export missed is minted from the constraint's own declaration.
    seen_fk: set[tuple[str, str]] = set()
    for src_physical, src_col, ref_raw, ref_col, name, ev in fk_pending:
        parts = [p for p in ref_raw.lower().strip().split(".") if p]
        ref_physical = None
        if len(parts) >= 2:
            ref_physical = crosswalk.physical_for_bq(parts[-2], parts[-1])
        if ref_physical is None and parts:
            ref_physical = crosswalk.physical_for_short(parts[-1])
        if ref_physical is None:
            report["fk_out_of_scope"] += 1
            continue
        s_cid = col_id(src_physical, src_col)
        o_cid = col_id(ref_physical, ref_col)
        if kind_of(s_cid) != "col" or kind_of(o_cid) != "col":
            report["constraints_unrecognized"] += 1
            continue
        if (s_cid, o_cid) in seen_fk:
            continue
        seen_fk.add((s_cid, o_cid))
        for cid, owner in ((s_cid, src_physical),
                           (o_cid, ref_physical)):
            if cid not in minted_cols:
                minted_cols.add(cid)
                graph.append_node(NodeRecord(
                    id=cid,
                    props={"observed_via": "constraint_declaration"},
                    prov=prov(evidence=ev)))
                graph.append_edge(Quad(
                    s=table_id(owner), r="has_column", o=cid,
                    prov=prov(evidence=ev)))
                report["cols_minted_from_constraints"] += 1
        graph.append_edge(Quad(
            s=s_cid, r="fk_references", o=o_cid,
            props={"constraint": name} if name else {},
            prov=prov(evidence=ev)))
        report["fk_edges"] += 1
    return report, blocking
