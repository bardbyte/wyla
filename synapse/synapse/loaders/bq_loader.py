"""BigQuery loader — reads the outputs of `synapse/sql/bq_table_extraction.sql`
and writes the canonical JSON the graph builder consumes.

Input layout (one folder per table, as the BQ_EXTRACTION_GUIDE.md describes):

    ~/synapse_bq_outputs/<table>/
      1_1__columns.csv
      1_3__table_meta.json
      1_4__table_options.csv
      1_5__constraints.csv               (optional)
      2_1__size_freshness.csv
      2_2__partitions.csv                (optional)
      3_1__cardinality_nulls.csv         (optional — Tier 4)
      3_2__topcount__<col>.csv           (optional — one per low-card column)
      4_1__top_users.csv                 (optional — Tier 2)
      4_2__peak_hours.csv                (optional)
      4_3__co_queried_tables.csv         (optional)
      4_4__sample_queries.json           (optional)
      5_1__policy_tags.csv               (optional — Tier 3)
      5_2__access_grants.csv             (optional)
      6_1__dataplex_dq.csv               (optional — Tier 6)
      8_1__ddl_history.csv               (optional)
      9_1__upstream_tables.csv           (optional — Tier 5)
      9_2__downstream_tables.csv         (optional)
      10__snapshot.json                  (smoke-test)

Output layout (matches synthetic generator — graph builder reads from here):

    synapse/data/real/
      bq_cache/<table>.json
      usage_history/<table>.json
      dq_rules/<table>.json
      lineage/<table>.json               (new — empirical from BQ JOBS)
      gold_queries/Q<table>__<n>.sql     (sample queries from 4.4)
"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any

from synapse.loaders.types import LoadResult


def load_bq_for_table(
    table_id: str,
    *,
    source_dir: Path,
    out_dir: Path,
    force_refresh: bool = False,
    dry_run: bool = False,
) -> LoadResult:
    """Read BQ-extraction artifacts for one table; emit canonical JSON files.

    Args:
        table_id: target table name (matches the subfolder name in source_dir).
        source_dir: parent folder containing <table_id>/ with the CSVs/JSONs.
        out_dir: parent folder under which bq_cache/, usage_history/, etc. land.
        force_refresh: ignored for BQ loader (read-only over user-supplied files).
        dry_run: if True, parse + validate inputs but don't write any outputs.

    Returns:
        LoadResult with paths written, record counts, and any per-section
        warnings (e.g. "Tier 3 policy tags file missing — skipped").
    """
    t0 = time.time()
    warnings: list[str] = []
    written: list[Path] = []

    src = source_dir / table_id
    if not src.exists():
        return LoadResult(
            status="error", source="bq", table_id=table_id,
            error=f"source dir not found: {src}",
        )

    bq_cache_dir   = out_dir / "bq_cache"
    usage_dir      = out_dir / "usage_history"
    dq_dir         = out_dir / "dq_rules"
    lineage_dir    = out_dir / "lineage"
    gold_dir       = out_dir / "gold_queries"

    # ── 1. Build the bq_cache/<table>.json blob ─────────────────
    bq_blob = _build_bq_cache_blob(src, table_id, warnings)
    if not dry_run:
        bq_cache_dir.mkdir(parents=True, exist_ok=True)
        path = bq_cache_dir / f"{table_id}.json"
        path.write_text(json.dumps(bq_blob, indent=2, default=str), encoding="utf-8")
        written.append(path)

    # ── 2. Usage history ────────────────────────────────────────
    usage_blob = _build_usage_blob(src, table_id, warnings)
    if usage_blob and not dry_run:
        usage_dir.mkdir(parents=True, exist_ok=True)
        path = usage_dir / f"{table_id}.json"
        path.write_text(json.dumps(usage_blob, indent=2, default=str), encoding="utf-8")
        written.append(path)

    # ── 3. DQ rules (from Dataplex if present, else from profiling) ──
    dq_blob = _build_dq_blob(src, table_id, warnings, bq_blob)
    if dq_blob and not dry_run:
        dq_dir.mkdir(parents=True, exist_ok=True)
        path = dq_dir / f"{table_id}.json"
        path.write_text(json.dumps(dq_blob, indent=2, default=str), encoding="utf-8")
        written.append(path)

    # ── 4. Lineage (empirical, from BQ JOBS sections 9.1 + 9.2) ─
    lineage_blob = _build_lineage_blob(src, table_id, warnings)
    if lineage_blob and not dry_run:
        lineage_dir.mkdir(parents=True, exist_ok=True)
        path = lineage_dir / f"{table_id}.json"
        path.write_text(json.dumps(lineage_blob, indent=2, default=str), encoding="utf-8")
        written.append(path)

    # ── 5. Gold queries (sample queries from 4.4 become the corpus) ──
    n_queries = _write_gold_queries(src, table_id, gold_dir, dry_run, warnings)

    return LoadResult(
        status="ok" if not warnings else "partial",
        source="bq",
        table_id=table_id,
        artifacts_written=written,
        records_count=(
            len(bq_blob.get("columns", []))
            + (usage_blob.get("total_queries", 0) if usage_blob else 0)
            + n_queries
        ),
        warnings=warnings,
        latency_ms=int((time.time() - t0) * 1000),
    )


# ─── Section builders ────────────────────────────────────────


def _build_bq_cache_blob(src: Path, table_id: str, warnings: list[str]) -> dict[str, Any]:
    """Map sections 1.1, 1.3, 1.4, 2.1, 2.2, 3.1, 3.2, 5.1 → bq_cache/<t>.json.

    Output shape matches what synapse/synthetic/generator.py:generate_bq_profile
    emits (so the existing graph builder reads it without change).
    """
    blob: dict[str, Any] = {
        "table_name": table_id,
        "columns": [],
        "row_count": None,
        "last_modified": None,
        "partition_field": None,
        "clustering_fields": [],
        "column_stats": {},
        "distinct_values": {},
        "policy_tags_by_column": {},
    }

    # 1.1 — columns + types + partition/cluster role
    cols_path = src / "1_1__columns.csv"
    if cols_path.exists():
        with cols_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                blob["columns"].append({
                    "name": row.get("column_name"),
                    "data_type": row.get("data_type"),
                    "is_nullable": (row.get("is_nullable") or "").upper() == "YES",
                    "is_partitioning_column": (row.get("is_partitioning_column") or "").upper() == "YES",
                    "clustering_ordinal": _maybe_int(row.get("clustering_ordinal_position")),
                    "description_bq": "",  # filled from 1.2 if present
                })
                if (row.get("is_partitioning_column") or "").upper() == "YES":
                    blob["partition_field"] = row.get("column_name")
                cluster_pos = _maybe_int(row.get("clustering_ordinal_position"))
                if cluster_pos:
                    blob["clustering_fields"].append(row.get("column_name"))
    else:
        warnings.append("missing 1_1__columns.csv (required) — Table node will be sparse")

    # 1.2 — column descriptions (overlay if present)
    desc_path = src / "1_2__column_descriptions.csv"
    if desc_path.exists():
        descs = {}
        with desc_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("description"):
                    descs[row["column_name"]] = row["description"]
        for c in blob["columns"]:
            if c["name"] in descs:
                c["description_bq"] = descs[c["name"]]

    # 1.3 — table meta + DDL
    meta_path = src / "1_3__table_meta.json"
    if meta_path.exists():
        meta = _read_json_first_row(meta_path)
        blob["table_type"] = meta.get("table_type")
        blob["created_at"] = meta.get("creation_time")
        blob["ddl_snapshot"] = meta.get("ddl")
        blob["asset_kind"] = {
            "BASE TABLE": "Table",
            "VIEW": "View",
            "MATERIALIZED VIEW": "MaterializedView",
            "EXTERNAL": "ExternalTable",
        }.get(meta.get("table_type", "BASE TABLE"), "Table")

    # 1.4 — options (labels, require_partition_filter, etc.)
    opts_path = src / "1_4__table_options.csv"
    if opts_path.exists():
        opts = {}
        with opts_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                opts[row.get("option_name")] = row.get("option_value")
        blob["table_options"] = opts
        # Surface labels as tags
        if "labels" in opts:
            try:
                # BigQuery returns labels as STRUCT array — string form parseable
                labels = opts["labels"]
                blob["tags"] = [labels] if isinstance(labels, str) else list(labels)
            except Exception:
                pass

    # 2.1 — size + freshness
    size_path = src / "2_1__size_freshness.csv"
    if size_path.exists():
        with size_path.open(encoding="utf-8-sig", newline="") as f:
            row = next(csv.DictReader(f), {})
            blob["row_count"] = _maybe_int(row.get("row_count"))
            blob["size_bytes"] = _maybe_int(row.get("size_bytes"))
            blob["last_modified"] = row.get("last_modified_at")

    # 2.2 — partitions (count + freshness per partition)
    parts_path = src / "2_2__partitions.csv"
    if parts_path.exists():
        partitions = []
        with parts_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                partitions.append({
                    "partition_id": row.get("partition_id"),
                    "total_rows": _maybe_int(row.get("total_rows")),
                    "last_modified": row.get("last_modified_time"),
                    "storage_tier": row.get("storage_tier"),
                })
        blob["partitions_sample"] = partitions[:60]
        # If any partition is a real date string, set grain=daily
        if partitions and len(partitions[0]["partition_id"] or "") == 8:
            blob["partition_grain"] = "daily"

    # 3.1 — cardinality + null fractions (one row, columns mashed together)
    card_path = src / "3_1__cardinality_nulls.csv"
    if card_path.exists():
        with card_path.open(encoding="utf-8-sig", newline="") as f:
            row = next(csv.DictReader(f), {})
            for k, v in row.items():
                if "__" not in k:
                    continue
                col, metric = k.rsplit("__", 1)
                blob["column_stats"].setdefault(col, {})[metric] = (
                    _maybe_float(v) if metric in ("null_frac", "avg", "min", "max")
                    else _maybe_int(v) if metric == "distinct"
                    else v
                )
                # Also write the canonical key our synthetic uses
                if metric == "distinct":
                    blob["column_stats"][col]["approx_distinct"] = _maybe_int(v)
                if metric == "null_frac":
                    blob["column_stats"][col]["null_fraction"] = _maybe_float(v)

    # 3.2 — per-column top values
    for top_path in src.glob("3_2__topcount__*.csv"):
        col_name = top_path.stem.replace("3_2__topcount__", "")
        values = []
        with top_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                values.append({
                    "value": row.get("value"),
                    "count": _maybe_int(row.get("row_count")),
                })
        blob["distinct_values"][col_name] = values

    # 5.1 — policy tags (PII classification)
    pt_path = src / "5_1__policy_tags.csv"
    if pt_path.exists():
        with pt_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                col = row.get("column_name")
                tag = row.get("display_name") or row.get("name")
                if col and tag:
                    blob["policy_tags_by_column"].setdefault(col, []).append(tag)

    return blob


def _build_usage_blob(
    src: Path, table_id: str, warnings: list[str],
) -> dict[str, Any] | None:
    """Section 4.1, 4.2, 4.4 → usage_history/<t>.json (matches synthetic shape)."""
    top_path  = src / "4_1__top_users.csv"
    peak_path = src / "4_2__peak_hours.csv"
    if not (top_path.exists() or peak_path.exists()):
        return None

    blob: dict[str, Any] = {
        "table_name": table_id,
        "total_queries": 0,
        "top_users": [],
        "peak_query_hours": [],
        "per_column_reference_count": {},
    }
    if top_path.exists():
        with top_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                qc = _maybe_int(row.get("query_count")) or 0
                blob["top_users"].append({
                    "email": row.get("user_email"),
                    "team": _team_from_email(row.get("user_email", "")),
                    "query_count": qc,
                    "total_bytes_billed": _maybe_int(row.get("bytes_billed")) or 0,
                    "first_seen": row.get("first_seen"),
                    "last_seen": row.get("last_seen"),
                    "active_days": _maybe_int(row.get("active_days")),
                })
                blob["total_queries"] += qc
    if peak_path.exists():
        hours = []
        with peak_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                h = _maybe_int(row.get("hour_utc"))
                if h is not None:
                    hours.append((h, _maybe_int(row.get("queries")) or 0))
        hours.sort(key=lambda kv: -kv[1])
        blob["peak_query_hours"] = [h for h, _ in hours[:4]]
    return blob


def _build_dq_blob(
    src: Path, table_id: str, warnings: list[str], bq_blob: dict,
) -> dict[str, Any] | None:
    """Section 6.1 if Dataplex; otherwise synthesize from profile + schema."""
    dx_path = src / "6_1__dataplex_dq.csv"
    rules: list[dict[str, Any]] = []
    if dx_path.exists():
        with dx_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                rules.append({
                    "rule_id": f"{table_id}__{row.get('rule_name')}",
                    "target_column": row.get("target_column"),
                    "rule_kind": (row.get("rule_dimension") or "").lower(),
                    "threshold": row.get("threshold_value"),
                    "last_run_status": "pass" if (row.get("passed") or "").lower() in ("true", "1") else "fail",
                    "last_run_value": row.get("passed_count"),
                    "severity": "warning",
                    "auto_suggested": False,
                })

    # Synthesize rules from profile if Dataplex didn't supply any
    if not rules and bq_blob.get("row_count"):
        rules.append({
            "rule_id": f"{table_id}__row_count",
            "target_column": None,
            "rule_kind": "row_count",
            "threshold": f"row_count > {max(1, bq_blob['row_count'] // 10):,}",
            "last_run_status": "pass",
            "last_run_value": str(bq_blob["row_count"]),
            "severity": "error",
            "auto_suggested": False,
        })
        if bq_blob.get("last_modified"):
            rules.append({
                "rule_id": f"{table_id}__freshness",
                "target_column": None,
                "rule_kind": "freshness",
                "threshold": "freshness_hours < 24",
                "last_run_status": "pass",
                "last_run_value": bq_blob["last_modified"],
                "severity": "warning",
                "auto_suggested": True,
            })
        # Per-column not_null from profile
        for col, stats in (bq_blob.get("column_stats") or {}).items():
            nf = stats.get("null_fraction")
            if nf is not None and nf < 0.01:
                rules.append({
                    "rule_id": f"{table_id}__{col}__not_null",
                    "target_column": col,
                    "rule_kind": "not_null",
                    "threshold": "null_pct < 0.01",
                    "last_run_status": "pass",
                    "last_run_value": f"{nf:.4f}",
                    "severity": "error",
                    "auto_suggested": True,
                })

    if not rules:
        return None
    return {
        "table_name": table_id,
        "engine": "bq_loader_synthesized" if not dx_path.exists() else "dataplex_auto_dq",
        "last_run_at": bq_blob.get("last_modified") or "",
        "rules": rules,
    }


def _build_lineage_blob(
    src: Path, table_id: str, warnings: list[str],
) -> dict[str, Any] | None:
    """Sections 9.1 + 9.2 → lineage/<t>.json (empirical upstream/downstream)."""
    up_path = src / "9_1__upstream_tables.csv"
    down_path = src / "9_2__downstream_tables.csv"
    co_path = src / "4_3__co_queried_tables.csv"
    if not (up_path.exists() or down_path.exists() or co_path.exists()):
        return None
    blob: dict[str, Any] = {
        "table_name": table_id,
        "lineage_upstream": [],
        "lineage_downstream": [],
        "co_queried": [],
    }
    if up_path.exists():
        with up_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                fqn = row.get("upstream_table") or ""
                short = fqn.rsplit(".", 1)[-1]
                if short and short != table_id:
                    blob["lineage_upstream"].append(short)
    if down_path.exists():
        with down_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                fqn = row.get("downstream_table") or ""
                short = fqn.rsplit(".", 1)[-1]
                if short and short != table_id:
                    blob["lineage_downstream"].append(short)
    if co_path.exists():
        with co_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                fqn = row.get("co_referenced_table") or ""
                short = fqn.rsplit(".", 1)[-1]
                cnt = _maybe_int(row.get("co_query_count")) or 0
                if short and short != table_id:
                    blob["co_queried"].append({"table": short, "count": cnt})
    return blob


def _write_gold_queries(
    src: Path, table_id: str, gold_dir: Path,
    dry_run: bool, warnings: list[str],
) -> int:
    """Section 4.4 → one .sql file per recent query (becomes corpus)."""
    sample_path = src / "4_4__sample_queries.json"
    if not sample_path.exists():
        return 0
    try:
        # BQ may export JSON as newline-delimited or as an array
        text = sample_path.read_text(encoding="utf-8").strip()
        if text.startswith("["):
            rows = json.loads(text)
        else:
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    except Exception as e:  # noqa: BLE001
        warnings.append(f"could not parse 4_4__sample_queries.json: {e}")
        return 0
    if dry_run:
        return len(rows)
    gold_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for i, row in enumerate(rows[:200]):
        q = row.get("query")
        if not q:
            continue
        path = gold_dir / f"Q__{table_id}__{i:03d}.sql"
        path.write_text(
            f"-- job_id: {row.get('job_id')}\n"
            f"-- user: {row.get('user_email')}\n"
            f"-- created_at: {row.get('creation_time')}\n"
            f"{q.strip()}\n",
            encoding="utf-8",
        )
        n += 1
    return n


# ─── Helpers ─────────────────────────────────────────────────


def _maybe_int(v: Any) -> int | None:
    if v in (None, "", "null", "NULL"):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _maybe_float(v: Any) -> float | None:
    if v in (None, "", "null", "NULL"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _team_from_email(email: str) -> str:
    """Best-effort team derivation from email prefix.

    `risk-modeling-3@example.com` → "Risk Modeling". `j.doe@x.com` → "".
    The MDM loader can override with declared team membership if available.
    """
    if not email or "@" not in email:
        return ""
    local = email.split("@", 1)[0]
    parts = local.replace(".", "-").split("-")
    if len(parts) <= 1:
        return ""
    # Drop trailing numeric suffixes
    while parts and parts[-1].isdigit():
        parts.pop()
    return " ".join(p.title() for p in parts) if parts else ""


def _read_json_first_row(path: Path) -> dict[str, Any]:
    """First/only JSON object from a file, whatever its shape.

    Accepts a single (possibly pretty-printed multi-line) object, a JSON
    array, or newline-delimited JSON — bq_batch_extract writes the meta
    with ``json.dump(..., indent=2)`` (pretty single object), so reading
    only the first line grabbed a bare ``{`` and crashed. Whole-file
    parse first, JSONL fallback second.
    """
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)                    # pretty object OR array
    except json.JSONDecodeError:
        data = None
        for line in text.splitlines():             # newline-delimited JSON
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                data = {}
            break
    if isinstance(data, list):
        data = data[0] if data else {}
    return data if isinstance(data, dict) else {}
