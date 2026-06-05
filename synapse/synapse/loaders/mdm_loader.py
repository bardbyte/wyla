"""MDM loader — fetches `/api/v1/ngbd/mdm-api/datasets/schemas?tableName=<t>`
from the corporate intranet and writes the canonical `mdm_cache/<t>.json`
the graph builder reads.

Discovery facts (already confirmed in scripts/check_mdm_access.py):
  - Endpoint: https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas?tableName=<table>
  - No auth required on VPN
  - Response is an ARRAY of length 1; real data at [0]
  - Top-level keys: dataset_details, schema, dataset_source_details, ownership_details
  - schema.schema_attributes is the column list

This loader is pure-function. ADK FunctionTool-wrappable.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import urllib.request
import urllib.error

from synapse.loaders.types import LoadResult


_MDM_ENDPOINT = "https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas"


def load_mdm_for_table(
    table_id: str,
    *,
    source_dir: Path | None = None,   # if provided, look for cached raw JSON here first
    out_dir: Path,
    force_refresh: bool = False,
    dry_run: bool = False,
    endpoint: str = _MDM_ENDPOINT,
    timeout_seconds: float = 10.0,
) -> LoadResult:
    """Hit MDM for one table; write the digested JSON the graph builder reads.

    Args:
        table_id: target table name (`tableName=` query param).
        source_dir: if provided, prefer reading `<source_dir>/<table>__mdm_raw.json`
            over a network call (offline / development mode).
        out_dir: parent folder; output lands at `out_dir/mdm_cache/<table>.json`.
        force_refresh: if True, hit the network even if a cached file exists.
        dry_run: parse + validate but skip the write.
        endpoint: override (testing).
        timeout_seconds: HTTP timeout.
    """
    t0 = time.time()
    raw: dict[str, Any] | None = None
    cache_hit = False

    # Try cached raw first if available
    if source_dir is not None:
        cached = source_dir / f"{table_id}__mdm_raw.json"
        if cached.exists() and not force_refresh:
            try:
                raw = _peel_array_wrapper(json.loads(cached.read_text(encoding="utf-8")))
                cache_hit = True
            except Exception:
                pass

    if raw is None:
        url = f"{endpoint}?tableName={table_id}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
                payload = json.loads(resp.read().decode("utf-8"))
            raw = _peel_array_wrapper(payload)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            return LoadResult(
                status="error", source="mdm", table_id=table_id,
                error=f"MDM fetch failed: {e}",
                latency_ms=int((time.time() - t0) * 1000),
            )

    blob = _digest_mdm_response(raw, table_id)
    written: list[Path] = []
    if not dry_run:
        out_path = out_dir / "mdm_cache" / f"{table_id}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(blob, indent=2, default=str), encoding="utf-8")
        written.append(out_path)

    return LoadResult(
        status="ok",
        source="mdm",
        table_id=table_id,
        artifacts_written=written,
        records_count=len(blob.get("columns", [])),
        cache_hit=cache_hit,
        latency_ms=int((time.time() - t0) * 1000),
    )


# ─── Response shaping ─────────────────────────────────────────


def _peel_array_wrapper(payload: Any) -> dict[str, Any]:
    """MDM returns `[ { … } ]`; always peel."""
    if isinstance(payload, list):
        return payload[0] if payload else {}
    if isinstance(payload, dict):
        return payload
    return {}


def _digest_mdm_response(raw: dict[str, Any], table_id: str) -> dict[str, Any]:
    """Map MDM's verbose response into the same shape the synthetic generator
    emits — so the graph builder reads it identically."""
    if not raw:
        return {"table_name": table_id, "columns": [], "mdm_coverage_pct": 0.0}

    dataset = raw.get("dataset_details", {}) or {}
    source  = raw.get("dataset_source_details", {}) or {}
    owners  = raw.get("ownership_details", {}) or {}
    schema  = raw.get("schema", {}) or {}
    sens    = raw.get("sensitivity_details", []) or []
    sens_by_attr = {
        s.get("attribute_name"): s for s in sens if isinstance(s, dict)
    }

    cols_out: list[dict[str, Any]] = []
    partition_field: str | None = None

    for c in (schema.get("schema_attributes") or []):
        if not isinstance(c, dict):
            continue
        details = c.get("attribute_details", {}) or {}
        name = c.get("attribute_name") or details.get("attribute_name")
        if not name:
            continue
        sens_row = sens_by_attr.get(name, {})
        is_partition = (
            (details.get("is_partitioned") in (True, "Y", "true", "1"))
            or (details.get("partition_role") in ("PARTITION", "PARTITION_KEY"))
        )
        if is_partition and not partition_field:
            partition_field = name
        cols_out.append({
            "name": name,
            "type": details.get("attribute_type") or details.get("data_type") or "",
            "description": details.get("attribute_desc") or "",
            "business_name": details.get("business_name") or "",
            "is_primary": bool(details.get("is_primary_key") or details.get("is_pk")),
            "is_dedupe_key": bool(details.get("is_dedupe_key")),
            "is_partitioned": is_partition,
            "cluster_position": details.get("clustering_ordinal_position"),
            "derived_logic": details.get("derived_logic"),
            "is_pii": bool(sens_row.get("is_pii")),
            "is_critical_data_element": bool(sens_row.get("is_critical_data_element")),
            "pii_role_id": sens_row.get("pii_role_id") or "Internal",
            "is_gdpr": bool(sens_row.get("is_gdpr")),
        })

    business_contacts = owners.get("business_contacts") or []
    tech_contacts = owners.get("tech_contacts") or []

    return {
        "table_name": table_id,
        "display_name": dataset.get("business_name") or table_id,
        "table_business_name": dataset.get("business_name") or "",
        "table_description": dataset.get("data_desc") or "",
        "data_category": dataset.get("data_category") or "",
        "data_sub_category": dataset.get("data_sub_category") or "",
        "bq_project": _resolve_templated(source.get("project_id")),
        "bq_dataset": source.get("dataset_name") or "",
        "bq_table": source.get("table_name") or table_id,
        "feed_type": dataset.get("feed_type") or "",
        "table_type": dataset.get("table_type") or "",
        "is_decommissioned": bool(dataset.get("is_decommissioned")),
        "partition_field": partition_field,
        "row_count_estimate": _maybe_int(dataset.get("row_count")),
        "ownership": {
            "imr_queue": owners.get("imr_queue"),
            "aim_id": owners.get("aim_id"),
            "business_contacts": business_contacts,
            "tech_contacts": tech_contacts,
        },
        "columns": cols_out,
        "mdm_coverage_pct": 1.0,
        # Dataplex-style fields stay empty for v1 — Phase 2 (steward + LLM)
        # will populate. The builder treats missing as default.
        "asset_kind": "Table",
        "tags": [],
        "lineage_upstream": [],
    }


def _resolve_templated(s: str | None) -> str:
    """MDM stores templated values like `@context.system/project_id`. Real
    project comes from env or BQ snapshot. Pass through for now."""
    return s or ""


def _maybe_int(v: Any) -> int | None:
    if v in (None, "", "null"):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None
