"""MDM crawler — the full read-side pull that makes MDM the metadata spine.

Implements the recommended sequences from the recovered API map, per table:

    1. exists      GET /datasets/table-name/exists?tableName={t}
    2. schema      GET /datasets/schemas?tableName={t}          → dataset_parent_id
    3. ownership   GET /datasets/{datasetParentId}/ownership    → business_unit (authoritative)
    4. appflow     GET /app-flows/cdm-storage?tableName={t}     → parent_app_flow_id
    5. pipeline    GET /portal-v2/pipeline/appflow-parent-id/{a}/details
                                                                → governance block
    6. lineage ←   GET /table-lineages/?tableName={t}&isSourceTableName=false
       lineage →   GET /table-lineages/?tableName={t}&isSourceTableName=true
    7. attr-lin    GET /attribute-lineage?tableName={t}         → column derivations
    8. lifecycle   GET /lifecycle/latest?tableName={t}

Safety properties (these are code, not policy prose):
  * GETs only; an explicit path allowlist gates every request, and a
    deny pattern hard-blocks the credential-bearing surfaces
    (api-polling-info, keys, key-schema-mappings) — audit P2-3, enforced.
  * cache-first: every step's raw response lands at
    ``<cache_dir>/<table>/<step>.json`` and is replayed from there
    unless --refresh — so the whole crawl runs OFFLINE from fixtures,
    and a laptop run is resumable.
  * every step is optional except schema; failures become per-step
    notes in the fetch report, never crashes.

Canonical artifacts (what the graph builder consumes):
    out_dir/mdm_cache/<t>.json           enriched digest (spine ID,
                                         business_unit, pipeline,
                                         lifecycle, lineage both ways)
    out_dir/attribute_lineage/<t>.json   column→column derivations
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from synapse.loaders.mdm_digest import (
    digest_schema_response,
    merge_lifecycle,
    merge_ownership,
    merge_pipeline,
)
from synapse.loaders.types import LoadResult

_DENY = re.compile(r"api-polling-info|/keys\b|key-schema-mappings", re.I)
_ALLOWED_PREFIXES = (
    "/datasets/", "/app-flows/", "/portal-v2/",
    "/table-lineages", "/attribute-lineage", "/lifecycle",
)

_DEFAULT_ENDPOINT_HINT = (
    "set SYNAPSE_MDM_BASE to the API base "
    "(e.g. https://<mdm-host>/api/v1/ngbd/mdm-api)"
)


def resolve_base_url(base_url: str | None = None) -> str:
    """SYNAPSE_MDM_BASE, or derived from the legacy SYNAPSE_MDM_ENDPOINT
    (…/mdm-api/datasets/schemas → …/mdm-api)."""
    if base_url:
        return base_url.rstrip("/")
    env_base = os.environ.get("SYNAPSE_MDM_BASE", "").strip()
    if env_base:
        return env_base.rstrip("/")
    legacy = os.environ.get("SYNAPSE_MDM_ENDPOINT", "").strip()
    marker = "/mdm-api"
    if marker in legacy:
        return legacy[: legacy.index(marker) + len(marker)]
    return ""


class MdmCrawler:
    def __init__(
        self,
        base_url: str | None = None,
        *,
        cache_dir: Path | None = None,
        refresh: bool = False,
        timeout_seconds: float = 15.0,
    ) -> None:
        self.base_url = resolve_base_url(base_url)
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.refresh = refresh
        self.timeout = timeout_seconds

    # ── guarded fetch ────────────────────────────────────────

    def _get(self, path_query: str, *, table: str, step: str,
             report: dict[str, str]) -> Any:
        """Allowlist-gated, cache-first GET. Returns parsed JSON or None."""
        if _DENY.search(path_query):
            report[step] = "denied: sensitive endpoint (allowlist policy)"
            return None
        if not path_query.startswith(_ALLOWED_PREFIXES):
            report[step] = f"denied: path not in allowlist ({path_query})"
            return None

        cache_file = (
            self.cache_dir / table / f"{step}.json" if self.cache_dir else None
        )
        if cache_file and cache_file.exists() and not self.refresh:
            try:
                report[step] = "cached"
                return json.loads(cache_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass  # fall through to live fetch

        if not self.base_url:
            report[step] = f"skipped: no MDM base url — {_DEFAULT_ENDPOINT_HINT}"
            return None
        url = f"{self.base_url}{path_query}"
        request = urllib.request.Request(
            url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            report[step] = f"http_{exc.code}"
            return None
        except Exception as exc:  # URLError, timeout, JSON decode
            report[step] = f"error: {str(exc)[:120]}"
            return None
        report[step] = "ok"
        if cache_file:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(
                json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return payload

    # ── the crawl ────────────────────────────────────────────

    def crawl_table(self, table: str) -> dict[str, Any]:
        """Run all eight steps; return {blob, attribute_lineage, report}."""
        report: dict[str, str] = {}
        quoted = urllib.parse.quote(table)

        self._get(f"/datasets/table-name/exists?tableName={quoted}",
                  table=table, step="exists", report=report)

        schema_raw = self._get(f"/datasets/schemas?tableName={quoted}",
                               table=table, step="schema", report=report)
        schema_raw = _peel(schema_raw)
        blob = digest_schema_response(
            schema_raw if isinstance(schema_raw, dict) else {}, table)

        parent_id = blob.get("dataset_parent_id")
        if parent_id:
            ownership = self._get(
                f"/datasets/{urllib.parse.quote(str(parent_id))}/ownership",
                table=table, step="ownership", report=report)
            merge_ownership(blob, _peel(ownership))
        else:
            report["ownership"] = "skipped: no dataset_parent_id in schema"

        appflow = _peel(self._get(
            f"/app-flows/cdm-storage?tableName={quoted}",
            table=table, step="appflow", report=report))
        appflow_parent = _first_key(
            appflow, "parent_app_flow_id", "app_flow_parent_id",
            "appflow_parent_id", "parentAppFlowId")
        if appflow_parent:
            pipeline = self._get(
                "/portal-v2/pipeline/appflow-parent-id/"
                f"{urllib.parse.quote(str(appflow_parent))}/details",
                table=table, step="pipeline", report=report)
            merge_pipeline(blob, _peel(pipeline))
        else:
            report.setdefault(
                "pipeline", "skipped: no appflow parent id")

        upstream_rows = self._get(
            f"/table-lineages/?tableName={quoted}&isSourceTableName=false",
            table=table, step="lineage_up", report=report)
        downstream_rows = self._get(
            f"/table-lineages/?tableName={quoted}&isSourceTableName=true",
            table=table, step="lineage_down", report=report)
        blob["lineage_upstream"] = _lineage_names(
            upstream_rows, side="source", exclude=table)
        blob["lineage_downstream"] = _lineage_names(
            downstream_rows, side="target", exclude=table)

        attr_rows = self._get(
            f"/attribute-lineage?tableName={quoted}",
            table=table, step="attr_lineage", report=report)
        attribute_lineage = _attribute_lineage(attr_rows)

        lifecycle = self._get(
            f"/lifecycle/latest?tableName={quoted}",
            table=table, step="lifecycle", report=report)
        merge_lifecycle(blob, _peel(lifecycle))

        return {
            "blob": blob,
            "attribute_lineage": attribute_lineage,
            "report": report,
        }


# ─── tolerant payload helpers ────────────────────────────────


def _peel(payload: Any) -> Any:
    """MDM wraps most single objects in a 1-element array."""
    if isinstance(payload, list):
        return payload[0] if payload else {}
    return payload


def _first_key(obj: Any, *keys: str) -> Any:
    if not isinstance(obj, dict):
        return None
    for key in keys:
        if obj.get(key) not in (None, ""):
            return obj[key]
    return None


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):  # sometimes {content: [...]}
        payload = payload.get("content") or payload.get("data") or []
    return [r for r in (payload or []) if isinstance(r, dict)]


def _lineage_names(payload: Any, *, side: str, exclude: str) -> list[str]:
    """TableLineage rows → the other-side table names, deduped."""
    keys = (f"{side}_table_name", f"{side}_name", f"{side}_table",
            f"{side}TableName")
    names: list[str] = []
    for row in _rows(payload):
        name = _first_key(row, *keys)
        if name and str(name).lower() != exclude.lower() \
                and name not in names:
            names.append(str(name))
    return names


def _attribute_lineage(payload: Any) -> list[dict[str, Any]]:
    out = []
    for row in _rows(payload):
        entry = {
            "src_table": _first_key(row, "source_table_name", "source_table",
                                    "sourceTableName") or "",
            "src_column": _first_key(row, "source_column_name",
                                     "source_column", "source_attribute_name",
                                     "sourceColumnName") or "",
            "dst_table": _first_key(row, "target_table_name", "target_table",
                                    "targetTableName") or "",
            "dst_column": _first_key(row, "target_column_name",
                                     "target_column", "target_attribute_name",
                                     "targetColumnName") or "",
            "derivation_logic": _first_key(row, "derivation_logic",
                                           "derivation", "transform_logic")
            or "",
            "pipeline_id": _first_key(row, "pipeline_id", "pipelineId") or "",
        }
        if entry["src_column"] and entry["dst_column"]:
            out.append(entry)
    return out


# ─── LoadResult entrypoint (pipeline-facing) ─────────────────


def crawl_mdm_for_table(
    table_id: str,
    *,
    out_dir: Path,
    base_url: str | None = None,
    cache_dir: Path | None = None,
    refresh: bool = False,
    timeout_seconds: float = 15.0,
    dry_run: bool = False,
) -> LoadResult:
    """Full crawl for one table → canonical artifacts + fetch report."""
    started = time.monotonic()
    crawler = MdmCrawler(base_url, cache_dir=cache_dir, refresh=refresh,
                         timeout_seconds=timeout_seconds)
    result = crawler.crawl_table(table_id)
    blob, report = result["blob"], result["report"]

    if report.get("schema") not in ("ok", "cached"):
        return LoadResult(
            status="error", source="mdm", table_id=table_id,
            error=f"schema step failed: {report.get('schema')}",
            metadata={"fetch_report": report},
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    written: list[Path] = []
    if not dry_run:
        cache_out = Path(out_dir) / "mdm_cache"
        cache_out.mkdir(parents=True, exist_ok=True)
        blob_path = cache_out / f"{table_id}.json"
        blob_path.write_text(
            json.dumps(blob, indent=2, default=str), encoding="utf-8")
        written.append(blob_path)
        if result["attribute_lineage"]:
            attr_dir = Path(out_dir) / "attribute_lineage"
            attr_dir.mkdir(parents=True, exist_ok=True)
            attr_path = attr_dir / f"{table_id}.json"
            attr_path.write_text(json.dumps({
                "table_name": table_id,
                "mappings": result["attribute_lineage"],
            }, indent=2), encoding="utf-8")
            written.append(attr_path)

    degraded = [s for s, v in report.items()
                if v not in ("ok", "cached") and s != "schema"]
    return LoadResult(
        status="ok" if not degraded else "partial",
        source="mdm",
        table_id=table_id,
        artifacts_written=written,
        records_count=len(blob.get("columns") or []),
        warnings=[f"{s}: {report[s]}" for s in degraded],
        latency_ms=int((time.monotonic() - started) * 1000),
        metadata={
            "fetch_report": report,
            "dataset_parent_id": blob.get("dataset_parent_id"),
            "business_unit": blob.get("business_unit"),
            "n_upstream": len(blob.get("lineage_upstream") or []),
            "n_downstream": len(blob.get("lineage_downstream") or []),
            "n_attribute_mappings": len(result["attribute_lineage"]),
        },
    )
