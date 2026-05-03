"""MDM client implementations.

Two real implementations:
  CachedMDMClient — reads pre-fetched digests from data/mdm_cache/*.json
                    (populated by scripts/probe_mdm.py). Use this in
                    production runs and for offline iteration.
  HttpMDMClient   — live HTTP call to the MDM endpoint. Use for one-shot
                    refreshes; otherwise CachedMDMClient is faster and
                    doesn't need VPN.

Both satisfy the MDMClientProto in lumi.sql_to_context — anything with a
.fetch(table_name) -> dict method works.

The cache files use the SAME shape as scripts/probe_mdm.py:digest()
output, so the cache is just probe_mdm.py's output dropped on disk.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger("lumi.mdm")


# ─── Cached client (offline, fast, the production default) ────────


class CachedMDMClient:
    """Reads pre-fetched MDM digests from disk.

    Populate the cache once with `python scripts/probe_mdm.py --save <dir>`,
    then point this client at the same dir. Cache misses log a warning and
    return an empty digest so the pipeline degrades gracefully (the table
    just gets mdm_coverage_pct=0.0 instead of crashing).
    """

    def __init__(self, cache_dir: str | Path) -> None:
        self.cache_dir = Path(cache_dir)
        self._misses: list[str] = []  # for diagnostics

    def fetch(self, table_name: str) -> dict[str, Any]:
        path = self.cache_dir / f"{table_name}.json"
        if not path.exists():
            logger.warning(
                "MDM cache miss for %s (expected at %s) — degraded context",
                table_name,
                path,
            )
            self._misses.append(table_name)
            return _empty_digest(table_name)
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logger.warning(
                "MDM cache read error for %s (%s) — using empty digest",
                table_name,
                e,
            )
            self._misses.append(table_name)
            return _empty_digest(table_name)

    @property
    def cache_misses(self) -> list[str]:
        """Tables we returned an empty digest for. Useful for reporting."""
        return list(self._misses)


# ─── Live HTTP client (use for refreshes; CachedMDMClient is the default) ─


class HttpMDMClient:
    """Live HTTP call to the MDM endpoint. Same shape as scripts/probe_mdm.py.

    Behaves identically to CachedMDMClient on the consumer side — both
    implement .fetch(table_name) -> dict. Use HttpMDMClient when you want
    to bypass the cache (e.g., scheduled refresh job).
    """

    DEFAULT_ENDPOINT = (
        "https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas"
    )
    DEFAULT_TIMEOUT_SECS = 30

    def __init__(
        self,
        endpoint: str = DEFAULT_ENDPOINT,
        timeout_secs: int = DEFAULT_TIMEOUT_SECS,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.timeout_secs = timeout_secs

    def fetch(self, table_name: str) -> dict[str, Any]:
        qs = urllib.parse.urlencode({"tableName": table_name})
        url = f"{self.endpoint}?{qs}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                payload = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            logger.warning("MDM HTTP %s for %s — empty digest", e.code, table_name)
            return _empty_digest(table_name)
        except urllib.error.URLError as e:
            logger.warning(
                "MDM connection failure for %s (%s) — empty digest",
                table_name,
                e.reason,
            )
            return _empty_digest(table_name)
        return _digest(payload)


# ─── Helpers shared by both clients ───────────────────────────────


def _empty_digest(table_name: str) -> dict[str, Any]:
    """Default empty response — same shape as a successful digest, just with
    no columns. Lets the pipeline build a TableContext without crashing.
    """
    return {
        "table_name": table_name,
        "table_business_name": None,
        "table_description": None,
        "data_category": None,
        "storage_type": None,
        "load_type": None,
        "bq_project": None,
        "bq_dataset": None,
        "bq_table": None,
        "column_count": 0,
        "mdm_coverage_pct": 0.0,
        "columns": [],
    }


def _digest(payload: list | dict) -> dict[str, Any]:
    """Same digest as scripts/probe_mdm.py — kept here because HttpMDMClient
    needs it inline (no probe_mdm import dependency on the runtime side).
    """
    if not isinstance(payload, list) or not payload:
        return _empty_digest("(unknown)")

    data = payload[0]
    schema = data.get("schema", {})
    cols = schema.get("schema_attributes") or []
    dataset = data.get("dataset_details", {})
    source = data.get("dataset_source_details", {})

    columns = []
    for col in cols:
        attr = col.get("attribute_details", {}) or {}
        sens = col.get("sensitivity_details", {}) or {}
        columns.append(
            {
                "name": attr.get("attribute_name") or col.get("attribute_name"),
                "business_name": attr.get("business_name"),
                "type": attr.get("attribute_type"),
                "description": attr.get("attribute_desc"),
                "is_partitioned": attr.get("is_partitioned"),
                "is_pii": sens.get("is_pii"),
                "is_gdpr": sens.get("is_gdpr"),
            }
        )

    described = sum(1 for c in columns if c["description"])
    coverage_pct = round(described / max(len(columns), 1), 3)

    return {
        "table_name": data.get("display_name"),
        "table_business_name": dataset.get("business_name"),
        "table_description": dataset.get("data_desc"),
        "data_category": dataset.get("data_category"),
        "storage_type": data.get("storage_type"),
        "load_type": data.get("load_type"),
        "bq_project": source.get("project_id"),
        "bq_dataset": source.get("dataset_name"),
        "bq_table": source.get("table_name"),
        "column_count": len(columns),
        "mdm_coverage_pct": coverage_pct,
        "columns": columns,
    }
