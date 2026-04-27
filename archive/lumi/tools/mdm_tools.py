"""Query the MDM API for canonical business metadata.

- 24h disk cache (TTL configurable)
- Optional bearer-token auth via env var name in config
- Fallback: if MDM returns empty, synthesize labels from snake_case column names

No LLM. Pure HTTP + deterministic fallback.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SECS = 15


def query_mdm_api(
    endpoint: str,
    entity_name: str,
    auth_env: str | None = None,
    cache_dir: str | Path = ".mdm_cache",
    cache_ttl_hours: int = 24,
    timeout_secs: int = _DEFAULT_TIMEOUT_SECS,
) -> dict[str, Any]:
    """Fetch metadata for `entity_name` from MDM, with cache + fallback.

    Args:
        endpoint: Base URL (no trailing slash needed).
        entity_name: The MDM entity to query (mapped from view in config).
        auth_env: Name of env var holding bearer token, or None.
        cache_dir: Local cache directory.
        cache_ttl_hours: Seconds since mtime > this → re-fetch.

    Returns:
        dict with:
          status: "success" | "error"
          source: "cache" | "api" | "fallback"
          entity: entity_name
          metadata: dict (canonical_name, definition, synonyms, allowed_values, ...)
          columns: dict[str, dict] — per-column metadata
          error: str | None
    """
    cache_path = Path(cache_dir) / f"{entity_name}.json"
    cached = _read_cache(cache_path, cache_ttl_hours)
    if cached is not None:
        return {
            "status": "success",
            "source": "cache",
            "entity": entity_name,
            **cached,
            "error": None,
        }

    url = f"{endpoint.rstrip('/')}/{entity_name}"
    headers = _auth_headers(auth_env)

    try:
        resp = requests.get(url, headers=headers, timeout=timeout_secs)
    except requests.RequestException as e:
        logger.warning("MDM request failed for %s: %s — using fallback", entity_name, e)
        return _fallback(entity_name, reason=str(e))

    if resp.status_code == 404:
        logger.info("MDM has no entry for %s — using fallback", entity_name)
        return _fallback(entity_name, reason="404")

    if not resp.ok:
        return {
            "status": "error",
            "source": "api",
            "entity": entity_name,
            "metadata": {},
            "columns": {},
            "error": f"HTTP {resp.status_code}: {resp.text[:200]}",
        }

    try:
        data = resp.json()
    except ValueError as e:
        return {
            "status": "error",
            "source": "api",
            "entity": entity_name,
            "metadata": {},
            "columns": {},
            "error": f"MDM returned non-JSON: {e}",
        }

    payload = _normalize(entity_name, data)
    _write_cache(cache_path, payload)
    return {
        "status": "success",
        "source": "api",
        "entity": entity_name,
        **payload,
        "error": None,
    }


def _normalize(entity_name: str, raw: Any) -> dict[str, Any]:
    """Coerce the MDM response into {metadata, columns} regardless of shape."""
    if not isinstance(raw, dict):
        return {"metadata": {}, "columns": {}}

    columns_raw = (
        raw.get("columns")
        or raw.get("attributes")
        or raw.get("fields")
        or {}
    )

    if isinstance(columns_raw, list):
        columns = {c.get("name"): c for c in columns_raw if isinstance(c, dict) and c.get("name")}
    elif isinstance(columns_raw, dict):
        columns = columns_raw
    else:
        columns = {}

    metadata = {
        "canonical_name": raw.get("canonical_name") or raw.get("display_name") or entity_name,
        "definition": raw.get("definition") or raw.get("description") or "",
        "synonyms": raw.get("synonyms") or raw.get("aliases") or [],
        "entity_type": raw.get("entity_type"),
        "relationships": raw.get("relationships") or [],
        "raw": raw,
    }
    return {"metadata": metadata, "columns": columns}


def _fallback(entity_name: str, reason: str) -> dict[str, Any]:
    return {
        "status": "success",
        "source": "fallback",
        "entity": entity_name,
        "metadata": {
            "canonical_name": _snake_to_title(entity_name),
            "definition": "",
            "synonyms": [],
            "entity_type": None,
            "relationships": [],
            "fallback_reason": reason,
        },
        "columns": {},
        "error": None,
    }


def _snake_to_title(name: str) -> str:
    parts = [p for p in name.replace("-", "_").split("_") if p]
    return " ".join(w.capitalize() for w in parts)


def column_label_fallback(column_name: str) -> str:
    """Public helper — used by ViewEnricher and others to get a clean label
    when MDM has no entry for a column.
    """
    return _snake_to_title(column_name)


def _auth_headers(auth_env: str | None) -> dict[str, str]:
    if not auth_env:
        return {"Accept": "application/json"}
    token = os.environ.get(auth_env, "").strip()
    if not token:
        logger.warning("Env var %s is not set; sending unauthenticated.", auth_env)
        return {"Accept": "application/json"}
    return {"Accept": "application/json", "Authorization": f"Bearer {token}"}


def _read_cache(path: Path, ttl_hours: int) -> dict[str, Any] | None:
    if not path.exists():
        return None
    age_secs = time.time() - path.stat().st_mtime
    if age_secs > ttl_hours * 3600:
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        logger.warning("Cache read failed for %s: %s", path, e)
        return None
    return data if isinstance(data, dict) else None


def _write_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(payload, default=str), encoding="utf-8")
    except OSError as e:
        logger.warning("Cache write failed for %s: %s", path, e)
