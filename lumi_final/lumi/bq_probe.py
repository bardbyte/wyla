"""BigQuery DISTINCT-value probe (env-gated).

When ``LUMI_BQ_ENABLE=1``, this module probes ``SELECT DISTINCT col``
against BigQuery for low-cardinality columns and populates the Radix-
shaped filter catalog's ``values`` lists. This unlocks Radix's pass-1
exact-match cascade for a wider set of filters than corpus-observed
WHERE-clause literals alone.

Safe to import without BQ dependencies — only fires when:
  1. ``LUMI_BQ_ENABLE=1`` is set
  2. ``google-cloud-bigquery`` is importable
  3. The configured service-account JSON exists

Otherwise the probe is a no-op and the existing corpus-derived values
remain. This keeps tests deterministic and prevents accidental BQ spend.

Public API:
    probe_distinct_values(catalog, *, contexts, dataset, project,
                          max_columns=20, sample_limit=50) -> dict
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("lumi.bq_probe")


def is_enabled() -> bool:
    """Return True iff env says we should probe BQ."""
    return os.environ.get("LUMI_BQ_ENABLE", "").lower() in {"1", "true", "yes"}


def probe_distinct_values(
    catalog: dict[str, dict[str, Any]],
    *,
    contexts: dict[str, Any] | None = None,
    project: str | None = None,
    dataset: str = "dw",
    max_columns: int = 20,
    sample_limit: int = 50,
    bq_client: Any = None,
) -> dict[str, dict[str, Any]]:
    """Populate ``catalog[key]["values"]`` from BigQuery DISTINCT samples.

    Mutates ``catalog`` in place AND returns it (convenience).

    Selection: only probes columns that are
      (a) string type
      (b) not partition columns (don't enumerate dates)
      (c) currently have empty or very small ``values`` lists
      (d) within ``max_columns`` budget

    Errors are absorbed: any column that fails to probe (auth, missing
    table, query timeout) is left with its existing values. The probe
    NEVER raises out — running with a misconfigured BQ env should
    degrade gracefully.

    Args:
        catalog: the Radix-shaped filter catalog from filter_catalog.build_*.
        contexts: optional table contexts (unused now but accepted for API
                  parity with build_filter_catalog).
        project: BQ project. Defaults to ``LUMI_BQ_BILLING_PROJECT`` env or
                 ``LumiConfig().bq_project``.
        dataset: BQ dataset name (default "dw" matches LumiConfig).
        max_columns: maximum DISTINCT probes per run.
        sample_limit: rows requested per DISTINCT.
        bq_client: optional pre-built client (tests inject mocks here).
    """
    if not is_enabled() and bq_client is None:
        logger.debug("LUMI_BQ_ENABLE not set — skipping BQ probe")
        return catalog

    client = bq_client or _build_client(project=project)
    if client is None:
        return catalog

    candidates = _select_columns_to_probe(catalog, max_columns=max_columns)
    logger.info(
        "BQ probe: %d columns selected (out of %d in catalog)",
        len(candidates), len(catalog),
    )

    for key, entry in candidates:
        table_name = entry.get("namespace") or key.split(".")[0]
        column = key.split(".")[-1]
        sql = (
            f"SELECT DISTINCT `{column}` AS v "
            f"FROM `{dataset}.{table_name}` "
            f"WHERE `{column}` IS NOT NULL "
            f"LIMIT {sample_limit}"
        )
        try:
            rows = list(client.query(sql).result(timeout=30))
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "BQ probe failed for %s: %s", key, type(e).__name__,
            )
            continue
        values: list[str] = []
        seen = set(entry.get("values") or [])
        for row in rows:
            v = row[0] if hasattr(row, "__getitem__") else row.v
            if v is None:
                continue
            sv = str(v).strip()
            if sv and sv not in seen and len(values) + len(seen) < sample_limit:
                values.append(sv)
                seen.add(sv)
        if values:
            entry["values"] = list(entry.get("values") or []) + values
            entry.setdefault("_probe_source", []).append("bq_distinct")
    return catalog


def _select_columns_to_probe(
    catalog: dict[str, dict[str, Any]], *, max_columns: int,
) -> list[tuple[str, dict[str, Any]]]:
    """Pick string columns with sparse values to probe."""
    candidates: list[tuple[str, dict[str, Any]]] = []
    for key, entry in catalog.items():
        if entry.get("type") != "string":
            continue
        if entry.get("partition"):
            continue  # don't enumerate date partitions
        existing = len(entry.get("values") or [])
        if existing >= 10:
            continue  # already populated enough
        candidates.append((key, entry))
    # Prefer columns that have any synonyms attached (likely
    # high-business-value enums).
    candidates.sort(
        key=lambda kv: (-len(kv[1].get("synonyms") or {}), kv[0]),
    )
    return candidates[:max_columns]


def _build_client(project: str | None = None) -> Any:
    """Construct a BigQuery client. Returns None on any failure."""
    try:
        from google.cloud import bigquery
    except ImportError:
        logger.warning(
            "google-cloud-bigquery not installed; BQ probe skipped",
        )
        return None
    bq_project = (
        project
        or os.environ.get("LUMI_BQ_BILLING_PROJECT")
        or "axp-lumi"
    )
    try:
        return bigquery.Client(project=bq_project)
    except Exception as e:  # noqa: BLE001
        logger.warning("could not construct BQ client: %s", e)
        return None
