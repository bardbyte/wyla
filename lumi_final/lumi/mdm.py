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


# Keys we extract explicitly from each MDM section. Anything in the section
# NOT in this list goes into the matching `*_extra` catch-all so future MDM
# additions surface automatically without code changes.
_DATASET_DETAILS_EXPLICIT = frozenset({
    "business_name", "data_desc", "data_category", "data_sub_category",
    "data_type", "table_type", "feed_type", "is_internal", "is_searchable",
    "is_sor_certified", "is_transactional", "is_history_required",
    "retention_period", "selective_update_required", "enable_sequence_check",
    "dataset_id", "dataset_parent_id",
})
_SOURCE_DETAILS_EXPLICIT = frozenset({
    "project_id", "dataset_name", "table_name",
    "country", "region", "feed_id", "base_or_view",
})
_OWNERSHIP_EXPLICIT = frozenset({
    "aim_id", "ownership_id", "imr_queue", "app_team_SN_workgroup",
    "business_contacts", "tech_contacts", "application_team_contacts",
    "status", "use_case_distribution_group_email",
    "business_details_updated_date", "business_recertification_date",
    "tech_details_updated_date", "tech_recertification_date",
    "external_reference_details", "impact_communication_owner_contacts",
    "business_unit", "created_by", "created_date", "updated_by", "updated_date",
})
_ATTRIBUTE_DETAILS_EXPLICIT = frozenset({
    "attribute_name", "business_name", "attribute_desc", "attribute_type",
    "attribute_format", "attribute_length", "max_length", "min_length",
    "attribute_position", "attribute_parse_expression",
    "is_partitioned", "partition_position", "time_partition_type",
    "is_clustered", "cluster_position",
    "is_mandatory", "is_normalization", "is_derived",
    "is_dedupe_column", "is_dedupe_key",
    "current_col_name", "derived_logic",
    "sor_non_sor", "target_identifier",
})
_SENSITIVITY_EXPLICIT = frozenset({
    "is_primary", "is_pii", "is_gdpr", "is_dqm", "is_oncop", "is_sensitive",
    "is_critical_data_element",
    "pii_role_id", "oncop_role_id", "publish_code",
})


def _split_known_extra(d: dict | None, known: frozenset[str]) -> tuple[dict, dict]:
    """Split a dict into (explicit, extras) — keeps every value MDM gives us
    without losing forward-compat for fields we haven't documented."""
    if not d:
        return {}, {}
    explicit = {k: v for k, v in d.items() if k in known}
    extras = {k: v for k, v in d.items() if k not in known}
    return explicit, extras


def _empty_digest(table_name: str) -> dict[str, Any]:
    """Default empty response — same shape as a successful digest, just with
    no columns. Lets the pipeline build a TableContext without crashing.
    """
    return {
        # Identity
        "table_name": table_name,
        "display_name": None,
        "key_id": None,
        "host_region": None,
        "status": None,
        "version": None,
        "storage_type": None,
        "load_type": None,
        # dataset_details (table-level, 17+ keys)
        "table_business_name": None,
        "table_description": None,
        "data_category": None,
        "data_sub_category": None,
        "data_type": None,
        "table_type": None,
        "feed_type": None,
        "is_internal": None,
        "is_searchable": None,
        "is_sor_certified": None,
        "is_transactional": None,
        "is_history_required": None,
        "retention_period": None,
        "selective_update_required": None,
        "enable_sequence_check": None,
        "dataset_id": None,
        "dataset_parent_id": None,
        "mdm_dataset_extra": {},
        # dataset_source_details (BQ location)
        "bq_project": None,
        "bq_dataset": None,
        "bq_table": None,
        "country": None,
        "region": None,
        "feed_id": None,
        "base_or_view": None,
        "mdm_source_extra": {},
        # decommission_details
        "is_decommissioned": False,
        "mdm_decommission_extra": {},
        # ownership_details (rich — business_contacts + tech_contacts with email/type)
        "ownership": {
            "aim_id": None,
            "ownership_id": None,
            "imr_queue": None,
            "app_team_sn_workgroup": None,
            "business_contacts": [],
            "tech_contacts": [],
            "application_team_contacts": [],
            "status": None,
            "ownership_extra": {},
        },
        # schema (column-level)
        "column_count": 0,
        "mdm_coverage_pct": 0.0,
        "columns": [],
    }


def _digest(payload: list | dict) -> dict[str, Any]:
    """Comprehensive MDM digest — captures every documented field plus
    catch-all dicts so undocumented future fields are forward-compatible.

    The shape matches what TableContext consumes plus structured extras
    (mdm_dataset_extra, mdm_source_extra, ownership.ownership_extra) for
    keys we haven't promoted to first-class fields yet.

    Field-mapping cheat sheet (top-tier signals):
      sensitivity_details.is_primary          → GROUNDED PK signal (+5 weight)
      sensitivity_details.is_dedupe_key       → natural-key signal
      sensitivity_details.pii_role_id         → cm11-style PII grounding
      attribute_details.is_partitioned +
        partition_position + time_partition_type → always_filter precision
      attribute_details.is_clustered +
        cluster_position                      → BQ-optimal filter ordering
      attribute_details.derived_logic         → MDM-declared formula
      attribute_details.attribute_format      → value_format hint
      dataset_details.table_type              → architectural role
      dataset_details.feed_type / load_type   → freshness clause
      ownership_details.business_contacts     → view header comment
    """
    if not isinstance(payload, list) or not payload:
        return _empty_digest("(unknown)")

    data = payload[0]
    schema = data.get("schema", {}) or {}
    cols = schema.get("schema_attributes") or []
    dataset_raw = data.get("dataset_details", {}) or {}
    source_raw = data.get("dataset_source_details", {}) or {}
    decommission_raw = data.get("decommission_details", {}) or {}
    ownership_raw = data.get("ownership_details", {}) or {}

    dataset, dataset_extra = _split_known_extra(dataset_raw, _DATASET_DETAILS_EXPLICIT)
    source, source_extra = _split_known_extra(source_raw, _SOURCE_DETAILS_EXPLICIT)
    ownership_explicit, ownership_extra = _split_known_extra(
        ownership_raw, _OWNERSHIP_EXPLICIT
    )

    columns: list[dict[str, Any]] = []
    for col in cols:
        attr_raw = col.get("attribute_details", {}) or {}
        sens_raw = col.get("sensitivity_details", {}) or {}
        attr, attr_extra = _split_known_extra(attr_raw, _ATTRIBUTE_DETAILS_EXPLICIT)
        sens, sens_extra = _split_known_extra(sens_raw, _SENSITIVITY_EXPLICIT)

        col_name = (
            attr.get("attribute_name")
            or col.get("attribute_name")
            or attr_raw.get("attribute_name")
        )
        columns.append({
            # Identity
            "name": col_name,
            "attribute_id": col.get("attribute_id"),
            "current_col_name": attr.get("current_col_name"),
            # Description / display
            "business_name": attr.get("business_name"),
            "description": attr.get("attribute_desc"),
            "type": attr.get("attribute_type"),
            "format": attr.get("attribute_format"),
            "length": attr.get("attribute_length"),
            "max_length": attr.get("max_length"),
            "min_length": attr.get("min_length"),
            "position": attr.get("attribute_position"),
            # Partition / cluster (BQ physical layout signals)
            "is_partitioned": attr.get("is_partitioned"),
            "partition_position": attr.get("partition_position"),
            "time_partition_type": attr.get("time_partition_type"),
            "is_clustered": attr.get("is_clustered"),
            "cluster_position": attr.get("cluster_position"),
            # Column-role flags (drive hidden / type / mandatory inference)
            "is_mandatory": attr.get("is_mandatory"),
            "is_normalization": attr.get("is_normalization"),
            "is_derived": attr.get("is_derived"),
            "is_meta_column": col.get("is_meta_column"),
            "is_dedupe_column": attr.get("is_dedupe_column"),
            "is_dedupe_key": attr.get("is_dedupe_key"),
            "derived_logic": attr.get("derived_logic"),
            "sor_non_sor": attr.get("sor_non_sor"),
            "target_identifier": attr.get("target_identifier"),
            # Sensitivity / PK / PII (the highest-value grounding signals)
            "is_primary": sens.get("is_primary"),
            "is_pii": sens.get("is_pii"),
            "is_gdpr": sens.get("is_gdpr"),
            "is_dqm": sens.get("is_dqm"),
            "is_oncop": sens.get("is_oncop"),
            "is_sensitive": sens.get("is_sensitive"),
            "is_critical_data_element": sens.get("is_critical_data_element"),
            "pii_role_id": _clean_role_id(sens.get("pii_role_id")),
            "oncop_role_id": _clean_role_id(sens.get("oncop_role_id")),
            "publish_code": sens.get("publish_code"),
            # External references (placeholder on AmEx tables today;
            # future-proof for tables where MDM declares joins).
            "external_references": col.get("external_reference_details") or [],
            # Catch-alls so undocumented MDM keys don't get lost.
            "attribute_details_extra": attr_extra,
            "sensitivity_details_extra": sens_extra,
        })

    described = sum(1 for c in columns if c["description"])
    coverage_pct = round(described / max(len(columns), 1), 3)

    # Ownership — promote business_contacts/tech_contacts to clean shape.
    business_contacts = _normalize_contacts(
        ownership_explicit.get("business_contacts") or []
    )
    tech_contacts = _normalize_contacts(
        ownership_explicit.get("tech_contacts") or []
    )
    application_contacts = _normalize_contacts(
        ownership_explicit.get("application_team_contacts") or []
    )

    return {
        # Identity
        "table_name": data.get("display_name"),
        "display_name": data.get("display_name"),
        "key_id": data.get("key_id"),
        "host_region": data.get("host_region"),
        "status": data.get("status"),
        "version": data.get("version"),
        "storage_type": data.get("storage_type"),
        "load_type": data.get("load_type"),
        # dataset_details (explicit fields hoisted to top level)
        "table_business_name": dataset.get("business_name"),
        "table_description": dataset.get("data_desc"),
        "data_category": dataset.get("data_category"),
        "data_sub_category": dataset.get("data_sub_category"),
        "data_type": dataset.get("data_type"),
        "table_type": dataset.get("table_type"),
        "feed_type": dataset.get("feed_type"),
        "is_internal": dataset.get("is_internal"),
        "is_searchable": dataset.get("is_searchable"),
        "is_sor_certified": dataset.get("is_sor_certified"),
        "is_transactional": dataset.get("is_transactional"),
        "is_history_required": dataset.get("is_history_required"),
        "retention_period": dataset.get("retention_period"),
        "selective_update_required": dataset.get("selective_update_required"),
        "enable_sequence_check": dataset.get("enable_sequence_check"),
        "dataset_id": dataset.get("dataset_id"),
        "dataset_parent_id": dataset.get("dataset_parent_id"),
        "mdm_dataset_extra": dataset_extra,
        # dataset_source_details
        "bq_project": source.get("project_id"),
        "bq_dataset": source.get("dataset_name"),
        "bq_table": source.get("table_name"),
        "country": source.get("country"),
        "region": source.get("region"),
        "feed_id": source.get("feed_id"),
        "base_or_view": source.get("base_or_view"),
        "mdm_source_extra": source_extra,
        # decommission_details
        "is_decommissioned": bool(decommission_raw.get("is_flagged")),
        "mdm_decommission_extra": {
            k: v for k, v in decommission_raw.items() if k != "is_flagged"
        },
        # ownership_details (structured — view header comment + escalation routing)
        "ownership": {
            "aim_id": ownership_explicit.get("aim_id"),
            "ownership_id": ownership_explicit.get("ownership_id"),
            "imr_queue": ownership_explicit.get("imr_queue"),
            "app_team_sn_workgroup": ownership_explicit.get("app_team_SN_workgroup"),
            "business_contacts": business_contacts,
            "tech_contacts": tech_contacts,
            "application_team_contacts": application_contacts,
            "status": ownership_explicit.get("status"),
            "ownership_extra": ownership_extra,
        },
        # schema
        "column_count": len(columns),
        "mdm_coverage_pct": coverage_pct,
        "columns": columns,
    }


def _clean_role_id(value: Any) -> str | None:
    """Empty / whitespace-only role IDs collapse to None.

    MDM puts ``""`` as a placeholder for "no role" in many cases; treating
    it as a real value clutters our PII tagging logic downstream.
    """
    if value is None:
        return None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _normalize_contacts(raw: list[dict] | None) -> list[dict[str, str]]:
    """Normalize contact-list dicts to a stable {email, type} shape.

    MDM business_contacts / tech_contacts items have varying keys across
    tables (some have name+email+type, some have just email+type). We
    keep what's there and discard nothing — extra keys land in `extra`.
    """
    if not raw:
        return []
    out: list[dict[str, str]] = []
    canonical = {"email", "type", "name"}
    for item in raw:
        if not isinstance(item, dict):
            continue
        normalized = {k: v for k, v in item.items() if k in canonical and v}
        extras = {k: v for k, v in item.items() if k not in canonical}
        if extras:
            normalized["extra"] = extras  # type: ignore[assignment]
        if normalized:
            out.append(normalized)
    return out
