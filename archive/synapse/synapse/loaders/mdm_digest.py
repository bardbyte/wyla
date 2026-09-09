"""Unified MDM digest — one normalizer for every MDM payload we ingest.

Supersedes the minimal digest in mdm_loader (kept for compat) and folds
in what lumi_final's richer `_digest` learned about real payloads:

  * sensitivity lives in EITHER place depending on MDM version — a
    top-level ``sensitivity_details[]`` array keyed by attribute_name,
    or nested per-attribute under ``schema_attributes[].sensitivity_
    details``. We read both; the attribute-level value wins.
  * ``dataset_parent_id`` is the stable spine ID the whole read-side
    API keys on (ownership, appflow, versioning) — always captured.
  * unknown table-level keys are preserved under ``mdm_extra`` so new
    MDM sections surface without code changes.

Output is the builder-facing blob shape (same keys the synthetic
generator emits, plus the crawler-era additions), so the graph builder
reads real, synthetic, and crawled data identically.
"""

from __future__ import annotations

from typing import Any

_TRUTHY = (True, "Y", "y", "true", "True", "1", 1)

_KNOWN_DATASET_KEYS = {
    "business_name", "data_desc", "data_category", "data_sub_category",
    "feed_type", "table_type", "is_decommissioned", "row_count",
    "dataset_id", "dataset_parent_id", "load_type", "storage_type",
    "host_region", "is_searchable",
}


def _truthy(value: Any) -> bool:
    return value in _TRUTHY


def _maybe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_templated(value: Any) -> str:
    """MDM sometimes returns '{{project_id}}' templates — blank them."""
    text = str(value or "")
    return "" if "{{" in text else text


def digest_schema_response(raw: dict[str, Any], table_id: str) -> dict[str, Any]:
    """The /datasets/schemas payload → builder-facing blob."""
    if not raw:
        return {"table_name": table_id, "columns": [], "mdm_coverage_pct": 0.0}

    dataset = raw.get("dataset_details", {}) or {}
    source = raw.get("dataset_source_details", {}) or {}
    owners = raw.get("ownership_details", {}) or {}
    schema = raw.get("schema", {}) or {}

    # sensitivity, shape A: top-level array keyed by attribute_name
    top_sens = {
        s.get("attribute_name"): s
        for s in (raw.get("sensitivity_details") or [])
        if isinstance(s, dict)
    }

    cols_out: list[dict[str, Any]] = []
    partition_field: str | None = None
    described = 0

    for attr in (schema.get("schema_attributes") or []):
        if not isinstance(attr, dict):
            continue
        details = attr.get("attribute_details", {}) or {}
        name = attr.get("attribute_name") or details.get("attribute_name")
        if not name:
            continue
        # sensitivity, shape B: nested per attribute — wins over shape A
        sens = attr.get("sensitivity_details") or top_sens.get(name) or {}
        if isinstance(sens, list):  # some payloads nest a 1-elem list
            sens = sens[0] if sens and isinstance(sens[0], dict) else {}

        is_partition = (
            _truthy(details.get("is_partitioned"))
            or details.get("partition_role") in ("PARTITION", "PARTITION_KEY")
        )
        if is_partition and not partition_field:
            partition_field = name
        description = details.get("attribute_desc") or ""
        if description:
            described += 1
        cols_out.append({
            "name": name,
            "type": details.get("attribute_type") or details.get("data_type") or "",
            "description": description,
            "business_name": details.get("business_name") or "",
            "is_primary": bool(details.get("is_primary_key") or details.get("is_pk")),
            "is_dedupe_key": _truthy(details.get("is_dedupe_key")),
            "is_partitioned": is_partition,
            "cluster_position": details.get("clustering_ordinal_position"),
            "derived_logic": details.get("derived_logic"),
            "external_references": details.get("external_reference_details")
            or details.get("external_references") or [],
            "is_pii": _truthy(sens.get("is_pii")),
            "is_critical_data_element": _truthy(
                sens.get("is_critical_data_element")),
            "pii_role_id": sens.get("pii_role_id") or "Internal",
            "is_gdpr": _truthy(sens.get("is_gdpr")),
            "is_sensitive": _truthy(sens.get("is_sensitive")),
        })

    table_type = str(dataset.get("table_type") or "")
    n_cols = len(cols_out)
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
        "table_type": table_type,
        "is_decommissioned": _truthy(dataset.get("is_decommissioned"))
        or _truthy((raw.get("decommission_details") or {}).get("is_flagged")),
        "partition_field": partition_field,
        "row_count_estimate": _maybe_int(dataset.get("row_count")),
        # ── spine + versioning identity ──
        "dataset_id": raw.get("dataset_id") or dataset.get("dataset_id"),
        "dataset_parent_id": (
            raw.get("dataset_parent_id") or dataset.get("dataset_parent_id")
        ),
        "ownership_id": raw.get("ownership_id") or owners.get("ownership_id"),
        # ── ownership (basic; the ownership endpoint enriches this) ──
        "ownership": {
            "imr_queue": owners.get("imr_queue"),
            "aim_id": owners.get("aim_id"),
            "business_unit": owners.get("business_unit") or "",
            "business_contacts": owners.get("business_contacts") or [],
            "tech_contacts": owners.get("tech_contacts") or [],
        },
        "business_unit": owners.get("business_unit") or "",
        "columns": cols_out,
        "mdm_coverage_pct": round(described / n_cols, 3) if n_cols else 0.0,
        "asset_kind": "View" if table_type.upper() == "VIEW" else "Table",
        "tags": [],
        # populated by the crawler's lineage steps:
        "lineage_upstream": [],
        "lineage_downstream": [],
        "mdm_extra": {
            k: v for k, v in dataset.items() if k not in _KNOWN_DATASET_KEYS
        },
    }


def merge_ownership(blob: dict[str, Any], ownership_raw: Any) -> None:
    """Fold the /datasets/{parent}/ownership payload into the blob —
    this is where the authoritative business_unit lives."""
    if isinstance(ownership_raw, list):
        ownership_raw = ownership_raw[0] if ownership_raw else {}
    if not isinstance(ownership_raw, dict):
        return
    own = blob.setdefault("ownership", {})
    for key in ("imr_queue", "aim_id", "business_unit", "workgroup",
                "app_team_sn_workgroup", "use_case_distribution_group_email",
                "recertification_date", "status"):
        value = ownership_raw.get(key)
        if value not in (None, "", []):
            own[key] = value
    for key in ("business_contacts", "tech_contacts",
                "application_team_contacts"):
        contacts = ownership_raw.get(key)
        if contacts:
            own[key] = contacts
    # business_unit is sometimes nested one level down (probe showed the
    # top level null on the real deployment) — scan child dicts before
    # giving up; the pipeline governance block remains the other source.
    if not own.get("business_unit"):
        for value in ownership_raw.values():
            if isinstance(value, dict) and value.get("business_unit"):
                own["business_unit"] = value["business_unit"]
                break
    if own.get("business_unit"):
        blob["business_unit"] = own["business_unit"]


def merge_pipeline(blob: dict[str, Any], pipeline_raw: Any) -> None:
    """Fold PipelineRequestEntity (portal-v2) — pipeline identity + the
    governance block (markets, PII flags, owners, business unit)."""
    if isinstance(pipeline_raw, list):
        pipeline_raw = pipeline_raw[0] if pipeline_raw else {}
    if not isinstance(pipeline_raw, dict):
        return
    governance = pipeline_raw.get("governance") or {}
    blob["pipeline"] = {
        "pipeline_id": pipeline_raw.get("id") or pipeline_raw.get("pipeline_id"),
        "pipeline_name": pipeline_raw.get("pipeline_name") or "",
        "pipeline_type": pipeline_raw.get("pipeline_type") or "",
        "feed_type": pipeline_raw.get("feed_type") or "",
        "business_unit": pipeline_raw.get("business_unit") or "",
        "governance": {
            k: governance.get(k) for k in (
                "markets", "regions", "source_system", "application_owner",
                "business_owner", "data_usage", "is_pii", "pii_flag",
                "third_party_flag",
            ) if governance.get(k) is not None
        },
    }
    if not blob.get("business_unit") and pipeline_raw.get("business_unit"):
        blob["business_unit"] = pipeline_raw["business_unit"]
    if not blob.get("feed_type") and pipeline_raw.get("feed_type"):
        blob["feed_type"] = pipeline_raw["feed_type"]


def merge_lifecycle(blob: dict[str, Any], lifecycle_raw: Any) -> None:
    if isinstance(lifecycle_raw, list):
        lifecycle_raw = lifecycle_raw[0] if lifecycle_raw else {}
    if not isinstance(lifecycle_raw, dict):
        return
    blob["lifecycle"] = {
        k: lifecycle_raw.get(k) for k in (
            "status", "md_registration_activity", "lifecycle_version",
            "region", "is_purge", "is_breaking_change", "updated_date",
        ) if lifecycle_raw.get(k) is not None
    }
