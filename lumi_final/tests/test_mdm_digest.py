"""MDM digest tests — locks in the comprehensive field capture.

Synthetic payloads mirror the real shape we observed via
scripts/explore_mdm_payload.py against AmEx tables. Specifically:
  - dataset_details with 17 fields (incl. data_type='ODL', table_type='DERIVED',
    feed_type='LumiFirst', retention_period, is_sor_certified, etc.)
  - per-column sensitivity_details with is_primary, is_dedupe_key,
    pii_role_id (the cm11 grounding signal)
  - per-column attribute_details with cluster_position + partition_position
    + time_partition_type + derived_logic + attribute_format
  - ownership_details with business_contacts/tech_contacts as lists of
    {email, type} dicts + imr_queue + aim_id
  - forward-compat: undocumented keys MUST flow through *_extra catch-alls
"""

from __future__ import annotations

from lumi.mdm import _digest, _empty_digest


def _payload(**overrides) -> list:
    """Build a realistic MDM payload with sensible defaults; override
    nested keys via dotted-path style (e.g. dataset_details_extra={...}).
    """
    return [{
        "display_name": "test_table",
        "key_id": "NGBD-Test-Key",
        "host_region": "USA",
        "status": "ACTIVE",
        "version": "2.16",
        "storage_type": "BigQuery",
        "load_type": "FULL_REFRESH",
        "created_by": "API",
        "dataset_details": {
            "business_name": "Test Table",
            "data_desc": "Test description for grounding",
            "data_category": "Customer",
            "data_sub_category": "Insights",
            "data_type": "ODL",
            "table_type": "DERIVED",
            "feed_type": "LumiFirst",
            "is_internal": True,
            "is_searchable": True,
            "is_sor_certified": True,
            "is_transactional": False,
            "is_history_required": True,
            "retention_period": 3650,
            "selective_update_required": False,
            "enable_sequence_check": False,
            "dataset_id": "abc-123",
            "dataset_parent_id": "parent-456",
            **(overrides.get("dataset_details_extra") or {}),
        },
        "dataset_source_details": {
            "project_id": "axp-lumi",
            "dataset_name": "dw",
            "table_name": "test_table",
            "country": "UNITED STATES",
            "region": "GLOBAL",
            "feed_id": "wrt1",
            "base_or_view": "Base",
            **(overrides.get("source_extra") or {}),
        },
        "decommission_details": {"is_flagged": False},
        "ownership_details": {
            "aim_id": "200000310",
            "ownership_id": "owner-uuid",
            "imr_queue": "FUEL_Marketing_Data_Support",
            "app_team_SN_workgroup": "Finance_Analytics_DEV",
            "business_contacts": [
                {"email": "owner@aexp.com", "type": "business_owner_p"},
                {"type": "di_business_owner"},  # missing email
            ],
            "tech_contacts": [
                {"email": "tech1@aexp.com", "type": "tech_owner"},
                {"email": "tech2@aexp.com", "type": "di_tech_owner"},
            ],
            "application_team_contacts": [],
            "status": "ACTIVE",
            "created_by": "API",
            **(overrides.get("ownership_extra") or {}),
        },
        "schema": {
            "schema_attributes": overrides.get("columns") or [
                _column("rpt_dt", desc="Report date", attr_type="DATE",
                        is_partitioned=True, partition_position=1,
                        time_partition_type="MONTH"),
                _column("cm11", desc="Cardmember field 11", attr_type="STRING",
                        is_pii=True, pii_role_id="NGBD-SDE-CM11"),
                _column("supp_nbr", desc="Supplemental number",
                        attr_type="STRING", is_primary=True,
                        is_dedupe_key=True),
                _column("bus_seg", desc="Business segment", attr_type="STRING",
                        is_clustered=True, cluster_position=2,
                        attribute_format="^[A-Z]+$"),
            ],
        },
    }]


def _column(
    name: str, *, desc: str | None = None, attr_type: str = "STRING",
    is_partitioned: bool = False, partition_position: int | None = None,
    time_partition_type: str | None = None,
    is_clustered: bool = False, cluster_position: int | None = None,
    is_pii: bool = False, pii_role_id: str | None = None,
    is_primary: bool = False, is_dedupe_key: bool = False,
    derived_logic: str | None = None, attribute_format: str | None = None,
    extra_attribute: dict | None = None,
    extra_sensitivity: dict | None = None,
) -> dict:
    """Build one schema_attributes item with the explored field shape."""
    return {
        "attribute_id": f"id-{name}",
        "attribute_name": name,
        "is_meta_column": False,
        "external_reference_details": [],
        "attribute_details": {
            "attribute_name": name,
            "business_name": name.replace("_", " ").title(),
            "attribute_desc": desc,
            "attribute_type": attr_type,
            "attribute_format": attribute_format,
            "attribute_length": 100 if attr_type == "STRING" else 8,
            "max_length": 100 if attr_type == "STRING" else 8,
            "min_length": 0,
            "attribute_position": 1,
            "is_partitioned": is_partitioned,
            "partition_position": partition_position,
            "time_partition_type": time_partition_type,
            "is_clustered": is_clustered,
            "cluster_position": cluster_position,
            "is_mandatory": False,
            "is_normalization": True,
            "is_derived": False,
            "is_dedupe_column": False,
            "is_dedupe_key": is_dedupe_key,
            "current_col_name": name,
            "derived_logic": derived_logic,
            "sor_non_sor": "SOR",
            "target_identifier": "wrt1",
            **(extra_attribute or {}),
        },
        "sensitivity_details": {
            "is_primary": is_primary,
            "is_pii": is_pii,
            "is_gdpr": False,
            "is_dqm": False,
            "is_oncop": False,
            "is_sensitive": is_pii,
            "is_critical_data_element": False,
            "pii_role_id": pii_role_id or "",
            "oncop_role_id": "",
            "publish_code": 1,
            **(extra_sensitivity or {}),
        },
    }


# ─── Top-level identity + dataset capture ────────────────────


def test_digest_captures_identity_block():
    d = _digest(_payload())
    assert d["table_name"] == "test_table"
    assert d["display_name"] == "test_table"
    assert d["key_id"] == "NGBD-Test-Key"
    assert d["host_region"] == "USA"
    assert d["status"] == "ACTIVE"
    assert d["version"] == "2.16"
    assert d["storage_type"] == "BigQuery"
    assert d["load_type"] == "FULL_REFRESH"


def test_digest_captures_all_dataset_details():
    """Every documented dataset_details key must surface as a top-level
    field in the digest."""
    d = _digest(_payload())
    assert d["table_business_name"] == "Test Table"
    assert d["table_description"] == "Test description for grounding"
    assert d["data_category"] == "Customer"
    assert d["data_sub_category"] == "Insights"
    assert d["data_type"] == "ODL"
    assert d["table_type"] == "DERIVED"
    assert d["feed_type"] == "LumiFirst"
    assert d["is_internal"] is True
    assert d["is_searchable"] is True
    assert d["is_sor_certified"] is True
    assert d["is_transactional"] is False
    assert d["is_history_required"] is True
    assert d["retention_period"] == 3650
    assert d["selective_update_required"] is False
    assert d["enable_sequence_check"] is False
    assert d["dataset_id"] == "abc-123"
    assert d["dataset_parent_id"] == "parent-456"


def test_digest_captures_dataset_source_details():
    d = _digest(_payload())
    assert d["bq_project"] == "axp-lumi"
    assert d["bq_dataset"] == "dw"
    assert d["bq_table"] == "test_table"
    assert d["country"] == "UNITED STATES"
    assert d["region"] == "GLOBAL"
    assert d["feed_id"] == "wrt1"
    assert d["base_or_view"] == "Base"


def test_dataset_extras_catch_all_undocumented_keys():
    """When MDM adds a new dataset_details key we haven't listed, it
    must still flow through via mdm_dataset_extra. Forward-compat
    without code changes."""
    d = _digest(_payload(
        dataset_details_extra={"new_field_2027": "foo", "another_new": 42}
    ))
    assert d["mdm_dataset_extra"]["new_field_2027"] == "foo"
    assert d["mdm_dataset_extra"]["another_new"] == 42


def test_source_extras_catch_all_undocumented_keys():
    d = _digest(_payload(source_extra={"new_source_field": "bar"}))
    assert d["mdm_source_extra"]["new_source_field"] == "bar"


# ─── Ownership ───────────────────────────────────────────────


def test_digest_captures_ownership_with_contacts():
    d = _digest(_payload())
    own = d["ownership"]
    assert own["aim_id"] == "200000310"
    assert own["imr_queue"] == "FUEL_Marketing_Data_Support"
    assert own["app_team_sn_workgroup"] == "Finance_Analytics_DEV"
    assert own["status"] == "ACTIVE"
    assert len(own["business_contacts"]) == 2
    assert own["business_contacts"][0]["email"] == "owner@aexp.com"
    assert own["business_contacts"][0]["type"] == "business_owner_p"
    # Contact missing email — kept, just no email key
    assert "email" not in own["business_contacts"][1]
    assert own["business_contacts"][1]["type"] == "di_business_owner"
    assert len(own["tech_contacts"]) == 2


# ─── Per-column field capture ────────────────────────────────


def test_digest_captures_full_column_shape():
    d = _digest(_payload())
    cm11 = next(c for c in d["columns"] if c["name"] == "cm11")
    assert cm11["business_name"] == "Cm11"
    assert cm11["description"] == "Cardmember field 11"
    assert cm11["type"] == "STRING"
    assert cm11["is_pii"] is True
    assert cm11["pii_role_id"] == "NGBD-SDE-CM11"  # the cm11 grounding signal!
    assert cm11["is_primary"] is False
    assert cm11["is_dedupe_key"] is False


def test_digest_captures_primary_key_signal():
    """is_primary=true on a column → top-tier PK signal exposed for the
    PK ranking heuristic. This is the unicorn signal we look for first."""
    d = _digest(_payload())
    pk = next(c for c in d["columns"] if c["name"] == "supp_nbr")
    assert pk["is_primary"] is True
    assert pk["is_dedupe_key"] is True


def test_digest_captures_partition_strategy():
    d = _digest(_payload())
    rpt_dt = next(c for c in d["columns"] if c["name"] == "rpt_dt")
    assert rpt_dt["is_partitioned"] is True
    assert rpt_dt["partition_position"] == 1
    assert rpt_dt["time_partition_type"] == "MONTH"


def test_digest_captures_clustering():
    d = _digest(_payload())
    bus_seg = next(c for c in d["columns"] if c["name"] == "bus_seg")
    assert bus_seg["is_clustered"] is True
    assert bus_seg["cluster_position"] == 2
    assert bus_seg["format"] == "^[A-Z]+$"


def test_pii_role_id_empty_string_normalized_to_none():
    """MDM puts "" as a placeholder for "no role". After normalization
    that should be None so downstream "if pii_role_id" predicates work."""
    d = _digest(_payload())
    rpt_dt = next(c for c in d["columns"] if c["name"] == "rpt_dt")
    assert rpt_dt["pii_role_id"] is None  # was ""


def test_attribute_details_extras_catch_all():
    """Undocumented per-column attribute keys flow through extras."""
    payload = _payload(columns=[
        _column("foo", extra_attribute={"new_attr_2027": "test"}),
    ])
    d = _digest(payload)
    foo = d["columns"][0]
    assert foo["attribute_details_extra"]["new_attr_2027"] == "test"


def test_sensitivity_details_extras_catch_all():
    payload = _payload(columns=[
        _column("bar", extra_sensitivity={"new_sens_2027": True}),
    ])
    d = _digest(payload)
    bar = d["columns"][0]
    assert bar["sensitivity_details_extra"]["new_sens_2027"] is True


def test_coverage_pct_computed_correctly():
    """3 of 4 cols described → 75%. (cm11 has a desc; supp_nbr has;
    bus_seg has; rpt_dt has — actually all 4 in our default fixture
    have desc, so coverage = 100%.)"""
    d = _digest(_payload())
    assert d["mdm_coverage_pct"] == 1.0  # all 4 fixture cols described

    # Now drop one description.
    d2 = _digest(_payload(columns=[
        _column("a", desc="described"),
        _column("b", desc=None),  # missing
        _column("c", desc="also described"),
    ]))
    assert d2["mdm_coverage_pct"] == round(2 / 3, 3)


# ─── Empty / malformed payload handling ──────────────────────


def test_empty_payload_returns_empty_digest():
    d = _digest([])
    assert d["table_name"] == "(unknown)"
    assert d["columns"] == []
    assert d["mdm_coverage_pct"] == 0.0
    # Even on empty, the structured ownership shape exists.
    assert "ownership" in d
    assert d["ownership"]["business_contacts"] == []


def test_non_list_payload_returns_empty_digest():
    d = _digest({"not_a_list": True})
    assert d["columns"] == []


def test_empty_digest_has_all_fields():
    """The empty-digest shape MUST include every field a populated digest
    would have, so TableContext construction never fails on a missing key."""
    e = _empty_digest("xyz")
    populated_keys = {
        "table_name", "display_name", "key_id", "host_region", "status",
        "version", "storage_type", "load_type",
        "table_business_name", "table_description", "data_category",
        "data_sub_category", "data_type", "table_type", "feed_type",
        "is_internal", "retention_period", "mdm_dataset_extra",
        "bq_project", "bq_dataset", "bq_table", "country", "region",
        "feed_id", "base_or_view", "mdm_source_extra",
        "is_decommissioned", "ownership", "column_count",
        "mdm_coverage_pct", "columns",
    }
    missing = populated_keys - set(e.keys())
    assert not missing, f"empty_digest missing fields: {sorted(missing)}"
