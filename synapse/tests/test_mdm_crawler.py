"""MDM crawler — full read-side pull, offline from fixture cache."""

from __future__ import annotations

import json
from pathlib import Path

from synapse.graph.builder import build_graph_from_sources
from synapse.graph.store import canonical_uri, normalize_table_name
from synapse.loaders.mdm_crawler import MdmCrawler, crawl_mdm_for_table
from synapse.loaders.skills_loader import load_skills_library
from synapse.mcp.service import GraphService

MDM_FIXTURES = Path(__file__).parent / "fixtures" / "mdm"
SKILLS_FIXTURES = Path(__file__).parent / "fixtures" / "skills_library"


# ─── identity normalization (the spine precondition) ─────────


def test_normalize_table_name_collapses_qualifiers():
    assert normalize_table_name("common.roll_rate_calc") == "roll_rate_calc"
    assert normalize_table_name("`axp-lumi`.dw.risk_pers_acct") == "risk_pers_acct"
    assert normalize_table_name("ROLL_RATE_CALC") == "roll_rate_calc"
    # qualified and bare forms mint the SAME node URI
    assert canonical_uri("table", "common.roll_rate_calc") == \
        canonical_uri("table", "roll_rate_calc")
    assert canonical_uri("column", "common.roll_rate_calc", "cm11_encrypted") \
        == canonical_uri("column", "roll_rate_calc", "cm11_encrypted")


# ─── the crawl, fully offline ────────────────────────────────


def test_offline_crawl_replays_all_steps_from_cache(tmp_path):
    result = crawl_mdm_for_table(
        "roll_rate_calc", out_dir=tmp_path,
        base_url="", cache_dir=MDM_FIXTURES,
    )
    assert result.status == "ok", result.warnings
    report = result.metadata["fetch_report"]
    steps = {k: v for k, v in report.items() if k != "schema_variant"}
    assert set(steps.values()) == {"cached"}           # every step from cache
    assert report["schema_variant"] in ("filter", "plain")

    blob = json.loads(
        (tmp_path / "mdm_cache" / "roll_rate_calc.json").read_text())
    # spine + org domain
    assert blob["dataset_parent_id"] == "dsp-9001"
    assert blob["business_unit"] == "Risk"             # authoritative, from ownership
    assert blob["ownership"]["recertification_date"] == "2026-03-01"
    # pipeline governance block
    assert blob["pipeline"]["pipeline_name"] == "rollrate_monthly_load"
    assert blob["pipeline"]["governance"]["source_system"] == "TRIUMPH"
    # lineage both directions, self excluded
    assert blob["lineage_upstream"] == ["raw_acct_balances", "acct_status_daily"]
    assert blob["lineage_downstream"] == ["us_daily_rr_smry_data"]
    # lifecycle
    assert blob["lifecycle"]["status"] == "COMPLETED"
    # view detection + templated project blanked
    assert blob["asset_kind"] == "View"
    assert blob["bq_project"] == ""
    assert blob["partition_field"] == "rpt_month"
    # sensitivity shape B (nested) — cm11_encrypted
    cm11 = next(c for c in blob["columns"] if c["name"] == "cm11_encrypted")
    assert cm11["is_pii"] is True and cm11["pii_role_id"] == "Restricted"
    # sensitivity shape A (top-level array) — dpd_bucket
    dpd = next(c for c in blob["columns"] if c["name"] == "dpd_bucket")
    assert dpd["is_critical_data_element"] is True
    # attribute lineage artifact written
    attr = json.loads(
        (tmp_path / "attribute_lineage" / "roll_rate_calc.json").read_text())
    assert attr["mappings"][0]["dst_column"] == "bal_lag1"


def test_partial_when_optional_steps_unavailable(tmp_path):
    result = crawl_mdm_for_table(
        "sbs_new_accounts", out_dir=tmp_path,
        base_url="", cache_dir=MDM_FIXTURES,
    )
    assert result.status == "partial"                  # schema ok, rest degraded
    report = result.metadata["fetch_report"]
    assert report["schema"] == "cached"
    assert report["ownership"].startswith("skipped: no dataset_parent_id")
    assert (tmp_path / "mdm_cache" / "sbs_new_accounts.json").exists()
    # primary key still landed from schema alone
    blob = json.loads(
        (tmp_path / "mdm_cache" / "sbs_new_accounts.json").read_text())
    assert next(c for c in blob["columns"]
                if c["name"] == "na_pcn_no")["is_primary"] is True


def test_schema_failure_is_a_structured_error(tmp_path):
    result = crawl_mdm_for_table(
        "no_such_table", out_dir=tmp_path,
        base_url="", cache_dir=MDM_FIXTURES,
    )
    assert result.status == "error"
    assert "schema step failed" in (result.error or "")


def test_sensitive_endpoints_are_hard_denied(tmp_path):
    crawler = MdmCrawler("https://mdm.example", cache_dir=tmp_path)
    report: dict[str, str] = {}
    assert crawler._get("/api-polling-info?id=1", table="t",
                        step="poll", report=report) is None
    assert report["poll"].startswith("denied: sensitive")
    assert crawler._get("/keys?keyTypes=LumiKey", table="t",
                        step="keys", report=report) is None
    assert report["keys"].startswith("denied")
    assert crawler._get("/admin/nuke", table="t",
                        step="admin", report=report) is None
    assert report["admin"].startswith("denied: path not in allowlist")


# ─── graph ingestion: the spine lands, witnesses fuse ────────


def test_crawled_facts_land_and_fuse_with_skills(tmp_path):
    sources = tmp_path / "sources"
    crawl_mdm_for_table("roll_rate_calc", out_dir=sources,
                        base_url="", cache_dir=MDM_FIXTURES)
    load_skills_library(SKILLS_FIXTURES, out_dir=sources)
    store = build_graph_from_sources(sources)

    # ONE node for the table, despite skills saying common.roll_rate_calc
    node = store.get(canonical_uri("table", "common.roll_rate_calc"))
    assert node is not None
    # MDM (the data authority) grounds the table; skills are OUT of the graph
    # entirely — no skill-derived nodes or edges.
    assert "mdm" in node.provenance.sources
    assert "skills" not in node.provenance.sources
    assert "Skill" not in store.stats()["nodes_by_type"]

    # spine + org-domain facts
    assert node.properties["company_domain"] == "Risk"
    assert node.properties["data_domain"] == "Portfolio Risk"
    assert node.properties["dataset_parent_id"] == "dsp-9001"
    assert node.properties["lifecycle_status"] == "COMPLETED"
    assert node.properties["pipeline_name"] == "rollrate_monthly_load"

    # table lineage, both directions
    from synapse.mcp.skills_registry import SkillsRegistry
    svc = GraphService(store,
                       skills=SkillsRegistry.from_dir(sources / "skills"))
    lineage = svc.get_lineage("roll_rate_calc")["data"]
    up = {n["table"] for n in lineage["upstream"]}
    down = {n["table"] for n in lineage["downstream"]}
    assert up == {"raw_acct_balances", "acct_status_daily"}
    assert down == {"us_daily_rr_smry_data"}

    # attribute lineage → DERIVES_FROM with the logic attached
    derives = store.outgoing(
        canonical_uri("column", "roll_rate_calc", "bal_lag1"), "DERIVES_FROM")
    assert len(derives) == 1
    assert derives[0].to_uri == canonical_uri(
        "column", "raw_acct_balances", "bal")
    assert "LAG(bal)" in derives[0].properties["derivation_logic"]

    # guardrails still fire against the fused node (column URI collapsed)
    check = svc.validate_sql_plan(
        "SELECT cm11_encrypted FROM common.roll_rate_calc")
    assert any("cm11_encrypted" in v["reason"]
               for v in check["data"]["violations"])


# ─── real-deployment fallbacks (probe paste-back, 2026-07-06) ─────


def test_appflow_fallback_route_reaches_pipeline(tmp_path):
    """cdm-storage 500s on the real MDM; /datasets/{dpid}/appflow works.
    The crawler must still reach the pipeline + business_unit."""
    result = crawl_mdm_for_table(
        "fallback_case", out_dir=tmp_path,
        base_url="", cache_dir=MDM_FIXTURES,
    )
    assert result.status == "partial"     # lineage/lifecycle degraded
    report = result.metadata["fetch_report"]
    assert report["appflow_by_parent"] == "cached"
    assert report["pipeline"] == "cached"
    assert report["schema_variant"] in ("filter", "plain")
    blob = json.loads(
        (tmp_path / "mdm_cache" / "fallback_case.json").read_text())
    assert blob["pipeline"]["pipeline_name"] == "fallback_load"
    assert blob["business_unit"] == "Fraud"     # via pipeline governance


def test_ownership_business_unit_found_when_nested():
    from synapse.loaders.mdm_digest import merge_ownership
    blob = {"ownership": {}, "business_unit": ""}
    merge_ownership(blob, {
        "aim_id": "AIM-9",
        "ownership_details_extra": {"business_unit": "Marketing"},
    })
    assert blob["business_unit"] == "Marketing"


def test_bare_string_appflow_ids_are_tolerated():
    from synapse.loaders.mdm_crawler import _appflow_parent_from
    assert _appflow_parent_from(["af-123", "af-456"]) == "af-123"
    assert _appflow_parent_from([{"parent_app_flow_id": "af-9"}]) == "af-9"
    assert _appflow_parent_from([]) is None
    assert _appflow_parent_from(None) is None
