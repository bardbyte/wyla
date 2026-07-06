"""MDM taxonomy → domain axes flow through into the graph and are queryable."""

from __future__ import annotations

import json
from pathlib import Path

from synapse.graph.builder import build_graph_from_sources
from synapse.graph.store import canonical_uri
from synapse.mcp.service import GraphService


def _write_mdm(cache: Path, table: str, category: str, sub: str) -> None:
    cache.mkdir(parents=True, exist_ok=True)
    (cache / f"{table}.json").write_text(json.dumps({
        "table_name": table,
        "table_business_name": table.replace("_", " ").title(),
        "data_category": category,
        "data_sub_category": sub,
        "bq_project": "p", "bq_dataset": "dw", "bq_table": table,
        "columns": [{"name": "id", "type": "STRING"}],
        "mdm_coverage_pct": 1.0,
    }), encoding="utf-8")


def test_mdm_category_populates_domain_axes(tmp_path: Path):
    _write_mdm(tmp_path / "mdm_cache", "sbs_new_accounts",
               "Acquisition", "New Accounts")
    store = build_graph_from_sources(tmp_path)
    node = store.get(canonical_uri("table", "sbs_new_accounts"))
    assert node is not None
    assert node.properties["data_domain"] == "Acquisition"
    assert node.properties["company_domain"] == "New Accounts"
    assert "mdm" in node.provenance.sources


def test_list_tables_for_domain_filters_on_mdm_domain(tmp_path: Path):
    mdm = tmp_path / "mdm_cache"
    _write_mdm(mdm, "sbs_new_accounts", "Acquisition", "New Accounts")
    _write_mdm(mdm, "roll_rate_calc", "Portfolio Risk", "Delinquency")
    svc = GraphService(build_graph_from_sources(tmp_path))

    hit = svc.list_tables_for_domain(data_domain="Acquisition")
    names = {t["table"] for t in hit["data"]["tables"]}
    assert names == {"sbs_new_accounts"}

    hit = svc.list_tables_for_domain(data_domain="risk")  # case-insensitive
    names = {t["table"] for t in hit["data"]["tables"]}
    assert names == {"roll_rate_calc"}


def test_inspect_table_surfaces_domain_in_identity(tmp_path: Path):
    _write_mdm(tmp_path / "mdm_cache", "roll_rate_calc",
               "Portfolio Risk", "Delinquency")
    svc = GraphService(build_graph_from_sources(tmp_path))
    res = svc.inspect_table("roll_rate_calc")
    identity = res["data"]["identity"]
    assert identity["data_domain"] == "Portfolio Risk"
    assert identity["company_domain"] == "Delinquency"


def test_table_catalog_domain_survives_when_mdm_category_empty(tmp_path: Path):
    # table_catalog (pass 1) sets domain; MDM (pass 2) with empty category
    # must NOT clobber it — empty values are skipped on merge.
    reg = tmp_path / "registries" / "raw"
    reg.mkdir(parents=True)
    (reg / "table_catalog.csv").write_text(
        "table_name,company_domain,data_domain\n"
        "roll_rate_calc,Risk,Portfolio\n", encoding="utf-8")
    _write_mdm(tmp_path / "mdm_cache", "roll_rate_calc", "", "")
    store = build_graph_from_sources(tmp_path)
    node = store.get(canonical_uri("table", "roll_rate_calc"))
    assert node.properties["data_domain"] == "Portfolio"
    assert node.properties["company_domain"] == "Risk"
