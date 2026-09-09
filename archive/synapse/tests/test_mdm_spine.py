"""MDM is the unclobberable spine — the governance-correctness contract.

The PII "0 PII" bug was one instance of a class: a later witness (BQ, lumi)
downgrading or misreading MDM's authoritative governance facts. These pin
the fixes so the spine holds:
  - sensitivity flags are monotonic (a later False never downgrades a True),
  - MDM "Y"/"N" strings read correctly ("N" is NOT truthy),
  - is_pii is derived from flat / nested / role-derived shapes,
  - BQ does not overwrite MDM's description,
  - the previously-dropped governance fields land on the graph.
"""

from __future__ import annotations

import json

from synapse.graph.builder import (
    _flag, _mdm_governance, build_graph_from_sources)
from synapse.graph.inspector import inspect_table
from synapse.graph.store import GraphStore, canonical_uri


# ─── _flag: the "Y"/"N" string trap ──────────────────────────


def test_flag_reads_yn_strings_not_naive_bool():
    assert _flag("Y") is True and _flag("y") is True
    assert _flag("N") is False and _flag("no") is False   # bool("N") would be True!
    assert _flag("true") is True and _flag("false") is False
    assert _flag("1") is True and _flag("0") is False
    assert _flag(True) is True and _flag(False) is False
    assert _flag(None) is False and _flag("") is False


# ─── _mdm_governance: PII from every shape ───────────────────


def test_governance_pii_from_yn_string():
    g = _mdm_governance({"name": "cm11", "is_pii": "Y",
                         "pii_role_id": "Sensitive>Identifier>MemberID"})
    assert g["is_pii"] is True
    assert g["pii_taxonomy"] == "Sensitive>Identifier>MemberID"


def test_governance_non_pii_stays_false():
    g = _mdm_governance({"name": "bus_seg", "is_pii": "N",
                         "pii_role_id": "Internal"})
    assert g["is_pii"] is False and g["is_sensitive"] is False


def test_governance_role_derived_pii():
    # a Sensitive>* taxonomy implies PII even with no explicit flag
    g = _mdm_governance({"name": "billed_business",
                         "pii_role_id": "Sensitive>FinancialAmount"})
    assert g["is_pii"] is True and g["is_sensitive"] is True


def test_governance_nested_array_shape():
    g = _mdm_governance({"name": "fico_score", "sensitivity_details": [
        {"attribute_name": "fico_score", "is_pii": "Y",
         "pii_role_id": "Restricted", "is_gdpr": "Y"}]})
    assert g["is_pii"] is True and g["is_gdpr"] is True


# ─── the store: sensitivity is monotonic (the clobber fix) ───


def test_sensitivity_flag_is_sticky_true():
    store = GraphStore()
    uri = canonical_uri("column", "risk_pers_acct", "fico_score")
    # MDM asserts PII
    store.upsert_node("Column", uri,
                      {"table_name": "risk_pers_acct", "is_pii": True},
                      source="mdm")
    # a later witness (e.g. a profile pass) writes a default False
    store.upsert_node("Column", uri, {"is_pii": False}, source="bq")
    # MDM's True must survive — under-flagging PII is a governance breach
    assert store.get(uri).properties["is_pii"] is True


def test_non_sensitivity_flag_can_still_be_updated():
    store = GraphStore()
    uri = canonical_uri("column", "t", "c")
    store.upsert_node("Column", uri, {"is_nullable": True}, source="mdm")
    # is_nullable is BQ-owned and must be correctable True -> False
    store.upsert_node("Column", uri, {"is_nullable": False}, source="bq")
    assert store.get(uri).properties["is_nullable"] is False


# ─── lifecycle + recertification trust signals reach the graph ───


def test_mdm_trust_signals_land_and_surface(tmp_path):
    """recertification, lifecycle version, and the breaking-change/purge
    flags — the defensibility signals — must ride on the table node and be
    visible to the agent through inspect_table's governance block."""
    mdm = tmp_path / "mdm_cache"
    mdm.mkdir(parents=True)
    (mdm / "risk_pers_acct.json").write_text(json.dumps({
        "table_name": "risk_pers_acct",
        "table_description": "Account-level risk.",
        "columns": [{"name": "acct_id", "type": "STRING"}],
        "dataset_id": "ds-1", "ownership_id": "own-1",
        "ownership": {"recertification_date": "2026-03-01",
                      "status": "CERTIFIED"},
        "pipeline": {"pipeline_name": "risk_load", "pipeline_type": "BATCH"},
        "lifecycle": {"status": "COMPLETED", "lifecycle_version": "3",
                      "region": "US", "updated_date": "2026-06-30",
                      "is_breaking_change": "Y", "is_purge": "N"},
    }), encoding="utf-8")

    store = build_graph_from_sources(tmp_path)
    p = store.get(canonical_uri("table", "risk_pers_acct")).properties
    assert p["recertification_date"] == "2026-03-01"
    assert p["ownership_status"] == "CERTIFIED"
    assert p["lifecycle_version"] == "3"
    assert p["pipeline_type"] == "BATCH"
    assert p["is_breaking_change"] is True      # "Y" read correctly
    assert p["is_purge"] is False               # "N" is NOT truthy
    assert p["dataset_id"] == "ds-1"

    gov = inspect_table(store, "risk_pers_acct")["governance"]
    assert gov["recertification_date"] == "2026-03-01"
    assert gov["is_breaking_change"] is True
    assert gov["lifecycle_version"] == "3"
    assert gov["pipeline_type"] == "BATCH"
