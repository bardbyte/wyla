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

from synapse.graph.builder import _flag, _mdm_governance
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
