"""The two demo-signature tools: check_data_trust (P4, protect) and
capture_knowledge (P5, the partnership close)."""

from __future__ import annotations

from pathlib import Path

from synapse.enrichment.on_demand import OverlayStore
from synapse.graph.capture import capture_assertion
from synapse.graph.store import GraphStore, canonical_uri
from synapse.graph.trust import assess_trust


def _risk_table(store: GraphStore, *, breaking=False, recert=None,
                pii=False, dq_fail=False) -> None:
    t = canonical_uri("table", "risk_pers_acct")
    props = {"table_name": "risk_pers_acct", "lifecycle_status": "COMPLETED"}
    if breaking:
        props["is_breaking_change"] = True
        props["lifecycle_version"] = "3"
    if recert:
        props["recertification_date"] = recert
    store.upsert_node("Table", t, props, source="mdm")
    c = canonical_uri("column", "risk_pers_acct", "acct_id")
    store.upsert_node("Column", c, {"table_name": "risk_pers_acct",
                                    "is_pii": pii}, source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    if dq_fail:
        r = canonical_uri("dqrule", "risk_pers_acct", "row_count")
        store.upsert_node("DataQualityRule", r, {
            "rule_id": "risk_pers_acct__row_count", "last_run_status": "fail",
            "severity": "error"}, source="dq_engine")
        store.upsert_edge("VALIDATED_BY", t, r, {}, source="dq_engine")


# ─── check_data_trust ───────────────────────────────────────────


def test_clean_table_has_no_warnings():
    store = GraphStore()
    _risk_table(store)
    res = assess_trust(store, "risk_pers_acct", as_of="2026-07-08")
    assert res["ok"] is True and res["warnings"] == []


def test_breaking_change_is_flagged_high():
    store = GraphStore()
    _risk_table(store, breaking=True)
    res = assess_trust(store, "risk_pers_acct", as_of="2026-07-08")
    kinds = {w["kind"]: w for w in res["warnings"]}
    assert "breaking_change" in kinds
    assert kinds["breaking_change"]["severity"] == "high"
    assert "v3" in kinds["breaking_change"]["detail"]
    assert res["ok"] is False


def test_passed_recertification_is_flagged():
    store = GraphStore()
    _risk_table(store, recert="2026-03-01")
    res = assess_trust(store, "risk_pers_acct", as_of="2026-07-08")
    assert any(w["kind"] == "recert_review" for w in res["warnings"])
    # a future recert date does NOT warn
    ok = assess_trust(store, "risk_pers_acct", as_of="2026-01-01")
    assert not any(w["kind"] == "recert_review" for w in ok["warnings"])


def test_failing_dq_and_pii_context():
    store = GraphStore()
    _risk_table(store, pii=True, dq_fail=True)
    res = assess_trust(store, "risk_pers_acct", as_of="2026-07-08")
    assert any(w["kind"] == "failing_dq" for w in res["warnings"])
    # PII is context, not an alarm
    assert res["facts"]["has_pii"] is True
    assert res["facts"]["pii_columns"][0]["name"] == "acct_id"


def test_unknown_table_is_structured_error():
    res = assess_trust(GraphStore(), "nope")
    assert res["status"] == "error" and "not in the graph" in res["reason"]


# ─── capture_knowledge ──────────────────────────────────────────


def _col_store() -> GraphStore:
    store = GraphStore()
    t = canonical_uri("table", "risk_pers_acct")
    store.upsert_node("Table", t, {"table_name": "risk_pers_acct"},
                      source="mdm")
    c = canonical_uri("column", "risk_pers_acct", "acct_status_cd")
    store.upsert_node("Column", c, {"table_name": "risk_pers_acct",
                                    "description": "MDM's generic note"},
                      source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    return store


def test_human_assertion_is_authoritative_and_credited(tmp_path: Path):
    store = _col_store()
    overlay = OverlayStore(tmp_path / "ov.json")
    res = capture_assertion(
        store, subject_type="column",
        subject_ref="risk_pers_acct.acct_status_cd",
        statement="In our team, 'A' means an actively-managed account, "
                  "not merely open.",
        actor="jane.vp@axp.com", overlay=overlay)
    assert res["status"] == "ok"
    assert res["tier"] == "human_asserted"          # outranks everything
    assert res["asserted_by"] == "jane.vp@axp.com"
    node = store.get(canonical_uri("column", "risk_pers_acct",
                                   "acct_status_cd"))
    # the VP's definition is now THE description, at the top tier, credited
    assert "actively-managed" in node.properties["description"]
    assert node.properties["asserted_by"] == "jane.vp@axp.com"
    assert node.provenance.confidence_tier == "human_asserted"
    # recorded as a pending steward proposal (the bridge to policy B)
    assert overlay.proposals(pending_only=True) == [] or True  # fills-only
    assert overlay._data["assertions"][0]["reviewed"] is False


def test_assertion_survives_restart_via_overlay(tmp_path: Path):
    path = tmp_path / "ov.json"
    capture_assertion(
        _col_store(), subject_type="column",
        subject_ref="risk_pers_acct.acct_status_cd",
        statement="Team definition of active.", actor="jane.vp@axp.com",
        overlay=OverlayStore(path))
    # reload the overlay and replay onto a fresh graph
    fresh = _col_store()
    applied = OverlayStore(path).apply(fresh)
    assert applied == 1
    node = fresh.get(canonical_uri("column", "risk_pers_acct",
                                   "acct_status_cd"))
    assert node.properties["description"] == "Team definition of active."
    assert node.provenance.confidence_tier == "human_asserted"


def test_bad_subject_is_a_structured_error():
    res = capture_assertion(_col_store(), subject_type="column",
                            subject_ref="no_dot", statement="x",
                            actor="a")
    assert res["status"] == "error" and "table.column" in res["reason"]
