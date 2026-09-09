"""Graph-derived starter questions — the capability tour, from THIS
graph's actual contents; honest-empty without a snapshot."""

from apps.console.backend.data import ConsoleData


def _world(tmp_path):
    from synapse.graph.store import GraphStore, canonical_uri
    store = GraphStore()
    for name in ("accounts", "txns"):
        t = canonical_uri("table", name)
        for src in ("mdm", "bq", "corpus"):
            store.upsert_node("Table", t, {
                "table_name": name,
                "business_owner": "New Accounts"}, source=src)
        c = canonical_uri("column", name, "acct_id")
        store.upsert_node("Column", c,
                          {"table_name": name, "name": "acct_id"},
                          source="bq")
        store.upsert_edge("CONTAINS", t, c, {}, source="bq")
    store.upsert_edge("EQUIVALENT_TO",
                      canonical_uri("column", "accounts", "acct_id"),
                      canonical_uri("column", "txns", "acct_id"),
                      {}, source="corpus")
    m = canonical_uri("metric", "approval_rate")
    store.upsert_node("Metric", m, {
        "name": "Approval rate", "sourced_from_table": "accounts",
        "formula_sql": "share of approved"}, source="metric_catalog")
    d = canonical_uri("dqrule", "accounts", "acct_id", "not_null")
    store.upsert_node("DataQualityRule", d,
                      {"target_table": "accounts"}, source="dq_engine")
    p = canonical_uri("column", "accounts", "cm11_encrypted")
    store.upsert_node("Column", p, {
        "table_name": "accounts", "name": "cm11_encrypted",
        "is_pii": True}, source="mdm")
    snap = tmp_path / "s.json"
    store.save_json(snap)
    return ConsoleData(snapshot_path=snap)


def test_starters_cover_the_capabilities(tmp_path):
    starters = _world(tmp_path).starter_questions()["starters"]
    cats = {s["category"] for s in starters}
    assert {"Live analysis", "Meaning", "Ownership & lineage",
            "Join paths", "Trust", "Data quality", "Governance",
            "Teach it"} <= cats
    by_cat = {s["category"]: s for s in starters}
    # every question names REAL graph objects
    assert "Approval rate" in by_cat["Live analysis"]["question"]
    assert "accounts" in by_cat["Join paths"]["question"]
    assert "txns" in by_cat["Join paths"]["question"]
    assert "cm11_encrypted" in by_cat["Governance"]["question"]
    # the capture starter prefills instead of sending
    assert by_cat["Teach it"]["prefill"] is True
    assert all(s["why"] for s in starters)


def test_starters_honest_when_empty():
    empty = ConsoleData(snapshot_path="/none/x.json").starter_questions()
    assert empty["starters"] == [] and empty["source"] == "empty"
