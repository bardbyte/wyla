"""The witness arithmetic (mockup 2a): every source's weight, capped
count, and contribution, plus the exact tier rule that fired."""

from apps.console.backend.data import ConsoleData


def _world(tmp_path):
    from synapse.graph.store import GraphStore, canonical_uri
    store = GraphStore()
    t = canonical_uri("table", "accounts")
    for src in ("mdm", "bq"):
        store.upsert_node("Table", t, {"table_name": "accounts"},
                          source=src)
    for _ in range(9):   # a chatty witness must cap at 5
        store.upsert_node("Table", t, {}, source="corpus")
    snap = tmp_path / "w.json"
    store.save_json(snap)
    return ConsoleData(snapshot_path=snap), t


def test_ledger_shows_weights_caps_and_rule(tmp_path):
    data, t = _world(tmp_path)
    led = data.witness(t)["witness"]["ledger"]
    rows = {r["source"]: r for r in led["rows"]}
    assert rows["mdm"]["weight"] == 8 and rows["mdm"]["capped"] == 1
    assert rows["corpus"]["count"] == 9 and rows["corpus"]["capped"] == 5
    # 8 + 6 + 1×5 = 19 → score 0.99 cap, 3 distinct
    assert led["weighted"] == 19
    assert led["score"] == 0.99
    assert led["distinct"] == 3
    assert "grounded" in led["rule"]
    # rows sorted by contribution, biggest first
    assert led["rows"][0]["contribution"] >= led["rows"][-1]["contribution"]
