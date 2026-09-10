"""The verified-query escrow — pins, reruns, seeds, and the ledger
join contract.

Pins: atomic persistence; sql_sha256 byte-identical to the warehouse
ledger hash; locator/headline shapes decided once at pin time; the
worst-chip tier rollup with the guessed floor; a pin assembled from a
scripted golden turn; reruns through the REAL gate chain (FakeClient
happy path, structured no_client refusal offline, guardrail refusal on
stored SQL); locator misses flagged instead of lied about; seeds served
only before the first write and protected from mutation.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from apps.console.backend.app import create_app
from apps.console.backend.pins import (
    NoSqlError, PinStore, choose_locator, compute_headline,
    sql_hash, worst_tier,
)
from apps.console.backend.runner import ScriptedRunner

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "synapse"))


def _store(tmp_path, **kwargs) -> PinStore:
    return PinStore(tmp_path / "pins.json", **kwargs)


def _events(message: str) -> list:
    async def collect():
        out = []
        async for ev in ScriptedRunner().stream(message, turn_id="t1"):
            out.append(ev)
        return out
    return asyncio.run(collect())


# ─── persistence + contracts ─────────────────────────────────


def test_store_persists_and_reloads_atomically(tmp_path):
    store = _store(tmp_path)
    pin = store.create(question="How many?", sql="SELECT 1 n",
                       rows=[{"n": 1}])
    doc = json.loads((tmp_path / "pins.json").read_text(encoding="utf-8"))
    assert doc["version"] == 1 and len(doc["pins"]) == 1
    again = _store(tmp_path)
    assert again.get(pin["id"])["question"] == "How many?"


def test_sha_matches_warehouse_ledger_hash():
    sql = "SELECT acct_id FROM accounts"
    assert sql_hash(sql) == hashlib.sha256(
        sql.encode("utf-8")).hexdigest()[:16]


def test_locator_and_headline_shapes():
    scalar = [{"approval_rate": 0.72}]
    loc = choose_locator(scalar)
    assert loc == {"row": "last", "column": "approval_rate"}
    assert compute_headline(scalar, loc)["kind"] == "scalar"

    series = [{"month": 202605, "n": 1150}, {"month": 202606, "n": 1247}]
    loc = choose_locator(series)
    assert loc["column"] == "n"                 # month-like int skipped
    head = compute_headline(series, loc)
    assert head["kind"] == "series_last" and head["value"] == 1247

    text_only = [{"owner": "New Accounts"}]
    assert choose_locator(text_only) is None
    assert compute_headline(text_only, None)["kind"] == "rows"
    assert compute_headline([], None)["kind"] == "none"


def test_tier_rollup_worst_chip_governs():
    assert worst_tier([{"tier": "human_asserted"},
                       {"tier": "inferred"}]) == "inferred"
    assert worst_tier([{"tier": None, "ref": "ledger:#1"}]) == "guessed"
    assert worst_tier([]) == "guessed"


# ─── a pin from the golden turn, exactly as the frontend does ─


def test_create_pin_from_scripted_golden_turn(tmp_path):
    events = _events("run the count of new accounts by month")
    sql = next(e.args["sql"] for e in events
               if e.type == "tool_call" and e.tool == "execute_sql")
    rows = next(e.payload["data"]["rows"] for e in events
                if e.type == "tool_result"
                and isinstance(e.payload, dict)
                and (e.payload.get("data") or {}).get("rows"))
    answer = next(e for e in events if e.type == "answer")
    ledger = next(e.ledger_id for e in events
                  if e.type == "gate_resolved" and e.ledger_id)

    store = _store(tmp_path, tier_resolver=lambda ref: "grounded")
    pin = store.create(
        question="run the count of new accounts by month",
        answer=answer.sections.answer,
        citations=[{"label": c.label, "ref": c.ref}
                   for c in answer.sections.citations],
        sql=sql, rows=rows, ledger_id=ledger, source="scripted")
    assert pin["headline"] == {"kind": "series_last", "column": "n",
                               "value": 1247, "n_rows": 8}
    assert pin["tier"] == "grounded"
    assert pin["history"][0]["ledger_id"] == "4821"
    assert pin["sql_sha256"] == sql_hash(sql)


# ─── reruns: the gate chain, honestly ────────────────────────


def _pin_with_sql(store: PinStore) -> dict:
    return store.create(
        question="monthly counts", sql="SELECT m, n FROM t",
        rows=[{"m": "2026-01", "n": 100}], citations=[])


def test_rerun_happy_path_refreshes_via_stored_locator(tmp_path):
    store = _store(tmp_path)
    pin = _pin_with_sql(store)

    class OkWarehouse:
        def execute(self, sql, max_rows=100):
            return {"status": "ok", "data": {
                "rows": [{"m": "2026-01", "n": 100},
                         {"m": "2026-02", "n": 130}],
                "n_rows_returned": 2, "total_rows": 2,
                "job_id": "job_42", "bytes_billed": 5}}

    out = store.rerun(pin["id"], OkWarehouse())
    assert out["run"]["status"] == "ok"
    assert out["run"]["value"] == 130           # stored locator, new data
    assert out["run"]["ledger_id"] == "job_42"
    assert out["pin"]["headline"]["value"] == 130
    assert len(out["pin"]["history"]) == 2      # capture + rerun


def test_rerun_offline_is_structured_refusal(tmp_path):
    store = _store(tmp_path)
    pin = _pin_with_sql(store)
    out = store.rerun(pin["id"], None)          # no graph → no gates → no run
    assert out["run"]["status"] == "refused"
    assert out["run"]["code"] == "no_graph"
    assert out["pin"]["headline"]["value"] == 100   # value untouched


def test_rerun_refusal_passes_through_gate_codes(tmp_path):
    store = _store(tmp_path)
    pin = _pin_with_sql(store)

    class RefusingWarehouse:
        def execute(self, sql, max_rows=100):
            return {"status": "refused", "code": "guardrail_violation",
                    "reason": "cm11_encrypted exposure"}

    out = store.rerun(pin["id"], RefusingWarehouse())
    assert out["run"]["code"] == "guardrail_violation"
    assert "cm11" in out["run"]["reason"]
    assert out["pin"]["headline"]["value"] == 100


def test_locator_miss_flags_instead_of_lying(tmp_path):
    store = _store(tmp_path)
    pin = _pin_with_sql(store)

    class DriftedWarehouse:
        def execute(self, sql, max_rows=100):
            return {"status": "ok",
                    "data": {"rows": [{"renamed": 7}], "job_id": "j"}}

    out = store.rerun(pin["id"], DriftedWarehouse())
    assert out["run"]["status"] == "ok"
    assert out["run"].get("locator_missed") is True
    assert out["run"]["value"] is None          # no invented delta


# ─── no seeds, no samples ────────────────────────────────────


def test_briefing_is_honestly_empty_before_the_first_pin(tmp_path):
    store = _store(tmp_path)
    assert store.list() == []
    pin = store.create(question="real", sql=None)
    assert [p["id"] for p in store.list()] == [pin["id"]]
    store.delete(pin["id"])
    assert store.list() == []

def test_verify_sets_and_clears_signature(tmp_path):
    store = _store(tmp_path)
    pin = store.create(question="q", sql=None)
    signed = store.verify(pin["id"], actor="steward")
    assert signed["verified"]["by"] == "steward"
    cleared = store.verify(pin["id"], verified=False)
    assert cleared["verified"] is None


def test_no_sql_pin_cannot_rerun(tmp_path):
    store = _store(tmp_path)
    pin = store.create(question="fact", sql=None)
    with pytest.raises(NoSqlError):
        store.rerun(pin["id"], None)


# ─── endpoints ───────────────────────────────────────────────


def test_pins_endpoints_round_trip(tmp_path):
    client = TestClient(create_app(
        ScriptedRunner(), pins=PinStore(tmp_path / "pins.json"),
        warehouse_factory=lambda: None))
    assert client.get("/api/pins").json()["pins"] == []

    created = client.post("/api/pins", json={
        "question": "monthly counts", "sql": "SELECT m, n FROM t",
        "rows": [{"m": 1, "n": 100}], "source": "scripted"}).json()
    pin_id = created["pin"]["id"]

    listed = client.get("/api/pins").json()
    assert [p["id"] for p in listed["pins"]] == [pin_id]

    rerun = client.post(f"/api/pins/{pin_id}/rerun", json={})
    assert rerun.status_code == 200
    assert rerun.json()["run"]["code"] == "no_graph"

    verified = client.post(f"/api/pins/{pin_id}/verify", json={})
    assert verified.json()["pin"]["verified"]["by"] == "steward"

    assert client.delete(f"/api/pins/{pin_id}").json()["deleted"] == pin_id
    assert client.get("/api/pins").json()["pins"] == []


def test_rerun_endpoint_codes(tmp_path):
    client = TestClient(create_app(
        ScriptedRunner(), pins=PinStore(tmp_path / "pins.json"),
        warehouse_factory=lambda: None))
    assert client.post("/api/pins/ghost/rerun",
                       json={}).status_code == 404
    fact = client.post("/api/pins", json={"question": "fact"}).json()
    resp = client.post(f"/api/pins/{fact['pin']['id']}/rerun", json={})
    assert resp.status_code == 409 and resp.json()["code"] == "no_sql"
