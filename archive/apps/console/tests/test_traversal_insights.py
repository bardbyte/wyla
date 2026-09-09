"""The two graph views + the live-path stream hardening.

Pins: (1) the ADK→SSE serialization fix — protobuf-ish composites nested
in tool args/payloads must never kill the stream (the works-in-adk-web,
hangs-in-console bug); (2) the per-table Insights read (relationships
with witness labels); (3) the agent selftest surface.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from apps.console.backend.app import create_app
from apps.console.backend.data import ConsoleData
from apps.console.backend.events import ToolCall, ToolResult, to_sse
from apps.console.backend.runner import ScriptedRunner, _jsonable, _map_adk_event

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "synapse"))


# ─── protobuf-ish fakes (shape, not the proto dependency) ────


class FakeMapComposite:
    """Mapping-like but NOT a dict — json.dumps raises on it."""

    def __init__(self, d):
        self._d = d

    def items(self):
        return self._d.items()


class FakeRepeated:
    def __init__(self, items):
        self._items = list(items)

    def __iter__(self):
        return iter(self._items)


def test_jsonable_flattens_nested_composites():
    nasty = FakeMapComposite({
        "sql": "SELECT 1",
        "nested": FakeMapComposite({
            "vals": FakeRepeated([1, b"raw-bytes",
                                  FakeMapComposite({"deep": 2})]),
        }),
    })
    out = _jsonable(nasty)
    json.dumps(out)                      # must be plain JSON now
    assert out["sql"] == "SELECT 1"
    assert out["nested"]["vals"][1] == "raw-bytes"
    assert out["nested"]["vals"][2] == {"deep": 2}


def test_to_sse_never_raises_on_unserializable_payload():
    """One bad tool result used to kill the whole SSE stream mid-turn —
    the console hung at 'Working…' while adk web (its own serializer)
    worked. Degraded output is fine; a dead stream is not."""
    frame = to_sse(ToolResult(call_id="c1",
                              payload={"raw": FakeRepeated([1, 2])}))
    assert frame.startswith("data: ") and frame.endswith("\n\n")
    json.loads(frame[len("data: "):])    # the frame is valid JSON


def test_map_adk_event_sanitizes_args_and_payload():
    class FC:  # function_call part
        id = "call1"; name = "execute_sql"
        args = FakeMapComposite({"sql": "SELECT 1",
                                 "opts": FakeMapComposite({"max": 5})})

    class FR:  # function_response part
        id = "call1"; name = "execute_sql"
        response = FakeMapComposite({
            "status": "ok",
            "data": FakeMapComposite({"rows": FakeRepeated([
                FakeMapComposite({"n": 1})])}),
        })

    class PartCall:
        function_call = FC(); function_response = None; text = None

    class PartResp:
        function_call = None; function_response = FR(); text = None

    class Content:
        parts = [PartCall(), PartResp()]

    class Ev:
        content = Content()

    events = _map_adk_event(Ev())
    assert [type(e) for e in events] == [ToolCall, ToolResult]
    # every event must survive strict JSON serialization
    for e in events:
        json.loads(to_sse(e)[len("data: "):])
    assert events[0].args == {"sql": "SELECT 1", "opts": {"max": 5}}
    assert events[1].payload["data"]["rows"] == [{"n": 1}]


# ─── the Insights read (per-table, witness-labeled) ──────────


def _insights_world(tmp_path) -> ConsoleData:
    from synapse.graph.store import GraphStore, canonical_uri
    store = GraphStore()
    for name in ("accounts", "txns"):
        t = canonical_uri("table", name)
        store.upsert_node("Table", t, {
            "table_name": name, "description": f"{name} master"},
            source="mdm")
        c = canonical_uri("column", name, "acct_id")
        store.upsert_node("Column", c, {"table_name": name,
                                        "description": "key"}, source="mdm")
        store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    # corpus-observed join (analyst query log) + a curated entity
    store.upsert_edge("EQUIVALENT_TO",
                      canonical_uri("column", "accounts", "acct_id"),
                      canonical_uri("column", "txns", "acct_id"),
                      {"observed_in_query": "Q1"}, source="corpus")
    ent = canonical_uri("entity", "Account")
    store.upsert_node("Entity", ent, {"name": "Account"},
                      source="human_approval")
    store.upsert_edge("IDENTIFIES",
                      canonical_uri("column", "accounts", "acct_id"),
                      ent, {}, source="human_approval")
    snap = tmp_path / "ins.json"
    store.save_json(snap)
    return ConsoleData(snapshot_path=snap)


def test_table_insights_labels_each_relationship_witness(tmp_path):
    ins = _insights_world(tmp_path).table_insights("accounts")["insights"]
    assert ins["found"] is True
    by_kind = {r["kind"]: r for r in ins["relationships"]}
    join = by_kind["join"]
    assert join["predicate"] == "accounts.acct_id = txns.acct_id"
    assert join["witness"] == "query log (analyst)"   # the curation story
    assert by_kind["identifies"]["witness"] == "curated"
    assert by_kind["identifies"]["tier"] == "human_asserted"
    assert ins["columns"]["count"] == 1


def test_table_insights_honest_when_missing(tmp_path):
    data = _insights_world(tmp_path)
    assert data.table_insights("ghost")["insights"]["found"] is False
    empty = ConsoleData(snapshot_path="/none/g.json").table_insights("x")
    assert empty["source"] == "empty"
    assert empty["insights"]["found"] is False


# ─── endpoints ───────────────────────────────────────────────


def test_insights_and_selftest_endpoints(tmp_path):
    client = TestClient(create_app(
        ScriptedRunner(), data=_insights_world(tmp_path)))
    body = client.get("/api/graph/insights",
                      params={"table": "accounts"}).json()
    assert body["insights"]["found"] is True
    assert body["insights"]["relationships"]
    st = client.get("/api/agent/selftest").json()
    assert st["ok"] is True and st["runner"] == "ScriptedRunner"
