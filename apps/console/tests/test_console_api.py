"""The read-side API + brief store + failure legibility.

Pins the truth-of-state rule (every payload says live vs sample and the
two worlds share one shape), the no-secrets config echo, the /chat →
brief tee, the guarded witness panel, and the actionable failure
messages the ADK path emits.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from apps.console.backend.app import create_app
from apps.console.backend.briefs import BriefStore
from apps.console.backend.data import ConsoleData
from apps.console.backend.runner import ScriptedRunner, _explain_failure

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "synapse"))


def _client(**kwargs) -> TestClient:
    return TestClient(create_app(ScriptedRunner(), **kwargs))


def _live_data(tmp_path) -> ConsoleData:
    from synapse.graph.store import GraphStore, canonical_uri
    store = GraphStore()
    t = canonical_uri("table", "accounts")
    store.upsert_node("Table", t, {"table_name": "accounts",
                                   "description": "account master"},
                      source="mdm")
    c = canonical_uri("column", "accounts", "acct_id")
    store.upsert_node("Column", c,
                      {"table_name": "accounts",
                       "description": "account key",
                       "sample_values": ["12", "34"]},   # must not leak
                      source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    store.upsert_node("Metric", canonical_uri("metric", "approval rate"),
                      {"name": "Approval rate",
                       "formula": "approved / decisioned"},
                      source="skills")
    snap = tmp_path / "snap.json"
    store.save_json(snap)
    return ConsoleData(snapshot_path=snap)


# ─── the two worlds share one shape ──────────────────────────


def test_sample_world_is_labeled_and_coherent():
    data = ConsoleData(snapshot_path="/nonexistent/graph.json")
    assert data.live is False
    for payload in (data.products(), data.metrics(), data.graph_summary(),
                    data.graph_thread(), data.resolve_term("roll rate")):
        assert payload["live"] is False
        assert payload["source"] == "sample"
    assert len(data.products()["products"]) == 5
    assert data.graph_thread()["thread"]["hops"][0]["kind"] == "entity"


def test_live_world_reads_the_graph(tmp_path):
    data = _live_data(tmp_path)
    assert data.live is True
    products = data.products()
    assert products["live"] is True and products["source"] == "graph"
    assert [p["name"] for p in products["products"]] == ["accounts"]
    assert products["products"][0]["readiness"]["columns"] == 1
    metrics = data.metrics()
    assert metrics["metrics"][0]["name"] == "Approval rate"


def test_witness_panel_strips_value_like_keys(tmp_path):
    data = _live_data(tmp_path)
    from synapse.graph.store import canonical_uri
    w = data.witness(canonical_uri("column", "accounts", "acct_id"))
    assert w["witness"]["found"] is True
    assert "sample_values" not in w["witness"]["properties"]
    assert w["witness"]["provenance"]["sources"] == ["mdm"]
    assert any(e["type"] == "CONTAINS" for e in w["witness"]["edges"])


def test_witness_panel_says_not_found_honestly(tmp_path):
    data = _live_data(tmp_path)
    assert data.witness("synapse://table/ghost")["witness"]["found"] is False


def test_threadless_live_graph_serves_sample_labeled_sample(tmp_path):
    """A live snapshot with no thread-worthy nodes falls back to the
    sample storyline — and says so. live:true + sample content is the
    one combination that must never ship."""
    from synapse.graph.store import GraphStore, canonical_uri
    store = GraphStore()
    t = canonical_uri("table", "bare")
    store.upsert_node("Table", t, {"table_name": "bare"}, source="mdm")
    snap = tmp_path / "thin.json"
    store.save_json(snap)
    data = ConsoleData(snapshot_path=snap)
    assert data.live is True                   # snapshot loaded fine
    thread = data.graph_thread()
    assert thread["live"] is False             # …but the thread is sample
    assert thread["source"] == "sample"


def test_briefs_not_seeded_for_a_live_runner():
    """Sample briefs furnish the scripted demo only — a live agent's
    workspace starts empty."""

    class _Liveish:                            # not a ScriptedRunner
        async def stream(self, m, *, turn_id, conversation_id=None):
            return
            yield

    client = TestClient(create_app(_Liveish()))
    assert client.get("/api/briefs").json()["briefs"] == []


# ─── metric viability: canon-first, three verdicts ───────────


def test_viability_exact_near_and_clear(tmp_path):
    data = _live_data(tmp_path)
    assert data.metric_viability("approval rate")["viability"]["verdict"] \
        == "exact"
    near = data.metric_viability(
        "rate of approval", "approved over decisioned applications")
    assert near["viability"]["verdict"] == "near_duplicate"
    assert near["viability"]["near"][0]["name"] == "Approval rate"
    assert data.metric_viability("median settlement lag")["viability"][
        "verdict"] == "clear"


# ─── briefs: seeded, then fed by the chat tee ────────────────


def test_brief_store_seeds_newest_first():
    briefs = BriefStore().list()
    assert len(briefs) == 3
    assert briefs[0]["created_at"] > briefs[-1]["created_at"]
    assert all(b["live"] is False for b in briefs)   # seeds are samples


def test_chat_tees_every_answer_into_a_brief():
    store = BriefStore(seed=False)
    client = TestClient(create_app(ScriptedRunner(), briefs=store))
    resp = client.post("/chat", json={
        "message": "Who owns sbs_new_accounts?"})
    assert resp.status_code == 200
    briefs = store.list()
    assert len(briefs) == 1
    brief = store.get(briefs[0]["id"])
    assert brief["question"] == "Who owns sbs_new_accounts?"
    assert brief["sections"]["citations"]
    assert brief["live"] is True


def test_warehouse_brief_records_its_ledger_row():
    store = BriefStore(seed=False)
    client = TestClient(create_app(ScriptedRunner(), briefs=store))
    client.post("/chat", json={"message": "run the count by month"})
    brief = store.get(store.list()[0]["id"])
    assert brief["ledger"] == [{"ref": "ledger:#4821"}]


# ─── endpoints ───────────────────────────────────────────────


def test_config_echo_has_no_secret_values():
    client = _client()
    body = client.get("/api/config").json()
    assert isinstance(body["credentials_set"], bool)
    assert isinstance(body["project_set"], bool)
    dumped = json.dumps(body)
    assert "PRIVATE KEY" not in dumped
    assert body["graph"]["live"] in (True, False)


def test_api_surface_round_trips():
    client = _client()
    assert client.get("/api/products").json()["products"]
    assert client.get("/api/metrics").json()["metrics"]
    assert client.get("/api/graph/summary").json()["summary"]["nodes"] > 0
    assert client.get("/api/graph/thread").json()["thread"]["hops"]
    assert client.get("/api/questions").json()["questions"]
    assert client.get("/api/briefs").json()["briefs"]
    v = client.post("/api/metrics/viability",
                    json={"name": "approval rate"}).json()
    assert v["viability"]["verdict"] in ("exact", "near_duplicate", "clear")
    r = client.get("/api/terms/resolve", params={"term": "roll rate"}).json()
    assert r["resolution"]["canonical"]["name"]
    missing = client.get("/api/briefs/nope").json()
    assert missing["found"] is False


# ─── failures name the fix ───────────────────────────────────


def test_failures_map_to_actions_and_keep_the_raw_error():
    tls = _explain_failure(Exception(
        "[SSL: CERTIFICATE_VERIFY_FAILED] self-signed certificate"))
    assert "GEMINI_TLS_INSECURE" in tls and "CERTIFICATE_VERIFY_FAILED" in tls
    creds = _explain_failure(Exception("403 PermissionDenied"))
    assert "GOOGLE_APPLICATION_CREDENTIALS" in creds
    quota = _explain_failure(Exception("429 RESOURCE_EXHAUSTED: quota"))
    assert "retry" in quota.lower()
    other = _explain_failure(ValueError("boom"))
    assert other == "ValueError: boom"
