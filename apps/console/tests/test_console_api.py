"""The read-side API + the classic chat surface + failure legibility.

Pins the truth-of-state rule (every payload says live vs sample and the
two worlds share one shape), the no-secrets config echo, the guarded
witness panel, the user-selected 14-tool classic roster, and the
actionable failure messages the ADK path emits.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from apps.console.backend.app import create_app
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


def test_relative_snapshot_path_falls_back_to_repo_root(tmp_path, monkeypatch):
    """`export SYNAPSE_GRAPH_PATH=synapse/data/…` must work no matter
    which directory uvicorn was launched from."""
    monkeypatch.chdir(tmp_path)                # cwd far from the repo
    data = ConsoleData(
        snapshot_path="synapse/data/cache/graph_snapshot.json")
    assert data.snapshot_path.is_absolute()
    assert data.live is True                   # resolved via repo root


def test_table_anchored_thread_explores_that_table(tmp_path):
    from synapse.graph.store import GraphStore, canonical_uri
    store = GraphStore()
    t = canonical_uri("table", "accounts")
    store.upsert_node("Table", t, {"table_name": "accounts",
                                   "description": "account master"},
                      source="mdm")
    c = canonical_uri("column", "accounts", "acct_id")
    store.upsert_node("Column", c, {"table_name": "accounts"}, source="mdm")
    store.upsert_edge("CONTAINS", t, c, {}, source="mdm")
    ent = canonical_uri("entity", "Account")
    store.upsert_node("Entity", ent, {"name": "Account"},
                      source="human_approval")
    store.upsert_edge("IDENTIFIES", c, ent, {}, source="human_approval")
    snap = tmp_path / "s.json"
    store.save_json(snap)

    data = ConsoleData(snapshot_path=snap)
    out = data.graph_thread("accounts")
    assert out["live"] is True
    kinds = [h["kind"] for h in out["thread"]["hops"]]
    assert kinds[0] == "table"
    assert "entity" in kinds                   # found via IDENTIFIES

    # unknown anchor: honest empty, never the sample storyline
    missing = data.graph_thread("ghost_table")
    assert missing["live"] is True
    assert missing["thread"]["hops"] == []


def test_version_mismatch_failures_name_the_certified_pair():
    for exc in (
        TypeError("unsupported operand type(s) for |: 'function' and "
                  "'NoneType'"),
        AttributeError("module 'google.genai.types' has no attribute "
                       "'TurnCompleteReason'"),
    ):
        msg = _explain_failure(exc)
        assert "google-adk==1.31.1" in msg
        assert "python -m uvicorn" in msg


def test_config_reports_what_this_process_imported():
    body = _client().get("/api/config").json()
    assert body["sdk"]["python"]
    assert body["sdk"]["fastapi"]              # versions, not secrets


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


# ─── the classic chat surface ────────────────────────────────


def test_classic_roster_is_the_user_selected_bound():
    """The bounded chat agent: the original agent's capabilities under
    their current names + the skills library + the warehouse pair."""
    from apps.analyst.tools import build_classic_tools
    names = {t.__name__ for t in build_classic_tools()}
    assert names == {
        "search_entities", "list_tables_for_domain", "inspect_table",
        "find_columns_for_concept", "get_join_path", "get_lineage",
        "get_metric", "get_skill", "get_dq_status", "disambiguate_term",
        "validate_sql_plan", "get_entity", "get_steward_review_queue",
        "dry_run_sql", "execute_sql",
    }
    assert len(names) == 15


def test_classic_instruction_keeps_the_output_contract():
    from apps.analyst.prompts import CLASSIC_INSTRUCTION
    for section in ("## Answer", "## How I got there", "## Citations",
                    "## Governance & caveats", "## Status"):
        assert section in CLASSIC_INSTRUCTION
    # the skills library shapes question-understanding AND the answer
    assert "get_skill BEFORE" in CLASSIC_INSTRUCTION
    assert "IN ITS VOCABULARY" in CLASSIC_INSTRUCTION
    # tools outside the bound are never referenced
    for absent in ("render_chart", "run_python_analysis",
                   "load_agent_skill", "get_filter_values"):
        assert absent not in CLASSIC_INSTRUCTION


def test_briefs_surface_is_gone():
    client = _client()
    assert client.get("/api/briefs").status_code == 404
    resp = client.post("/chat", json={"message": "who owns this table?"})
    assert resp.status_code == 200               # chat unaffected


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
    v = client.post("/api/metrics/viability",
                    json={"name": "approval rate"}).json()
    assert v["viability"]["verdict"] in ("exact", "near_duplicate", "clear")
    r = client.get("/api/terms/resolve", params={"term": "roll rate"}).json()
    assert r["resolution"]["canonical"]["name"]


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
