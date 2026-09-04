"""Synapse Semantic Intelligence (apps/synapse): the second surface,
served beside the admin console by the same server. The shell is
stripped and renamed, the chats have a search page of their own,
artifacts publish inside the chat, and the library pages are cards
read from the compiled build. File pins because the frontend has no
build step; API pins against the REAL compiled fixture build."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from apps.lumi.backend.app import create_app

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
FX = SILO / "tests" / "fixtures"
FRONT = REPO_ROOT / "apps" / "synapse" / "frontend"
INDEX = (FRONT / "index.html").read_text(encoding="utf-8")
MAIN = (FRONT / "js" / "main.js").read_text(encoding="utf-8")
CHAT = (FRONT / "js" / "pages" / "chat.js").read_text(encoding="utf-8")
CSS = (FRONT / "styles" / "synapse.css").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def compiled(tmp_path_factory) -> dict:
    tmp = tmp_path_factory.mktemp("synapse")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain",
         "--run-id", "synapse_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    builds = tmp / "builds"
    _dir, _manifest, failures = compile_build(graph_dir, builds)
    assert not failures
    return {"builds": builds, "graph": graph_dir}


@pytest.fixture()
def client(compiled) -> TestClient:
    os.environ["MERIDIAN_BUILDS_DIR"] = str(compiled["builds"])
    os.environ["MERIDIAN_GRAPH_DIR"] = str(compiled["graph"])
    return TestClient(create_app())


def test_shell_is_stripped_and_renamed():
    """The left header says Synapse Semantic Intelligence; Home, Skills,
    Cosmos and Operate are gone; New chat and Search chats sit at the
    top, the chats under them, and Data Products, Metrics Explorer and
    Artifacts in their own section at the bottom above the account."""
    assert "<title>Synapse Semantic Intelligence</title>" in INDEX
    assert ">Synapse</a>" in INDEX and "Semantic Intelligence" in INDEX
    for gone in ("#/home", "#/skills", "#/cosmos", "#/operate", "#/ask",
                 "powered by Lumi", "Semantics Explorer", ">Tables<",
                 ">Home<", ">Skills<", "chats-search"):
        assert gone not in INDEX, gone
    explore = INDEX.split('aria-label="Explore"')[1].split("</nav>")[0]
    for kept in ("Data Products", "Metrics Explorer", "Artifacts"):
        assert kept in explore, kept
    order = [INDEX.index('href="#/chat/new"'), INDEX.index('href="#/search"'),
             INDEX.index('class="chats"'), INDEX.index('aria-label="Explore"'),
             INDEX.index('class="account"')]
    assert order == sorted(order)
    assert "Search chats" in INDEX
    # served under /synapse/: relative asset paths, no vendored three.js
    assert 'src="js/main.js"' in INDEX and 'href="styles/synapse.css"' in INDEX
    assert 'href="/styles' not in INDEX and 'src="/js' not in INDEX
    assert not (FRONT / "vendor").exists()
    for page in ("cosmos", "operate", "home", "skills", "ask"):
        assert not (FRONT / "js" / "pages" / f"{page}.js").exists(), page


def test_routes_are_the_new_surface_and_chat_is_the_door():
    for route in ("chat:", "search:", "products:", "product:", "metrics:",
                  "metric:", "artifacts:"):
        assert route in MAIN, route
    assert '|| "chat"' in MAIN                     # the default route
    assert "renderSearch" in MAIN and "renderProducts" in MAIN
    for gone in ("renderHome", "renderCosmos", "renderOperate",
                 "renderSkills", "renderAsk"):
        assert gone not in MAIN, gone
    for page in ("metric.js", "table.js"):
        text = (FRONT / "js" / "pages" / page).read_text(encoding="utf-8")
        assert "#/semantics" not in text and "#/tables" not in text, page
        assert "#/table/" not in text, page


def test_chat_publishes_artifacts_inside_the_chat():
    """No drawer: an artifact renders in a block where the turn made
    it, with its exports and versions; the Skills door is gone."""
    for gone in ("chat-panel", "panel-open", "panel-body", "Browse skills",
                 "openArtifact(", "artifact-inline"):
        assert gone not in CHAT, gone
    assert "artifact-block" in CHAT and "showArtifact(" in CHAT
    assert "exportButtons(row, block)" in CHAT
    assert ".artifact-block" in CSS and "grid-template-columns: minmax(0, 1fr)" in CSS


def test_library_pages_are_cards():
    products = (FRONT / "js" / "pages" / "tables.js").read_text(encoding="utf-8")
    metrics = (FRONT / "js" / "pages" / "semantics.js").read_text(encoding="utf-8")
    assert "Data Products" in products and "product-card" in products
    for field in ("r.description", "r.rows", "r.latest_partition",
                  "r.metric_names", "r.join_partners", "r.owner"):
        assert field in products, field
    assert "Metrics Explorer" in metrics and "metric-card" in metrics
    for field in ("r.question", "r.grain", "r.dimensions", "r.execution_count",
                  "r.description", "statusLabel(r.status_served)"):
        assert field in metrics, field
    assert ".card-grid" in CSS and ".product-card" in CSS and ".metric-card" in CSS


def test_second_surface_is_served_beside_the_first(client):
    page = client.get("/synapse/")
    assert page.status_code == 200
    assert "Synapse Semantic Intelligence" in page.text
    assert client.get("/synapse/js/main.js").status_code == 200
    assert client.get("/synapse/styles/synapse.css").status_code == 200
    home = client.get("/")
    assert home.status_code == 200 and "powered by Lumi" in home.text


def test_data_products_carry_their_texture(client):
    rows = client.get("/api/meridian/explorer/tables").json()["rows"]
    gms = next(r for r in rows if r["physical"] == "dw.gms_transaction")
    for key in ("description", "business_unit", "owner", "layer", "rows",
                "latest_partition", "lifecycle", "object_type",
                "primary_key", "metric_names", "join_partners"):
        assert key in gms, key
    assert gms["latest_partition"] == "2026-08-22"      # the id 20260822
    assert gms["metric_names"] and len(gms["metric_names"]) <= 4
    assert "dw.wwcas_authorization" in gms["join_partners"]
    assert any(r["description"] for r in rows)          # atlas purpose


def test_metrics_explorer_carries_the_catalog_texture(client):
    rows = client.get("/api/meridian/explorer/metrics").json()["rows"]
    for key in ("question", "grain", "dimensions", "description", "domain",
                "execution_count", "confidence", "last_seen",
                "business_unit", "data_category"):
        assert all(key in r for r in rows), key
    assert any(r["dimensions"] for r in rows)
    assert any(r["execution_count"] for r in rows)      # the catalog's


def test_search_chats_lists_everything_and_forgives_a_typo(client):
    from apps.lumi.backend import chat as chat_module
    made = client.post("/api/chat/sessions").json()["session"]
    client.post(f"/api/chat/sessions/{made['id']}/rename",
                json={"title": "Merchant churn framing"})
    store = chat_module._RUNTIME.store
    store.add_message(made["id"], "user",
                      "how should I think about merchant churn?")
    store.add_message(made["id"], "assistant",
                      "Churn splits into a rate question and a mix question.")
    everything = client.get("/api/chat/search").json()
    assert everything["available"]
    mine = next(s for s in everything["sessions"] if s["id"] == made["id"])
    assert mine["messages"] == 2 and mine["preview"].startswith("how should")
    hits = client.get("/api/chat/search",
                      params={"q": "merchnt churn"}).json()["sessions"]
    assert hits and hits[0]["id"] == made["id"]
    assert "merchant" in hits[0]["title_hits"]
    assert hits[0]["snippets"] and "merchant" in hits[0]["snippets"][0]["hits"]
    assert "churn" in hits[0]["snippets"][0]["text"].lower()
    none = client.get("/api/chat/search",
                      params={"q": "zebra quantum"}).json()["sessions"]
    assert not any(s["id"] == made["id"] for s in none)
