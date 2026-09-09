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
    """The left header says Synapse Semantic Intelligence; Home, Cosmos
    and Operate are gone; New chat and Search chats sit at the top, the
    chats under them, and Data Products, Semantics Explorer and Skills in
    their own section at the bottom above the account (Artifacts left
    the nav: the shelf lives in the chat, the Skills page showcases
    what the agent knows how to do)."""
    assert "<title>Synapse Semantic Intelligence</title>" in INDEX
    assert ">Synapse</a>" in INDEX and "Semantic Intelligence" in INDEX
    for gone in ("#/home", "#/cosmos", "#/operate", "#/ask", "#/artifacts",
                 "powered by Lumi", "Metrics Explorer", ">Tables<",
                 ">Home<", ">Artifacts<", "chats-search"):
        assert gone not in INDEX, gone
    explore = INDEX.split('aria-label="Explore"')[1].split("</nav>")[0]
    for kept in ("Data Products", "Semantics Explorer", "Skills", "Knowledge"):
        assert kept in explore, kept
    assert explore.index("Skills") < explore.index("Knowledge")
    order = [INDEX.index('href="#/chat/new"'), INDEX.index('href="#/search"'),
             INDEX.index('class="chats"'), INDEX.index('aria-label="Explore"'),
             INDEX.index('class="account"')]
    assert order == sorted(order)
    assert "Search chats" in INDEX
    # served under /synapse/: relative asset paths, no vendored three.js
    assert 'src="js/main.js"' in INDEX and 'href="styles/synapse.css"' in INDEX
    assert 'href="/styles' not in INDEX and 'src="/js' not in INDEX
    assert not (FRONT / "vendor").exists()
    for page in ("cosmos", "operate", "home", "ask"):
        assert not (FRONT / "js" / "pages" / f"{page}.js").exists(), page
    assert (FRONT / "js" / "pages" / "skills.js").exists()
    assert (FRONT / "js" / "pages" / "knowledge.js").exists()
    assert not (FRONT / "js" / "pages" / "artifacts.js").exists()


def test_routes_are_the_new_surface_and_chat_is_the_door():
    for route in ("chat:", "search:", "products:", "product:", "metrics:",
                  "metric:", "skills:", "knowledge:", "artifacts:"):
        assert route in MAIN, route            # artifacts: the old name
    assert '|| "chat"' in MAIN                     # the default route
    assert "renderSearch" in MAIN and "renderProducts" in MAIN
    assert "renderSkills" in MAIN and "renderKnowledge" in MAIN
    assert "renderArtifacts" not in MAIN
    for gone in ("renderHome", "renderCosmos", "renderOperate", "renderAsk"):
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
    assert "Semantics Explorer" in metrics and "metric-card" in metrics
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



def test_logo_from_the_env_replaces_the_words(client, tmp_path, monkeypatch):
    """SYNAPSE_LOGO in the silo .env names an image on the machine: the
    server serves it and says whether one is configured; the page
    swaps its words for the image only when one is served. A missing
    file or a non-image is refused with a reason, and the words stay."""
    monkeypatch.delenv("SYNAPSE_LOGO", raising=False)
    bare = client.get("/api/lumi/brand").json()
    assert {k: bare[k] for k in ("logo", "configured", "reason", "stamp")} \
        == {"logo": False, "configured": False, "reason": "", "stamp": ""}
    assert {"env_file", "path", "exists", "looks_like"} <= set(bare)
    # a path with nothing at it: the exact path tried, and the note
    # about inline comments
    monkeypatch.setenv("SYNAPSE_LOGO", str(tmp_path / "missing.png"))
    lost = client.get("/api/lumi/brand").json()
    assert lost["configured"] and not lost["logo"] and not lost["exists"]
    assert str(tmp_path / "missing.png") in lost["reason"]
    assert "' #'" in lost["reason"]
    # a .png that is not a PNG (an export gone wrong): refused, the
    # bytes named, never served for the browser to drop in silence
    fake = tmp_path / "logo.png"
    fake.write_bytes(b"\x00\x00\x00\x18ftypheic")
    monkeypatch.setenv("SYNAPSE_LOGO", str(fake))
    wrong = client.get("/api/lumi/brand").json()
    assert wrong["exists"] and not wrong["logo"]
    assert "named .png but its bytes" in wrong["reason"]
    assert client.get("/api/lumi/logo").status_code == 404
    # a real PNG: served as image/png
    import struct
    import zlib
    def chunk(kind, body):
        return (struct.pack(">I", len(body)) + kind + body
                + struct.pack(">I", zlib.crc32(kind + body) & 0xffffffff))
    png = (b"\x89PNG\r\n\x1a\n"
           + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
           + chunk(b"IDAT", zlib.compress(b"\x00\xff\x00\x00"))
           + chunk(b"IEND", b""))
    real = tmp_path / "real.PNG"
    real.write_bytes(png)
    monkeypatch.setenv("SYNAPSE_LOGO", str(real))
    good = client.get("/api/lumi/brand").json()
    assert good["logo"] and good["looks_like"] == ".png" and good["stamp"]
    served = client.get("/api/lumi/logo")
    assert served.status_code == 200
    assert served.headers["content-type"].startswith("image/png")
    assert served.content == png
    monkeypatch.delenv("SYNAPSE_LOGO", raising=False)   # back to unset
    refused = client.get("/api/lumi/logo")
    assert refused.status_code == 404
    assert "SYNAPSE_LOGO" in refused.json()["reason"]
    # configured to something that is not an image: refused, explained
    text = tmp_path / "logo.txt"
    text.write_text("not an image")
    monkeypatch.setenv("SYNAPSE_LOGO", str(text))
    got = client.get("/api/lumi/brand").json()
    assert got["logo"] is False and got["configured"] is True
    assert "png" in got["reason"]
    assert client.get("/api/lumi/logo").status_code == 404
    # configured to an svg: served with its media type and a stamp
    svg = tmp_path / "logo.svg"
    svg.write_text("<svg xmlns='http://www.w3.org/2000/svg' width='120' "
                   "height='40'><text y='28' font-size='24'>ACME</text>"
                   "</svg>", encoding="utf-8")
    monkeypatch.setenv("SYNAPSE_LOGO", str(svg))
    got = client.get("/api/lumi/brand").json()
    assert got["logo"] is True and got["stamp"]
    served = client.get("/api/lumi/logo")
    assert served.status_code == 200
    assert served.headers["content-type"].startswith("image/svg+xml")
    assert b"ACME" in served.content
    # the page: the words are the fallback, the swap waits for the load
    assert 'id="brand"' in INDEX and "Semantic Intelligence" in INDEX
    assert "brandLogo" in MAIN and "/api/lumi/brand" in MAIN
    assert "img.onload" in MAIN and "replaceChildren" in MAIN
    assert "img.onerror" in MAIN and "console.warn(`SYNAPSE_LOGO" in MAIN
    assert ".brand-logo" in CSS


def test_the_second_surface_switches_models_and_explains_the_dials(client):
    """The same switch and the same "?" as the admin console, from the
    same catalog: the composer's model select, the popover with the
    three groups, and the send that carries the chat's plane."""
    for piece in ('id="chat-model"', 'id="chat-help"', "chat-help-pop",
                  "api.chatDials()", "api.chatSetModel(",
                  "state.mode, state.plane", "help-group",
                  "Depth <span>", "Model <span>"):
        assert piece in CHAT, piece
    app_css = (FRONT / "styles" / "app.css").read_text(encoding="utf-8")
    for cls in (".chat-plane", ".chat-help", ".chat-help-pop",
                ".help-group + .help-group", ".help-row"):
        assert cls in app_css, cls
    dials = client.get("/api/chat/dials").json()
    assert len(dials["planes"]) == 2 and len(dials["depths"]) == 3
    assert client.get("/synapse/js/api.js").text.count("chatSetModel") == 1


def test_data_products_filter_by_line_of_business(client):
    """The explorer rows name the line of business by code and by name,
    and the page filters on it beside the search: every code the build
    maps a table to, with its count, and "unmapped" for the rest."""
    rows = client.get("/api/meridian/explorer/tables").json()["rows"]
    gms = next(r for r in rows if r["physical"] == "dw.gms_transaction")
    assert gms["lob"] == "GMNS"
    assert gms["lob_name"] == "Global Merchant & Network Services"
    assert all("lob_name" in r for r in rows)
    products = (FRONT / "js" / "pages" / "tables.js").read_text(encoding="utf-8")
    for piece in ('id="p-filter"', "filterBar(", "optionsFrom(",
                  '"Line of business"', '"Layer"', '"Lifecycle"', "unmapped",
                  "r.lob_name", 'picks.lob === "unmapped"'):
        assert piece in products, piece
    assert "p-lob" not in products and "<select" not in products
    metrics = (FRONT / "js" / "pages" / "semantics.js").read_text(encoding="utf-8")
    for piece in ('id="m-filter"', "filterBar(", '"Data product"',
                  "lob: picks.lob", "table: picks.table"):
        assert piece in metrics, piece
    assert "table-filter" not in metrics and "<select" not in metrics
    filters = (FRONT / "js" / "filters.js").read_text(encoding="utf-8")
    for piece in ("export function filterBar", "export function optionsFrom",
                  "filter-chip", "filter-opt", 'data-value=""'):
        assert piece in filters, piece
    for cls in (".filter-bar", ".filter-pop", ".filter-chip", ".filter-opt.on"):
        assert cls in CSS, cls


def test_the_product_page_explains_every_column(client):
    """Every servable column with what it is: the compiler's columns
    index served on the table detail (description, Lumi's supplementary
    meaning, sensitivity, agreement), the product's own description and
    line of business beside it, and a page that searches the columns,
    shows the first twelve, and opens a row to its meaning and its
    uses in joins and metrics."""
    detail = client.get("/api/meridian/table/dw.gms_transaction").json()
    assert detail["found"]
    assert detail["description"] == "Global merchant transaction spine."
    assert detail["lob"] == "GMNS" and detail["business_unit"] == "GMNS"
    cols = {c["name"]: c for c in detail["columns_detail"]}
    assert set(cols) == set(detail["columns"])            # complete
    assert cols["cm13"]["sensitive"]
    assert cols["cm13"]["description"] == "Card member number."
    assert cols["trans_usd_am"]["supplementary"] \
        == "Signed transaction amount in US dollars."
    assert cols["bq_only_col"]["ungoverned"]
    assert cols["cm13"]["type"] == "STRING"          # typed by BigQuery
    assert cols["txn_uid"]["type"] == ""              # atlas-only: no type
    assert cols["txn_uid"]["type_source"] == "atlas"
    assert any(m["expr"] for m in detail["metrics_here"])
    page = (FRONT / "js" / "pages" / "table.js").read_text(encoding="utf-8")
    for piece in ("const FIRST = 12", 'id="col-search"', "columns_detail",
                  "col-detail", "c.supplementary", "c.description_source",
                  "in joins:", "in metrics:", "search for the rest",
                  "show all ${hits.length} columns", "product-desc-full",
                  "detail.lob_name"):
        assert piece in page, piece
    for cls in (".col-row", ".col-head", ".col-detail", ".col-uses",
                ".product-desc-full"):
        assert cls in CSS, cls


def test_the_column_detail_falls_back_to_the_served_card():
    """A build compiled before columns.json existed: the served card's
    column lines carry the meaning, merged with the schema so no
    servable column is missing (past the card's budget a column keeps
    its type and an empty description)."""
    from apps.lumi.backend.meridian import _DATA

    class Stub:
        columns = {}
        schema = {"dw.t": {"cm13": "STRING", "amt": "FLOAT64",
                           "late": "DATE", "nested.x": "STRING"}}
    text = ("# table dw.t\n## columns\n"
            "- cm13 string (SENSITIVE): Card member number. "
            "[prov:bq·agree=3]\n"
            "- amt float64: Amount. | lumi: Signed amount. [prov:bq·agree=2]\n"
            "- nested.x string (ungoverned, no business meaning on record) "
            "[prov:bq·agree=1]\n"
            "## joined with (observed)\n- dw.u · 3 co-queries [prov:bq]\n")
    rows = {r["name"]: r for r in _DATA._columns_detail(Stub(), "dw.t", text)}
    assert list(rows) == ["cm13", "amt", "late", "nested.x"]
    assert rows["cm13"]["sensitive"] and rows["cm13"]["description"] \
        == "Card member number." and rows["cm13"]["agreement"] == 3
    assert rows["amt"]["description"] == "Amount."
    assert rows["amt"]["supplementary"] == "Signed amount."
    assert rows["nested.x"]["ungoverned"] and not rows["nested.x"]["description"]
    assert rows["late"] == {
        "name": "late", "type": "DATE", "type_source": "",
        "description": "", "description_source": "", "supplementary": "",
        "business_name": "", "sensitive": False, "sensitivity_sources": [],
        "ungoverned": False, "agreement": 1, "flags": []}


def test_metric_cards_open_in_place_with_the_definition_and_the_table():
    metrics = (FRONT / "js" / "pages" / "semantics.js").read_text(encoding="utf-8")
    for piece in ('role="button"', "metric-detail", "api.metric(id)",
                  "api.table(r.table)", "COMPUTED ON", "DEFINITION",
                  "m.canonical_sql", "m.common_filters", "reads",
                  "full profile →", "openCard(el)", "e.key !== \"Enter\""):
        assert piece in metrics, piece
    for cls in (".metric-card.open", ".metric-detail", ".metric-foot"):
        assert cls in CSS, cls


def test_skills_showcase_what_the_agent_knows(client):
    """Skills in place of Artifacts: the doctrine packs the agent loads
    by itself, each with its slash command, its moves, the full text a
    click away, and a Use-in-chat door that opens a new chat with the
    slash command in the composer."""
    got = client.get("/api/chat/skills").json()
    assert got["available"] and len(got["skills"]) >= 4
    names = {s["name"] for s in got["skills"]}
    assert {"analysis-playbooks", "dashboard-design", "executive-summary",
            "lumi-data-connect"} <= names
    assert all(s["text"] and s["origin"] for s in got["skills"])
    page = (FRONT / "js" / "pages" / "skills.js").read_text(encoding="utf-8")
    for piece in ("Use in chat", "synapse.prefill", "read the doctrine",
                  "skills-how", "slash-name", "skill-moves",
                  'location.hash = "#/chat/new"', "api.chatSkills()"):
        assert piece in page, piece
    assert 'sessionStorage.getItem("synapse.prefill")' in CHAT
    assert "paintSlash();" in CHAT
    for cls in (".skills-how", ".skill-actions", ".slash-name"):
        assert cls in CSS, cls
    assert 'href="#/skills"' in INDEX


def test_short_table_names_on_the_face(client):
    """The pages name a table by its short name; the physical name
    stays in the tooltip, the link and the API."""
    products = (FRONT / "js" / "pages" / "tables.js").read_text(encoding="utf-8")
    assert "product-physical" not in products
    page = (FRONT / "js" / "pages" / "table.js").read_text(encoding="utf-8")
    assert 'title="${esc(physical)}">${\n          esc(physical.split(".").pop())}' in page
    assert 'title="${esc(t)}">${esc(t.split(".").pop())}' in page
    metrics = (FRONT / "js" / "pages" / "semantics.js").read_text(encoding="utf-8")
    assert 'esc(r.table.split(".").pop())' in metrics
    rows = client.get("/api/meridian/explorer/tables").json()["rows"]
    assert all(r["physical"].startswith("dw.") for r in rows)     # unchanged


def test_own_skills_and_the_creators(client):
    """Skills marks the person's own packs; a pack saves for the
    configured person and loads for them; the draft needs a model and
    says so here; a file becomes text for the creators (a Word file
    converted, a PDF refused with where to take it)."""
    import base64
    import io
    import zipfile
    shelf = client.get("/api/chat/skills").json()
    assert shelf["available"] and shelf["owner"]
    assert all("mine" in s and "owner" in s for s in shelf["skills"])
    text = ("# Churn triage\n\nThe moves for a churn question.\n\n"
            "## Split rate from mix\n1. search first.\n")
    saved = client.post("/api/chat/skills/mine",
                        json={"name": "Churn Triage", "text": text}).json()
    assert saved["available"] and saved["skill"]["name"] == "churn-triage"
    assert saved["skill"]["owner"] == shelf["owner"]
    mine = next(s for s in client.get("/api/chat/skills").json()["skills"]
                if s["name"] == "churn-triage")
    assert mine["mine"] and mine["origin"] == "unreviewed"
    refused = client.post("/api/chat/skills/mine", json={
        "name": "analysis-playbooks", "text": text}).json()
    assert refused["available"] is False and "built-in" in refused["reason"]
    gone = client.delete("/api/chat/skills/mine/churn-triage").json()
    assert gone["removed"] is True
    assert not any(s["name"] == "churn-triage" for s in
                   client.get("/api/chat/skills").json()["skills"])
    draft = client.post("/api/chat/skills/draft", json={
        "kind": "skill", "title": "Churn triage",
        "material": "rate vs mix first"}).json()
    assert draft["available"] is False and "not configured" in draft["reason"]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("word/document.xml", '<?xml version="1.0"?><w:document '
                   'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml'
                   '/2006/main"><w:body><w:p><w:r><w:t>Approvals are counted '
                   'at decision time.</w:t></w:r></w:p></w:body></w:document>')
    got = client.post("/api/chat/files/text", json={
        "name": "memo.docx",
        "data_b64": base64.b64encode(buf.getvalue()).decode()}).json()
    assert got["available"] and got["converted"]
    assert got["text"] == "Approvals are counted at decision time."
    pdf = client.post("/api/chat/files/text", json={
        "name": "memo.pdf", "data_b64": base64.b64encode(b"%PDF-1.4").decode()}
        ).json()
    assert pdf["available"] is False and "attach it in a chat" in pdf["reason"]
    skills_js = (FRONT / "js" / "pages" / "skills.js").read_text(encoding="utf-8")
    for piece in ("creatorPanel", "wireCreator", "Draft with the model",
                  "Save as my skill", "api.chatDraft(", "api.chatSaveSkill(",
                  "api.chatDeleteSkill(", "api.chatFileText(", "skill-delete",
                  ">Yours<", ">Shared<", ">Built in<"):
        assert piece in skills_js, piece
    knowledge_js = (FRONT / "js" / "pages" / "knowledge.js").read_text(
        encoding="utf-8")
    for piece in ("renderKnowledge", 'creatorPanel("knowledge")',
                  "api.stageArtifact(", 'startsWith("skills/")'):
        assert piece in knowledge_js, piece
    assert "Stage as a knowledge file" in skills_js     # the shared panel
    for cls in (".creator", ".creator-rendered", ".origin-tag.o-mine"):
        assert cls in CSS, cls


def test_the_shelf_and_the_help_read_plainly():
    """An empty chat stays off Recent and is reused by New chat; the
    "?" explains Depth and Model only, in plain words, and the model
    is named without its plane."""
    chats = (FRONT / "js" / "chats.js").read_text(encoding="utf-8")
    assert "(row.messages ?? 1) > 0" in chats
    assert 'find((r) => r.messages === 0)' in CHAT
    assert "Mode <span>" not in CHAT and "on Vertex" not in CHAT
    assert "Depth <span>how much Synapse thinks before each step" in CHAT
    assert 'if (d) o.title = d.means;' in CHAT
    assert "Semantics Explorer" in INDEX and "Metrics Explorer" not in INDEX
