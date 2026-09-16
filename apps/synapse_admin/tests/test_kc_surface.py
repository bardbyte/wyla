"""KC Enrichment on the Synapse app: every endpoint against the REAL
compiled fixture build, the honest no-build state, the stream, the
writes, and the frontend contract (the tab is served; it holds no
vendor host and no key)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from apps.synapse_admin.backend.app import create_app

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
FX = SILO / "tests" / "fixtures"
TABLE = "dw.gms_transaction"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory) -> dict:
    tmp = tmp_path_factory.mktemp("kc_surface")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "kc_s1"],
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


def test_tab_is_served_and_holds_no_secret(client):
    page = client.get("/").text
    assert 'href="#/kc"' in page and "KC Enrichment" in page
    for path in ("/js/pages/kc.js", "/js/api.js", "/js/main.js"):
        text = client.get(path).text
        assert client.get(path).status_code == 200
        for banned in ("aiplatform.googleapis", "generativelanguage", "Bearer ",
                       "GEMINI_", "VERTEX_", "api_key", "apiKey"):
            assert banned not in text, (path, banned)
    kc_js = client.get("/js/pages/kc.js").text
    assert "renderKc" in kc_js and "renderKcDictionary" in kc_js and "renderKcTable" in kc_js
    assert "renderKcGlossary" in kc_js and "data-copy-text" in kc_js
    assert "EventSource" in kc_js                # the model sections stream in


def test_tables_coverage_and_bundle(client):
    tables = client.get("/api/kc/tables").json()
    assert tables["available"] and tables["rows"]
    row = tables["rows"][0]
    assert {"physical", "facts", "pct_copy", "gate", "cached", "last_push",
            "sections", "columns", "metrics"} <= set(row)
    assert tables["scope"] == "all" and "scope: every table" in tables["note"]
    assert tables["totals"]["tables"] == len(tables["rows"]) and "lobs" in tables
    cov = client.get("/api/kc/coverage").json()
    assert cov["available"] and cov["missing"] == [] and cov["dictionary"]["forward"]
    bundle = client.get(f"/api/kc/bundle/{TABLE}").json()
    assert bundle["available"] and bundle["found"]
    assert bundle["section_order"] == ["description", "overview", "columns", "glossary",
                                       "related_entries", "aspects", "dq", "queries",
                                       "contacts", "review", "push"]
    for key in bundle["section_order"]:
        sec = bundle["sections"][key]
        assert sec["text"] or sec["empty_reason"], key      # honest empties
    assert bundle["ledger"] and bundle["suggestions"] and bundle["guide"]
    assert bundle["aspect_types"] and all("metadataTemplate" in a for a in bundle["aspect_types"])
    assert bundle["llm"]["enabled"] is False or bundle["llm"]["cached"] is False
    ledger = client.get(f"/api/kc/bundle/{TABLE}/ledger").json()
    assert sum(r["count"] for r in ledger["ledger"]) == len(bundle["facts"])
    missing = client.get("/api/kc/bundle/zz.nope").json()
    assert missing == {"available": True, "found": False, "table": "zz.nope"}


def test_glossary_across_and_export_all(client):
    merged = client.get("/api/kc/glossary").json()
    assert merged["available"] and merged["terms"] and merged["categories"]
    assert len(merged["tables"]) == len(client.get("/api/kc/tables").json()["rows"])
    for fmt, media in (("jsonl", "application/x-ndjson"), ("links", "application/x-ndjson"),
                       ("sheet", "text/csv")):
        r = client.get(f"/api/kc/glossary/export?format={fmt}")
        assert r.status_code == 200 and r.headers["content-type"].startswith(media), fmt
    assert "error" in client.get("/api/kc/glossary/export?format=xml").json()
    r = client.get("/api/kc/export-all")
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/zip")
    assert "kc_all_" in r.headers["content-disposition"]


def test_exports_every_format(client):
    for fmt, media in (("md", "text/markdown"), ("json", "application/json"),
                       ("csv", "text/csv"), ("sheet", "text/csv"), ("zip", "application/zip")):
        r = client.get(f"/api/kc/bundle/{TABLE}/export?format={fmt}")
        assert r.status_code == 200 and r.headers["content-type"].startswith(media), fmt
        assert "attachment" in r.headers["content-disposition"]
    assert "error" in client.get(f"/api/kc/bundle/{TABLE}/export?format=docx").json()


def test_stream_ends_with_the_bundle(client):
    with client.stream("GET", f"/api/kc/bundle/{TABLE}/stream") as s:
        events, payload = [], None
        for line in s.iter_lines():
            if line.startswith("event:"):
                events.append(line.split(":", 1)[1].strip())
            if line.startswith("data:") and events and events[-1] == "bundle":
                payload = json.loads(line[5:])
                break
    assert events[-2:] == ["bundle_ready", "bundle"]
    assert payload["available"] and payload["found"]
    # no model contract on this machine: the reason is named, never hidden
    assert payload["llm"]["reason"].startswith("model unavailable") or payload["llm"]["generated"]


def test_push_record_and_witness_import(client, compiled):
    refused = client.post(f"/api/kc/push-record/{TABLE}",
                          json={"sections": ["description"], "actor": " "})
    assert refused.json()["recorded"] is False
    ok = client.post(f"/api/kc/push-record/{TABLE}",
                     json={"sections": ["description", "overview"], "actor": "tester"})
    assert ok.status_code == 201 and ok.json()["recorded"]
    quads = (compiled["graph"] / "edges" / "kc_pushed.jsonl").read_text().splitlines()
    assert json.loads(quads[-1])["prov"]["actor"] == "tester"
    tables = client.get("/api/kc/tables").json()
    row = next(r for r in tables["rows"] if r["physical"] == TABLE)
    assert row["last_push"]["actor"] == "tester"
    imported = client.post("/api/kc/witness-import", json={
        "kind": "glossary", "actor": "tester",
        "payload": {"items": [{"term": "Spend", "description": "Money moved", "table": TABLE},
                              {"term": "X", "table": "zz.unknown"}]}})
    assert imported.status_code == 201
    body = imported.json()
    assert body["nodes"] == 1 and body["skipped"] == ["item 1: unknown table zz.unknown"]
    assert client.post("/api/kc/witness-import", json={"kind": "bogus", "payload": {}}).status_code == 422


def test_no_build_is_honest(tmp_path):
    os.environ["MERIDIAN_BUILDS_DIR"] = str(tmp_path / "nobuilds")
    os.environ["MERIDIAN_GRAPH_DIR"] = str(tmp_path / "nograph")
    c = TestClient(create_app())
    for path in ("/api/kc/tables", "/api/kc/coverage", f"/api/kc/bundle/{TABLE}",
                 f"/api/kc/bundle/{TABLE}/ledger", f"/api/kc/bundle/{TABLE}/export?format=md",
                 "/api/kc/glossary", "/api/kc/glossary/export?format=jsonl",
                 "/api/kc/export-all"):
        payload = c.get(path).json()
        assert payload["available"] is False and "no compiled build" in payload["reason"], path
    assert c.post(f"/api/kc/push-record/{TABLE}",
                  json={"sections": ["description"], "actor": "x"}).json()["available"] is False
