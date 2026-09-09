"""Meridian read plane (E17): every admin endpoint renders the REAL
compiled fixture build end-to-end — and renders its designed
unavailable state when no build exists. Reality law as CI."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from apps.console.backend.app import create_app
from apps.console.backend.runner import ScriptedRunner

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
FX = SILO / "tests" / "fixtures"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory) -> dict:
    tmp = tmp_path_factory.mktemp("meridian")
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
         "--run-id", "console_r1"],
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
    return TestClient(create_app(ScriptedRunner()))


def test_home_serves_the_real_build(client):
    payload = client.get("/api/meridian/home").json()
    assert payload["available"] is True
    assert payload["counts"]["tables"] > 0
    assert payload["metrics_by_status"]        # status_served stack
    assert payload["excluded_tables"], \
        "the intentional exclusion is a first-class fact"
    assert payload["sources_count"] > 0
    assert "readiness" in payload


def test_sources_shelf_and_display_names(client):
    payload = client.get("/api/meridian/sources").json()
    assert payload["available"] is True
    by_source = {s["source"]: s for s in payload["sources"]}
    assert by_source["metrics_dmp"]["display"] \
        == "Data Marketplace Steward Definitions"
    assert by_source["glossary"]["display"] == "Acropedia"


def test_explorer_and_profiles(client):
    metrics = client.get("/api/meridian/explorer/metrics",
                         params={"status": "certified"}).json()
    assert metrics["available"] and metrics["rows"]
    assert all(r["status_served"] == "certified"
               for r in metrics["rows"])
    assert all(r["tier"] in ("ha", "gr", "in", "gu")
               for r in metrics["rows"])
    first = metrics["rows"][0]
    detail = client.get(
        f"/api/meridian/metric/{first['id']}").json()
    assert detail["found"] and detail["metric"]["id"] == first["id"]

    tables = client.get("/api/meridian/explorer/tables").json()
    assert tables["available"]
    gms = next(r for r in tables["rows"]
               if r["physical"] == "dw.gms_transaction")
    assert gms["columns"] > 0
    table = client.get(
        "/api/meridian/table/dw.gms_transaction").json()
    assert table["found"] and table["card"]
    assert table["metrics_here"]


def test_graph_map_builds_and_feedback(client, compiled):
    cosmos = client.get("/api/meridian/graph_map").json()
    assert cosmos["available"] and cosmos["nodes"]
    builds = client.get("/api/meridian/builds").json()
    assert builds["available"] and builds["current"] in builds["builds"]

    response = client.post("/api/meridian/feedback", json={
        "screen": "metric_profile", "object_id": "metric:abc",
        "vote": "up", "note": "grain reads right",
        "session_kind": "steward"})
    assert response.status_code == 201
    feedback_dir = compiled["graph"] / "runs" / "feedback"
    lines = []
    for path in feedback_dir.glob("feedback_*.jsonl"):
        lines += [json.loads(line) for line in
                  path.read_text().splitlines() if line.strip()]
    assert any(r["screen"] == "metric_profile"
               and r["vote"] == "up" and r["build"]
               for r in lines)


def test_artifact_staging_is_a_source_drop(client, tmp_path):
    """Staging a Knowledge File writes into sources/artifacts/ with
    the actor header — a source drop, never a graph write — and
    refuses silent overwrites."""
    os.environ["MERIDIAN_SOURCES_DIR"] = str(tmp_path / "sources")
    payload = {"business_unit": "USCS",
               "name": "lending vocabulary",
               "content": "ALIL means Active Lending In Force."}
    first = client.post("/api/meridian/artifacts", json=payload)
    assert first.status_code == 201 and first.json()["staged"]
    staged_file = (tmp_path / "sources" / "artifacts"
                   / "uscs_lending-vocabulary.md")
    text = staged_file.read_text()
    assert "actor admin" in text and "Active Lending" in text
    second = client.post("/api/meridian/artifacts", json=payload)
    assert second.json()["staged"] is False        # no silent overwrite
    listing = client.get("/api/meridian/artifacts").json()
    assert "uscs_lending-vocabulary.md" in listing["staged"]
    del os.environ["MERIDIAN_SOURCES_DIR"]


def test_no_build_renders_honest_unavailable(tmp_path):
    os.environ["MERIDIAN_BUILDS_DIR"] = str(tmp_path / "empty")
    os.environ["MERIDIAN_GRAPH_DIR"] = str(tmp_path / "graph")
    empty_client = TestClient(create_app(ScriptedRunner()))
    payload = empty_client.get("/api/meridian/home").json()
    assert payload["available"] is False
    assert "laptop.py compile" in payload["reason"]
