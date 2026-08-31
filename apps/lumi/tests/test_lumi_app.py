"""Synapse by Lumi (apps/lumi): the fresh app serves the shell, the
planes echo, and the full Meridian read plane against the REAL
compiled fixture build — plus the designed unavailable state and the
artifact staging round-trip. Reality law as CI."""

from __future__ import annotations

import json
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


@pytest.fixture(scope="module")
def compiled(tmp_path_factory) -> dict:
    tmp = tmp_path_factory.mktemp("lumi")
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
         "--run-id", "lumi_r1"],
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


def test_shell_and_planes(client):
    """One process serves the product: the shell HTML (no build step),
    health, and the planes echo — booleans, never secrets."""
    page = client.get("/")
    assert page.status_code == 200
    assert "LUMI" in page.text and "powered by Synapse" in page.text
    assert client.get("/health").json()["app"] == "synapse-by-lumi"
    planes = client.get("/api/lumi/planes").json()
    assert set(planes) == {"bq", "vertex"}
    for plane in ("bq", "vertex"):
        for value in planes[plane].values():
            assert isinstance(value, (bool, str))
    vendor = client.get("/vendor/three.module.min.js")
    assert vendor.status_code == 200        # the sky renders offline


def test_read_plane_end_to_end(client):
    home = client.get("/api/meridian/home").json()
    assert home["available"] is True
    assert home["counts"]["tables"] > 0
    assert home["metrics_by_status"]
    assert home["excluded_tables"]
    shelf = client.get("/api/meridian/sources").json()
    by_source = {s["source"]: s for s in shelf["sources"]}
    assert by_source["glossary"]["display"] == "Acropedia"
    metrics = client.get("/api/meridian/explorer/metrics",
                         params={"status": "certified"}).json()
    assert metrics["rows"] and all(
        r["tier"] in ("ha", "gr", "in", "gu") for r in metrics["rows"])
    first = metrics["rows"][0]
    detail = client.get(
        f"/api/meridian/metric/{first['id']}").json()
    assert detail["found"]
    table = client.get(
        "/api/meridian/table/dw.gms_transaction").json()
    assert table["found"] and table["metrics_here"]
    cosmos = client.get("/api/meridian/graph_map").json()
    assert cosmos["available"] and cosmos["nodes"]


def test_feedback_and_staging(client, compiled, tmp_path):
    response = client.post("/api/meridian/feedback", json={
        "screen": "metric_profile", "object_id": "metric:abc",
        "vote": "up"})
    assert response.status_code == 201
    feedback_dir = compiled["graph"] / "runs" / "feedback"
    assert any("metric_profile" in line
               for path in feedback_dir.glob("feedback_*.jsonl")
               for line in path.read_text().splitlines())

    os.environ["MERIDIAN_SOURCES_DIR"] = str(tmp_path / "sources")
    staged = client.post("/api/meridian/artifacts", json={
        "business_unit": "USCS", "name": "lending vocabulary",
        "content": "ALIL means Active Lending In Force."}).json()
    assert staged["staged"]
    listing = client.get("/api/meridian/artifacts").json()
    assert "uscs_lending-vocabulary.md" in listing["staged"]
    del os.environ["MERIDIAN_SOURCES_DIR"]


def test_no_build_is_honest(tmp_path):
    os.environ["MERIDIAN_BUILDS_DIR"] = str(tmp_path / "empty")
    os.environ["MERIDIAN_GRAPH_DIR"] = str(tmp_path / "graph")
    empty = TestClient(create_app())
    payload = empty.get("/api/meridian/home").json()
    assert payload["available"] is False
    assert "laptop.py compile" in payload["reason"]
