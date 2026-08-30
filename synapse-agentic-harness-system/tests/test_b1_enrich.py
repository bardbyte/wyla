"""B1 gate: the enrichment loop — A5 blind tiers, never-clobber writes,
collision routing, halt semantics, Vertex env contract. No network:
the client is faked at the .generate seam."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.compiler.compile import compile_build           # noqa: E402
from sahs.enrich.client import parse_json_answer          # noqa: E402
from sahs.enrich.loop import (                            # noqa: E402
    grade_recovery,
    plan_metric_items,
    run_enrich,
)
from sahs.graph.quads import GraphDir                     # noqa: E402
from sahs.graph.validate import validate_graph            # noqa: E402
from sahs.tools.api import Build                          # noqa: E402

FX = SILO / "tests" / "fixtures"


def _compiled(tmp_path: Path) -> tuple[Path, Path]:
    graph_dir = tmp_path / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp_path / "run"), "--plain",
         "--run-id", "test_r1"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    builds = tmp_path / "builds"
    _dir, _manifest, failures = compile_build(graph_dir, builds)
    assert not failures
    return graph_dir, builds


class FakeVertex:
    """Stands in at the exact seam the loop uses: .generate, .usage,
    .connection.model."""

    def __init__(self, responder):
        self.responder = responder
        self.usage = {"calls": 0, "prompt_tokens": 0, "output_tokens": 0}
        self.connection = SimpleNamespace(model="fake-model")

    def generate(self, prompt: str, **_kw) -> str:
        self.usage["calls"] += 1
        return self.responder(prompt)


def _blind_perfect(build: Build):
    """Responder that recovers every certified name (keyed by the SQL
    the prompt carries — the label itself is withheld, as A5 demands)."""
    by_sql = {m["canonical_sql"]: m["label"] for m in build.metrics
              if m.get("status") in ("certified", "pending")
              and m.get("label")}

    def answer(prompt: str) -> str:
        if "lost its name" in prompt:
            for sql, label in by_sql.items():
                if sql and sql in prompt:
                    return json.dumps({"name": label, "confidence": 0.9})
            return json.dumps({"name": "unknown", "confidence": 0.1})
        if "mined metric was observed" in prompt:
            tag = abs(hash(prompt)) % 10_000_000
            return json.dumps({
                "question": f"What is mined measure {tag}?",
                "grain": "per day", "confidence": 0.8, "caveat": ""})
        return json.dumps({
            "description": "A test concept meaning.",
            "disambiguation": "", "confidence": 0.7})
    return answer


def test_recovery_grader_and_json_parsing():
    assert grade_recovery("GMNS Merchant Spend", "merchant spend (GMNS)")
    assert not grade_recovery("GMNS Merchant Spend", "customer count")
    assert parse_json_answer('```json\n{"a": 1}\n```') == {"a": 1}
    assert parse_json_answer("not json") is None
    assert parse_json_answer('["list"]') is None


def test_plan_targets_only_blank_metrics(tmp_path):
    graph_dir, builds = _compiled(tmp_path)
    build = Build.open(builds)
    items = plan_metric_items(build, GraphDir(graph_dir).fold_nodes(),
                              limit=500)
    assert items                                # mined metrics need work
    planned = {i["id"] for i in items}
    for row in build.metrics:
        if row.get("question") and row.get("grain"):
            assert row["id"] not in planned     # catalog never re-asked
    supports = [i["support"] for i in items]
    assert supports == sorted(supports, reverse=True)


def test_enrich_writes_witnessed_and_never_clobbers(tmp_path):
    graph_dir, builds = _compiled(tmp_path)
    build = Build.open(builds)
    fake = FakeVertex(_blind_perfect(build))
    report = run_enrich(
        graph_root=graph_dir, builds_root=builds,
        out_dir=tmp_path / "b1", run_id="enrich_r1", limit=5,
        client=fake, log=lambda _m: None)
    assert report["blind"]["tier"] == "batch"   # perfect recovery
    assert report["metrics_enriched"] >= 1
    assert report["concepts_enriched"] >= 1

    graph = GraphDir(graph_dir)
    nodes = graph.fold_nodes()
    enriched = [n for n in nodes.values()
                if n.props.get("question_enriched")]
    assert enriched
    certified = next(n for nid, n in nodes.items()
                     if n.props.get("question_answered"))
    assert not certified.props.get("question_enriched")   # never clobber
    assert validate_graph(graph_dir).ok         # writes keep the gate

    # re-plan: the graph fold guard drops already-enriched nodes even
    # BEFORE a recompile — the next run works the NEXT slice, never
    # re-asking what it already wrote
    enriched_ids = {nid for nid, n in nodes.items()
                    if n.props.get("question_enriched")}
    replanned = {i["id"] for i in plan_metric_items(
        build, nodes, limit=500)}
    assert enriched_ids and not (enriched_ids & replanned)

    # recompile: enriched question serves with its source flagged and
    # the card says unreviewed
    build_dir, _manifest, failures = compile_build(graph_dir,
                                                   tmp_path / "builds2")
    assert not failures
    rows = [json.loads(x) for x in
            (build_dir / "indexes" / "metrics.jsonl"
             ).read_text().splitlines()]
    filled = [r for r in rows if r.get("question_source")
              == "llm_enriched"]
    assert filled
    card = (build_dir / "cards" / "metrics"
            / f"{filled[0]['fp']}.md").read_text()
    assert "[prov:llm_enriched·unreviewed]" in card
    dmp_rows = [r for r in rows if r.get("question_source") == "dmp"]
    assert dmp_rows                             # catalog flag intact


def test_collision_routes_to_review_not_write(tmp_path):
    graph_dir, builds = _compiled(tmp_path)
    build = Build.open(builds)
    certified_question = next(
        m["question"] for m in build.metrics
        if m.get("status") == "certified" and m.get("question"))
    blind = _blind_perfect(build)

    def responder(prompt: str) -> str:
        if "mined metric was observed" in prompt:
            return json.dumps({"question": certified_question,
                               "grain": "per day", "confidence": 0.9,
                               "caveat": ""})
        return blind(prompt)

    report = run_enrich(
        graph_root=graph_dir, builds_root=builds,
        out_dir=tmp_path / "b1", run_id="enrich_r1", limit=3,
        targets=("metrics",), client=FakeVertex(responder),
        log=lambda _m: None)
    assert report["collisions"] == report["planned_metrics"] > 0
    assert report["metrics_enriched"] == 0
    graph = GraphDir(graph_dir)
    reviews = [n for nid, n in graph.fold_nodes().items()
               if nid.startswith("review:")
               and n.props.get("kind") == "metric_conflict"
               and n.prov.witness == "llm_enriched"]
    assert reviews
    assert validate_graph(graph_dir).ok         # [14] satisfied


def test_a5_halt_writes_nothing(tmp_path):
    graph_dir, builds = _compiled(tmp_path)
    before = len(list(GraphDir(graph_dir).fold_nodes()))

    def responder(prompt: str) -> str:
        return json.dumps({"name": "zzz nonsense", "question": "q",
                           "grain": "g", "confidence": 0.1})

    report = run_enrich(
        graph_root=graph_dir, builds_root=builds,
        out_dir=tmp_path / "b1", run_id="enrich_r1", limit=5,
        client=FakeVertex(responder), log=lambda _m: None)
    assert report["blind"]["tier"] == "halt"
    assert report["metrics_enriched"] == 0
    assert report["concepts_enriched"] == 0
    assert len(list(GraphDir(graph_dir).fold_nodes())) == before


def test_vertex_env_contract_is_typed_and_separate(tmp_path,
                                                   monkeypatch):
    from sahs.util.auth import AuthError, VertexConnection
    empty_env = tmp_path / "empty.env"
    empty_env.write_text("", encoding="utf-8")
    monkeypatch.setenv("SAHS_ENV_FILE", str(empty_env))
    for name in ("VERTEX_PROJECT_ID", "LUMI_VERTEX_PROJECT",
                 "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION",
                 "VERTEX_MODEL", "LUMI_VERTEX_MODEL", "GEMINI_MODEL",
                 "VERTEX_LOCATION", "LUMI_VERTEX_LOCATION",
                 "VERTEX_API_BASE_URL",
                 "LUMI_VERTEX_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS"):
        monkeypatch.delenv(name, raising=False)
    # the BQ project must NOT leak into the Vertex contract
    monkeypatch.setenv("BQ_PROJECT_ID", "bq-project-not-vertex")
    try:
        VertexConnection.from_env()
        raise AssertionError("missing Vertex project must be typed")
    except AuthError as e:
        assert "VERTEX_PROJECT_ID" in str(e)
        assert "never" in str(e).lower()
    # the proven ADK laptop env resolves as-is: GOOGLE_CLOUD_PROJECT
    # is the VERTEX project there, and the defaults are the proven
    # global + gemini-3.1-pro-preview pair
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "prj-d-ea-poc")
    key = tmp_path / "k.json"
    key.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("LUMI_VERTEX_SA_KEY", str(key))
    connection = VertexConnection.from_env()
    assert connection.project == "prj-d-ea-poc"
    assert connection.location == "global"
    assert connection.model == "gemini-3.1-pro-preview"
    assert connection.endpoint == "https://aiplatform.googleapis.com"
    # silo-first names win; a regional location derives its own host
    monkeypatch.setenv("VERTEX_PROJECT_ID", "vertex-proj")
    monkeypatch.setenv("VERTEX_LOCATION", "us-central1")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-test")
    connection = VertexConnection.from_env()
    assert connection.project == "vertex-proj"
    assert connection.model == "gemini-test"
    assert connection.endpoint == \
        "https://us-central1-aiplatform.googleapis.com"


def test_vertex_rides_the_proxy_bq_bypasses_it(tmp_path, monkeypatch):
    """The two planes have OPPOSITE proven network contracts. BQ's PSC
    endpoint needs the NO_PROXY injection; Vertex rides the corporate
    proxy exactly like check_vertex_gemini.py did — injecting
    googleapis into NO_PROXY sent the Vertex OAuth call direct and the
    corporate network blackholed it (timeout at the auth step)."""
    from sahs.util.auth import (
        VertexConnection,
        configure_network,
        configure_vertex_network,
    )
    empty_env = tmp_path / "empty.env"
    empty_env.write_text("", encoding="utf-8")
    monkeypatch.setenv("SAHS_ENV_FILE", str(empty_env))
    for name in ("NO_PROXY", "no_proxy", "VERTEX_DISABLE_PROXY",
                 "VERTEX_NO_PROXY_GOOGLE", "BQ_DISABLE_PROXY",
                 "BQ_FORCE_PROXY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.corp:8080")
    key = tmp_path / "k.json"
    key.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("LUMI_VERTEX_SA_KEY", str(key))
    monkeypatch.setenv("VERTEX_PROJECT_ID", "prj-d-ea-poc")

    # Vertex default: proxy untouched, NOTHING injected into NO_PROXY
    connection = VertexConnection.from_env()
    assert "googleapis" not in os.environ.get("NO_PROXY", "")
    assert os.environ["HTTPS_PROXY"] == "http://proxy.corp:8080"
    summary = configure_vertex_network(connection.endpoint)
    assert "via corporate proxy" in summary["proxy"]

    # the BQ contract still injects (PSC needs direct)
    configure_network("https://bigquery-prod.p.googleapis.com")
    assert "oauth2.googleapis.com" in os.environ["NO_PROXY"]

    # knobs: opt back into injection, or drop the proxy entirely
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("VERTEX_NO_PROXY_GOOGLE", "1")
    configure_vertex_network("https://aiplatform.googleapis.com")
    assert "aiplatform.googleapis.com" in os.environ["NO_PROXY"]
    monkeypatch.delenv("VERTEX_NO_PROXY_GOOGLE")
    monkeypatch.setenv("VERTEX_DISABLE_PROXY", "1")
    configure_vertex_network("https://aiplatform.googleapis.com")
    assert "HTTPS_PROXY" not in os.environ
