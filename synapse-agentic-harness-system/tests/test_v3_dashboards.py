"""Synapse v3 — dashboards, kpis, diagrams (the artifact validator),
the literal hook on run_sql, and a dashboard through the real loop
with native tool calls."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
sys.path.insert(0, str(SILO))
KEY = "You are Synapse, an analytical colleague"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("v3dash")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v3d"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def _certified(build):
    return next(m for m in build.metrics
                if (m.get("status_served") or m.get("status"))
                == "certified")


class Extract:
    name = "frozen_extract"

    def run(self, sql, limit):
        return {"rows": [{"country_cd": "CA", "spend": 7.0}],
                "schema": [{"name": "country_cd", "type": "string"}]}


def _kit(compiled, tmp_path, **kw):
    from sahs.assistant.kit import build_kit
    from sahs.assistant.sandbox import prepare_workspace
    from sahs.assistant.state import AssistantState
    from sahs.assistant.store import AssistantStore
    build, _ = compiled
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    state = AssistantState()
    workspace = tmp_path / "ws"
    prepare_workspace(workspace, build.root)
    tools = build_kit(build, state, store=store,
                      session_id=session["id"], turn_id="t1",
                      workspace=workspace, **kw)
    return tools, state, store, session["id"], workspace


PROV = {"status": "composed",
        "meridian_line": "Composed from certified parts."}


# ─── the dashboard rules: rule 1 per tile ────────────────────


def test_dashboard_refuses_undisclosed_tiles_by_name(compiled):
    from sahs.assistant.artifacts import validate_artifact
    build, _ = compiled
    spec = {"panels": [
        {"type": "kpi", "title": "Spend",
         "spec": {"value": 100.0, "provenance": dict(PROV)}},
        {"type": "chart", "spec": {"kind": "bar", "series": [
            {"name": "s", "points": [["CA", 40.0]]}]}},
        {"type": "dashboard", "spec": {}},
    ]}
    normalized, problems = validate_artifact("dashboard", spec,
                                             build=build)
    assert normalized is None
    details = " | ".join(p["detail"] for p in problems)
    assert "panels[1] (chart)" in details
    assert any(p["code"] == "panel_type" for p in problems)


def test_dashboard_watermarks_follow_the_tiles(compiled):
    from sahs.assistant.artifacts import validate_artifact
    build, _ = compiled
    certified = _certified(build)
    spec = {"panels": [
        {"type": "kpi", "title": "Spend", "spec": {
            "value": 100.0, "unit": "USD",
            "provenance": {"status": "certified",
                           "metric_id": certified["id"],
                           "meridian_line": "Certified spend."}}},
        {"type": "chart", "title": "By country", "spec": {
            "kind": "bar",
            "series": [{"name": "s", "points": [["CA", 40.0]]}],
            "provenance": dict(PROV)}},
    ], "filters": [{"slot": "country", "options": ["US", "CA"]}],
        "notes": "one certified tile, one composed"}
    normalized, problems = validate_artifact("dashboard", spec,
                                             build=build)
    assert problems == []
    tiles = normalized["panels"]
    assert "watermark" not in tiles[0]["spec"]
    assert tiles[1]["spec"]["watermark"] == "EXPLORATORY"
    assert normalized["watermark"] == "EXPLORATORY"
    assert normalized["filters"][0]["active"] == "US"
    spec["panels"][1]["spec"]["provenance"]["facts"] = ["f1"]
    normalized, problems = validate_artifact(
        "dashboard", spec, build=build, facts=frozenset({"f1"}))
    assert problems == []
    assert "watermark" not in normalized


def test_diagram_kinds_validate(compiled):
    from sahs.assistant.artifacts import validate_artifact
    bad, problems = validate_artifact("diagram", {
        "kind": "graph", "nodes": [{"id": "a"}],
        "edges": [{"a": "a", "b": "ghost"}]})
    assert bad is None and problems[0]["code"] == "diagram_edges"
    good, problems = validate_artifact("diagram", {
        "kind": "graph",
        "nodes": [{"id": "metric:x", "kind": "metric",
                   "label": "Spend", "status": "certified"},
                  {"id": "table:t", "kind": "table", "label": "t"}],
        "edges": [{"a": "metric:x", "b": "table:t",
                   "rel": "bound_to"}]})
    assert problems == [] and good["kind"] == "graph"
    assert "watermark" not in good
    none, problems = validate_artifact("diagram", {"kind": "mermaid"})
    assert none is None and problems[0]["code"] == "diagram_source"
    mmd, problems = validate_artifact("diagram", {
        "kind": "mermaid", "source": "flowchart TD; A-->B"})
    assert problems == [] and mmd["source"].startswith("flowchart")


def test_kpi_is_a_first_class_artifact(compiled):
    from sahs.assistant.artifacts import validate_artifact
    build, _ = compiled
    refused, problems = validate_artifact("kpi", {"unit": "USD"})
    assert refused is None
    assert {p["code"] for p in problems} >= {"kpi_value",
                                             "provenance_missing"}
    certified = _certified(build)
    good, problems = validate_artifact("kpi", {
        "value": 12.5, "unit": "USD", "delta": -0.4,
        "provenance": {"status": "certified",
                       "metric_id": certified["id"],
                       "meridian_line": "Certified."}}, build=build)
    assert problems == [] and good["delta"] == -0.4


# ─── the artifact door: spec_json, versions, subgraph diagrams ─


def test_artifact_spec_json_teaches_and_versions(compiled, tmp_path):
    build, _ = compiled
    tools, state, store, sid, _ws = _kit(compiled, tmp_path)
    bad = tools["artifact"].fn("chart", "Broken", "{not json")
    assert "not valid JSON" in bad["error"]
    certified = _certified(build)
    spec = {"kind": "bar", "series": [{"name": "n",
                                       "points": [["a", 1.0]]}],
            "provenance": {"status": "certified",
                           "metric_id": certified["id"],
                           "meridian_line": "Certified."}}
    first = tools["artifact"].fn("chart", "Counts", json.dumps(spec))
    assert first["ok"] and first["version"] == 1
    ghost = tools["artifact"].fn("chart", "", json.dumps(spec),
                                 artifact_id="a_ghost")
    assert "no artifact" in ghost["error"]
    assert first["artifact_id"] in ghost["hint"]
    second = tools["artifact"].fn("chart", "", json.dumps(spec),
                                  artifact_id=first["artifact_id"])
    assert second["version"] == 2
    assert store.get_artifact(first["artifact_id"])["type"] == "chart"


def test_diagram_from_the_subgraph_lands_an_artifact(compiled,
                                                     tmp_path):
    build, _ = compiled
    tools, state, store, sid, _ws = _kit(compiled, tmp_path)
    certified = _certified(build)
    got = tools["artifact"].fn(
        "diagram", "What this chat used",
        json.dumps({"from_subgraph": [certified["id"]],
                    "caption": "the receipts"}))
    assert got.get("ok"), got
    row = store.get_artifact(got["artifact_id"])
    assert row["type"] == "diagram"
    assert row["spec"]["kind"] == "graph"
    assert row["spec"].get("caption") == "the receipts"
    kinds = {n["kind"] for n in row["spec"]["nodes"]}
    assert "metric" in kinds and "table" in kinds
    assert any(e["rel"] == "bound_to" for e in row["spec"]["edges"])
    assert got["artifact_id"] in state.artifacts_touched


# ─── the literal hook: deterministic, no prose rule needed ───


def test_run_sql_warns_on_unobserved_literals(compiled, tmp_path,
                                              monkeypatch):
    from sahs.assistant.hooks import literal_warnings
    from sahs.evals.substrate import DryRunOutcome

    class NoWarehouse:
        def dry_run(self, sql):
            return DryRunOutcome(valid=True, result_schema=None)

    import sahs.evals.substrate as substrate_module
    monkeypatch.setattr(substrate_module, "BQDryRun", NoWarehouse)
    build, _ = compiled
    tools, _state, _store, _sid, _ws = _kit(compiled, tmp_path)
    observed = tools["sample_values"].fn("gms_transaction",
                                         "country_cd").get("values")
    if not observed:
        pytest.skip("the fixture records no domain for country_cd")
    real = observed[0]["value"] if isinstance(observed[0], dict) \
        else observed[0]
    base = ("SELECT country_cd, sum(trans_usd_am) AS spend FROM "
            "dw.gms_transaction WHERE country_cd = '{}' "
            "GROUP BY country_cd")
    assert literal_warnings(build, base.format(real)) == []
    warned = literal_warnings(build, base.format("ZZ"))
    assert len(warned) == 1
    assert "'ZZ' is not among" in warned[0]
    assert "country_cd" in warned[0] and "closest" in warned[0]
    # no domain on record → no opinion
    assert literal_warnings(
        build, "SELECT 1 FROM dw.gms_transaction WHERE "
               "no_such_column = 'x'") == []
    # and the warning rides on the run_sql result the model sees
    got = tools["run_sql"].fn(base.format("ZZ"), mode="dry_run")
    assert any("'ZZ'" in w for w in got.get("warnings", [])), got


# ─── run mode: rows under the two limits, refusals taught ────


class _Runner:
    name = "bq_jobs_query"
    connection = None

    def __init__(self):
        self.seen = []

    def run(self, sql, limit):
        self.seen.append((sql, limit))
        return {"rows": [["CA", 7.0], ["US", 9.0], ["GB", 1.0]],
                "columns": ["country_cd", "spend"],
                "schema": [{"name": "country_cd", "type": "STRING"},
                           {"name": "spend", "type": "FLOAT"}],
                "bytes_processed": 1234}


def test_run_sql_run_mode_under_the_limits(compiled, tmp_path,
                                           monkeypatch):
    from sahs.canon.canonical import try_canon
    from sahs.evals.substrate import DryRunOutcome, StaticSubstrate
    monkeypatch.setenv("SAHS_ALLOW_LIVE", "1")
    monkeypatch.delenv("SAHS_LIVE_MAX_BYTES", raising=False)
    # a data project left in the process by an earlier test would
    # qualify the SQL and move it off the static substrate's key
    monkeypatch.delenv("BQ_DATA_PROJECT", raising=False)
    monkeypatch.delenv("LUMI_BQ_DATA_PROJECT", raising=False)
    sql = ("SELECT country_cd, sum(trans_usd_am) AS spend FROM "
           "dw.gms_transaction GROUP BY country_cd")
    runner = _Runner()
    tools, state, _store, _sid, ws = _kit(
        compiled, tmp_path, substrate=StaticSubstrate({}), runner=runner)
    got = tools["run_sql"].fn(sql, mode="run", limit=2)
    assert got["mode"] == "run" and got["row_count"] == 2
    assert got["rows"] == [{"country_cd": "CA", "spend": 7.0},
                           {"country_cd": "US", "spend": 9.0}]
    assert got["capped"] is True and got["limit"] == 2
    assert got["saved_as"] == "q1" and (ws / "q1.json").exists()
    assert "LIMIT 2" in got["note"] and got["scan_ceiling_bytes"] > 0
    assert runner.seen[0][1] == 2 and "LIMIT 2" in runner.seen[0][0]
    # the row cap has a ceiling of its own
    got = tools["run_sql"].fn(sql, mode="run", limit=5000)
    assert got["limit"] == 1000
    # priced above the scan ceiling: refused, the model's to narrow
    fp = try_canon(sql)[0].fp_expr
    pricey = StaticSubstrate({fp: DryRunOutcome(valid=True,
                                                bytes_processed=10**12)})
    tools, *_ = _kit(compiled, tmp_path / "b", substrate=pricey,
                     runner=runner)
    got = tools["run_sql"].fn(sql, mode="run")
    assert "cost_gate_budget" in got["error"]
    assert got["kind"] == "cost" and got["yours_to_fix"] is True
    assert "partition" in got["hint"]
    # live off: configuration, not the model's
    monkeypatch.delenv("SAHS_ALLOW_LIVE")
    tools, *_ = _kit(compiled, tmp_path / "c",
                     substrate=StaticSubstrate({}), runner=runner)
    got = tools["run_sql"].fn(sql, mode="run")
    assert "live_disabled" in got["error"]
    assert got["kind"] == "access" and got["yours_to_fix"] is False
    # dry_run is untouched, and the summary reads in bytes
    from sahs.assistant.loop import summarize
    got = tools["run_sql"].fn(sql, mode="dry_run")
    assert got["mode"] == "dry_run"
    assert summarize("run_sql", got).startswith("valid · would scan")
    assert summarize("run_sql", {"mode": "run", "row_count": 3,
                                 "capped": True, "limit": 3,
                                 "bytes_processed": 82_737_261_052}) \
        == "3 rows (LIMIT 3) · scanned 82.7 GB"


# ─── the dashboard through the real loop ─────────────────────


def test_dashboard_turn_through_the_loop(compiled, tmp_path):
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import ScriptedAgent
    build, _ = compiled
    certified = _certified(build)
    spec = {"panels": [
        {"type": "kpi", "title": "Net spend", "spec": {
            "value": 812.0, "unit": "USD",
            "provenance": {"status": "certified",
                           "metric_id": certified["id"],
                           "meridian_line": "Certified spend."}}},
        {"type": "chart", "title": "By country",
         "spec": {"kind": "bar", "series": [
             {"name": "spend", "points": [["CA", 300.0],
                                          ["US", 512.0]]}],
             "provenance": dict(PROV)}}],
        "filters": [{"slot": "period", "options": ["Q1", "Q2"],
                     "active": "Q2"}]}
    model = ScriptedAgent([
        [{"thought": "Assemble the dashboard: one certified tile, "
                     "one composed split."},
         {"call": {"name": "artifact",
                   "args": {"type": "dashboard", "title": "Q2 spend",
                            "spec_json": json.dumps(spec)}}}],
        [{"text": "The Q2 dashboard is in the panel; the country "
                  "split is composed and watermarked until checked."},
         {"call": {"name": "suggest_next",
                   "args": {"options": ["Check the country split"]}}}],
    ])
    tmp = Path(tempfile.mkdtemp())
    runtime = AssistantRuntime(builds_root=build.root.parent,
                               graph_root=tmp / "graph",
                               store_path=tmp / "chat.sqlite3",
                               model_factory=lambda budget: model)
    session = runtime.create_session()
    runtime.start_turn(session["id"], "Build me a Q2 dashboard")
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(0)
    landed = [e for e in events if e["ev"] == "artifact"]
    assert len(landed) == 1 and landed[0]["type"] == "dashboard"
    got = landed[0]["spec"]
    assert len(got["panels"]) == 2
    assert got["watermark"] == "EXPLORATORY"
    assert got["filters"][0]["active"] == "Q2"
    done = [e for e in events if e["ev"] == "turn_done"][-1]
    assert done["status"] == "answered"
    assert [e["ev"] for e in events if e["ev"] == "chips"] == ["chips"]
