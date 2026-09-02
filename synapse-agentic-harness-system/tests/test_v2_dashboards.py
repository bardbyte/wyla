"""Synapse v2 §13.4 — dashboards with per-tile disclosure, diagrams,
and the magic pair (whatif / compare) plus constellation.

The dashboard rules are rule 1 applied per TILE: an undisclosed panel
is refused by name, a composed tile keeps its own watermark, and the
dashboard wears one whenever any tile does. Filters are declared in
schema and re-run through the conversation — whatif is the tool a
filter pick lands on, never a hidden query path.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
KEY = "You are Synapse, an analytical colleague"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("v2dash")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v2d"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def _certified(build):
    return next(m for m in build.metrics
                if (m.get("status_served") or m.get("status"))
                == "certified")


class Scripted:
    def __init__(self, steps=()):
        self.steps = list(steps)
        self.prompts: list[str] = []

    def json(self, prompt, *, system="", temperature=0.0,
             max_tokens=1024):
        if KEY in system:
            self.prompts.append(prompt)
            return self.steps.pop(0) if self.steps else None
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        yield ""


class Extract:
    name = "frozen_extract"

    def run(self, sql, limit):
        return {"rows": [{"country": "CA", "spend": 7.0}],
                "schema": [{"name": "country", "type": "string"}]}


def _kit(compiled, tmp_path, **kw):
    from sahs.assistant.sandbox import prepare_workspace
    from sahs.assistant.store import AssistantStore
    from sahs.assistant.tools import AssistantState, assistant_toolkit
    build, _ = compiled
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    state = AssistantState()
    workspace = tmp_path / "ws"
    prepare_workspace(workspace, build.root)
    tools = assistant_toolkit(build, state, store=store,
                              session_id=session["id"], turn_id="t1",
                              workspace=workspace, **kw)
    return tools, state, store, session["id"], workspace


PROV = {"status": "composed",
        "meridian_line": "Composed from certified parts."}


# ─── the dashboard rules: rule 1 per tile ────────────────────


def test_dashboard_refuses_undisclosed_tiles_by_name(compiled):
    sys.path.insert(0, str(SILO))
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
    assert "panels[1] (chart)" in details      # the tile is named
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
    assert "watermark" not in tiles[0]["spec"]           # certified
    assert tiles[1]["spec"]["watermark"] == "EXPLORATORY"
    assert normalized["watermark"] == "EXPLORATORY"      # inherited
    assert normalized["filters"][0]["active"] == "US"    # defaulted
    # a cited passing fact sheds the tile AND the dashboard mark
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


# ─── the magic pair ──────────────────────────────────────────


def test_whatif_changes_one_slot(compiled, tmp_path, monkeypatch):
    from sahs.evals.substrate import DryRunOutcome

    class NoWarehouse:
        def dry_run(self, sql):
            return DryRunOutcome(valid=True, result_schema=None)

    import sahs.evals.substrate as substrate_module
    monkeypatch.setattr(substrate_module, "BQDryRun", NoWarehouse)
    tools, state, _store, _sid, ws = _kit(
        compiled, tmp_path, snapshot_runner=Extract())
    sql = ("SELECT country_cd, sum(trans_usd_am) AS spend FROM "
           "dw.gms_transaction WHERE country_cd = 'US' "
           "GROUP BY country_cd")
    miss = tools["whatif"].fn(sql, "'GB'", "'CA'")
    assert "does not appear" in miss["error"]
    got = tools["whatif"].fn(sql, "'US'", "'CA'")
    assert got.get("saved_as") == "q1"
    assert "'CA'" in got["sql"] and "'US'" not in got["sql"]
    assert got["changed"]["occurrences"] == 1
    assert (ws / "q1.json").exists()


def test_compare_aligns_two_results(compiled, tmp_path):
    from sahs.assistant.sandbox import save_rows
    tools, _state, _store, _sid, ws = _kit(compiled, tmp_path)
    missing = tools["compare"].fn("q1", "q2")
    assert "no saved result" in missing["error"]
    save_rows(ws, "q1", [{"country": "CA", "spend": 40.0},
                         {"country": "US", "spend": 60.0}])
    save_rows(ws, "q2", [{"country": "CA", "spend": 44.0},
                         {"country": "MX", "spend": 5.0}])
    got = tools["compare"].fn("q1", "q2", labels=["july", "august"])
    assert got["aligned_on"] == ["country"]
    ca = next(r for r in got["frame"] if r["country"] == "CA")
    assert ca["july"] == 40.0 and ca["august"] == 44.0
    assert ca["delta"] == 4.0 and ca["pct"] == 0.1
    assert got["only_in_a"] == 1 and got["only_in_b"] == 1
    assert got["totals"]["delta"] == -51.0
    assert "check_crosscheck" in got["hint"]     # aligns ≠ verifies


def test_constellation_lands_a_diagram_artifact(compiled, tmp_path):
    build, _ = compiled
    tools, state, store, sid, _ws = _kit(compiled, tmp_path)
    empty = tools["constellation"].fn()
    assert "nothing to draw" in empty["error"]
    certified = _certified(build)
    got = tools["constellation"].fn(ids=[certified["id"]])
    assert got.get("ok"), got
    row = store.get_artifact(got["artifact_id"])
    assert row["type"] == "diagram"
    assert row["spec"]["kind"] == "graph"
    kinds = {n["kind"] for n in row["spec"]["nodes"]}
    assert "metric" in kinds and "table" in kinds
    assert any(e["rel"] == "bound_to" for e in row["spec"]["edges"])
    assert got["artifact_id"] in state.artifacts_touched


# ─── the dashboard through the real loop ─────────────────────


def test_dashboard_turn_through_the_loop(compiled, tmp_path):
    from sahs.assistant import AssistantRuntime
    build, _ = compiled
    certified = _certified(build)
    model = Scripted([
        {"think": "assemble the dashboard",
         "tool": "artifact",
         "args": {"type": "dashboard", "title": "Q2 spend",
                  "spec": {"panels": [
                      {"type": "kpi", "title": "Net spend", "spec": {
                          "value": 812.0, "unit": "USD",
                          "provenance": {
                              "status": "certified",
                              "metric_id": certified["id"],
                              "meridian_line": "Certified spend."}}},
                      {"type": "chart", "title": "By country",
                       "spec": {"kind": "bar", "series": [
                           {"name": "spend",
                            "points": [["CA", 300.0],
                                       ["US", 512.0]]}],
                           "provenance": dict(PROV)}}],
                      "filters": [{"slot": "period",
                                   "options": ["Q1", "Q2"],
                                   "active": "Q2"}]}}},
        {"say": "The Q2 dashboard is in the panel; the country "
                "split is composed and watermarked until checked.",
         "done": True, "chips": ["Check the country split"]},
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
    spec = landed[0]["spec"]
    assert len(spec["panels"]) == 2
    assert spec["watermark"] == "EXPLORATORY"    # the composed tile
    assert spec["filters"][0]["active"] == "Q2"
    done = [e for e in events if e["ev"] == "turn_done"][-1]
    assert done["status"] == "answered"
