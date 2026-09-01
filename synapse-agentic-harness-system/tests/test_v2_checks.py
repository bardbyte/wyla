"""Synapse v2 §13.2 — checks as citable facts, verify_answer, the
subgraph, template bindings, and rendering rule 2 with real teeth.

The headline test drives the whole §6 arc through the real loop: a
composed number is watermarked EXPLORATORY, the model runs the checks,
cites the passing facts, and the watermark comes off — never by
prose, always by evidence."""

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
    tmp = tmp_path_factory.mktemp("v2checks")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v2c"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


@pytest.fixture()
def kit(compiled, tmp_path):
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
                              workspace=workspace)
    return tools, state, build, workspace


# ─── the check primitives produce citable facts ──────────────


def test_part_whole_passes_and_fails_on_the_numbers(kit):
    from sahs.assistant.sandbox import save_rows
    tools, state, _build, ws = kit
    save_rows(ws, "q1", [{"c": "CA", "spend": 40.0},
                         {"c": "US", "spend": 60.0}])
    save_rows(ws, "q2", [{"spend": 100.0}])
    good = tools["check_part_whole"].fn("q1", "q2")
    assert good["passed"] is True and good["fact_id"] in state.facts
    save_rows(ws, "q3", [{"spend": 80.0}])
    bad = tools["check_part_whole"].fn("q1", "q3")
    assert bad["passed"] is False
    assert bad["fact_id"] not in state.facts
    assert "does not add up" in bad["detail"]


def test_checks_teach_when_results_are_missing(kit):
    tools, _state, _build, _ws = kit
    out = tools["check_part_whole"].fn("q7", "q8")
    assert "no saved result" in out["error"]
    assert "run_sql snapshot saves rows" in out["error"]


def test_crosscheck_and_coverage_read_the_saved_rows(kit):
    from sahs.assistant.sandbox import save_rows
    tools, state, _build, ws = kit
    save_rows(ws, "q1", [{"total": 50.0}])
    save_rows(ws, "q2", [{"total": 50.1}])
    agree = tools["check_crosscheck"].fn("q1", "q2")
    assert agree["passed"] is True
    save_rows(ws, "q3", [])
    empty = tools["check_coverage"].fn("q3")
    assert empty["passed"] is False
    assert "zero rows" in empty["detail"]
    save_rows(ws, "q4", [{"day": "d1", "n": 1}, {"day": None, "n": 2}])
    holed = tools["check_coverage"].fn("q4")
    assert holed["passed"] is False and "null" in holed["detail"]


def test_fanout_reads_the_join_topology(kit):
    tools, _state, build, _ws = kit
    out = tools["check_fanout"].fn(["gms_transaction",
                                    "wwcas_authorization"])
    assert out["kind"] == "fanout" and out["method"] == "topology"
    lonely = [t for t in build.schema
              if not any(t in (j["a"], j["b"]) for j in build.joins)]
    if lonely:
        none = tools["check_fanout"].fn(
            ["gms_transaction", build.short_table(lonely[0])])
        assert none["passed"] is False
        assert "not that raw rows join" in none["detail"]


def test_reconcile_is_structural_and_says_so(kit):
    tools, state, build, _ws = kit
    spend = next(m for m in build.metrics
                 if m["label"] == "Acquirer Net Spend"
                 and m["status"] == "certified")
    good = tools["check_reconcile"].fn(
        "SELECT part_dt, sum(trans_usd_am) AS s FROM "
        "dw.gms_transaction GROUP BY part_dt", spend["id"])
    assert good["passed"] is True and good["method"] == "structural"
    assert "NUMERIC" in good["hint"]        # honesty about the method
    bad = tools["check_reconcile"].fn(
        "SELECT part_dt, avg(trans_usd_am) AS s FROM "
        "dw.gms_transaction GROUP BY part_dt", spend["id"])
    assert bad["passed"] is False
    assert bad["fact_id"] not in state.facts


def test_verify_answer_judges_in_fresh_context(compiled, tmp_path):
    from sahs.assistant.sandbox import prepare_workspace
    from sahs.assistant.store import AssistantStore
    from sahs.assistant.tools import AssistantState, assistant_toolkit
    from sahs.evals.substrate import StaticSubstrate

    class Judge:
        def __init__(self, grounded):
            self.grounded = grounded
            self.systems = []

        def json(self, prompt, *, system="", temperature=0.0,
                 max_tokens=1024):
            self.systems.append(system)
            if "skeptical reviewer" in system:
                return {"grounded": self.grounded, "why": "checked"}
            return {}

    build, _ = compiled
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    for grounded in (True, False):
        state = AssistantState()
        ws = tmp_path / f"ws{grounded}"
        prepare_workspace(ws, build.root)
        judge = Judge(grounded)
        tools = assistant_toolkit(build, state, store=store,
                                  session_id=session["id"],
                                  turn_id="t", workspace=ws,
                                  model=judge,
                                  substrate=StaticSubstrate({}))
        fact = tools["verify_answer"].fn(
            "SELECT part_dt, sum(trans_usd_am) AS s FROM "
            "dw.gms_transaction GROUP BY part_dt",
            claim="spend rose day over day")
        assert fact["passed"] is grounded
        assert any("skeptical reviewer" in s for s in judge.systems)
        assert ("claim_grounded" if grounded
                else "claim_unsupported") in fact["detail"]


def test_subgraph_returns_the_receipts_as_data(kit):
    tools, state, build, _ws = kit
    spend = next(m for m in build.metrics
                 if m["label"] == "Acquirer Net Spend")
    out = tools["subgraph"].fn([spend["id"], "table:gms_transaction",
                                "table:wwcas_authorization"])
    kinds = {n["kind"] for n in out["nodes"]}
    assert "metric" in kinds and "table" in kinds
    rels = {e["rel"] for e in out["edges"]}
    assert "bound_to" in rels
    join_edges = [e for e in out["edges"] if e["rel"] == "joins"]
    assert join_edges and join_edges[0]["tier"] in (
        "certified", "witnessed", "candidate")
    # with no ids, it serves the recorded trace
    tools["read_card"].fn("table:gms_transaction")
    trace = tools["subgraph"].fn()
    assert any(n["kind"] == "table" for n in trace["nodes"])


def test_template_bindings_demote_the_witnessed_literal(kit):
    tools, _state, _build, _ws = kit
    out = tools["search_semantics"].fn("gb market", kind="concepts")
    hit = next(r for r in out["results"] if r["label"] == "gb_market")
    assert hit["template"]["column"] == "country_cd"
    assert hit["template"]["op"] == "="
    assert "example" in [k[:7] for k in hit["template"]]


# ─── rule 2, end to end through the loop ─────────────────────


def test_composed_number_sheds_watermark_only_by_evidence(compiled):
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.sandbox import save_rows

    composed_spec = {
        "kind": "bar",
        "series": [{"name": "share", "points": [["CA", 40.0],
                                                ["US", 60.0]]}],
        "provenance": {"status": "composed",
                       "meridian_line": "Composed: country slices "
                                        "of certified spend."}}

    class Model:
        def __init__(self, runtime_box):
            self.box = runtime_box
            self.n = 0

        def json(self, prompt, *, system="", temperature=0.0,
                 max_tokens=1024):
            if KEY not in system:
                return {}
            self.n += 1
            ws = self.box["ws"]
            if self.n == 1:      # unproven composed chart
                return {"tool": "artifact",
                        "args": {"type": "chart", "title": "Share",
                                 "spec": composed_spec}}
            if self.n == 2:      # seed the results a real turn would
                save_rows(ws, "q1", [{"c": "CA", "spend": 40.0},
                                     {"c": "US", "spend": 60.0}])
                save_rows(ws, "q2", [{"spend": 100.0}])
                return {"tool": "check_part_whole",
                        "args": {"breakdown": "q1", "total": "q2"}}
            if self.n == 3:      # cite the fact, shed the watermark
                aid = self.box["store"].list_artifacts(
                    self.box["sid"])[0]["artifact_id"]
                self.box["aid"] = aid
                spec = dict(composed_spec)
                spec["provenance"] = dict(spec["provenance"],
                                          facts=["f1"])
                return {"tool": "artifact_update",
                        "args": {"artifact_id": aid, "spec": spec}}
            return {"say": "Slices verified against the total.",
                    "done": True}

        def stream(self, *a, **k):
            yield ""

    build, _ = compiled
    tmp = Path(tempfile.mkdtemp())
    box = {}
    model = Model(box)
    runtime = AssistantRuntime(builds_root=build.root.parent,
                               graph_root=tmp / "graph",
                               store_path=tmp / "chat.sqlite3",
                               model_factory=lambda budget: model)
    session = runtime.create_session()
    box.update(ws=runtime.workspace(session["id"]),
               store=runtime.store, sid=session["id"])
    runtime.start_turn(session["id"], "share of spend by country, "
                                      "verified")
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(0)
    artifacts = [e for e in events if e["ev"] == "artifact"]
    # first version wore the watermark, second shed it by citation
    assert artifacts[0]["spec"]["watermark"] == "EXPLORATORY"
    v1 = runtime.store.get_artifact(box["aid"], 1)
    assert v1["spec"]["watermark"] == "EXPLORATORY"
    v2 = runtime.store.get_artifact(box["aid"], 2)
    assert v2 is not None
    assert "watermark" not in v2["spec"]
    assert v2["spec"]["provenance"]["facts_verified"] == ["f1"]
