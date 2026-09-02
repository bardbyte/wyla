"""Synapse v3 — intent understanding: business words resolve to the
business map, never to furniture.

The scenario that motivated this: "give me all GMNS metrics" browsed
tables. GMNS is a line of business the build knows by name — so the
digest carries the business map, the search door answers a business
word with the AREA itself, and kind=list turns "all X metrics" into
one call whose WHOLE result reaches the model. The graders run
against the real fixture build, whose GMNS rows are mined from the
real archives.
"""

from __future__ import annotations

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
    tmp = tmp_path_factory.mktemp("v3intent")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v3i"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


@pytest.fixture()
def kit(compiled):
    from sahs.loop.tools import LoopState, toolkit
    build, _ = compiled
    return toolkit(build, LoopState()), build


@pytest.fixture()
def v3(compiled, tmp_path):
    from sahs.assistant.kit import build_kit
    from sahs.assistant.sandbox import prepare_workspace
    from sahs.assistant.state import AssistantState
    from sahs.assistant.store import AssistantStore
    build, _ = compiled
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    workspace = tmp_path / "ws"
    prepare_workspace(workspace, build.root)
    return build_kit(build, AssistantState(), store=store,
                     session_id=session["id"], turn_id="t1",
                     workspace=workspace), build


def test_list_metrics_speaks_business(kit):
    tools, build = kit
    got = tools["list_metrics"].fn("GMNS")
    assert got["count"] >= 3
    assert "Global Merchant" in got["scope"]
    labels = {m["label"] for m in got["metrics"]}
    assert "GMNS Merchant Spend" in labels
    assert any(m["lob"] == "Global Merchant & Network Svcs"
               for m in got["metrics"])
    certified = tools["list_metrics"].fn("certified")
    assert certified["count"] >= 1
    assert all(m["status"] == "certified"
               for m in certified["metrics"])
    everything = tools["list_metrics"].fn()
    assert everything["count"] == min(len(build.metrics), 40)


def test_area_membership_reaches_through_the_tables(kit):
    # §12: only a few metric rows carry the LOB string; "all GMNS
    # metrics" must mean the metrics on GMNS tables as well
    from sahs.loop.tools import _metric_in_lob
    tools, build = kit
    lob = next(r for r in build.lob if r["code"] == "GMNS")
    assert lob.get("tables"), "the fixture maps GMNS to tables"
    on_table = [m for m in build.metrics
                if str(m.get("table", "")) in set(lob["tables"])]
    assert on_table and all(_metric_in_lob(m, lob) for m in on_table)
    listed = {m["id"] for m in tools["list_metrics"].fn("GMNS")["metrics"]}
    assert listed >= {m["id"] for m in on_table[:5]}


def test_list_metrics_teaches_the_map_on_a_miss(kit):
    tools, _build = kit
    miss = tools["list_metrics"].fn("payments")
    assert "no metrics match" in miss["error"]
    assert "GMNS (Global Merchant & Network Services)" in miss["hint"]


def test_search_answers_a_business_word_with_the_area(kit):
    tools, _build = kit
    hits = tools["search_semantics"].fn(
        "give me all GMNS metrics")["results"]
    top = hits[0]
    assert top["kind"] == "line_of_business"
    assert top["code"] == "GMNS"
    assert top["metrics"] >= 3
    assert "not a table" in top["hint"]
    plain = tools["search_semantics"].fn("certified spend by day")
    assert plain["results"][0]["kind"] != "line_of_business"


def test_the_one_search_door(v3):
    tools, _build = v3
    area = tools["search"].fn("give me all GMNS metrics")
    assert area["results"][0]["kind"] == "line_of_business"
    listed = tools["search"].fn("GMNS", kind="list")
    assert listed["count"] >= 3 and "Global Merchant" in listed["scope"]
    exact = tools["search"].fn("trans_usd_am", kind="exact")
    assert exact["count"] >= 1
    # a miss by meaning falls back to the token grep, with a hint
    fallback = tools["search"].fn("zzqx")
    assert fallback.get("count", 0) == 0 or fallback.get("results") == []


def test_digest_carries_the_business_map(compiled):
    from sahs.loop.digest import synapse_digest
    build, _ = compiled
    digest = synapse_digest(build)
    assert "## the business map" in digest
    assert "GMNS — Global Merchant & Network Services" in digest
    assert 'list_metrics("GMNS")' in digest            # the v1 kit
    v3 = synapse_digest(build, list_hint='search("GMNS", kind="list")')
    assert 'search("GMNS", kind="list")' in v3
    assert "list_metrics" not in v3


def test_intent_doctrine_reaches_the_assistant_prompt(compiled):
    from sahs.assistant.loop import system_prompt
    build, _ = compiled
    system = system_prompt(build)
    assert "Understand the intent before reaching for a tool" in system
    assert "the business map" in system
    assert 'search("GMNS", kind="list")' in system


def test_the_gmns_ask_through_the_real_loop(compiled):
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import ScriptedAgent

    build, _ = compiled
    model = ScriptedAgent([
        [{"thought": "GMNS is a business area on the map — list its "
                     "metrics, not the tables."},
         {"call": {"name": "search",
                   "args": {"query": "GMNS", "kind": "list"}}}],
        [{"text": "GMNS — Global Merchant & Network Services — has "
                  "these governed metrics; the certified ones lead."},
         {"call": {"name": "suggest_next",
                   "args": {"options": ["chart GMNS Merchant Spend"]}}}],
    ])
    tmp = Path(tempfile.mkdtemp())
    runtime = AssistantRuntime(builds_root=build.root.parent,
                               graph_root=tmp / "graph",
                               store_path=tmp / "chat.sqlite3",
                               model_factory=lambda budget: model)
    session = runtime.create_session()
    runtime.start_turn(session["id"], "give me all GMNS metrics")
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(0)
    thinking = [e for e in events if e["ev"] == "thinking"][0]
    assert "business area" in thinking["delta"]
    step = [e for e in events if e["ev"] == "tool_step"][0]
    assert step["tool"] == "search"
    assert "Global Merchant & Network Services" in step["summary"]
    # the model reasoned over the WHOLE listing, not three labels
    response = next(
        p["functionResponse"]["response"]
        for c in model.calls[-1]["contents"]
        for p in c["parts"] if "functionResponse" in p)
    labels = {m["label"] for m in response["metrics"]}
    assert "GMNS Merchant Spend" in labels
    assert response["count"] >= 3
    done = [e for e in events if e["ev"] == "turn_done"][-1]
    assert done["status"] == "answered"
    assert len(model.calls) == 2                # one interaction
