"""Synapse V2.7 — intent understanding: business words resolve to
the business map, never to furniture.

The scenario that motivated this: "give me all GMNS metrics" browsed
tables. GMNS is a line of business the build knows by name — so the
digest now carries the business map, search_semantics answers a
business word with the AREA itself, and list_metrics turns "all X
metrics" into one call. The graders here run against the real fixture
build, whose GMNS rows are mined from the real archives.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
KEY = "You are Synapse, an analytical colleague"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("v2intent")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v2i"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
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


def test_list_metrics_speaks_business(kit):
    tools, build = kit
    got = tools["list_metrics"].fn("GMNS")
    assert got["count"] >= 3
    assert "Global Merchant" in got["scope"]
    labels = {m["label"] for m in got["metrics"]}
    assert "GMNS Merchant Spend" in labels
    # the variant spelling on a metric row still lands in the area
    assert any(m["lob"] == "Global Merchant & Network Svcs"
               for m in got["metrics"])
    # status and label filters keep working
    certified = tools["list_metrics"].fn("certified")
    assert certified["count"] >= 1
    assert all(m["status"] == "certified"
               for m in certified["metrics"])
    everything = tools["list_metrics"].fn()
    assert everything["count"] == min(len(build.metrics), 40)


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
    assert "list_metrics" in top["hint"]
    # a plain metric question is untouched by the lane
    plain = tools["search_semantics"].fn("certified spend by day")
    assert plain["results"][0]["kind"] != "line_of_business"


def test_digest_carries_the_business_map(compiled):
    from sahs.loop.digest import synapse_digest
    build, _ = compiled
    digest = synapse_digest(build)
    assert "## the business map" in digest
    assert "GMNS — Global Merchant & Network Services" in digest
    assert 'list_metrics("GMNS")' in digest


def test_intent_doctrine_reaches_the_assistant_prompt(compiled):
    from sahs.assistant.loop import system_prompt
    build, _ = compiled
    system = system_prompt(build, [], "")
    assert "Understand INTENT before reaching for a tool" in system
    assert "the business map" in system
    assert 'list_metrics("GMNS")' in system


def test_the_gmns_ask_through_the_real_loop(compiled, tmp_path):
    import tempfile

    from sahs.assistant import AssistantRuntime

    class Scripted:
        def __init__(self, steps):
            self.steps = list(steps)
            self.prompts: list[str] = []

        def json(self, prompt, *, system="", temperature=0.0,
                 max_tokens=1024):
            if KEY in system:
                self.prompts.append(prompt)
                return self.steps.pop(0) if self.steps else None
            return {}

        def stream(self, *a, **k):
            yield ""

    build, _ = compiled
    model = Scripted([
        {"think": "GMNS is a business area on the map — list its "
                  "metrics, not the tables",
         "tool": "list_metrics", "args": {"filter": "GMNS"}},
        {"say": "GMNS — Global Merchant & Network Services — has "
                "these governed metrics; the certified ones lead.",
         "done": True, "chips": ["chart GMNS Merchant Spend"]},
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
    step = [e for e in events if e["ev"] == "tool_step"][0]
    # the compacted result the model reasons over names the metrics
    assert "GMNS Merchant Spend [certified]" in step["summary"]
    assert "Global Merchant & Network Services" in step["summary"]
    done = [e for e in events if e["ev"] == "turn_done"][-1]
    assert done["status"] == "answered"
