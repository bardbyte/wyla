"""Agent Loop v1 §9.5 — delegate_scout and the exploratory lane.

The scout is read-only by construction (its kit drops every writer),
cannot delegate further (no worker spawns workers), reports compact,
and never raises. The exploratory lane is the same loop with a
frozen-extract runner attached at the runtime — nothing else changes.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
GOOD_SQL = ("SELECT part_dt, sum(trans_usd_am) AS acquirer_net_spend "
            "FROM dw.gms_transaction GROUP BY part_dt")
NO_MATCH = "quantum flux capacitance per moon phase"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("loop_scout")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "loopscout"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


class ScoutModel:
    """Routes the scout lane; a separate scripted queue from any
    navigator sharing the same transport."""

    def __init__(self, scout_steps=(), nav_steps=()):
        self.scout_steps = list(scout_steps)
        self.nav_steps = list(nav_steps)
        self.scout_prompts: list[str] = []

    def json(self, prompt, *, system="", temperature=0.0,
             max_tokens=1024):
        if "You are a read-only scout" in system:
            self.scout_prompts.append(prompt)
            return self.scout_steps.pop(0) if self.scout_steps \
                else None
        if "You navigate a governed data graph" in system:
            return self.nav_steps.pop(0) if self.nav_steps else None
        if "You compose ONE BigQuery SELECT" in system:
            return {"sql": GOOD_SQL, "why": "certified"}
        if "skeptical reviewer" in system:
            return {"grounded": True, "why": "traces"}
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        yield "the governed answer."


# ─── the scout alone ─────────────────────────────────────────


def test_wired_kit_offers_the_scout_with_its_pinned_description(
        compiled):
    from sahs.loop.tools import LoopState, toolkit
    build, _ = compiled
    kit = toolkit(build, LoopState(), scout=lambda q: {"summary": ""})
    spec = kit["delegate_scout"]
    assert spec.maps_to == "Task"
    assert spec.writes is False and spec.ends_turn is False
    assert spec.description == (
        "A read-only scout explores one question and returns a "
        "summary of at most 400 tokens.\nHard cap on its looks; it "
        "cannot write the plan, ask the user, or delegate further.")
    assert list(kit)[-1] == "delegate_scout"   # last, per §3's order


def test_scout_looks_reports_compact_and_records_its_reads(compiled):
    from sahs.loop.scout import SCOUT_SUMMARY_CHARS, run_scout
    build, _ = compiled
    model = ScoutModel(scout_steps=[
        {"think": "find the column", "tool": "grep_cards",
         "args": {"pattern": "country_cd"}},
        {"think": "read it in context", "tool": "read_card",
         "args": {"id": "table:gms_transaction"}},
        {"summary": "merchant country lives on dw.gms_transaction "
                    "as country_cd (card tables/dw__gms_transaction);"
                    " observed values include US, GB, CA. " + "x" * 2000},
    ])
    report = run_scout(build, model, "which table holds merchant "
                                     "country?")
    assert len(report["summary"]) <= SCOUT_SUMMARY_CHARS
    assert report["summary"].startswith("merchant country lives")
    assert report["steps"] == 3
    assert "tables/dw__gms_transaction" in report["cards_read"]


def test_scout_kit_is_read_only_and_cannot_delegate(compiled):
    from sahs.loop.scout import run_scout
    build, _ = compiled
    model = ScoutModel(scout_steps=[
        {"tool": "plan_set", "args": {"patch": {"grain": "day"}}},
        {"tool": "delegate_scout", "args": {"question": "recurse?"}},
        {"summary": "nothing settled"},
    ])
    report = run_scout(build, model, "anything")
    assert report["summary"] == "nothing settled"
    # both writer calls were refused as unknown tools, and the
    # teaching named only the read-only kit
    taught = "\n".join(model.scout_prompts)
    assert "unknown tool 'plan_set'" in taught
    assert "unknown tool 'delegate_scout'" in taught
    for absent in ("plan_set", "note", "ask_user"):
        assert f", {absent}," not in taught.split("unknown tool")[1]


def test_scout_never_raises_and_stops_honestly(compiled):
    from sahs.loop.scout import run_scout
    build, _ = compiled

    class Exploding:
        def json(self, *a, **k):
            raise RuntimeError("transport gone")

    report = run_scout(build, Exploding(), "anything")
    assert "model call failed" in report["summary"]

    babbler = ScoutModel(scout_steps=["not json", "still not json"])
    report = run_scout(build, babbler, "anything")
    assert "could not keep to the step protocol" in report["summary"]


# ─── the scout inside the loop ───────────────────────────────


def test_navigator_delegates_and_inherits_the_scouts_subgraph(
        compiled, tmp_path, monkeypatch):
    from sahs.ask import AskRuntime
    monkeypatch.setenv("SYNAPSE_NAVIGATE", "1")
    build, _ = compiled
    spend = next(m for m in build.metrics
                 if m["label"] == "Acquirer Net Spend"
                 and m["status"] == "certified")
    model = ScoutModel(
        nav_steps=[
            {"think": "send a scout for the table",
             "tool": "delegate_scout",
             "args": {"question": "which table holds spend?"}},
            {"tool": "plan_set",
             "args": {"patch": {"metric": spend["id"],
                                "grain": "transaction"}}},
            {"final": True},
        ],
        scout_steps=[
            {"tool": "read_card",
             "args": {"id": "table:gms_transaction"}},
            {"summary": "spend lives on dw.gms_transaction "
                        "(certified Acquirer Net Spend)"},
        ])
    runtime = AskRuntime(builds_root=build.root.parent,
                         graph_root=tmp_path / "graph",
                         store_path=tmp_path / "s.sqlite3",
                         model_factory=lambda budget: model)
    session = runtime.create_session("analyst")
    runtime.start_turn(session["id"], NO_MATCH)
    assert runtime.wait(session["id"], 30)
    events = runtime.runtime(session["id"]).bus.since(0)
    assert any(e["ev"] == "answer_payload" for e in events)
    scout_step = next(e for e in events if e["ev"] == "loop_step"
                      and e["tool"] == "delegate_scout")
    assert scout_step["summary"].startswith("scout (2 looks, 1 cards")
    # disclosure stays complete: the scout's read joined the turn's
    # sub-graph even though the navigator never opened that card
    done = next(e for e in events if e["ev"] == "loop_done")
    assert "tables/dw__gms_transaction" in \
        done["subgraph_used"]["cards_read"]


# ─── the exploratory lane: the loop with snapshot on ─────────


def test_runtime_threads_the_snapshot_runner_into_run_sql(
        compiled, tmp_path, monkeypatch):
    from sahs.ask import AskRuntime
    from sahs.evals.substrate import DryRunOutcome
    monkeypatch.setenv("SYNAPSE_NAVIGATE", "1")

    class NoWarehouse:
        def dry_run(self, sql):
            return DryRunOutcome(valid=True, result_schema=None)

    import sahs.evals.substrate as substrate_module
    monkeypatch.setattr(substrate_module, "BQDryRun", NoWarehouse)

    class EmptyExtract:
        name = "frozen_extract"

        def run(self, sql, limit):
            return {"rows": [], "schema": [
                {"name": "part_dt", "type": "date"}]}

    build, _ = compiled
    spend = next(m for m in build.metrics
                 if m["label"] == "Acquirer Net Spend"
                 and m["status"] == "certified")
    model = ScoutModel(nav_steps=[
        {"tool": "plan_set",
         "args": {"patch": {"metric": spend["id"],
                            "grain": "transaction"}}},
        {"think": "the plan is set: probe the frozen extract",
         "tool": "run_sql",
         "args": {"sql": GOOD_SQL, "mode": "snapshot"}},
        {"final": True},
    ])
    runtime = AskRuntime(builds_root=build.root.parent,
                         graph_root=tmp_path / "graph",
                         store_path=tmp_path / "s.sqlite3",
                         model_factory=lambda budget: model,
                         snapshot_runner=EmptyExtract())
    session = runtime.create_session("analyst")
    runtime.start_turn(session["id"], NO_MATCH)
    assert runtime.wait(session["id"], 30)
    events = runtime.runtime(session["id"]).bus.since(0)
    sql_step = next(e for e in events if e["ev"] == "loop_step"
                    and e["tool"] == "run_sql")
    assert "0 rows from frozen_extract" in sql_step["summary"]
    assert any(e["ev"] == "answer_payload" for e in events)
