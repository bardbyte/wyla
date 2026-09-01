"""Agent Loop v1 §9.2 — the loop engine: one door, three honest exits.

The deterministic opening stays free (classify shortcut, delta
resolve, instant mutations); the model-driven middle engages behind
``SYNAPSE_NAVIGATE`` when the opening cannot complete the plan; every
answered exit goes through the same ``finish()`` the fast path uses.
The model transport is scripted (A1 discipline: transport replaced,
data never); every step the "model" takes runs against the real
compiled fixture build.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
GOOD_SQL = ("SELECT part_dt, sum(trans_usd_am) AS acquirer_net_spend "
            "FROM dw.gms_transaction GROUP BY part_dt")
NO_MATCH = "quantum flux capacitance per moon phase"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("loop_engine")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "loopeng"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def _spend(build):
    return next(m for m in build.metrics
                if m["label"] == "Acquirer Net Spend"
                and m["status"] == "certified")


class Navigator:
    """Scripted transport with a navigator lane: each queued step is
    what the model 'decides' next. Everything else routes like the
    capability suite's ScriptedTransport."""

    def __init__(self, steps=()):
        self.steps = list(steps)
        self.prompts: list[str] = []      # navigator prompts, in order
        self.calls: list[str] = []

    def json(self, prompt, *, system="", temperature=0.0,
             max_tokens=1024):
        if "You navigate a governed data graph" in system:
            self.calls.append("navigate")
            self.prompts.append(prompt)
            return self.steps.pop(0) if self.steps else None
        if "You classify ONE turn" in system:
            self.calls.append("classify")
            if "canada" in prompt.lower():
                return {"kind": "mutate", "why": "a filter changed",
                        "edits": [{"slot": "filters.country",
                                   "value": "CA"}]}
            return {"kind": "new_question", "question": prompt,
                    "why": "fresh subject", "edits": []}
        if "You compose ONE BigQuery SELECT" in system:
            self.calls.append("sql")
            return {"sql": GOOD_SQL, "why": "certified expression"}
        if "skeptical reviewer" in system:
            self.calls.append("judge")
            return {"grounded": True, "why": "claims trace"}
        if "conversational voice" in system:
            self.calls.append("chat")
            return {"reply": "Hello."}
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        self.calls.append("stream")
        yield "the governed answer."


def _run(compiled, model, turns, *, env_nav=True, monkeypatch=None):
    from sahs.ask import AskRuntime
    if monkeypatch is not None:
        if env_nav:
            monkeypatch.setenv("SYNAPSE_NAVIGATE", "1")
        else:
            monkeypatch.delenv("SYNAPSE_NAVIGATE", raising=False)
    build, _ = compiled
    tmp = Path(tempfile.mkdtemp())
    runtime = AskRuntime(builds_root=build.root.parent,
                         graph_root=tmp / "graph",
                         store_path=tmp / "s.sqlite3",
                         model_factory=lambda budget: model)
    session = runtime.create_session("analyst")
    for turn in turns:
        text, choice = (turn if isinstance(turn, tuple) else (turn, None))
        runtime.start_turn(session["id"], text, choice=choice)
        assert runtime.wait(session["id"], 30)
    rt = runtime.runtime(session["id"])
    return rt.bus.since(0), runtime, session["id"]


def _by(events, name):
    return [e for e in events if e["ev"] == name]


def _status(events, nth=-1):
    return _by(events, "turn_done")[nth]["status"]


# ─── the family and the flag ─────────────────────────────────


def test_loop_events_join_the_pinned_family():
    sys.path.insert(0, str(SILO))
    from sahs.ask.events import EVENTS
    order = list(EVENTS)
    for name in ("loop_started", "loop_step", "loop_artifact",
                 "loop_done"):
        assert name in order
    # they sit between the resolver and the clarify exit, where a
    # healthy navigating turn actually emits them
    assert order.index("resolve_result") < order.index("loop_started")
    assert order.index("loop_done") < order.index("clarify_request")


def test_navigate_off_keeps_the_deterministic_chips(compiled,
                                                    monkeypatch):
    model = Navigator()
    events, _rt, _sid = _run(compiled, model, [NO_MATCH],
                             env_nav=False, monkeypatch=monkeypatch)
    clarify = _by(events, "clarify_request")[0]
    assert clarify["slot"] == "metric"      # T3's pinned lineup
    assert "navigate" not in model.calls
    assert not _by(events, "loop_started")
    assert _status(events) == "clarify"


# ─── navigation: find, bind, finish through the one exit ─────


def test_navigation_finds_binds_and_answers(compiled, monkeypatch):
    build, _ = compiled
    spend = _spend(build)
    model = Navigator(steps=[
        {"think": "nothing bound: look for spend metrics",
         "tool": "search_semantics", "args": {"query": "spend"}},
        {"think": "bind the certified one",
         "tool": "plan_set", "args": {"patch": {"metric": spend["id"]}}},
        {"think": "done", "final": True},          # refused: no grain
        {"think": "the card has no grain: one row per transaction",
         "tool": "plan_set", "args": {"patch": {"grain": "transaction"}}},
        {"think": "metric, table, grain bound", "final": True},
    ])
    events, runtime, sid = _run(compiled, model, [NO_MATCH],
                                monkeypatch=monkeypatch)
    assert _status(events) == "answered"
    assert _by(events, "answer_payload"), "no answer reached the page"
    # the loop's record: started once, three tool steps, done=answered
    assert len(_by(events, "loop_started")) == 1
    steps = _by(events, "loop_step")
    assert [s["tool"] for s in steps] == ["search_semantics",
                                          "plan_set", "plan_set"]
    assert len(_by(events, "loop_artifact")) == 3
    done = _by(events, "loop_done")[0]
    assert done["outcome"] == "answered" and done["steps"] == 5
    # the refused final taught in-context, and the model recovered
    assert any("final refused" in p for p in model.prompts)
    # the answer went through the SAME finish: contract then verdict
    assert _by(events, "contract_ready") and _by(events,
                                                 "verify_verdict")
    # and the stored plan carries the binding and the grain
    plan = runtime.store.latest_plan(sid)["plan"]
    assert plan["metric_id"] == spend["id"]
    assert plan["grain"] == "transaction"
    # disclosure came along for free: the loop reported its sub-graph
    assert "subgraph_used" in done


def test_mutations_stay_instant_even_with_the_flag_on(compiled,
                                                      monkeypatch):
    build, _ = compiled
    spend = _spend(build)
    model = Navigator(steps=[
        {"tool": "plan_set", "args": {"patch": {"metric": spend["id"],
                                                "grain": "transaction"}}},
        {"final": True},
    ])
    events, _rt, _sid = _run(
        compiled, model, [NO_MATCH, "same for Canada"],
        monkeypatch=monkeypatch)
    navigations = [e for e in _by(events, "loop_started")]
    assert len(navigations) == 1, "the mutation re-entered the loop"
    # turn 2: classify (model) + deterministic apply/resolve + finish,
    # no navigator step — the fast path is the loop's cheap case
    assert model.calls.count("navigate") == 2
    assert _status(events, -1) == "answered"


# ─── ask_user ends the turn; the chip answer resumes ─────────


def test_ask_user_ends_turn_and_the_chip_resumes_with_notes(
        compiled, monkeypatch):
    build, _ = compiled
    spend = _spend(build)
    asker = Navigator(steps=[
        {"think": "record what I ruled out",
         "tool": "note", "args": {"text": "no gadget metric exists; "
                                          "closest world is spend"}},
        {"think": "two readings remain; ask with evidence",
         "tool": "ask_user", "args": {
             "question": "Did you mean acquirer spend, or nothing "
                         "in this build?",
             "options": [{"value": "spend", "label": "acquirer spend",
                          "evidence": "certified on gms_transaction"},
                         {"value": "drop", "label": "never mind"}]}},
    ])
    events, runtime, sid = _run(compiled, asker, [NO_MATCH],
                                monkeypatch=monkeypatch)
    assert _status(events) == "clarify"
    clarify = _by(events, "clarify_request")[0]
    assert clarify["slot"] == "agent"
    assert clarify["options"][0]["evidence"].startswith("certified")
    assert _by(events, "loop_done")[0]["outcome"] == "ask"
    # the notes rode the stored message, not any in-memory state
    stored = [m for m in runtime.store.messages(sid)
              if (m.get("payload") or {}).get("clarify")]
    assert stored[-1]["payload"]["loop_notes"]

    # ── the chip answer resumes navigation with what it learned ──
    resumer = Navigator(steps=[
        {"tool": "plan_set", "args": {"patch": {"metric": spend["id"],
                                                "grain": "transaction"}}},
        {"final": True},
    ])
    runtime2 = runtime          # same store, same session
    runtime2._model_factory = lambda budget: resumer
    runtime2.start_turn(sid, "acquirer spend",
                        choice={"slot": "agent", "value": "spend"})
    assert runtime2.wait(sid, 30)
    events2 = runtime2.runtime(sid).bus.since(0)
    assert _status(events2, -1) == "answered"
    opening = resumer.prompts[0]
    assert "you asked" in opening and "spend" in opening
    assert "no gadget metric exists" in opening   # notes re-injected


# ─── the third honest exit ───────────────────────────────────


def test_strict_json_failure_fails_closed_into_a_partial(compiled,
                                                         monkeypatch):
    model = Navigator(steps=[
        {"tool": "note", "args": {"text": "the concept is not in "
                                          "any card"}},
        "this is not a json step",
        "still not a json step",
    ])
    events, runtime, sid = _run(compiled, model, [NO_MATCH],
                                monkeypatch=monkeypatch)
    assert _status(events) == "partial"
    done = _by(events, "loop_done")[0]
    assert done["outcome"] == "partial"
    assert "strict JSON" in done["reason"]
    prose = "".join(e.get("delta", "") for e in events
                    if e["ev"] == "generate_token")
    assert "I stopped before finishing" in prose
    assert "the concept is not in any card" in prose  # notes surfaced
    stored = runtime.store.messages(sid)[-1]
    assert stored["payload"]["loop_partial"] is True
    # no answer, no contract: a partial never wears an answer's badge
    assert not _by(events, "answer_payload")
    assert not _by(events, "contract_ready")


def test_session_breaker_shares_the_loop_exit(compiled, monkeypatch):
    from sahs.ask import AskRuntime
    monkeypatch.setenv("SYNAPSE_NAVIGATE", "1")
    build, _ = compiled
    tmp = Path(tempfile.mkdtemp())
    model = Navigator(steps=[{"tool": "note",
                              "args": {"text": "should never run"}}])
    runtime = AskRuntime(builds_root=build.root.parent,
                         graph_root=tmp / "graph",
                         store_path=tmp / "s.sqlite3",
                         model_factory=lambda budget: model)
    session = runtime.create_session("analyst")
    rt = runtime.runtime(session["id"])
    rt.budget.charge(calls=rt.budget.session_calls)   # trip the cap
    runtime.start_turn(session["id"], NO_MATCH)
    assert runtime.wait(session["id"], 30)
    events = rt.bus.since(0)
    assert _status(events) == "partial"
    assert "session breaker" in _by(events, "loop_done")[0]["reason"]
    assert "navigate" not in model.calls   # not one step was taken


def test_unknown_tool_is_taught_in_context(compiled, monkeypatch):
    model = Navigator(steps=[
        {"tool": "grep", "args": {"pattern": "spend"}},
        "garbage", "garbage",
    ])
    events, _rt, _sid = _run(compiled, model, [NO_MATCH],
                             monkeypatch=monkeypatch)
    taught = model.prompts[1]
    assert "unknown tool 'grep'" in taught
    assert "grep_cards" in taught          # the real names, offered
    assert _status(events) == "partial"


def test_loop_budget_names_what_tripped():
    sys.path.insert(0, str(SILO))
    from sahs.loop.loop import LoopBudget
    budget = LoopBudget(max_steps=1)
    assert budget.tripped() == ""
    budget.charge()
    assert "step budget" in budget.tripped()
    slow = LoopBudget(max_steps=99, wall_seconds=0.0)
    assert "wall clock" in slow.tripped()
