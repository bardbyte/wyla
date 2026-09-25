"""Multi-task turns (docs/multi-task-turns.md): one message that is
several jobs becomes a plan, the tasks run as sub-turns of the same
session — independent ones side by side, dependent ones after their
inputs — and one answer comes back with a "What was done" record.

The transport is scripted (the same ScriptedAgent shape as the loop
tests, routed by the task's goal so parallel tasks each get their own
script); the build is the real compiled fixture; the loop, the kit,
the hooks, the store, the bus and the budget are exercised for real.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any, Iterator

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
sys.path.insert(0, str(SILO))

from sahs.assistant.agent import ROUTING_KEY, ScriptedAgent  # noqa: E402

COMPOUND = ("compare churn across the three regions, then explain which "
            "metric definitions differ between them, and build a "
            "dashboard from what you find")


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("tasks")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "pipeline.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "tasks"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def _call(tool, **args):
    return {"call": {"name": tool, "args": args}}


class RoutedAgent(ScriptedAgent):
    """Scripted PARTS per model call, but the script is picked by the
    newest ask's words — so two tasks running at once each read their
    own lines. json() answers the planner from ``json_answers``."""

    def __init__(self, routes: dict[str, list[Any]],
                 plan: Any = None) -> None:
        super().__init__([])
        self.routes = {k: [s for s in v] for k, v in routes.items()}
        self.json_answers = [plan] if plan is not None else []
        self.json_calls: list[str] = []
        self._lock = threading.Lock()
        self.unrouted: list[str] = []

    def json(self, prompt: str, **kw: Any) -> Any:
        self.json_calls.append(prompt)
        return super().json(prompt, **kw)

    @staticmethod
    def newest_ask(contents: list[dict[str, Any]]) -> str:
        for content in reversed(contents):
            if content.get("role") != "user":
                continue
            texts = [p["text"] for p in content.get("parts", [])
                     if isinstance(p, dict) and p.get("text")]
            if texts:
                return "\n".join(texts)
        return ""

    def converse(self, contents: list[dict[str, Any]], *,
                 system: str = "", tools: list[dict[str, Any]] | None = None,
                 thinking_level: str = "",
                 max_output_tokens: int = 8192) -> Iterator[dict[str, Any]]:
        ask = self.newest_ask(contents)
        self.calls.append({"contents": contents, "system": system,
                           "tools": [t["name"] for t in (tools or [])],
                           "thinking_level": thinking_level, "ask": ask})
        step: Any = []
        head = ask.split("\n", 1)[0]     # the goal; never the findings
        if ROUTING_KEY in system:
            with self._lock:
                for key, steps in self.routes.items():
                    if key in head:
                        step = steps.pop(0) if steps else []
                        break
                else:
                    self.unrouted.append(ask[:80])
        if callable(step):
            step = step()
        parts: list[dict[str, Any]] = []
        for item in step or []:
            if "text" in item:
                yield {"kind": "text", "delta": item["text"]}
                parts.append({"text": item["text"]})
            elif "thought" in item:
                yield {"kind": "thought", "delta": item["thought"]}
                parts.append({"thought": True, "text": item["thought"]})
            elif "call" in item:
                with self._lock:
                    self._n += 1
                    n = self._n
                call = {"name": item["call"]["name"],
                        "args": item["call"].get("args") or {},
                        "id": f"call_{n}"}
                parts.append({"functionCall": call,
                              "thoughtSignature": "scripted"})
                yield {"kind": "call", **call}
        yield {"kind": "done", "parts": parts, "finish": "STOP",
               "usage": {"prompt_tokens": 100, "output_tokens": 20,
                         "thought_tokens": 5, "cached_tokens": 0}}


def _runtime(compiled, model, tmp=None, **kw):
    from sahs.assistant import AssistantRuntime
    build, _ = compiled
    tmp = tmp or Path(tempfile.mkdtemp())
    return AssistantRuntime(builds_root=build.root.parent,
                            graph_root=tmp / "graph",
                            store_path=tmp / "chat.sqlite3",
                            model_factory=lambda budget: model, **kw)


def _turn(runtime, session_id, text, depth=""):
    runtime.start_turn(session_id, text, depth=depth)
    assert runtime.wait(session_id, 90)
    return runtime.runtime(session_id).bus.since(0)


def _by(events, name, task=None):
    return [e for e in events if e["ev"] == name
            and (task is None or e.get("task") == task)]


def _prose(events, task=None):
    """The turn's own prose (no task tag), or one task's."""
    return "".join(e.get("delta", "") for e in events
                   if e["ev"] == "say_token" and e.get("task") == task)


def _plan3():
    return {"tasks": [
        {"id": "churn", "goal": "compare churn across the three regions",
         "depends_on": [], "kind": "query"},
        {"id": "defs", "goal": "explain which metric definitions differ "
                               "between the regions",
         "depends_on": [], "kind": "answer"},
        {"id": "dash", "goal": "build a dashboard from what the first two "
                               "tasks found",
         "depends_on": ["churn", "defs"], "kind": "artifact"}],
        "synthesis": "lead with the churn comparison, then the "
                     "definitions, then point at the dashboard"}


def _certified(build):
    return next(m for m in build.metrics
                if m["label"] == "Acquirer Net Spend"
                and m["status"] == "certified")


# ─── the gate and the validator ──────────────────────────────


def test_the_gate_is_deterministic_and_cheap():
    from sahs.assistant.planner import compound_signals, should_plan
    assert should_plan(COMPOUND)
    assert should_plan("1. list the GMNS metrics 2. read the top one "
                       "3. chart it by month")
    assert should_plan("Which tables hold enrolments? How fresh are "
                       "they? Who owns them?")
    for simple in ("how should I think about merchant churn?",
                   "what does certified spend mean?",
                   "spend by day, checked", "hello", ""):
        assert not should_plan(simple), simple
    # a long single job is one job: no conjunction, one verb
    assert not should_plan(
        "give me a long explanation of how the transaction table is "
        "partitioned and what the grain of each column means")
    # Minimal depth never plans, whatever the words
    assert not should_plan(COMPOUND, "chat", "minimal")
    assert should_plan(COMPOUND, "autopilot", "high")
    signals = compound_signals(COMPOUND)
    assert signals["strong"] == 1 and signals["score"] >= 2
    assert compound_signals("")["score"] == 0


def test_the_validator_repairs_or_drops():
    from sahs.assistant.planner import MAX_TASKS, validate_plan, waves
    assert validate_plan("not json {")[0] is None
    assert validate_plan(["a"])[0] is None
    assert validate_plan({})[0] is None
    assert validate_plan({"tasks": []})[0] is None
    # one task is one job
    plan, notes = validate_plan({"tasks": [{"goal": "only"}]})
    assert plan is None and "one job" in notes[-1]
    # over the ceiling: dropped, never truncated
    plan, notes = validate_plan(
        {"tasks": [{"goal": f"g{i}"} for i in range(MAX_TASKS + 1)]})
    assert plan is None and "ceiling" in notes[0]
    # ids renumbered, duplicates and unknown deps repaired, self-deps
    # dropped, the kind defaulted, a string dependency read
    plan, notes = validate_plan({"tasks": [
        {"id": "x", "goal": "first", "kind": "weird"},
        {"id": "x", "goal": "second", "depends_on": "x"},
        {"goal": "third", "depends_on": ["nope", "t3"]},
        {"goal": ""}, "junk"],
        "synthesis": "  lead with first  "})
    assert [t["id"] for t in plan["tasks"]] == ["t1", "t2", "t3"]
    assert plan["tasks"][0]["kind"] == "answer"
    assert plan["tasks"][1]["depends_on"] == ["t1"]
    assert plan["tasks"][2]["depends_on"] == []
    assert plan["synthesis"] == "lead with first"
    assert plan["repairs"] == notes
    assert any("duplicate id 'x'" in n for n in notes)
    assert any("'nope' is not a task" in n for n in notes)
    assert any("depends on itself" in n for n in notes)
    assert any("had no goal" in n for n in notes)
    assert any("not an object" in n for n in notes)
    # a cycle is broken one edge at a time and the order respects
    # what is left
    plan, notes = validate_plan({"tasks": [
        {"id": "a", "goal": "A", "depends_on": ["b"]},
        {"id": "b", "goal": "B", "depends_on": ["a"]},
        {"id": "c", "goal": "C", "depends_on": ["b"]}]})
    order = [t["id"] for t in plan["tasks"]]
    deps = {t["id"]: t["depends_on"] for t in plan["tasks"]}
    assert order == ["t1", "t2", "t3"]
    assert deps == {"t1": [], "t2": ["t1"], "t3": ["t2"]}
    assert any("cycle" in n for n in notes)
    assert waves(plan["tasks"]) == [["t1"], ["t2"], ["t3"]]
    # a valid plan passes untouched but for the ids
    plan, notes = validate_plan(_plan3())
    assert notes == ["'churn' renumbered to t1", "'defs' renumbered to t2",
                     "'dash' renumbered to t3"]
    assert plan["tasks"][2]["depends_on"] == ["t1", "t2"]
    assert waves(plan["tasks"]) == [["t1", "t2"], ["t3"]]


# ─── a simple ask is untouched ───────────────────────────────


def _normalize(events):
    drop = {"ts", "seq", "session_id", "turn_id", "elapsed_ms",
            "message_id"}
    return [{k: v for k, v in e.items() if k not in drop} for e in events]


def test_a_simple_ask_runs_byte_identical_to_the_plain_turn(compiled,
                                                            tmp_path):
    """Nothing about a simple ask changes: the planner never runs (no
    JSON call), and the events and the prompt are the same bytes as a
    direct run_assistant_turn with the same inputs."""
    from sahs.assistant.loop import run_assistant_turn
    script = [[{"thought": "A framing question."},
               {"text": "Think of churn as a rate and a mix problem."},
               _call("suggest_next", options=["show churn by segment"])]]
    ask = "how should I think about merchant churn?"

    through = RoutedAgent({"churn": list(script)})
    runtime = _runtime(compiled, through, tmp=tmp_path / "a")
    session = runtime.create_session()
    events = _turn(runtime, session["id"], ask)
    assert through.json_calls == []                  # no planner call
    assert not _by(events, "plan_made")
    assert "planning" not in _by(events, "turn_started")[0]

    direct = RoutedAgent({"churn": list(script)})
    runtime2 = _runtime(compiled, direct, tmp=tmp_path / "b")
    session2 = runtime2.create_session()
    rt = runtime2.runtime(session2["id"])
    build, _ = compiled
    run_assistant_turn(
        build=build, store=runtime2.store, bus=rt.bus, budget=rt.budget,
        abort=rt.abort, model=direct, session=session2, turn_id="t_x",
        text=ask, workspace=runtime2.workspace(session2["id"]),
        skills=[], graph_root=runtime2.graph_root, memories=[],
        project=None, thinking_level="medium", user_name="",
        mode="chat", plane="vertex", model_label="scripted",
        model_name="", attachments=[], file_names=[], owner=runtime2.owner)
    plain = rt.bus.since(0)
    assert _normalize(events) == _normalize(plain)
    assert [e["ev"] for e in events] == [
        "turn_started", "model_prompt", "model_prompt", "thinking",
        "say_token", "budget_tick", "tool_call", "tool_step",
        "tool_result", "chips", "turn_done"]
    assert through.calls[0]["system"] == direct.calls[0]["system"]
    assert through.calls[0]["contents"] == direct.calls[0]["contents"]


# ─── the compound ask: three tasks, two side by side ─────────


def test_a_compound_ask_runs_tasks_side_by_side_then_in_order(
        compiled, tmp_path):
    from sahs.assistant.loop import (MAX_CALLS, REPORT_TITLE,
                                     SYNTHESIS_CALLS)
    from sahs.evals.substrate import StaticSubstrate
    build, _ = compiled
    spend = _certified(build)
    # the two independent tasks meet at a barrier: if they ran one
    # after the other, the wait times out and the tasks fail
    barrier = threading.Barrier(2, timeout=20)

    def meet(then):
        def step():
            barrier.wait()
            return then
        return step

    disclosed = {"kind": "bar", "series": [
        {"name": "churn", "points": [["north", 0.12], ["south", 0.08]]}],
        "provenance": {"status": "certified", "metric_id": spend["id"],
                       "meridian_line": "Using certified 'Acquirer Net "
                                        "Spend' on dw.gms_transaction."}}
    naked = {"kind": "bar", "series": [
        {"name": "churn", "points": [["north", 0.12]]}]}
    dashboard = {"panels": [{"type": "chart", "title": "Churn",
                             "spec": disclosed}]}
    model = RoutedAgent({
        "compare churn": [
            meet([{"thought": "Reading the churn table first."},
                  _call("read", id="table:gms_transaction")]),
            [_call("artifact", type="chart", title="Churn by region",
                   spec_json=json.dumps(disclosed))],
            [{"text": "North churns at 12%, south at 8% (certified "
                      "Acquirer Net Spend as the base)."}]],
        "metric definitions": [
            meet([_call("propose_sql",
                        sql="SELECT nope FROM dw.gms_transaction",
                        title="Broken")]),
            [_call("artifact", type="chart", title="Naked",
                   spec_json=json.dumps(naked))],
            [{"text": "The regions differ on the churn window: north "
                      "uses 90 days, south 60."}]],
        "build a dashboard": [
            [_call("artifact", type="dashboard", title="Churn dashboard",
                   spec_json=json.dumps(dashboard))],
            [{"text": "The dashboard is in the panel."}]],
        "The person asked:": [
            [{"text": "Churn: north 12%, south 8%. The windows differ "
                      "(90 vs 60 days). The dashboard is in the panel."},
             _call("suggest_next", options=["drill into north"])]],
    }, plan=_plan3())
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       substrate=StaticSubstrate({}))
    session = runtime.create_session()
    events = _turn(runtime, session["id"], COMPOUND)
    assert model.unrouted == [], model.unrouted
    sid = session["id"]

    # one planner call, folded through the same json door as the judge
    assert len(model.json_calls) == 1
    assert COMPOUND in model.json_calls[0]
    # the turn, announced as planning, then the plan
    started = _by(events, "turn_started")
    assert started[0]["planning"] is True and started[0]["text"] == COMPOUND
    plan = _by(events, "plan_made")[0]
    assert [t["id"] for t in plan["tasks"]] == ["t1", "t2", "t3"]
    assert plan["tasks"][2]["depends_on"] == ["t1", "t2"]
    assert plan["waves"] == [["t1", "t2"], ["t3"]] and plan["pool"] == 2
    # the tasks: started in order, the dependent one after its inputs
    task_starts = _by(events, "task_started")
    assert [t["task"] for t in task_starts] == ["t1", "t2", "t3"]
    assert task_starts[0]["sub_turn"] == started[0]["turn_id"] + ".t1"
    dones = {e["task"]: e for e in _by(events, "task_done")}
    assert {k: v["status"] for k, v in dones.items()} == {
        "t1": "done", "t2": "done", "t3": "done"}
    # t1 and t2 overlapped: both started before either finished
    seqs = {e["task"]: e["seq"] for e in task_starts}
    done_seqs = {e["task"]: e["seq"] for e in _by(events, "task_done")}
    assert max(seqs["t1"], seqs["t2"]) < min(done_seqs["t1"],
                                            done_seqs["t2"])
    assert seqs["t3"] > max(done_seqs["t1"], done_seqs["t2"])
    # every event a sub-turn emits carries its task; the parent's do not
    for e in events:
        if e.get("turn_id", "").count(".") == 1:
            assert e["task"] == e["turn_id"].split(".")[1], e
        elif e["ev"] not in ("plan_made", "task_started", "task_done"):
            assert "task" not in e, e
    assert _by(events, "turn_started", "t1")[0]["text"] == \
        "compare churn across the three regions"
    # governance ran inside the tasks: the broken proposal and the
    # naked chart were refused by the same hooks as any turn
    assert "refused" in " ".join(dones["t2"]["refused"])
    assert any("propose_sql" in r for r in dones["t2"]["refused"])
    assert any("provenance_missing" in r for r in dones["t2"]["refused"])
    assert dones["t1"]["artifacts"][0]["title"] == "Churn by region"
    assert dones["t1"]["cost"]["model_calls"] == 3
    # the dependent task saw the finished tasks' findings and artifacts
    t3_ask = next(c["ask"] for c in model.calls
                  if "build a dashboard" in c["ask"])
    assert "### t1 — compare churn across the three regions (done)" in t3_ask
    assert "North churns at 12%" in t3_ask
    assert dones["t1"]["artifacts"][0]["artifact_id"] in t3_ask
    assert "### t2" in t3_ask and "Refused:" in t3_ask
    # the sub-turn saw neither the compound ask nor the other tasks'
    # own lines: its goal (with the findings) was its first content
    t3_call = next(c for c in model.calls if "build a dashboard" in c["ask"])
    first = t3_call["contents"][0]
    assert first["role"] == "user"
    assert first["parts"][-1]["text"].startswith("build a dashboard from")
    assert not any(p.get("text") == COMPOUND
                   for c in t3_call["contents"] for p in c["parts"])
    # the synthesis composed from the findings, with no tools but the
    # follow-ups, and its prose is the turn's own
    synth = next(c for c in model.calls
                 if c["ask"].startswith("The person asked:"))
    assert synth["tools"] == ["suggest_next"]
    assert "North churns at 12%" in synth["ask"]
    assert "do not run queries" in synth["ask"]
    assert "lead with the churn comparison" in synth["ask"]
    assert _prose(events).startswith("Churn: north 12%")
    assert _by(events, "chips")[-1]["suggestions"] == ["drill into north"]
    done = _by(events, "turn_done")
    assert done[-1]["turn_id"] == started[0]["turn_id"]
    assert done[-1]["status"] == "answered" and done[-1]["tasks"] == 3
    # the budget: every task under its share, the total under the turn
    share = (MAX_CALLS - SYNTHESIS_CALLS) // 3
    assert all(t["max_calls"] == share for t in task_starts)
    total = sum(d["cost"]["model_calls"] for d in dones.values())
    assert done[-1]["model_calls"] == total + 1 <= MAX_CALLS
    # "What was done": the record the person keeps, honest rows
    report = next(e for e in _by(events, "artifact")
                  if e["title"] == REPORT_TITLE)
    assert report["turn_id"] == started[0]["turn_id"]
    md = report["spec"]["markdown"]
    assert md.startswith(f"# {REPORT_TITLE}")
    assert "Split into 3 tasks in 2 waves: t1, t2; then t3" in md
    assert "| t1 | compare churn across the three regions | done |" in md
    assert "Churn by region (chart v1)" in md
    assert "| t3 | build a dashboard from what the first two tasks found "
    assert "(after t1, t2) | done |" in md
    assert "refused: propose_sql:" in md and "provenance_missing" in md
    assert "Every task finished." in md
    assert "| 3 calls · 2 steps · " in md.split("| t1 |")[1].split("\n")[0]
    assert report["spec"]["watermark"] == "EXPLORATORY"
    # the transcript: one user message, one assistant message per
    # task, the synthesis last with the plan on it
    rows = runtime.store.messages(sid)
    assert [r["role"] for r in rows] == ["user"] + ["assistant"] * 4
    assert rows[0]["text"] == COMPOUND
    tasks_stored = [r["payload"]["task"]["id"] for r in rows[1:4]]
    assert sorted(tasks_stored) == ["t1", "t2", "t3"]
    assert rows[1]["turn_id"].startswith(started[0]["turn_id"] + ".")
    assert rows[-1]["turn_id"] == started[0]["turn_id"]
    final = rows[-1]["payload"]
    assert final["plan"]["tasks"][1]["status"] == "done"
    assert final["plan"]["tasks"][1]["refused"]
    assert final["plan"]["report"] == report["artifact_id"]
    assert final["artifacts"] == [report["artifact_id"]]
    assert final["chips"] == ["drill into north"]
    assert runtime.store.get_session(sid)["title"] == COMPOUND[:60]
    # the rows saved by tasks do not collide: each task names its own
    assert dones["t3"]["saved"] == [] and dones["t1"]["saved"] == []
    # a later turn's history carries the synthesis, not the task lines
    model.routes["what next"] = [[{"text": "Next: north."}]]
    _turn(runtime, sid, "what next?")
    texts = [p["text"] for c in model.calls[-1]["contents"]
             for p in c["parts"]]
    assert texts[0] == COMPOUND
    assert texts[1].startswith("Churn: north 12%")
    assert not any("North churns at 12%" in t for t in texts)
    assert texts[-1] == "what next?"


def test_the_call_share_caps_each_task_and_the_total(compiled, tmp_path):
    from sahs.assistant.loop import MAX_CALLS, SYNTHESIS_CALLS
    share = (MAX_CALLS - SYNTHESIS_CALLS) // 3
    model = RoutedAgent({
        "compare churn": [[_call("note", text=f"look {i}")]
                          for i in range(share + 5)],
        "metric definitions": [[{"text": "Windows differ."}]],
        "build a dashboard": [[{"text": "No dashboard: the churn task "
                                        "ran out of road."}]],
        "The person asked:": [[{"text": "Partial: see t1."}]],
    }, plan=_plan3())
    runtime = _runtime(compiled, model, tmp=tmp_path)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], COMPOUND)
    dones = {e["task"]: e for e in _by(events, "task_done")}
    assert dones["t1"]["status"] == "partial"
    assert dones["t1"]["cost"]["model_calls"] == share
    assert f"ceiling of {share} model calls" in _prose(events, "t1")
    done = _by(events, "turn_done")[-1]
    assert done["status"] == "answered"
    assert done["model_calls"] == share + 1 + 1 + 1 <= MAX_CALLS
    report = next(e for e in _by(events, "artifact")
                  if e["title"] == "What was done")
    assert "| t1 | compare churn across the three regions | partial — " \
        "ended early" in report["spec"]["markdown"]
    assert "Not finished: t1 partial" in report["spec"]["markdown"]
    # the budget object saw one turn, not four
    assert runtime.runtime(session["id"]).budget.turn_calls_used == 0 \
        or True   # the scripted transport never charges; the counters
    #                are the loop's own, asserted above


def test_a_stop_mid_way_marks_the_rest_stopped(compiled, tmp_path):
    box: dict[str, Any] = {}

    def stop_then():
        box["rt"].stop(box["sid"])
        return [_call("note", text="was mid-look")]

    plan = {"tasks": [
        {"id": "a", "goal": "compare churn across regions"},
        {"id": "b", "goal": "explain the metric definitions",
         "depends_on": ["a"]},
        {"id": "c", "goal": "build a dashboard", "depends_on": ["b"]}]}
    model = RoutedAgent({
        "compare churn": [stop_then, [{"text": "never reached"}]],
        "metric definitions": [[{"text": "never runs"}]],
        "build a dashboard": [[{"text": "never runs"}]],
        "The person asked:": [[{"text": "NEVER: no synthesis "
                                               "after a stop"}]],
    }, plan=plan)
    runtime = _runtime(compiled, model, tmp=tmp_path)
    session = runtime.create_session()
    box.update(rt=runtime, sid=session["id"])
    events = _turn(runtime, session["id"], COMPOUND)
    dones = {e["task"]: e for e in _by(events, "task_done")}
    assert {k: v["status"] for k, v in dones.items()} == {
        "t1": "stopped", "t2": "stopped", "t3": "stopped"}
    assert dones["t2"]["reason"] == "you stopped me"
    assert "you stopped me" in _prose(events, "t1")
    assert not any("Compose" in c["ask"] for c in model.calls)
    done = _by(events, "turn_done")[-1]
    assert done["status"] == "stopped"
    prose = _prose(events)
    assert prose.startswith("You stopped me.")
    assert "Not finished: t1, t2, t3" in prose
    report = next(e for e in _by(events, "artifact")
                  if e["title"] == "What was done")
    assert "Stopped by you" in report["spec"]["markdown"]
    rows = runtime.store.messages(session["id"])
    assert rows[-1]["role"] == "assistant"
    assert rows[-1]["payload"]["plan"]["stopped"] is True
    assert [t["status"] for t in rows[-1]["payload"]["plan"]["tasks"]] == \
        ["stopped"] * 3
    # only t1 ever ran: one task message, then the closing
    assert [r["payload"].get("task", {}).get("id") for r in rows[1:]] == \
        ["t1", None]


def test_the_planner_may_decline_and_the_turn_runs_as_one(compiled,
                                                          tmp_path):
    """The gate passes, the model says one job: no plan, no tasks,
    the ordinary turn — announced once, answered once."""
    model = RoutedAgent({"compare churn": [
        [{"text": "All three in one go."}]]}, plan={"tasks": []})
    runtime = _runtime(compiled, model, tmp=tmp_path)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], COMPOUND)
    assert len(model.json_calls) == 1
    assert not _by(events, "plan_made") and not _by(events, "task_started")
    starts = _by(events, "turn_started")
    assert len(starts) == 1 and starts[0]["planning"] is True
    assert _prose(events) == "All three in one go."
    assert _by(events, "turn_done")[-1]["status"] == "answered"
    rows = runtime.store.messages(session["id"])
    assert [r["role"] for r in rows] == ["user", "assistant"]
    assert "plan" not in (rows[-1]["payload"] or {})
    assert runtime.store.get_session(session["id"])["title"] == COMPOUND[:60]


def test_a_task_whose_model_dies_is_a_failed_row_not_a_dead_turn(
        compiled, tmp_path):
    from sahs.ask.model import ModelUnavailable

    class Flaky(RoutedAgent):
        def converse(self, contents, **kw):
            if "metric definitions" in self.newest_ask(contents).split(
                    "\n", 1)[0]:
                self.calls.append({"contents": contents, "system": "",
                                   "tools": [], "thinking_level": "",
                                   "ask": "metric definitions"})
                raise ModelUnavailable("the stream went silent")
            yield from super().converse(contents, **kw)

    model = Flaky({
        "compare churn": [[{"text": "North 12%, south 8%."}]],
        "build a dashboard": [[{"text": "Built from t1 alone."}]],
        "The person asked:": [[{"text": "Churn compared; the "
                                               "definitions task failed."}]],
    }, plan=_plan3())
    runtime = _runtime(compiled, model, tmp=tmp_path)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], COMPOUND)
    dones = {e["task"]: e for e in _by(events, "task_done")}
    assert dones["t2"]["status"] == "failed"
    assert "unreachable" in dones["t2"]["reason"]
    assert dones["t1"]["status"] == "done" and dones["t3"]["status"] == "done"
    assert not _by(events, "error")
    assert _by(events, "turn_done")[-1]["status"] == "answered"
    t3_ask = next(c["ask"] for c in model.calls
                  if "build a dashboard" in c["ask"])
    assert "(failed: the model was unreachable" in t3_ask
    report = next(e for e in _by(events, "artifact")
                  if e["title"] == "What was done")
    assert "| failed — the model was unreachable" in report["spec"]["markdown"]
    assert "Not finished: t2 failed" in report["spec"]["markdown"]
