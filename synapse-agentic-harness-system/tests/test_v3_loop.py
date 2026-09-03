"""Synapse v3 Stage 1 — the thin loop over native tool calls, the
python sandbox, artifacts, and the rendering rules, against the real
compiled fixture build.

The transport is scripted with PARTS (A1: transport replaced, data
never): the ScriptedAgent emits thoughts, text, and functionCall
parts exactly as the Vertex client does, so the loop, the kit, the
hooks, the store, and the events are exercised for real. The rules
are exercised the way the design states them: no strict JSON, no
strikes, whole results back to the model, limits in plain language.
"""

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
    tmp = tmp_path_factory.mktemp("v3")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v3"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def _call(tool, **args):
    return {"call": {"name": tool, "args": args}}


def _agent(*steps):
    from sahs.assistant.agent import ScriptedAgent
    return ScriptedAgent([list(s) for s in steps])


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
    assert runtime.wait(session_id, 60)
    return runtime.runtime(session_id).bus.since(0)


def _by(events, name):
    return [e for e in events if e["ev"] == name]


def _work(events):
    return [e for e in _by(events, "tool_step")
            if e["tool"] != "suggest_next"]


def _prose(events):
    return "".join(e.get("delta", "") for e in events
                   if e["ev"] == "say_token")


def _responses(model):
    """Every functionResponse the model was handed, in order."""
    out = []
    for call in model.calls:
        for content in call["contents"]:
            for part in content.get("parts", []):
                if "functionResponse" in part:
                    out.append(part["functionResponse"])
    return out


# ─── rendering rules (§6), as the validator enforces them ────


def test_rule_one_refuses_undisclosed_numbers():
    from sahs.assistant.artifacts import validate_artifact
    spec = {"kind": "line",
            "series": [{"name": "spend", "points": [["d1", 1.0]]}]}
    normalized, problems = validate_artifact("chart", spec)
    assert normalized is None
    codes = [p["code"] for p in problems]
    assert "provenance_missing" in codes


def test_rule_one_passes_disclosed_numbers():
    from sahs.assistant.artifacts import validate_artifact
    spec = {"kind": "line",
            "series": [{"name": "spend", "points": [["d1", 1.0]]}],
            "provenance": {"status": "certified",
                           "metric_id": "metric:301a4f124096",
                           "meridian_line": "Using certified "
                                            "'Acquirer Net Spend'."}}
    normalized, problems = validate_artifact("chart", spec,
                                             build_id="b_x")
    assert problems == []
    assert normalized["build_id"] == "b_x"
    assert "watermark" not in normalized


def test_rule_two_composed_keeps_watermark_without_a_fact():
    from sahs.assistant.artifacts import validate_artifact
    spec = {"kind": "bar",
            "series": [{"name": "share", "points": [["a", 0.4]]}],
            "provenance": {"status": "composed",
                           "meridian_line": "Composed from two "
                                            "certified parts."}}
    unproven, _ = validate_artifact("chart", spec)
    assert unproven["watermark"] == "EXPLORATORY"
    spec["provenance"]["facts"] = ["f1"]
    proven, _ = validate_artifact("chart", spec,
                                  facts=frozenset({"f1"}))
    assert "watermark" not in proven
    assert proven["provenance"]["facts_verified"] == ["f1"]


def test_tables_detect_numbers_and_documents_default_exploratory():
    from sahs.assistant.artifacts import validate_artifact
    words_only, problems = validate_artifact(
        "table", {"columns": [{"key": "t", "label": "Table"}],
                  "rows": [{"t": "dw.gms_transaction"}]})
    assert problems == [] and "provenance" not in words_only
    _none, problems = validate_artifact(
        "table", {"columns": [{"key": "n", "label": "N"}],
                  "rows": [{"n": 42}]})
    assert any(p["code"] == "provenance_missing" for p in problems)
    doc, problems = validate_artifact(
        "document", {"markdown": "# Notes\nno numbers claimed"})
    assert problems == [] and doc["watermark"] == "EXPLORATORY"


# ─── the sandbox (§4) ────────────────────────────────────────


def test_sandbox_reads_the_real_build_and_strips_the_env(compiled,
                                                         tmp_path,
                                                         monkeypatch):
    from sahs.assistant.sandbox import prepare_workspace, run_python
    build, _ = compiled
    monkeypatch.setenv("FAKE_SECRET_TOKEN", "should-not-leak")
    prepare_workspace(tmp_path, build.root)
    out = run_python(
        "import os, meridian\n"
        "print(len(meridian.metrics()), 'metrics')\n"
        "print('leak' if os.environ.get('FAKE_SECRET_TOKEN') else "
        "'clean')\n", tmp_path)
    assert out["ok"], out
    assert f"{len(build.metrics)} metrics" in out["stdout"]
    assert "clean" in out["stdout"] and "leak" not in out["stdout"]


def test_sandbox_rows_teach_and_files_persist(compiled, tmp_path):
    from sahs.assistant.sandbox import (prepare_workspace, run_python,
                                        save_rows)
    build, _ = compiled
    prepare_workspace(tmp_path, build.root)
    missing = run_python(
        "import meridian\n"
        "try: meridian.rows('q9')\n"
        "except FileNotFoundError as e: print(e)\n", tmp_path)
    assert "no saved result" in missing["stdout"]
    save_rows(tmp_path, "q1", [{"day": "d1", "spend": 2.0}])
    out = run_python(
        "import meridian, json\n"
        "rows = meridian.rows('q1')\n"
        "open('total.txt', 'w').write(str(sum(r['spend'] "
        "for r in rows)))\n"
        "print('saved')\n", tmp_path)
    assert out["ok"] and "total.txt" in out.get("files", [])
    again = run_python("print(open('total.txt').read())", tmp_path)
    assert "2.0" in again["stdout"]


def test_sandbox_timeout_is_a_taught_error(compiled, tmp_path):
    from sahs.assistant.sandbox import prepare_workspace, run_python
    build, _ = compiled
    prepare_workspace(tmp_path, build.root)
    out = run_python("while True: pass", tmp_path, timeout=1.0)
    assert "timed out" in out["error"]
    assert "workspace keeps files" in out["hint"]


# ─── the prompt: sections, no protocol ───────────────────────


def test_the_prompt_is_sections_not_protocol(compiled):
    from sahs.assistant.loop import system_prompt
    from sahs.assistant.skills_loader import all_skills
    build, _ = compiled
    system = system_prompt(build, skill_index=all_skills(None))
    assert system.startswith("<identity>\n" + KEY)
    for tag in ("<chain>", "<mode>", "<graph>", "<skills>", "<memory>"):
        assert tag in system, tag
    # the autonomy slider is a section: chat hands over, autopilot runs
    assert "hand it over with propose_sql" in system
    auto = system_prompt(build, mode="autopilot")
    assert "Autopilot: run the query yourself" in auto
    assert "propose_sql is not needed" in auto
    assert "## Skills on demand" in system
    # an empty shelf leaves no empty section behind
    assert "<skills>" not in system_prompt(build)
    assert "the business map" in system
    assert 'search("GMNS", kind="list")' in system
    assert "list_metrics" not in system         # the v1 kit's word
    assert "A failed tool call is information" in system
    for gone in ("STRICT JSON", "Next step:", "BUDGET:", '"think"',
                 "Your tools:"):
        assert gone not in system, gone
    # the prefix is stable: same inputs, same bytes (cacheable)
    assert system == system_prompt(build, skill_index=all_skills(None))


def test_the_kit_declares_eleven_tools_plus_follow_ups(compiled,
                                                        tmp_path):
    from sahs.assistant.agent import declarations
    from sahs.assistant.kit import build_kit
    from sahs.assistant.state import AssistantState
    from sahs.assistant.store import AssistantStore
    build, _ = compiled
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    kit = build_kit(build, AssistantState(), store=store,
                    session_id=session["id"], turn_id="t",
                    workspace=tmp_path / "ws")
    names = [d["name"] for d in declarations(kit)]
    assert names == ["search", "read", "sample_values", "run_sql",
                     "propose_sql", "python", "check", "artifact", "ask",
                     "load_skill", "remember", "note", "suggest_next"]
    for decl in declarations(kit):
        assert decl["parameters"]["type"] == "OBJECT"
        assert decl["description"]


def test_whole_results_cap_with_a_note():
    from sahs.assistant.kit import RESULT_CAP
    from sahs.assistant.loop import _response_payload
    small = {"ok": True, "rows": [1, 2, 3], "_artifact": {"x": 1}}
    payload, text = _response_payload(small)
    assert "_artifact" not in payload and payload["ok"] is True
    big = {"text": "x" * (RESULT_CAP + 5000)}
    payload, text = _response_payload(big)
    assert payload["truncated"] is True
    assert "read(section=" in payload["note"]
    assert len(payload["text"]) == RESULT_CAP
    assert text.endswith("]") and "truncated at" in text


# ─── the thin loop, end to end ───────────────────────────────


def test_reasoning_turn_needs_no_tools(compiled):
    model = _agent([
        {"thought": "A framing question — no data needed."},
        {"text": "Think of churn as a rate and a mix problem: which "
                 "merchants leave, and whether the leavers are big."},
        _call("suggest_next", options=["show churn by segment",
                                       "which tables cover this?"])])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "how should I think "
                                           "about merchant churn?")
    assert _by(events, "turn_done")[-1]["status"] == "answered"
    assert "rate and a mix" in _prose(events)
    assert not _work(events)
    assert _by(events, "thinking")[0]["delta"].startswith("A framing")
    assert _by(events, "chips")[0]["suggestions"] == [
        "show churn by segment", "which tables cover this?"]
    # follow-ups after the answer END the turn: one model call
    assert len(model.calls) == 1
    stored = runtime.store.messages(session["id"])[-1]
    assert stored["payload"]["chips"]
    assert stored["payload"]["artifacts"] == []
    # the thinking travels with the message: the transcript shows it
    # folded ("Thought for …"), expandable, after a reload
    trace = stored["payload"]["trace"]
    assert trace[0] == {"kind": "thought",
                        "text": "A framing question — no data needed."}
    assert trace[1]["kind"] == "tool" and trace[1]["tool"] == "suggest_next"
    assert stored["payload"]["elapsed_ms"] >= 0


def test_artifact_refusal_teaches_and_the_second_try_lands(compiled):
    build, _ = compiled
    spend = next(m for m in build.metrics
                 if m["label"] == "Acquirer Net Spend"
                 and m["status"] == "certified")
    naked = {"kind": "line", "series": [
        {"name": "spend", "points": [["2026-08-01", 4.0],
                                     ["2026-08-02", 6.0]]}]}
    disclosed = dict(naked, provenance={
        "status": "certified", "metric_id": spend["id"],
        "meridian_line": "Using certified 'Acquirer Net Spend' on "
                         "dw.gms_transaction."})
    model = _agent(
        [_call("artifact", type="chart", title="Spend by day",
               spec_json=json.dumps(naked))],
        [_call("artifact", type="chart", title="Spend by day",
               spec_json=json.dumps(disclosed))],
        [{"text": "Spend by day is in the panel, certified."},
         _call("suggest_next", options=["break it down by country"])])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "chart spend by day")
    refusals = [e for e in _by(events, "tool_step")
                if "artifact refused" in e["summary"]]
    assert refusals, "the naked chart was not refused"
    panel = _by(events, "artifact")
    assert len(panel) == 1 and panel[0]["version"] == 1
    assert panel[0]["spec"]["provenance"]["status"] == "certified"
    # the refusal reached the model WHOLE, as the tool's response
    first = _responses(model)[0]
    assert first["name"] == "artifact"
    assert any(p["code"] == "provenance_missing"
               for p in first["response"]["problems"])
    rows = runtime.store.list_artifacts(session["id"])
    assert len(rows) == 1 and rows[0]["title"] == "Spend by day"
    # the model content went back verbatim, signatures included
    model_turns = [c for c in model.calls[-1]["contents"]
                   if c["role"] == "model"]
    assert model_turns[0]["parts"][0]["thoughtSignature"] == "scripted"
    assert model_turns[0]["parts"][0]["functionCall"]["id"] == "call_1"


def test_artifact_update_makes_a_version_not_a_copy(compiled):
    build, _ = compiled
    count = next(m for m in build.metrics
                 if m["label"] == "Transaction Count"
                 and m["status"] == "certified")
    disclosed = {"kind": "bar", "series": [
        {"name": "n", "points": [["a", 1.0]]}],
        "provenance": {"status": "certified",
                       "metric_id": count["id"],
                       "meridian_line": "Using certified "
                                        "'Transaction Count'."}}
    v2 = dict(disclosed, series=[{"name": "n",
                                  "points": [["a", 1.0], ["b", 2.0]]}])
    model = _agent(
        [_call("artifact", type="chart", title="Counts",
               spec_json=json.dumps(disclosed))],
        [{"text": "Done."}])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    _turn(runtime, session["id"], "chart counts")
    artifact_id = runtime.store.list_artifacts(
        session["id"])[0]["artifact_id"]
    model.steps = [
        [_call("artifact", type="chart", title="Counts",
               spec_json=json.dumps(v2), artifact_id=artifact_id)],
        [{"text": "Extended."}]]
    events = _turn(runtime, session["id"], "add point b")
    assert _by(events, "artifact")[-1]["version"] == 2
    versions = runtime.store.artifact_versions(artifact_id)
    assert [v["version"] for v in versions] == [1, 2]
    assert runtime.store.get_artifact(
        artifact_id, 1)["spec"]["series"][0]["points"] == [["a", 1.0]]
    # the second turn's prompt named the artifact it could version
    assert artifact_id in model.calls[-1]["system"]


def test_python_turn_reads_the_build(compiled):
    model = _agent(
        [_call("python", code="import meridian\n"
                              "c = [m for m in meridian.metrics() "
                              "if m['status'] == 'certified']\n"
                              "print(len(c), 'certified')")],
        [{"text": "Counted straight from the build."}])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "how many certified?")
    build, _ = compiled
    certified = sum(1 for m in build.metrics
                    if m["status"] == "certified")
    step = _by(events, "tool_step")[0]
    assert f"{certified} certified" in step["summary"]
    assert _by(events, "tool_call")[0]["tool"] == "python"
    trace = runtime.store.messages(session["id"])[-1]["payload"]["trace"]
    assert [t["kind"] for t in trace] == ["tool"]
    assert trace[0]["tool"] == "python" and "certified" in trace[0]["summary"]
    assert "import meridian" in trace[0]["args"]
    # the call's input travels whole: the code, the SQL — what the
    # transcript shows when a step row is clicked
    assert trace[0]["input"].startswith("import meridian")
    assert trace[0]["elapsed_ms"] >= 0
    assert _by(events, "tool_call")[0]["input"].startswith("import meridian")
    assert _by(events, "tool_step")[0]["input"].startswith("import meridian")


def test_sql_rows_flow_into_the_sandbox(compiled, tmp_path,
                                        monkeypatch):
    from sahs.evals.substrate import DryRunOutcome

    class NoWarehouse:
        def dry_run(self, sql):
            return DryRunOutcome(valid=True, result_schema=None)

    import sahs.evals.substrate as substrate_module
    monkeypatch.setattr(substrate_module, "BQDryRun", NoWarehouse)

    class Extract:
        name = "frozen_extract"

        def run(self, sql, limit):
            return {"rows": [{"part_dt": "2026-08-01",
                              "acquirer_net_spend": 5.0}],
                    "schema": [{"name": "part_dt", "type": "date"}]}

    model = _agent(
        [_call("run_sql", sql="SELECT part_dt, sum(trans_usd_am) AS "
                              "acquirer_net_spend FROM "
                              "dw.gms_transaction GROUP BY part_dt",
               mode="snapshot")],
        [_call("python", code="import meridian\n"
                              "print(meridian.rows('q1')[0]"
                              "['acquirer_net_spend'])")],
        [{"text": "The rows made it to python."}])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       snapshot_runner=Extract())
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "spend by day, checked")
    sql_step, py_step = _by(events, "tool_step")[:2]
    assert "q1" in sql_step["summary"]
    assert "5.0" in py_step["summary"]
    # the rows went back to the model whole, not as a count
    assert _responses(model)[0]["response"]["rows"][0][
        "acquirer_net_spend"] == 5.0


def test_an_empty_answer_is_an_honest_partial(compiled):
    model = _agent([])                  # the model said nothing
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "anything")
    assert _by(events, "turn_done")[-1]["status"] == "partial"
    prose = _prose(events)
    assert "nothing usable" in prose
    assert "JSON" not in prose


def test_limits_end_in_plain_language(compiled):
    from sahs.assistant.loop import MAX_CALLS
    model = _agent(*[[_call("note", text=f"look {i}")]
                     for i in range(MAX_CALLS + 5)])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "never-ending")
    done = _by(events, "turn_done")[-1]
    assert done["status"] == "partial"
    assert done["model_calls"] == MAX_CALLS
    prose = _prose(events)
    assert f"ceiling of {MAX_CALLS} model calls" in prose
    assert "continue" in prose
    for harness_word in ("budget", "breaker", "JSON", "strict"):
        assert harness_word not in prose


def test_stop_lands_in_a_recorded_stop_not_a_vanished_turn(compiled):
    from sahs.assistant.agent import ScriptedAgent
    runtime_box = {}

    class Stopper(ScriptedAgent):
        def converse(self, contents, **kw):
            if not self.calls:
                runtime_box["rt"].stop(runtime_box["sid"])
            yield from super().converse(contents, **kw)

    model = Stopper([[_call("note", text="was mid-look")],
                     [{"text": "never reached"}]])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    runtime_box.update(rt=runtime, sid=session["id"])
    events = _turn(runtime, session["id"], "long question")
    done = _by(events, "turn_done")[-1]
    assert done["status"] == "stopped"
    assert "you stopped me" in _prose(events)
    assert runtime.store.messages(session["id"])[-1]["role"] == \
        "assistant"


def test_a_lost_connection_mid_turn_keeps_what_was_said(compiled):
    from sahs.ask.model import ModelUnavailable
    from sahs.assistant.agent import ScriptedAgent

    class Flaky(ScriptedAgent):
        def converse(self, contents, **kw):
            if len(self.calls) == 1:          # the second call dies
                self.calls.append({"contents": contents, "system": "",
                                   "tools": [], "thinking_level": ""})
                raise ModelUnavailable("the model stream went silent "
                                       "for 120s")
            yield from super().converse(contents, **kw)

    model = Flaky([[{"text": "Looking at the enrolment table first."},
                    _call("read", id="table:gms_transaction")]])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "how many enrolled?")
    done = _by(events, "turn_done")[-1]
    assert done["status"] == "partial"
    prose = _prose(events)
    assert "Looking at the enrolment table first." in prose
    assert "I lost the connection to the model" in prose
    assert "continue" in prose
    assert not _by(events, "error")
    stored = runtime.store.messages(session["id"])[-1]
    assert stored["role"] == "assistant" and "lost the connection" \
        in stored["text"]

    # nothing happened yet: the honest error card, not a closing line
    class Dead(ScriptedAgent):
        def converse(self, contents, **kw):
            raise ModelUnavailable("no Vertex contract in .env")
            yield  # pragma: no cover

    runtime = _runtime(compiled, Dead())
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "hello")
    assert _by(events, "error")[0]["code"] == "model_unavailable"
    assert _by(events, "turn_done")[-1]["status"] == "error"


def test_turn_window_points_at_the_in_flight_turn(compiled):
    import time
    from sahs.assistant.agent import ScriptedAgent

    def slow():
        time.sleep(1.5)
        return [{"text": "Done after a pause."}]

    runtime = _runtime(compiled, ScriptedAgent([slow]))
    session = runtime.create_session()
    assert runtime.turn_window(session["id"])["running"] is False
    runtime.start_turn(session["id"], "a slow one")
    time.sleep(0.5)
    window = runtime.turn_window(session["id"])
    assert window["running"] is True and window["turn_id"].startswith("t_")
    bus = runtime.runtime(session["id"]).bus
    first = next(e for e in bus.since(0) if e["ev"] == "turn_started")
    assert window["after"] == first["seq"] - 1
    # replaying from the window starts at the turn's own first event
    assert bus.since(window["after"])[0]["ev"] == "turn_started"
    assert runtime.wait(session["id"], 30)
    assert runtime.turn_window(session["id"])["running"] is False
    assert "Done after a pause." in _prose(bus.since(0))


def test_second_turn_sees_the_first_newest_ask_last(compiled):
    model = _agent(
        [{"text": "Certified spend means the meridian definition."}],
        [{"text": "As I said, the meridian one."}])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    _turn(runtime, session["id"], "what does certified spend mean?")
    _turn(runtime, session["id"], "which one did you mean?")
    contents = model.calls[-1]["contents"]
    roles = [c["role"] for c in contents]
    assert roles == ["user", "model", "user"]
    texts = [p["text"] for c in contents for p in c["parts"]]
    assert texts[0] == "what does certified spend mean?"
    assert "meridian definition" in texts[1]
    assert texts[-1] == "which one did you mean?"     # newest LAST


def test_parallel_calls_answer_in_order(compiled):
    model = _agent(
        [_call("note", text="first"), _call("note", text="second")],
        [{"text": "Both noted."}])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "note two things")
    assert [e["n"] for e in _by(events, "tool_step")] == [1, 2]
    answer = model.calls[-1]["contents"][-1]
    assert answer["role"] == "user"
    ids = [p["functionResponse"]["id"] for p in answer["parts"]]
    assert ids == ["call_1", "call_2"]
    assert all(p["functionResponse"]["name"] == "note"
               for p in answer["parts"])
    assert runtime.store.get_session(session["id"])["notes"] == [
        "first", "second"]


def test_depth_sets_the_thinking_level(compiled):
    model = _agent([{"text": "Quick one."}], [{"text": "Deep one."}],
                   [{"text": "Default."}])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "hi", depth="quick")
    assert _by(events, "turn_started")[-1]["thinking_level"] == "low"
    assert model.calls[-1]["thinking_level"] == "low"
    events = _turn(runtime, session["id"], "why", depth="deep")
    assert model.calls[-1]["thinking_level"] == "high"
    assert _by(events, "turn_done")[-1]["thinking_level"] == "high"
    _turn(runtime, session["id"], "and", depth="")
    assert model.calls[-1]["thinking_level"] == "medium"


def test_the_transcript_record_and_the_event_family(compiled):
    from sahs.assistant.events import ASSISTANT_EVENTS
    model = _agent(
        [{"thought": "A quick look at the transaction table."},
         _call("read", id="table:gms_transaction")],
        [{"text": "The transaction table, read whole."}])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "what is in gms_transaction?")
    kinds = {e["ev"] for e in events}
    assert kinds <= set(ASSISTANT_EVENTS)
    prompts = _by(events, "model_prompt")
    assert prompts[0]["kind"] == "system"
    assert KEY in prompts[0]["content"]
    assert [p["kind"] for p in prompts[1:]] == ["call", "call"]
    assert "read(" in prompts[2]["content"]      # what the model saw
    assert _by(events, "thinking")[0]["delta"].startswith("A quick")
    assert _by(events, "tool_call")[0]["tool"] == "read"
    result = _by(events, "tool_result")[0]
    assert result["tool"] == "read"
    # whole, not four lines: the card text rides in the record too
    assert len(result["content"]) > 800
    assert "gms_transaction" in _responses(model)[0]["response"]["card"]
    done = _by(events, "turn_done")[-1]
    assert done["model_calls"] == 2 and done["steps"] == 1
    assert done["status"] == "answered"


# ─── the handover (§5): SQL first, the rows on a tap ─────────

SPEND_SQL = ("SELECT part_dt, sum(trans_usd_am) AS acquirer_net_spend "
             "FROM dw.gms_transaction GROUP BY part_dt")


class Warehouse:
    """A live runner that answers without a warehouse: the sandbox's
    gates, the kit's limits, and the artifact all stay real."""
    name = "fake_live"

    def __init__(self):
        self.calls = []

    def run(self, sql, limit):
        self.calls.append((sql, limit))
        return {"rows": [["2026-08-01", 5.0], ["2026-08-02", 7.5]],
                "schema": [{"name": "part_dt", "type": "DATE"},
                           {"name": "acquirer_net_spend",
                            "type": "FLOAT"}],
                "columns": ["part_dt", "acquirer_net_spend"],
                "bytes_processed": 1234}


def _certified(build):
    return next(m["id"] for m in build.metrics
                if m.get("status") == "certified")


def test_propose_sql_hands_the_query_over_and_ends_the_turn(
        compiled, tmp_path):
    from sahs.evals.substrate import StaticSubstrate
    build, _ = compiled
    model = _agent(
        [{"text": "Here is the query for spend by day."},
         _call("propose_sql", sql=SPEND_SQL, title="Spend by day",
               why="The certified expression, by settlement day.",
               metric_id=_certified(build))],
        [{"text": "NEVER: the turn ended on the handover"}])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       substrate=StaticSubstrate({}))
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "spend by day")
    handed = _by(events, "proposal")
    assert len(handed) == 1
    proposal = handed[0]["proposal"]
    assert proposal["title"] == "Spend by day"
    assert proposal["status"] == "certified"
    assert proposal["metric_id"] == _certified(build)
    assert "meridian line" in proposal["meridian_line"]
    assert "gms_transaction" in proposal["sql"]
    assert handed[0]["message_id"]
    # one model call: the person runs it, not another round-trip
    assert len(model.calls) == 1
    done = _by(events, "turn_done")[0]
    assert done["status"] == "proposed" and done["model_calls"] == 1
    assert "Here is the query" in _prose(events)
    step = _by(events, "tool_step")[0]
    assert step["tool"] == "propose_sql" and "handed over" in step["summary"]
    assert step["input"] == SPEND_SQL          # the SQL on the row
    # the transcript keeps the proposal, so a reload shows the card
    last = runtime.store.messages(session["id"])[-1]
    assert last["role"] == "assistant"
    assert last["payload"]["proposal"]["sql_written"] == SPEND_SQL
    assert runtime.store.get_session(session["id"])["title"] == "spend by day"


def test_propose_sql_refuses_an_unproved_query(compiled, tmp_path):
    from sahs.evals.substrate import StaticSubstrate
    model = _agent(
        [_call("propose_sql", sql="SELECT nope FROM dw.gms_transaction",
               title="Broken")],
        [{"text": "I fixed it."},
         _call("propose_sql", sql=SPEND_SQL, title="Spend by day")])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       substrate=StaticSubstrate({}))
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "spend by day")
    first = _responses(model)[0]["response"]
    assert first.get("error") and "propose again" in first["hint"]
    # the second, proved, proposal ends the turn without a metric:
    # exploratory, and the line says so
    proposal = _by(events, "proposal")[0]["proposal"]
    assert proposal["status"] == "exploratory"
    assert "not on the meridian line" in proposal["meridian_line"]
    assert len(model.calls) == 2


def test_run_executes_the_proposal_without_the_model(compiled, tmp_path,
                                                     monkeypatch):
    from sahs.evals.substrate import StaticSubstrate
    monkeypatch.setenv("SAHS_ALLOW_LIVE", "1")
    build, _ = compiled
    warehouse = Warehouse()
    model = _agent(
        [{"text": "Here is the query."},
         _call("propose_sql", sql=SPEND_SQL, title="Spend by day",
               metric_id=_certified(build))],
        [{"text": "NEVER: the run needs no model"}])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       substrate=StaticSubstrate({}), runner=warehouse)
    session = runtime.create_session()
    _turn(runtime, session["id"], "spend by day")
    before = runtime.runtime(session["id"]).bus.head()
    started = runtime.run_proposal(session["id"])
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(before)
    assert started["turn_id"] == events[0]["turn_id"]
    assert events[0]["ev"] == "turn_started" and events[0]["mode"] == "run"
    assert len(model.calls) == 1                 # no model call at all
    assert warehouse.calls and "LIMIT" in warehouse.calls[0][0].upper()
    step = _by(events, "tool_step")[0]
    assert step["tool"] == "run_sql" and "2 rows" in step["summary"]
    assert "q1" in step["summary"]
    table = _by(events, "artifact")[0]
    assert table["type"] == "table" and table["title"] == "Spend by day"
    assert table["spec"]["provenance"]["status"] == "certified"
    assert table["spec"]["rows"][0]["acquirer_net_spend"] == 5.0
    assert "watermark" not in table["spec"]
    prose = _prose(events)
    assert prose.startswith("Ran it: 2 rows") and "saved as q1" in prose
    chips = _by(events, "chips")[0]["suggestions"]
    assert chips[0] == {"label": "Chart these rows", "action": "chart",
                        "saved_as": "q1"}
    assert chips[1] == "Build a dashboard from these rows"
    done = _by(events, "turn_done")[0]
    assert done["status"] == "answered" and done["model_calls"] == 0
    # the rows are in the workspace for the next turn's python
    assert (runtime.workspace(session["id"]) / "q1.json").exists()
    # the transcript: the run as the person's turn, the receipts after
    rows = runtime.store.messages(session["id"])
    assert rows[-2]["role"] == "user" and rows[-2]["text"] == "Run: Spend by day"
    assert rows[-1]["payload"]["ran"]["status"] == "answered"
    assert rows[-1]["payload"]["artifacts"] == [table["artifact_id"]]


def test_run_with_a_dashboard_chains_the_build_on_autopilot(
        compiled, tmp_path, monkeypatch):
    from sahs.evals.substrate import StaticSubstrate
    monkeypatch.setenv("SAHS_ALLOW_LIVE", "1")
    build, _ = compiled
    model = _agent(
        [_call("propose_sql", sql=SPEND_SQL, title="Spend by day",
               metric_id=_certified(build))],
        [{"text": "Built from q1: two days of spend."}])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       substrate=StaticSubstrate({}), runner=Warehouse())
    session = runtime.create_session()
    _turn(runtime, session["id"], "spend by day")
    before = runtime.runtime(session["id"]).bus.head()
    runtime.run_proposal(session["id"], sql=SPEND_SQL + " LIMIT 5",
                         dashboard=True)
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(before)
    starts = _by(events, "turn_started")
    assert [s["mode"] for s in starts] == ["run", "autopilot"]
    assert starts[0]["text"] == "Run: Spend by day"
    assert "Build a dashboard" in starts[1]["text"]
    # the edited SQL ran, and the run says so
    assert "with your edits" in _prose(events)
    # the model turn saw the autopilot section and the rows
    assert "Autopilot: run the query yourself" in model.calls[-1]["system"]
    assert "q1" in model.calls[-1]["contents"][-1]["parts"][0]["text"]
    assert [d["status"] for d in _by(events, "turn_done")] == [
        "answered", "answered"]


def test_run_refusals_are_taught_not_swallowed(compiled, tmp_path,
                                               monkeypatch):
    from sahs.evals.substrate import StaticSubstrate
    monkeypatch.delenv("SAHS_ALLOW_LIVE", raising=False)
    monkeypatch.setenv("SAHS_LIVE", "1")          # the laptop's near miss
    build, _ = compiled
    model = _agent([_call("propose_sql", sql=SPEND_SQL,
                          title="Spend by day")])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       substrate=StaticSubstrate({}), runner=Warehouse())
    session = runtime.create_session()
    _turn(runtime, session["id"], "spend by day")
    before = runtime.runtime(session["id"]).bus.head()
    runtime.run_proposal(session["id"])
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(before)
    prose = _prose(events)
    assert prose.startswith("I could not run it")
    assert "configuration, not the query" in prose
    assert "SAHS_LIVE=1 is set, but the switch is SAHS_ALLOW_LIVE=1" \
        in prose
    assert "read on the next run" in prose
    assert _by(events, "turn_done")[0]["status"] == "partial"
    assert not _by(events, "artifact")
    with pytest.raises(ValueError):
        runtime.find_proposal(session["id"], "m_nope")


def test_a_slash_command_loads_that_skill_for_the_turn(compiled,
                                                       tmp_path):
    from sahs.assistant.skills_loader import all_skills
    model = _agent([{"text": "With the pack loaded: a plain answer."}])
    runtime = _runtime(compiled, model, tmp=tmp_path)
    pack = all_skills(None)[0]
    text, loaded = runtime.slash_skill(f"/{pack.name} what joins gms?")
    assert text == "what joins gms?" and loaded == [pack.name]
    assert runtime.slash_skill("/nope what?") == ("/nope what?", [])
    assert runtime.slash_skill("plain ask")[1] == []
    session = runtime.create_session()
    events = _turn(runtime, session["id"], f"/{pack.name} what joins gms?")
    started = _by(events, "turn_started")[0]
    assert started["skills"] == [pack.name]
    assert started["text"] == "what joins gms?"
    # the pack's doctrine reached the model whole
    assert pack.name in model.calls[0]["system"]
    # the person's own words stay in the transcript
    assert runtime.store.messages(session["id"])[0]["text"].startswith(
        f"/{pack.name}")
    assert runtime.mode_for("AUTOPILOT") == "autopilot"
    assert runtime.mode_for("nope") == "chat"
    assert runtime.model_label == "scripted"


def test_the_composer_names_the_model_the_client_will_use(compiled,
                                                          tmp_path,
                                                          monkeypatch):
    from sahs.util.auth import DEFAULT_VERTEX_MODEL
    runtime = _runtime(compiled, None, tmp=tmp_path)
    runtime._model_factory = None            # the laptop: env-bound
    for var in ("VERTEX_MODEL", "LUMI_VERTEX_MODEL", "GEMINI_MODEL"):
        monkeypatch.delenv(var, raising=False)
    assert DEFAULT_VERTEX_MODEL.split("-")[0].capitalize() \
        in runtime.model_label
    monkeypatch.setenv("VERTEX_MODEL", "gemini-2.5-pro")
    assert runtime.model_label == "Gemini 2.5 Pro"


def test_chart_rows_draws_the_saved_rows_without_the_model(
        compiled, tmp_path, monkeypatch):
    from sahs.evals.substrate import StaticSubstrate
    monkeypatch.setenv("SAHS_ALLOW_LIVE", "1")
    build, _ = compiled
    model = _agent([_call("propose_sql", sql=SPEND_SQL,
                          title="Spend by day",
                          metric_id=_certified(build))])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       substrate=StaticSubstrate({}), runner=Warehouse())
    session = runtime.create_session()
    _turn(runtime, session["id"], "spend by day")
    runtime.run_proposal(session["id"])
    assert runtime.wait(session["id"], 60)
    # the run offered the picture as an action chip; the handoff keeps
    # the label only
    chips = _by(runtime.runtime(session["id"]).bus.since(0),
                "chips")[-1]["suggestions"]
    assert chips[0] == {"label": "Chart these rows", "action": "chart",
                        "saved_as": "q1"}
    handoff = runtime.store.get_session(session["id"])["handoff"]
    assert handoff["chips"][0] == "Chart these rows"

    before = runtime.runtime(session["id"]).bus.head()
    started = runtime.chart_rows(session["id"])
    assert started["saved_as"] == "q1"
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(before)
    assert events[0]["ev"] == "turn_started"
    assert events[0]["mode"] == "chart"
    assert events[0]["text"] == "Chart: Spend by day"
    assert len(model.calls) == 1                     # no model call
    step = _by(events, "tool_step")[0]
    assert step["tool"] == "chart"
    assert step["summary"] == "line · 1 series · 2 points · x=part_dt"
    chart = _by(events, "artifact")[0]
    assert chart["type"] == "chart"
    assert chart["title"] == "Spend by day — chart"
    assert chart["spec"]["kind"] == "line"
    assert chart["spec"]["series"] == [{"name": "acquirer_net_spend",
                                        "points": [["2026-08-01", 5.0],
                                                   ["2026-08-02", 7.5]]}]
    assert chart["spec"]["provenance"]["status"] == "certified"
    assert "watermark" not in chart["spec"]
    assert _prose(events).startswith(
        "Drew it: a line chart of acquirer_net_spend by part_dt")
    assert _by(events, "turn_done")[0]["model_calls"] == 0
    rows = runtime.store.messages(session["id"])
    assert rows[-2]["text"] == "Chart: Spend by day"
    assert rows[-1]["payload"]["charted"]["x"] == "part_dt"
    # named axes and kind are honored
    runtime.chart_rows(session["id"], kind="bar", x="part_dt",
                       y=["acquirer_net_spend"])
    assert runtime.wait(session["id"], 60)
    bars = _by(runtime.runtime(session["id"]).bus.since(0),
               "artifact")[-1]
    assert bars["spec"]["kind"] == "bar"
    with pytest.raises(ValueError):
        runtime.find_run(session["id"], "q9")


def test_an_over_ceiling_query_comes_back_once_then_hands_over_with_the_warning(
        compiled, tmp_path, monkeypatch):
    """4.2 TB against a 1 TB ceiling: the model learns it at handover,
    not at Run. Once to narrow; the second time the card says so."""
    from sahs.evals.substrate import DryRunOutcome
    monkeypatch.setenv("SAHS_LIVE_MAX_BYTES", "1000000000000")
    # an earlier module's .env can leave a data project in the process
    # environment, which qualifies the SQL the sandbox sends: price by
    # outcome, not by fingerprint, so the order of the suite is moot
    for name in ("LUMI_BQ_DATA_PROJECT", "BQ_DATA_PROJECT"):
        monkeypatch.delenv(name, raising=False)

    class BigTable:
        name = "big_table"

        def dry_run(self, sql):
            return DryRunOutcome(
                valid=True,
                result_schema=[{"name": "part_dt", "type": "DATE"}],
                bytes_processed=4_200_000_000_000)

    substrate = BigTable()
    model = _agent(
        [_call("propose_sql", sql=SPEND_SQL, title="Spend by day")],
        [{"text": "It cannot be narrowed further; here it is."},
         _call("propose_sql", sql=SPEND_SQL, title="Spend by day")])
    runtime = _runtime(compiled, model, tmp=tmp_path, substrate=substrate)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "spend by day")
    first = _responses(model)[0]["response"]
    assert first["error"].startswith("over_ceiling: this query would scan "
                                     "4.2 TB, over the 1.0 TB live ceiling")
    assert first["kind"] == "cost" and first["yours_to_fix"] is True
    assert "partition column" in first["hint"]
    second = _responses(model)[1]["response"]
    assert second["ok"] and "over the 1.0 TB live ceiling" in second["note"]
    proposal = _by(events, "proposal")[0]["proposal"]
    assert proposal["over_ceiling"] is True
    assert proposal["scan_ceiling_bytes"] == 10**12
    assert proposal["bytes_processed"] == 4_200_000_000_000
    assert _by(events, "turn_done")[0]["status"] == "proposed"
    assert len(model.calls) == 2


class FutureWarehouse(Warehouse):
    """The laptop's table: real days, then rows dated in 2118."""

    def run(self, sql, limit):
        self.calls.append((sql, limit))
        return {"rows": [["2026-08-01", 5.0], ["2026-08-02", 7.5],
                         ["2118-02-14", 0.0]],
                "schema": [{"name": "part_dt", "type": "DATE"},
                           {"name": "acquirer_net_spend",
                            "type": "FLOAT"}],
                "columns": ["part_dt", "acquirer_net_spend"],
                "bytes_processed": 99}


def test_future_dated_rows_are_named_in_the_receipts(compiled, tmp_path,
                                                     monkeypatch):
    """The dashboard whose axis ran to 2118: the run says so first."""
    from sahs.assistant.loop import _future_dates, _future_note
    from sahs.evals.substrate import StaticSubstrate
    import datetime as dt
    rows = [{"part_dt": "2026-08-01", "n": 1},
            {"part_dt": "2118-02-14", "n": 0},
            {"part_dt": "2117-01-01", "n": 0}]
    found = _future_dates(rows, today=dt.date(2026, 9, 3))
    assert found == {"part_dt": {"rows": 2, "latest": dt.date(2118, 2, 14)}}
    assert _future_dates(rows[:1], today=dt.date(2026, 9, 3)) == {}
    assert "2 of these rows are dated after today in part_dt (up to " \
        "2118-02-14)" in _future_note(rows)
    assert "part_dt <= CURRENT_DATE()" in _future_note(rows)

    monkeypatch.setenv("SAHS_ALLOW_LIVE", "1")
    build, _ = compiled
    model = _agent([_call("propose_sql", sql=SPEND_SQL, title="Spend by day",
                          metric_id=_certified(build))])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       substrate=StaticSubstrate({}),
                       runner=FutureWarehouse())
    session = runtime.create_session()
    _turn(runtime, session["id"], "spend by day")
    before = runtime.runtime(session["id"]).bus.head()
    runtime.run_proposal(session["id"])
    assert runtime.wait(session["id"], 60)
    prose = _prose(runtime.runtime(session["id"]).bus.since(before))
    assert prose.startswith("Ran it: 3 rows")
    assert "1 of these rows is dated after today in part_dt (up to " \
        "2118-02-14)" in prose
    before = runtime.runtime(session["id"]).bus.head()
    runtime.chart_rows(session["id"])
    assert runtime.wait(session["id"], 60)
    drawn = _prose(runtime.runtime(session["id"]).bus.since(before))
    assert drawn.startswith("Drew it")
    assert "1 of these points is dated after today" in drawn
    # the doctrine reached the prompt: a window names both ends
    assert "A time window names both ends" in model.calls[0]["system"]


def test_the_prompt_tells_the_model_what_day_it_is(compiled):
    """The model has no clock: the session section says the date and
    the periods the relative words resolve to, and the identity says
    to resolve against it. The horizon line appears only when the
    build knows a newest partition, read as a date from the archive's
    partition id; nothing is invented."""
    import datetime as dt
    from types import SimpleNamespace
    from sahs.assistant.loop import _date_block, system_prompt
    build, _ = compiled
    system = system_prompt(build, today=dt.date(2026, 9, 3))
    assert "<session>" in system
    assert "Today is Thursday, 2026-09-03." in system
    assert ("This month is September 2026; last month was August 2026; "
            "this quarter is Q3 2026, from 2026-07-01; last quarter was "
            "Q2 2026; the year to date runs from 2026-01-01.") in system
    assert "never against your own sense of now" in system
    assert "resolve against that date" in system           # identity
    assert "Data on record runs to 2026-08-22, the newest partition" \
        in system                                  # fixture: 20260822
    # the date is the last section: the cached prefix is untouched
    assert system.index("<session>") > system.index("<memory>")
    # the year boundary: January resolves to last year's December and Q4
    january = _date_block(build, dt.date(2026, 1, 15))
    assert "last month was December 2025" in january
    assert "last quarter was Q4 2025" in january
    # a build that knows its newest partition says so
    horizon = _date_block(SimpleNamespace(tables=[
        {"physical": "dw.a", "partition_latest": "2026-08-30"},
        {"physical": "dw.b", "partition_latest": "2026-08-31"},
        {"physical": "dw.c"}]), dt.date(2026, 9, 3))
    assert "Data on record runs to 2026-08-31, the newest partition" \
        in horizon
    # two calls on the same day are the same bytes: still cacheable
    assert system == system_prompt(build, today=dt.date(2026, 9, 3))

