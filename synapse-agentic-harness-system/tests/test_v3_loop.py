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
    for tag in ("<chain>", "<graph>", "<skills>", "<memory>"):
        assert tag in system, tag
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
                     "python", "check", "artifact", "ask",
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
