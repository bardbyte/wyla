"""Synapse v2 §13.1 — the thin loop, the python sandbox, artifacts,
and the rendering rules, against the real compiled fixture build.

The transport is scripted (A1: transport replaced, data never); every
artifact and every sandbox read touches real build content. The
rendering rules are exercised the way the spec states them: the
validator refuses undisclosed numbers with a teaching message, and
the model fixes its own spec.
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
    tmp = tmp_path_factory.mktemp("v2")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v2"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


class Scripted:
    """One queued step per model call on the assistant lane."""

    def __init__(self, steps=()):
        self.steps = list(steps)
        self.prompts: list[str] = []
        self.systems: list[str] = []

    def json(self, prompt, *, system="", temperature=0.0,
             max_tokens=1024):
        if KEY in system:
            self.prompts.append(prompt)
            self.systems.append(system)
            return self.steps.pop(0) if self.steps else None
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        yield ""


def _runtime(compiled, model, tmp=None, **kw):
    from sahs.assistant import AssistantRuntime
    build, _ = compiled
    tmp = tmp or Path(tempfile.mkdtemp())
    return AssistantRuntime(builds_root=build.root.parent,
                            graph_root=tmp / "graph",
                            store_path=tmp / "chat.sqlite3",
                            model_factory=lambda budget: model, **kw)


def _turn(runtime, session_id, text):
    runtime.start_turn(session_id, text)
    assert runtime.wait(session_id, 60)
    return runtime.runtime(session_id).bus.since(0)


def _by(events, name):
    return [e for e in events if e["ev"] == name]


def _prose(events):
    return "".join(e.get("delta", "") for e in events
                   if e["ev"] == "say_token")


# ─── rendering rules (§6), as the validator enforces them ────


def test_rule_one_refuses_undisclosed_numbers():
    sys.path.insert(0, str(SILO))
    from sahs.assistant.artifacts import validate_artifact
    spec = {"kind": "line",
            "series": [{"name": "spend", "points": [["d1", 1.0]]}]}
    normalized, problems = validate_artifact("chart", spec)
    assert normalized is None
    codes = [p["code"] for p in problems]
    assert "provenance_missing" in codes
    assert any("get_definition_line" in p["hint"] for p in problems)


def test_rule_one_passes_disclosed_numbers():
    from sahs.assistant.artifacts import validate_artifact
    spec = {"kind": "line",
            "series": [{"name": "spend", "points": [["d1", 1.0]]}],
            "provenance": {"status": "certified",
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
        "'clean')\n"
        "print(sorted(k for k in meridian.available() "
        "if meridian.available()[k]))\n",
        tmp_path)
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


# ─── the thin loop (§2) end to end ───────────────────────────


def test_reasoning_turn_needs_no_tools(compiled):
    model = Scripted([{
        "say": "Think of churn as a rate and a mix problem: which "
               "merchants leave, and whether the leavers are big.",
        "done": True,
        "chips": ["show churn by segment", "which tables cover this?"],
    }])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "how should I think "
                                           "about merchant churn?")
    assert _by(events, "turn_done")[-1]["status"] == "answered"
    assert "rate and a mix" in _prose(events)
    assert not _by(events, "tool_step")
    assert _by(events, "chips")[0]["suggestions"] == [
        "show churn by segment", "which tables cover this?"]
    stored = runtime.store.messages(session["id"])[-1]
    assert stored["payload"]["chips"]


def test_artifact_refusal_teaches_and_the_second_try_lands(compiled):
    naked = {"kind": "line", "series": [
        {"name": "spend", "points": [["2026-08-01", 4.0],
                                     ["2026-08-02", 6.0]]}]}
    disclosed = dict(naked, provenance={
        "status": "certified",
        "meridian_line": "Using certified 'Acquirer Net Spend' on "
                         "dw.gms_transaction."})
    model = Scripted([
        {"tool": "artifact",
         "args": {"type": "chart", "title": "Spend by day",
                  "spec": naked}},
        {"tool": "artifact",
         "args": {"type": "chart", "title": "Spend by day",
                  "spec": disclosed}},
        {"say": "Spend by day is in the panel, certified.",
         "done": True, "chips": ["break it down by country"]},
    ])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "chart spend by day")
    refusals = [e for e in _by(events, "tool_step")
                if "artifact refused" in e["summary"]]
    assert refusals, "the naked chart was not refused"
    panel = _by(events, "artifact")
    assert len(panel) == 1 and panel[0]["version"] == 1
    assert panel[0]["spec"]["provenance"]["status"] == "certified"
    # the refusal reached the model's next prompt as teaching
    assert any("provenance_missing" in p for p in model.prompts)
    rows = runtime.store.list_artifacts(session["id"])
    assert len(rows) == 1 and rows[0]["title"] == "Spend by day"


def test_artifact_update_makes_a_version_not_a_copy(compiled):
    disclosed = {"kind": "bar", "series": [
        {"name": "n", "points": [["a", 1.0]]}],
        "provenance": {"status": "certified",
                       "meridian_line": "Using certified "
                                        "'Transaction Count'."}}
    v2 = dict(disclosed, series=[{"name": "n",
                                  "points": [["a", 1.0], ["b", 2.0]]}])
    model = Scripted([
        {"tool": "artifact", "args": {"type": "chart",
                                      "title": "Counts", "spec": disclosed}},
        {"say": "Done.", "done": True},
    ])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    _turn(runtime, session["id"], "chart counts")
    artifact_id = runtime.store.list_artifacts(
        session["id"])[0]["artifact_id"]
    model.steps = [
        {"tool": "artifact_update",
         "args": {"artifact_id": artifact_id, "spec": v2}},
        {"say": "Extended.", "done": True},
    ]
    events = _turn(runtime, session["id"], "add point b")
    assert _by(events, "artifact")[-1]["version"] == 2
    versions = runtime.store.artifact_versions(artifact_id)
    assert [v["version"] for v in versions] == [1, 2]
    assert runtime.store.get_artifact(
        artifact_id, 1)["spec"]["series"][0]["points"] == [["a", 1.0]]


def test_python_turn_reads_the_build(compiled):
    model = Scripted([
        {"tool": "python",
         "args": {"code": "import meridian\n"
                          "c = [m for m in meridian.metrics() "
                          "if m['status'] == 'certified']\n"
                          "print(len(c), 'certified')"}},
        {"say": "Counted straight from the build.", "done": True},
    ])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "how many certified?")
    build, _ = compiled
    certified = sum(1 for m in build.metrics
                    if m["status"] == "certified")
    step = _by(events, "tool_step")[0]
    assert f"{certified} certified" in step["summary"]


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

    model = Scripted([
        {"tool": "run_sql",
         "args": {"sql": "SELECT part_dt, sum(trans_usd_am) AS "
                         "acquirer_net_spend FROM dw.gms_transaction "
                         "GROUP BY part_dt", "mode": "snapshot"}},
        {"tool": "python",
         "args": {"code": "import meridian\n"
                          "print(meridian.rows('q1')[0]"
                          "['acquirer_net_spend'])"}},
        {"say": "The rows made it to python.", "done": True},
    ])
    runtime = _runtime(compiled, model, tmp=tmp_path,
                       snapshot_runner=Extract())
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "spend by day, checked")
    sql_step, py_step = _by(events, "tool_step")[:2]
    assert "saved" in sql_step["summary"] or "q1" in sql_step["summary"]
    assert "5.0" in py_step["summary"]


def test_strict_json_failure_is_an_honest_partial(compiled):
    model = Scripted(["not json", "still not json"])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "anything")
    assert _by(events, "turn_done")[-1]["status"] == "partial"
    assert "I stopped before finishing" in _prose(events)


def test_stop_lands_in_a_recorded_stop_not_a_vanished_turn(compiled):
    runtime_box = {}

    class Stopper(Scripted):
        def json(self, prompt, *, system="", temperature=0.0,
                 max_tokens=1024):
            if KEY in system and not self.prompts:
                runtime_box["rt"].stop(runtime_box["sid"])
            return super().json(prompt, system=system,
                                temperature=temperature,
                                max_tokens=max_tokens)

    model = Stopper([{"tool": "note",
                      "args": {"text": "was mid-look"}},
                     {"say": "never reached", "done": True}])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    runtime_box.update(rt=runtime, sid=session["id"])
    events = _turn(runtime, session["id"], "long question")
    done = _by(events, "turn_done")[-1]
    assert done["status"] == "stopped"
    assert "stopped by the analyst" in _prose(events)
    assert runtime.store.messages(session["id"])[-1]["role"] == \
        "assistant"


def test_second_turn_sees_the_first(compiled):
    model = Scripted([
        {"say": "Certified spend means the meridian definition.",
         "done": True},
        {"say": "As I said, the meridian one.", "done": True},
    ])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    _turn(runtime, session["id"], "what does certified spend mean?")
    _turn(runtime, session["id"], "which one did you mean?")
    second_prompt = model.prompts[-1]
    assert "what does certified spend mean?" in second_prompt
    assert "meridian definition" in second_prompt


def test_the_saw_trail_and_the_event_family(compiled):
    from sahs.assistant.events import ASSISTANT_EVENTS
    model = Scripted([
        {"tool": "list_tables", "args": {}},
        {"say": "Three tables on the shelf.", "done": True},
    ])
    runtime = _runtime(compiled, model)
    session = runtime.create_session()
    events = _turn(runtime, session["id"], "what tables exist?")
    kinds = {e["ev"] for e in events}
    assert kinds <= set(ASSISTANT_EVENTS)
    prompts = _by(events, "model_prompt")
    assert prompts[0]["kind"] == "system"
    assert KEY in prompts[0]["content"]
    assert any(p["kind"] == "step" for p in prompts)
    assert _by(events, "tool_result")[0]["tool"] == "list_tables"
