"""Synapse v2 §13.6 — the assistant eval harness math, pinned.

There is deliberately no scripted baseline number: the suites grade
the model's judgement, so a scripted assistant would grade the
script. What CI pins instead is the HARNESS: the task files are
real (every tool and type they name exists), the graders separate
an honest actor from a dishonest one on the right dimension, and
the runner drives the real loop end to end. Bad-actor records are
fed to the graders synthetically where the validator would (rightly)
refuse to produce them through the live path — the grader must
catch regressions even if the validator someday fails.
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
    tmp = tmp_path_factory.mktemp("v2evals")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v2e"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


class Scripted:
    def __init__(self, steps=()):
        self.steps = list(steps)

    def json(self, prompt, *, system="", temperature=0.0,
             max_tokens=1024):
        if KEY in system:
            return self.steps.pop(0) if self.steps else None
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        yield ""


def _certified(build):
    return next(m for m in build.metrics
                if (m.get("status_served") or m.get("status"))
                == "certified")


# ─── the task files are real ─────────────────────────────────


def test_task_files_name_only_real_things(compiled):
    sys.path.insert(0, str(SILO))
    from sahs.assistant.artifacts import TYPES
    from sahs.assistant.evals import CHECK_TOOLS, KINDS, load_tasks
    from sahs.assistant.skills_loader import builtin_skills
    tasks = load_tasks()
    assert len(tasks) >= 14
    ids = [t["id"] for t in tasks]
    assert len(ids) == len(set(ids))
    packs = {p.name for p in builtin_skills()}
    for task in tasks:
        assert task["kind"] in KINDS, task["id"]
        assert task["question"] and task["why"], task["id"]
        expect = task.get("expect", {})
        if expect.get("type"):
            assert expect["type"] in TYPES, task["id"]
        for check in expect.get("checks_any") or []:
            assert check in CHECK_TOOLS, task["id"]
        if expect.get("skill"):
            assert expect["skill"] in packs, task["id"]


# ─── artifact grading: honest lands, dishonest is named ──────


def test_artifact_grader_through_the_real_loop(compiled):
    from sahs.assistant.evals import load_tasks, run_task
    build, _ = compiled
    certified = _certified(build)
    task = next(t for t in load_tasks("artifact")
                if t["id"] == "art_chart_certified_trend")
    model = Scripted([
        {"tool": "artifact",
         "args": {"type": "chart", "title": "Spend by day", "spec": {
             "kind": "line",
             "series": [{"name": "spend",
                         "points": [["d1", 4.0], ["d2", 5.0]]}],
             "provenance": {"status": "certified",
                            "metric_id": certified["id"],
                            "meridian_line": "Certified spend."}}}},
        {"say": "Certified spend, drawn.", "done": True},
    ])
    row = run_task(build, lambda budget: model, task)
    assert row["found"] is True, row
    assert row["statuses"] == ["certified"]
    assert row["export_ok"] is True
    assert row["hygiene"]["steps"] == 1


def test_artifact_grader_catches_synthetic_dishonesty():
    from sahs.assistant.evals import grade_artifact
    naked = [{"ev": "artifact", "artifact_id": "a1", "version": 1,
              "type": "chart", "title": "x",
              "spec": {"kind": "line", "series": [
                  {"name": "s", "points": [["d", 1.0]]}]}},
             {"ev": "turn_done", "status": "answered"}]
    row = grade_artifact({"id": "t", "kind": "artifact",
                          "expect": {"type": "chart"}}, naked)
    assert row["produced"] and row["wrong"]
    assert "undisclosed_number" in row["problems"]

    unmarked = [{"ev": "artifact", "artifact_id": "a2", "version": 1,
                 "type": "kpi", "title": "n",
                 "spec": {"value": 5.0, "provenance": {
                     "status": "composed",
                     "meridian_line": "Composed."}}},
                {"ev": "turn_done", "status": "answered"}]
    row = grade_artifact({"id": "t2", "kind": "artifact",
                          "expect": {"type": "kpi"}}, unmarked)
    assert "naked_composed" in row["problems"]

    absent = grade_artifact({"id": "t3", "kind": "artifact",
                             "expect": {"type": "dashboard"}},
                            [{"ev": "turn_done",
                              "status": "answered"}])
    assert not absent["produced"] and not absent["found"] \
        and not absent["wrong"]


# ─── reasoning grading: thinking, with no tools leaking ──────


def test_reasoning_grader_separates_clean_from_leaky(compiled):
    from sahs.assistant.evals import load_tasks, run_task
    build, _ = compiled
    task = next(t for t in load_tasks("reasoning")
                if t["id"] == "rsn_churn_frame")
    clean = Scripted([
        {"say": "Split churn into a rate question and a mix "
                "question, then read each cohort separately: "
                "who leaves, how large they are, and whether the "
                "leavers differ from the stayers. Only then is a "
                "single churn number worth quoting, because the "
                "aggregate hides the cohort story completely.",
         "done": True, "chips": ["pull the churn cohorts"]}])
    row = run_task(build, lambda budget: clean, task)
    assert row["found"] is True and row["tools_used"] == 0

    leaky = Scripted([
        {"tool": "list_tables", "args": {}},
        {"say": "Split churn into rate and mix and read each "
                "cohort separately before quoting one number — "
                "the aggregate hides the story of who actually "
                "left and how large those merchants were.",
         "done": True}])
    row = run_task(build, lambda budget: leaky, task)
    assert row["wrong"] is True and row["found"] is False

    # the calibrated judge outranks the keyword floor
    from sahs.assistant.evals import grade_reasoning
    events = [{"ev": "say_token",
               "delta": "rate and mix and cohort " * 20},
              {"ev": "turn_done", "status": "answered"}]
    hard = grade_reasoning(task, events,
                           judge=lambda q, a: False)
    assert hard["keyword_ok"] is True and hard["found"] is False


# ─── playbook grading: a why runs its checks, or it is named ─


def test_playbook_grader_through_the_real_loop(compiled,
                                               monkeypatch):
    from sahs.assistant.evals import load_tasks, run_task
    from sahs.evals.substrate import DryRunOutcome

    class NoWarehouse:
        def dry_run(self, sql):
            return DryRunOutcome(valid=True, result_schema=None)

    import sahs.evals.substrate as substrate_module
    monkeypatch.setattr(substrate_module, "BQDryRun", NoWarehouse)

    class Extract:
        name = "frozen_extract"
        calls = 0

        def run(self, sql, limit):
            Extract.calls += 1
            if Extract.calls == 1:
                return {"rows": [{"country_cd": "CA", "spend": 40.0},
                                 {"country_cd": "US", "spend": 60.0}],
                        "schema": []}
            return {"rows": [{"spend": 100.0}], "schema": []}

    build, _ = compiled
    task = next(t for t in load_tasks("playbook")
                if t["id"] == "pb_why_spend_change")
    diligent = Scripted([
        {"tool": "load_skill", "args": {"name": "analysis-playbooks"}},
        {"tool": "run_sql", "args": {
            "sql": "SELECT country_cd, sum(trans_usd_am) AS spend "
                   "FROM dw.gms_transaction GROUP BY country_cd",
            "mode": "snapshot"}},
        {"tool": "run_sql", "args": {
            "sql": "SELECT sum(trans_usd_am) AS spend FROM "
                   "dw.gms_transaction", "mode": "snapshot"}},
        {"tool": "check_part_whole", "args": {"breakdown": "q1",
                                              "total": "q2"}},
        {"say": "The split adds up; the change is mix, not rate.",
         "done": True, "chips": ["chart the mix shift"]}])
    row = run_task(build, lambda budget: diligent, task,
                   snapshot_runner=Extract())
    assert row["found"] is True, row
    assert row["checks_passed"] == ["check_part_whole"]
    assert row["queries"] == 2 and row["skill_ok"] is True

    storyteller = Scripted([
        {"say": "Spend changed because volumes shifted between "
                "countries; the mix moved toward larger markets.",
         "done": True}])
    row = run_task(build, lambda budget: storyteller, task)
    assert row["wrong"] is True and row["found"] is False
    assert row["checks_passed"] == []


# ─── the report: two-number lines + the rows beneath them ────


def test_summarize_and_markdown_carry_the_lines():
    from sahs.assistant.evals import render_markdown, summarize
    hygiene = {"steps": 3, "sampled_before_filter": True,
               "read_before_query": True, "artifact_refusals": 1,
               "recovered_from_refusal": True, "tools": []}
    rows = [
        {"id": "a1", "kind": "artifact", "found": True,
         "wrong": False, "problems": [], "hygiene": dict(hygiene)},
        {"id": "a2", "kind": "artifact", "found": False,
         "wrong": True, "problems": ["undisclosed_number"],
         "hygiene": dict(hygiene)},
        {"id": "r1", "kind": "reasoning", "found": True,
         "wrong": False, "tools_used": 0, "hygiene": dict(hygiene)},
        {"id": "p1", "kind": "playbook", "found": True,
         "wrong": False, "checks_passed": ["check_part_whole"],
         "hygiene": dict(hygiene)},
    ]
    report = summarize(rows)
    assert report["suites"]["artifact"]["found_pct"] == 50.0
    assert report["suites"]["artifact"]["wrong"] == 1
    assert report["suites"]["reasoning"]["found_pct"] == 100.0
    assert report["hygiene"]["refusal_recovery_rate"] == 1.0
    markdown = render_markdown(report, label="test")
    assert "artifact: 50.0% / 1" in markdown
    assert "undisclosed_number" in markdown
    assert "| p1 | playbook | ✓ |" in markdown
