"""Agent Loop v1 §9.4 — the navigation task set and its grader.

Two duties: (1) pin every task to REALITY — each expected metric,
table, and filter column must exist in the fixture build, so the
answer key can never drift from the world; (2) pin the grader math on
scripted trajectories driven through the real engine, so the number
the laptop run produces is a number this suite has already audited.
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


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("nav_eval")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "naveval"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


# ─── the answer key can never drift from the world ───────────


def test_thirty_tasks_each_pinned_to_the_real_build(compiled):
    from sahs.evals.navigation import load_tasks
    build, _ = compiled
    tasks = load_tasks()
    assert len(tasks) == 30
    assert len({t["id"] for t in tasks}) == 30
    metric_ids = {m["id"] for m in build.metrics}
    for task in tasks:
        expect = task["expect"]
        assert expect["mode"] in ("bind", "either", "no_answer"), \
            task["id"]
        for metric in expect.get("metric_one_of", []):
            assert metric in metric_ids, f"{task['id']}: {metric}"
        if expect.get("table"):
            assert expect["table"] in build.schema, task["id"]
        if expect.get("filter_column"):
            table = expect.get("table")
            assert table, f"{task['id']}: filter_column needs table"
            # real = in the compiled schema, OR witnessed inside a
            # binding's SQL on that table (mined columns are evidence
            # too — the fixture's card_present binds pos_entry_cd
            # which extraction never profiled)
            in_schema = expect["filter_column"] in build.schema[table]
            in_bindings = any(
                expect["filter_column"] in (b.get("canonical_sql")
                                            or "")
                for b in build.bindings if b.get("table") == table)
            assert in_schema or in_bindings, \
                f"{task['id']}: {expect['filter_column']}"
        for forbidden in expect.get("forbidden_tables", []):
            assert forbidden in build.schema, task["id"]
        assert task.get("why"), f"{task['id']} carries no rationale"


def test_routes_cover_the_spec_families():
    from sahs.evals.navigation import load_tasks
    routes = {t["route"] for t in load_tasks()}
    # cold discovery, ambiguity settling, filter navigation, fast-path
    # controls, and honest stops — §8's shape of the set
    assert {"cold", "ambiguous", "filter", "fast",
            "honest"} <= routes


# ─── the grader math, audited on real engine runs ────────────


class Navigator:
    def __init__(self, steps=()):
        self.steps = list(steps)

    def json(self, prompt, *, system="", temperature=0.0,
             max_tokens=1024):
        if "You navigate a governed data graph" in system:
            return self.steps.pop(0) if self.steps else None
        if "You compose ONE BigQuery SELECT" in system:
            return {"sql": GOOD_SQL, "why": "certified"}
        if "skeptical reviewer" in system:
            return {"grounded": True, "why": "traces"}
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        yield "the governed answer."


def _run_one(compiled, task, steps):
    from sahs.evals.navigation import run_navigation
    build, _ = compiled
    model = Navigator(steps=steps)
    report = run_navigation(build, lambda budget: model, [task])
    return report, report["rows"][0]


def test_grader_credits_a_clean_find_and_reads_hygiene(compiled):
    build, _ = compiled
    task = {"id": "t", "route": "cold", "question":
            "acquiring volume in dollars",
            "expect": {"mode": "either",
                       "metric_one_of": ["metric:301a4f124096"],
                       "table": "dw.gms_transaction"}}
    report, row = _run_one(compiled, task, [
        {"tool": "search_semantics", "args": {"query": "spend"}},
        {"tool": "plan_set",
         "args": {"patch": {"metric": "metric:301a4f124096",
                            "grain": "transaction"}}},
        {"final": True},
    ])
    assert row["found"] is True and row["wrong"] is False
    assert row["precision_ok"] is True
    assert row["looked_before_bind"] is True     # searched, then bound
    assert report["found_pct"] == 100.0
    assert report["hygiene"]["steps_per_task"] >= 3


def test_grader_flags_a_wrong_bind_and_a_dragged_table(compiled):
    task = {"id": "t", "route": "ambiguous", "question":
            "approval rate for small business applicants",
            "expect": {"mode": "either",
                       "metric_one_of": ["metric:40a21c9f3620"],
                       "table": "dw.sbs_new_accounts",
                       "forbidden_tables": ["dw.wwcas_authorization"]}}
    report, row = _run_one(compiled, task, [
        {"tool": "read_card", "args": {"id": "table:wwcas_authorization"}},
        {"tool": "plan_set",
         "args": {"patch": {"metric": "metric:6551441a7e3b",
                            "grain": "daily"}}},
        {"final": True},
    ])
    assert row["found"] is False
    assert row["wrong"] is True                  # answered off the key
    assert row["dragged"] == ["dw.wwcas_authorization"]
    assert report["wrong_when_found"] == 1
    assert report["precision_violations"] == 1


def test_grader_counts_an_honest_ask_as_found_under_either(compiled):
    task = {"id": "t", "route": "ambiguous", "question":
            "how often do card authorizations succeed",
            "expect": {"mode": "either",
                       "metric_one_of": ["metric:6551441a7e3b",
                                         "metric:c724216905c1"]}}
    _report, row = _run_one(compiled, task, [
        {"tool": "ask_user", "args": {
            "question": "Two approval-rate variants exist. Which?",
            "options": [{"value": "a", "label": "share of decisions",
                         "evidence": "safe_divide variant"},
                        {"value": "b", "label": "average of flags",
                         "evidence": "avg variant"}]}},
    ])
    assert row["status"] == "clarify"
    assert row["found"] is True and row["asked"] is True


def test_grader_wants_honest_stops_on_no_answer_tasks(compiled):
    task = {"id": "t", "route": "honest",
            "question": "how many active locations in force",
            "expect": {"mode": "no_answer"}}
    _report, row = _run_one(compiled, task, [
        {"tool": "note", "args": {"text": "no locations metric in "
                                          "any card"}},
        "garbage", "garbage",        # strict-JSON failure → partial
    ])
    assert row["status"] == "partial"
    assert row["found"] is True                  # honesty is the win
    assert row["budget_stop"] is True


def test_grader_reads_the_literal_check_rate(compiled):
    task = {"id": "t", "route": "filter",
            "question": "spend from merchants in Britain",
            "expect": {"mode": "either",
                       "metric_one_of": ["metric:301a4f124096"],
                       "table": "dw.gms_transaction",
                       "filter_column": "country_cd"}}
    _report, row = _run_one(compiled, task, [
        {"tool": "sample_values",
         "args": {"table": "gms_transaction",
                  "column": "country_cd"}},
        {"tool": "plan_set",
         "args": {"patch": {"metric": "metric:301a4f124096",
                            "grain": "transaction",
                            "filters": {"country_cd": "GB"}}}},
        {"final": True},
    ])
    assert row["found"] is True
    assert row["sampled_before_filter"] is True
