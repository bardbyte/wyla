"""Agent craft skills (progressive disclosure) + analyst tool wiring."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.analyst.agent_skills import list_agent_skills, load_agent_skill  # noqa: E402


def test_skill_index_has_all_three_with_when_to_use():
    index = list_agent_skills()
    assert index["status"] == "ok"
    by_name = {s["name"]: s["description"] for s in index["skills"]}
    assert set(by_name) == {
        "response-design", "visualization", "executive-communication",
    }
    # descriptions carry both WHAT and WHEN (discovery quality)
    assert "Use" in by_name["response-design"]
    assert len(by_name["visualization"]) <= 1024


def test_load_skill_returns_body_without_frontmatter():
    out = load_agent_skill("response-design")
    assert out["status"] == "ok"
    assert out["body"].startswith("# Response design")
    assert "---" not in out["body"][:10]
    # the decision table is present — the part the model actually uses
    assert "stat tile" in out["body"]


def test_load_unknown_skill_lists_available():
    out = load_agent_skill("nope")
    assert out["status"] == "error"
    assert "response-design" in out["error"]


def test_analyst_toolbelt_composition(tmp_path, monkeypatch):
    """The agent gets graph + gated warehouse + presentation + skills +
    sandbox — and nothing overlapping."""
    import json

    from synapse.graph.builder import build_graph_from_sources
    from synapse.loaders.skills_loader import load_skills_library

    fixture = Path(__file__).parent / "fixtures" / "skills_library"
    sources = tmp_path / "sources"
    load_skills_library(fixture, out_dir=sources)
    store = build_graph_from_sources(sources)
    snap = tmp_path / "snap.json"
    store.save_json(snap)

    monkeypatch.setenv("SYNAPSE_GRAPH_PATH", str(snap))
    monkeypatch.setenv("SYNAPSE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    import apps.analyst.tools as tools_mod
    tools_mod._service.cache_clear()
    tools_mod._runner.cache_clear()

    names = [t.__name__ for t in tools_mod.build_analyst_tools()]
    assert names.count("validate_sql_plan") == 1     # no duplicates
    for expected in ("search_entities", "get_skill", "dry_run_sql",
                     "execute_sql", "render_chart", "render_dashboard",
                     "list_agent_skills", "load_agent_skill",
                     "run_python_analysis"):
        assert expected in names, expected

    # gated execution refuses cleanly with no client, pointing at the laptop
    out = tools_mod.dry_run_sql(
        "SELECT rpt_month FROM common.roll_rate_calc GROUP BY rpt_month")
    assert out["status"] == "refused" and out["code"] == "no_client"

    # guardrails refuse BEFORE the missing client is even consulted
    out = tools_mod.execute_sql(
        "SELECT cm11_encrypted FROM common.roll_rate_calc")
    assert out["code"] == "guardrail_violation"

    # audit ledger recorded both attempts
    ledger = tmp_path / "artifacts" / "audit" / "warehouse_ledger.jsonl"
    lines = [json.loads(l) for l in ledger.read_text().splitlines()]
    assert len(lines) == 2

    # chart rendering works end-to-end and writes a themed artifact
    out = tools_mod.render_chart({
        "kind": "stat", "title": "C-30", "value": 0.0231,
        "format": "percent", "delta": 0.0014, "delta_is_good": False,
    })
    assert out["status"] == "ok"
    html = Path(out["path"]).read_text()
    assert "2.31%" in html and "data-theme" in html

    # bad spec → instructive error, not a stack trace
    out = tools_mod.render_chart({"kind": "line", "title": "x", "series": [
        {"name": f"s{i}", "points": [["a", 1.0]]} for i in range(5)
    ]})
    assert out["status"] == "error" and "Fold the tail" in out["error"]
