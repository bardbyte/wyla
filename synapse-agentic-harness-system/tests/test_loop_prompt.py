"""Agent Loop v1 §9.3 — system prompt v1, the world digest, session
skills, and the "what the model saw" event trail.

The digest is held to the reality law (every line derivable from the
build), the prompt to its budget and its sections, skills to their
two pins (steer-never-assert is a prompt-text pin; explicit-and-
visible is store + event), and the panel to its ground truth: the
events file carries exactly what the model saw.
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
    tmp = tmp_path_factory.mktemp("loop_prompt")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "loopprompt"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    sys.path.insert(0, str(SILO))
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


# ─── the digest: real facts, bounded, deterministic ──────────


def test_digest_is_real_bounded_and_deterministic(compiled):
    from sahs.loop.digest import MAX_CHARS, synapse_digest
    build, _ = compiled
    once, twice = synapse_digest(build), synapse_digest(build)
    assert once == twice
    assert len(once) <= MAX_CHARS
    assert build.version in once
    # a certified metric the build actually holds appears by name
    certified = next(m for m in build.metrics
                     if m["status"] == "certified")
    assert certified["label"] in once
    # the gaps section counts, it never invents
    assert "## known gaps" in once


def test_digest_reports_grainless_metrics_as_a_gap(compiled):
    from sahs.loop.digest import synapse_digest
    build, _ = compiled
    grainless = sum(1 for m in build.metrics
                    if not (m.get("grain") or "").strip())
    assert f"{grainless} metrics carry no recorded grain" in \
        synapse_digest(build)


# ─── the prompt: six sections, versioned, budgeted ───────────


def test_prompt_carries_its_sections_and_its_version(compiled):
    from sahs.loop.prompt import PROMPT_VERSION, system_prompt
    build, _ = compiled
    text = system_prompt(build)
    # the §4 architecture, in order
    for marker in ("You navigate a governed data graph",  # identity/key
                   "# SYNAPSE.md",                        # digest
                   "## How to find things",               # doctrine
                   "## When to stop",                     # stop
                   "## Two traces",                       # few-shot
                   "## Tone",                             # E22
                   "## Each step"):                       # protocol
        assert marker in text, marker
    assert text.index("# SYNAPSE.md") < text.index("## How to find")
    assert PROMPT_VERSION == "loop-prompt/1"
    # the budget: prompt without tools ≈ 2.5K tokens
    assert len(text) < 14000, f"prompt grew to {len(text)} chars"


def test_prompt_is_byte_identical_across_calls(compiled):
    from sahs.loop.prompt import system_prompt
    build, _ = compiled
    assert system_prompt(build) == system_prompt(build)  # cacheable


# ─── skills: files → picker → context, explicit and visible ──


def _write_skill(root: Path, name: str, title: str, body: str) -> None:
    skills = root / "skills"
    skills.mkdir(parents=True, exist_ok=True)
    (skills / f"{name}.md").write_text(f"# {title}\n\n{body}\n",
                                       encoding="utf-8")


def test_skills_list_parse_and_load(tmp_path):
    sys.path.insert(0, str(SILO))
    from sahs.loop.skills import list_skills, load_skills
    _write_skill(tmp_path, "fiscal", "Fiscal calendar",
                 "Quarters are fiscal, ending March and June.")
    _write_skill(tmp_path, "smb", "SMB reading",
                 "SMB means the small-business segment.")
    skills = list_skills(tmp_path)
    assert [s.name for s in skills] == ["fiscal", "smb"]
    assert skills[0].title == "Fiscal calendar"
    assert skills[0].description.startswith("Quarters are fiscal")
    loaded, missing = load_skills(tmp_path, ["smb", "nope"])
    assert [s.name for s in loaded] == ["smb"]
    assert missing == ["nope"]


def test_rendered_skills_carry_the_steer_never_assert_pin(tmp_path):
    from sahs.loop.skills import load_skills, render_skills
    _write_skill(tmp_path, "fiscal", "Fiscal calendar", "Quarters.")
    loaded, _ = load_skills(tmp_path, ["fiscal"])
    text = render_skills(loaded)
    assert "Skills the analyst loaded" in text
    assert "cannot add tables, metrics, or numbers" in text
    assert "Quarters." in text
    assert render_skills([]) == ""     # skill-less prompt unchanged


def test_prompt_includes_loaded_skills_between_doctrine_and_stop(
        compiled, tmp_path):
    from sahs.loop.prompt import system_prompt
    from sahs.loop.skills import load_skills
    build, _ = compiled
    _write_skill(tmp_path, "fiscal", "Fiscal calendar",
                 "Quarters are fiscal.")
    loaded, _ = load_skills(tmp_path, ["fiscal"])
    text = system_prompt(build, loaded)
    assert "Quarters are fiscal." in text
    assert text.index("## How to find things") \
        < text.index("Fiscal calendar") < text.index("## When to stop")


def test_store_skills_roundtrip_and_forward_migration(tmp_path):
    import sqlite3
    from sahs.ask.store import SessionStore
    # a store born BEFORE the skills column: same tables, no column
    old_db = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(old_db)
    conn.executescript(
        "CREATE TABLE sessions (id TEXT PRIMARY KEY, kind TEXT NOT "
        "NULL, title TEXT NOT NULL DEFAULT '', build_id TEXT NOT "
        "NULL DEFAULT '', actor TEXT NOT NULL DEFAULT 'admin', "
        "created_at TEXT NOT NULL, updated_at TEXT NOT NULL);")
    conn.execute("INSERT INTO sessions VALUES ('s_old','analyst','',"
                 "'','admin','2026-01-01','2026-01-01')")
    conn.commit()
    conn.close()
    store = SessionStore(old_db)          # migrates on open
    assert store.get_session("s_old")["skills"] == []
    store.set_skills("s_old", ["fiscal"])
    assert store.get_session("s_old")["skills"] == ["fiscal"]


def test_runtime_refuses_unknown_and_overlong_skill_sets(compiled,
                                                         tmp_path):
    from sahs.ask import AskRuntime
    build, _ = compiled
    _write_skill(tmp_path / "graph", "fiscal", "Fiscal", "Quarters.")
    runtime = AskRuntime(builds_root=build.root.parent,
                         graph_root=tmp_path / "graph",
                         store_path=tmp_path / "s.sqlite3",
                         model_factory=lambda budget: None)
    session = runtime.create_session("analyst")
    assert [s["name"] for s in runtime.skills()] == ["fiscal"]
    ok = runtime.set_skills(session["id"], ["fiscal"])
    assert ok["ok"] is True
    bad = runtime.set_skills(session["id"], ["invented"])
    assert bad["ok"] is False and "invented" in bad["reason"]
    too_many = runtime.set_skills(session["id"],
                                  ["a", "b", "c", "d", "e"])
    assert too_many["ok"] is False and "at most" in too_many["reason"]
    # the refused sets never landed
    assert runtime.store.get_session(session["id"])["skills"] == \
        ["fiscal"]


# ─── the loop wears the prompt; the events carry what it saw ─


class Navigator:
    def __init__(self, steps=()):
        self.steps = list(steps)
        self.systems: list[str] = []
        self.calls: list[str] = []

    def json(self, prompt, *, system="", temperature=0.0,
             max_tokens=1024):
        if "You navigate a governed data graph" in system:
            self.calls.append("navigate")
            self.systems.append(system)
            return self.steps.pop(0) if self.steps else None
        if "You compose ONE BigQuery SELECT" in system:
            return {"sql": GOOD_SQL, "why": "certified"}
        if "skeptical reviewer" in system:
            return {"grounded": True, "why": "traces"}
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        yield "the governed answer."


def test_loop_runs_under_prompt_v1_with_skills_and_emits_the_trail(
        compiled, tmp_path, monkeypatch):
    from sahs.ask import AskRuntime
    from sahs.loop.prompt import PROMPT_VERSION
    monkeypatch.setenv("SYNAPSE_NAVIGATE", "1")
    build, _ = compiled
    spend = next(m for m in build.metrics
                 if m["label"] == "Acquirer Net Spend"
                 and m["status"] == "certified")
    _write_skill(tmp_path / "graph", "fiscal", "Fiscal calendar",
                 "Quarters are fiscal, ending March.")
    model = Navigator(steps=[
        {"tool": "plan_set", "args": {"patch": {"metric": spend["id"],
                                                "grain": "transaction"}}},
        {"final": True},
    ])
    runtime = AskRuntime(builds_root=build.root.parent,
                         graph_root=tmp_path / "graph",
                         store_path=tmp_path / "s.sqlite3",
                         model_factory=lambda budget: model)
    session = runtime.create_session("analyst")
    assert runtime.set_skills(session["id"], ["fiscal"])["ok"]
    runtime.start_turn(session["id"], NO_MATCH)
    assert runtime.wait(session["id"], 30)
    events = runtime.runtime(session["id"]).bus.since(0)

    # the navigator ran under the assembled prompt: digest + skill
    system = model.systems[0]
    assert "# SYNAPSE.md" in system
    assert "Quarters are fiscal, ending March." in system
    assert "## Two traces" in system

    started = next(e for e in events if e["ev"] == "loop_started")
    assert started["prompt_version"] == PROMPT_VERSION
    assert started["skills"] == ["fiscal"]

    prompts = [e for e in events if e["ev"] == "loop_prompt"]
    system_ev = next(p for p in prompts if p["kind"] == "system")
    assert "You navigate a governed data graph" in system_ev["content"]
    step_prompts = [p for p in prompts if p["kind"] == "step"]
    # one per model step, numbered to match loop_step events
    step_ns = [e["n"] for e in events if e["ev"] == "loop_step"]
    assert step_ns and set(step_ns) <= {p["n"] for p in step_prompts}
    assert any(e["ev"] == "answer_payload" for e in events)
