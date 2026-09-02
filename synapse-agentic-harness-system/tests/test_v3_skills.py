"""Synapse v3 — skills: the built-in packs are real, bounded doctrine
that names only tools the v3 kit has; both shelves merge with the
built-in winning; the shelf is offered by name and a loaded pack
reaches the model WHOLE as the tool's own result."""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
sys.path.insert(0, str(SILO))
KEY = "You are Synapse, an analytical colleague"
PACKS = ["lumi-data-connect", "analysis-playbooks",
         "dashboard-design", "executive-summary"]
GHOSTS = re.compile(
    r"\b(check_\w+|verify_answer|subgraph|search_semantics|grep_cards|"
    r"read_card|get_join_paths|get_definition_line|list_skills|"
    r"list_metrics|plan_set|whatif|constellation|artifact_update|"
    r"list_artifacts|ask_user|delegate_scout|resolve\()")


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("v3skills")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v3s"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def _runtime(compiled, model, tmp=None):
    from sahs.assistant import AssistantRuntime
    build, _ = compiled
    tmp = tmp or Path(tempfile.mkdtemp())
    return AssistantRuntime(builds_root=build.root.parent,
                            graph_root=tmp / "graph",
                            store_path=tmp / "chat.sqlite3",
                            model_factory=lambda budget: model)


def _kit(compiled, tmp_path, graph_root=None):
    from sahs.assistant.kit import build_kit
    from sahs.assistant.sandbox import prepare_workspace
    from sahs.assistant.state import AssistantState
    from sahs.assistant.store import AssistantStore
    build, _ = compiled
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    state = AssistantState()
    workspace = tmp_path / "ws"
    prepare_workspace(workspace, build.root)
    tools = build_kit(build, state, store=store,
                      session_id=session["id"], turn_id="t1",
                      workspace=workspace, graph_root=graph_root)
    return tools, state


def _user_shelf(tmp_path: Path) -> Path:
    graph_root = tmp_path / "graph"
    (graph_root / "skills").mkdir(parents=True)
    (graph_root / "skills" / "fiscal-notes.md").write_text(
        "# Fiscal notes\n\nOur fiscal year starts in February; "
        "January belongs to the prior year.\n", encoding="utf-8")
    (graph_root / "skills" / "lumi-data-connect.md").write_text(
        "# Impostor\n\nA user file wearing a built-in name.\n",
        encoding="utf-8")
    return graph_root


# ─── the built-in packs are real, bounded doctrine ───────────


def test_builtin_packs_are_real_and_speak_the_v3_kit(compiled,
                                                     tmp_path):
    from sahs.assistant.skills_loader import builtin_skills
    from sahs.loop.skills import MAX_SKILL_CHARS
    packs = {p.name: p for p in builtin_skills()}
    assert sorted(packs) == sorted(PACKS)
    tools, _state = _kit(compiled, tmp_path)
    for pack in packs.values():
        assert pack.origin == "built-in"
        assert pack.title and pack.description
        assert len(pack.text) <= MAX_SKILL_CHARS
        assert "truncated" not in pack.text
        # doctrine names only tools that exist: a pack teaching a
        # ghost tool (the v2 kit's names) is worse than no pack
        ghosts = GHOSTS.findall(pack.text)
        assert not ghosts, f"{pack.name} teaches {ghosts}"
        for kind in re.findall(r"check\(kind=(\w+)", pack.text):
            assert kind in ("part_whole", "crosscheck", "coverage",
                            "fanout", "reconcile", "answer"), kind
    assert "search" in packs["lumi-data-connect"].text
    assert "kind=list" in packs["lumi-data-connect"].text


def test_shelves_merge_and_builtin_wins(tmp_path):
    from sahs.assistant.skills_loader import all_skills, load_packs
    graph_root = _user_shelf(tmp_path)
    packs = {p.name: p for p in all_skills(graph_root)}
    assert packs["fiscal-notes"].origin == "unreviewed"
    assert packs["lumi-data-connect"].origin == "built-in"
    assert "Impostor" not in packs["lumi-data-connect"].text
    loaded, missing = load_packs(
        graph_root, ["executive-summary", "fiscal-notes", "ghost"])
    assert [p.name for p in loaded] == ["executive-summary",
                                        "fiscal-notes"]
    assert missing == ["ghost"]


# ─── the tool: load whole, teach on a miss, record ───────────


def test_load_skill_teaches_and_records(compiled, tmp_path):
    from sahs.assistant.skills_loader import all_skills
    graph_root = _user_shelf(tmp_path / "g")
    tools, state = _kit(compiled, tmp_path, graph_root=graph_root)
    assert "list_skills" not in tools          # the shelf is in the prompt
    assert len(all_skills(graph_root)) == 5
    got = tools["load_skill"].fn("lumi-data-connect")
    assert got["ok"] and got["origin"] == "built-in"
    assert "resolve first" in got["text"]
    assert state.skills_loaded == ["lumi-data-connect"]
    again = tools["load_skill"].fn("lumi-data-connect")
    assert again.get("note") and "already loaded" in again["note"]
    assert state.skills_loaded == ["lumi-data-connect"]
    miss = tools["load_skill"].fn("ghost")
    assert "no skill named" in miss["error"]
    assert "fiscal-notes (unreviewed)" in miss["hint"]


# ─── progressive disclosure through the real loop ────────────


def test_loaded_pack_reaches_the_model_whole(compiled, tmp_path):
    from sahs.assistant.agent import ScriptedAgent
    model = ScriptedAgent([
        [{"thought": "A why-question: load the playbooks first."},
         {"call": {"name": "load_skill",
                   "args": {"name": "analysis-playbooks"}}}],
        [{"text": "Splitting rate from mix next."},
         {"call": {"name": "suggest_next",
                   "args": {"options": ["Run the decomposition"]}}}],
    ])
    runtime = _runtime(compiled, model, tmp_path)
    session = runtime.create_session()
    runtime.start_turn(session["id"], "Why did spend change?")
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(0)
    system = model.calls[0]["system"]
    assert "## Skills on demand" in system
    assert "analysis-playbooks" in system
    assert "rate vs mix" not in system          # names only
    # after load_skill the pack reaches the model WHOLE, as the
    # tool's own response — nothing compacted it to three lines
    response = next(
        p["functionResponse"]["response"]
        for c in model.calls[-1]["contents"]
        for p in c["parts"] if "functionResponse" in p)
    assert response["name"] == "analysis-playbooks"
    assert "rate vs mix" in response["text"]
    assert "check(kind=part_whole" in response["text"]
    done = [e for e in events if e["ev"] == "turn_done"][-1]
    assert done["skills_loaded"] == ["analysis-playbooks"]
    assert done["status"] == "answered"
    step = [e for e in events if e["ev"] == "tool_step"][0]
    assert step["summary"] == "skill analysis-playbooks loaded"


def test_preloaded_skill_leaves_the_shelf_index(compiled):
    from sahs.assistant.loop import system_prompt
    from sahs.assistant.skills_loader import all_skills
    build, _ = compiled
    index = all_skills(None)
    offered = system_prompt(build, [], skill_index=index)
    preloaded = system_prompt(
        build, [p for p in index if p.name == "lumi-data-connect"],
        skill_index=index)
    assert "- lumi-data-connect" in offered
    assert "- lumi-data-connect" not in preloaded
    assert "resolve first" in preloaded
    assert system_prompt(build, []) \
        == system_prompt(build, None, skill_index=[])


# ─── the runtime serves both shelves to the Skills page ──────


def test_runtime_serves_both_shelves(compiled, tmp_path):
    from sahs.assistant.agent import ScriptedAgent
    runtime = _runtime(compiled, ScriptedAgent(), tmp_path)
    _user_shelf(tmp_path)
    rows = {r["name"]: r for r in runtime.skills()}
    assert set(PACKS) <= set(rows)
    assert rows["executive-summary"]["origin"] == "built-in"
    assert rows["fiscal-notes"]["origin"] == "unreviewed"
    assert rows["lumi-data-connect"]["text"]
    session = runtime.create_session()
    saved = runtime.set_skills(session["id"],
                               ["executive-summary", "fiscal-notes"])
    assert saved["ok"] and saved["skills"] == ["executive-summary",
                                               "fiscal-notes"]
    bad = runtime.set_skills(session["id"], ["ghost"])
    assert not bad["ok"] and "ghost" in bad["reason"]
