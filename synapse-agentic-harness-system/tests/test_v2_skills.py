"""Synapse v2 §13.3 — skills on demand: the built-in doctrine packs,
the two-shelf loader, and progressive disclosure through the loop.

The headline test drives the whole idea end to end: the system prompt
carries only names, the model calls load_skill, and the pack's full
text is in its very next prompt — context injected by tool result,
never by ambient magic. The E14 door is pinned too: user packs load
immediately but wear ``unreviewed`` everywhere, and no user file can
shadow a built-in name.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
KEY = "You are Synapse, an analytical colleague"
PACKS = ("analysis-playbooks", "dashboard-design",
         "executive-summary", "meridian-sql")


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("v2skills")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v2s"],
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


def _runtime(compiled, model, tmp=None):
    from sahs.assistant import AssistantRuntime
    build, _ = compiled
    tmp = tmp or Path(tempfile.mkdtemp())
    return AssistantRuntime(builds_root=build.root.parent,
                            graph_root=tmp / "graph",
                            store_path=tmp / "chat.sqlite3",
                            model_factory=lambda budget: model)


def _kit(compiled, tmp_path, graph_root=None):
    from sahs.assistant.sandbox import prepare_workspace
    from sahs.assistant.store import AssistantStore
    from sahs.assistant.tools import AssistantState, assistant_toolkit
    build, _ = compiled
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    state = AssistantState()
    workspace = tmp_path / "ws"
    prepare_workspace(workspace, build.root)
    tools = assistant_toolkit(build, state, store=store,
                              session_id=session["id"], turn_id="t1",
                              workspace=workspace,
                              graph_root=graph_root)
    return tools, state


def _user_shelf(tmp_path: Path) -> Path:
    graph_root = tmp_path / "graph"
    (graph_root / "skills").mkdir(parents=True)
    (graph_root / "skills" / "fiscal-notes.md").write_text(
        "# Fiscal notes\n\nOur fiscal year starts in February; "
        "January belongs to the prior year.\n", encoding="utf-8")
    (graph_root / "skills" / "meridian-sql.md").write_text(
        "# Impostor\n\nA user file wearing a built-in name.\n",
        encoding="utf-8")
    return graph_root


# ─── the built-in packs are real, bounded doctrine ───────────


def test_builtin_packs_are_real_and_bounded(compiled, tmp_path):
    sys.path.insert(0, str(SILO))
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
        # ghost tool would be worse than no pack at all
        for name in re.findall(
                r"\b(check_\w+|run_sql|sample_values|verify_answer|"
                r"subgraph|resolve|search_semantics|grep_cards|"
                r"read_card|get_join_paths|get_definition_line|"
                r"load_skill|list_skills)\b", pack.text):
            assert name in tools, f"{pack.name} teaches {name!r}"


def test_shelves_merge_and_builtin_wins(tmp_path):
    from sahs.assistant.skills_loader import all_skills, load_packs
    graph_root = _user_shelf(tmp_path)
    packs = {p.name: p for p in all_skills(graph_root)}
    assert packs["fiscal-notes"].origin == "unreviewed"
    # the impostor never shadows the shipped doctrine
    assert packs["meridian-sql"].origin == "built-in"
    assert "Impostor" not in packs["meridian-sql"].text
    loaded, missing = load_packs(
        graph_root, ["executive-summary", "fiscal-notes", "ghost"])
    assert [p.name for p in loaded] == ["executive-summary",
                                        "fiscal-notes"]
    assert missing == ["ghost"]


# ─── the tools: list cheaply, load whole, teach on a miss ────


def test_list_and_load_skill_teach_and_record(compiled, tmp_path):
    tools, state = _kit(compiled, tmp_path,
                        graph_root=_user_shelf(tmp_path / "g"))
    shelf = tools["list_skills"].fn()
    assert shelf["count"] == 5
    origins = {s["name"]: s["origin"] for s in shelf["skills"]}
    assert origins["fiscal-notes"] == "unreviewed"

    got = tools["load_skill"].fn("meridian-sql")
    assert got["ok"] and got["origin"] == "built-in"
    assert "resolve first" in got["text"]
    assert state.skills_loaded == ["meridian-sql"]

    again = tools["load_skill"].fn("meridian-sql")
    assert again.get("note") and "already loaded" in again["note"]
    assert state.skills_loaded == ["meridian-sql"]

    miss = tools["load_skill"].fn("ghost")
    assert "no skill named" in miss["error"]
    assert "fiscal-notes (unreviewed)" in miss["hint"]


# ─── progressive disclosure through the real loop ────────────


def test_loaded_pack_reaches_the_next_prompt(compiled, tmp_path):
    model = Scripted([
        {"think": "a why-question: load the playbooks",
         "tool": "load_skill", "args": {"name": "analysis-playbooks"}},
        {"say": "Splitting rate from mix next.", "done": True,
         "chips": ["Run the decomposition"]},
    ])
    runtime = _runtime(compiled, model, tmp_path)
    session = runtime.create_session()
    runtime.start_turn(session["id"], "Why did spend change?")
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(0)

    # names-only in the system prompt: the shelf is offered, the
    # doctrine is not
    system = model.systems[0]
    assert "## Skills on demand" in system
    assert "analysis-playbooks" in system
    assert "rate vs mix" not in system
    # after load_skill, the model's next prompt carries the pack
    # whole — the tool result IS the injection
    assert "rate vs mix" in model.prompts[-1]
    assert "doctrine now applies" in model.prompts[-1]
    done = [e for e in events if e["ev"] == "turn_done"][-1]
    assert done["skills_loaded"] == ["analysis-playbooks"]
    assert done["status"] == "answered"


def test_preloaded_skill_leaves_the_shelf_index(compiled):
    from sahs.assistant.loop import system_prompt
    from sahs.assistant.skills_loader import all_skills
    build, _ = compiled
    index = all_skills(None)
    offered = system_prompt(build, [], "", skill_index=index)
    preloaded = system_prompt(
        build, [p for p in index if p.name == "meridian-sql"], "",
        skill_index=index)
    assert "- meridian-sql" in offered
    assert "- meridian-sql" not in preloaded     # already in full
    assert "resolve first" in preloaded          # …as loaded text
    # no shelf, no skills → the §13.1 prompt, byte-identical
    assert system_prompt(build, [], "") \
        == system_prompt(build, None, "", skill_index=[])


# ─── the runtime serves both shelves to the picker ───────────


def test_runtime_serves_both_shelves(compiled, tmp_path):
    runtime = _runtime(compiled, Scripted(), tmp_path)
    _user_shelf(tmp_path)
    rows = {r["name"]: r for r in runtime.skills()}
    assert set(PACKS) <= set(rows)
    assert rows["executive-summary"]["origin"] == "built-in"
    assert rows["fiscal-notes"]["origin"] == "unreviewed"
    session = runtime.create_session()
    saved = runtime.set_skills(session["id"],
                               ["executive-summary", "fiscal-notes"])
    assert saved["ok"] and saved["skills"] == ["executive-summary",
                                               "fiscal-notes"]
    bad = runtime.set_skills(session["id"], ["ghost"])
    assert not bad["ok"] and "ghost" in bad["reason"]
