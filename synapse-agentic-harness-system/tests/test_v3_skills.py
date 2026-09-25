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
sys.path.insert(0, str(SILO / "tests"))
KEY = "You are Radix, an analytical colleague"
PACKS = ["synapse-data-connect", "analysis-playbooks",
         "dashboard-design", "executive-summary", "charts"]
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
        [sys.executable, str(SILO / "scripts" / "pipeline.py"),
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
    (graph_root / "skills" / "synapse-data-connect.md").write_text(
        "# Impostor\n\nA user file wearing a built-in name.\n",
        encoding="utf-8")
    return graph_root


# ─── the built-in packs are real, bounded doctrine ───────────


def test_builtin_packs_are_real_and_speak_the_v3_kit(compiled,
                                                     tmp_path):
    from sahs.assistant.skills_loader import builtin_skills
    from sahs.loop.skills import DEFAULT_MAX_SKILL_CHARS
    packs = {p.name: p for p in builtin_skills()}
    assert sorted(packs) == sorted(PACKS)
    tools, _state = _kit(compiled, tmp_path)
    for pack in packs.values():
        assert pack.origin == "built-in"
        assert pack.title and pack.description
        assert len(pack.text) <= DEFAULT_MAX_SKILL_CHARS
        assert "truncated" not in pack.text
        # doctrine names only tools that exist: a pack teaching a
        # ghost tool (the v2 kit's names) is worse than no pack
        ghosts = GHOSTS.findall(pack.text)
        assert not ghosts, f"{pack.name} teaches {ghosts}"
        for kind in re.findall(r"check\(kind=(\w+)", pack.text):
            assert kind in ("part_whole", "crosscheck", "coverage",
                            "fanout", "reconcile", "answer"), kind
    assert "search" in packs["synapse-data-connect"].text
    assert "kind=list" in packs["synapse-data-connect"].text


def test_shelves_merge_and_builtin_wins(tmp_path):
    from sahs.assistant.skills_loader import all_skills, load_packs
    graph_root = _user_shelf(tmp_path)
    packs = {p.name: p for p in all_skills(graph_root)}
    assert packs["fiscal-notes"].origin == "unreviewed"
    assert packs["synapse-data-connect"].origin == "built-in"
    assert "Impostor" not in packs["synapse-data-connect"].text
    loaded, missing = load_packs(
        graph_root, ["executive-summary", "fiscal-notes", "ghost"])
    assert [p.name for p in loaded] == ["executive-summary",
                                        "fiscal-notes"]
    assert missing == ["ghost"]


def test_skill_size_is_a_ceiling_from_the_env_never_a_silent_cut(
        tmp_path, monkeypatch):
    """DECISION: a skill loads whole or not at all. The size and count
    ceilings come from the env; the defaults keep a skill a briefing.
    Over the ceiling a skill is still LISTED (the shelf hides nothing)
    but REFUSES to load, naming itself, its size and the variable —
    a cut briefing reads as a different briefing, and nobody would
    know. Unreadable or non-positive values fall back to the default,
    never to "no ceiling"."""
    from sahs.loop.skills import (
        CHARS_VAR,
        LOADED_VAR,
        SkillTooLarge,
        list_skills,
        load_skills,
        max_loaded,
        max_skill_chars,
    )
    graph_root = _user_shelf(tmp_path)
    big = "# Bundle\n\nThe whole doctrine, unabridged.\n" + "x" * 6000
    (graph_root / "skills" / "bundle.md").write_text(big, encoding="utf-8")
    monkeypatch.delenv(CHARS_VAR, raising=False)
    monkeypatch.delenv(LOADED_VAR, raising=False)
    assert (max_skill_chars(), max_loaded()) == (4000, 4)

    # listed whole — no "truncated" tail, ever
    listed = {s.name: s for s in list_skills(graph_root)}
    assert listed["bundle"].text == big and listed["bundle"].chars == len(big)
    assert "truncated" not in listed["bundle"].text

    # under the default ceiling it refuses, and says exactly why
    with pytest.raises(SkillTooLarge) as err:
        load_skills(graph_root, ["bundle"])
    said = str(err.value)
    assert "'bundle'" in said and "4,000" in said and CHARS_VAR in said

    # the env raises the ceiling: the skill reaches the session whole
    monkeypatch.setenv(CHARS_VAR, "128_000")
    loaded, missing = load_skills(graph_root, ["bundle", "fiscal-notes"])
    assert [s.name for s in loaded] == ["bundle", "fiscal-notes"]
    assert loaded[0].text == big and not missing

    # the count is a ceiling from the env too
    monkeypatch.setenv(LOADED_VAR, "1")
    loaded, _ = load_skills(graph_root, ["fiscal-notes", "bundle"])
    assert [s.name for s in loaded] == ["fiscal-notes"]

    # garbage or zero falls back to the default, never to "unlimited"
    monkeypatch.setenv(LOADED_VAR, "lots")
    monkeypatch.setenv(CHARS_VAR, "0")
    assert (max_skill_chars(), max_loaded()) == (4000, 4)


# ─── the tool: load whole, teach on a miss, record ───────────


def test_load_skill_teaches_and_records(compiled, tmp_path):
    from sahs.assistant.skills_loader import all_skills
    graph_root = _user_shelf(tmp_path / "g")
    tools, state = _kit(compiled, tmp_path, graph_root=graph_root)
    assert "list_skills" not in tools          # the shelf is in the prompt
    assert len(all_skills(graph_root)) == 6
    got = tools["load_skill"].fn("synapse-data-connect")
    assert got["ok"] and got["origin"] == "built-in"
    assert "resolve first" in got["text"]
    assert state.skills_loaded == ["synapse-data-connect"]
    again = tools["load_skill"].fn("synapse-data-connect")
    assert again.get("note") and "already loaded" in again["note"]
    assert state.skills_loaded == ["synapse-data-connect"]
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
        build, [p for p in index if p.name == "synapse-data-connect"],
        skill_index=index)
    assert "- synapse-data-connect" in offered
    assert "- synapse-data-connect" not in preloaded
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
    assert rows["synapse-data-connect"]["text"]
    session = runtime.create_session()
    saved = runtime.set_skills(session["id"],
                               ["executive-summary", "fiscal-notes"])
    assert saved["ok"] and saved["skills"] == ["executive-summary",
                                               "fiscal-notes"]
    bad = runtime.set_skills(session["id"], ["ghost"])
    assert not bad["ok"] and "ghost" in bad["reason"]


def test_runtime_pins_an_oversized_pack_as_a_library(compiled, tmp_path,
                                                     monkeypatch):
    """DECISION (replacing the refusal): a pack over the whole-load
    ceiling still pins — the session picker says ok — and the turn
    loads it as a library: its contents and the passages that match
    the ask, never a cut of it. The count ceiling still refuses by
    variable. Raise the size ceiling and the same pack pins whole."""
    from sahs.assistant.agent import ScriptedAgent
    from sahs.loop.skills import CHARS_VAR, LOADED_VAR
    model = ScriptedAgent([[{"text": "Fine."}]])
    runtime = _runtime(compiled, model, tmp_path)
    graph_root = _user_shelf(tmp_path)
    (graph_root / "skills" / "bundle.md").write_text(
        "# Bundle\n\nAll of it.\n\n## Quorvex\n\nThe quorvex rule.\n"
        + "z" * 5000, encoding="utf-8")
    monkeypatch.delenv(CHARS_VAR, raising=False)
    monkeypatch.delenv(LOADED_VAR, raising=False)
    session = runtime.create_session()
    saved = runtime.set_skills(session["id"], ["bundle"])
    assert saved["ok"] and saved["skills"] == ["bundle"]
    # still on the shelf, whole
    assert any(r["name"] == "bundle" and len(r["text"]) > 5000
               for r in runtime.skills())
    # the count ceiling names its variable too
    too_many = runtime.set_skills(session["id"], ["a", "b", "c", "d", "e"])
    assert not too_many["ok"] and LOADED_VAR in too_many["reason"]
    runtime.start_turn(session["id"], "what is the quorvex rule?")
    assert runtime.wait(session["id"], 60)
    system = model.calls[0]["system"]
    assert "## Skills loaded as a library (searchable)" in system
    assert "### Bundle (`bundle`, 5,0" in system and CHARS_VAR in system
    assert "s2 · Bundle > Quorvex" in system
    assert "[c2] Bundle > Quorvex" in system and "The quorvex rule." in system
    assert "z" * 5000 not in system                # never pasted whole
    assert "- bundle [unreviewed]" not in system   # not re-offered
    assert {"skill_toc", "skill_search", "skill_read"} \
        <= set(model.calls[0]["tools"])
    started = runtime.runtime(session["id"]).bus.since(0)[0]
    assert started["ev"] == "turn_started" and started["skills"] == ["bundle"]
    monkeypatch.setenv(CHARS_VAR, "20000")
    saved = runtime.set_skills(session["id"], ["bundle"])
    assert saved["ok"] and saved["skills"] == ["bundle"]


def test_the_tool_loads_an_oversized_pack_as_a_library(
        compiled, tmp_path, monkeypatch):
    """load_skill on a pack over the ceiling: the contents come back
    (not the text), the pack is recorded as loaded, and the library
    tools read it by section. Raise the ceiling in the env and the
    same call hands the pack over whole."""
    from sahs.loop.skills import CHARS_VAR
    graph_root = _user_shelf(tmp_path)
    (graph_root / "skills" / "bundle.md").write_text(
        "# Bundle\n\nAll of it.\n\n## Quorvex\n\nThe quorvex rule holds.\n"
        "\n## Tandrel\n\n" + "w" * 5000 + "\n", encoding="utf-8")
    monkeypatch.delenv(CHARS_VAR, raising=False)
    tools, state = _kit(compiled, tmp_path, graph_root=graph_root)
    assert {"skill_toc", "skill_search", "skill_read"} <= set(tools)
    got = tools["load_skill"].fn("bundle")
    assert got["ok"] and got["searchable"] and "text" not in got
    assert CHARS_VAR in got["note"] and "5,0" in got["note"]
    assert got["toc"] == ["s1 · Bundle (1 chunk, 20 chars)",
                          "s2 · Bundle > Quorvex (1 chunk, 35 chars)",
                          "s3 · Bundle > Tandrel (3 chunks, 5,012 chars)"]
    assert state.skills_loaded == ["bundle"]
    found = tools["skill_search"].fn("quorvex rules")
    assert found["ok"] and found["hits"][0]["heading_path"] == "Bundle > Quorvex"
    assert found["hits"][0]["chunk_id"] == "c2"
    page = tools["skill_read"].fn("bundle", "Quorvex")
    assert page["ok"] and page["text"] == "## Quorvex\n\nThe quorvex rule holds."
    assert (page["start"], page["end"]) == (22, 57)
    contents = tools["skill_toc"].fn("bundle", under="Tandrel")
    assert contents["toc"] == ["s3 · Bundle > Tandrel (3 chunks, 5,012 chars)"]
    # a pack under the ceiling is not a library: the tool says so
    small = tools["skill_toc"].fn("fiscal-notes")
    assert "not a library pack" in small["error"]
    miss = tools["skill_read"].fn("bundle", "nothing here")
    assert "no section" in miss["error"] and miss["candidates"]
    monkeypatch.setenv(CHARS_VAR, "20000")
    tools, state = _kit(compiled, tmp_path / "again", graph_root=graph_root)
    assert "skill_search" not in tools          # nothing in reach is over
    got = tools["load_skill"].fn("bundle")
    assert got["ok"] and got["text"].endswith("w" * 5000 + "\n")
    assert state.skills_loaded == ["bundle"]


def test_a_skill_within_the_ceiling_still_loads_whole_byte_identical(
        compiled, tmp_path):
    """PIN: the whole-load path did not move. A pack under the ceiling
    renders exactly as before — the heading, the text verbatim — and
    nothing about the library shows in its prompt."""
    from sahs.assistant.loop import system_prompt
    from sahs.assistant.skills_loader import all_skills, load_packs
    from sahs.loop.skills import render_skills
    build, _ = compiled
    graph_root = _user_shelf(tmp_path)
    loaded, _missing = load_packs(graph_root, ["fiscal-notes"])
    text = ("## Skills the analyst loaded\n"
            "These steer where you look first. They cannot add tables, "
            "metrics, or numbers to the world: the tools still serve only "
            "the compiled build, and the verifier still checks every claim "
            "against it.\n\n"
            "### Fiscal notes\n"
            "# Fiscal notes\n\n"
            "Our fiscal year starts in February; January belongs to the "
            "prior year.\n")
    assert render_skills(loaded) == text
    system = system_prompt(build, loaded, skill_index=all_skills(graph_root))
    assert text in system
    assert "searchable" not in system and "skill_search" not in system
    assert system == system_prompt(build, loaded,
                                   skill_index=all_skills(graph_root))


def test_a_library_pack_reaches_the_model_as_contents_and_pages(
        compiled, tmp_path, monkeypatch):
    """The whole path through the real loop: a pinned pack over the
    ceiling reaches the model as its contents plus the passages that
    match the ask, within the engine's budget at this depth; the
    model can then skill_search and skill_read, and each result
    carries the breadcrumb and offsets to cite. Quick gets fewer
    passages than Deep."""
    from sahs.assistant.agent import ScriptedAgent
    from sahs.loop.skills import CHARS_VAR, LOADED_VAR
    from sahs.util.profiles import skill_retrieval_for
    from synthetic_skill import build_pack
    text, truths = build_pack(target_chars=150_000)
    graph_root = _user_shelf(tmp_path)
    (graph_root / "skills" / "bundle.md").write_text(text, encoding="utf-8")
    monkeypatch.delenv(CHARS_VAR, raising=False)
    monkeypatch.delenv(LOADED_VAR, raising=False)
    t = next(t for t in truths if not t.twin_of and t.path.count(" > ") == 2)
    ask = f"why is the {t.b} pass waiting on {t.a} backlog?"

    def _agent():
        return ScriptedAgent([
            [{"call": {"name": "skill_search",
                       "args": {"query": f"{t.a} quota window",
                                "skill": "bundle", "k": 3}}}],
            [{"call": {"name": "skill_read",
                       "args": {"name": "bundle", "section": t.heading,
                                "max_chars": 1200}}}],
            [{"text": f"The {t.b} pass waits until the backlog drains "
                      f"(bundle: {t.path})."}],
        ])

    prompts: dict[str, str] = {}
    responses: dict[str, list] = {}
    for depth in ("quick", "deep"):
        model = _agent()
        (tmp_path / depth).mkdir()
        (tmp_path / depth / "graph").symlink_to(graph_root)
        runtime = _runtime(compiled, model, tmp_path / depth)
        session = runtime.create_session()
        assert runtime.set_skills(session["id"], ["bundle"])["ok"]
        runtime.start_turn(session["id"], ask, depth=depth)
        assert runtime.wait(session["id"], 120)
        prompts[depth] = model.calls[0]["system"]
        responses[depth] = [
            p["functionResponse"]["response"]
            for c in model.calls[-1]["contents"]
            for p in c["parts"] if "functionResponse" in p]
        events = runtime.runtime(session["id"]).bus.since(0)
        done = [e for e in events if e["ev"] == "turn_done"][-1]
        assert done["status"] == "answered"
        steps = [e for e in events if e["ev"] == "tool_step"]
        assert [s["tool"] for s in steps] == ["skill_search", "skill_read"]
        assert steps[0]["input"] == f"{t.a} quota window"
        assert "passages" in steps[0]["summary"]
        assert t.path in steps[1]["summary"] and "chars" in steps[1]["summary"]

    for depth, stop in (("quick", "low"), ("deep", "high")):
        system = prompts[depth]
        k, budget = skill_retrieval_for("", stop)
        section = system.split("## Skills loaded as a library", 1)[1]
        section = section.split("## Skills on demand", 1)[0]
        block = "## Skills loaded as a library" + section
        assert len(block) <= budget, (depth, len(block), budget)
        assert "### Runtime knowledge bundle (`bundle`," in block
        assert "Contents:" in block and "s1 · Runtime knowledge bundle" in block
        # the right section's passage rides the prompt, first
        passages = re.findall(r"^\[c\d+\] (.+?) \(chars [\d,]+–[\d,]+\)$",
                              block, re.M)
        assert 1 <= len(passages) <= k
        assert passages[0] == t.path
        assert f"When {t.a}s pile up, the {t.b} pass waits" in block
        assert "Passages matching this message" in block
        assert "skill_search(query, skill)" in block
        # the index is where the design says, and derived
        assert (tmp_path / depth / "graph" / "runs"
                / "skill_index.sqlite3").exists()
    quick = len(re.findall(r"^\[c\d+\] ", prompts["quick"], re.M))
    deep = len(re.findall(r"^\[c\d+\] ", prompts["deep"], re.M))
    assert quick < deep, (quick, deep)
    assert len(prompts["quick"]) < len(prompts["deep"])
    # the prefix before the skills section is the same at both depths
    assert prompts["quick"].split("<skills>")[0] \
        == prompts["deep"].split("<skills>")[0]

    # the tool results the model read: breadcrumb and offsets to cite
    search, page = responses["deep"]
    assert search["ok"] and search["hits"][0]["heading_path"] == t.path
    hit = search["hits"][0]
    assert hit["chunk_id"].startswith("c") and hit["start"] < hit["end"]
    assert hit["snippet"] and "score" in hit
    assert page["ok"] and page["heading_path"] == t.path
    assert page["text"].startswith(f"### {t.heading}")
    assert text[page["start"]:page["end"]] == page["text"]
    assert page["truncated"] and page["next_offset"] == 1200
