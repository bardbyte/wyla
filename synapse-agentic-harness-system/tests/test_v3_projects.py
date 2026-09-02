"""Synapse v3 — organization that survives reopening: the store's
projects, flags, notes, and handoff; memory bound to the person and
disclosed in the prompt; notes and handoff through two real turns;
the PPTX deck; project instructions riding along."""

from __future__ import annotations

import io
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
sys.path.insert(0, str(SILO))
KEY = "You are Synapse, an analytical colleague"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("v3proj")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "v3p"],
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


# ─── the store: organization that survives reopening ─────────


def test_store_organization_and_migration(tmp_path):
    from sahs.assistant.store import AssistantStore
    store = AssistantStore(tmp_path / "s.sqlite3")
    project = store.create_project(
        "CFO pack", instructions="Certified only; USD.",
        skills=["executive-summary"])
    session = store.create_session("assistant")
    store.set_project(session["id"], project["id"])
    store.set_flag(session["id"], "starred", True)
    store.set_notes(session["id"], ["split rate from mix"])
    store.set_handoff(session["id"], {"say": "left off here",
                                      "chips": ["verify"]})
    again = AssistantStore(tmp_path / "s.sqlite3")
    row = again.get_session(session["id"])
    assert row["project_id"] == project["id"]
    assert row["starred"] is True and row["archived"] is False
    assert row["notes"] == ["split rate from mix"]
    assert row["handoff"]["say"] == "left off here"
    again.set_flag(session["id"], "archived", True)
    assert again.get_session(session["id"])["archived"] is True
    updated = again.update_project(project["id"],
                                   instructions="Certified only.",
                                   archived=True)
    assert updated["archived"] is True
    assert again.list_projects() == []
    assert len(again.list_projects(include_archived=True)) == 1


def test_memory_is_scoped_statused_never_silently_gone(tmp_path):
    from sahs.assistant.store import AssistantStore
    store = AssistantStore(tmp_path / "s.sqlite3")
    project = store.create_project("CFO pack")
    keep = store.add_memory("by Canada they mean merchant country")
    scoped = store.add_memory("prefers quarters, not months",
                              scope=f"project:{project['id']}")
    assert [m["id"] for m in store.list_memories()] == [keep["id"]]
    both = store.list_memories(project_id=project["id"])
    assert {m["id"] for m in both} == {keep["id"], scoped["id"]}
    assert store.retire_memory(keep["id"]) is True
    assert store.retire_memory(keep["id"]) is False
    assert [m["id"] for m in
            store.list_memories(project_id=project["id"])] \
        == [scoped["id"]]
    retired = store.list_memories(status="retired")
    assert [m["id"] for m in retired] == [keep["id"]]


# ─── remember teaches its pins; listing lives in the prompt ──


def test_remember_teaches_and_discloses(compiled, tmp_path):
    from sahs.assistant.kit import build_kit
    from sahs.assistant.sandbox import prepare_workspace
    from sahs.assistant.state import AssistantState
    from sahs.assistant.store import AssistantStore
    build, _ = compiled
    store = AssistantStore(tmp_path / "s.sqlite3")
    session = store.create_session("assistant")
    workspace = tmp_path / "ws"
    prepare_workspace(workspace, build.root)

    def kit(project_id=""):
        return build_kit(build, AssistantState(), store=store,
                         session_id=session["id"], turn_id="t1",
                         workspace=workspace, project_id=project_id)

    tools = kit()
    assert "memories" not in tools and "forget" not in tools
    miss = tools["remember"].fn("prefers quarters", scope="project")
    assert "no project" in miss["error"]
    bad = tools["remember"].fn("x", scope="everywhere")
    assert "unknown scope" in bad["error"]
    empty = tools["remember"].fn("   ")
    assert "nothing to remember" in empty["error"]
    ok = tools["remember"].fn("by Canada they mean merchant country")
    assert ok["ok"] and ok["scope"] == "global"
    assert "retire" in ok["note"]
    project = store.create_project("CFO pack")
    tools = kit(project_id=project["id"])
    scoped = tools["remember"].fn("prefers quarters", scope="project")
    assert scoped["scope"] == f"project:{project['id']}"
    assert len(store.list_memories(project_id=project["id"])) == 2


# ─── disclosure: the prompt carries what is remembered ───────


def test_prompt_discloses_memory_bound_to_the_person(compiled):
    from sahs.assistant.loop import system_prompt
    build, _ = compiled
    plain = system_prompt(build)
    assert "<memory>" in plain
    assert "Nothing remembered about this user yet" in plain
    assert "What you remember" not in plain
    told = system_prompt(
        build,
        memories=[{"text": "by Canada they mean merchant country",
                   "scope": "global"},
                  {"text": "prefers quarters", "scope": "project:p"}],
        project={"id": "p", "name": "CFO pack",
                 "instructions": "Certified numbers only; USD."},
        user_name="Saheb Singh")
    assert "What you remember about Saheb Singh" in told
    assert "by Canada they mean merchant country" in told
    assert "prefers quarters [this project]" in told
    assert "Project: CFO pack" in told
    assert "Certified numbers only; USD." in told
    # platform governance outranks memory, in so many words
    assert "never softens a rule" in told


# ─── the deck: one panel per slide, disclosure in the notes ──


def test_pptx_deck_carries_the_disclosure(compiled):
    from sahs.assistant.artifacts import validate_artifact
    from sahs.assistant.export import artifact_pptx
    build, _ = compiled
    certified = next(m for m in build.metrics
                     if (m.get("status_served") or m.get("status"))
                     == "certified")
    spec, problems = validate_artifact("dashboard", {
        "panels": [
            {"type": "kpi", "title": "Net spend", "spec": {
                "value": 812.0, "unit": "USD",
                "provenance": {"status": "certified",
                               "metric_id": certified["id"],
                               "meridian_line":
                               "Certified spend on the meridian."}}},
            {"type": "chart", "title": "By country", "spec": {
                "kind": "bar", "series": [
                    {"name": "spend",
                     "points": [["CA", 300.0], ["US", 512.0]]}],
                "provenance": {"status": "composed",
                               "meridian_line": "Composed split."}}},
        ]}, build_id=build.version, build=build)
    assert problems == []
    data = artifact_pptx({"type": "dashboard", "title": "Q2 spend",
                          "version": 1, "spec": spec})
    archive = zipfile.ZipFile(io.BytesIO(data))
    slides = [n for n in archive.namelist()
              if n.startswith("ppt/slides/slide")]
    notes = b"".join(archive.read(n) for n in archive.namelist()
                     if n.startswith("ppt/notesSlides/notesSlide"))
    assert len(slides) == 3
    assert b"Certified spend on the meridian." in notes
    assert b"EXPLORATORY" in notes
    assert build.version.encode() in archive.read(slides[0])


# ─── two turns: notes persist, the handoff says where ────────


def test_notes_persist_and_handoff_says_where(compiled, tmp_path):
    model = _agent(
        [{"thought": "Keep the thread, and keep the meaning they "
                     "settled."},
         _call("note", text="comparing Q1 vs Q2 next"),
         _call("remember", text="by spend they mean acquirer net "
                                "spend")],
        [{"text": "Noted — I'll compare quarters next."},
         _call("suggest_next", options=["Compare Q1 vs Q2"])],
        # ── turn 2 ──
        [{"text": "Picking up the comparison."}])
    runtime = _runtime(compiled, model, tmp_path,
                       user_name="Saheb Singh")
    session = runtime.create_session()
    runtime.start_turn(session["id"], "spend is our net metric")
    assert runtime.wait(session["id"], 60)

    row = runtime.store.get_session(session["id"])
    assert row["notes"] == ["comparing Q1 vs Q2 next"]
    handoff = row["handoff"]
    assert handoff["status"] == "answered"
    assert "compare quarters" in handoff["say"]
    assert handoff["chips"] == ["Compare Q1 vs Q2"]

    runtime.start_turn(session["id"], "go on")
    assert runtime.wait(session["id"], 60)
    system = model.calls[-1]["system"]
    assert "comparing Q1 vs Q2 next" in system     # <session> notes
    assert "Your working notes" in system
    assert "acquirer net spend" in system           # <memory>
    assert "What you remember about Saheb Singh" in system
    events = runtime.runtime(session["id"]).bus.since(0)
    started = [e for e in events if e["ev"] == "turn_started"][-1]
    assert started["memories"] == 1


def test_project_instructions_and_pinned_skills_ride_along(
        compiled, tmp_path):
    model = _agent([{"text": "Ready."}])
    runtime = _runtime(compiled, model, tmp_path)
    project = runtime.store.create_project(
        "CFO pack", instructions="Lead with certified USD numbers.",
        skills=["executive-summary"])
    session = runtime.create_session()
    saved = runtime.set_session_project(session["id"], project["id"])
    assert saved["ok"]
    assert not runtime.set_session_project(session["id"],
                                           "p_ghost")["ok"]
    runtime.start_turn(session["id"], "hello")
    assert runtime.wait(session["id"], 60)
    system = model.calls[-1]["system"]
    assert "Project: CFO pack" in system
    assert "Lead with certified USD numbers." in system
    assert "The memo shape leaders actually read" in system
    events = runtime.runtime(session["id"]).bus.since(0)
    started = [e for e in events if e["ev"] == "turn_started"][-1]
    assert started["project"] == "CFO pack"
    assert started["skills"] == ["executive-summary"]
    runtime.set_session_flag(session["id"], "archived", True)
    assert session["id"] not in [r["id"] for r in runtime.sessions()]
    assert session["id"] in [
        r["id"] for r in runtime.sessions(include_archived=True)]
