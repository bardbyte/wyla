"""Skills and knowledge files authored with the model: the owner's own
packs load for the owner alone, the draft is the model's rewrite in
the house format (checked, never invented into an empty file), saving
refuses what would never load, and the agent consumes an own pack at
runtime through load_skill — for that person, not for another."""

from __future__ import annotations

import sys
from pathlib import Path

from sahs.assistant import authoring
from sahs.assistant.agent import ScriptedAgent
from sahs.assistant.skills_loader import (all_skills, get_skill, load_packs,
                                          owner_slug, user_root)

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO / "tests"))
from test_gateway_plane import compiled  # noqa: E402,F401

PACK = ("# Churn triage\n\nThe moves for a churn question on merchants.\n\n"
        "## Split rate from mix\n1. search(\"churn\") for the certified "
        "definition.\n2. run_sql(mode=\"dry_run\") to prove the split.\n\n"
        "## Never\n- Never quote a rate without its denominator.\n")


def test_own_packs_load_for_their_owner_alone(tmp_path):
    graph = tmp_path / "graph"
    (graph / "skills").mkdir(parents=True)
    (graph / "skills" / "team-notes.md").write_text(
        "# Team notes\n\nShared words.\n", encoding="utf-8")
    assert owner_slug("Saheb Singh") == "saheb-singh"
    assert user_root(graph, "Saheb Singh") == graph / "skills" / "users" \
        / "saheb-singh"
    got = authoring.save_skill(graph, "Saheb Singh", "Churn Triage", PACK)
    assert got["ok"] and got["name"] == "churn-triage"
    assert got["owner"] == "saheb-singh" and got["origin"] == "unreviewed"
    assert got["title"] == "Churn triage"
    assert got["description"].startswith("The moves for a churn question")
    mine = {p.name: p for p in all_skills(graph, "Saheb Singh")}
    assert mine["churn-triage"].owner == "saheb-singh"
    assert mine["team-notes"].owner == "" and mine["team-notes"].origin \
        == "unreviewed"
    assert mine["analysis-playbooks"].origin == "built-in"
    # another person: the shared pack, not this one's own
    other = {p.name for p in all_skills(graph, "Alice")}
    assert "team-notes" in other and "churn-triage" not in other
    assert get_skill(graph, "churn-triage", "Alice") is None
    assert get_skill(graph, "churn-triage", "Saheb Singh").text == PACK
    loaded, missing = load_packs(graph, ["churn-triage", "team-notes"],
                                 owner="Saheb Singh")
    assert [p.name for p in loaded] == ["churn-triage", "team-notes"]
    assert not missing
    # an own pack shadows a shared one of the same name, for its owner
    authoring.save_skill(graph, "Saheb Singh", "team-notes",
                         "# Team notes, mine\n\nMy words.\n")
    assert get_skill(graph, "team-notes", "Saheb Singh").owner == "saheb-singh"
    assert get_skill(graph, "team-notes", "Alice").owner == ""
    # never a built-in
    refused = authoring.save_skill(graph, "Saheb Singh",
                                   "analysis-playbooks", PACK)
    assert not refused["ok"] and "built-in" in refused["reason"]
    for name, text, why in (("x", "", "empty"),
                            ("y", "# T\n\n" + "w" * 13_000, "over 12,000"),
                            ("", PACK, "needs a name")):
        got = authoring.save_skill(graph, "Saheb Singh", name, text)
        assert not got["ok"] and why in got["reason"], (name, got)
    assert not authoring.save_skill(graph, "", "z", PACK)["ok"]
    again = authoring.save_skill(graph, "Saheb Singh", "churn-triage", PACK)
    assert again["ok"] and again["replaced"]
    assert authoring.delete_skill(graph, "Saheb Singh", "churn-triage")
    assert not authoring.delete_skill(graph, "Saheb Singh", "churn-triage")
    assert get_skill(graph, "churn-triage", "Saheb Singh") is None


def test_the_draft_is_the_models_rewrite_in_the_house_format():
    system, user = authoring.prompt_for(
        "skill", "Churn triage", "rate vs mix first", "why churn moved")
    assert "## Never" in system and "Nothing is invented" in system
    assert "under 3800 characters" in system
    assert "Title the person gave: Churn triage" in user
    assert "<<<\nrate vs mix first\n>>>" in user
    system_k, _ = authoring.prompt_for("knowledge", "Lending", "notes")
    assert "## Tables and columns" in system_k and "## Owner" in system_k
    agent = ScriptedAgent(json_answers=[
        {"name": "Churn Triage!", "title": "Churn triage",
         "description": "The moves for a churn question.",
         "text": PACK, "notes": ["the denominator was not named"]},
        {},                                   # the model returned nothing
        {"text": "no heading here"},          # a heading is supplied
    ])
    got = authoring.draft(agent, "skill", "Churn triage", "rate vs mix")
    assert got["ok"] and got["name"] == "churn-triage"
    assert got["text"] == PACK.strip() and got["chars"] == len(PACK.strip())
    assert got["notes"] == ["the denominator was not named"]
    assert not authoring.draft(agent, "skill", "t", "m")["ok"]
    fixed = authoring.draft(agent, "knowledge", "Lending", "m")
    assert fixed["ok"] and fixed["text"].startswith("# Lending\n\nno heading")
    assert "no material" in authoring.draft(agent, "skill", "t", "  ")["reason"]
    try:
        authoring.prompt_for("poem", "t", "m")
    except ValueError as e:
        assert "skill or knowledge" in str(e)


def test_the_agent_consumes_an_own_pack_at_runtime_for_that_person(
        compiled, tmp_path):
    from sahs.assistant import AssistantRuntime
    build, tmp = compiled
    graph = tmp / "graph"

    def factory(budget):
        return ScriptedAgent(
            steps=[[{"call": {"name": "load_skill",
                              "args": {"name": "churn-triage"}}}],
                   [{"text": "Loaded the triage moves."}]],
            json_answers=[{"name": "churn-triage", "title": "Churn triage",
                           "description": "The moves.", "text": PACK,
                           "notes": []}])

    saheb = AssistantRuntime(
        builds_root=build.root.parent, graph_root=graph,
        store_path=tmp_path / "saheb.sqlite3", model_factory=factory,
        user_name="Saheb Singh")
    assert saheb.owner == "saheb-singh"
    drafted = saheb.draft("skill", "Churn triage", "rate vs mix first")
    assert drafted["ok"] and drafted["text"] == PACK.strip()
    saved = saheb.save_my_skill(drafted["name"], drafted["text"])
    assert saved["ok"]
    listed = {s["name"]: s for s in saheb.skills()}
    assert listed["churn-triage"]["mine"] and listed["churn-triage"][
        "owner"] == "saheb-singh"
    assert not listed["analysis-playbooks"]["mine"]
    session = saheb.create_session()
    saheb.start_turn(session["id"], "why did churn move?")
    assert saheb.wait(session["id"], 60)
    events = saheb.runtime(session["id"]).bus.since(0)
    step = next(e for e in events if e["ev"] == "tool_step")
    assert step["tool"] == "load_skill" and "error" not in step.get(
        "summary", "").lower()
    stored = saheb.store.messages(session["id"])[-1]
    assert "Loaded the triage moves." in stored["text"]
    # the system prompt offered the own pack by name
    prompt = next(e for e in events if e["ev"] == "model_prompt"
                  and e.get("kind") == "system")
    assert "churn-triage [unreviewed]" in prompt["content"]
    # another person on the same graph: the pack is not theirs to load
    alice = AssistantRuntime(
        builds_root=build.root.parent, graph_root=graph,
        store_path=tmp_path / "alice.sqlite3", model_factory=factory,
        user_name="Alice")
    assert "churn-triage" not in {s["name"] for s in alice.skills()}
    session = alice.create_session()
    alice.start_turn(session["id"], "why did churn move?")
    assert alice.wait(session["id"], 60)
    events = alice.runtime(session["id"]).bus.since(0)
    result = next(e for e in events if e["ev"] == "tool_result")
    assert "no skill named 'churn-triage'" in result["content"]
    assert saheb.delete_my_skill("churn-triage")
