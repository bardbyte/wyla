"""The approval workflow (the PRD): a submission goes to the manager
before the agent may read it; the checks and the model's read sit
beside it; approve publishes through the same doors the creators
used, reject carries the comments, resubmit bumps the version; the
notices are read off the ledger; nothing pending or rejected reaches
the loader."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from sahs.assistant import reviews as rv
from sahs.assistant.agent import ScriptedAgent
from sahs.assistant.skills_loader import all_skills, get_skill

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO / "tests"))
from test_gateway_plane import compiled  # noqa: E402,F401

PACK = ("# Approvals triage\n\nThe moves for an approvals question.\n\n"
        "## Rate first\n1. search(\"approval rate\") for the definition.\n"
        "2. run_sql(mode=\"dry_run\") to prove the split.\n\n"
        "## Never\n- never quote a rate without its denominator\n")
ME = "John Doe"


def _reviews(tmp_path) -> rv.Reviews:
    return rv.Reviews(tmp_path / "graph" / "runs" / "reviews")


def test_the_approver_is_assigned_never_chosen():
    manager = rv.approver_for(ME, {"SYNAPSE_USER_MANAGER": "Jane Doe",
                                   "SYNAPSE_USER_MANAGER_BAND": "45"})
    assert manager == rv.Approver("Jane Doe", 45)
    assert manager.may_approve and not manager.self_review
    junior = rv.approver_for(ME, {"SYNAPSE_USER_MANAGER": "Sam",
                                  "SYNAPSE_USER_MANAGER_BAND": "35"})
    assert not junior.may_approve
    own = rv.approver_for(ME, {})
    assert own.self_review and own.name == ME and own.band == rv.MIN_BAND
    assert "no manager configured" in own.note
    assert rv.approver_for(ME, {"SYNAPSE_USER_MANAGER_BAND": "x",
                                "SYNAPSE_USER_MANAGER": "J"}).band == 40


def test_a_submission_is_pending_and_off_the_loader(tmp_path):
    r = _reviews(tmp_path)
    got = r.submit(kind="skill", name="Approvals triage", text=PACK,
                   purpose="approvals asks", submitter=ME,
                   submitter_slug="john-doe",
                   approver=rv.approver_for(ME, {}))
    assert got["ok"] and not got["resubmitted"]
    sub = got["submission"]
    assert sub["status"] == "pending" and sub["version"] == 1
    assert sub["name"] == "approvals-triage"
    assert sub["title"] == "Approvals triage"
    assert sub["description"].startswith("The moves for an approvals")
    assert sub["purpose"] == "approvals asks"
    assert sub["approver"]["name"] == ME and sub["approver"]["self_review"]
    assert sub["text"] == PACK
    assert sub["status_label"] == "Pending manager approval"
    # the text lives under the reviews folder, never on the shelf
    assert (tmp_path / "graph" / "runs" / "reviews" / "files" / sub["id"]
            / "v1.md").read_text(encoding="utf-8") == PACK
    assert get_skill(tmp_path / "graph", "approvals-triage", ME) is None
    assert "approvals-triage" not in {p.name for p in
                                      all_skills(tmp_path / "graph", ME)}
    # the ledger is one record per event
    lines = (tmp_path / "graph" / "runs" / "reviews" / "ledger.jsonl") \
        .read_text(encoding="utf-8").splitlines()
    assert [json.loads(ln)["ev"] for ln in lines] == ["submitted"]
    # what the person must give, and what is refused
    for fields, why in ((dict(kind="skill", name="x", text="short"), "empty"),
                        (dict(kind="poem", name="x", text=PACK), "skill or knowledge"),
                        (dict(kind="knowledge", name="x", text=PACK), "business unit"),
                        (dict(kind="skill", name="analysis-playbooks", text=PACK),
                         "built-in")):
        got = r.submit(purpose="p", submitter=ME, submitter_slug="john-doe",
                       approver=rv.approver_for(ME, {}),
                       reserved={"analysis-playbooks"}, **fields)
        assert not got["ok"] and why in got["reason"], (fields, got)


def test_the_checks_stand_in_and_the_model_reads(tmp_path):
    r = _reviews(tmp_path)
    line = "2. run the query, TBD, etc. to prove the split of rate and mix."
    vague = PACK.replace('2. run_sql(mode="dry_run") to prove the split.', line)
    vague += "\n" + line + "\n"                     # the same line twice
    got = r.submit(kind="skill", name="triage", text=vague, purpose="p",
                   submitter=ME, submitter_slug="john-doe",
                   approver=rv.approver_for(ME, {}))
    sid = got["submission"]["id"]
    # the checks, synchronously, then the model's read in the background
    r.start_ai(sid, lambda sub, text: {"ok": True, "review": rv._norm_review({
        "summary": {"executive": "A triage pack for approvals.",
                    "topics": ["approvals", "rates"],
                    "purpose": "approvals asks", "audience": "risk analysts"},
        "insights": [{"category": "Ambiguous statements", "confidence": "High",
                      "explanation": "step 2 says TBD", "reference": "Rate first"},
                     {"category": "nonsense", "confidence": "?",
                      "explanation": "odd", "reference": ""}],
        "recommendation": "Needs Minor Updates", "reason": "one TBD"},
        by="Gemini 3.1 Pro Preview")})
    assert r.wait(sid, 10)
    sub = r.get(sid)
    assert sub["ai_status"] == "done"
    review = sub["ai_review"]
    assert review["by"] == "Gemini 3.1 Pro Preview"
    assert review["recommendation"] == "minor"
    assert [i["category"] for i in review["insights"]] == [
        "ambiguous", "missing_context"]
    assert review["insights"][0]["confidence"] == "high"
    assert review["insights"][1]["confidence"] == "medium"
    assert review["summary"]["topics"] == ["approvals", "rates"]
    assert sub["ai_summary"]["recommendation_label"] == "Needs minor updates"
    # the checks alone, when the model is away: labelled as checks
    checks = rv.checks_review("skill", "triage", vague, purpose="p", year=2026)
    assert checks["by"] == "checks"
    cats = {i["category"] for i in checks["insights"]}
    assert {"ambiguous", "duplicate"} <= cats
    assert checks["recommendation"] == "significant"      # a high finding
    assert any("TBD" in i["explanation"] for i in checks["insights"])
    clean = rv.checks_review("skill", "t", PACK, year=2026)
    assert clean["recommendation"] == "ready" and clean["insights"] == []
    dated = rv.checks_review("knowledge", "t", "# T\n\nThe 2021 rules apply.\n"
                             "Card-member spend and cardmember spend.\n",
                             year=2026)
    assert {"outdated", "inconsistent_terminology"} <= {
        i["category"] for i in dated["insights"]}
    # a model that answers nothing usable: the checks stay, marked failed
    got = r.submit(kind="skill", name="second", text=PACK, purpose="p",
                   submitter=ME, submitter_slug="john-doe",
                   approver=rv.approver_for(ME, {}))
    sid2 = got["submission"]["id"]
    r.start_ai(sid2, lambda sub, text: {"ok": False, "reason": "no model"})
    assert r.wait(sid2, 10)
    sub2 = r.get(sid2)
    assert sub2["ai_status"] == "failed" and sub2["ai_reason"] == "no model"
    assert sub2["ai_review"]["by"] == "checks"
    # the norm refuses what is not a review
    assert rv._norm_review({"summary": {}, "insights": []}, by="x") is None
    assert rv._norm_review("nope", by="x") is None
    assert rv.ai_review(ScriptedAgent(json_answers=[{}]), "skill", "t",
                        PACK)["ok"] is False


def test_approve_publishes_reject_sends_back_resubmit_bumps(tmp_path):
    r = _reviews(tmp_path)
    approver = rv.approver_for(ME, {"SYNAPSE_USER_MANAGER": "Jane Doe"})
    sid = r.submit(kind="skill", name="triage", text=PACK, purpose="p",
                   submitter=ME, submitter_slug="john-doe",
                   approver=approver)["submission"]["id"]
    # a rejection needs the reason
    refused = r.decide(sid, "reject")
    assert not refused["ok"] and "reason" in refused["reason"]
    rejected = r.decide(sid, "reject", comment="name the tool in step 1")
    sub = rejected["submission"]
    assert sub["status"] == "rejected" and sub["decided_by"] == "Jane Doe"
    assert sub["comments"][-1] == {**sub["comments"][-1], "event": "rejected",
                                   "text": "name the tool in step 1"}
    assert not r.decide(sid, "approve")["ok"]          # not pending
    again = r.resubmit(sid, text=PACK.replace("search(", "search(q="),
                       comment="named it", by=ME)
    assert again["ok"] and again["submission"]["version"] == 2
    assert again["submission"]["status"] == "pending"
    assert again["submission"]["ai_status"] == "running"
    assert r.text_of(sid, 1) == PACK and "search(q=" in r.text_of(sid)
    # a second submit under the same name is a new version, not a twin
    third = r.submit(kind="skill", name="triage", text=PACK, purpose="p",
                     submitter=ME, submitter_slug="john-doe",
                     approver=approver)
    assert third["resubmitted"] and third["submission"]["version"] == 3
    assert len(r.list()) == 1
    published: list = []
    ok = r.decide(sid, "approve", comment="good",
                  publish=lambda sub, text: published.append((sub["name"], text))
                  or {"ok": True, "path": "/shelf/triage.md"})
    assert ok["ok"] and ok["submission"]["status"] == "published"
    assert ok["submission"]["published_path"] == "/shelf/triage.md"
    assert published == [("triage", PACK)]
    assert r.published_names() == {"triage"}
    # a publish door that refuses leaves it pending
    sid2 = r.submit(kind="skill", name="other", text=PACK, purpose="p",
                    submitter=ME, submitter_slug="john-doe",
                    approver=approver)["submission"]["id"]
    bad = r.decide(sid2, "approve",
                   publish=lambda sub, text: {"ok": False, "reason": "disk full"})
    assert not bad["ok"] and bad["reason"] == "disk full"
    assert r.get(sid2)["status"] == "pending"
    # band 40 is the floor
    junior = rv.approver_for(ME, {"SYNAPSE_USER_MANAGER": "Sam",
                                  "SYNAPSE_USER_MANAGER_BAND": "30"})
    sid3 = r.submit(kind="skill", name="third", text=PACK, purpose="p",
                    submitter=ME, submitter_slug="john-doe",
                    approver=junior)["submission"]["id"]
    held = r.decide(sid3, "approve")
    assert not held["ok"] and "band 40 or above" in held["reason"]
    assert r.withdraw(sid3, by=ME)["submission"]["status"] == "withdrawn"
    assert r.find("skill", "third", "john-doe") is None


def test_the_notices_are_the_ledger_read_for_a_person(tmp_path):
    r = _reviews(tmp_path)
    approver = rv.approver_for(ME, {"SYNAPSE_USER_MANAGER": "Jane Doe"})
    sid = r.submit(kind="knowledge", name="TLS glossary", business_unit="TLS",
                   text="# TLS glossary\n\nNet sales: gross less cancels.\n",
                   purpose="definitions", submitter=ME,
                   submitter_slug="john-doe", approver=approver)["submission"]["id"]
    board = r.notices("john-doe")
    assert board["pending"] == 1 and board["unread"] == 2
    texts = [n["text"] for n in board["notices"]]
    assert any("submitted 'TLS glossary' (knowledge) for your review" in t
               for t in texts)
    assert any("submitted to Jane Doe for approval" in t for t in texts)
    assert {n["to"] for n in board["notices"]} == {"approver", "submitter"}
    r.mark_seen("john-doe")
    assert r.notices("john-doe")["unread"] == 0
    r.decide(sid, "reject", comment="cite the source")
    after = r.notices("john-doe")
    assert after["unread"] == 1 and after["pending"] == 0
    assert after["notices"][0]["event"] == "rejected"
    assert "cite the source" in after["notices"][0]["text"]


def test_the_runtime_wires_the_doors(compiled, tmp_path, monkeypatch):  # noqa: F811
    """Through the runtime: the submitter is the configured person, the
    reserved names are the built-ins, approval writes the own pack
    where the loader reads it or stages the knowledge file, and the
    model's read comes from the chat's plane."""
    from sahs.assistant import AssistantRuntime
    monkeypatch.setenv("SYNAPSE_USER_NAME", ME)
    monkeypatch.delenv("SYNAPSE_USER_MANAGER", raising=False)
    build, _ = compiled
    agent = ScriptedAgent(json_answers=[{
        "summary": {"executive": "Doctrine for approvals.", "topics": ["approvals"],
                    "purpose": "approvals asks", "audience": "risk"},
        "insights": [], "recommendation": "ready", "reason": "clean"}])
    runtime = AssistantRuntime(builds_root=build.root.parent,
                               graph_root=tmp_path / "graph",
                               store_path=tmp_path / "chat.sqlite3",
                               model_factory=lambda budget: agent)
    runtime.knowledge_dir = tmp_path / "sources" / "artifacts"
    got = runtime.submit_for_review(kind="skill", name="Approvals triage",
                                    text=PACK, purpose="approvals asks")
    assert got["ok"]
    sid = got["submission"]["id"]
    assert runtime.reviews.wait(sid, 10)
    sub = runtime.reviews.get(sid)
    assert sub["ai_status"] == "done" and sub["ai_review"]["by"] == "scripted"
    assert sub["ai_review"]["recommendation"] == "ready"
    assert sub["submitter"] == ME and sub["approver"]["self_review"]
    assert "approvals-triage" not in {p["name"] for p in runtime.skills()}
    board = runtime.review_board()
    assert board["pending"] == 1 and board["approver"]["name"] == ME
    assert board["min_band"] == 40 and board["me"] == ME
    assert runtime.decide_review(sid, "approve", "fine")["ok"]
    assert (tmp_path / "graph" / "skills" / "users" / "john-doe"
            / "approvals-triage.md").read_text(encoding="utf-8") == PACK
    mine = {p["name"]: p for p in runtime.skills()}
    assert mine["approvals-triage"]["mine"] and mine["approvals-triage"]["author"] == "You"
    # a built-in name never gets in
    assert "built-in" in runtime.submit_for_review(
        kind="skill", name="charts", text=PACK, purpose="p")["reason"]
    # a knowledge file lands staged for its unit, with its provenance
    kn = runtime.submit_for_review(kind="knowledge", name="TLS glossary",
                                   business_unit="TLS", purpose="definitions",
                                   text="# TLS glossary\n\nNet sales: gross less cancels.\n")
    assert kn["ok"]
    runtime.reviews.wait(kn["submission"]["id"], 10)
    done = runtime.decide_review(kn["submission"]["id"], "approve")
    path = Path(done["submission"]["published_path"])
    assert path == tmp_path / "sources" / "artifacts" / "tls_tls-glossary.md"
    head = path.read_text(encoding="utf-8").splitlines()[0]
    assert "business unit TLS" in head and f"actor {ME}" in head
    assert f"approved by {ME}" in head
    # deleting the published pack withdraws its record
    assert runtime.delete_my_skill("approvals-triage")
    assert runtime.reviews.get(sid)["status"] == "withdrawn"
    # the model away: the checks stay and say so
    runtime._model_factory = None
    monkeypatch.delenv("SYNAPSE_VERTEX_SA_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("APP_ID", raising=False)
    monkeypatch.delenv("APP_SECRET", raising=False)
    monkeypatch.delenv("GEMINI_BEARER_TOKEN", raising=False)
    away = runtime.submit_for_review(kind="skill", name="later", text=PACK,
                                     purpose="p")
    runtime.reviews.wait(away["submission"]["id"], 10)
    sub = runtime.reviews.get(away["submission"]["id"])
    assert sub["ai_status"] == "failed" and sub["ai_review"]["by"] == "checks"
    assert "unavailable" in sub["ai_reason"]
