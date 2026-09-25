"""The content store (007_content.sql and the content tables of
002_chat.sql): the files on a chat with their bytes in chunks, a
person's own skills, the knowledge files and the review board — on
the sqlite stand-in directly and through ``SpannerDatabase`` over the
fake SDK database, so the Spanner code path (typed params, JsonObject
cells, BYTES cells, mutations) runs without a Spanner. Each group
mirrors the filesystem module it replaces: the same rows, the same
parts, the same fold."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "tests"))

from fake_spanner import FakeSpannerDatabase  # noqa: E402
from sahs.assistant import authoring, reviews as rv  # noqa: E402
from sahs.assistant import files as files_mod  # noqa: E402
from sahs.assistant.content_store import (CHUNK_BYTES,  # noqa: E402
                                          SpannerContentStore)
from sahs.assistant.skills_loader import (all_skills, bind_own_skills,  # noqa: E402
                                          get_skill, load_packs,
                                          unbind_own_skills)
from sahs.assistant.spanner_store import SpannerAssistantStore  # noqa: E402
from sahs.identity.database import SpannerDatabase, SqliteDatabase  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "files_check", SILO / "scripts" / "files_check.py")
files_check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(files_check)

PACK = ("# Approvals triage\n\nThe moves for an approvals question.\n\n"
        "## Rate first\n1. search(\"approval rate\") for the definition.\n"
        "2. run_sql(mode=\"dry_run\") to prove the split.\n\n"
        "## Never\n- never quote a rate without its denominator\n")
ME = "Ana Lyst"


@pytest.fixture(params=["sqlite", "spanner"])
def backend(request):
    """(database, a reader of the raw rows) for each backend."""
    if request.param == "sqlite":
        db = SqliteDatabase(":memory:")
        return db, lambda table: db.query(f"SELECT * FROM {table}")
    fake = FakeSpannerDatabase()
    return SpannerDatabase(fake), fake.rows


@pytest.fixture()
def store(backend) -> SpannerContentStore:
    return SpannerContentStore(backend[0], "u-ana")


@pytest.fixture()
def chat(backend) -> SpannerAssistantStore:
    return SpannerAssistantStore(backend[0], "u-ana")


def test_needs_an_owner(backend):
    with pytest.raises(ValueError, match="owner"):
        SpannerContentStore(backend[0], "")


# ── (a) files ────────────────────────────────────────────────
def test_files_land_as_rows_and_ride_the_turn_as_from_the_workspace(
        store, chat, backend, tmp_path):
    sid = chat.create_session("assistant")["id"]
    with pytest.raises(KeyError):
        store.add_file("s_nope", "rows.csv", b"a,b\n1,2\n")
    with pytest.raises(files_mod.FileRefused):
        store.add_file(sid, "a.exe", b"x")
    assert store.files(sid) == [] and backend[1]("ChatFiles") == []
    pdf = store.add_file(sid, "token.pdf", files_check.tiny_pdf("P"))
    csv = store.add_file(sid, "rows.csv", b"a,b\n1,2\n")
    book = store.add_file(sid, "Q2 spend.xlsx", files_check.tiny_xlsx("XLSX-1"))
    assert pdf["rides"] == "inline" and pdf["family"] == "document"
    assert csv["rides"] == "text" and csv["text_chars"] == 8
    assert book["rides"] == "convert" and book["name"] == "Q2-spend.xlsx"
    assert set(pdf) == {"id", "name", "suffix", "mime", "family", "rides", "size",
                        "text_chars", "note"}                 # Stored.row()
    listed = store.files(sid)
    assert [f["id"] for f in listed] == [pdf["id"], csv["id"], book["id"]]
    assert listed[0] == pdf and "sent_turn" not in listed[0]
    assert store.file_bytes(sid, pdf["id"]) == files_check.tiny_pdf("P")
    assert "## sheet: Summary" in store.file_text(sid, book["id"])
    assert store.file_text(sid, csv["id"]) == "a,b\n1,2\n"
    assert store.file_text(sid, pdf["id"]) == ""

    # the parts on the turn: the same as the workspace builds for the
    # same files, row for row
    ws = tmp_path / "ws"
    ref = [files_mod.store(ws, "token.pdf", files_check.tiny_pdf("P")),
           files_mod.store(ws, "rows.csv", b"a,b\n1,2\n"),
           files_mod.store(ws, "Q2 spend.xlsx", files_check.tiny_xlsx("XLSX-1"))]
    theirs, _used = files_mod.parts_for(ws, [r.id for r in ref])
    ours, used = store.parts_for(sid, [pdf["id"], csv["id"], book["id"]])
    assert ours == theirs and [u["id"] for u in used] == [pdf["id"], csv["id"], book["id"]]
    with pytest.raises(files_mod.FileRefused, match="no file f_nope"):
        store.parts_for(sid, ["f_nope"])

    # pending until sent, then remembered as sent
    store.mark_sent(sid, [pdf["id"], csv["id"]], "t_1")
    assert {r["id"] for r in store.pending(sid)} == {book["id"]}
    assert next(f for f in store.files(sid) if f["id"] == pdf["id"])["sent_turn"] == "t_1"
    assert backend[1]("ChatFiles")[0]["SentTurn"] == "t_1"
    # removed: the row and its chunks
    assert store.remove_file(sid, csv["id"]) and not store.remove_file(sid, csv["id"])
    assert [f["id"] for f in store.files(sid)] == [pdf["id"], book["id"]]
    assert len(backend[1]("ChatFiles")) == 2
    assert {r["FileId"] for r in backend[1]("ChatFileChunks")} == {pdf["id"], book["id"]}
    with pytest.raises(KeyError):
        store.file_bytes(sid, csv["id"])
    # another person's store sees nothing of this chat
    bo = SpannerContentStore(backend[0], "u-bo")
    with pytest.raises(KeyError):
        bo.files(sid)
    with pytest.raises(KeyError):
        bo.file_bytes(sid, pdf["id"])
    assert bo.remove_file(sid, pdf["id"]) is False


def test_a_nine_megabyte_file_is_two_chunks_and_comes_back_whole(store, chat, backend):
    sid = chat.create_session("assistant")["id"]
    data = os.urandom(9 * 1024 * 1024)
    row = store.add_file(sid, "scan.png", data)
    assert row["size"] == len(data)
    chunks = backend[1]("ChatFileChunks")
    assert [c["Seq"] for c in chunks] == [0, 1]
    assert [len(c["Chunk"]) for c in chunks] == [CHUNK_BYTES, len(data) - CHUNK_BYTES]
    assert store.file_bytes(sid, row["id"]) == data
    parts, _ = store.parts_for(sid, [row["id"]])
    import base64
    assert base64.b64decode(parts[1]["inlineData"]["data"]) == data


# ── (b) own skills ───────────────────────────────────────────
def test_own_skills_are_rows_per_person_and_the_loader_reads_them(store, backend, tmp_path):
    with pytest.raises(ValueError):
        store.save_skill("Bad Name", "t", "d", PACK)
    saved = store.save_skill("approvals-triage", "Approvals triage", "The moves", PACK)
    assert saved["replaced"] is False and saved["user_id"] == "u-ana"
    again = store.save_skill("approvals-triage", "Approvals triage", "The moves", PACK + "\n")
    assert again["replaced"] is True
    rows = backend[1]("UserSkills")
    assert len(rows) == 1 and rows[0]["UserId"] == "u-ana" and rows[0]["Origin"] == "unreviewed"
    assert [s["name"] for s in store.my_skills()] == ["approvals-triage"]
    bo = SpannerContentStore(backend[0], "u-bo")
    assert bo.my_skills() == []
    # the approver publishes onto the submitter's shelf by user id
    bo.save_skill("from-bo", "From Bo", "", PACK, user_id="u-ana")
    assert [s["name"] for s in store.my_skills()] == ["approvals-triage", "from-bo"]

    # through authoring: the same checks as the folder, the row as the home
    graph = tmp_path / "graph"
    got = authoring.save_skill(graph, ME, "Churn Triage", PACK, store=store)
    assert got["ok"] and got["name"] == "churn-triage" and got["owner"] == "ana-lyst"
    assert got["path"] == "UserSkills/u-ana/churn-triage" and not got["replaced"]
    assert authoring.save_skill(graph, ME, "churn-triage", PACK, store=store)["replaced"]
    assert not authoring.save_skill(graph, ME, "analysis-playbooks", PACK, store=store)["ok"]
    assert not (graph / "skills").exists()               # nothing on disk
    # the loader, once the shelf is bound for the owner
    bind_own_skills(ME, store.my_skills)
    try:
        mine = {p.name: p for p in all_skills(graph, ME)}
        assert mine["churn-triage"].owner == "ana-lyst"
        assert mine["churn-triage"].origin == "unreviewed" and mine["churn-triage"].text == PACK
        assert mine["analysis-playbooks"].origin == "built-in"
        assert get_skill(graph, "churn-triage", "Alice") is None
        loaded, missing = load_packs(graph, ["churn-triage", "nope"], owner=ME)
        assert [p.name for p in loaded] == ["churn-triage"] and missing == ["nope"]
        assert authoring.delete_skill(graph, ME, "churn-triage", store=store)
        assert not authoring.delete_skill(graph, ME, "churn-triage", store=store)
        assert get_skill(graph, "churn-triage", ME) is None
    finally:
        unbind_own_skills(ME)
    assert "approvals-triage" not in {p.name for p in all_skills(graph, ME)}


# ── (c) knowledge files ──────────────────────────────────────
def test_knowledge_files_are_shared_rows(store, backend):
    bad = store.stage_knowledge("T L S", "glossary", "md", "# G\n\nwords\n")
    assert not bad["ok"] and "business unit" in bad["reason"]
    assert not store.stage_knowledge("TLS", "g", "exe", "x")["ok"]
    assert not store.stage_knowledge("TLS", "g", "md", "  ")["ok"]
    got = store.stage_knowledge("TLS", "tls-glossary", "md", "# TLS glossary\n\nNet sales.\n")
    assert got["ok"] and got["file"] == "tls_tls-glossary.md" and not got["replaced"]
    assert got["staged_by"] == "u-ana"
    twice = store.stage_knowledge("TLS", "tls-glossary", "md", "# again\n")
    assert not twice["ok"] and "already staged" in twice["reason"]
    replaced = store.stage_knowledge("TLS", "tls-glossary", "md", "# v2\n", "u-bo", replace=True)
    assert replaced["ok"] and replaced["replaced"] and replaced["id"] == got["id"]
    other = SpannerContentStore(backend[0], "u-bo")
    other.stage_knowledge("CFR", "rules", "txt", "rules here")
    shelf = store.knowledge_files()
    assert [(f["business_unit"], f["file"], f["staged_by"]) for f in shelf] == [
        ("CFR", "cfr_rules.txt", "u-bo"), ("TLS", "tls_tls-glossary.md", "u-bo")]
    assert shelf[1]["content"] == "# v2\n" and shelf[1]["staged_at"].endswith("+00:00")
    assert [f["file"] for f in store.knowledge_files("TLS")] == ["tls_tls-glossary.md"]
    assert store.knowledge_file(got["id"])["name"] == "tls-glossary"
    assert store.retire_knowledge(got["id"]) and not store.retire_knowledge(got["id"])
    assert [f["file"] for f in store.knowledge_files()] == ["cfr_rules.txt"]
    assert next(r for r in backend[1]("KnowledgeFiles") if r["FileId"] == got["id"])["RetiredAt"]
    # staged again after retiring: the row comes back, new content
    back = store.stage_knowledge("TLS", "tls-glossary", "md", "# v3\n")
    assert back["ok"] and back["id"] == got["id"]
    assert store.knowledge_file(got["id"])["content"] == "# v3\n"
    assert len(backend[1]("KnowledgeFiles")) == 2


# ── (d) the review board ─────────────────────────────────────
def _scrub(sub: dict) -> dict:
    """A folded submission without what differs by construction: the
    id and the instants."""
    out = {k: v for k, v in sub.items()
           if k not in ("id", "submitted_at", "updated_at", "decided_at", "text")}
    out["comments"] = [{k: v for k, v in c.items() if k != "at"} for c in sub["comments"]]
    if out.get("ai_review"):
        out["ai_review"] = {k: v for k, v in out["ai_review"].items() if k != "at"}
    return out


def _run_the_workflow(board, approver):
    """submit, ai_review, reject, resubmit, approve, withdraw — the
    same sequence on either board; → (the submission, the log)."""
    log = []
    got = board.submit(kind="skill", name="Approvals triage", text=PACK,
                       purpose="approvals asks", submitter=ME,
                       submitter_slug="ana-lyst", approver=approver)
    assert got["ok"] and not got["resubmitted"]
    sid = got["submission"]["id"]
    log.append(got["submission"])
    board.start_ai(sid, lambda sub, text: {"ok": True, "review": rv._norm_review({
        "summary": {"executive": "A triage pack for approvals.", "topics": ["approvals"],
                    "purpose": "approvals asks", "audience": "risk analysts"},
        "insights": [{"category": "ambiguous", "confidence": "high",
                      "explanation": "step 2", "reference": "Rate first"}],
        "recommendation": "minor", "reason": "one thing"}, by="scripted")})
    assert board.wait(sid, 10)
    log.append(board.get(sid))
    assert not board.decide(sid, "reject")["ok"]                   # no reason
    log.append(board.decide(sid, "reject", comment="name the tool")["submission"])
    assert not board.decide(sid, "approve")["ok"]                  # not pending
    again = board.resubmit(sid, text=PACK.replace("search(", "search(q="),
                           comment="named it", by=ME)
    log.append(again["submission"])
    third = board.submit(kind="skill", name="approvals-triage", text=PACK, purpose="p",
                         submitter=ME, submitter_slug="ana-lyst", approver=approver)
    assert third["resubmitted"] and third["submission"]["version"] == 3
    log.append(third["submission"])
    published = []
    ok = board.decide(sid, "approve", comment="good",
                      publish=lambda sub, text: published.append((sub["name"], text))
                      or {"ok": True, "path": "/shelf/approvals-triage.md"})
    assert ok["ok"] and published == [("approvals-triage", PACK)]
    log.append(ok["submission"])
    log.append(board.withdraw(sid, by=ME)["submission"])
    return sid, log


def test_the_board_folds_to_the_ledger_shape(store, backend, tmp_path):
    approver = rv.approver_for(ME, {"SYNAPSE_USER_MANAGER": "Jane Doe",
                                    "SYNAPSE_USER_MANAGER_BAND": "42"})
    ledger = rv.Reviews(tmp_path / "graph" / "runs" / "reviews")
    _lsid, theirs = _run_the_workflow(ledger, approver)
    sid, ours = _run_the_workflow(store, approver)
    assert len(ours) == len(theirs) == 7
    for mine, ref in zip(ours, theirs, strict=True):
        assert set(mine) >= set(ref), set(ref) - set(mine)
        assert {k: v for k, v in _scrub(mine).items() if k in ref} == _scrub(ref)
        assert mine["submitted_at"].endswith("+00:00") and mine["updated_at"] >= mine["submitted_at"]
    # the extra the store knows: whose row an approval writes
    assert ours[0]["submitter_user_id"] == "u-ana"
    statuses = [s["status"] for s in ours]
    assert statuses == ["pending", "pending", "rejected", "pending", "pending",
                        "published", "withdrawn"]
    assert ours[1]["ai_status"] == "done" and ours[1]["ai_review"]["by"] == "scripted"
    assert ours[1]["ai_summary"]["recommendation_label"] == "Needs minor updates"
    assert ours[3]["ai_status"] == "running" and ours[3]["ai_review"] is None
    assert ours[5]["published_path"] == "/shelf/approvals-triage.md"
    assert ours[5]["decided_by"] == "Jane Doe"
    assert [c["event"] for c in ours[6]["comments"]] == ["rejected", "resubmitted", "approved"]
    # the text of every version, and the head row
    assert store.text_of(sid, 1) == PACK and "search(q=" in store.text_of(sid, 2)
    assert store.text_of(sid) == PACK and store.text_of("sub_nope") == ""
    head = backend[1]("ReviewSubmissions")
    assert len(head) == 1 and head[0]["Status"] == "withdrawn" and head[0]["Version"] == 3
    assert head[0]["SubmitterUserId"] == "u-ana" and head[0]["ApproverBand"] == 42
    events = backend[1]("ReviewEvents")
    assert [e["Event"] for e in sorted(events, key=lambda e: e["Seq"])] == [
        "submitted", "ai_review", "ai_review", "rejected", "resubmitted",
        "resubmitted", "approved", "withdrawn"]
    assert [v["Version"] for v in backend[1]("ReviewVersions")] == [1, 2, 3]
    # withdrawn: no longer found, not published, not resubmittable
    assert store.find("skill", "approvals-triage", "ana-lyst") is None
    assert store.published_names() == set()
    assert "withdrawn" in store.resubmit(sid, text=PACK)["reason"]
    assert store.get("sub_nope") is None
    assert not store.decide("sub_nope", "approve")["ok"]
    assert not store.withdraw("sub_nope")["ok"]
    # the door's checks, the ledger's words
    for fields, why in ((dict(kind="skill", name="x", text="short"), "empty"),
                        (dict(kind="poem", name="x", text=PACK), "skill or knowledge"),
                        (dict(kind="knowledge", name="x", text=PACK), "business unit"),
                        (dict(kind="skill", name="charts", text=PACK), "built-in")):
        got = store.submit(purpose="p", submitter=ME, submitter_slug="ana-lyst",
                           approver=approver, reserved={"charts"}, **fields)
        assert not got["ok"] and why in got["reason"], (fields, got)
    # a publish door that refuses leaves it pending; a junior cannot approve
    other = store.submit(kind="skill", name="other", text=PACK, purpose="p", submitter=ME,
                         submitter_slug="ana-lyst", approver=approver)["submission"]["id"]
    bad = store.decide(other, "approve", publish=lambda s, t: {"ok": False, "reason": "disk full"})
    assert not bad["ok"] and bad["reason"] == "disk full" and store.get(other)["status"] == "pending"
    junior = rv.approver_for(ME, {"SYNAPSE_USER_MANAGER": "Sam", "SYNAPSE_USER_MANAGER_BAND": "30"})
    low = store.submit(kind="skill", name="junior", text=PACK, purpose="p", submitter=ME,
                       submitter_slug="ana-lyst", approver=junior)["submission"]["id"]
    assert "band 40 or above" in store.decide(low, "approve")["reason"]
    assert {s["name"] for s in store.list()} == {"junior", "other", "approvals-triage"}
    assert "text" not in store.list()[0] and "text" in store.list(with_text=True)[0]
    assert store.published_names() == set()
    ok = store.decide(other, "approve")
    assert ok["ok"] and store.published_names() == {"other"}


def test_the_notices_are_the_events_read_for_a_person(store, backend):
    approver = rv.approver_for(ME, {"SYNAPSE_USER_MANAGER": "Jane Doe"})
    sid = store.submit(kind="knowledge", name="TLS glossary", business_unit="TLS",
                       text="# TLS glossary\n\nNet sales: gross less cancels.\n",
                       purpose="definitions", submitter=ME, submitter_slug="ana-lyst",
                       approver=approver)["submission"]["id"]
    board = store.notices("ana-lyst")
    assert board["pending"] == 1 and board["unread"] == 2
    texts = [n["text"] for n in board["notices"]]
    assert any("submitted 'TLS glossary' (knowledge) for your review" in t for t in texts)
    assert any("submitted to Jane Doe for approval" in t for t in texts)
    assert {n["to"] for n in board["notices"]} == {"approver", "submitter"}
    store.mark_seen("ana-lyst")
    assert store.notices("ana-lyst")["unread"] == 0
    assert backend[1]("ReviewSeen")[0]["UserId"] == "u-ana"      # the owner's mark
    store.decide(sid, "reject", comment="cite the source")
    after = store.notices("ana-lyst")
    assert after["unread"] == 1 and after["pending"] == 0
    assert after["notices"][0]["event"] == "rejected"
    assert "cite the source" in after["notices"][0]["text"]
    # another person on the same board: the same notices, their own mark
    bo = SpannerContentStore(backend[0], "u-bo")
    assert bo.notices()["unread"] == 3
    bo.mark_seen()
    assert bo.notices()["unread"] == 0 and store.notices()["unread"] == 1
    assert len(backend[1]("ReviewSeen")) == 2
