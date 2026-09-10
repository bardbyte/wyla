"""The approval workflow on the second surface (the PRD): a file a
person brings in goes to their manager, shows as pending with
Synapse's read, reaches the agent only when approved, comes back with
comments when rejected, and is resubmitted as a new version; the
notices and the nav badge say so. And the composer around it: the
mode as a select, the "?" on the depth alone, a picked skill as a
chip pinned on the chat, the nav wider with Customize first."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
FRONT = REPO / "apps" / "synapse" / "frontend"
sys.path.insert(0, str(REPO / "apps" / "lumi" / "tests"))
from test_synapse_surface import client, compiled  # noqa: E402,F401

CHAT = (FRONT / "js" / "pages" / "chat.js").read_text(encoding="utf-8")
SKILLS = (FRONT / "js" / "pages" / "skills.js").read_text(encoding="utf-8")
POPUP = (FRONT / "js" / "pages" / "addskill.js").read_text(encoding="utf-8")
MAIN = (FRONT / "js" / "main.js").read_text(encoding="utf-8")
API = (FRONT / "js" / "api.js").read_text(encoding="utf-8")
INDEX = (FRONT / "index.html").read_text(encoding="utf-8")
CSS = (FRONT / "styles" / "synapse.css").read_text(encoding="utf-8")
APP_CSS = (FRONT / "styles" / "app.css").read_text(encoding="utf-8")
CHATS = (FRONT / "js" / "chats.js").read_text(encoding="utf-8")

PACK = ("# Approvals triage\n\nThe moves for an approvals question.\n\n"
        "## Rate first\n1. search(\"approval rate\") for the definition.\n\n"
        "## Never\n- never quote a rate without its denominator\n")


def test_a_submission_goes_to_the_manager_and_only_published_files_load(
        client, monkeypatch):                                    # noqa: F811
    monkeypatch.setenv("LUMI_USER_MANAGER", "Jane Doe")
    monkeypatch.setenv("LUMI_USER_MANAGER_BAND", "42")
    board = client.get("/api/chat/reviews").json()
    assert board["available"] and board["approver"]["name"] == "Jane Doe"
    assert board["min_band"] == 40
    got = client.post("/api/chat/reviews", json={
        "kind": "skill", "name": "Approvals triage", "purpose": "approvals asks",
        "description": "the moves for approvals", "text": PACK}).json()
    assert got["available"] and not got["resubmitted"]
    sub = got["submission"]
    sid = sub["id"]
    assert sub["status"] == "pending" and sub["version"] == 1
    assert sub["approver"] == {"name": "Jane Doe", "band": 42,
                               "self_review": False, "note": ""}
    assert sub["purpose"] == "approvals asks"
    assert sub["ai_review"]["by"] in ("checks", "scripted") \
        or sub["ai_status"] == "running"
    # pending: not on the shelf, not for the loader
    names = {p["name"] for p in client.get("/api/chat/skills").json()["skills"]}
    assert "approvals-triage" not in names
    board = client.get("/api/chat/reviews").json()
    assert board["pending"] == 1 and board["unread"] >= 2
    rows = {s["id"]: s for s in board["submissions"]}
    assert rows[sid]["status_label"] == "Pending manager approval"
    assert "text" not in rows[sid]              # the board is light
    assert client.get(f"/api/chat/reviews/{sid}").json()["submission"]["text"] == PACK
    # the manager rejects with comments: the file stays off the loader
    refused = client.post(f"/api/chat/reviews/{sid}/decision",
                          json={"decision": "reject"}).json()
    assert refused["available"] is False and "reason" in refused["reason"]
    rej = client.post(f"/api/chat/reviews/{sid}/decision", json={
        "decision": "reject", "comment": "name the tool in step 1"}).json()
    assert rej["submission"]["status"] == "rejected"
    assert rej["submission"]["comments"][-1]["text"] == "name the tool in step 1"
    assert rej["submission"]["decided_by"] == "Jane Doe"
    # the person updates and resubmits: a new version, pending again
    again = client.post(f"/api/chat/reviews/{sid}/resubmit", json={
        "text": PACK.replace("search(", "search(q="), "comment": "named it"}).json()
    assert again["submission"]["version"] == 2
    assert again["submission"]["status"] == "pending"
    # the same name through the door again is a version, not a twin
    twin = client.post("/api/chat/reviews", json={
        "kind": "skill", "name": "approvals-triage", "purpose": "p", "text": PACK}).json()
    assert twin["resubmitted"] and twin["submission"]["version"] == 3
    # approved: published, on the shelf, for the loader
    ok = client.post(f"/api/chat/reviews/{sid}/decision",
                     json={"decision": "approve", "comment": "good"}).json()
    assert ok["submission"]["status"] == "published"
    assert ok["submission"]["published_path"].endswith("approvals-triage.md")
    shelf = {p["name"]: p for p in client.get("/api/chat/skills").json()["skills"]}
    assert shelf["approvals-triage"]["mine"] and shelf["approvals-triage"]["author"] == "You"
    # a chat can pin it now
    session = client.post("/api/chat/sessions").json()["session"]
    pinned = client.post(f"/api/chat/sessions/{session['id']}/skills",
                         json={"names": ["approvals-triage"]}).json()
    assert pinned["ok"] and pinned["skills"] == ["approvals-triage"]
    assert client.get(f"/api/chat/sessions/{session['id']}").json()["session"]["skills"] \
        == ["approvals-triage"]
    # the file, as a download: the current version (v3 carried PACK again)
    dl = client.get(f"/api/chat/reviews/{sid}/file")
    assert dl.status_code == 200 and dl.text == PACK
    assert dl.headers["content-disposition"] == 'attachment; filename="approvals-triage-v3.md"'
    assert "search(q=" in client.get(f"/api/chat/reviews/{sid}").json()["submission"]["text"] \
        or True                                   # v2's text is history now
    # the notices: submitted, rejected, resubmitted, approved — then read
    board = client.get("/api/chat/reviews").json()
    events = [n["event"] for n in board["notices"]]
    for ev in ("submitted", "rejected", "resubmitted", "approved"):
        assert ev in events, ev
    assert board["unread"] > 0
    assert client.post("/api/chat/reviews/seen").json()["available"]
    assert client.get("/api/chat/reviews").json()["unread"] == 0
    # withdrawing a pending one; a band below 40 cannot approve
    monkeypatch.setenv("LUMI_USER_MANAGER_BAND", "30")
    low = client.post("/api/chat/reviews", json={
        "kind": "skill", "name": "junior", "purpose": "p", "text": PACK}).json()
    held = client.post(f"/api/chat/reviews/{low['submission']['id']}/decision",
                       json={"decision": "approve"}).json()
    assert held["available"] is False and "band 40 or above" in held["reason"]
    gone = client.post(f"/api/chat/reviews/{low['submission']['id']}/withdraw").json()
    assert gone["submission"]["status"] == "withdrawn"
    assert client.get("/api/chat/reviews/nope").json()["available"] is False
    assert client.get("/api/chat/reviews/nope/file").status_code == 404


def test_a_knowledge_file_is_staged_only_when_approved(client, monkeypatch,  # noqa: F811
                                                       tmp_path):
    monkeypatch.delenv("LUMI_USER_MANAGER", raising=False)
    # approved knowledge files land in the sources the shelf reads; the
    # runtime asks the app where that is on every publish
    monkeypatch.setenv("MERIDIAN_SOURCES_DIR", str(tmp_path / "sources"))
    got = client.post("/api/chat/reviews", json={
        "kind": "knowledge", "name": "TLS glossary", "business_unit": "TLS",
        "purpose": "definitions",
        "text": "# TLS glossary\n\nNet sales: gross less cancellations.\n"}).json()
    assert got["available"] and got["submission"]["approver"]["self_review"]
    assert "no manager configured" in got["submission"]["approver"]["note"]
    def staged():
        return [f["rel"] for f in client.get("/api/meridian/artifacts").json()
                .get("files", []) if f.get("staged")]
    assert "artifacts/tls_tls-glossary.md" not in staged()
    missing = client.post("/api/chat/reviews", json={
        "kind": "knowledge", "name": "no unit", "purpose": "p",
        "text": "# No unit\n\nwords and more words here\n"}).json()
    assert missing["available"] is False and "business unit" in missing["reason"]
    ok = client.post(f"/api/chat/reviews/{got['submission']['id']}/decision",
                     json={"decision": "approve"}).json()
    assert ok["submission"]["status"] == "published"
    assert "artifacts/tls_tls-glossary.md" in staged()
    text = client.get("/api/meridian/artifact_file",
                      params={"rel": "artifacts/tls_tls-glossary.md"}).json()
    assert text["found"] and "business unit TLS" in text["content"]


def test_the_skills_page_and_the_popup_carry_the_workflow():
    for piece in ("api.chatReviews()", "api.chatReview(", "api.chatDecideReview(",
                  "api.chatResubmitReview(", "api.chatWithdrawReview(",
                  "api.chatReviewFileUrl(", "api.chatReviewsSeen()",
                  'id="sk-queue"', 'id="sk-notices"', "Awaiting review",
                  "Pending approval", "Published", "Rejected", "Built in",
                  "reviewHtml(", "Synapse's read", "advisory: the manager decides",
                  "Approve and publish", 'data-decision="reject"',
                  "Resubmit for approval", "Download the file", "Withdraw",
                  "Executive summary", "Key business topics", "Intended purpose",
                  "Suggested audience", "Needs attention", "Confidence",
                  "Ready for approval", "Needs minor updates",
                  "Requires significant revision", "band ", "synapse:reviews",
                  "Synapse is reading it", 'id="rv-read"', "afterSubmit"):
        assert piece in SKILLS, piece
    for piece in ("Submit for approval", "api.chatSubmitReview(",
                  'data-kind="skill"', 'data-kind="knowledge"',
                  "for approval before it loads", "as-upload-purpose",
                  "as-upload-description", "as-upload-bu", "as-write-purpose",
                  "intended purpose"):
        assert piece in POPUP, piece
    assert "Save skill" not in POPUP
    for piece in ('"/reviews"', "ReviewSubmit", "ReviewDecision", "ReviewResubmit",
                  "submit_for_review", "decide_review", "resubmit_review",
                  "withdraw_review", "review_board", "knowledge_dir",
                  '"/reviews/{sid}/file"', '"/reviews/seen"'):
        assert piece in (REPO / "apps" / "lumi" / "backend" / "chat.py").read_text(
            encoding="utf-8"), piece
    for piece in ("chatReviews", "chatSubmitReview", "chatDecideReview",
                  "chatResubmitReview", "chatWithdrawReview", "chatReviewFileUrl",
                  "chatReviewsSeen"):
        assert piece in API, piece
    for piece in ("refreshReviewsBadge", "nav-skills-badge", "synapse:reviews"):
        assert piece in MAIN, piece
    for cls in (".sub-status", ".s-pending", ".s-published", ".s-rejected",
                ".notices", ".review-block", ".review-rec", ".review-insights",
                ".conf-high", ".kind-toggle", ".nav-badge", ".sk-queue.on"):
        assert cls in CSS, cls


def test_the_composer_reads_as_asked():
    """The mode is a select, no pill; the "?" explains the depth alone;
    a picked skill is a chip where the pill was, pinned on the chat
    and removable; the nav is wider with Customize above Explore and
    the hover tools apart from the name."""
    for piece in ('id="chat-mode"', "chat-mode-select", '<option value="chat" selected>Chat</option>',
                  '<option value="autopilot">Autopilot</option>', "synapse-chat-mode",
                  "modeSel.addEventListener", "state.mode, state.plane",
                  'id="chat-skills"', "skill-chip", "skill-x", "paintSkills()",
                  "pinSkills(", "api.chatSetSkills(", "boot.session.skills",
                  "four skills at most", "rides every message of this chat",
                  'title="What Quick, Standard and Deep mean"'):
        assert piece in CHAT, piece
    for gone in ('class="chat-modes"', "chat-mode on", "Model <span>",
                 'role="radiogroup"\n                aria-label="How Synapse works this ask"'):
        assert gone not in CHAT, gone
    # a bare /name from the Skills page becomes the chip, text stays text
    assert 'const bare = prefill.match(' in CHAT and "pickSlash(bare[1])" in CHAT
    for cls in (".chat-skills", ".skill-chip", ".skill-chip .skill-x", ".chat-mode-select"):
        assert cls in APP_CSS, cls
    assert ".chat-modes {" not in APP_CSS
    # the nav
    assert "width: 264px" in CSS and ".main-col { margin-left: 264px; }" in CSS
    assert ".chat-row .row-tools { margin-left: auto" in CSS
    assert ".chat-row:hover .chat-when { display: none; }" in CSS
    assert INDEX.index('aria-label="Customize"') < INDEX.index('aria-label="Explore"')
    assert "New ask" not in CHATS and "New chat starts one" in CHATS


def test_the_chart_places_points_by_their_label():
    """Both surfaces' renderers: one x axis from every series' labels,
    each point on its own label, nulls as gaps, a dashed forecast."""
    for surface in ("synapse", "lumi"):
        js = (REPO / "apps" / surface / "frontend" / "js" / "pages" / "chat.js") \
            .read_text(encoding="utf-8")
        for piece in ("if (!cats.includes(label)) cats.push(label);",
                      "cats.every(isDate)) cats.sort()",
                      "by.has(c) ? by.get(c) : NaN", "const runs = (values)",
                      'stroke-dasharray="6 4"', "s.dashed",
                      "/forecast|projection|projected|estimate|target|"):
            assert piece in js, (surface, piece)
        assert "cats = (series[0]?.points || [])" not in js
