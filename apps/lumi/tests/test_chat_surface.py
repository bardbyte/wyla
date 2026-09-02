"""Synapse v3 §6 — the conversational surface is wired to the loop.

The frontend has no build step, so these are the missing compiler:
every event the assistant emits must reach an arm on the page, every
api helper the page calls must exist and point at a served route, and
the governance the validator enforces must be VISIBLE (status chips,
meridian lines, watermarks) — an enforced rule the user cannot see is
a rule they cannot trust.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONTEND = REPO_ROOT / "apps" / "lumi" / "frontend"
SILO = REPO_ROOT / "synapse-agentic-harness-system"

CHAT_JS = (FRONTEND / "js" / "pages" / "chat.js").read_text(
    encoding="utf-8")
CHATS_JS = (FRONTEND / "js" / "chats.js").read_text(encoding="utf-8")
API_JS = (FRONTEND / "js" / "api.js").read_text(encoding="utf-8")
MAIN_JS = (FRONTEND / "js" / "main.js").read_text(encoding="utf-8")
INDEX = (FRONTEND / "index.html").read_text(encoding="utf-8")
CSS = (FRONTEND / "styles" / "app.css").read_text(encoding="utf-8")
BACKEND = (REPO_ROOT / "apps" / "lumi" / "backend"
           / "chat.py").read_text(encoding="utf-8")


def test_every_assistant_event_reaches_the_page():
    sys.path.insert(0, str(SILO))
    from sahs.assistant.events import ASSISTANT_EVENTS
    handled = set(re.findall(r'case "(\w+)":', CHAT_JS))
    missing = [e for e in ASSISTANT_EVENTS if e not in handled]
    assert not missing, f"the page ignores {missing}"
    subscribed = set(re.findall(r'"(\w+)"', CHAT_JS.split(
        "for (const name of [")[1].split("]")[0]))
    unsubscribed = [e for e in ASSISTANT_EVENTS
                    if e not in subscribed]
    assert not unsubscribed, f"no SSE listener for {unsubscribed}"


def test_every_chat_helper_exists_and_is_served():
    called = set(re.findall(r"api\.(chat\w+)\(", CHAT_JS))
    assert called
    defined = set(re.findall(r"\n  (chat\w+):", API_JS))
    missing = called - defined
    assert not missing, f"page calls undefined helpers {missing}"
    for route in ("/sessions", "/sessions/{session_id}/messages",
                  "/sessions/{session_id}/stream",
                  "/sessions/{session_id}/stop",
                  "/sessions/{session_id}/skills",
                  '"/skills"', '"/projects"',
                  "/sessions/{session_id}/project",
                  "/sessions/{session_id}/star",
                  "/sessions/{session_id}/archive",
                  '"/memories"', "/memories/{memory_id}/retire",
                  "/artifacts/{artifact_id}",
                  "/artifacts/{artifact_id}/versions",
                  "/artifacts/{artifact_id}/export.pptx"):
        assert route in BACKEND, f"no served route {route}"
    # the shelf is served for the Skills page (no picker anywhere)
    assert '"/skills"' in BACKEND
    assert ".origin-tag" in CSS


def test_the_shell_offers_the_door():
    # one nav, not two: "New ask" starts a chat; the shelf below
    # lists them; the old Ask tab left the nav (deep links survive)
    assert 'href="#/chat/new" data-tab="chat"' in INDEX
    assert "New ask" in INDEX
    assert 'data-tab="ask"' not in INDEX
    assert 'class="chats-search"' in INDEX
    assert "chat: () => renderChat(outlet, arg)" in MAIN_JS
    assert 'page === "chat"' in MAIN_JS
    assert "api.chatSessions" in CHATS_JS
    assert "#/chat/" in CHATS_JS
    # the page itself carries no second sidebar
    assert "chat-side" not in CHAT_JS
    assert 'wanted === "new"' in CHAT_JS


def test_the_claude_shape_is_present():
    for piece in ("chat-panel", "chat-thread", "chat-chiprow",
                  "panel-version", "panel-export", "pingShelf"):
        assert piece in CHAT_JS, piece
    for cls in (".chatv2", ".chat-panel", ".chat-row",
                ".chats-search", ".chartv2", ".artifact-footer"):
        assert cls in CSS, cls
    # the artifact panel is model-invoked: it opens on an artifact
    # event in this interaction, never on reopening an old chat — a
    # card in the transcript reopens it
    assert "openArtifact(boot.artifacts" not in CHAT_JS
    assert "if (live) openArtifact(row.artifact_id);" in CHAT_JS
    assert "artifactCard(turn.extras, event, true)" in CHAT_JS
    assert "artifactCard(div.querySelector(\".chat-extras\")" in CHAT_JS
    # chips: at most three, model-authored
    assert ".slice(0, 3)" in CHAT_JS


def test_the_organization_is_present():
    # §8: starred + archive live in the shell shelf; projects stay
    # implemented (store, API) but deliberately OFF the surface
    for piece in ("data-star", "data-archive", "shelf-head"):
        assert piece in CHATS_JS, piece
    assert "project-row" not in CHATS_JS
    assert "chat-project" not in CHAT_JS
    assert '"/projects"' in BACKEND          # the door stays served
    # the chat page: memory panel, handoff banner, deck export
    for piece in ("chat-memory-btn", "chatRetireMemory",
                  "handoff-note", "Where you left off",
                  "chatPptxUrl"):
        assert piece in CHAT_JS, piece
    for cls in (".memory-row", ".handoff-note", ".row-btn"):
        assert cls in CSS, cls
    # memory is disclosed and retirable, never silently gone — and a
    # save is disclosed inline with an undo the moment it happens
    assert "retire" in BACKEND and "retire_memory" in BACKEND
    assert "memoryNote" in CHAT_JS and "Remembered:" in CHAT_JS
    assert ".memory-note" in CSS


def test_thinking_is_alive_and_skills_are_browsable():
    # one live line: the model's own thought summaries and a friendly
    # verb per call, replaced in place, collapsed into "Worked for …"
    # when the turn lands — verbs deduplicated, no tool names
    for piece in ("thinking-line", "think-orb", "showThinking",
                  "doneThinking", "friendly", "VERBS", "PAST",
                  "Worked for", 'case "thinking"', 'case "tool_call"',
                  "lastLine", "new Set(turn.verbs)"):
        assert piece in CHAT_JS, piece
    for cls in (".thinking-line", "@keyframes think-orb",
                "@keyframes think-shimmer", "prefers-reduced-motion"):
        assert cls in CSS, cls
    # the hidden attribute always wins — the bug the real transcript
    # exposed was display:flex outranking [hidden]
    assert "[hidden] { display: none !important; }" in CSS
    # the thinking line never returns after the turn landed
    assert "if (turn.done) return;" in CHAT_JS
    # the live line has a heartbeat: seconds tick, each model call
    # restarts the clock, prose or the end stops it
    for piece in ("function pulse", "stopPulse", "Still ",
                  'if (event.kind === "call") pulse(turn, "Thinking…", event.ts)',
                  "clearInterval(turn.tick)"):
        assert piece in CHAT_JS, piece
    # no harness words in the user's language
    for gone in ("what the model saw", "saw-toggle", "saw-panel",
                 "strict JSON", "Worked through"):
        assert gone not in CHAT_JS, gone
    # the depth dial rides on every send
    assert "chat-depth" in CHAT_JS and "depth" in BACKEND
    # a turn belongs to the server: coming back mid-turn reattaches
    # from the turn's first event, and the shelf marks a working chat
    assert '"turn_after": window["after"]' in BACKEND
    assert "state.seq = boot.turn_after;" in CHAT_JS
    assert "setRunning(true);\n  }" in CHAT_JS
    assert "chat-when working" in CHATS_JS
    assert ".chat-when.working" in CSS
    # no picker anywhere: the agent loads packs itself; people browse
    # the shelf on the Skills tab
    assert "chat-skills-btn" not in CHAT_JS
    assert "chatSetSkills" not in CHAT_JS
    assert 'href="#/skills" data-tab="skills"' in INDEX
    assert "skills: renderSkills" in MAIN_JS
    skills_js = (FRONTEND / "js" / "pages" / "skills.js").read_text(
        encoding="utf-8")
    assert "api.chatSkills" in skills_js
    assert "read the doctrine" in skills_js
    assert "origin-tag" in skills_js


def test_dashboards_and_diagrams_render():
    for piece in ("kpiTile", "diagramSVG", "dash-grid",
                  "filter-opt", "mermaid-src",
                  "bindDashboardFilters", "tileFooter"):
        assert piece in CHAT_JS, piece
    for cls in (".dash-grid", ".kpi-tile", ".tile-footer",
                ".diagramv2", ".filter-opt"):
        assert cls in CSS, cls
    # a filter pick goes through the conversation, never a hidden
    # client-side query — the binder composes a whatif message
    assert "whatif" in CHAT_JS.split("bindDashboardFilters")[1][:600]
    # dashboards export as an HTML bundle; diagrams as SVG or .mmd
    assert '".html"' in CHAT_JS.replace("`${slug}.html`",
                                        '".html"')
    assert ".mmd" in CHAT_JS


def test_governance_is_visible_not_just_enforced():
    # status chips, meridian line, and the watermark all render
    assert "status-chip" in CHAT_JS
    assert "meridian_line" in CHAT_JS
    assert "watermark" in CHAT_JS
    for status in ("s-certified", "s-pending", "s-composed",
                   "s-exploratory"):
        assert status in CSS, status
    # exports carry the provenance footer
    assert "provenanceLine" in CHAT_JS
