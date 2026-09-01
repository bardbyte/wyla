"""Synapse v2 §10 — the chat surface is wired to the assistant loop.

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
                  "/artifacts/{artifact_id}",
                  "/artifacts/{artifact_id}/versions"):
        assert route in BACKEND, f"no served route {route}"


def test_the_shell_offers_the_door():
    assert 'href="#/chat" data-tab="chat"' in INDEX
    assert "chat: () => renderChat(outlet, arg)" in MAIN_JS
    assert 'page === "chat"' in MAIN_JS


def test_the_claude_shape_is_present():
    for piece in ("chat-side", "chat-recents", "chat-panel",
                  "chat-thread", "chat-chiprow", "panel-version",
                  "panel-export", "chat-new", "chat-search"):
        assert piece in CHAT_JS, piece
    for cls in (".chatv2", ".chat-panel", ".recent-row",
                ".chartv2", ".artifact-footer"):
        assert cls in CSS, cls


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
