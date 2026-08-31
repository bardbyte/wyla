"""Ask (E18) Stage B: the chat surface is wired to the loop.

The frontend has no build step, so nothing catches a rename: a page
that calls ``api.askSend`` after somebody renames it fails silently in
the browser and green in CI. These tests are the missing compiler.

They pin the three seams the browser walk cannot run in CI:
  * the shell offers the door and no longer promises a shipped feature;
  * every event the loop can emit is HANDLED by the page (an event the
    UI ignores is information the analyst never sees);
  * every ``api.ask*`` the page calls exists, and points at a route the
    app actually serves.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONTEND = REPO_ROOT / "apps" / "lumi" / "frontend"
SILO = REPO_ROOT / "synapse-agentic-harness-system"

ASK_JS = (FRONTEND / "js" / "pages" / "ask.js").read_text(encoding="utf-8")
API_JS = (FRONTEND / "js" / "api.js").read_text(encoding="utf-8")
MAIN_JS = (FRONTEND / "js" / "main.js").read_text(encoding="utf-8")
CHATS_JS = (FRONTEND / "js" / "chats.js").read_text(encoding="utf-8")
INDEX = (FRONTEND / "index.html").read_text(encoding="utf-8")
CSS = (FRONTEND / "styles" / "app.css").read_text(encoding="utf-8")

# Stage D brings the exploratory lane; until then the page has nothing
# truthful to render for a notebook artifact, and says so here rather
# than pretending in a switch arm.
NOT_YET = {"notebook_artifact"}


def test_the_shell_offers_ask_and_stops_promising_it():
    assert 'href="#/ask" data-tab="ask"' in INDEX, "no Ask door in the nav"
    assert "E16" not in INDEX, (
        "the chats shelf still promises Ask as future work, but it shipped")
    assert 'class="chats-body"' in INDEX, "the shelf has nowhere to render"
    # the router knows the route AND the deep link into one session
    assert "ask: () => renderAsk(outlet, arg)" in MAIN_JS
    assert 'page === "ask"' in MAIN_JS, "renderAsk never receives its arg"
    assert "#/ask/" in CHATS_JS, "the shelf cannot link to a session"


def test_every_event_the_loop_emits_reaches_the_page():
    """The pinned family is the contract. An event with no arm in the
    page is a step the analyst is never told about."""
    import sys
    sys.path.insert(0, str(SILO))
    from sahs.ask.events import EVENTS

    handled = set(re.findall(r'case "(\w+)":', ASK_JS))
    handled |= set(re.findall(r'"(\w+)"', ASK_JS.split(
        "for (const name of [")[1].split("]")[0]))
    missing = [e for e in EVENTS if e not in handled and e not in NOT_YET]
    assert not missing, f"the page ignores {missing}"

    # and it subscribes to each one by name: EventSource.onmessage only
    # fires for UNNAMED events, so a missing listener is a silent drop
    subscribed = set(re.findall(r'"(\w+)"', ASK_JS.split(
        "for (const name of [")[1].split("]")[0]))
    unsubscribed = [e for e in EVENTS
                    if e not in subscribed and e not in NOT_YET]
    assert not unsubscribed, f"no SSE listener for {unsubscribed}"


def test_every_api_helper_the_page_calls_exists():
    called = set(re.findall(r"api\.(ask\w+)\(", ASK_JS + CHATS_JS))
    assert called, "the page reaches the loop through no helper at all"
    defined = set(re.findall(r"\n  (ask\w+):", API_JS))
    missing = sorted(called - defined)
    assert not missing, f"api.js is missing {missing}"


@pytest.mark.parametrize("route", [
    "/api/sessions",
    "/api/sessions/{session_id}",
    "/api/sessions/{session_id}/messages",
    "/api/sessions/{session_id}/stop",
    "/api/sessions/{session_id}/feedback",
    "/api/sessions/{session_id}/stream",
])
def test_the_helpers_point_at_routes_the_app_serves(route):
    # read the served contract from the OpenAPI schema, not app.routes:
    # this FastAPI keeps included routers wrapped, so walking .routes
    # would report a green app with no API on it
    from apps.lumi.backend.app import create_app
    served = set(create_app().openapi()["paths"])
    assert route in served, f"{route} is called by api.js but not served"


def test_the_page_never_holds_a_key_or_an_endpoint():
    """Pinned: ALL model calls are server-side. The surface is a pure
    consumer of the stream; if a key or a vendor host ever appears in
    the frontend, this fails before it can ship."""
    for name, source in (("ask.js", ASK_JS), ("api.js", API_JS),
                         ("chats.js", CHATS_JS)):
        low = source.lower()
        for banned in ("aiplatform.googleapis", "generativelanguage",
                       "vertex_project", "sa_key", "api_key", "bearer "):
            assert banned not in low, f"{name} carries {banned!r}"


def test_the_classes_the_page_emits_are_styled():
    """A zero-build frontend ships whatever it says: a class with no
    rule renders as unstyled text and nobody finds out until a user
    does."""
    emitted = {"ask-thread", "ask-user", "ask-turn", "ask-prose",
               "ask-extras", "ask-composer", "ask-actions", "theater",
               "theater-steps", "theater-step", "theater-title",
               "chips-card", "chips-question", "chips-row", "chip-choice",
               "answer-card", "answer-head", "answer-detail",
               "answer-detail-body", "verdict-list", "verdict-row",
               "meridian", "limits", "error-card", "ask-note",
               "result-wrap", "chat-row", "chat-title", "chats-body"}
    unstyled = sorted(c for c in emitted if f".{c}" not in CSS)
    assert not unstyled, f"no CSS rule for {unstyled}"


def test_served_prose_on_this_surface_is_de_dashed():
    """Em dashes never reach the page. Evidence, limits, criteria and
    error messages are served PROSE and go through prose(); the SQL and
    the ids are data artifacts and stay verbatim."""
    assert 'import { esc, prose } from "../ui.js"' in ASK_JS
    for served in ("clarify.question", "option.why", "option.evidence",
                   "payload.meridian_line", "c.text", "c.evidence",
                   "event.message"):
        assert f"prose({served})" in ASK_JS, f"{served} is not de-dashed"
    for verbatim in ("payload.sql", "payload.build_id", "payload.grain"):
        assert f"esc({verbatim})" in ASK_JS, (
            f"{verbatim} is a data artifact and must render verbatim")
