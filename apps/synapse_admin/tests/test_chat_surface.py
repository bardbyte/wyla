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
FRONTEND = REPO_ROOT / "apps" / "synapse_admin" / "frontend"
SILO = REPO_ROOT / "synapse-agentic-harness-system"

CHAT_JS = (FRONTEND / "js" / "pages" / "chat.js").read_text(
    encoding="utf-8")
CHATS_JS = (FRONTEND / "js" / "chats.js").read_text(encoding="utf-8")
API_JS = (FRONTEND / "js" / "api.js").read_text(encoding="utf-8")
MAIN_JS = (FRONTEND / "js" / "main.js").read_text(encoding="utf-8")
INDEX = (FRONTEND / "index.html").read_text(encoding="utf-8")
CSS = (FRONTEND / "styles" / "app.css").read_text(encoding="utf-8")
BACKEND = (REPO_ROOT / "apps" / "synapse_admin" / "backend"
           / "chat.py").read_text(encoding="utf-8")
SECOND = REPO_ROOT / "apps" / "synapse" / "frontend"
sys.path.insert(0, str(REPO_ROOT / "apps" / "synapse_admin" / "tests"))
from test_synapse_surface import client, compiled  # noqa: E402,F401


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


def test_the_assistant_shape_is_present():
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
                  "Thought", 'case "thinking"', 'case "tool_call"',
                  "lastLine", "new Set(turn.verbs)",
                  # the thinking block: thought segments interleaved
                  # with steps, folded to "Thought for" on the answer,
                  # reopened by new work, replayed from the trace
                  "think-seg", "thoughtSegment", "settleBlock",
                  "openBlock", "traceBlock", "payload?.trace",
                  "if (!turn.settled) settleBlock(turn)",
                  # a step's input (the SQL, the code) on a click
                  "attachInput", "step-input", "event.input", "t.input"):
        assert piece in CHAT_JS, piece
    assert ".think-seg" in CSS and ".step-input" in CSS
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
                  'pulse(turn, "Thinking…", event.ts)',
                  "clearInterval(turn.tick)"):
        assert piece in CHAT_JS, piece
    # no harness words in the user's language
    for gone in ("what the model saw", "saw-toggle", "saw-panel",
                 "strict JSON", "Worked through", "working…"):
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


def test_the_ask_starts_like_a_chat_assistant_and_hands_queries_over():
    # the empty state: a greeting and the composer, nothing else; the
    # first message turns it into the conversation with its title,
    # a Share door, the composer docked, the disclaimer under it
    for piece in ("chat-hero", "chat-greet", "how are things",
                  "Type / for skills", "Write a message…",
                  'classList.toggle("empty"', "setEmpty(false)",
                  "chat-title", "chat-title-input", "api.chatRename",
                  "chat-share", "Link copied", "chat-plus-pop",
                  "Browse skills", "chat-mode", "Autopilot",
                  "synapse-chat-mode", "chat-model", "boot.model",
                  "boot.user_name", "chat-slash", "paintSlash",
                  "pickSlash", "chat-foot", "can make mistakes"):
        assert piece in CHAT_JS, piece
    for cls in (".chat-hero", ".chatv2.empty", ".chat-box",
                ".chat-modes", ".chat-mode.on", ".chat-slash",
                ".chat-title-btn", ".chat-foot", ".proposal-card",
                ".proposal-sql", ".proposal-actions"):
        assert cls in CSS, cls
    # no dead controls: every composer button reaches a real door
    for gone in ("mic", "Cowork"):
        assert gone not in CHAT_JS, gone
    # the handover: the query first, the rows on a tap, no model call
    for piece in ('case "proposal"', "proposalCard", "Run query",
                  "Run + build dashboard", "Edit SQL", "api.chatRun",
                  "payload?.proposal", "propose_sql",
                  "Writing the query for you to run",
                  "state.mode", "dashboard, depth"):
        assert piece in CHAT_JS, piece
    assert "chatRun" in API_JS and "{ text, depth, mode, model }" in API_JS
    for piece in ("/run\"", "run_proposal", "RunProposal",
                  "mode=req.mode", "dashboard", '"user_name"',
                  '"choice": runtime.choice_of(session)'):
        assert piece in BACKEND, piece
    # the mode and the slash command are the runtime's, not the page's
    runtime_py = (SILO / "sahs" / "assistant" / "runtime.py").read_text(
        encoding="utf-8")
    for piece in ("def run_proposal", "def slash_skill", "def mode_for",
                  "model_label", "run_proposal_turn"):
        assert piece in runtime_py, piece


def test_the_first_picture_needs_no_model():
    # the run offers "Chart these rows" as an action chip: the page
    # calls the chart step, never the model; a plain chip still sends
    for piece in ("api.chatChart", 'action === "chart"', "Drawing the rows",
                  "drew the chart", "typeof c === \"string\""):
        assert piece in CHAT_JS, piece
    assert "chatChart" in API_JS
    for piece in ('/chart"', "ChartRows", "chart_rows"):
        assert piece in BACKEND, piece
    # the card says when Run would be refused for cost
    assert "proposal.over_ceiling" in CHAT_JS
    assert "ceiling for live" in CHAT_JS


def test_the_artifact_drawer_and_the_report():
    # the panel is a drawer: it slides in from the right edge and
    # moves the chat to the middle; opening a card always brings the
    # artifact into view; the masthead carries no tokens or build id
    for piece in ('classList.add("open")', "panel-open", "scrollTop = 0",
                  'id="chat-meter" hidden', 'id="chat-build" hidden',
                  "tableReport", "animateNumbers", "sparkline",
                  "report-strip", "tablev3", "Show all",
                  'class="line"', 'class="chart-bar"', "--i:"):
        assert piece in CHAT_JS, piece
    for cls in (".chat-panel.open", ".chatv2.panel-open .chat-main",
                "--panel-w", ".report-strip", ".stat-spark", ".tablev3",
                "@keyframes draw", "@keyframes grow", "@keyframes tile-in",
                ".chartv2 .line", "prefers-reduced-motion"):
        assert cls in CSS, cls


def test_metric_status_reads_published_on_every_chip():
    """The product says "published" where the graph says certified:
    one label map in ui.js, used by the chat's status chip and the
    provenance footer and by the metrics, table and metric pages. The
    CSS class, the filter key and the API word stay the graph's value."""
    ui_js = (FRONTEND / "js" / "ui.js").read_text(encoding="utf-8")
    assert 'certified: "published"' in ui_js
    assert "statusLabel(prov.status)" in CHAT_JS
    assert "esc(prov.status)}</span>" not in CHAT_JS
    for page in ("semantics", "table", "metric"):
        text = (FRONTEND / "js" / "pages" / f"{page}.js").read_text(
            encoding="utf-8")
        assert "statusLabel" in text, page
        assert 'certified: "certified"' not in text, page
    assert ".status-chip.s-certified" in CSS



def test_the_composer_switches_models_and_explains_every_dial():
    """The model switch and the "?" beside the dials, on the page: the
    select that is the model label, the popover with its three groups,
    the send that carries the chat's plane, and the refusal wording.
    The catalog and the switch itself are pinned against the running
    app in test_synapse_admin_app."""
    for piece in ('id="chat-model"', 'id="chat-help"', "chat-help-pop",
                  "api.chatDials()", "api.chatSetModel(",
                  "state.mode, state.plane", "model not switched",
                  "help-row", "help-group", "not configured",
                  "Thinking effort <span>", "Model <span>",
                  "from the next message on"):
        assert piece in CHAT_JS, piece
    for cls in (".chat-plane", ".chat-help", ".chat-help-pop",
                ".help-group + .help-group", ".help-row", ".help-fact"):
        assert cls in CSS, cls
    assert "chatDials" in API_JS and "chatSetModel" in API_JS
    for piece in ('"/dials"', "SessionModel", "set_session_model",
                  "model=req.model", '"plane": plane'):
        assert piece in BACKEND, piece


def test_a_compound_ask_draws_a_task_board():
    """Multi-task turns (docs/multi-task-turns.md): the plan is a board
    under the person's message, one row per task with its status and
    cost, the task's own events grouped under its row (the running one
    open, the finished ones folded), and the transcript replays the
    same board from the stored plan. Every new event has an arm and a
    listener (pinned above with ASSISTANT_EVENTS)."""
    for piece in ('case "plan_made"', 'case "task_started"',
                  'case "task_done"', "task-board", "task-row",
                  "boardFor", "taskTurnFor", "taskStatus", "homeOf",
                  "Split into", "side by side", "Sorting out the asks",
                  "Working through", 'task.el.open = status === "running"',
                  "replayTaskMessage", "payload?.task", "payload?.plan",
                  "if (turn.task) break;", "event.planning",
                  'split(".")[0]'):
        assert piece in CHAT_JS, piece
    for status in ("running", "done", "partial", "failed", "stopped"):
        assert f".task-row.{status}" in CSS, status
    for cls in (".task-board", ".task-board-head", ".task-status",
                ".task-cost", ".task-body", ".task-note"):
        assert cls in CSS, cls
    # the harness side: the planner gate, the runner, the report, the
    # tag on every sub-turn record, no new route (the stream carries it)
    runtime_py = (SILO / "sahs" / "assistant" / "runtime.py").read_text(
        encoding="utf-8")
    loop_py = (SILO / "sahs" / "assistant" / "loop.py").read_text(
        encoding="utf-8")
    assert "should_plan(" in runtime_py and "def _task_turn" in runtime_py
    for piece in ("def run_task_turn", "class SubTurn", "class TaskRecord",
                  'REPORT_TITLE = "What was done"', "task=self._task or None",
                  "ThreadPoolExecutor"):
        assert piece in loop_py, piece
    assert (SILO / "sahs" / "assistant" / "planner.py").exists()
    assert "task" not in BACKEND.split("class NewMessage")[0]


def test_the_stop_is_an_icon_button_in_the_send_buttons_place():
    """Both surfaces: while a turn runs the send button gives way to a
    square stop glyph of the same size; pressed, it locks and the live
    line says Stopping… until the turn's end (turn_done, status
    stopped) restores the composer through the stream. The two pills
    are one-line controls: the value truncates, the chevron never
    drops below the label."""
    for root in (FRONTEND, SECOND):
        js = (root / "js" / "pages" / "chat.js").read_text(encoding="utf-8")
        css = (root / "styles" / "app.css").read_text(encoding="utf-8")
        assert '<button class="btn" id="chat-stop" hidden>stop</button>' not in js
        for piece in ('class="btn primary chat-send chat-stop" id="chat-stop"',
                      'aria-label="Stop" title="Stop"', 'class="stop-glyph"',
                      "sendBtn.hidden = running;", "stopBtn.hidden = !running;",
                      "async function stop()", "if (!state.running || state.stopping) return;",
                      "stopBtn.disabled = true;", 'stopBtn.classList.add("stopping");',
                      'pulse(turn, "Stopping…")', "await api.chatStop(state.session.id);",
                      "state.liveTurn = turn;", 'el("chat-stop").addEventListener("click", stop);',
                      'e.key === "Escape" && state.running) stop();',
                      'stopBtn.classList.remove("stopping");',
                      'event.status === "stopped"'):
            assert piece in js, (root.name, piece)
        for cls in (".chat-stop {", ".chat-stop .stop-glyph", ".chat-stop.stopping",
                    "flex-flow: row nowrap", ".chat-pill .pill-label {",
                    "text-overflow: ellipsis", ".chat-pill .chev { flex: none;"):
            assert cls in css, (root.name, cls)
        # the same size as the send button: the stop wears its class
        assert ".chat-send { width: 32px; height: 32px;" in css


def test_the_stop_route_ends_a_running_turn(client, compiled, tmp_path):
    """POST /api/chat/sessions/{id}/stop on a running turn: the abort
    flag reaches the model client mid-stream, the worker ends, the
    session reads as not running, the turn lands as stopped with the
    partial prose kept, and a second stop says nothing is running."""
    import threading
    import time
    from apps.synapse_admin.backend import chat as chat_module
    sys.path.insert(0, str(SILO))
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import ScriptedAgent
    streaming = threading.Event()

    class Slow(ScriptedAgent):
        def converse(self, contents, *, should_stop=None, **kw):
            usage = {"prompt_tokens": 100, "output_tokens": 20,
                     "thought_tokens": 5, "cached_tokens": 0}
            yield {"kind": "text", "delta": "Working on it: "}
            streaming.set()
            deadline = time.time() + 20
            while time.time() < deadline and not (should_stop and should_stop()):
                time.sleep(0.02)                  # the stream, waiting on Vertex
            yield {"kind": "done", "finish": "STOPPED", "usage": usage,
                   "parts": [{"text": "Working on it: "}]}

    runtime = AssistantRuntime(
        builds_root=compiled["builds"], graph_root=tmp_path / "graph",
        store_path=tmp_path / "chat.sqlite3",
        model_factory=lambda budget: Slow())
    previous = chat_module._RUNTIME
    chat_module._RUNTIME = runtime
    try:
        sid = client.post("/api/chat/sessions").json()["session"]["id"]
        accepted = client.post(f"/api/chat/sessions/{sid}/messages",
                               json={"text": "a long one"}).json()
        assert accepted["available"], accepted
        assert streaming.wait(10), "the turn never started streaming"
        assert client.get(f"/api/chat/sessions/{sid}").json()["running"] is True
        stopped = client.post(f"/api/chat/sessions/{sid}/stop").json()
        assert stopped["available"] and stopped["stopped"]
        assert stopped["turn_id"] == accepted["turn_id"]
        assert runtime.wait(sid, 10), "the worker did not end on stop"
        detail = client.get(f"/api/chat/sessions/{sid}").json()
        assert detail["running"] is False and detail["turn_id"] == ""
        events = runtime.runtime(sid).bus.since(0)
        done = [e for e in events if e["ev"] == "turn_done"][-1]
        assert done["status"] == "stopped"
        last = detail["messages"][-1]
        assert last["role"] == "assistant"
        assert last["text"].startswith("Working on it: ")
        assert "you stopped me" in last["text"]
        again = client.post(f"/api/chat/sessions/{sid}/stop").json()
        assert again["stopped"] is False and "no turn is running" in again["reason"]
        # the composer is free again: the next message is accepted,
        # and stops the same way
        streaming.clear()
        assert client.post(f"/api/chat/sessions/{sid}/messages",
                           json={"text": "again"}).json()["available"]
        assert streaming.wait(10)
        assert client.post(f"/api/chat/sessions/{sid}/stop").json()["stopped"]
        assert runtime.wait(sid, 10)
    finally:
        chat_module._RUNTIME = previous


def test_the_pane_scrolls_the_thinking_folds_and_the_usage_shows():
    """The owner's laptop test: the whole main pane is the scroll
    surface (#chat-scroll — the thread scrolls under the sticky
    masthead and past the chips, which sit in the flow; the composer
    docks with position: sticky; the wheel works in the gutters because
    the outlet loses its width cap on this page), the "Latest" pill
    lives outside the scroller and sits above the composer; the
    thinking block is closed by default — one compact "Radix is
    thinking… 12s" line, a summary element (keyboard), the choice kept
    per browser under synapse-thinking-open, the answer folds it to
    "Thought for 12s" unless it was kept open, a replayed turn renders
    the folded summary; the usage shows live beside the line (from
    budget_tick's turn split), as a footer under the answer (from
    turn_done, replayed from payload.usage), on the sidebar rows and on
    the People page's Tokens column with the breakdown on hover."""
    users_js = (FRONTEND / "js" / "pages" / "users.js").read_text(encoding="utf-8")
    admin_py = (REPO_ROOT / "apps" / "synapse_admin" / "backend" / "admin.py").read_text(
        encoding="utf-8")
    for piece in ('id="chat-scroll"', 'const scroller = el("chat-scroll")',
                  "scroller.scrollTop = scroller.scrollHeight",
                  'scroller.addEventListener("scroll"', "composer.offsetHeight",
                  "let stuck = true", "stuck = atBottom()", "scroll(true)",
                  # the thinking block: closed, remembered, one line
                  '<details class="tool-activity" hidden>', "synapse-thinking-open",
                  "thinkOpen()", "rememberThinking", "Radix is thinking…",
                  "Radix is still", 'pulse(turn, "Thinking…", event.ts)',
                  "<summary title=", "activityHTML()", "activityParts(",
                  "turn.activity.open = thinkOpen()",
                  # the usage: live, the footer, replayed
                  "think-usage", "liveUsage(turn.task ? turn.parent : turn, event)",
                  "usageFooter(turn.el, usageOf(event))", "payload.usage",
                  "usageFooter(turn.el, usageOf(p.usage))",
                  "turn-usage", "model call", "turn_tokens_in", "turn_cost_usd",
                  "SYNAPSE_COST_IN"):
        assert piece in CHAT_JS, piece
    assert "hidden open>" not in CHAT_JS
    assert CHAT_JS.count('<details class="tool-activity" hidden>') == 1   # one template
    # the pill is a child of the pane, not of the scroller
    assert 'id="chat-jump"' in CHAT_JS.split("</button>\n      </div>")[0]
    assert CHAT_JS.index('id="chat-scroll"') < CHAT_JS.index('id="chat-jump"')
    for cls in (".chat-scroll {", ".chat-scroll .chat-composer {", "position: sticky",
                ".chat-scroll .chat-masthead {", ".outlet.chatv2page {",
                ".think-usage {", ".turn-usage {", ".chat-row .chat-usage {",
                ".tool-activity summary { flex-wrap: wrap;", ".users-page td.tokens",
                "overscroll-behavior: contain"):
        assert cls in CSS, cls
    for piece in ("usageLine", "usageTitle", 'class="chat-usage"', "tokens · ", "turn"):
        assert piece in CHATS_JS, piece
    # the People page: a Tokens column, the breakdown on hover
    for piece in (">tokens</th>", 'class="tokens"', "usageTitle(u.usage)",
                  "tokensCell(u.usage)", "model calls", "no chat turn yet"):
        assert piece in users_js, piece
    for piece in ("usage_by_owner", "EMPTY_USAGE", '"usage":', "usage_totals",
                  "spanner_is_enabled"):
        assert piece in admin_py, piece
    # the harness side: the budget's turn split rides every tick and the
    # turn_done, the runtime settles it into every store
    budget_py = (SILO / "sahs" / "ask" / "budget.py").read_text(encoding="utf-8")
    runtime_py = (SILO / "sahs" / "assistant" / "runtime.py").read_text(encoding="utf-8")
    for piece in ('"turn_tokens_in"', '"turn_tokens_out"', '"turn_calls"', '"turn_cost_usd"'):
        assert piece in budget_py, piece
    for piece in ("def _settle_usage", "store.add_usage(", "store.set_message_usage(",
                  'record.get("task")'):
        assert piece in runtime_py, piece
    for store in ("store.py", "spanner_store.py"):
        text = (SILO / "sahs" / "assistant" / store).read_text(encoding="utf-8")
        assert "def add_usage" in text and "def set_message_usage" in text, store
    assert (SILO / "db" / "spanner" / "009_usage.sql").exists()


def test_a_turns_usage_lands_on_the_message_the_row_and_the_routes(client, compiled, tmp_path):
    """A turn through the app's local runtime with a scripted model that
    charges the budget the way the Vertex agent does: turn_done carries
    the turn's own split, the final assistant message's payload carries
    ``usage`` (what the footer replays), the session row adds it and a
    second turn adds again, and GET /api/chat/sessions, /sessions/{id}
    and /search carry the totals; under SAHS_STORE=local the People
    route gives the one developer their totals from this store."""
    from apps.synapse_admin.backend import chat as chat_module
    sys.path.insert(0, str(SILO))
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import ScriptedAgent

    class Charging(ScriptedAgent):
        def __init__(self, budget):
            super().__init__(steps=[[{"text": "Churn is a rate, not a count."}]])
            self.budget = budget

        def converse(self, contents, **kw):
            self.budget.charge(tokens_in=7900, tokens_out=310)
            yield from super().converse(contents, **kw)

    runtime = AssistantRuntime(
        builds_root=compiled["builds"], graph_root=tmp_path / "graph",
        store_path=tmp_path / "chat.sqlite3",
        model_factory=lambda budget: Charging(budget))
    previous = chat_module._RUNTIME
    chat_module._RUNTIME = runtime
    try:
        sid = client.post("/api/chat/sessions").json()["session"]["id"]
        turns = []
        for text in ("what is churn", "and once more"):
            accepted = client.post(f"/api/chat/sessions/{sid}/messages",
                                   json={"text": text}).json()
            assert accepted["available"], accepted
            assert runtime.wait(sid, 60), "the turn did not end"
            turns.append(accepted["turn_id"])
        events = runtime.runtime(sid).bus.since(0)
        done = [e for e in events if e["ev"] == "turn_done" and not e.get("task")]
        assert [e["turn_id"] for e in done] == turns
        for e in done:
            assert e["turn_tokens_in"] > 0 and e["turn_tokens_in"] % 7900 == 0
            assert e["turn_tokens"] == e["turn_tokens_in"] + e["turn_tokens_out"]
            # no rate configured: no cost on the record (the bus drops
            # a None field), so the page draws tokens alone
            assert e["model_calls"] >= 1 and e.get("turn_cost_usd") is None
        ticks = [e for e in events if e["ev"] == "budget_tick"]
        assert ticks and all("turn_tokens_in" in t and "turn_tokens_out" in t
                             and "turn_calls" in t for t in ticks)
        detail = client.get(f"/api/chat/sessions/{sid}").json()
        last = detail["messages"][-1]
        assert last["role"] == "assistant" and last["turn_id"] == turns[-1]
        usage = last["payload"]["usage"]
        assert usage["tokens_in"] == done[1]["turn_tokens_in"]
        assert usage["tokens_out"] == done[1]["turn_tokens_out"]
        assert usage["calls"] == done[1]["model_calls"]
        assert usage["tokens"] == usage["tokens_in"] + usage["tokens_out"]
        assert usage["elapsed_ms"] >= 1 and usage["cost_usd"] is None
        assert last["payload"]["chips"] is not None            # the rest kept
        session = detail["session"]
        assert session["turns"] == 2
        assert session["tokens_in"] == sum(e["turn_tokens_in"] for e in done)
        assert session["tokens_out"] == sum(e["turn_tokens_out"] for e in done)
        assert session["model_calls"] == sum(e["model_calls"] for e in done)
        assert session["tokens"] == session["tokens_in"] + session["tokens_out"]
        assert session["elapsed_ms"] >= 2
        listed = next(s for s in client.get("/api/chat/sessions").json()["sessions"]
                      if s["id"] == sid)
        assert listed["tokens"] == session["tokens"] and listed["turns"] == 2
        found = next(s for s in client.get("/api/chat/search?q=churn").json()["sessions"]
                     if s["id"] == sid)
        assert found["tokens"] == session["tokens"] and found["turns"] == 2
        assert found["tokens_in"] == session["tokens_in"]
        # the People page under SAHS_STORE=local: the one developer, the
        # totals of this store (this chat is its only one)
        people = client.get("/api/admin/users").json()
        assert people["available"] and len(people["users"]) == 1
        me = people["users"][0]
        assert me["user_id"] == "local" and "admin" in me["roles"]
        assert me["usage"]["tokens"] == session["tokens"]
        assert me["usage"]["turns"] == 2 and me["usage"]["chats"] == 1
    finally:
        chat_module._RUNTIME = previous
