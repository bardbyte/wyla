# UI fixes: composer pills, a stop that stops, Radix on both surfaces, model lines for the people who pick

Branch `worktree-agent-aacde09ee370a32de` (branched from `claude/production-ready`),
worktree `/home/user/wyla/.claude/worktrees/agent-aacde09ee370a32de`.
Work commit `762ac2b`; this report is the commit after it. Nothing pushed, no PR opened.

## What the owner reported, and what changed

1. **Composer pills wrapped** (chevron below the label). Both pills are now single-line
   inline-flex controls; the value truncates with an ellipsis. The Thinking-effort pill names
   the stop that is on ("Thinking · Deep") from the session's remembered depth at load and the
   moment a stop is chosen; the slider panel marks the current stop (a `role=radio` row of stops
   under the rail, `aria-checked` + `.on`); a panel never leaves the window (opens upward when
   there is no room below, downward when none above, scrolls inside otherwise). The Model pill
   shows the model's short label and its plane ("Gemini 3.7 Flash · Gateway"). The hidden
   `chat-model` / `chat-depth` selects the send path reads are unchanged.
2. **Stop.** The text "stop" button is an icon button (square glyph, `aria-label="Stop"`,
   `title="Stop"`, the send button's classes and size) shown in place of the send button while a
   turn runs. On click: it locks (`disabled`, `.stopping`), the live line pulses "Stopping…",
   `api.chatStop` is called, and the stream's `turn_done` (status `stopped`) restores the
   composer through `setRunning(false)`. Escape uses the same path. Stop now stops in the
   harness: the abort flag rides every model call as `should_stop`, the loop checks it before
   every chunk it would show, once more when the call returns, and before every tool.
3. **Radix on the admin console too.** Every line of chat copy on `apps/synapse_admin` says
   Radix; the backend strings that reach the chat on both surfaces say Radix (memory document
   heading, mode blurbs, the built-in packs' author label, the "no query proposed yet" refusal).
   Wordmark, `MERIDIAN_*`, `/api/meridian/*`, module names: unchanged. Surface pins flipped; the
   skill's naming pin updated.
4. **Scope addition (model and depth copy).** The line under each model in the picker is one
   plain sentence on when to pick it; the engineer's facts moved to a new `facts` field shown on
   the hover title and in the "?" explainer. "Not sure? Keep the default." heads the picker. The
   five thinking-effort stops say when to use them with cost and speed in plain words.

## File-by-file

### Frontend (both surfaces)

- `apps/synapse_admin/frontend/js/knobs.js` and `apps/synapse/frontend/js/knobs.js` (identical,
  the surface test pins that):
  - `place(pop, below)`: measures the panel after it opens, flips `.below` when the preferred
    side has no room, caps `max-height` to the room on the side it opens to (min 160px).
  - Depth knob: the pill's `.pill-label` reads `Thinking · <stop>` on every `paint()` (load,
    `setDepths`, a pick, and any `change` on the select); a `.knob-stops` radiogroup under the
    rail with one `role=radio` button per stop, `aria-checked` + `.on` on the current one, click
    to pick; the pill title carries the stop's blurb.
  - Model picker: the pill reads `<label> · <plane_name>`; `Not sure? Keep the default.` hint at
    the top of the list; each row's small line is `fit` (one plain sentence), with
    `Where a new chat starts.` appended on the default; the hover title is `means` + `facts`
    for an available model and the greyed reason for an unavailable one (unchanged).
- `apps/synapse_admin/frontend/js/pages/chat.js` and `apps/synapse/frontend/js/pages/chat.js`:
  - Stop markup: `<button class="btn primary chat-send chat-stop" id="chat-stop" type="button"
    hidden aria-label="Stop" title="Stop"><span class="stop-glyph" aria-hidden="true"></span></button>`.
  - `setRunning(running)`: hides the send button while running (the stop takes its place);
    on the way back it clears `state.stopping`, re-enables the stop and drops `.stopping`.
  - `async function stop()`: guards on `state.running && !state.stopping`, locks the button,
    `pulse(turn, "Stopping…")` on `state.liveTurn` (set in the `turn_started` arm), awaits
    `api.chatStop`. Click and Escape both call it.
  - "?" explainer: each model row shows `fit` + `means`, with the availability and `facts` as
    the small fact line.
  - Admin only: every Synapse-as-assistant string → Radix (mode radiogroup label and titles,
    both "How deeply … thinks" titles, the disclaimer strip, the explainer head, the memory
    empty state, one comment).
- `apps/synapse_admin/frontend/js/pages/skills.js`: "Doctrine packs Radix applies on its own"
  (and the header comment).
- `apps/synapse_admin/frontend/styles/app.css` and `apps/synapse/frontend/styles/app.css`
  (the same four edits in each; the two files differ elsewhere by design):
  - `.chat-pill`: `flex-flow: row nowrap`, `flex: 0 1 auto; min-width: 0; max-width: 240px`,
    `line-height: 1`; `.pill-label` is a block with `min-width: 0; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis`; `.chev { flex: none }`.
  - `.knob-pop`: `box-sizing: border-box; max-height: calc(100vh - 24px); overflow-y: auto`;
    `.knob-hint`.
  - `.knob-stops`, `.knob-stop`, `.knob-stop.on, .knob-stop[aria-checked="true"]`.
  - `.chat-stop`, `.chat-stop .stop-glyph` (12px square in `currentColor`), `.chat-stop.stopping`
    (dimmed, the glyph breathes on `think-orb`, reduced-motion parity).

### Harness (`synapse-agentic-harness-system`)

- `sahs/assistant/loop.py`:
  - `_converse(model, contents, should_stop=…, **kw)`: calls `model.converse` with
    `should_stop`; a double written before the flag existed (TypeError naming `should_stop`)
    is called without it and the loop's own checks stop it at the next chunk.
  - The stream loop: `abort.check()` before every event is processed and after it; the stream
    generator is closed in a `finally` (which closes the HTTP response); spoken text is kept in
    `said` even on a stop; `abort.check()` again the moment the call returns; `abort.check()`
    before every tool call in the pending loop.
  - `DEPTHS[*]["means"]` rewritten for the product's people (below); `MODE_MEANS` say Radix.
- `sahs/assistant/agent.py`: `VertexAgent.converse(..., should_stop=None)` passes the flag to
  the client, returns after a chunk when it fires, and closes the client stream in a `finally`;
  `ScriptedAgent.converse(..., should_stop=None)` checks the flag before each part and, when it
  fires, yields `done` with `finish == "STOPPED"` and the parts so far (the real client's
  behaviour); `model_catalog()` rows carry `facts`.
- `sahs/enrich/client.py`: `VertexClient.converse(..., should_stop=None)` — checked before and
  after every SSE chunk, `finish = "STOPPED"` and the loop breaks; `finally` closes the chunk
  generator; `_sse(..., should_stop=None)` returns from inside the `with` (closing the
  response) when the flag fires; `generate_stream(..., should_stop=None)` the same for the ask
  path.
- `sahs/enrich/gateway_client.py`: `GatewayClient.converse(..., should_stop=None)` — no stream
  to cut; the moment `_post` returns, a fired flag accounts the usage and yields only
  `done` (`STOPPED`), so nothing of the answer is spoken or run.
- `sahs/ask/model.py`: `VertexModel.stream(..., should_stop=None)` passes the flag down and
  stops at the next chunk.
- `sahs/assistant/runtime.py`: `MEMORY_HEAD` → "What Radix remembers about {name}";
  `find_proposal` refusal → "Radix hands one over"; built-in packs' author label → "Radix" (it
  lives here, not in `skills_loader.py`, so no off-limits file was touched).
- `sahs/util/profiles.py`: `ModelProfile.facts` (new field, default ""), carried by
  `as_row()` and `profile_for()`; every `fit` rewritten, every profile given `facts`:
  - Gemini 3.1 Pro — fit "Most careful. Pick for complex metric questions, multi-step SQL and
    dashboards. Slower."; facts "Thinks by level (low, medium, high); Deep and Extra deep fold
    onto high; streams on Vertex; 65,536-token ceiling."
  - Gemini 3.7 Flash — "Fast everyday answers: definitions, quick lookups, simple queries."
  - Gemini 3.5 Flash — "As fast as 3.7 Flash; use when 3.7 is not offered in your environment."
  - Gemini 3.1 Flash Lite — "Fastest and lightest: short factual questions and quick checks,
    not multi-step analysis." (the one-shot JSON note is in `facts`)
  - Gemini 2.5 family — "Older model kept for compatibility; prefer 3.1 Pro or 3.7 Flash."
    ("Retiring…" is in `facts`)
  - unknown Gemini 3 / other families: plain fallbacks, engineer notes in `facts`.
  - The owner's "The default for a new chat" clause is not baked into the 3.1 Pro string
    because the default plane comes from the environment; the picker appends
    "Where a new chat starts." to whichever row the catalog marks `default`.
- Depth stops (`sahs/assistant/loop.py` `DEPTHS`, served by `runtime.dials()`):
  Minimal "Yes-or-no answers and simple lookups. Fastest and cheapest; not for analysis." ·
  Quick "Simple questions: a definition, one number, a follow-up on rows already here. Fast and
  cheap." · Standard "Most questions. The default: a balance of speed, cost and care." · Deep
  "Multi-step analysis or a tricky definition. Slower, and it costs more." · Extra deep "When
  Deep got it wrong, or the question spans several metrics. Slowest and costliest."

### Skill and tests

- `.claude/skills/synapse-ui-designer/SKILL.md`: the naming pin now says Radix is the assistant
  on both surfaces (and which backend strings follow), and lists what does not change.
- `apps/synapse_admin/tests/test_chat_surface.py`: imports the `client`/`compiled` fixtures;
  `test_the_stop_is_an_icon_button_in_the_send_buttons_place` (both surfaces: markup, the
  running-state and stop functions, the pill CSS); `test_the_stop_route_ends_a_running_turn`
  (a scripted model that streams one chunk then waits on `should_stop`; `POST
  /api/chat/sessions/{id}/stop` ends it: `running` false, `turn_done` stopped, partial prose
  kept with "you stopped me", a second stop says nothing is running, the next message is
  accepted).
- `apps/synapse_admin/tests/test_knobs_surface.py`: both surfaces pinned as Radix; pins for the
  pill labels, the stops radiogroup, `place()`, the picker hint, `fit`/`facts` on every catalog
  row, the five depth blurbs verbatim (and no engineer words in them);
  `test_the_model_lines_are_written_for_the_people_who_pick` pins the five `fit` sentences and
  that the engineer words live in `facts`.
- `apps/synapse_admin/tests/test_synapse_surface.py`: memory heading → Radix, built-in author →
  Radix, the naming test now asserts the admin console says Radix (and keeps its definition
  line and its "Synapse by Lumi" title).
- `apps/synapse_admin/tests/test_approval_workflow.py`: the second-surface "gone" pin follows
  the admin string's new wording.
- `synapse-agentic-harness-system/tests/test_v3_loop.py`:
  `test_stop_mid_stream_ends_the_turn_and_nothing_runs_after` (the flag reaches the client
  mid-stream, the client stops reading, one model call, no `tool_call`/`tool_step`, status
  `stopped`, transcript keeps the partial prose with "I stopped there: you stopped me.", handoff
  status `stopped`, the streamed thought stays in the trace) and
  `test_stop_between_the_call_and_the_tool_runs_nothing`.
- `synapse-agentic-harness-system/tests/test_model_catalog.py`: the "JSON"/"Retiring"/"Deep"
  pins moved from `fit` to `facts` (with the new `fit` words pinned).

## Verification

- `node --check` on `apps/synapse_admin/frontend/js/knobs.js`, `apps/synapse/frontend/js/knobs.js`,
  `apps/synapse_admin/frontend/js/pages/chat.js`, `apps/synapse/frontend/js/pages/chat.js`,
  `apps/synapse_admin/frontend/js/pages/skills.js`: all ok; the two `knobs.js` are byte-identical.
- App suite, from the repo root:
  `PYTHONPATH=synapse-agentic-harness-system python -m pytest -q apps/synapse_admin/tests -p no:cacheprovider`
  → `148 passed, 2 skipped`, **exit=0**.
- Harness suite, from inside `synapse-agentic-harness-system`:
  `python -m pytest -q tests -p no:cacheprovider` → 559 collected, all passed, **exit=0**
  (the silo's `addopts = "-q --tb=short"` makes `-q` doubly quiet, so the run prints dots and
  no summary line; the count is from `--co`).

## Not finished / notes for the owner

- **`task_done`**: the brief says to restore the composer on `turn_done` (status stopped) *or*
  `task_done`. There is no `task_done` event and no multi-task runner in this branch
  (`sahs/assistant/events.py` lists thirteen events; nothing in `sahs/` submits tasks), so the
  page restores on `turn_done` (and on `error`) only. If the multi-task work lands from the
  other agent's branch, its sub-turns go through `run_assistant_turn` and will stop at the next
  chunk through the same `should_stop`/`abort.check()` path; the page will need a `case
  "task_done"` arm that calls `setRunning(false)`.
- The built-in packs' author label lived in `sahs/assistant/runtime.py` (`skills()`), not in
  the off-limits `skills_loader.py`, so it was changed to "Radix".
- Not touched, as instructed: `apps/synapse_admin/backend/chat.py`, `sahs/assistant/spanner_store.py`,
  `sahs/tools/validate_sql.py`, `sahs/assistant/skills_loader.py`, `sahs/loop/skills.py`, `db/spanner/`.
- No new environment variables, no model identifiers added to code or commits beyond the
  engine table that already named them, stdlib only.
- The old `-q` harness run cannot be made to print a count without changing `pyproject.toml`;
  left as is.
