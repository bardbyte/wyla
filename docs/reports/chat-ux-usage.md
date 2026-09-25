# Chat UX and usage: the pane scrolls, the thinking folds, what a turn cost is stored and shown, Systematic Intelligence on the second surface

Branch `worktree-agent-a6ceaae395e3002cd` (branched from `claude/production-ready` at
`e3524b6`), worktree `/home/user/wyla/.claude/worktrees/agent-a6ceaae395e3002cd`.
Work commit `f8710b8`; this report is the commit after it. Nothing pushed, no PR opened.

## What the owner reported, and what changed

1. **Scrolling.** (a) The chips block sat between the thread and the composer, outside the
   thread's scroll container: the wheel over it did nothing, and a tall chip row hid the end of
   the thread. (b) The outlet was a 1240px column with 28px padding and the thread the only
   scroll container, so the gutters beside it were dead. Fix, both surfaces: the whole main
   pane is the scroll surface. `#chat-scroll` wraps masthead, hero, thread, chips and composer;
   it is the flex column that scrolls; the masthead is `position: sticky; top: 0`, the composer
   `position: sticky; bottom: 0` (docked, with the page background), the thread and the chip
   row are in the flow at a 1240px reading width centered in a full-width pane
   (`.outlet.chatv2page` drops the width cap and padding). The chips never trap the wheel
   because nothing between the masthead and the composer scrolls on its own. The "Latest" pill
   moved out of the scroller (a child of `.chat-main`, above the composer by its measured
   height). Stick-to-bottom, the pill and re-stick-on-send are unchanged in behaviour, now on
   the scroller (`let stuck = true`, `stuck = atBottom()`, `scroll(true)` all pinned). Chips
   call `scroll()` when drawn so a stuck reader sees them.
2. **Thinking collapsed by default.** `<details class="tool-activity" hidden>` (no `open`).
   While the model thinks the summary is one compact line, "Radix is thinking… 4s", seconds
   from the first one, "Radix is still thinking · 24s" past twenty (a tool's verb reads
   "Searching the graph for X… 3s" / "Still searching…"); the orb and shimmer are the pulse,
   still under reduced motion. The latest thought line rides the summary's hover title. A
   click on the summary (Enter/Space too — it is a `summary`) opens the streamed thoughts and
   steps and records the choice under `localStorage["synapse-thinking-open"]`; `openBlock` and
   `settleBlock` read it, so the answer folds the block to "Thought for 12s" unless the person
   keeps it open, and the next turn opens the way they left it. Replay (`traceBlock`) renders
   the folded summary and records a click the same way. Task rows share the template
   (`activityHTML`, `activityParts`).
3. **Usage.** (a) `Budget` counts the turn's own split (`turn_tokens_in`, `turn_tokens_out`,
   `turn_calls`, `turn_cost_usd` — None when no rate) on every `tick()`, so `budget_tick` and
   `turn_done` carry it (the loop's `_finish` spreads the tick; loop.py untouched). The live
   line shows "· 8,210 tokens (7,900 in · 310 out)" beside the seconds from each tick (a task's
   tick counts on the parent's line); `turn_done` draws the footer "12.4s · 8,210 tokens (7,900
   in · 310 out) · 2 model calls" (a cost only when `SYNAPSE_COST_IN/OUT` are set; "no model
   call" for a Run turn). The runtime writes the same usage onto the turn's final assistant
   message payload (`set_message_usage`), and both replay paths (plain and task-plan messages)
   draw the footer from `payload.usage`. (b) `009_usage.sql`: five `ALTER TABLE ChatSessions ADD
   COLUMN … INT64 NOT NULL DEFAULT (0)`; the stand-in schema has the columns and an existing
   stand-in file migrates on open; the sqlite `AssistantStore` gains the same five columns in
   its forward list. `add_usage` on both stores (one DML statement on the chat tables, owner
   in the WHERE), called from a bus sink the runtime attaches to every session
   (`_settle_usage`: the parent turn's `turn_done` only — a task-tagged record is skipped, so a
   multi-task turn counts once through the parent's totals; a store that fails is logged).
   `list_sessions`/`get_session` on both stores return `tokens_in`, `tokens_out`, `tokens`,
   `model_calls`, `elapsed_ms`, `turns`; `/api/chat/sessions`, `/sessions/{id}` and
   `/api/chat/search` carry them; the shelf rows and the search rows say "8.2K tokens · 3
   turns" (full breakdown on hover). `GET /api/admin/users` gains `usage` per person from
   `usage_by_owner` (one `SUM … GROUP BY OwnerUserId`) — a database from before 009 still
   lists people with `usage_note`; under `SAHS_STORE=local` the route answers with the one
   developer and `usage_totals()` of the local store (it used to 503 there). The People page
   shows a Tokens column, compact, with "7,900 in · 310 out · 2 model calls · 3 turns across 1
   chat · 12.4s" on hover.
4. **Wordmark.** `apps/synapse/frontend/index.html`: title "Systematic Intelligence by Lumi",
   the wordmark "Systematic Intelligence", tagline "by Lumi"; `main.js` logo alt and link title
   the same; `SYNAPSE_LOGO` still replaces the words. `synapse.css` sets the two-word mark
   smaller and tracked wider so it stays on one line. The admin console keeps "Synapse by
   Lumi"; the assistant stays Radix. Pins updated in `test_synapse_surface.py`; the UI skill's
   naming paragraph says which surface reads what.

## File-by-file

### Harness (`synapse-agentic-harness-system`)

- `sahs/ask/budget.py`: `turn_tokens_in`/`turn_tokens_out` counters (reset in `start_turn`,
  added in `charge`); `price()` (static), `cost()`, `turn_cost()`; `tick()` adds
  `turn_tokens_in`, `turn_tokens_out`, `turn_calls`, `turn_cost_usd`.
- `sahs/assistant/store.py`: `_SESSION_COLUMNS` + `tokens_in`, `tokens_out`, `model_calls`,
  `elapsed_ms`, `turns`; `USAGE_FIELDS`, `usage_shape(row)`, `usage_of(record)` (the turn's
  usage from a `turn_done` record; `model_calls`, else the budget's `turn_calls`);
  `_session_out` adds the six usage keys; `add_usage`, `set_message_usage`, `usage_totals`.
- `sahs/assistant/spanner_store.py`: the five columns in `CHAT_SQLITE_SCHEMA`; `USAGE_COLUMNS`;
  `_SESSION_COLUMNS` includes them; the stand-in's forward migration in `__init__` (PRAGMA,
  then `ADD COLUMN` per missing one); `_session_out` adds the usage keys; `add_usage` (one
  `execute_update`), `set_message_usage` (the last assistant message of the turn, payload
  merged, `tx.update`); module `usage_by_owner(database)`.
- `sahs/assistant/runtime.py`: `runtime()` appends the usage sink to every new bus;
  `_settle_usage(session_id, record)`.
- `sahs/assistant/search.py`: the six usage keys on every result row.
- `db/spanner/009_usage.sql`: new.
- `scripts/spanner_ddl_check.py`: `ALTER_ADD_COLUMN_RE` and its arm (a column the table lacks,
  no reserved word, NOT NULL needs a DEFAULT); `COLUMN_RE` reads `INT64`/`FLOAT64` whole; the
  ChatSessions usage columns held to `USAGE_COLUMNS` as INT64; docstrings.
- `db/spanner/README.md`: 001…009, the `009_usage.sql` row, the apply paragraph, the lint
  paragraph.
- `tests/test_usage.py`: new — the budget's turn split; a turn on the row and a second adds on
  the sqlite stand-in, through `SpannerDatabase` over the fake SDK, and on the local store;
  `set_message_usage`; a person's aggregate is the sum of their chats and excludes the other
  person's; the local store's `usage_totals`; a stand-in file from before 009 learns the columns
  on open (and the sqlite store after a `DROP COLUMN`); the runtime sink on both backends (a
  task's `turn_done` skipped, a second turn adds, an error turn counts, a store without the verb
  never breaks the turn); the search rows.
- `tests/test_spanner_ddl.py`: 9 files; `test_009_adds_the_usage_columns_the_store_adds_to`.
- `tests/test_chat_events_store.py`, `tests/test_observe.py`: the sink-list pins (every bus now
  carries the usage settler).

### App (`apps/synapse_admin`)

- `backend/admin.py`: `EMPTY_USAGE`, `_usage_by_person(store)`, `users()` carries `usage` per
  person (and `usage_note` when the aggregate fails); the local-mode answer.
- `frontend/js/pages/chat.js`: the markup (`#chat-scroll`, the pill outside it), the scroll
  logic on the scroller, `activityHTML`/`activityParts`/`rememberThinking`/`thinkOpen`,
  `wording`/`still` in `pulse` (seconds from 1s), `openBlock`/`settleBlock` on the preference,
  `liveUsage`, `usageOf`, `usageFooter`, the `budget_tick`/`turn_done` arms, the two replay
  paths, `traceBlock` records a click, `chipRow` scrolls.
- `frontend/js/chats.js`: `usageLine`, `usageTitle` (exported), the `.chat-usage` line on a row.
- `frontend/js/pages/users.js`: the Tokens column (`tokensCell`, `usageTitle`).
- `frontend/styles/app.css`: the pane/scroller/sticky rules, the thinking-line and usage
  rules, the shelf usage line, `td.tokens`.
- `tests/test_chat_surface.py`: `test_the_pane_scrolls_the_thinking_folds_and_the_usage_shows`
  (markup, CSS, People column, backend pins); `test_a_turns_usage_lands_on_the_message_the_row_and_the_routes`
  (a charging scripted model through the app's local runtime: `turn_done`'s split, the message
  payload's `usage`, the row after two turns, the three chat routes, the local People route).
- `tests/test_usage_routes.py`: new — sqlite and spanner through the app with a signed-in
  admin: two turns on the row and the messages, the shelf/chat/search routes, the raw columns,
  a second person's chat theirs alone, the People aggregate per person, a chat with no turn
  counts as a chat, a non-admin refused.
- `tests/test_synapse_surface.py`: the wordmark pins (title, mark, tagline, `main.js` alt and
  title, the old strings absent, `/synapse/` serves the new title);
  `test_the_pane_scrolls_the_thinking_folds_and_the_usage_shows_here_too`.
- `README.md`: the second surface's heading and header line; a paragraph on the usage line,
  footer, shelf rows and the People column.

### Second surface (`apps/synapse`)

- `frontend/index.html`: title and wordmark.
- `frontend/js/main.js`: logo alt and link title.
- `frontend/js/pages/chat.js`: the same changes as the admin page (its own file).
- `frontend/js/chats.js`: `usageLine`, `usageTitle`, the row line.
- `frontend/js/pages/search.js`: the usage on each result's meta line, breakdown on hover.
- `frontend/styles/app.css`: the same pane/thinking/usage rules; `frontend/styles/synapse.css`:
  the two-word wordmark.

### Docs and skill

- `docs/spanner-wiring.md`: the "what a chat cost" row.
- `.claude/skills/synapse-ui-designer/SKILL.md`: the naming paragraph — admin console "Synapse
  · by Lumi", second surface "Systematic Intelligence" + "by Lumi".

## Tests and exit codes

- `PYTHONPATH=synapse-agentic-harness-system python -m pytest -q apps/synapse_admin/tests -p no:cacheprovider`
  → 163 passed, 2 skipped, exit 0 (baseline before the change: exit 0).
- `cd synapse-agentic-harness-system && python -m pytest -q -p no:cacheprovider` → exit 0
  (baseline: exit 0). Fixed on the way: `test_chat_events_store` and `test_observe` pinned the
  bus sink list as empty; every bus now carries the usage settler.
- `python scripts/spanner_ddl_check.py` → "ok: 44 tables across 9 files …", exit 0.
- `node --check` on every changed JS file (both `chat.js`, both `chats.js`, `search.js`,
  `users.js`, `main.js`): clean.

## Not done, and choices to know about

- The second surface's sign-in card (`apps/synapse/frontend/js/pages/signin.js`) still says
  "SYNAPSE · powered by Lumi": the ask named the top-left header, the title and the surface
  tests; the card is shared markup with the console's sign-in page and was left alone.
- The thinking line's cost estimate shows only when `SYNAPSE_COST_IN`/`OUT` are set (the
  budget's `turn_cost_usd`; the bus drops a None field, so an unpriced turn carries no cost
  key). No env variable was added.
- The People page's aggregate is per store: under Spanner/sqlite one query over `ChatSessions`;
  under `SAHS_STORE=local` the route now answers (it used to 503 there) with the one developer.
- The task rows of a multi-task turn keep their "2 calls · 3.1s" cost; the tokens are on the
  parent's line and footer (the sub-turns spend the parent's turn budget).
- `loop.py` is untouched: the message payload's `usage` is written by the runtime's sink after
  `_finish`'s `turn_done`, onto the last assistant message of that turn (the loop stores it
  before `_finish` on every path).
- Not touched: `artifacts.py`, `kit.py`, `loop.py`, `skills_loader.py`, `validate_sql.py`, the
  artifact rendering functions and the chart CSS (the other agent's lane).

## Files outside the lane

None beyond the two harness test pins named above (`tests/test_chat_events_store.py`,
`tests/test_observe.py`), changed only where they asserted the bus's sink list.
