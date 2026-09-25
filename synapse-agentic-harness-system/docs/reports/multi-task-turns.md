# Report: multi-task turns (workstream "multi-task turns")

Feature commit: `d256f1f` ("Multi-task turns: a compound ask becomes
tasks that run side by side and in order, with a "What was done"
report").

## What changed, file by file

### `synapse-agentic-harness-system/sahs/assistant/planner.py` (new)
- `compound_signals(text)` / `should_plan(text, mode, depth)`: the
  deterministic gate. No plan under 8 words or at Minimal depth;
  otherwise a score (strong joiners "then / and also / as well as /
  after that / for each / followed by" = 2, weak ones = 1, 2+
  enumerated items = 2, 2+ question marks = 2, 3+ ask verbs = 1, a
  long message = 1); 2 points sends the message to the model.
- `PLAN_SYSTEM`, `plan_prompt`, `plan_for(model, text, mode, depth)`:
  one JSON one-shot through `model.json(...)`, the same door the
  judge, title and memory passes use (temperature policy and the
  "json" thinking stop fold per engine in the agent/client, as there).
- `validate_plan(raw)`: renumbers ids `t1..tN`, maps the model's ids
  for dependencies, drops unknown and self dependencies, breaks
  cycles one edge at a time (Kahn's order, one ready task per pass so
  the model's order survives where valid), drops empty goals, defaults
  an unknown kind to `answer`, reads a string `depends_on`, caps goal
  and synthesis lengths; returns no plan for non-JSON, non-object, no
  tasks, one task after repair, or more than six tasks. Repairs ride
  on the plan.
- `waves(tasks)`: the dependency waves for the record and the report.

### `synapse-agentic-harness-system/sahs/assistant/events.py`
- `ASSISTANT_EVENTS` gains `plan_made`, `task_started`, `task_done`
  (after `turn_started`), with a comment that a task's sub-turn
  records carry a `task` field.

### `synapse-agentic-harness-system/sahs/assistant/loop.py`
- Constants `TASK_POOL = 2`, `SYNTHESIS_CALLS = 2`, `MIN_TASK_CALLS = 4`,
  `QUERY_STRIDE = 20`, `FINDINGS_CAP`, `REPORT_TITLE = "What was done"`.
- `SubTurn` dataclass: how a turn runs inside another (task tag,
  label, announced, finish, title, query_offset, tools filter, extra
  payload, prior artifacts).
- `run_assistant_turn(..., sub_turn=None)`: with a `SubTurn` it skips
  `turn_started` when announced, never calls `budget.start_turn()` or
  `prepare_workspace` (the parent owns both), offsets `q<N>` naming,
  filters the kit, merges the extra payload and prior artifacts into
  every stored message, sets the title only when allowed, and emits
  `turn_done` only when `finish`. With `None` the turn is unchanged.
- `_history` skips assistant rows with `payload.task` (task lines),
  and the history skip for "this turn's ask" keys on the parent id
  (`turn_id.split(".")[0]`) so a task's sub-turn sees neither the
  compound ask nor the other tasks.
- `_TaggedBus`: the bus a task writes to — every record gets `task`,
  and a `TaskRecord` observes each record (prose, artifacts, `check`
  summaries, `ERROR` refusals, `saved as qN`, model calls counted from
  `model_prompt` calls, steps from `tool_step`, elapsed from `turn_done`).
- `TaskRecord`, `_findings`, `_task_text`, `_synthesis_text`,
  `report_markdown`: the context a dependent task and the synthesis
  receive, and the "What was done" document (ask, waves, repairs,
  a table with goal / status+reason / checked+refused / left behind /
  cost, a "Not finished" line, totals).
- `run_task_turn(...)`: announces the turn (`planning: true`), asks the
  planner, falls back to the plain turn (already announced) with no
  plan; else emits `plan_made`, sets the title, carves the call share
  (`(max_calls - 2) // n`, floor 4), schedules ready tasks on a
  `ThreadPoolExecutor(2)` with `wait(FIRST_COMPLETED)`, emits
  `task_started`/`task_done`, hands finished findings to dependents,
  stops submitting on abort and marks unstarted tasks `stopped`,
  publishes the report document under the parent turn, closes without
  a model call after a stop, else runs the synthesis sub-turn (tools:
  `suggest_next` only, at most 2 calls, `payload.plan` on its message)
  and emits the parent `turn_done` with the summed calls and steps.
  A model that dies during the synthesis is closed in plain words
  with the plan still stored.
- `__all__` extended.

### `synapse-agentic-harness-system/sahs/assistant/runtime.py`
(only `_model_turn` and the new `_task_turn`)
- `_model_turn` builds one `common` kwargs dict and picks the body:
  `self._task_turn(common)` when `should_plan(prompt_text, mode, level)`,
  else `run_assistant_turn(**common)` exactly as before; the same
  worker guard wraps both.
- `_task_turn(common)` returns `lambda: run_task_turn(**common,
  pool_size=TASK_POOL)`.
- Imports `TASK_POOL`, `run_task_turn`, `should_plan`.

### `apps/synapse_admin/frontend/js/pages/chat.js` and
### `apps/synapse/frontend/js/pages/chat.js`
- Task board: `boardFor`, `boardHead` ("Split into N tasks · up to 2
  side by side"), `taskTurnFor` (a row per task with the same object
  shape as a turn, so the existing step/thought/card/pulse helpers work
  on it), `taskStatus` (status chip, mark, cost "3 calls · 2.1s",
  reason note, running row open), `homeOf` (a tagged event lands under
  its task on the parent turn `<parent>.<task>`).
- `handle`: arms for `plan_made`, `task_started`, `task_done`; a
  task's own `turn_started`/`turn_done` settle its row only; a task's
  `chips` are ignored; the parent's pulse reads "Sorting out the asks…"
  while planning and "Working through N tasks…" after the plan.
- `subscribe`: the three new SSE listeners.
- Replay: `replayTaskMessage` draws task messages (`payload.task`) under
  the board and the synthesis (`payload.plan`) as the turn's answer with
  every row's status and cost; messages of an in-flight turn are left
  to the event replay so nothing draws twice.

### `apps/synapse_admin/frontend/styles/app.css` and
### `apps/synapse/frontend/styles/synapse.css`
- `.task-board`, `.task-board-head`, `.task-row` (+ `.running`, `.done`,
  `.partial`, `.failed`, `.stopped`), `.task-mark`, `.task-goal`,
  `.task-after`, `.task-status`, `.task-cost`, `.task-body`,
  `.task-note`, reduced-motion parity.

### Docs
- `synapse-agentic-harness-system/docs/multi-task-turns.md` (new): the
  foreman model, when a plan happens and when not, how tasks run, the
  budget split, the report, the transcript, how to read the board,
  what would have to be true to delete it.
- `synapse-agentic-harness-system/docs/specs/synapse_v3_harness.md`:
  decision 26 links the page.
- `apps/synapse_admin/README.md`: a paragraph after the dials
  paragraph links the page.
- `.env.example`: unchanged — no new environment variable was needed
  (the pool size is the constant `TASK_POOL = 2`).

## Tests added and exit codes

- `synapse-agentic-harness-system/tests/test_multi_task_turns.py` (new,
  8 tests, `RoutedAgent` scripted double routed by the ask's first line):
  - `test_the_gate_is_deterministic_and_cheap`
  - `test_the_validator_repairs_or_drops` (non-JSON, non-object, no
    tasks, one task, over six, duplicate ids, unknown/self deps, empty
    goal, unknown kind, string deps, cycle broken one edge at a time,
    topological order, waves, a valid plan untouched but for the ids)
  - `test_a_simple_ask_runs_byte_identical_to_the_plain_turn` (no JSON
    call; normalized events, the system prompt and the contents equal
    to a direct `run_assistant_turn`)
  - `test_a_compound_ask_runs_tasks_side_by_side_then_in_order`
    (3-task plan with one dependency; a `threading.Barrier(2)` inside
    t1 and t2 proves they overlapped, and the event seqs do too; t3
    saw t1's prose and artifact id and t2's refusals; the tag on every
    sub-turn record; a broken `propose_sql` and a naked chart refused
    by the hooks inside t2, named in `task_done` and in the report;
    synthesis with `suggest_next` only; share and total under
    `MAX_CALLS`; the "What was done" rows; one user message, three task
    messages, the synthesis last with `payload.plan`; a later turn's
    history carries the synthesis, not the task lines)
  - `test_the_call_share_caps_each_task_and_the_total`
  - `test_a_stop_mid_way_marks_the_rest_stopped` (no synthesis after a
    stop, "Stopped by you" in the report, stored plan says stopped)
  - `test_the_planner_may_decline_and_the_turn_runs_as_one`
  - `test_a_task_whose_model_dies_is_a_failed_row_not_a_dead_turn`
- `apps/synapse_admin/tests/test_chat_surface.py`:
  `test_a_compound_ask_draws_a_task_board` (the existing
  `test_every_assistant_event_reaches_the_page` now covers the three
  new events).
- `apps/synapse_admin/tests/test_synapse_surface.py`:
  `test_every_assistant_event_reaches_this_page_too`,
  `test_a_compound_ask_draws_a_task_board_here_too`.

Runs (PIPESTATUS discipline):
- `python -m pytest -q tests -p no:cacheprovider` from inside
  `synapse-agentic-harness-system`: all passed, `exit=0`.
- `PYTHONPATH=synapse-agentic-harness-system python -m pytest -q
  apps/synapse_admin/tests -p no:cacheprovider` from the repo root:
  148 passed, 2 skipped, `exit=0`. (Without `PYTHONPATH` the app suite
  fails at collection on `import sahs` in `backend/app.py`; that is
  pre-existing, not from this change.)

## Not finished / caveats

- Per-task token cost is not attributed: tasks that overlap share one
  `Budget`, so a task's cost is reported as model calls, tool steps and
  seconds; tokens appear as the session total in the report and on the
  parent's `turn_done`. Attributing tokens per thread would need a
  per-task budget proxy the agents charge, which lives outside my
  ownership (`agent.py`, `sahs/ask/model.py`).
- The pages were syntax-checked (`node --check`) and pinned by the
  surface tests; there is no DOM harness in the repo, so the board was
  not exercised in a browser here.
- A task's `turn_started` is tagged and opens its own trace in the
  Langfuse tracer (one trace per sub-turn); `plan_made` /
  `task_started` / `task_done` have no tracer handler and are ignored
  there, which is the tracer's documented behaviour for unknown events.

## Files outside my ownership touched

None. `kit.py`, `sahs/loop/skills.py`, `spanner_store.py`, `files.py`,
`reviews.py`, `authoring.py`, `skills_loader.py`, `chat.py` (backend)
and the skills section of the prompt in `loop.py` are untouched.
`runtime.py` changes are confined to `_model_turn` and the new
`_task_turn` (plus the import line).
