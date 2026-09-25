# Multi-task turns: a foreman handing out jobs

One message used to be one pass of the loop. A compound ask —
"compare churn across the three regions, then explain which metric
definitions differ, and build a dashboard" — got the same single
interaction as "what does certified spend mean?", and the model had to
juggle three jobs in one context. This page says what happens now,
when it happens, and how to read what it leaves behind.

The code is `sahs/assistant/planner.py` (decide and decompose),
`run_task_turn` in `sahs/assistant/loop.py` (run and report), the
`_task_turn` seam in `sahs/assistant/runtime.py`, and the task board on
both chat pages. The tests are `tests/test_multi_task_turns.py` and the
surface pins in `apps/synapse_admin/tests/test_chat_surface.py` and
`test_synapse_surface.py`.

## The mental model

Think of a foreman with a list of asks. The foreman reads the list,
splits it into jobs, hands the independent ones to two people at once,
holds the dependent ones until their inputs are on the table, and then
writes the answer from what came back — plus a one-page note of who did
what, what was checked, and what did not get done. The foreman never
does a job themself and never re-runs one to check it; the note is
where the checking shows.

In the harness:

- **The planner is the foreman reading the list.** It decides whether
  the message is several jobs and, if so, what they are and which
  waits on which.
- **Each task is a sub-turn of the same session** — the ordinary
  `run_assistant_turn`, with the task's goal as the ask, the parent's
  skills, memories and project, the same kit, the same hooks, the same
  budget object and the same stop button. Nothing about governance is
  different inside a task: the artifact validator still refuses an
  undisclosed number, `propose_sql` still prices with a dry run, the
  scan ceiling and the row cap still hold, the literal check still
  warns.
- **The synthesis is the foreman writing the answer.** One more
  sub-turn, with no tools but `suggest_next`, told to compose from the
  findings only and to say plainly what failed.
- **"What was done" is the note.** A `document` artifact the person
  keeps, one row per task.

## When a plan happens, and when it does not

Two gates, in order, so a simple ask costs nothing extra.

1. **A deterministic pre-filter**, `should_plan(text, mode, depth)`.
   It never calls a model. It says no for anything under eight words
   and for Minimal depth (a one-line answer was asked for). Otherwise
   it scores the message: a strong joining phrase ("then", "and also",
   "as well as", "after that", "for each", "followed by") counts two, a
   weak one ("also", "plus", "next", "finally") one, two or more
   enumerated items count two, two or more question marks count two,
   three or more ask verbs one, and a long message one. Two points and
   the planner is asked. Both modes (Chat and Autopilot) may plan.
2. **One JSON one-shot** through the same `model.json` door the judge,
   the title and the memory pass use, so the temperature policy and the
   "json" thinking stop fold per engine exactly as they do there. The
   model returns `{"tasks": [{"id", "goal", "depends_on", "kind"}],
   "synthesis"}` or `{"tasks": []}` for one job.

The plan is then **validated, repaired or dropped** — never trusted
blindly. Ids are renumbered `t1..tN` (the model's ids are kept only to
resolve dependencies); a dependency on an unknown task or on itself is
dropped; a cycle is broken one edge at a time; a task with no goal is
dropped; an unknown kind reads as `answer`; more than six tasks, or
fewer than two after repair, is no plan at all. Every repair is kept on
the plan and rides the `plan_made` event and the report.

A simple ask — one that fails the pre-filter — runs exactly as before:
the same call, the same events, the same bytes in the prompt.
`test_a_simple_ask_runs_byte_identical_to_the_plain_turn` pins that.
A message that passes the pre-filter but that the model calls one job
runs as one turn too, already announced (`turn_started` carries
`planning: true`), with one planner call spent.

## How the tasks run

`run_task_turn` announces the turn, asks the planner, and emits
`plan_made` with the task list, the dependency waves and the pool
size. Then it schedules:

- Whatever is ready (every dependency finished) is submitted to a pool
  of **two threads** (`TASK_POOL`); independent tasks therefore run
  side by side, and a finished task frees the ones that waited on it.
  There is no knob for the pool: two is the size that keeps a gateway
  rate limit and a laptop happy, and a constant is honest about it.
- A dependent task's ask is its goal plus the **findings of the tasks
  it builds on**: their final prose (capped), the ids and titles of the
  artifacts they published, the names of the rows they saved
  (`q21`, …), the checks they ran and anything that was refused. It is
  told to build on them, not redo them.
- A task's sub-turn runs on a **tagged bus**: every record it emits —
  `turn_started`, `thinking`, `tool_call`, `tool_step`, `artifact`,
  `say_token`, `turn_done`, all of them — carries a `task` field and a
  `turn_id` of `<parent>.<task>`. The page groups them under the
  task's row; the observer sees them as the sub-turns they are.
- The parent brackets each task with `task_started` (goal, dependencies,
  order, its call share) and `task_done` (status, cost, artifacts, saved
  rows, what was checked, what was refused, the first lines of its
  prose).
- **Stop stops everything.** The abort flag is the parent's; a stop
  ends every running task at its next check, nothing new is submitted,
  and the tasks that never started are marked `stopped`. No synthesis
  runs after a stop: the parent closes in plain words, and the report
  still lands.

Task statuses: `planned`, `running`, `done` (answered, proposed or
asked a question), `partial` (hit its share or the clock, or came back
with nothing usable — its own line says which), `failed` (the model was
unreachable, or an exception) and `stopped`.

## The budget split

The turn budget does not grow because the turn has tasks.

- **Model calls.** The turn's ceiling (`MAX_CALLS`, 40) is carved:
  two calls are kept back for the synthesis (`SYNTHESIS_CALLS`) and
  the rest is shared equally, no task under four
  (`MIN_TASK_CALLS`). Three tasks get 12 each; a task that hits its
  share ends with the usual "I hit my ceiling of 12 model calls" line
  and the status `partial`. The parent's `turn_done` reports the total
  across tasks and synthesis, which never exceeds the ceiling.
- **The clock.** Each task gets what is left of the turn's wall clock
  when it starts.
- **Tokens and the session breaker.** One `Budget` object, one
  `start_turn()` at the top of the parent — a sub-turn never resets
  the turn counters — so the turn token cap and the session caps trip
  inside any task exactly as they would in a plain turn.
- **Saved rows.** Each task's `q<N>` names start at a different
  offset (`QUERY_STRIDE`, 20: `t1` saves `q1…`, `t2` saves `q21…`), so
  two tasks running at once never overwrite each other's rows and a
  dependent task can read its inputs' rows by name.

## What the report contains

"What was done" is a `document` artifact published under the parent
turn before the synthesis runs (so the final message lists it), and
it is watermarked EXPLORATORY like every document — it is a record of
process, not a claim about the data. It holds:

- the ask, the number of tasks and their waves, how many ran side by
  side, and any plan repairs;
- a table with one row per task: the goal (and what it came after),
  the status with its reason, what was checked (each `check` step's
  summary) and what was refused (every hook refusal, by tool, with the
  validator's code — `artifact: artifact refused — provenance_missing:
  …`), what it left behind (artifacts by title and version, saved
  rows), and its cost (model calls, tool steps, seconds);
- a "Not finished" line naming every task that is not `done`, or
  "Every task finished";
- the totals, and the reminder that the same checks, gates and limits
  applied inside every task.

The final assistant message carries the same rows in its payload
(`payload.plan`), which is how the transcript replays the board.

## The transcript

One user message, as today. One assistant message per task, with
`payload.task` naming the task, its goal and its parent, so the
transcript replays each task's own prose, trace and artifacts. Then
the synthesis as the final assistant message, with `payload.plan`.
The history a later turn sees carries the synthesis, not the task
lines, so the conversation stays one answer per ask.

## How to read the task board

Under the person's message, a card: "Split into 3 tasks · up to 2
side by side". One row per task — a mark, the goal, "after t1, t2"
when it waited, a status chip, and the cost once it is done ("3 calls
· 2.1s"). The running row is open and shows the task's own thinking
line, steps and prose, the same shape as any turn; a finished row
folds and opens on a tap. A failed, partial or stopped row says why
under its steps. The turn's own answer — the synthesis — comes below
the board as the assistant's prose, with "What was done" as a card
beside any artifacts the tasks made. Reopening the chat later draws
the same board from the stored plan.

## What would have to be true to delete this

The harness-discipline page asks. If a model release handles a
three-job message in one interaction as well as three sub-turns do —
the same E19 delta, the same transcript reading — the planner's gate
can be set to never and the runner removed; nothing else depends on
it. The `SubTurn` contract on `run_assistant_turn` is the only thing
the plain turn learned, and a `None` there is the plain turn.
