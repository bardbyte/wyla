# Langfuse insight: everything it shows, rebuilt from the record

Langfuse is a mirror of the record, never a source. That is the rule
`docs/runbooks/langfuse.md` states and this guide keeps: every object
the team will see in Langfuse — a trace, a session, a user, a
generation's usage, a tool span, a score, a prompt version, a dataset
item, an experiment run — is derived, one way, from something Synapse
already writes, and since the record moved into Spanner
(`db/spanner/002_chat.sql`: `ChatSessions`, `ChatEvents`,
`ChatMessages`, `ChatFeedback`, with `Users` behind them) it is derived
from those tables. Turn Langfuse on tomorrow and the last ninety days
of chats appear in it as if it had been on all along; turn it off and
nothing in the product notices.

Think of Langfuse as the reading room of a library whose stacks are
Spanner. Nothing is shelved in the reading room; every page on the
tables was carried in from the stacks, and the same page can be
carried in again without a second copy appearing.

## What was set up

| piece | where | what it does |
|---|---|---|
| the translator | `sahs/observe/tracer.py` | one turn's records → one trace. New: a compound turn's task sub-turns nest as task spans of the parent trace; the plan, the loader record and the chips are events; the planner's usage is a generation; the turn is scored on status, refusals and failed tasks; the prompt fingerprint rides on every generation |
| pinned ids | `sahs/observe/langfuse_emitter.py` | observation and score ids are a hash of the trace id and the tracer's key, handed to the SDK through an OpenTelemetry id generator (`PINNED`), so a second backfill updates in place |
| the prompt fingerprint | `sahs/assistant/prompt_version.py`, one emit in `loop.py` | `prompt_version = ASSISTANT_VERSION + "+" + hash(prefix before <skills>)`, and `prompt_parts` = a hash per section, on the `model_prompt` n=0 record |
| the record reader | `sahs/observe/record.py` | reads the chat tables through `SpannerAssistantStore` (one store per owner), groups the rows into turns, replays them through the tracer (`backfill`) |
| the dataset builders | `sahs/observe/datasets.py` | precedents (gold), silver (production turns worth keeping), scenarios (compound asks and their plans): pure functions of rows, written to JSONL for review, uploaded on `--push`, read back as harness tasks |
| the harness | `sahs/evals/{schema,grading,suts,assistant_sut}.py` | a `decompose` task kind, a `plan` answer, the skills a SUT loaded, the assistant loop and the planner as SUTs |
| the coverage check | `sahs/observe/coverage.py` | concept → table and columns → present or missing, from the live rows |
| the prompt registry | `sahs/observe/prompts.py` | the assembled templates plus every static part (identity, chain, the mode blurbs, the style section per engine family, the planner's, judge's and reviewer's one-shots) with `production` and the family as labels |
| the commands | `scripts/langfuse_sync.py`, `scripts/run_evals.py` | below |

## A new environment, in order

Everything below reads the silo `.env`: `SAHS_LANGFUSE=1` with the SDK
keys for anything that writes to Langfuse, `SAHS_STORE=spanner` (or
`sqlite` for a rehearsal) with the store block for anything that reads
the record. No new variable was added.

```
cd synapse-agentic-harness-system

# 1. the keys work
python scripts/langfuse_sync.py check

# 2. the prompt versions, so every generation links to its template
python scripts/langfuse_sync.py prompts

# 3. the datasets: the task files, then the three built sets
python scripts/langfuse_sync.py datasets
python scripts/langfuse_sync.py datasets --build precedents            # → graph/langfuse/precedents.jsonl
python scripts/langfuse_sync.py datasets --build silver --since 2026-09-01
python scripts/langfuse_sync.py datasets --build scenarios
#    read the three files, then upload what you reviewed
python scripts/langfuse_sync.py datasets --build precedents --push
python scripts/langfuse_sync.py datasets --build silver --push
python scripts/langfuse_sync.py datasets --build scenarios --push

# 4. the record: every turn of the last ninety days becomes its trace
python scripts/langfuse_sync.py backfill --from spanner
#    or narrower
python scripts/langfuse_sync.py backfill --from spanner --since 2026-09-01 --owner <user id>
python scripts/langfuse_sync.py backfill --from spanner --session <session id>

# 5. what the rows can and cannot supply
python scripts/langfuse_sync.py coverage

# 6. the calibration lines, then a real run, each as a dataset run
python scripts/run_evals.py --tasks tests/tasks/curated/curated.jsonl --sut oracle --langfuse
python scripts/run_evals.py --tasks tests/tasks/curated/curated.jsonl --sut null --langfuse
python scripts/run_evals.py --tasks graph/langfuse/precedents.jsonl --sut oracle --langfuse
```

Then start the app with the switch on: live turns land beside the
backfilled ones under the same ids, so the day the switch went on is
not a seam in the data.

`backfill --from spanner` is idempotent three ways: the trace id is a
hash of session and turn, every observation id a hash of the trace id
and the tracer's key (`root`, `gen1`, `tool2`, `task:a`, `a/gen1`,
`event3:chips`), every score id a hash of the trace id and the score
name. Run it nightly if you like; Langfuse holds one copy.

**The window.** `ChatEvents` carries `ROW DELETION POLICY
(OLDER_THAN(Ts, INTERVAL 90 DAY))`. A backfill reaches the turns still
in the table; `ChatSessions`, `ChatMessages` and `ChatFeedback` have
no such policy, so an older session keeps its messages and votes with
no trace to hang them on (the silver builder still finds its SQL in
the message payload, the backfill does not find a turn). Either
backfill within the window — the first time the switch goes on, and
then on a schedule — or, if the team wants the whole history in
Langfuse, extend the policy in `002_chat.sql` before the first ninety
days pass. The sync does not extend it for you.

## The trace a reader sees

One `assistant.turn` trace per top-level turn, in session
`ChatSessions.SessionId`, for user `ChatSessions.OwnerUserId`. Its
input is the person's message, its output the assistant's prose; its
metadata carries the four coordinates (`build_id`, `version`, the
plane and the model label) plus `prompt_version`, `prompt_parts`,
`thinking_level`, `mode`, `skills`, `elapsed_ms`, the token totals,
`refused` (when anything was) and, on a compound turn, `tasks`
(id → status).

```
assistant.turn                       tags: assistant plane:vertex mode:chat skill:authorizations [compound] env:laptop
├─ skills_loaded  (event)            loaded, modes per skill, aggregate chars, the whole-load limit
├─ model call 1   (generation)       input: system (once) + contents · usage: the budget-tick delta · prompt: wyla-assistant-system vN
├─ search         (tool)             args, the compact summary; the rows only with SAHS_LANGFUSE_FULL_RESULTS=1
├─ model call 2   (generation)
├─ artifact:chart (event)            artifact_id, version, title
├─ proposal       (event)            the card: sql, title, metric_id, bytes, warnings
├─ chips          (event)            the follow-ups offered
└─ scores: turn_status, refused [, tasks_failed] [, feedback]
```

A compound ask is still one trace:

```
assistant.turn                       tag compound; metadata.tasks = {a: done, b: failed}
├─ planner        (generation)       the one-shot's usage, read off the first budget tick
├─ plan           (event)            tasks, synthesis, waves, pool
├─ task a         (span)             goal, kind, depends_on; output: the sub-turn's prose; metadata: status, reason, cost, saved, checked, refused
│  ├─ model call 1 (generation)      keys a/gen1, a/tool1 …: the sub-turn's own calls and tools
│  └─ run_sql      (tool)            level ERROR when the step was refused
├─ task b         (span)             level WARNING when not done
├─ model call 2   (generation)       the synthesis, on the parent
└─ scores: turn_status, refused, tasks_failed
```

## The scores

| score | type | on | meaning |
|---|---|---|---|
| `turn_status` | categorical | every turn | `turn_done.status`: answered, proposed, clarify, partial, stopped, error |
| `refused` | numeric | every turn | tool steps whose summary starts `ERROR` (the validator, the cost ceiling, a configuration refusal), the first three in the comment — the same rule the task record uses |
| `tasks_failed` | numeric | compound turns | tasks of the plan that did not end `done`, named in the comment |
| `feedback` | numeric | turns with a vote | `ChatFeedback`: 1 up, 0 down; subject and note in the comment. Written by the backfill (joined by turn id), so a vote after the backfill arrives on the next run |
| `verdict`, `pass` | categorical, numeric | eval trials | the harness's verdict (`sahs/evals/grading.py`): pass, fail, ambiguous |
| `skill_hit` | numeric | eval trials on precedents and silver | 1 when the turn loaded the skill the item names |
| `resolution` | categorical | queued trials | a steward's accept or fail on an ambiguous verdict (`annotations.py`) |

## Views worth saving

Every filter below is on a tag or a metadata key the tracer sets;
the names are pinned in `sahs/observe/tracer.py`.

| view | filter |
|---|---|
| this build's turns | metadata `build_id` = the promoted build |
| one prompt version | metadata `prompt_version` (the version string plus the prefix hash) |
| one engine | tag `plane:vertex` or `plane:gateway`; metadata `model` for the label |
| one skill's turns | tag `skill:<name>` (pinned on the chat), or the `skills_loaded` event's `loaded` |
| compound turns | tag `compound`; score `tasks_failed` > 0 for the ones that lost a task |
| refused anything | score `refused` > 0 |
| thumbed down | score `feedback` = 0 |
| laptop vs deployment | tag `env:<SAHS_LANGFUSE_ENV>` |
| the eval lines | tag `eval`, tag `verdict:ambiguous`; the dataset-run view per `<sut>·tasks@<hash>·canon<version>·<time>` |

### Self-hosted: the SQL for the weekly read

On a self-hosted Langfuse v3 the store is ClickHouse: `traces`
(`id`, `session_id`, `user_id`, `tags`, `metadata` as a map),
`observations` (`trace_id`, `type`, `name`, `level`, `metadata`,
`usage_details`, `cost_details`, `total_cost`, `prompt_name`,
`prompt_version`, `start_time`, `completion_start_time`) and `scores`
(`trace_id`, `name`, `value`, `string_value`). Column names move
between releases; check them against your install before saving.

```sql
-- 1. cost per prompt version, this week
SELECT t.metadata['prompt_version'] AS prompt_version,
       count(DISTINCT t.id)          AS turns,
       sum(o.total_cost)             AS cost,
       sum(o.usage_details['input']) AS tokens_in
FROM observations o JOIN traces t ON t.id = o.trace_id
WHERE o.type = 'GENERATION' AND t.timestamp >= now() - INTERVAL 7 DAY
GROUP BY 1 ORDER BY cost DESC;

-- 2. failure rate per task kind (the task spans of compound turns)
SELECT o.metadata['kind']                                       AS kind,
       count()                                                  AS tasks,
       countIf(o.metadata['status'] != 'done') / count()        AS failure_rate
FROM observations o
WHERE o.name LIKE 'task %' AND o.type = 'SPAN'
  AND o.start_time >= now() - INTERVAL 7 DAY
GROUP BY 1 ORDER BY failure_rate DESC;

-- 3. cache hit rate per plane — once the transport records cached
--    tokens on the budget tick (see the gaps); the column will be
--    usage_details['cached'] and this query is ready for it
SELECT arrayFirst(x -> startsWith(x, 'plane:'), t.tags)       AS plane,
       sum(o.usage_details['cached']) / sum(o.usage_details['input']) AS cache_hit_rate
FROM observations o JOIN traces t ON t.id = o.trace_id
WHERE o.type = 'GENERATION' AND t.timestamp >= now() - INTERVAL 7 DAY
GROUP BY 1;

-- 4. refused checks by skill
SELECT arrayJoin(arrayFilter(x -> startsWith(x, 'skill:'), t.tags)) AS skill,
       count()                     AS turns,
       sum(s.value)                AS refusals,
       countIf(s.value > 0) / count() AS share_with_a_refusal
FROM scores s JOIN traces t ON t.id = s.trace_id
WHERE s.name = 'refused' AND t.timestamp >= now() - INTERVAL 7 DAY
GROUP BY 1 ORDER BY refusals DESC;

-- 5. time to first token per engine: the first generation's start
--    against the trace's start (the budget tick closes a generation,
--    so completion_start_time is the closest the record comes)
SELECT arrayFirst(x -> startsWith(x, 'plane:'), t.tags) AS plane,
       quantile(0.5)(o.start_time - t.timestamp)          AS p50_seconds,
       quantile(0.9)(o.start_time - t.timestamp)          AS p90_seconds
FROM observations o JOIN traces t ON t.id = o.trace_id
WHERE o.type = 'GENERATION' AND o.name = 'model call 1'
  AND t.timestamp >= now() - INTERVAL 7 DAY
GROUP BY 1;
```

The last one is honest only for live traces: a backfill's timing is
import time. For backfilled turns read `elapsed_ms` from the trace
metadata instead — it is the turn's own clock, carried in the record.

## Experiment runs: the harness on the three datasets

`run_evals.py --tasks <file>` takes a task file or an item file the
builders wrote; the grader is unchanged (`sahs/evals/grading.py`).
Precedent and silver items become `nl2sql` tasks graded by canonical
fingerprint (an item with prose and no SQL is skipped — fingerprints
cannot grade prose), scenario items become `decompose` tasks graded on
the plan's shape: the same task count, kinds and dependency depth
pass; the same count with different kinds or depth is ambiguous and
goes to the queue; a different count fails. With `--langfuse` each
verdict is a score on the item's run trace; on precedents and silver
`skill_hit` says whether the turn loaded the skill the item names.

The SUTs:

| `--sut` | what runs | answers |
|---|---|---|
| `oracle`, `null` | the calibration instruments | every kind |
| `assistant:<builds dir>` | one real turn per prompt through `AssistantRuntime` on the engine the `.env` names (`SAHS_MODEL_PLANE`); the SQL is read off the proposal card, or the query the loop ran; the skills off `skills_loaded` | `nl2sql` |
| `planner` | `plan_for` on the engine the `.env` names | `decompose` |

On a laptop, against the real engine, with the promoted build:

```
python scripts/run_evals.py --tasks graph/langfuse/precedents.jsonl --sut assistant:graph/builds --langfuse --wait-seconds 180
python scripts/run_evals.py --tasks graph/langfuse/silver.jsonl     --sut assistant:graph/builds --langfuse
python scripts/run_evals.py --tasks graph/langfuse/scenarios.jsonl  --sut planner --langfuse
```

Run the oracle and the null SUT on the same file first: 100 percent
pass and exactly the abstain share (zero on these sets) are the
proof the sync is sound. In the tests the same SUTs run on
`ScriptedAgent` (`tests/test_langfuse_insight.py`), so the wiring is
exercised with no model host.

## Growing eval sets from production: silver → reviewed → gold

1. **Silver** is built, never curated: a turn a person thumbed up, or
   one that finished (`answered`, `proposed`, `clarify`) with nothing
   refused, and never one thumbed down. The item keeps the question,
   the SQL on the card, the answer, the skills loaded, the build and
   the prompt version, and names its source turn so a reviewer can
   open the trace beside it. Rebuild it weekly with `--since`; it is a
   sample of what production believed was right, not a gold set.
2. **Reviewed.** Run the assistant SUT on the silver file with
   `--langfuse`. Every fingerprint mismatch with a matching shape is
   `ambiguous` and lands on the `wyla-ambiguous` annotation queue
   (`annotations.py`), where a steward scores `resolution` accept or
   fail in the Langfuse UI. `pull-annotations` writes each accept into
   the task file's `accepted_fps`; a person commits the diff. A silver
   item that survives the review — its SQL accepted, its frame checked
   by a person — is ready to be promoted.
3. **Gold.** Promotion is a copy into the precedents JSONL (the
   documented shape in `sahs/observe/datasets.py`: question, SQL,
   skill, frame, source, notes), committed under `tests/fixtures/` or
   wherever the team keeps its precedents, and pushed with
   `datasets --build precedents --push`. From then on it is graded
   like any curated task, and the silver item that fed it is only
   history.

Nothing in this loop reads Langfuse at grading time; the suite learns
through git, as the runbook already says.

## Coverage, as `langfuse_sync.py coverage` prints it

The table computed on the test fixture (two sessions, three turns,
one compound) — the rows are the same on a deployment, the counts
are yours:

| concept | built from | state |
|---|---|---|
| trace | ChatEvents (turn_started … turn_done) | present |
| session | ChatSessions.SessionId | present |
| user | ChatSessions.OwnerUserId → Users.UserId | present |
| generation | ChatEvents model_prompt | present |
| generation usage | ChatEvents budget_tick deltas | present |
| tool span | ChatEvents tool_call / tool_step / tool_result | present |
| task span | ChatEvents task_started / task_done + the sub-turn | present |
| artifact event | ChatEvents artifact ≈ ChatArtifacts | present |
| proposal event | ChatEvents proposal / ChatMessages.Payload.proposal | present |
| chips event | ChatEvents chips | present |
| score turn_status | ChatEvents turn_done.status | present |
| score refused | ChatEvents tool_step.summary starting ERROR | present |
| score tasks_failed | ChatEvents task_done.status | present |
| score feedback | ChatFeedback joined by turn | present |
| prompt version | ChatEvents model_prompt n=0 prompt_version | present from this build on; older turns lack it |
| dataset item: silver | the turns above, SQL from the message payload | present |
| dataset item: scenarios | ChatEvents plan_made | present |
| dataset item: precedents | a checked-in JSONL, not Spanner | not from the rows |
| experiment run | run_evals.py, not Spanner | not from the rows |
| per-task usage | budget_tick is a session counter | gap |
| full prompt text | cut at 12 000 chars by design | gap |
| human labels beyond thumbs | no table | gap |

## The gaps, honestly

- **Per-task token attribution.** `budget_tick` is the session's
  counter. A compound turn runs two tasks side by side, so the delta
  between consecutive ticks is right in total for the session and
  approximate per generation while tasks overlap. The sum is exact;
  the split is not. Fixing it means the model client tagging each
  tick with the call it closed, which is the loop's change, not the
  mirror's.
- **The prompt text is truncated on purpose.** `model_prompt` n=0
  keeps the first 12 000 characters of the system prompt; a skill
  pack can run past that. The fingerprint (`prompt_version`,
  `prompt_parts`) says exactly which text a turn ran under; the
  registered templates say what the static parts were; the skills
  section is reproducible from the build and the pinned skills at
  that version, not from the record.
- **Human labels beyond thumbs.** The record has `ChatFeedback`
  (up, down, a note). Any richer label — a steward's accept on an
  ambiguous verdict, a corrected SQL — lives only in Langfuse's
  annotation queue until `pull-annotations` writes the accepts into
  the task files. Langfuse is a source for exactly that one thing,
  and only through a diff a person commits.
- **Cached-token counts.** The transport does not yet put the cached
  prefix tokens on the budget tick, so the cache hit rate is a query
  waiting for its column (`usage_details['cached']`).
- **Turns older than ninety days** are messages and votes with no
  trace (the deletion policy above).
- **Timing on a backfill** is import time; the record's own
  `elapsed_ms` is on the trace.
- **Prompt versions before this build** have no fingerprint: a
  backfilled turn from before it links to the registered prompt by
  `ASSISTANT_VERSION` alone, with no prefix hash.
- **The title and memory one-shots** named in the agent's docstrings
  have no separate template in this tree (the memory section is part
  of the assembled prompt), so the registry carries the planner's,
  the judge's and the reviewer's one-shots and no more.
