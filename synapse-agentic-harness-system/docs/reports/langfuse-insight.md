# Report: Langfuse insight — the mirror rebuilt from Spanner

Based on the integration branch at `33fade0` (the store's `add_event`,
`events`, `last_turn` and `docs/spanner-wiring.md` exist from there).
Commits: `14e7af6` (the mirror change), `f97fba5` (the insight change),
plus this report.

The guide is `docs/runbooks/langfuse-insight.md`; it is linked from
`docs/runbooks/langfuse.md` (step two) and from `apps/synapse_admin/README.md`.

## File by file

| file | change |
|---|---|
| `sahs/assistant/prompt_version.py` | new: `prompt_fingerprint(system, version)` → `prompt_version` (`ASSISTANT_VERSION + "+" + sha[:8]` of the prefix before `<skills>`), `prompt_prefix`, `prompt_parts` (a hash per `<tag>` section); `prompt_parts()` the section parser. Pure function of the text. |
| `sahs/assistant/loop.py` | two lines, outside the skills block: the import, and `**prompt_fingerprint(system, ASSISTANT_VERSION)` on the `model_prompt` n=0 emit (content stays cut at 12 000 on purpose). |
| `sahs/observe/tracer.py` | rewritten in place: a compound turn's sub-turns (`<turn>.<task>`, records tagged `task`) attach to the parent trace under a `task:<id>` span opened on `task_started` (or on the sub-turn's own `turn_started` when never announced) and closed on `task_done` with status, reason, cost, saved, checked, refused; keys prefixed `<task>/` so concurrent sub-turns never collide; `trace_id_for` maps a sub-turn id to the parent's. `plan_made` → `plan` event; `skills_loaded` → event (loaded, modes per skill, chars, limit, chunks); `chips` → event; the planner's tokens (a `budget_tick` with no open generation) → a `planner` generation, any other unannounced usage → `model call (untracked)`; the fingerprint on every generation's metadata and the trace's; refusals counted from `tool_step` summaries starting `ERROR` (the task record's rule) → score `refused`; `tasks_failed` on a planned turn; `feedback_score()` for `ChatFeedback` votes; `replay_records()` for the store's records. Emitter protocol gains `parent` on `generation_open`, `span_open`, `event`. |
| `sahs/observe/langfuse_emitter.py` | `PinnedIds` (an OpenTelemetry id generator primed per thread), `PINNED`, `observation_id_for(trace_id, key)`, `score_id_for(trace_id, name)`; every observation and score gets a deterministic id; `_holder()` resolves `parent` to the task span; a `task:` span is `as_type="span"`, a tool stays `tool`. |
| `sahs/observe/setup.py` | `langfuse_client()` constructs `Langfuse(id_generator=PINNED)`. |
| `sahs/observe/record.py` | new: `RecordReader(database)` — `sessions(since, session_id, owner)`, `events` (paged through `SpannerAssistantStore.events`), `messages`, `feedback`, `turns`, `counts`; `turns_of()` groups records + message payloads + votes into turns (question, status, skills loaded and modes, refused, checks, prompt version, answer, proposal SQL from the message payload first, artifacts, plan, task_done rows, feedback, tokens); `backfill(reader, tracer, since, session_id, owner)` replays each session under its owner and scores its votes; `parse_since`. |
| `sahs/observe/datasets.py` | new: `PRECEDENT_SCHEMA`/`SILVER_SCHEMA`/`SCENARIO_SCHEMA`, `read_precedents` (the documented JSONL shape), `precedent_items`, `is_silver`, `silver_items`, `scenario_items`, `write_jsonl`, `read_items`, `is_item_file`, `push_items`, `items_to_tasks` (precedent/silver → `nl2sql` tasks with the canonical fingerprint accepted and `skill=` tag; scenario → `decompose`). |
| `sahs/observe/coverage.py` | new: `CONCEPTS`, `coverage(reader)`, `format_coverage`, `format_coverage_markdown`. |
| `sahs/observe/experiments.py` | `dataset_name()` names an item file by its schema (`synapse-precedents`, `synapse-silver`, `synapse-scenarios`); `task_item()` passes items through; `read_any_tasks()`; the recorder scores `skill_hit` when the item names a skill and the answer carries `skills`. |
| `sahs/observe/prompts.py` | `part_registry()`: identity, chain, mode blurbs, style per engine family, `PLAN_SYSTEM`, `JUDGE_SYSTEM`, `REVIEW_SYSTEM`; `content_version()` (a text hash as the version string for one-shots with no constant); `registry(parts=True)`; entries carry `labels` (`production`, the family) which `register_prompts` applies; the assembled template gains the `{{style}}` section the real prompt has. |
| `sahs/evals/schema.py` | `TaskKind` + `decompose`; `TaskGold.expected_tasks`, `synthesis`. |
| `sahs/evals/grading.py` | `SutAnswer.kind` + `plan`, `tasks`, `skills`; `grade_decompose` (count → fail; kinds or wave depth → ambiguous; else pass); dispatch. |
| `sahs/evals/suts.py` | the oracle echoes the gold task list on `decompose`. |
| `sahs/evals/assistant_sut.py` | new: `answer_from_events` (proposal SQL, else the `run_sql` the loop called, else abstain; the skills off `skills_loaded`), `assistant_sut(build, model_factory)` (one real turn per task through `AssistantRuntime`), `planner_sut(model)`; both declare `answerable_kinds`. |
| `scripts/langfuse_sync.py` | `backfill --from spanner [--since --session --owner]`; `datasets --build precedents\|silver\|scenarios [--out] [--push] [--precedents] [--since --owner --session]`; `coverage`; `prompts` prints the parts too. |
| `scripts/run_evals.py` | `--tasks` takes item files (`read_any_tasks`); `--sut assistant:<builds>` and `--sut planner` on the engine the `.env` names; `--wait-seconds`. |
| `tests/fixtures/precedents/precedents.jsonl` | three example precedents in the documented shape (nothing was checked in under "precedent"; the research note only asks for the eval). |
| `tests/test_langfuse_insight.py` | new, 16 tests (below). |
| `tests/test_observe.py` | the SDK fixture hands `PINNED` to the client; two assertions widened for the `chips` event and the `refused` score. |
| `docs/runbooks/langfuse-insight.md` | the guide. |
| `docs/runbooks/langfuse.md` | step two names `--from spanner`, the pinned ids, the 90-day window, and links the guide. |
| `apps/synapse_admin/README.md` | one paragraph linking the guide. |
| `apps/synapse_admin/backend/chat.py` | `user_id=runtime.owner_user_id or runtime.user_name`: live traces file under the same id (`ChatSessions.OwnerUserId`) the backfill uses. |

No new environment variable; `.env.example` untouched. No model identifier in code or commits. `sahs/loop/skills.py`, `sahs/assistant/skills_loader.py`, `sahs/assistant/kit.py` and the skills block of `loop.py` were not touched — the one `loop.py` edit is the fingerprint on the system `model_prompt` emit, after `system_prompt()` returns.

## Tests

| suite | command | result |
|---|---|---|
| harness, whole | `cd synapse-agentic-harness-system; PYTHONPATH=. python -m pytest tests -p no:cacheprovider` | `637 passed in 95.95s`, exit=0 |
| app | `PYTHONPATH=synapse-agentic-harness-system python -m pytest -q apps/synapse_admin/tests -p no:cacheprovider` (repo root) | `153 passed, 2 skipped`, exit=0 |
| the new file | `tests/test_langfuse_insight.py` + `tests/test_observe.py` | 37 passed, exit=0 |

What the new tests pin: the fingerprint is stable across two identical
prompts and across two identical live turns on the compiled build with a
`ScriptedAgent`, and moves in the `memory` part only when a memory is added
(the prefix hash and the version string unchanged) and in the version string
when the mode changes; a compound turn is one trace with `task a` / `task b`
spans, the sub-turn's generation and tool under the task span, the planner's
usage as a generation, `tasks_failed` = 1; on the real SDK with the in-memory
exporter the task span's parent is the root and the tool's parent is the task
span, and a second replay yields the same span ids and the same score ids
(three scores, one id each); the reader groups the fake database's rows into
turns (skills, modes, prompt version, proposal SQL from the payload, votes,
task statuses); `backfill` from the fake through `SpannerDatabase` produces
exactly the calls a live tracer makes on the same records, files the traces
under the owner, scores the up and the down vote, is identical on a second
run, and narrows by session, owner and `--since`; precedents load from the
fixture; silver keeps the thumbed-up and the clean done turn and drops the
thumbed-down/refused one; scenarios come from `plan_made`; items become
tasks the oracle passes and the null SUT fails; `grade_decompose`'s three
outcomes; a run on the item file lands `verdict`, `pass` and `skill_hit` on
each item; `answer_from_events`, `planner_sut` on a scripted JSON answer, and
`assistant_sut` running a real turn on the scripted engine; coverage on the
seeded fake and on an empty store; the prompt parts register once with the
labels and a second registration is a no-op; the CLI builds the three item
files without Langfuse, prints coverage, and refuses to push or backfill
without the switch; `run_evals.py` takes an item file.

## Coverage, as computed on the fake

`langfuse_sync.py coverage` on the seeded fake SDK database (two sessions,
three turns, one compound, two votes):

| concept | built from | state | rows |
|---|---|---|---|
| trace | ChatEvents (turn_started … turn_done; TurnId, Ev, Payload) | present | 3 |
| session | ChatSessions.SessionId | present | 2 |
| user | ChatSessions.OwnerUserId → Users.UserId | present | 1 |
| generation | ChatEvents model_prompt (n, kind, content[:12000]) | present | 8 |
| generation usage | ChatEvents budget_tick (tokens_in, tokens_out, tokens: deltas per call) | present | 6 |
| tool span | ChatEvents tool_call / tool_step / tool_result (n, tool, args, summary, ref) | present | 2 |
| task span | ChatEvents task_started / task_done + the sub-turn's records (task, sub_turn) | present | 2 |
| artifact event | ChatEvents artifact (artifact_id, version, type) ≈ ChatArtifacts | present | 1 |
| proposal event | ChatEvents proposal / ChatMessages.Payload.proposal (sql, title, metric_id) | present | 2 |
| chips event | ChatEvents chips (suggestions) | present | 1 |
| score turn_status | ChatEvents turn_done.status | present | 3 |
| score refused | ChatEvents tool_step.summary starting ERROR | present | 1 |
| score tasks_failed | ChatEvents task_done.status | present | 2 |
| score feedback | ChatFeedback (TurnId, Vote, Note) joined by turn | present | 2 |
| prompt version | ChatEvents model_prompt n=0 (prompt_version, prompt_parts) — turns before this build lack it | present | 2 |
| dataset item: silver | turns with ChatFeedback up, or turn_done done and nothing refused; SQL from ChatMessages.Payload.proposal | present | 2 |
| dataset item: scenarios | ChatEvents plan_made (tasks, synthesis, waves) | present | 1 |
| dataset item: precedents | a checked-in JSONL, not Spanner (tests/fixtures/precedents) | not from the rows | 0 |
| experiment run | run_evals.py --langfuse: the harness's verdicts, not Spanner | not from the rows | 0 |
| per-task usage | budget_tick is a session counter; concurrent tasks share it — not attributable by design | not from the rows | 0 |
| full prompt text | model_prompt content is cut at 12 000 chars on purpose; the fingerprint stands for the rest | not from the rows | 0 |
| human labels beyond thumbs | no table: the annotation queue in Langfuse is the only place | not from the rows | 0 |

## Gaps (also in the guide)

- Per-task token attribution: `budget_tick` is the session counter; with two
  tasks side by side the split between overlapping generations is
  approximate, the session total exact. The fix is the model client tagging
  each tick with the call it closed — the loop's change, not the mirror's.
- The prompt text stays truncated at 12 000 characters by design; the
  fingerprint and the registered parts stand for it; the skills section is
  reproducible from the build and the pinned skills, not from the record.
- Human labels beyond thumbs live only in Langfuse's annotation queue until
  `pull-annotations` writes accepts into the task files.
- Cached-token counts are not on the budget tick yet; the cache-hit query in
  the guide waits for `usage_details['cached']`.
- Turns older than the 90-day `ChatEvents` policy are messages and votes
  with no trace; the guide says to backfill within the window or extend the
  policy in `002_chat.sql` — the sync does not extend it.
- Backfilled timing is import time; the record's `elapsed_ms` rides on the
  trace metadata.
- Turns recorded before this build carry no fingerprint and link to the
  prompt by `ASSISTANT_VERSION` alone.
- No separate title/memory one-shot template exists in this tree; the
  registry carries the planner's, judge's and reviewer's one-shots.
- The ClickHouse column names in the guide's five queries are Langfuse v3's
  and are to be checked against the installed release.

## Files outside my lane

- `apps/synapse_admin/backend/chat.py` (one expression: the observer's
  `user_id` is the owner id, falling back to the configured name).
- `apps/synapse_admin/README.md` (one paragraph, the link the task asked for).
- `sahs/assistant/loop.py` (the two-line fingerprint edit described above;
  not in the skills block).
- `sahs/evals/schema.py`, `grading.py`, `suts.py` (the `decompose` kind, the
  `plan` answer and `skills`, all additive).
