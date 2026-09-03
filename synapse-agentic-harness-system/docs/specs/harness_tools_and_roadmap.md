# The harness, tool by tool — what runs today and what to add next

Status: inventory of 2026-09-03, after the handover shipped. Read with
`synapse_v3_harness.md` (the design), `harness-discipline.md` (the ten
principles every PR holds to), and `vocabulary_and_values.md`.

## 1 · What the chat runs today

### 1.1 The v3 kit — 13 tools, one interaction per turn (`sahs/assistant/kit.py`)

| tool | signature | what it does | ends the turn |
| --- | --- | --- | --- |
| `search` | `search(query, kind?)` | the one door into the graph: metrics, concepts, joins, vocabulary, stored values; business areas rank first; `kind=list` is the catalog for an area, `kind=exact` greps the cards, `kind=vocab` expands a scoped acronym, `kind=values` turns a phrase or a stored value into the column, the code and the predicate | no |
| `read` | `read(id, section?, graph_ids?)` | a card whole, with its definition line; or the subgraph behind a set of ids | no |
| `sample_values` | `sample_values(table, column, n?)` | the profiler's observed values with their share of rows, the meanings on record, the distinct estimate and whether the list is partial — never a live query | no |
| `run_sql` | `run_sql(sql, mode?, limit?)` | validates, then prices (`dry_run`: shape and bytes) or runs (`run`: rows under the scan ceiling and the row cap, both disclosed); rows save as `q<N>` for python and check; refusals are taught (cost is the model's to narrow, configuration is reported) | no |
| `propose_sql` | `propose_sql(sql, title, why?, metric_id?)` | the handover: the proved query, priced again, carrying its status and meridian line; the card offers Run query and Run + build dashboard | **yes** |
| `python` | `python(code)` | sandboxed compute over the saved rows and the build (`meridian` shim), files persist per session | no |
| `check` | `check(kind, …)` | six verification kinds (part_whole, crosscheck, coverage, fanout, reconcile, answer) that mint citable facts; a passing fact lifts the EXPLORATORY watermark | no |
| `artifact` | `artifact(type, title, spec_json, artifact_id?)` | chart, table, document, kpi, dashboard, diagram in the panel the user keeps; the validator refuses undisclosed numbers; a new version, never a copy | no |
| `ask` | `ask(question, options)` | one clarifying question with named options and their evidence | **yes** |
| `load_skill` | `load_skill(name)` | a doctrine pack into this turn (the model loads by intent; `/name` in the composer loads one for the turn) | no |
| `remember` | `remember(text, scope?)` | one preference or disambiguation, disclosed inline with an undo, retirable | no |
| `note` | `note(text)` | working notes the next turn reads | no |
| `suggest_next` | `suggest_next(options)` | up to three follow-up chips; after prose it ends the turn | after prose |

**Hooks** (`sahs/assistant/hooks.py`, governance as code rather than
prose): `artifact_schema` (provenance in schema), `sql_gates` (validator,
ACL, cost and access gates), `literal_check` (an unobserved literal or a
meaning-instead-of-code comes back as a warning naming the code),
`rows_to_workspace` (q<N>.json), `warehouse_errors` (a taught error per
warehouse failure class).

**Turn kinds** (`sahs/assistant/loop.py`, `runtime.py`): a model turn in
`chat` mode (hand the query over) or `autopilot` (run and build), a
`clarify` end, a `proposed` end, and the **run turn** —
`run_proposal_turn` executes a handed-over query with no model call and
lands the rows as a table artifact and as `q1`; `dashboard=true` chains a
model turn on autopilot that builds from them.

### 1.2 The v1 loop toolkit — 14 tools, still behind Ask and the evals (`sahs/loop/tools.py`)

`delegate_scout`, `list_tables`, `grep_cards`, `read_card`,
`search_semantics`, `list_metrics`, `resolve`, `sample_values`,
`get_join_paths`, `get_definition_line`, `run_sql`, `plan_set`, `note`,
`ask_user`. The v3 kit wraps nine of them (`search` = search_semantics +
list_metrics + grep_cards; `read` = read_card + get_definition_line;
`run_sql`, `sample_values`, `ask`, `note` pass through). `resolve`,
`get_join_paths`, `plan_set` and `delegate_scout` are not in the chat.

### 1.3 What the surface consumes

The pinned event family: `turn_started`, `model_prompt`, `thinking`,
`tool_call`, `tool_step`, `tool_result`, `say_token`, `artifact`,
`proposal`, `chips`, `budget_tick`, `turn_done`, `error`. The page is a
pure consumer: every event has an arm, every arm has an SSE listener,
the test refuses a new event without both.

### 1.4 Deterministic tooling around the chat (`scripts/`)

`laptop.py` (build-graph, compile, census, enrich…), `turn_doctor.py` (a
stalled turn read from its events), `bq_check.py` and `vertex_check.py`
(the two network planes), `run_evals.py` and `chat_eval.py` (the suites,
including the recovery kind with fault injection), `e19_baseline.py`.

## 2 · What to add, in the order the transcripts argue for

The ground is the two real laptop transcripts (v3 §0), the stall
diagnosis (a model call after a successful dry run with no pulse), and
the ten principles. Each item names its principle and its delete test.

1. **A turn that always finishes — continue and compaction (v3 Stage 2).**
   "Ask me to continue" is prose today. Make it a chip that resumes the
   same interaction on a compacted history: tool results older than N
   calls keep their one-line summary and lose their body, the workspace
   keeps the rows, and a context ledger (tokens per prompt section) is
   visible in Operate. Principle 7 (budget everything in code). Delete
   when turns stop hitting the wall clock.
2. **Plan first, then go (v3 Stage 3).** The handover generalized: a
   `plan(steps)` tool ends the turn with a checklist card and a "Go
   ahead" that continues; plan-first is a toggle beside the mode. The
   v1 `plan_set` slots (metric, table, filters, grain, dims, time,
   checks) are the shape. Principle 1 (a workflow where one will do).
3. **Delegate a scout (v3 Stage 2).** Port `delegate_scout`: a scoped
   sub-turn with `search`, `read`, `sample_values` only, returning a
   brief, so exploration never bloats the main context. Principle 3
   (read the context window: the stalls were long contexts re-sent).
4. **Receipts as a tool.** `receipts()` returns this turn's facts,
   definition lines and queries in one structure the answer and the
   artifact footer cite; today the model assembles it from check results.
   Principle 4 (tools get the care of a UI).
5. **Decode codes in rendered tables** (vocabulary §5). A `decode: true`
   spec flag on tables and dashboards maps a column's codes to the
   meanings on record (`D` shows "Declined (D)"). No new tool: the
   renderer and `domains.jsonl`. Delete when the warehouse stores the
   meanings itself.
6. **A warehouse doctor in the chat.** `warehouse(action="doctor")` runs
   the read-only checks (`bq_check`, `transport_check`, the turn doctor)
   from the thread, so "the check query is not working" gets a diagnosis
   in the chat instead of a laptop paste. Principle 8 (a truthful
   environment before a clever model).
7. **Feedback on every turn** (design doctrine §5): 👍/👎 and a note,
   stored as JSONL with build id, turn, artifact; each becomes an eval
   task or a steward item. A surface affordance, not a tool.
8. **Evals for the new turn kinds** (v3 §8): a `proposed` kind (a data
   ask in chat mode ends with a proposal whose dry run passed and whose
   status matches the metric), a run-turn kind (rows to a table
   artifact under the limits), an autopilot kind, a continue kind.
   Principle 6 (evals are the loss function).
9. **Memory page and behavioral evals (v3 Stage 4)**: the memory panel
   becomes a page with scopes and provenance; tone and warmth are
   launch-gated evals.

Deliberately not added (principles 1, 5, 9): a planner-executor
framework, a vector store over the cards (grep and the indexes suffice
at this scale), one sub-agent per tool, automatic disambiguation of
scoped acronyms (a memory, never a guess).

## 3 · The flow as it stands

```
ask ─► search / read / sample_values ─► run_sql(dry_run) ─► propose_sql
     (chat mode)                                                │
   card: [Run query] [Run + build dashboard] [Edit SQL] ◄───────┘
     │                        │
     ▼                        ▼
   run turn (no model)      run turn, then a model turn on autopilot
   table artifact + q1        dashboard artifact from q1
   chips: Build a dashboard from these rows · Refine the query
```

Autopilot skips the card: the model runs under the limits, checks, and
builds. A `/skill-name` prefix loads that pack for the turn. The limits
(`SAHS_LIVE_MAX_BYTES`, the row cap) hold in every mode.
