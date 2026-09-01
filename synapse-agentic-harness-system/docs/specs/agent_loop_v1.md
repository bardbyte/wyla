# Synapse Agent Loop v1 — the operational spec

> Landed verbatim as the governing harness design (the author's
> words below, unedited). Its §9 build order supersedes E21
> Step 4's explore.py and re-sequences the remaining E21 steps
> under the loop. The tool descriptions in §3 are shipped
> character-for-character by `sahs/loop/tools.py` and pinned by
> `tests/test_loop_tools.py`.

How the agent actually finds the right answer. Written at the level Claude Code is built at: a loop, a system prompt, tools with real descriptions, and traces.
0 · The one-sentence design
Every turn is the agent loop: the model drives tool calls over the cards, indexes, and snapshot until it can answer, must ask, or honestly stops — with determinism living inside the tools, and the contract, verifier, budgets, and disclosure as the harness around it.
Claude Code's insight is that the model is the planner and the navigator — it reads a result and decides what to look at next. There is no second, shortcut implementation of that decision. One door, one behavior, one place a bug can live.
1 · One path

```
turn(user_msg, session):
    return agent_loop(user_msg, session)   # every turn, every kind

```

Determinism is not lost — it is relocated into the tools. `resolve` is still the same scored, lexicographic binder, still refusing to guess below margin; the model chooses when to call it, never how it decides. `plan_set` still runs the typechecker; `apply_edit` semantics still enforce single-slot mutation. Same graph + same build + same tool calls ⇒ same bindings, every time. What the model adds is judgment about what to look at next — which is precisely the part code was bad at.
What this costs, stated plainly: a bound-in-one-hop question now costs a model round-trip instead of 0.6ms, and turn cost rises. Mitigations, pinned: prompt-cache the system prompt + world digest + tool schemas (they're identical across turns); keep the loop's first step cheap by putting `resolve` and the current plan in the opening context so trivial turns finish in one tool call and one final; measure p50 turn latency as a first-class eval metric with a target (<3s for single-hop, <15s for navigation). If latency becomes the binding constraint, the answer is a faster model or a cached first hop — not a second code path.
The resolver floor stays measurable as a component eval (resolve() alone against binding tasks) even though it is no longer a serving path. Keep publishing it: it's how you know whether a regression is in the binder or in the navigation.
2 · The loop (this is the whole harness)

```python
def agent_loop(msg, session):
    ctx = Context(system=SYSTEM_PROMPT, digest=build.synapse_md,
                  skills=session.skills, plan=session.plan, msg=msg)
    budget = Budget(tool_calls=24, tokens=..., wall=..., cost=...)
    while budget.ok():
        step = model(ctx)                       # returns thinking + tool_call | final
        if step.final:
            return finish(step.plan)            # contract/verify/render — same as fast path
        result = tools[step.tool](**step.args)  # every tool is read-only except plan/note/ask
        ctx.append(step, compact(result))       # raw result → artifact, ref stays in context
        if step.tool == "ask_user":
            return await_user(step)             # turn ends; chip answer resumes the loop
        budget.charge(step, result)
    return finish_partial(ctx)                  # budget_grace: say what it found and didn't

```

Four properties carried over from Claude Code, deliberately: the model sees tool results and chooses the next action (no fixed order); tools are primitive and composable; results are compacted into artifacts so 24 calls don't rot the context; and the loop ends in one of three honest states — answer, ask, or "here's what I found and where I stopped."
3 · The tools, as the agent sees them (descriptions are the product)
Claude Code's Glob/Grep/Read/Bash/TodoWrite/Task map onto the graph one-for-one. Each tool below ships with exactly this description; each error message names the correct next call.

```
list_tables(domain?, lob?)         # Glob
  Lists governed tables with one-line purpose, row count, readiness, owner.
  Use first when you don't know which table holds the concept.

grep_cards(pattern, scope=all|tables|metrics|concepts)   # Grep
  Literal/regex search across every compiled card line. Returns card id, line, [prov].
  Fast and exact. Use to find where a word, column, or code appears before reading.

read_card(id, section?)            # Read
  Returns a card (or one section). Every line carries its witness tag.
  Read a table card before touching its columns; read a metric card before using it.

search_semantics(query, kind?)     # ranked, fuzzy — the index, not the text
  Ranked metrics/concepts/joins/vocab with status, support, agreement, aliases.
  Use for meaning ("spend", "SMB"); use grep_cards for exact tokens.

resolve(text, table?)              # the deterministic binder, callable mid-loop
  Binds words to governed metrics/concepts with confidence + candidates. Never guesses;
  returns ambiguities you may settle by evidence or by asking.

sample_values(table, column, n=20) # observed low-cardinality domain, never live
get_join_paths(tables[])           # tiered: certified / witnessed / candidate / none
get_definition_line(metric, variant?)

run_sql(sql, mode=dry_run|snapshot, limit=200)   # Bash, fenced
  Validates, then dry-runs or runs on the frozen snapshot under cost gates and ACL.
  Returns schema, rows, bytes. Errors teach: unknown column → the 3 closest real ones.

plan_set(patch)                    # TodoWrite — the ONLY state the loop writes
  Update the session's semantic plan (metric, table, filters, grain, dims, time, checks).
  The plan is what gets verified and disclosed; keep it current as you learn.

note(text)                         # scratchpad artifact, never in prompt after compaction
ask_user(question, options[])      # one question, named options with evidence; ends the turn
delegate_scout(question)           # Task — read-only worker, ≤400-token summary, hard cap

```

Not tools: `verify` (the harness runs it on the final plan, fresh context), `write anything to truth` (clerk only), `execute live` (snapshot only in the loop).
4 · The system prompt architecture (~2.5K tokens, versioned like code)

1. Identity & job (150 tokens): a conversational analytics agent over a governed graph; answers carry receipts; asking once beats guessing; never invents a table, column, or metric — if it isn't in a card or the index, it doesn't exist for you.
2. World digest (`SYNAPSE.md`, ≤2K): build id, top metrics + grains, geography concepts, join topology, conventions, known gaps.
3. How to find things (400 tokens) — the search doctrine, as behavior:
   * start with `resolve`; trust what it binds, investigate what it doesn't
   * `search_semantics` for meaning → `grep_cards` for exact tokens → `read_card` before use
   * `sample_values` before writing any filter literal; `get_join_paths` before any join
   * prefer certified → pending (say so) → composed from real atoms (say so, reconcile)
   * `plan_set` as you learn; `note` what you ruled out and why
   * `run_sql dry_run` to check shape; `snapshot` only when the plan is set
   * ask once, with options that carry evidence; stop and say what you found if budget ends
4. Stop conditions (100 tokens): finish when the plan has metric, table, grain, filters bound, and checks written; ask when two candidates remain within margin; stop honestly at budget.
5. Two short traces (few-shot, ~500 tokens): one good navigation, one wrong turn caught by evidence (the model learns the shape of good behavior from examples, not rules).
6. Tone (E22): warm, brief, colleague; no mystique; numbers only from tools.

5 · A trace — how it finds the right answer
User: "why did approval rate drop for SMB merchants in Canada last quarter?"

```
fast_path: approval_rate → metric certified ✓; "SMB" → 2 concept candidates (margin .04);
           "Canada" → country_cd ambiguity (merchant vs cardmember); "drop" → analysis
           → confidence below τ → agent_loop

[think] two ambiguities + a diagnostic ask. Settle by evidence before asking.
grep_cards("SMB")                → 3 hits: concept smb_segment (2 bindings), card gcs_txn L41
read_card("concept:smb_segment") → binds segment_cd='S' on gcs_transactions ●x917;
                                    seg_flag='SMB' on cfr_accounts ◐x33 (conflict cell #204)
[think] approval_rate lives on gcs_transactions; the certified SMB binding is on that table.
        No need to ask — evidence settles it. Canada is genuinely ambiguous.
ask_user("Canada by merchant country or cardmember country?",
         [merchant_country ●x412 (approval_rate is a merchant metric), cardmember ◆x88])
── turn ends; user taps merchant ──
plan_set(metric=approval_rate, table=gcs_transactions, filters=[smb ●, merchant_ctry=CA ●],
         time=last_quarter vs prior, grain=transaction, dims=[month])
sample_values("gcs_transactions","merchant_ctry") → 'CA' present ✓
read_card("metric:approval_rate", "definition") → approved/decisioned, denominator decisioned
[think] "why did it drop" = decompose: numerator vs denominator move, and by sub-dimension.
        Diagnostic → exploratory lane per ladder; checks first.
plan_set(checks=[part/whole by month, ratio reconciles to certified quarterly, coverage>0])
run_sql(dry_run: monthly approval_rate CA SMB last 2 quarters) → schema ok, 1.1 GB
run_sql(snapshot: same)              → 6 rows; Q drop concentrated in month 2
run_sql(snapshot: by channel)        → CNP channel −9pts; CP flat
note("drop is CNP-driven, month 2; denominators stable → numerator effect")
[final] plan complete; findings labeled exploratory; headline number governed
→ finish(): contract (5 promises + 3 checks) → verify (fresh ctx) → render:
   answer card: approval_rate CA·SMB Q2 68.1% (Q1 74.3%) · MERIDIAN LINE certified ·
   verdict 8/8 · notebook: decomposition (EXPLORATORY) · subgraph_used: 7 nodes

```

Eleven tool calls, one question, every literal checked against observed values, every join avoided because evidence made it unnecessary, every number through the verifier. That is what "go find the right answer" looks like — and nothing in it is a fixed pipeline.
6 · Context management (how 24 calls don't rot the window)

* Each tool result is stored as an artifact; the context keeps a ≤3-line summary + ref. `read_card` keeps the used section verbatim; the rest is a ref.
* The plan and notes are re-injected fresh at each step (they are the state).
* On budget pressure: compact history to {plan, notes, last 2 results}; never drop the plan.
* Scout summaries arrive already compact.

7 · Where the guardrails sit (unchanged, relocated)

* Typecheck runs on every `plan_set` (teaching errors come back as tool results — the model fixes its own plan, Claude-Code style).
* Contract + verifier run once, on `final`, fresh context, default-FAIL.
* Budgets in the loop; `ask_user` ends the turn; scout is the only delegation.
* Disclosure by schema: meridian line, grain, subgraph_used (every card read and every binding used is recorded automatically — the trace is the sub-graph).
* Floor: persona floor filters what `search_semantics`/`resolve` may return; execs never see pending as candidates unless allowed.

8 · Evals for a loop (grade outcomes, read trajectories)

* Outcome: final plan/SQL/number vs gold (E19); never the tool sequence.
* Trajectory hygiene (soft): tool calls per task, asks per task, literal-check rate (did it sample before filtering), read-before-use rate, budget stops.
* Navigation tasks: questions whose answer lives in a card the fast path can't bind — graded on whether the loop found it (recall) and didn't drag in wrong tables (precision).
* The ritual: 20 trajectories read weekly; every bad turn becomes a system-prompt example or a tool-description fix — Claude Code improves its prompt and tools from transcripts, and so will this.

9 · Build order (replaces E21 Step 4's "explore.py")

1. Tools with descriptions + error teaching (`grep_cards`, `list_tables`, `read_card`, `plan_set`, `note`, `ask_user`, `run_sql`) — one afternoon each, tested by the new-hire test.
2. The loop + budget + compaction, wired to the existing `finish()` (contract/verify/render).
3. System prompt v1 with the two traces; "What the model saw" panel to read it live.
4. Navigation task set (30) + trajectory reading; iterate prompt and tools from what you see.
5. Scout as `delegate_scout`. Then the exploratory lane is just the loop with `snapshot` on.

10 · Why this is the Claude-Code-class design
Claude Code is good because the model can look, cheaply and truthfully, as many times as it needs, and because the harness verifies rather than trusts. Everything here is that: cards are the files, indexes are the grep, the snapshot is the test runner, the plan is the todo list, the verifier is CI, and the model is the engineer. The fast path is just the case where the engineer already knows.

I want us to build the same and this should be our agent harness behavior and design