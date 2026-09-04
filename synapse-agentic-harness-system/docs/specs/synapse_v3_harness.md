# Synapse v3 — the thin harness, the Karpathy way

Status: DESIGN, Stage 1 BUILT (2026-09-02). Applies the Gemini 3.1
Pro harness research (Cherny / Model Spec / Karpathy) to Synapse.
Stage 1 of §10 is in the tree (`sahs/assistant/{agent,loop,kit,
hooks,state}.py`, the surface in `apps/lumi/frontend/js/pages/chat.js`);
Stages 2–4 are not. v2's checks, artifacts, store, events, and evals
stay; the turn loop, the kit, and the surface changed.

## 0 · What the two real transcripts proved

- The ALIF turn died because the harness demanded strict JSON per
  step and capped each call at 1200 output tokens. A long answer with
  SQL in it cannot survive either. The model had the answer.
- The six repeated lookups happened because per-result compaction
  starved the model: a whole card reached it as four lines, a search
  as three forty-character labels. It re-read the same card three
  times because it never saw it.
- The thinking line and the artifact panel never hid: `display:flex`
  in the stylesheet overrides the HTML `hidden` attribute, and the
  browser walk asserted the property, not visibility.
- Seven model calls per answer, each with full thinking, is why a
  simple question took 35 seconds.

Every one of these is harness, not model. "Never bet against the
model" is the operating rule from here.

## 1 · Layer 0 — system prompt and context (press delete)

Delete: the strict-JSON PROTOCOL, the strikes, the per-step prompt
rebuild ("CONVERSATION SO FAR / STEPS THIS TURN / BUDGET"), the RULES
prose that repeats tool descriptions, the `think` field. Keep, as a
stable cacheable prefix (≥4,096 tokens so implicit caching engages):

```
<identity>     Synapse, an analytical colleague. Warm, brief, plain.
               Numbers come from tools; reasoning comes from you.
<chain>        platform (governance, immutable) > Lumi (product) >
               the user's memory and asks > defaults
<business_map> the LOB rows, metric counts, tables   (data, stable)
<shelf>        top metrics · concepts · skills by name  (data, stable)
<memory>       what is remembered about this user      (per user)
<tools>        the schemas, generated                  (stable)
```

Then the conversation as messages. The user's newest ask is always
LAST ("instructions after data"), which Gemini 3.1 wants and which
the message shape gives for free. Temperature stays at the model
default; we ran 0.0 and that is a documented cause of looping.

The governance rules do not live in prose any more. They are hooks
(§4). Prose only steers preferences the model cannot know: tone,
the disclosure sentence shape, when to ask.

## 2 · Layer 1 — tools: 33 → 11, sharp and non-overlapping

| v3 tool | absorbs from v2 | notes |
| --- | --- | --- |
| `search(query, kind?)` | search_semantics, grep_cards, list_metrics, resolve | one door; business areas rank first; `kind=list` for "all X metrics"; exact-token mode inside; `kind=vocab` expands an acronym with its scope (common-word guard); `kind=values` turns a phrase into the stored code and predicate (docs/specs/vocabulary_and_values.md) |
| `read(id, section?)` | read_card, get_definition_line, get_join_paths, subgraph | whole card by default; joins are in the table card; `read(ids=[…], as="graph")` returns the subgraph |
| `sample_values(table, column)` | same | unchanged |
| `run_sql(sql, mode?)` | run_sql, whatif | the model rewrites SQL itself; rows auto-save as q<N> |
| `python(code)` | python, compare | aligning two frames is three lines of python |
| `check(kind, …)` | check_part_whole, check_crosscheck, check_coverage, check_fanout, check_reconcile, verify_answer | one tool, kind enum; returns the same citable FACT |
| `artifact(type, title, spec, artifact_id?)` | artifact, artifact_update, list_artifacts, constellation | id present = new version; a diagram of the subgraph is `read(as="graph")` + `artifact(type="diagram")` |
| `ask(question, options?)` | ask_user | the Model Spec clarify switch, interactive=true |
| `load_skill(name)` | list_skills | the shelf is in the prefix |
| `remember(text, scope?)` | memories, forget | listing is in the prefix; retiring is the memory page |
| `note(text)` | plan_set, note | structured note-taking; persists across turns |
| `delegate(brief)` | delegate_scout | a sub-agent with an isolated context; returns a summary. Stage 2. |

Descriptions carry the "when", in one or two sentences. No tool
description repeats what another tool does.

## 3 · Layer 2 — context management

- Transport: our proven REST client (`sahs/enrich/client.py`: urllib,
  SA token, corporate-TLS trust, no SDK) extended to speak native
  function calling on `streamGenerateContent`: `tools` with function
  declarations, `functionCall` parts in, `functionResponse` parts
  back, `thoughtSignature` carried opaquely on every part we echo,
  `thinking_level` set per turn. Client-managed history, so our store
  stays the single system of record and the same transport serves the
  scripted test double. The Interactions API is a Stage 2 option if
  background execution or server-side history earns its place; it
  would need the google-genai SDK and a second pass at the laptop's
  TLS trust, which is why it is not Stage 1.
- Tool results go back **whole**, capped at ~20K characters per
  result with an explicit "truncated: read(section=…) for the rest".
- Compaction only under pressure: at ~150K tokens (well under the
  200K pricing cliff) summarize the trajectory — decisions, open
  questions, artifacts, notes — and start a new interaction chain
  from that summary plus the last few turns. Tune for recall first.
- The scout (`delegate`) is the LLM-OS "process": noisy exploration
  in its own window, a summary back.
- Metrics per turn: tokens in/out, cached tokens, thinking level,
  tool calls, checks passed, asks, stops, wall time. Surfaced in
  Operate, not in the chat.

## 4 · Layer 3 — verification first, enforced by hooks not prose

What we already have becomes the hooks, named as such:

| hook | kind | what it enforces |
| --- | --- | --- |
| artifact schema | PreToolUse on `artifact` | rule 1 and rule 2: disclosure, watermark, certified-needs-certified-metric |
| cost + ACL gates | PreToolUse on `run_sql` | the two cost gates, live-mode policy |
| literal check | PreToolUse on `run_sql` (new) | a WHERE literal that is not in observed values comes back as a warning with the three closest real ones — deterministic, no prose rule needed |
| rows to workspace | PostToolUse on `run_sql` | q<N> saved for python and check |
| warehouse errors | PostToolUse on `run_sql` (new) | a failed dry run or execution comes back classified — `sql` or `cost` is the model's to fix (the closest real names ride along), `environment` or `access` is configuration to report with the exact `.env` change — and the recovery eval suite grades both behaviours |
| clerk-only truth | absent tool | nothing writes to the graph from a chat |

`check` is the generation-verification loop the model runs itself.
"Read before use" and "sample before filter" leave the prompt: the
literal hook and the card-shaped tools make the right path the easy
path.

## 5 · Layer 4 — the autonomy slider, legibly

- **Depth dial** in the composer: Quick (thinking low), Standard
  (medium, default), Deep (high). A lightweight router proposes the
  level from the ask (chat vs data vs "why") and the dial overrides
  it; the chosen level is visible on the turn.
- **The mode, two positions (BUILT 2026-09-03).** *Chat* (default):
  the model proves a query with a dry run and hands it over with
  `propose_sql`; the card carries the SQL, its price, its status and
  meridian line, and offers **Run query**, **Run + build dashboard**
  and **Edit SQL**. Run executes with no model call
  (`run_proposal_turn`): the rows land as a table artifact and as
  `q1`, the receipts stream as prose, the chips offer a dashboard
  from the rows; Run + build dashboard chains a model turn on
  autopilot that builds from `q1`. *Autopilot*: the model runs under
  the limits, checks, and builds without stopping. The mode is a
  prompt section, not a gate: the limits hold in both. `/skill-name`
  in the composer loads that pack for the turn.
- **Query first, rows on a tap, the first picture without the
  model.** In Chat mode a data question ends with `propose_sql`: the
  query on a card with its price, status and meridian line, and Run
  query / Run + build dashboard / Edit SQL. The run turn executes with
  no model call and lands the rows as a table and as q1; "Chart these
  rows" is an action chip that draws them with no model call either,
  under the run's provenance (x = the first date-like or text column,
  the numeric columns as series, a line on a date axis, a bar
  otherwise); "Build a dashboard from these rows" is the model's turn,
  on autopilot. Only the showcase and the designed dashboard need the
  model; execution and the first picture never wait on it.
- **Plan-first toggle** (Stage 3): when on, the model writes its plan
  as prose and stops before the first `run_sql`; "go ahead" continues.
  The handover above is its first, narrow form.
- Irreversible actions do not exist in a chat (read-only graph,
  sandboxed SQL, artifacts are versions). Live warehouse execution
  stays behind its cost tiers and is the chat's `run` mode: a scan
  ceiling in bytes and a row cap the model cannot lift, both disclosed
  on every result; refused for cost, the model narrows the scan;
  refused as disabled or restricted, it reports configuration. Memory
  writes disclose and undo.
- Budgets become a wall clock and a session cost ceiling, both
  generous, both visible. A limit ends the turn gracefully with what
  was already said and a "continue" that resumes the same
  interaction. Streamed text is never discarded.

## 6 · Layer 5 — the conversational surface

- Text streams as it arrives. Function-call arguments stream too, so
  the activity line can say "Reading the Submitter Active Locations
  card…" before the call returns.
- **The thinking block, the way Claude shows it.** Under the user's
  message, one block streams the model's own thought summaries in
  the order they happen, interleaved with the steps ("Searching the
  graph for enrolments — 16 results"); its header is the live line
  (the latest thought, or a plain verb, with the seconds ticking).
  When the answer starts it folds to "Thought for 34s · searched the
  graph, read the cards" and stays expandable; new work reopens it.
  The trace is kept with the message, so a reload shows every turn's
  thinking folded, and a turn that used no tools and thought nothing
  shows no block. Verbs deduplicated; no tool names, no ids, no raw
  output. The prompts and full results still live in Operate →
  Transcripts for the weekly reading ritual.
- The thinking line stops the moment final text starts and never
  returns after the turn. `[hidden] { display: none !important }`
  globally; the walk asserts visibility. The line has a heartbeat:
  the seconds tick, and each model call restarts the clock, so a long
  think never wears a tool's name ("Checking the query…" while the
  model is the one working was the first laptop stall).
- **A turn belongs to the server, not the tab.** Leaving the page
  closes only the listener; coming back to a session mid-turn
  reattaches from the turn's first event (`turn_after` on the session
  GET) and replays it whole, so switching chats or tabs never stops or
  loses a turn. The shelf marks a working chat.
- **Artifact panel is model-invoked, and it is a drawer.** It opens
  on an artifact in this interaction, never on reopening an old chat;
  a card in the transcript reopens it; closing reflows the chat to
  full width. It slides in from the right edge and moves the chat to
  the middle (reduced-motion parity), the artifact shows from its top
  every time, and the card it came from lights up. A table is a
  report: a summary strip of the data pool first — the rows and their
  date span, each numeric column's total, range and mean with its
  shape — then the rows with a sticky header, formatted numbers and
  the first fifty shown; every number in the strip is computed from
  the artifact's own rows. Tiles count up once, charts draw
  themselves in, dashboard tiles arrive in order. The masthead
  carries the title and Share, never tokens or a build id: the build
  travels on every artifact's footer.
- **Clarify vs proceed** (Model Spec): interactive=true, so `ask`
  when the ask is markedly unclear, and the prompt says so
  explicitly because models "know but don't show".
- Tone: warmth is steered explicitly and is a launch-gated eval.
- No "what the model saw" in the chat. No harness sentences in the
  user's language ("strict JSON" never appears again).
- Chips: model-authored, at most three, optional. (Decision below.)
- **The ask starts the way Claude's does (BUILT 2026-09-03).** A
  greeting for the time of day (the person's first name when
  `LUMI_USER_NAME` is set), a centered composer ("Type / for skills"),
  the + menu (skills, memory, a new chat), the Chat | Autopilot
  toggle, the model and the depth. The first message turns it into
  the conversation: the title (a click renames it) and Share at the
  top, the thread, the composer docked with the disclaimer under it.
  No dead controls: no microphone, no upload, until they do something.

## 7 · Memory, bound to the person

- Memory belongs to the user (Saheb Singh), lives under the account
  block in the sidenav, on by default.
- Three writers: the model when a preference or disambiguation is
  stated; a **post-turn memory pass** (a Flash-tier call, thinking
  low) that proposes durable preferences from the turn; the user
  directly on the memory page.
- Every save discloses inline ("Remembered: by spend you mean
  acquirer net spend · undo") and retiring strikes through, never
  deletes. Never a metric definition; those go through the steward.
- The chain of command puts platform governance above memory, so a
  remembered preference can never soften a rendering rule.

## 8 · Evals: behavior is the UI

Keep the three v2 suites and add behavioral, launch-gating evals:
sycophancy (a wrong premise stated confidently), over-assumption
(an ambiguous ask that should clarify), format adherence, tone. Add
per-turn instrumentation (§3). Before every ship, one real
conversation read against the experience rubric; the scripted walk
proves plumbing only.

## 9 · Press delete — the inventory

Removed outright: strict-JSON protocol, strikes, 1200-token cap,
24-step cap, per-result compaction, the `think` field, RULES prose,
"what the model saw" in chat, 22 tools (see §2), projects UI,
skills picker. Kept because measured: the checks, the rendering
rules, the business map, the skill packs, memory scoping, the
handoff, the eval suites.

## 10 · Build order

1. **Transport + loop — BUILT.** Native function calling and
   streaming on the REST client (`VertexClient.converse`, thought
   signatures echoed verbatim, `thinkingLevel` per turn); the
   11-tool kit plus `suggest_next` (`kit.py`, schemas declared to
   the transport, never pasted in the prompt); results back WHOLE
   under a 20K-character cap with an explicit note; hooks named
   (`hooks.py`, the literal check new); the prompt as sections
   (`<identity> <chain> <graph> <skills> <memory> <session>`), the
   conversation as messages, newest ask last; limits are a wall
   clock, a 40-call ceiling, and a generous session breaker, each
   ending the turn in plain language; the two visibility bugs fixed
   (`[hidden] { display: none !important }`) and the walk asserting
   visibility; the scripted test transport emits parts. Still owed
   from the threshold: the GMNS and ALIF asks completing in one
   interaction against Vertex — the laptop paste decides.
2. **Context.** Cached prefix, compaction under pressure, delegate.
   Threshold: a 30-turn session survives; cache hits and cost per
   turn visible in Operate.
3. **Autonomy.** Depth dial with router and override, plan-first,
   graceful limits with continue. Threshold: depth changes latency
   measurably; nothing irreversible ever auto-runs.
4. **Polish + behavioral evals.** Activity summaries, memory pass
   and page, clarify-vs-proceed, tone; sycophancy and over-assumption
   evals gating; per-turn metrics. Threshold: evals green on
   behavior and the real transcript feels right to us.

## 11 · Decisions taken at "let's get started"

1. Stage 1 transport: native function calling on our REST client
   with client-managed history (built). The Interactions API stays
   a Stage 2 option.
2. The tool consolidation and deletions in §2, as listed; `delegate`
   waits for Stage 2. `suggest_next` is the twelfth declaration: a
   model with no JSON wrapper needs a door for follow-ups, and
   calling it after the answer ends the turn without another call.
3. Memory: silent save with inline undo ("Remembered: … · undo" in
   the transcript, the count on the memory button, retirable in the
   panel). The memory page under the account block is Stage 4.
4. Chips: model-authored, at most three.
5. Depth dial default Standard (medium); Quick = low, Deep = high,
   in the composer now, the router in Stage 3. The judge runs on
   Pro at thinking low until the org allows a Flash model (§12).
6. Model id: `gemini-3.1-pro-preview` by default (`sahs/util/auth.py`),
   overridable by `VERTEX_MODEL`; confirm the laptop .env does not
   pin an older id.
7. Out of scope for now, by the user's word: jobs_30d mining and
   cost priors.
8. Rows on the laptop come from live execution under the gates
   (`SAHS_ALLOW_LIVE=1`, `SAHS_LIVE_MAX_BYTES`, the row cap); the
   frozen-snapshot runner stays an eval fixture. The step row shows
   the SQL that ran.
9. SQL first (2026-09-03). A data question in Chat mode ends with
   the query on a card, not with rows: the person presses Run, and
   the run needs no model. Reason: every laptop stall came after the
   model already had the query; the handover makes the query the
   deliverable and the rows a tap. Autopilot keeps the old behavior
   for those who want it.
10. Routes are pinned per connection (2026-09-03). The stall after
   every first dry run was the BigQuery plane writing googleapis.com
   into the process's NO_PROXY, which rerouted every later Vertex
   call direct into the corporate blackhole. Each connection now
   carries its own proxies and opener; nothing writes the
   environment; the doctor prints both routes first. Reason: two
   planes with opposite network contracts cannot share a
   process-wide switch.
11. The measures catalog is the fifth join family (2026-09-03). Its
   `joined_tables` become `joins_via` edges with witness
   `catalog_mined`: WHICH tables a metric's real queries join, FOR
   WHAT metric, how many users — never HOW, because the catalog does
   not record the ON condition. A catalog row therefore never tiers
   above candidate, cannot vouch for a fan-out, and annotates the
   measured, declared and studio rows for the same pair
   (`also_in_catalog`) as they annotate it (`also_witnessed_by`).
   The rest of the catalog's texture (group-by patterns, executions,
   miner confidence, business unit, category) rides the metric row,
   its card, the search hit as `dimensions`, and the digest's
   "usually by" note. Reason: the audit found the joins and the usage
   fields loaded onto the metric node and dropped at compile; the
   agent was reading a poorer catalog than the one on disk.
12. The model is told what day it is (2026-09-03). The session
   section, the last one so the cached prefix holds within a day,
   opens with today's date, the periods "this month", "last month",
   "this quarter", "last quarter" and "year to date" resolve to, and
   the newest partition the build saw, read as a date. The identity
   says relative words resolve against that date, never against the
   model's own sense of now. Reason: a model has no clock; the first
   dashboards were windowed against its training-time present.
13. The product says "published" where the graph says certified
   (2026-09-03). One label map in the frontend's shared helpers
   renders the chip word; the CSS class, the filter key, the API
   value and the model's disclosure vocabulary stay `certified`, so
   nothing in the graph, the evals or the prose contract moves.
   Reason: "published" is the Data Marketplace's word for a steward-
   approved definition, and the chip is read by the people who use
   that word.
14. A second surface, Synapse Semantic Intelligence (2026-09-03), a
   duplicate of the frontend under `apps/synapse/frontend`, served at
   `/synapse/` by the same server against the same API and build.
   Stripped for the people who ask: New chat and Search chats at the
   top, the recent chats, then Data Products, Metrics Explorer and
   Artifacts in their own section above the account; no Home,
   Skills, Cosmos or Operate. Search chats is a page (every chat,
   fuzzy, the matched lines lit; `sahs/assistant/search.py` is the
   finder). Data Products and Metrics Explorer are cards carrying the
   description, stewardship, size, freshness, metrics and joins the
   build already holds. Artifacts publish inside the chat where the
   turn made them; the drawer belongs to the admin surface. Reason:
   the user asked for a duplicate to strip, so the admin console
   keeps its full nav and the asking surface loses what asking never
   needs; the two do not share files, so a change to one is a change
   to one.

## 12 · The laptop, measured (state report of 2026-09-02)

What v3 is actually built against — not the fixture.

- **Vertex, confirmed for Stage 1:** project prj-d-ea-poc, location
  global, `gemini-3.1-pro-preview` behind the proxy with truststore.
  Native function calling works on our REST client and the
  `functionCall` part comes back WITH a thought signature; thinking
  level `low` is accepted (lowercase) and `includeThoughts` returns
  thought summaries; SSE streaming works. A trivial Pro call costs
  ~3 s and ~140 thought tokens — one more reason a turn must be one
  interaction, not seven.
- **No Flash model is usable:** 3.7 / 3.6 / 3.5 / 3.5-lite / 3-preview
  / 2.5 / 2.5-lite all answer HTTP 400 "Organization Policy
  constraint" (they exist; the project may not use them);
  3.1-flash ids do not exist. Ask the org admin to allow one under
  `constraints/vertexai.allowedModels`. Until then the memory pass
  and the judge run on Pro at thinking low.
- **The graph:** 3,146 metric nodes, 60,502 columns over 268 table
  nodes, 8,645 concepts, 25,483 vocabulary terms, 14,886 column
  domains; witnesses bq 55k, atlas 21k, catalog_mined 9k, snippet
  8.6k, dmp 183, steward 100, studio 47 — and **zero jobs_30d
  quads**: the 30-day query history has never been mined on this
  graph, which is why the build has 2 joins (both candidate) and 0
  cost priors. That extraction is the single biggest data gap for
  join safety, recency, and cost gates.
- **The build (b_d552bcfa5829, first promotion):** 45 of 46 crosswalk
  tables, 3,074 metrics of which **34 certified and 3,040
  unreviewed** (usage-mined), 8,666 bindings, 8 business areas with
  tables mapped for ETS (4), Finance (22), GMNS (10), USCS (10) and
  none for AET / CFR / EDDS / TLS; only 35 metrics carry a business
  area on the row itself. So "all GMNS metrics" must mean *metrics
  on GMNS tables* (area → tables → metrics), ranked certified first,
  not the 13 rows that happen to carry the LOB string — a Stage 1
  change to `search`.
- **Enrichment:** five blind-gate runs climbing 56% → 76% (item tier,
  6 leaky contexts each time); 50 metrics carry an LLM-enriched
  question and grain; 35 have a description. The batch gate (80%)
  is not yet met.
- **Housekeeping the report exposed:** the checkout lives in the
  OneDrive-synced desktop, so the SA key and `.env` must stay
  outside it (`~/.gcp/`); 1,706 tickets and 0 reviews means the
  steward door has never been opened on this graph.
