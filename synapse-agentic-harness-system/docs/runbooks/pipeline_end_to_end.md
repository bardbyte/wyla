# Laptop end-to-end — where everything lands, and the whole sequence

The one-page map. Detail per phase lives in `p0_census.md` /
`p1_ground.md` / `p2_build.md` / `p3_tools.md`; this page is the
directory contract and the order of operations.

Nothing is hardwired to absolute paths — every subcommand takes explicit
path flags. Two kinds of input:

- **the two archives you already have** — used exactly as they sit on
  disk; never restructure them;
- **the ten semantic sources** — must sit together in ONE folder under
  the EXACT names below (each loader discovers by name; a misspelled
  file is silently skipped, except the gold set which hard-fails
  `make-tasks`).

## Input directory map (outside the repo — anywhere you like)

```
~/meridian-data/                       # example root; flags point at it
├── real_extractions_production/       # BQ archive — AS-IS
│   ├── _batch_summary.csv             # ← doubles as --registry
│   ├── _run_report.json
│   ├── _shared/   _history/
│   └── <table_name>/                  # × 46 — the 00–17 artifacts
├── mdm_46_patched_v2/                 # MDM archive — AS-IS
│   ├── run_manifest.json  coverage.json  table_summaries.json
│   └── tables/<table_name>/           # summary.json + responses/*.json
└── sources/                           # the ten semantic sources
    ├── blue_business_insights.csv     # ~35.7K mined snippets
    ├── extracted_gold_queries.json    # 158 gold pairs
    ├── measures_catalog.json          # mined measures
    ├── metrics_dmp.json               # DMP certified metrics
    ├── extended_gmns_semantics.json   # GMNS pending metrics
    ├── studio_results_*.csv           # raw Studio catalog export(s) —
    │   # consumed whole: metric rows fuse onto canonical ids, the full
    │   # SQL rides as doc evidence, joins are mined in-silo (scoped)
    ├── data_cleaned.csv               # enterprise glossary
    ├── business_terms.csv             # Atlas/Collibra terms
    ├── std_tech_metadata/             # 46 per-table Atlas JSONs …
    │   └── <table>.json               # … OR one combined export:
    ├── std_tech_metadata_all.json     # accepted as an alternative —
    │                                  # and it WINS when both exist
    └── skills/                        # packs: dirs holding skill.yaml
        └── <PackName>/                # (flat or one level nested)
            skill.yaml, metric_contracts.yaml, …
```

The TLS rulebook is deliberately NOT parsed (doc evidence later); it can
sit in `sources/` harmlessly. `--registry` accepts either the archive's
`_batch_summary.csv` (column `table` or `table_name`) or a plain text
file with one table name per line.

## Output directory map (inside the repo — committed)

```
synapse-agentic-harness-system/
├── graph/                             # L2 truth store (build-graph)
│   ├── identity/crosswalk.jsonl       # YOU author — 46 verified rows
│   ├── identity/aliases.jsonl         # YOU author — alternative names
│   │   # (data-product display names, skill-pack nicknames) mapping
│   │   # onto crosswalk rows: {"alias": ..., "physical": "dw.<t>", …}.
│   │   # An alias to a non-crosswalk physical refuses to load.
│   ├── identity/lob_map.jsonl         # YOU author — line-of-business map
│   │   # {"lob_code": "GMNS", "lob_name": "…", "physical": "dw.<t>",
│   │   #  "verified_by": …, "verified_on": …, "notes": "",
│   │   #  "aliases": [<catalog spellings of the same LOB>]} — one row
│   │   # per (LOB, table); multi-membership = several rows. Strict:
│   │   # physical must be a crosswalk row. Steward witness; the
│   │   # catalogs corroborate with their own witnesses via aliases.
│   ├── identity/org_map.jsonl         # YOU author — org units
│   │   # (sub-LOBs): {"org_code","org_name","parent_lob","aliases",…}.
│   │   # Mined business_unit = WHO QUERIES → used_by edges (usage),
│   │   # never in_lob (ownership); unmapped values are counted.
│   ├── nodes/  edges/                 # append-only JSONL quads
│   └── runs/<run_name>/               # every run's committed record:
│       events.jsonl · census.json · census_tail.jsonl ·
│       quarantine.jsonl · coverage_crosstab.json · tasks/gold.jsonl ·
│       triage/empty_sql_backlog.jsonl · validation.json ·
│       eval_report.json · triage/floor_failures.jsonl
└── builds/                            # L3 (compile)
    ├── CURRENT                        # the promoted build id (E4)
    ├── b_<hash12>/                    # manifest.json, cards/, indexes/,
    │                                  # census.json, acl.json,
    │                                  # schema.json, tickets.jsonl,
    │                                  # DIFF_vs_prev.md
    └── sandbox_ledger.jsonl           # sandbox decision audit trail
```

`builds/.gitignore` keeps only manifest/census/DIFF/CURRENT — the review
record stays in git, the bulk artifacts do not.

## Setup (once)

```bash
git clone <repo> && cd wyla
git checkout main
cd synapse-agentic-harness-system
python3 --version                      # needs >= 3.11
pip install -e ".[sql]"                # sqlglot 30.15.* + pydantic + pyyaml
pip install google-auth                # P1+ only (dry-run token)
python -m pytest tests/ -q             # 65 green = environment proven
export DATA=~/meridian-data            # wherever you put the inputs
```

`meridian-data/` may also live inside the repo checkout — the root
`.gitignore` excludes it — but never `git add` archive data manually.

Env (P1 onward — P0 needs no network at all): `SYNAPSE_BQ_SA_KEY` (or
`GOOGLE_APPLICATION_CREDENTIALS`), `BQ_PROJECT_ID` (or
`SYNAPSE_BQ_PROJECT` / `GOOGLE_CLOUD_PROJECT`); optional
`BIGQUERY_API_BASE_URL` (defaults to the enterprise PSC endpoint),
`BQ_LOCATION` (defaults `US`). Same contract the extraction laptop
already uses. Missing env exits **3** with a typed message.

## The sequence

```bash
# P0 — census + gold tasks (no network)              → p0_census.md
python scripts/pipeline.py census \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
  --out graph/runs/p0_census --json
python scripts/pipeline.py make-tasks \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
  --out graph/runs/p0_census
# human: triage graph/runs/p0_census/triage/empty_sql_backlog.jsonl

# P1 — calibrate the ground (dry-run)                → p1_ground.md
python scripts/run_evals.py \
  --tasks graph/runs/p0_census/tasks/gold.jsonl \
  --sut oracle --fail-under 1.0 \
  --out graph/runs/p1_ground_oracle --json

# P2 — truth graph + first real compile              → p2_build.md
# human: author graph/identity/crosswalk.jsonl (46 rows) FIRST
# RUN 1 (A7): omit --mdm-archive — std_tech relays the same MDM
# declarations; run 2 adds it back and the DIFF measures what it adds
# RUN 1 (A8): --no-jobs-30d — the 30-day query history was judged
# incorrect; nothing derived from it (jobs witness, cost priors,
# top_users, co_queried, templates) enters the graph. The files stay
# ledgered as deferred. A corrected extract re-enables the witness
# (drop the flag) and the DIFF measures what real usage adds.
python scripts/pipeline.py build-graph \
  --graph graph \
  --crosswalk graph/identity/crosswalk.jsonl \
  --bq-archive $DATA/real_extractions_production \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
  --no-jobs-30d \
  --out graph/runs/p2_build --json
python scripts/pipeline.py compile \
  --graph graph --builds builds \
  --out graph/runs/p2_compile --json
# human: review builds/<id>/DIFF_vs_prev.md + tickets.jsonl

# P3 — the resolver floor                            → p3_tools.md
python scripts/run_evals.py \
  --tasks graph/runs/p0_census/tasks/gold.jsonl \
  --tasks tests/tasks/curated/curated.jsonl \
  --sut resolver:builds \
  --out graph/runs/p3_floor --json
# human: triage graph/runs/p3_floor/triage/floor_failures.jsonl
```

Commit after each phase (each runbook has the exact `git add` line).

## Behavior contract (all subcommands)

- Exit codes: **0** ok · **1** gate failure · **2** validation error ·
  **3** env/auth · **4** interrupted-with-checkpoint.
- Interrupted? Re-run the same command — `--resume` is the default;
  `--fresh` restarts deliberately.
- `--plain` for log-friendly output, `--json` for a machine summary on
  stdout; the full event stream is always in `<out>/events.jsonl`.

## Synapse v3 chat (the assistant) — after main is pulled

```bash
pip install -e ".[sql,dev,assistant]"   # assistant adds python-pptx + numpy
uvicorn apps.synapse_admin.backend.app:app --port 8400   # from the repo root
# → open http://127.0.0.1:8400/#/chat  (New ask in the nav)

# the two asks that decide Stage 1 (docs/specs/synapse_v3_harness.md §10):
#   "give me all GMNS metrics"     — one interaction, the area's metrics
#   the ALIF ask with SQL in it    — a long answer that survives whole
# then PASTE the transcript back: the chat page as you see it, plus
#   graph/runs/chat/events/<session>.jsonl   (the record, whole)

# the assistant baseline:
python scripts/chat_eval.py --real              # Vertex creds in the silo .env
# → PASTE docs/evals/assistant_baseline_vertex.md back into the session
# short/cheap variants: --limit 4 · --kind playbook · --no-judge
# --kind recovery injects warehouse failures (a missing partition
# filter, a type mismatch, a wrong data project) and grades whether
# the agent fixes what is its own and reports what is configuration
```

What changed in v3 (Stage 1): one interaction per turn over native
tool calls — no strict JSON, no per-step token cap, no step cap; tool
results reach the model whole; the depth dial in the composer
(Quick / Standard / Deep = thinking low / medium / high); one live
line that speaks the model's own thought summaries and collapses to
"Worked for 12s · searched the graph, read the cards"; the artifact
panel opens only when the model puts something in it; a memory save
is disclosed inline with an undo. Set `SYNAPSE_USER_NAME` in the silo
`.env` so memory addresses the person by name.

**Vocabulary and values.** Drop the five files in the sources dir and
rebuild: `data_cleaned.csv` and `business_terms.csv` (already loaded),
`potential_common_word_acronyms.csv` (the common-word guard, a flag on
the acronym nodes), `glossary_terms.csv` (a generated view: counted for
drift, never loaded twice), and `low_cardinality_synonyms_index.json` (stored code → meaning,
per table and column; `value_lookup.json` is accepted too). The build report shows the counts; the chat then
expands acronyms with their scope, refuses to expand REST unless it is
written as one, and turns "KYC done" into the code to filter on. The
strategy is `docs/specs/vocabulary_and_values.md`.

**The archive's own value profile is used too.** Every
`15_low_cardinality_values/<column>.csv` becomes the column's observed
values with their share of rows, and `15_low_cardinality_manifest.csv`
puts the profiler's `distinct_estimate` on the same domain, so
`sample_values` says "3 values on record of an estimated 24 distinct"
instead of posing as the whole domain. `search(kind="values")` matches a
value written as stored ("transactions in GB" → `country_cd = 'GB'`,
6.3% of rows) as well as a meaning from the lookup.

**Rebuilding the graph with the new sources.** The store is
append-only: a second build-graph into the same tree appends every quad
again. Start a new tree and keep `graph/identity` (the crosswalk and the
human maps) and `graph/runs` (the evidence of earlier runs):

```bash
mv graph/nodes graph/nodes.before-vocab && mv graph/edges graph/edges.before-vocab
python scripts/pipeline.py build-graph \
  --graph graph \
  --crosswalk graph/identity/crosswalk.jsonl \
  --bq-archive $DATA/real_extractions_production \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
  --no-jobs-30d \
  --fresh --run-id vocab1 \
  --out graph/runs/p2_build_vocab --json
python scripts/pipeline.py compile \
  --graph graph --builds builds \
  --out graph/runs/p2_compile_vocab --json
```

Read the build report for `vocab.common_word_acronyms`,
`vocab.glossary_view_drift`, `value_meanings.domains_annotated` /
`domains_minted` / `skipped_unknown_table`, and `bq.domains_with_estimate`;
the ledger lists `15_low_cardinality_manifest.csv` and
`low_cardinality_synonyms_index.json` as consumed and `glossary_terms.csv`
as deferred with its reason. `expressions.catalog_join_edges` counts the
`joined_tables` pairs from the measures catalog that became join edges
(witness `catalog_mined`, `source: catalog` in `indexes/joins.jsonl`, no
ON condition, so never above candidate) and `catalog_join_unresolved`
counts the joined names the crosswalk could not place — a large number
there means the alias sidecar wants those names. The new build's
`DIFF_vs_prev.md` shows what the sources added. Restart the app so the
chat serves the new build.

**The second surface.** The same app serves Synapse Semantic
Intelligence at `http://localhost:8400/synapse/`: the chat with its
artifacts published in the transcript, Search chats as a page, and
Data Products and Metrics Explorer as cards. It reads the same build
and needs nothing extra; a hard refresh picks up a new page. To put
your logo in its left header, set `SYNAPSE_LOGO=/path/to/logo.png` in
the silo `.env` (png, jpg, svg, webp or gif) and restart the app: the
image replaces the words, and the words stay when the file is not
there.

When the words stay, open `http://localhost:8400/api/synapse/brand`: it
says which `.env` was read (`env_file` — the silo's `.env` wins, and a
line in any other `.env` is never read), the value as read, the path
tried, whether a file is there, and what its bytes are. The three
usual causes: the app was not restarted after the `.env` changed
(the file is read once, at start); a note after the path on the same
line without a space before the `#`; and a `.png` that is not a PNG
(a HEIC or WebP export renamed, or an SVG saved with the wrong
suffix), which the browser drops in silence — the brand page names
the bytes, and the page's console carries the same line. The logo
shows on `/synapse/` only.

**The query comes first.** In Chat mode a data question ends with the
query on a card — the SQL, what it will scan, its status and meridian
line — and three buttons: **Run query** executes it with no model call
and puts the rows in the panel as a table (saved as `q1`), **Run +
build dashboard** runs it and then builds the tiles from the rows, and
**Edit SQL** lets you change the query before running. The chips after
a run offer a dashboard or a refinement. Switch to **Autopilot** in the
composer when Synapse should run and build without stopping. Both need
`SAHS_ALLOW_LIVE=1` for the rows; without it the card still shows the
query and the run explains the configuration. Type `/` in the composer
to pick a skill pack for the turn.

**The query comes first, then the rows, then the picture.** In Chat
mode a data question ends with the query on a card (its price, status
and meridian line); Run query executes it with no model call and puts
the rows in the panel and in the workspace as q1; the "Chart these
rows" chip draws them with no model call under the same provenance;
"Build a dashboard from these rows" and "Run + build dashboard" are
the model's turns, on autopilot. Autopilot mode skips the card. Run
needs `SAHS_ALLOW_LIVE=1` (that exact name: `SAHS_LIVE=1` does
nothing, and the refusal now names such a near miss); put it in the
silo `.env`, which every run reads, or export it in the shell that
starts the app — an export in another shell never reaches a running
server. Without it the card still shows the query and the run explains
the configuration. The scan ceiling (`SAHS_LIVE_MAX_BYTES`, 1 GB by
default) is enforced at handover time too: a query over it comes back
to the model once to narrow, and if it is handed over anyway the card
says Run will be refused unless it is narrowed or the ceiling raised.
`python scripts/turn_doctor.py` and `planes_check.py` print the live
switch and the ceiling with the planes. When a run's rows carry dates
after today (a window with no upper bound over a table that holds
future-dated rows: the dashboard whose axis ran to 2118), the run's
receipts and the chart say so and name the bound to add.

**Rows come from the warehouse under two limits.** Until live
execution is on, the chat can only price a query (dry run) — it never
sees rows, so a "how many" question ends in dry runs and a partial. Put
in the silo `.env`:

```bash
SAHS_ALLOW_LIVE=1                 # run_sql mode run: rows, gated
SAHS_LIVE_MAX_BYTES=10000000000   # scan ceiling, 10 GB (default 1 GB)
```

A query priced above the ceiling is refused with the partition-filter
hint and the model narrows it; the row cap is the tool's `limit`
(default 200, at most 1000). Every result discloses both, and the SQL
that ran is in the step row — click it.

**Switching chats or tabs never stops a turn.** The turn runs on the
server; the page only listens. Coming back to a session mid-turn
replays the turn from its first event and keeps following it, and the
chats shelf marks a working chat with a pulsing dot while you are away.

**If a turn looks stuck:** the live line now ticks ("Checking the
query… 12s", then "Still thinking · 34s"), so a slow model call and a
slow tool read differently. For the record, ask the event file:

```bash
python scripts/turn_doctor.py          # newest session: each model call
                                       # and tool with its seconds, and the
                                       # OPEN segment a stuck turn sits in
```

The doctor now opens with the **network planes**: the route each
connection pins (BigQuery direct on the PSC contract, Vertex via the
corporate proxy) and a note when the environment's NO_PROXY names
googleapis. A turn that hung on the model call right after the first
dry run was exactly this: the BigQuery connection used to write
googleapis.com into NO_PROXY for the whole process, so every later
Vertex call (the OAuth refresh and the stream) went direct into the
corporate blackhole until the 120 s silence timeout. Each plane now
carries its own route and its opener never consults NO_PROXY, so a
dry run cannot reroute a model call. Separate check scripts never
shared the bug (each is its own process); the proof is one process
doing both in the chat's order:

```bash
python scripts/planes_check.py     # a dry run, then one model call,
                                   # the environment watched in between
```

The client gives up on a model stream after 120 s of silence, retries
once if nothing had arrived, and the turn then closes in plain language
with what was already said. Paste the doctor's output with the transcript.

**Gemini 2.5 Pro through the gateway (a candidate for the model plane).** The
guide's path is an identity-service bearer token minted from `APP_ID` and
`APP_SECRET` (an HMAC-signed request), then Gemini's own REST protocol
behind `eag-dev.aexp.com`. Before any of it enters the program, prove
what it does on the laptop:

```bash
# in the silo .env: APP_ID, APP_SECRET (or AUTH_MODE=env + GEMINI_BEARER_TOKEN)
python scripts/gateway_check.py                       # token · generate · stream · tools · system
python scripts/gateway_check.py --probe-ttl 7 --json gateway_report.json   # then watch the token die
```

The route is decided by the first real request, the token POST:
direct first, then the corporate proxy, whichever answers with any
HTTP status. The model id comes from `GATEWAY_MODEL` (leave `GEMINI_MODEL`
unset: the Vertex plane reads that name as a fallback and the check
warns when it is set without `VERTEX_MODEL`). The check mints the
token with a milliseconds timestamp (the unit
The identity service takes; a seconds timestamp is tried only if the gateway
refuses, never after a 200) and reads it from the answer's
`authorization_token` field (the other usual names are tried after
it, and a lone long string as a last resort), reads what the answer
says about expiry (nothing, on this gateway) and what the token's own
JWT claims say (this gateway's token is a JWT with an `exp` claim
and no `iat`, so the lifetime is `exp` minus the minting moment, and
the probe confirms it), then makes the model calls, plus a prompt-cache
check: the same 3K-token prefix twice, reading `cachedContentTokenCount`
on the second call, which says whether the harness's stable prefix
still earns its cache through the gateway. Each is addressed the guide's way,
`…/models/gemini-2.5-pro/generateContent` with a slash: the gateway's scopes
are path patterns under the model name, and Google's own colon form
(`gemini-2.5-pro:generateContent`) falls outside them, which the
gateway answers with a bare 401 before Gemini is reached. The check
tries the slash first, falls back to the colon on a 401 or 404, and
the `path` row says which form was taken and what the other got, the
reason read from the response headers when the body is empty. The
calls: `generateContent` with
thought summaries, `streamGenerateContent?alt=sse` (and says whether
the bytes streamed or arrived in one burst, as SSE or as a JSON
array), a native tool call with its thought signature echoed back on
the round trip, the thoughts flag in both spellings (the guide's
`include_thoughts` and the harness's `includeThoughts`), and a system
instruction (with room under the output cap: on 2.5 the thinking
tokens count against `maxOutputTokens`, and a tight cap yields an
empty answer). With `--probe-ttl` it keeps
sending a deliberately invalid request every 20 s until the gateway
answers 401: that is the token's real lifetime, which the client will
have to keep itself. Secrets never print; paste the block (and the
JSON) back.

**The chat on the gateway (Gemini 2.5 Pro).** With `APP_ID` and `APP_SECRET`
in the silo `.env` the chat's model calls ride the gateway (`SAHS_MODEL_PLANE`
is `auto`; set it to `vertex` to go back, `gateway` to insist). Restart
the app: the composer's model label reads "Gemini 2.5 Pro",
`python scripts/turn_doctor.py` prints the plane and why, and
`python scripts/planes_check.py` proves it in one process after a dry
run: the token minted, its remaining life, one model answer. What
changes in the chat: the gateway serves no stream, so each model call lands
whole. The thinking block fills and the prose appears when a call
returns; between calls the live line ticks and the tool rows appear.
Run, the chart from saved rows and the dashboards are untouched: they
never call the model. The depth dial maps to thinking budgets (Quick
1024, Standard 4096, Deep 16384 tokens; `GATEWAY_THINKING_BUDGETS`
overrides), and since 2.5 counts the thinking against the output cap
the client raises the cap by the budget. The token lives 599 s and is
minted again at 80% of that or on the first 401, with the call
retried once. The prompt is unchanged; where 2.5 slips is a matter
for the evals, not for guessing.

**Switching models from the composer.** The label by the composer is
a select: "Gemini 3.1 Pro Preview" or "Gemini 2.5 Pro" (the plane
each rides is the "?"'s business, not the label's), both surfaces. A
plane the machine cannot ride is listed greyed with "not configured"
and the reason in its tooltip; picking one that
can be ridden is remembered on that chat, rides from the next message
on, and survives a reload (`SAHS_MODEL_PLANE` only says where a new
chat starts). The conversation carries over as text, so a chat can
change model mid-way. The "?" beside the dials opens the explanation
in plain words: Quick, Standard and Deep (how much Synapse thinks
before each step), and both models with whether each is available
here. The per-plane facts (a thinking level on Vertex, a thinking
budget on the gateway) stay in `GET /api/chat/dials` for the curious. `GET /api/chat/dials` is the one source, and
`python scripts/turn_doctor.py` prints the plane and the depth on
every turn's line.

**The library on the second surface.** Data Products filters by line
of business beside the search (every code the build maps a table to,
by name, with its count; "unmapped" for the rest). A product page
says what the product is and lists its columns as a searchable list:
the first twelve open, the rest a search away, and a row opens to
what the column is (the description on record, the MDM's supplementary
meaning, sensitivity, how many sources agree) and where it is used
(joins, metrics). The meaning comes from `columns.json`, an index the
compiler writes beside `schema.json` since this change: run
`python scripts/pipeline.py compile` once after pulling, or the page
falls back to the served card, which is budgeted and may carry no
meaning past the twelfth column. Metric cards open in place with the
full definition, the data product they are computed on and the
columns they read. Skills replaces Artifacts in the nav: the doctrine
packs the agent loads by itself, each with its slash command and a
Use-in-chat door; `#/artifacts` still answers by URL.

**Files in the chat.** The composer's "+" offers Add files. What the
model reads natively rides as itself, inline on the user turn: PDF
and images (PNG, JPEG, WEBP, HEIC). Text in its suffixes (txt, md,
csv, tsv, json, xml, html, sql, py, yaml) rides as a text part. A
workbook, a Word file or a deck is not native: it is converted to
text here (sheets to CSV, paragraphs to text; a deck needs
python-pptx from the assistant extra) and rides as text, disclosed
as such. Old Office formats, archives, audio and video are refused
with the reason. Limits: 10 MB a file, 18 MB of inline bytes on one
message, 10 files a message. A file rides the message it is sent
with; the conversation remembers that it was sent, by name, not its
bytes: attach it again to ask more. `python scripts/files_check.py`
proves what the machine's plane actually reads: it builds a PDF, a
PNG, a CSV, a text file, a workbook and a Word file here, sends each
on one call the way the chat would, and reads the answer for the
token. Run it once on each plane before trusting a kind of file in
the chat; the report is the answer to "what files are supported".

**Skills: one shelf, the way a settings page lists it.** Customize →
Skills in the nav. One list of what Synapse knows how to do and what
it knows about: the packs (built in, author Synapse; yours, author
You, living in `graph/skills/users/<you>/` and loading for you alone;
shared packs in `graph/skills/*.md` with the author line the file
carries) and the knowledge files (the folders under the skills root
such as `CFR/` and `TLS/`, author from the file's own `author:` line
or its folder; the staged drops in `sources/artifacts/` by business
unit; the reference docs in `sources/*.md`), each with its last
write. Search from the toolbar. Browse and Add open one pop-up with
three ways in: bring a markdown file (it becomes a skill of yours),
write one from the house template, or Draft with Synapse — a guided
flow: your material in (paste it, or add a text file, a workbook or a
Word file), the draft out with Synapse's notes on what was unclear,
read and edit, name, save. Without a model the draft is refused with
the reason and the Write tab is the way on. A row opens in the reader
on the right, a pack with Use in chat (and Delete for yours). A
knowledge file is still staged through `POST /api/meridian/artifacts`;
the page no longer offers it. `#/knowledge` and `#/artifacts` still
answer, on this page.

**The composer, plainer still.** The "?" explains Quick, Standard and
Deep alone; the model select names the model and each option's hover
says what riding it means. Chat or Autopilot is a select beside the
model and the depth (Chat hands queries over for you to run;
Autopilot runs under the limits and builds the deliverable). A new
chat lands on Gemini 3.1 Pro Preview whenever the Vertex contract is
in the `.env`; `SAHS_MODEL_PLANE=gateway` names the other default.

**A skill on the chat.** Type `/` and pick a pack: it sits in the
composer as a chip where the mode pill used to be — pinned on the
conversation, so every message rides with it — until you × it, the
way an attached image sits in a chat. Up to four. Use in chat on the
Skills page does the same. Typing `/name and your words` still works
as one message.

**Skills go to your manager first.** Bring a file, Write a skill or
Draft with Synapse: every way ends as a submission — the name, a
description and the intended purpose from you; the submitter, the
date, the version and the status from the system — to your direct
manager (`SYNAPSE_USER_MANAGER` and `SYNAPSE_USER_MANAGER_BAND` in the
silo `.env`; band 40 or above may approve; unset, you review your
own on the laptop and the record says so). The shelf shows it as
Pending approval with Synapse's read beside it: a summary (executive,
key business topics, intended purpose, suggested audience), the
insights that need a reader's attention (ambiguous statements,
inconsistent terminology, contradictions, missing context, duplicate
content, outdated references — each with a confidence and where it
is), and an advisory recommendation. The checks land at once; the
model's read follows in the background. The manager opens the row,
downloads the file if they like, adds comments, and approves —
Published: an own pack loads for you from the next chat, a
knowledge file is staged for its business unit — or rejects, with
the comments shown; you update and resubmit, a new version. Pending
and rejected files never reach the agent. The notices land at the
top of the Skills page and on the nav badge; a Bring a file can be a
knowledge file too (pick the kind, name the business unit). The
ledger is `graph/runs/reviews/ledger.jsonl`, the versions beside it.

**Charts that are right.** A built-in skill, `/charts`, says which
chart for which question and how the spec is built: every series on
ONE shared x axis, a null where a series has no value, a forecast as
a second dashed series that starts at the last actual. The renderer
places every point by its label — a forecast over July to December
draws over July to December, never over the actuals — and the pptx
export draws the same axis.

**Memory under Customize.** `memory.md`: what Synapse remembers about
you as a document, one line per memory. Edit the lines and save — a
line you add is remembered, a line you remove is retired, the rest
stay — and the next chat reads it. Synapse adds a line itself when
you settle a preference in chat and says so inline with an undo; the
header's memory button shows the same list in the chat.

**Before the first query:** the graph names tables `dw.<table>`, and
BigQuery resolves that against the project that runs the query
(`demo-billing`), not the one that hosts the data. Set
`SYNAPSE_BQ_DATA_PROJECT=demo-warehouse` in the silo `.env` (and `BQ_LOCATION`
if the dataset is regional); the sandbox then qualifies every known
table as `demo-warehouse.dw.<table>` before any dry run or execution, and
the run_sql result shows the SQL it sent as `sql_sent`. Prove it once:

```bash
python scripts/bq_check.py --table dw.gms_transaction
# ✓ `demo-warehouse.dw.gms_transaction` resolves · bytes …
```

The chat stores its sessions under `graph/runs/chat/`; artifacts,
memory, and projects live in `sessions.sqlite3` there; the full
per-turn record (what the model saw, every tool result) is the
events file beside it — the chat itself shows none of that. PPTX
export needs the `assistant` extra — the route says so if it is
missing.

**Leaving the laptop.** The stores above are the laptop's. What they
become on Spanner — people with an email and a password, roles that
open a surface, every chat and message and file and memory as a row
with its person, the graph still on the filesystem — is explained
table by table in `docs/spanner_schema.md`, with the DDL under
`db/spanner/` and the `.env` block in `.env.example` (`SAHS_STORE`,
`SPANNER_*`, `AUTH_*`). Nothing here reads them yet: the laptop runs
`SAHS_STORE=local`, which is what it does today.
