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
    ├── data_cleaned.csv               # acropedia glossary
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
git checkout claude/semantic-layer-sources-m9jymm   # or main once merged
cd synapse-agentic-harness-system
python3 --version                      # needs >= 3.11
pip install -e ".[sql]"                # sqlglot 30.15.* + pydantic + pyyaml
pip install google-auth                # P1+ only (dry-run token)
python -m pytest tests/ -q             # 65 green = environment proven
export DATA=~/meridian-data            # wherever you put the inputs
```

`meridian-data/` may also live inside the repo checkout — the root
`.gitignore` excludes it — but never `git add` archive data manually.

Env (P1 onward — P0 needs no network at all): `LUMI_BQ_SA_KEY` (or
`GOOGLE_APPLICATION_CREDENTIALS`), `BQ_PROJECT_ID` (or
`LUMI_BQ_PROJECT` / `GOOGLE_CLOUD_PROJECT`); optional
`BIGQUERY_API_BASE_URL` (defaults to the enterprise PSC endpoint),
`BQ_LOCATION` (defaults `US`). Same contract the extraction laptop
already uses. Missing env exits **3** with a typed message.

## The sequence

```bash
# P0 — census + gold tasks (no network)              → p0_census.md
python scripts/laptop.py census \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
  --out graph/runs/p0_census --json
python scripts/laptop.py make-tasks \
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
python scripts/laptop.py build-graph \
  --graph graph \
  --crosswalk graph/identity/crosswalk.jsonl \
  --bq-archive $DATA/real_extractions_production \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
  --no-jobs-30d \
  --out graph/runs/p2_build --json
python scripts/laptop.py compile \
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
uvicorn apps.lumi.backend.app:app --port 8400   # from the repo root
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
is disclosed inline with an undo. Set `LUMI_USER_NAME` in the silo
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
python scripts/laptop.py build-graph \
  --graph graph \
  --crosswalk graph/identity/crosswalk.jsonl \
  --bq-archive $DATA/real_extractions_production \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
  --no-jobs-30d \
  --fresh --run-id vocab1 \
  --out graph/runs/p2_build_vocab --json
python scripts/laptop.py compile \
  --graph graph --builds builds \
  --out graph/runs/p2_compile_vocab --json
```

Read the build report for `vocab.common_word_acronyms`,
`vocab.glossary_view_drift`, `value_meanings.domains_annotated` /
`domains_minted` / `skipped_unknown_table`, and `bq.domains_with_estimate`;
the ledger lists `15_low_cardinality_manifest.csv` and
`low_cardinality_synonyms_index.json` as consumed and `glossary_terms.csv`
as deferred with its reason. The new build's `DIFF_vs_prev.md` shows what
the sources added. Restart the app so the chat serves the new build.

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
needs `SAHS_ALLOW_LIVE=1`; without it the card still shows the query
and the run explains the configuration.

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

**Before the first query:** the graph names tables `dw.<table>`, and
BigQuery resolves that against the project that runs the query
(`prj-p-lumi-gpt`), not the one that hosts the data. Set
`LUMI_BQ_DATA_PROJECT=axp-lumi` in the silo `.env` (and `BQ_LOCATION`
if the dataset is regional); the sandbox then qualifies every known
table as `axp-lumi.dw.<table>` before any dry run or execution, and
the run_sql result shows the SQL it sent as `sql_sent`. Prove it once:

```bash
python scripts/bq_check.py --table dw.gms_transaction
# ✓ `axp-lumi.dw.gms_transaction` resolves · bytes …
```

The chat stores its sessions under `graph/runs/chat/`; artifacts,
memory, and projects live in `sessions.sqlite3` there; the full
per-turn record (what the model saw, every tool result) is the
events file beside it — the chat itself shows none of that. PPTX
export needs the `assistant` extra — the route says so if it is
missing.
