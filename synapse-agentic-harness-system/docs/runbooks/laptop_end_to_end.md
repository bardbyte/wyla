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
python scripts/laptop.py build-graph \
  --graph graph \
  --crosswalk graph/identity/crosswalk.jsonl \
  --bq-archive $DATA/real_extractions_production \
  --mdm-archive $DATA/mdm_46_patched_v2 \
  --sources-dir $DATA/sources \
  --registry $DATA/real_extractions_production/_batch_summary.csv \
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
