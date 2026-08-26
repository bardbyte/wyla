# P0 runbook — the census (laptop)

Run against the real archives; commit the outputs. Two numbers on one page
come out of this: **conflict counts per concept/metric** and **gold-set
health**.

## Prereqs

- The BQ archive at `<BQ_ROOT>` (`real_extractions_production/`) and the
  semantic sources dir `<SRC>` containing (conventional names):
  `blue_business_insights.csv`, `extracted_gold_queries.json`,
  `measures_catalog.json`, `metrics_dmp.json`,
  `extended_gmns_semantics.json`, `data_cleaned.csv`,
  `business_terms.csv`, `std_tech_metadata/`, `skills/`.
- `pip install -e "synapse-agentic-harness-system[sql,dev]"` (or ensure
  sqlglot==30.15.* + pydantic + pyyaml on path).

## Run

```bash
cd synapse-agentic-harness-system

python scripts/laptop.py census \
  --sources-dir <SRC> \
  --registry <BQ_ROOT>/_batch_summary.csv \
  --out graph/runs/p0_census \
  --json
```

- Interrupted? Re-run the same command — `--resume` is the default and the
  checkpoint picks up where it died. `--fresh` restarts deliberately.
- Exit codes: 0 ok · 1 gate failure · 2 validation error · 3 env/auth ·
  4 interrupted-with-checkpoint.

## Gates (the run enforces these; exit 1 names the failure)

- `blue_canon_rate` ≥ 95% of in-scope snippets canonicalized. The
  denominator is rows that CLAIM to be SQL on tables this run carries:
  enterprise-wide rows naming tables outside the registry quarantine as
  `out_of_scope` and zero-signal prose in the sql_logic column ("Cheque
  Cashing") quarantines as `not_sql` — both counted in
  `quarantine.jsonl`, neither in the gate. Borderline rows (any SQL
  signal at all) still go to the parser and fail the gate honestly.
- `blocker_sources_100pct`: zero canon failures across gold non-empty
  pairs, metrics_dmp, extended_gmns, skill contracts — any miss is a
  release blocker, not a statistic.

## Then materialize the gold tasks

```bash
python scripts/laptop.py make-tasks \
  --sources-dir <SRC> \
  --registry <BQ_ROOT>/_batch_summary.csv \
  --out graph/runs/p0_census
```

## Triage the empty-SQL backlog (human step)

Open `graph/runs/p0_census/triage/empty_sql_backlog.jsonl` (~30 rows).
For each row set `"triage"` to one of:
- `"abstain_gold"` — the question genuinely shouldn't be answered
  (becomes an abstention task in P1);
- `"broken_extraction"` — the pair is defective; note why in
  `"resolution"`.

## Commit

```bash
git add graph/runs/p0_census
git commit -m "meridian: P0 census run — conflicts + gold health"
```

Exit criteria met when: gates green (exit 0), `census.json` +
`coverage_crosstab.json` + `quarantine.jsonl` committed, tasks
materialized, backlog triaged.
