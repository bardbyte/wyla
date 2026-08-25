# P1 runbook — the ground (laptop, dry-run substrate)

Prove the graders against the real gold set with BigQuery **dry-run
only** (assumption A1): validity + result-schema equivalence + a bytes
band. No rows move. One number comes out of this: **gold pass@1 under
the oracle**, which must be 1.000 — anything less is a grader or task
defect, not a model problem (there is no model yet).

## Prereqs

- P0 complete: `graph/runs/p0_census/tasks/gold.jsonl` committed and the
  empty-SQL backlog triaged.
- SVC-ID env (same contract the extraction laptop already uses):
  `LUMI_BQ_SA_KEY` (key file), `BQ_PROJECT_ID`, optional
  `BIGQUERY_API_BASE_URL`, `BQ_LOCATION`. `google-auth` installed.
  Missing/broken env exits **3** before any request is made.

## Run — calibrate the ground on itself

```bash
cd synapse-agentic-harness-system

# 1. oracle: echoes gold — must be a perfect 1.000 / 0 ambiguous
python scripts/run_evals.py \
  --tasks graph/runs/p0_census/tasks/gold.jsonl \
  --sut oracle --fail-under 1.0 \
  --out graph/runs/p1_ground_oracle --json

# 2. null: always abstains — must score EXACTLY the abstention share
python scripts/run_evals.py \
  --tasks graph/runs/p0_census/tasks/gold.jsonl \
  --tasks tests/tasks/curated/curated.jsonl \
  --sut null --out graph/runs/p1_ground_null --json
```

- The excluded line (`excluded (coverage=external): N of M`) is printed
  on every run — the denominator never shrinks silently (E5). Record N.
- Verdicts are the E2 lattice: fp-match → PASS; fp-mismatch + invalid
  dry-run or schema mismatch → FAIL; fp-mismatch + schema-match →
  **AMBIGUOUS**, appended to `<out>/triage/ambiguous.jsonl`. Triage
  grows a task's `accepted_fps`; it never silently moves the floor.
- Dry-run coverage: the oracle run's report includes how many tasks
  actually reached the substrate — report that number in the commit
  message.

## Gates

- oracle pass@1 = 1.000, ambiguous 0 (else fix the grader or the task —
  every miss is a defect in the ground itself).
- null pass@1 = exact abstention share of the combined set.
- `docs/assumptions.md` carries A1–A3 with today's date checked.

## Commit

```bash
git add graph/runs/p1_ground_oracle graph/runs/p1_ground_null
git commit -m "meridian: P1 ground calibrated — oracle 1.000, dry-run coverage reported"
```

Exit criteria met when: both runs exit 0, reports + events committed,
ambiguous triage file (possibly empty) present.
