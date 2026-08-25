# Meridian assumption register (E6)

Every harness/platform component is a dated bet about what we couldn't or
shouldn't build yet. Each entry: what the bet is, the evidence behind it,
and the trigger that reopens it. Reviewed on every model upgrade and every
time a trigger fires. Status: `active` | `expired` | `resolved`.

---

## A1 — dry-run-only execution substrate

- **component**: evals/substrate.py (`ExecutionSubstrate`), graders
- **bet**: exists/chosen because no warehouse rows are available to the
  repo (the BQ extraction deliberately excludes data) and the user locked
  "dry-run only for now". BigQuery dry-run returns the RESULT SCHEMA at
  zero cost, so validity + output-schema equivalence recover most of the
  shape-level truth without any row-level execution.
- **evidence**: plan decision #1 (AskUserQuestion, 2026-08-25); BQ dry-run
  schema behavior documented in the BQ extraction contract.
- **date**: 2026-08-25
- **revisit_trigger**: a governed BQ sample is approved, OR a synthetic
  micro-warehouse is approved. Either slots in behind the same Protocol;
  snapshot_equal edges (deferred with this) land then too.
- **status**: active

## A2 — resolver constants are uncalibrated bets

- **component**: tools/resolver.py
- **bet**: weights 0.4 (support) / 0.3 (recency) / 0.3 (context-fit),
  90-day recency half-life, margin threshold 0.15, tier confidence
  ceilings 0.95/0.8/0.7/0.55/0.4 — chosen by judgment, not calibration.
  Lexicographic (authority, score_rest) ordering is the one part that is
  principle, not tuning.
- **evidence**: no usage data exists yet to calibrate against; constants
  are versioned in every build manifest and logged per-slot in resolve()
  responses so every wrong bind is a readable trace.
- **date**: 2026-08-25
- **revisit_trigger**: resolver-floor triage shows feature misweighting,
  OR the curated task set doubles.
- **status**: active

## A3 — JSONL single-writer truth store

- **component**: graph/ (quads.py, append-only discipline)
- **bet**: flat JSONL quads in git deliver durability, provenance,
  diffability, and versioning with zero operational surface because the
  writers are inherently serial (batch extraction runs + a governance
  clerk) and the corpus is 46-table scale (~200K quads).
- **evidence**: NO-AGE decision in the approved design; serving never
  reads the graph (compiled builds only), so no concurrent-read pressure.
- **date**: 2026-08-25
- **revisit_trigger**: governance becomes interactive-concurrent, OR the
  corpus grows materially past the 46-table scope.
- **status**: active
