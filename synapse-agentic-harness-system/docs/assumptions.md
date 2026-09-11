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

## A4 — the floor baseline includes self-mined witnesses (E12)

- **component**: loaders/archives/jobs_30d.py, compiler witness features,
  the P3 resolver floor
- **bet**: raw-history mining landed BEFORE the first real graph build
  (user sequencing decision), so the P3 floor baseline includes jobs-
  witness support/recency from day one — witness mining is never
  separately delta-measured against a pre-witness floor. INCLUDED PIN:
  `gold_attested` never feeds a resolver-ranked feature
  (support_effective, witness_agreement, recency) — the 158 gold pairs
  are the answer key, and the answer key must not rank the answers;
  gold-only classes rank at the support floor by design. audit_30d
  likewise corroborates but never votes.
- **evidence**: E12 spec ("Consequence, accepted and logged");
  RANKING_WITNESSES guard-asserts in compile; the gold-exclusion test.
- **date**: 2026-08-26
- **revisit_trigger**: the floor misses the 0.90 tripwire AND triage
  implicates support/recency features.
- **status**: active

## A5 — enricher confidence thresholds are initial bets (E13, Part B)

- **component**: the metric enrichment agent (Part B — not yet built;
  thresholds registered NOW so Part B lands against a committed bet)
- **bet**: blind-test recovery ≥80% ⇒ batch-tier review eligible;
  60–80% ⇒ item-review only; <60% ⇒ do not run at scale, iterate the
  prompt. The 80/60 split is judgment, not measurement.
- **evidence**: none yet — the blind test (strip names from the 35 DMP +
  14 GMNS certified metrics, enricher renames from expression + cards
  alone) produces the first number.
- **date**: 2026-08-26
- **revisit_trigger**: the first steward review cycle's approve/correct
  rates per tier.
- **status**: active

## A6 — cost-gate constants: 3× p95 anomaly multiplier, 20-job floor

- **component**: tools/sandbox.py (cost_gate_anomaly), jobs_30d cost
  priors
- **bet**: a query scanning more than 3× its table's observed p95
  bytes-per-job is an anomaly worth refusing; a p95 computed from fewer
  than 20 canonicalized jobs in the 30-day window is an anecdote in a
  percentile's clothes, so the prior is discarded and the global budget
  ceiling governs alone. The prior may TIGHTEN the effective cap below
  the global ceiling, never loosen it above (a relative gate must not
  raise an absolute one — this warehouse has billed ~35 TiB in a single
  extraction run).
- **evidence**: none yet — both constants chosen by judgment; the deny
  ledger records every gate decision with the observed bytes.
- **date**: 2026-08-26
- **revisit_trigger**: denied-query triage (from the sandbox ledger)
  shows the anomaly gate refusing legitimate work or waving through
  waste.
- **status**: active

## A7 — run-1 graph is two-witness structural (MDM archive deferred)

- **component**: build-graph inputs; E1 reconciliation; the first real
  compile
- **bet**: run 1 builds WITHOUT `--mdm-archive`: std_tech (Atlas) is a
  relay of the same MDM declarations (`datasystem: Metadata Management`, `sourceType: Declared`), so the sensitivity,
  description, type, ownership, and term-link planes are fully served
  by bq + atlas — and skipping the 305,955-file MDM response tree cuts
  build-graph time dramatically (and removes the cloud-drive hydration
  risk). KNOWN LOSSES for run 1, accepted: table/column lineage
  (`upstream_of`/`derived_from`), lifecycle_status (incl. the honest
  `unknown_unavailable` pattern), `business_unit` from pipeline
  metadata, and structural witness_agreement capped at 2 (bq+atlas)
  instead of 3.
- **evidence**: reconcile.py consumes `data_type_atlas`,
  `description_atlas`, and `pii_role_id` as first-class witnesses;
  cards read `ownership_atlas`; user decision 2026-08-26.
- **date**: 2026-08-26
- **revisit_trigger**: run 2 adds `--mdm-archive` (hydrated, local) —
  the append-only store lands lumi as its own witness and
  `DIFF_vs_prev.md` IS the acceptance test for what MDM adds (lineage
  edges, lifecycle fill-in, agreement 2→3). Crosswalk `lumi_asset_id`
  may stay blank in run 1 and be backfilled then.
- **status**: active

## A8 — run-1 graph excludes the 30-day query history entirely

- **component**: build-graph `--no-jobs-30d`; jobs_30d adapter;
  bq_extraction jobs digests; sandbox cost priors
- **bet**: the extracted 30-day query history is judged INCORRECT by the
  steward, and incorrect history must not witness anything — so run 1
  builds with `--no-jobs-30d`: no jobs_30d witness quads, no joins_via
  harvest, no cost_prior/usage_rhythm props, no audit corroboration
  ReviewItems, and none of the bq-archive jobs digests (top_users,
  co_queried_with, query templates). KNOWN LOSSES, accepted: recency
  falls back to catalog dates everywhere (`recency_source: catalog`),
  witness_agreement loses the jobs family, the sandbox anomaly gate has
  no per-table priors (the global budget ceiling alone applies — the A6
  thin-prior fallback, now for every table), and co-query structure is
  absent. Metric mining rests on the catalogs (measures_catalog,
  metrics_dmp, gmns, skill contracts) — the catalog was mined upstream
  from a longer horizon and remains the mined-metric witness.
  The 17_queries_30d files stay LEDGERED as deferred with this
  assumption named — deliberately unread, never unaccounted.
- **evidence**: user decision 2026-08-26 ("past 30 days queries we have
  are incorrect — turn the adapter off").
- **date**: 2026-08-26
- **revisit_trigger**: a corrected history extract lands — run N+1
  re-enables the witness (drop the flag) and `DIFF_vs_prev.md` IS the
  acceptance test for what real usage adds (support corroboration, true
  recency, joins_via, cost priors), exactly the A7 pattern.
- **status**: active

## A9 — std_tech_metadata sensitivity is withheld from the graph

- **component**: `sahs/loaders/sensitivity.py`; the std_tech (Atlas)
  loader only; E1 D5; `acl.json`
- **bet**: no compliance declaration from **std_tech_metadata** reaches
  the graph. Atlas `has_pii`/`has_gdpr`/`has_oncop`, the three parallel
  declaration lists (`pii_columns`/`gdpr_columns`/`oncop_columns`), and
  column `pii_role_id`/`sde_group` are read and deliberately not
  carried. SCOPE IS ONE SOURCE: the MDM plane's `is_pii` and its
  `policy:pii` edge flow exactly as before, and so does every BigQuery
  row-access policy (`policy:unknown_denied`, `policy:row_access_N`) —
  row-access is access control the WAREHOUSE enforces, not a claim
  about a column's contents, and it remains the one honest
  live-execution gate. `data_classification`, a table-wide handling
  label, also stays.
- **KNOWN LOSSES, accepted**:
  - **D5 inflates and cannot be closed.** D5 fires when exactly ONE
    plane calls a column sensitive. With the Atlas plane silent by
    construction, every MDM-flagged column now reads "flagged by lumi
    only" and opens a `sensitivity_conflict` ticket that nobody can
    resolve, because resolving it needs the withheld witness. On the
    fixture, D5 goes 1 → 2 (cm13 was corroborated before the hold and
    is a ticket now). This is the pre-existing D5 design meeting a
    systematically absent plane, not new logic — no compiler code
    changed — but the ticket backlog it produces is real.
  - **A column named ONLY by an Atlas declaration is never minted.**
    `cm15_hash` in the fixtures; D1 drops 2 → 1 accordingly.
  - **Atlas can no longer corroborate or contradict MDM on
    sensitivity.** The disagreement still exists in the feeds; it is
    simply no longer visible.
- **NOT a loss**: `validate_sql` still refuses a column MDM calls
  sensitive. The hold changes which SOURCES may declare sensitivity,
  not what sensitivity MEANS downstream. Whether a violation (refusal)
  is the right meaning, versus a warning (flag), remains open — see
  the revisit trigger.
- **evidence**: user decision 2026-09-11 ("Leave out PII information
  for now to be considered in the loader we will decide after"),
  narrowed the same day to "just avoid sensitivity from
  std_tech_metadata". `scripts/std_tech_keys.py` reports every held key
  as a DEFERRAL naming this hold, so the withholding is on record
  rather than looking like a field gone dark;
  `test_the_sensitivity_hold_withholds_atlas_and_only_atlas` is the
  fence on a default build, asserting both halves — Atlas silent, MDM
  and BigQuery untouched.
- **date**: 2026-09-11
- **revisit_trigger**: two separable decisions. (1) Whether Atlas may
  declare sensitivity again — lift with `SAHS_LOAD_SENSITIVITY=1` for
  one run to see exactly what it would add, and watch D5 fall back as
  corroboration returns. (2) Whether `sensitive_column` should stay a
  violation (refusal) or become a warning (flag); this one is
  independent of the hold and applies to the MDM plane today. The
  graph is append-only, so lifting ADDS the declarations on the next
  build rather than needing a rebuild — but it is not retroactive for
  rows nobody re-registers.
- **status**: active
