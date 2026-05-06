# LUMI Architecture Diagrams

ASCII diagrams of the system at four zoom levels. Render in any monospace
viewer.

---

## Level 1 — System overview (10,000ft view)

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                                  LUMI                                         │
│         (Workflow that enriches LookML views from gold queries +              │
│          MDM business metadata + baseline LookML, via Gemini 3.1 Pro)         │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │
   ┌────────────────────────────┼─────────────────────────────────┐
   │                            │                                 │
   ▼                            ▼                                 ▼
┌──────────┐             ┌─────────────┐                  ┌──────────────────┐
│  INPUT   │             │   PIPELINE  │                  │     OUTPUT       │
├──────────┤             ├─────────────┤                  ├──────────────────┤
│ Gold     │             │ Phase 1:    │                  │ output/          │
│ queries  │  ──────▶    │  parse      │                  │  ├─ views/       │
│ (138)    │             │  discover   │                  │  │  └─ <table>   │
│          │             │  stage      │                  │  │     .view.lkml│
│ Baseline │             │  plan       │                  │  ├─ models/     │
│ LookML   │             │ ════════    │                  │  │  └─ lumi_    │
│ (29 .lkml│             │ HUMAN GATE  │                  │  │     enriched │
│  files)  │             │ ════════    │                  │  │     .model   │
│          │             │ Phase 2:    │                  │  ├─ catalogs    │
│ MDM      │             │  enrich     │  ──────▶         │  │  ├─ metric   │
│ digests  │             │  validate   │                  │  │  ├─ filter   │
│ (29 JSON │             │  publish    │                  │  │  └─ golden   │
│  files)  │             └─────────────┘                  │  │     _quest   │
└──────────┘                                              │  ├─ coverage    │
                                                          │  │     _report  │
                                                          │  ├─ proposed_   │
                                                          │  │     overwrites│
                                                          │  └─ uncertain_  │
                                                          │        fields   │
                                                          └──────────────────┘
```

---

## Level 2 — Pipeline stages and human gate

```
                         ┌──────────────────┐
                         │   data/          │
                         │   gold_queries/  │
                         │   *.sql          │
                         └────────┬─────────┘
                                  │
                                  ▼
       ┌──────────────────────────────────────────────────────┐
       │ STAGE 1 — PARSE (sqlglot, deterministic)             │
       │   tables, columns, aggregations, joins,              │
       │   ctes, temp_tables, filters, date_functions,        │
       │   case_whens, select_aliases                         │
       └──────────────────────────┬───────────────────────────┘
                                  │ SQLFingerprint × N queries
                                  ▼
       ┌──────────────────────────────────────────────────────┐
       │ STAGE 2 — DISCOVER (deterministic)                   │
       │   group fingerprints by table                        │
       │   load MDM digest per table                          │
       │   parse baseline LookML per table (lkml lib)         │
       │   produce TableContext × N tables                    │
       └──────────────────────────┬───────────────────────────┘
                                  │ TableContext dict
                                  ▼
       ┌──────────────────────────────────────────────────────┐
       │ STAGE 3 — STAGE (deterministic prioritization)       │
       │   topological sort by dependency                     │
       │   tie-break: query_count desc, then complexity       │
       └──────────────────────────┬───────────────────────────┘
                                  │ TablePriority list
                                  ▼
       ┌──────────────────────────────────────────────────────┐
       │ STAGE 4 — PLAN  (deterministic skeleton + LLM author)│
       │   build_enrichment_plan_skeleton (deterministic)     │
       │     for each table: propose dims/measures/dim_groups │
       │     /derived_tables based on fingerprint+MDM+baseline│
       │                                                      │
       │   when --with-llm:                                   │
       │     compute grounding signals + table narrative      │
       │     LlmAgent (gemini-3.1-pro, T=0,                   │
       │              output_schema=EnrichmentPlan,           │
       │              response_mime_type=application/json)    │
       │     LLM authors: reasoning, refined descriptions,    │
       │                  real risks, real questions          │
       │     fallback to skeleton on any LLM failure          │
       │                                                      │
       │   write review_queue/<table>.plan.md                 │
       │   write data/plans/<table>.plan.json                 │
       └──────────────────────────┬───────────────────────────┘
                                  │
                                  │ EnrichmentPlan +
                                  │ markdown for human
                                  │
       ╔══════════════════════════▼═══════════════════════════╗
       ║              HUMAN APPROVAL GATE                     ║
       ║                                                      ║
       ║   review_queue/<table>.plan.md                       ║
       ║                                                      ║
       ║   open in editor; tick:                              ║
       ║     - [x] ✅ APPROVED      OR                        ║
       ║     - [x] ❌ REJECTED                                ║
       ║          + feedback                                  ║
       ║                                                      ║
       ║   Phase 2 blocks until check_approvals passes        ║
       ╚══════════════════════════╤═══════════════════════════╝
                                  │
                                  ▼
       ┌──────────────────────────────────────────────────────┐
       │ STAGE 5 — ENRICH (parallel, capped at 5 concurrent)  │
       │   For each approved table:                           │
       │     LlmAgent (gemini-3.1-pro, T=0)                   │
       │     prompt = base + plan + baseline_gaps +           │
       │              ## Table narrative ◄── HOLISTIC FIRST   │
       │              ## Grounding signals ◄── EVIDENCE       │
       │              ## Confidence rules                     │
       │              ## SKILL.md sections 1-7                │
       │     output_schema = EnrichedOutput                   │
       │     self-repair retry × 2 if check_enrichment fails  │
       │     write data/enriched/<table>.json (resumable)     │
       └──────────────────────────┬───────────────────────────┘
                                  │ EnrichedOutput dict
                                  ▼
       ┌──────────────────────────────────────────────────────┐
       │ STAGE 6 — VALIDATE (deterministic)                   │
       │   coverage_check: every gold query answerable?       │
       │   reconstruct_sql_check: enriched LookML produces    │
       │                          equivalent SQL              │
       │   regression detection vs prior coverage             │
       │   write coverage_report.json                         │
       └──────────────────────────┬───────────────────────────┘
                                  │
                                  ▼
       ┌──────────────────────────────────────────────────────┐
       │ STAGE 7 — PUBLISH (deterministic)                    │
       │   additive_merge_view per table                      │
       │     - 30-char quality threshold for descriptions     │
       │     - tags always cumulative                         │
       │     - sql/type/primary_key NEVER overwritten         │
       │   write output/views/<table>.view.lkml               │
       │   write output/models/lumi_enriched.model.lkml       │
       │   write metric_catalog.json                          │
       │   write filter_catalog.json                          │
       │   write golden_questions.json                        │
       │   write proposed_overwrites.md (audit trail)         │
       │   write uncertain_fields.md (LLM admitted guesses)   │
       └──────────────────────────────────────────────────────┘
```

---

## Level 3 — Context engineering payload

The complete context bundle that flows into every Gemini call. Ordering
matters: holistic narrative first, per-column evidence second, prescriptive
LookML rules last.

```
                        ╔═══════════════════════════════════════╗
                        ║    INSTRUCTION → Gemini 3.1 Pro       ║
                        ╚═══════════════════════════════════════╝
                                          ▲
                                          │ assembled prompt
                                          │
        ┌─────────────────────────────────┼──────────────────────────────────┐
        │                                                                    │
   ┌────┴────────────────────────────────────────────────────────────────────┴────┐
   │                         enrich.py::build_enrichment_prompt                   │
   │                                                                              │
   │  1. Base template (lumi/prompts/enrich_view.md)                              │
   │     - {table_name}, {table_mdm_description}, {selected_mdm_columns}, ...     │
   │                                                                              │
   │  2. ## Approved enrichment plan (scope contract)                             │
   │     - proposed dims/measures/derived_tables/explore                          │
   │     - reasoning + risks (LLM-authored if --with-llm)                         │
   │                                                                              │
   │  3. ## Baseline gap analysis                                                 │
   │     - 12 dims missing description                                            │
   │     - 4 measures missing value_format                                        │
   │     - PRIMARY_KEY_COLUMN: rpt_dt_pk (preserve!)                              │
   │                                                                              │
   │  4. ## Table narrative ◄── HOLISTIC UNDERSTANDING FIRST                      │
   │     - Identity (table_type=DERIVED, feed_type=LumiFirst, retention=3650d)    │
   │     - MDM table description paragraph                                        │
   │     - Owner header (jane.doe@aexp.com, IMR-CM-INSIGHTS)                      │
   │     - Description corpus grouped by topical cluster                          │
   │       Customer / Identity: cm11, cm15, cust_xref_id                          │
   │       Time / Reporting:    rpt_dt, snapshot_ts                               │
   │       Financial:           billed_business, fee_amt                          │
   │       Codes / Categories:  bus_seg, data_source                              │
   │     - PII role assignments (cm11 → NGBD-SDE-CM11) ◄── cm11 grounding         │
   │     - Domain aliases analysts gave columns (total_revenue ← billed_business) │
   │     - Baseline rename pairs (customer_segment ← bus_seg)                     │
   │     - Named intermediate concepts (CTE/temp table names)                     │
   │     - Canonical filter values (allowed_values w/o BQ)                        │
   │     - MDM-declared formulas (derived_logic verbatim)                         │
   │     - Inferred grain (Cardmember × day)                                      │
   │                                                                              │
   │  5. ## Grounding signals ◄── PER-COLUMN EVIDENCE                             │
   │     - Table role + partition column                                          │
   │     - Primary-key candidates (ranked, scored, with reason chains)            │
   │       cm11 [GROUNDED, score 17]                                              │
   │         · name pattern matches *_id / cmNN                                   │
   │         · used as JOIN key in 4 queries                                      │
   │         · COUNT(DISTINCT cm11) seen                                          │
   │         · MDM pii_role_id set                                                │
   │     - Join relationships (observed/MDM/inferred)                             │
   │     - explore.always_filter candidates (mdm_partitioned)                     │
   │     - hidden: yes candidates (technical/audit fields)                        │
   │     - drill_fields ordering (top SELECT-frequency)                           │
   │     - Filtered-measure candidates (CASE-WHEN-in-SUM patterns)                │
   │     - Per-column intelligence row per column:                                │
   │       cm11 [grounded] | agg:COUNT-DISTINCT×2 | join:4 | name:id_like |       │
   │       mdm:partition,pii  | base:has-desc                                     │
   │                                                                              │
   │  6. ## Confidence-labeling rules                                             │
   │     "For EVERY field, set confidence to grounded|inferred|guessed.           │
   │      `guessed` MUST go to uncertain_fields. Do NOT silently emit             │
   │      speculative descriptions as if grounded."                               │
   │                                                                              │
   │  7. ## LookML SKILL.md sections 1-7                                          │
   │     - SQL pattern → LookML pattern mapping                                   │
   │     - Required attributes checklist                                          │
   │     - primary_key + symmetric aggregates                                     │
   │     - Relationship inference rules                                           │
   │     - Refinements (additive merge pattern)                                   │
   └──────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          │ output_schema=EnrichedOutput
                                          │ T=0, mime=application/json
                                          ▼
                        ┌───────────────────────────────────────┐
                        │           EnrichedOutput              │
                        ├───────────────────────────────────────┤
                        │ view_lkml          (additive merge ⤵) │
                        │ derived_table_views                   │
                        │ explore_lkml                          │
                        │ filter_catalog                        │
                        │ metric_catalog                        │
                        │ nl_questions                          │
                        │ field_confidences                     │
                        │ uncertain_fields    ◄── LLM honest    │
                        │ proposed_overwrites                   │
                        └───────────────────────────────────────┘
```

---

## Level 4 — TableContext composition and signal flow

How signals from three sources fuse into a single TableContext that
feeds grounding signals + table narrative for one table.

```
   ┌─────────────────────────────────────────────────────────────────────┐
   │                      INPUTS (on disk)                               │
   ├─────────────────────────────────────────────────────────────────────┤
   │                                                                     │
   │  data/gold_queries/Q*.sql      data/looker_master/**/*.view.lkml   │
   │  ┌─────────────────┐           ┌──────────────────┐                │
   │  │ 138 SQL queries │           │ 29 baseline files│                │
   │  │ (122 valid +    │           │ (canonical or    │                │
   │  │  35 empty cells │           │  prefix variants)│                │
   │  │  + 1 real err)  │           └──────────────────┘                │
   │  └─────────────────┘                                                │
   │                                                                     │
   │  data/mdm_cache/<table>.json                                        │
   │  ┌──────────────────────────────────────┐                          │
   │  │ Per-table digest with:               │                          │
   │  │  - 17 dataset_details fields         │                          │
   │  │  - ownership w/ business+tech        │                          │
   │  │    contacts + imr_queue              │                          │
   │  │  - 30 per-column fields incl.        │                          │
   │  │    is_primary, is_dedupe_key,        │                          │
   │  │    pii_role_id, is_partitioned,      │                          │
   │  │    is_clustered, derived_logic       │                          │
   │  │  - *_extra catch-all dicts for       │                          │
   │  │    forward-compat                    │                          │
   │  └──────────────────────────────────────┘                          │
   └────────────────────────┬────────────────────────────────────────────┘
                            │
                            ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                  sql_to_context.py                                  │
   │                                                                     │
   │  parse_sqls (sqlglot)        ─┐                                     │
   │     produces SQLFingerprint   │                                     │
   │     [tables, joins, ctes,     │                                     │
   │      temp_tables, filters,    │                                     │
   │      aggregations,            │                                     │
   │      case_whens,              │                                     │
   │      date_functions,          │                                     │
   │      select_aliases]          │                                     │
   │                               │                                     │
   │  discover_tables (per table): │                                     │
   │     1. group fps by table     │                                     │
   │     2. attribute CTE/temp     │                                     │
   │        source tables          │                                     │
   │     3. fetch MDM digest       ─┼────► TableContext                  │
   │        (CachedMDMClient)      │      (Pydantic, ~40 fields)         │
   │     4. parse baseline LookML  │                                     │
   │        via _parse_baseline    │                                     │
   │        _view (lkml lib)       │                                     │
   │       - full structural       │                                     │
   │         extraction (12 new    │                                     │
   │         signals)              │                                     │
   │       - quality_signals       │                                     │
   │     5. _build_mdm_dataset    ─┘                                     │
   │        _details (collapse)                                          │
   └─────────────────────────────┬───────────────────────────────────────┘
                                 │
                                 ▼ TableContext
   ┌─────────────────────────────────────────────────────────────────────┐
   │                    GROUNDING + NARRATIVE                            │
   │                                                                     │
   │   grounding.py::build_grounding_signals(ctx, all_fps, by_table)     │
   │   ──────────────────────────────────────────                        │
   │     per-column ColumnUsageProfile:                                  │
   │       - SELECT/WHERE/JOIN/AGG/DATE counts                           │
   │       - observed_values (allowed_values w/o BQ)                     │
   │       - MDM hints + baseline status                                 │
   │       - name_signal pattern detection                               │
   │     PrimaryKeyCandidates (ranked w/ reason chains)                  │
   │     JoinHints (observed + cardinality-inferred)                     │
   │     AlwaysFilterCandidates (MDM partition + freq)                   │
   │     HiddenCandidates (technical suffix + unused)                    │
   │     FilteredMeasureCandidates (CASE-WHEN-in-SUM)                    │
   │     column_confidence per col: grounded/inferred/guessed            │
   │                                                                     │
   │   narrative.py::build_table_narrative(ctx, all_fps)                 │
   │   ─────────────────────────────────────                             │
   │     identity + ownership header                                     │
   │     description corpus by topical cluster                           │
   │     PII role assignments per col (cm11-class signals)               │
   │     analyst alias glossary (quality-filtered)                       │
   │     baseline rename pairs                                           │
   │     CTE/temp-table semantic concepts                                │
   │     filter-value frequencies                                        │
   │     MDM-declared formulas                                           │
   │     inferred grain synthesis                                        │
   └─────────────────────────────┬───────────────────────────────────────┘
                                 │
                                 ▼ rendered Markdown sections
   ┌─────────────────────────────────────────────────────────────────────┐
   │   enrich.py::build_enrichment_prompt assembles:                     │
   │   - base template                                                   │
   │   - approved plan (scope contract)                                  │
   │   - baseline gap analysis                                           │
   │   - table narrative (HOLISTIC FIRST)                                │
   │   - grounding signals (EVIDENCE SECOND)                             │
   │   - confidence rules                                                │
   │   - SKILL.md excerpt                                                │
   │                                                                     │
   │   → fed to LlmAgent for plan stage (--with-llm)                     │
   │   → fed to LlmAgent for enrich stage (every approved table)         │
   └─────────────────────────────────────────────────────────────────────┘
```

---

## Failure-mode flow

```
                    ┌────────────────────────────┐
                    │ Pipeline starts            │
                    └────────────┬───────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
       ┌──────────┐       ┌──────────┐       ┌────────────┐
       │ Auth     │       │ Per-table│       │ Validation │
       │ failure  │       │ runtime  │       │ regression │
       │          │       │ error    │       │            │
       └────┬─────┘       └────┬─────┘       └─────┬──────┘
            │                  │                   │
            ▼                  ▼                   ▼
       ┌──────────┐       ┌──────────────┐   ┌──────────────┐
       │ HALT     │       │ Log warning, │   │ Run completes│
       │ Pipeline │       │ skip table,  │   │ exit code 1  │
       │ stops    │       │ continue.    │   │ Coverage     │
       │          │       │ Partial      │   │ report has   │
       │          │       │ output       │   │ regressions  │
       └──────────┘       │ produced.    │   │ list.        │
                          └──────────────┘   └──────────────┘
                                 │
                                 ▼
                          ┌──────────────────┐
                          │ Per-table        │
                          │ specific failures│
                          │ logged in:       │
                          │  - PipelineResult│
                          │    .failures     │
                          │  - lumi_status.md│
                          │  - per-plan.md   │
                          │    "Authored by:"│
                          │    badge         │
                          └──────────────────┘
```

---

*Last updated: 2026-05-04. See `ARCHITECTURE.md` for detailed design rationale and tradeoff discussion.*
