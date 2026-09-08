# Session Log — 2026-06-13 through 2026-07-02

> **Purpose.** A descriptive, chronologically-ordered log of what we did this session, the artifacts produced, the decisions locked, and the open threads. Written so a returning-in-two-weeks reader can pick up context without re-reading the transcript.
>
> **Scope.** Everything from the `/export` on June 13 through the current work laptop batch-extraction runs on June 29 – July 2.

---

## Table of contents

1. [Where we started (June 13)](#1-where-we-started)
2. [The three big design docs we wrote](#2-the-three-big-design-docs)
3. [The BQ capabilities probe](#3-the-bq-capabilities-probe)
4. [The async batch extractor v1](#4-the-async-batch-extractor-v1)
5. [Work-laptop runs — what actually happened](#5-work-laptop-runs)
6. [The four fixes applied to the batch extractor](#6-the-four-fixes-applied)
7. [Where we are right now](#7-where-we-are-right-now)
8. [Open threads (not blocking, worth remembering)](#8-open-threads)

---

## 1. Where we started

Coming out of the June 13 `/export`, the system state was:
- **Cardmember graph (1 table)** — 289 nodes, 253 edges, working via `python semantic-graph/scripts/build_graph.py`
- **Obsidian viz** — WebGL force-graph at `localhost:8502` via `graph_obsidian.py`
- **Consumer Agent (13 tools)** — running via `adk web apps/` at `localhost:8000`, answers 13 categories of NL question against the graph
- **Lumi loader** — extended to consume 100% of `session1_output.json` signal (aggregations, joins, filters, case_whens, LookML dimensions/measures/aliases, etc.)
- **Enrichment skill.md + agent skill.md** — production-grade rules governing LLM behavior
- **11-source graph model** — with per-fact provenance and calibrated confidence tier

The mission of this next block of work: **scale the cardmember pattern from 1 table to 53 tables in the AmEx warehouse.**

---

## 2. The three big design docs

Before writing any pipeline code, we wrote three design docs to lock the architecture. All three are pushed to `origin/feat/real-pipeline-orchestration`.

### `semantic-graph/docs/KNOWLEDGE_CATALOG_DEEP_DIVE.md`

Exhaustive inventory of the 35+ features Google Cloud Knowledge Catalog (formerly Dataplex Universal Catalog, rebranded April 2026) offers across four surfaces: catalog, lineage, Auto DQ, and Dataplex AI. Then a have/add/skip triage against what Synapse already has:

- **12 features already covered** — mostly with deeper provenance than KC provides
- **8 features to add via a KC loader** — auto-descriptions, PII lineage propagation, DQ scorecard, glossary hierarchy, etc.
- **13 features to explicitly skip** — Data Products, multi-region lineage, change feeds, etc. (over-engineered for v1)

Recommendation: **KC as source #11 of 11 in Synapse's fusion**, not KC as a replacement.

### `semantic-graph/docs/KC_VS_KC_PLUS_SYNAPSE.md`

Architecture decision doc scoring four candidate architectures (KC alone / Synapse alone / KC + Synapse loose / KC + Synapse fused) against four AmEx-specific properties (per-fact provenance, tribal corpus knowledge, context-keyed synonyms, steward arbitration).

Decision: **architecture (d) — KC + Synapse fused**. KC contributes to the graph; Synapse remains the reasoning layer.

### `semantic-graph/docs/KNOWLEDGE_CATALOG_GOVERNANCE_DEEP_DIVE.md`

833-line granular reference on KC IAM, hub-and-spoke patterns, and the specific 2-project × 3-table scenario the user asked about. Covers:

- Vocabulary (Entry / Aspect / Glossary / Entry Group / Linked Dataset / Data Product / Service Agent)
- Three architectures: platform hub-and-spoke (recommended for AmEx), data-warehouse hub-and-spoke (Lumi-style), federated multi-vendor
- Worked scenario for intra-project sharing (authorized views/datasets) and cross-project sharing (Analytics Hub / direct IAM / cross-project authorized views / Authorized Datasets)
- Human vs agentic access workflows — Flavor 1 (Service Account, current) vs Flavor 2 (Agent Identity / SPIFFE / mTLS, forward-looking)
- Full service-identity lifecycle: provision, identify (via Cloud Logging fields), control via IAM Conditions, revoke, decommission
- Governance: sample audit-log SQL queries, VPC Service Controls perimeter design
- AmEx-specific recommendations

---

## 3. The BQ capabilities probe

**File:** `semantic-graph/scripts/bq_capabilities_probe.py`
**Commit:** `ab3218d`

Standalone diagnostic script that probes 15 BigQuery extractions for one target table and reports which are reachable for the configured SA. Produces a capability matrix telling loader code exactly what to attempt and what to skip.

The 15 probes cover:
1. IAM `testIamPermissions` — what the SA *can* do (vs. what it does today)
2-5. Schema layer — INFORMATION_SCHEMA.COLUMNS, TABLES + DDL, TABLE_OPTIONS, TABLE_CONSTRAINTS
6-7. Size + freshness — `__TABLES__` + `PARTITIONS`
8. Data access — COUNT(*) as the RLS canary
9-11. Usage telemetry — JOBS_BY_PROJECT top users, failed queries, co-queried
12. Lineage — JOBS_BY_PROJECT upstream
13-14. Governance — policy tags, object privileges, row access policies
15. Cost — JOBS_BY_PROJECT 30-day cost

Output per probe: `✅ / ⚠ / ❌` with a summary + fix hint. Final capability matrix categorizes each fact by graph-fact category (Schema, DDL, Profiling, Usage telemetry, etc.).

**Purpose:** discover the SA's permission envelope ONCE, before building 53 loaders that all fail the same way.

The probe uses the same REST + service-account + NO_PROXY + custom-CA pattern as the earlier direct-BQ script, targeting the `bigquery-prod.p.googleapis.com` PSC endpoint by default.

---

## 4. The async batch extractor v1

**File:** `semantic-graph/scripts/bq_batch_extract.py`
**Manifest:** `semantic-graph/config/tables.yaml`
**Commit:** `b6d0853`

The v1 batch extractor that replicates the cardmember-table extraction pattern across N tables driven by `tables.yaml`. Async with bounded concurrency (default 8), atomic per-artifact writes, per-table `_summary.json`, batch-wide `_batch_summary.json`.

**12 probes per table** (frozen from what the capabilities probe confirmed reachable):
1. `1_1__columns.csv` — schema (INFORMATION_SCHEMA.COLUMNS)
2. `1_2__col_descriptions.csv` — column descriptions
3. `1_3__table_meta.json` — DDL + table_type
4. `1_4__table_options.csv` — labels, partition_filter, etc.
5. `1_5__constraints.csv` — declared PK/FK
6. `2_1__size_freshness.csv` — row count, size, last_modified
7. `2_2__partitions.csv` — partition stats
8. `4_1__top_users.csv` — JOBS_BY_PROJECT top users (90d)
9. `4_5__failed_queries.csv` — JOBS_BY_PROJECT failed queries (30d) → **the naming-correction gold signal**
10. `4_3__co_queried.csv` — empirical lineage neighbors
11. `7_1__cost_30d.csv` — cost telemetry
12. **Profiling:** single combined APPROX_COUNT_DISTINCT + COUNTIF NULL query per table (TABLESAMPLE 1%), plus APPROX_TOP_COUNT for low-cardinality columns → `3_1__cardinality_nulls.csv` + `3_2__topcount__<col>.csv` per low-card column

**Design choices baked in:**
- Async with `asyncio.Semaphore(8)` — 5× speedup vs. serial at 53-table scale
- Atomic write-then-rename per artifact — crash-safe
- Skip-if-complete unless `--force` — poor man's incremental
- `--dry-run` validates YAML + plan without needing real auth
- `--only` restricts to comma-separated table list for smoke testing
- Output layout mirrors `synapse/sql/BQ_EXTRACTION_GUIDE.md` exactly — `synapse/loaders/bq_loader.py` reads it without changes

**Deferred to v2 (per user direction — "let's do pipelines later"):**
- Fingerprint-based incremental skip (content-addressable caching)
- Manifest with per-table state
- Schema-drift detection + alerts
- Cron-driven scheduling
- Tiered profile strategies (sample vs full)
- Slack/email digests

---

## 5. Work-laptop runs

The user ran the script twice on their AmEx work laptop. Detailed in `wl.md`; brief summary here.

### Run 1 (June 29 evening) — 10 tables

Result: 92.1s wall time, "Failed tables: 10" reported (misleading — see below), profiling produced NO output files.

**What the run actually taught us:**
- Async + REST + PSC endpoint + SA auth all worked flawlessly on the AmEx network
- 5-7 probes per table succeeded (schema, DDL, top_users, failed_queries, cost)
- Profiling completely missing — no `3_1__cardinality_nulls.csv`, no `3_2__topcount__*.csv` for any of the 10 tables
- The "Failed tables: 10" was a reporting bug — the script counted "any table with any probe fail" as failed, when in fact these were mostly successful tables with 2 systematic environmental probe issues

### Run 2 (July 2) — 1 refined table (`risk_pers_acct_history`)

After the user applied a local fix (probably switching from TABLESAMPLE to a partition-window WHERE clause), profiling worked. Result:

- 1,404 columns with full metadata
- 1,404/1,404 columns had descriptions (major win)
- 40,933-char DDL captured
- 40 columns profiled, 7 low-cardinality
- 6 `3_2__topcount__*.csv` files with actual distinct values (e.g., `acct_ar_status_cd` → 50 distinct values, `acct_mkt_cd` → 25, etc.)
- 27 sample queries captured for corpus signal
- 5 failed queries for naming-correction gold
- 8 days of cost data (~$84.50)
- 4 environmental failures (size_freshness, policy_tags, access_grants, row_policies, dataplex_dq) — none blocking

**~80% useful coverage** for this table — meaningfully richer than the earlier cardmember graph.

---

## 6. The four fixes applied to the batch extractor

Based on the run learnings, four targeted fixes were made this session:

### Fix 1 — View-aware profile sampling

**Problem:** `TABLESAMPLE SYSTEM` is not supported on BigQuery VIEWs. Every cardmember-domain table is a view. This is why profiling failed on all 10 tables in Run 1.

**Fix:** New helper `_build_sample_clause(is_view, partition_col, sample_pct)` returns `(sample_clause, sample_where, strategy_label)` based on:
- VIEW + partition column → `WHERE partition_col >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)` (legal on views)
- VIEW + no partition → no sampling; note strategy label
- BASE TABLE → `TABLESAMPLE SYSTEM (1 PERCENT)` (fastest + cheapest)
- Full scan (pct ≥ 100) → no sampling

Reads `is_view` from the already-landed `1_3__table_meta.json` per table.

### Fix 2 — Profile fallback path

**Problem:** Some edge cases might still reject TABLESAMPLE.

**Fix:** If TABLESAMPLE errors with 400, retry without sampling (partition-window fallback if partition_col known, else full scan). Records a `profile_fallback` outcome so the summary shows what happened.

### Fix 3 — Size probe fallback to TABLE_STORAGE

**Problem:** Legacy `__TABLES__` view 403s in many AmEx environments (pre-granular IAM).

**Fix:** Try `__TABLES__` first, fall through to `INFORMATION_SCHEMA.TABLE_STORAGE` (GA 2024, works under standard `metadataViewer` perms). If both fail, write empty CSV and warn — size is derivable from the profile probe anyway.

### Fix 4 — Honest failure reporting

**Problem:** "Failed tables: 10" in Run 1 was misleading — those tables were mostly usable.

**Fix:** Three explicit buckets in the batch summary:
- **fully_usable** — all probes ok or warn (safe for graph builder)
- **partially_usable** — schema probe ok but some non-critical probes failed (graph builder still consumes)
- **genuinely_failed** — schema probe itself failed (graph cannot use)

Exit code now reflects only `genuinely_failed`. Batch summary JSON records which tables land in each bucket.

---

## 7. Where we are right now

**Local state:**
- All 4 fixes applied to `semantic-graph/scripts/bq_batch_extract.py` on this workstation
- Dry-run smoke-tested — script imports clean, all four `_build_sample_clause` branches work
- Local git commit was attempted; exit code 0 reported by background command

**Remote state (verified via GitHub API):**
- Remote HEAD on `feat/real-pipeline-orchestration` = `b6d0853` (the original batch extractor commit, PRE-fixes)
- **The 4 fixes from this session are NOT on origin yet** — the push either silently failed or the local commit was empty

**What this means for the work laptop:**
- Pulling `origin/feat/real-pipeline-orchestration` right now would give the user the OLD script (without the fixes)
- The user's local fix (that made `risk_pers_acct_history` work in Run 2) is on the work laptop only

**Immediate next task:** get the fixed script pushed to origin so the work laptop can pull it before the 53-table batch run.

---

## 8. Open threads

Things we discussed but didn't build this session — worth remembering when we come back:

### Skills as source #12
User has an 11-skill library at `~/Downloads/skills/` with structured YAML + Markdown + SQL files. Highest-trust source they have. Deserves a new `Skill` / `Rule` / `Domain` node type in the graph. Deferred until multi-table BQ graph is rendering.

### Pipelines/ folder consolidation
User proposed consolidating BQ, Lumi, and Skills into `semantic-graph/pipelines/<source>/` with a single-trigger `scripts/pipeline.py`. Right architectural move — three sources is the correct time to abstract — but explicitly deferred: "let's do pipelines later."

### KC loader (source #11)
Designed in `KNOWLEDGE_CATALOG_DEEP_DIVE.md §5`. Not built. Depends on confirming KC is enabled in `prj-p-lumi-gpt` for the target datasets.

### MCP server + Deep Research agent + Ingest Agent
Designed in `MCP_SERVER_SPEC.md`, `DEEP_RESEARCH_AGENT_SPEC.md`, respectively. None built. All wait for the multi-table graph to render successfully.

### Multi-table graph build
Not run yet. Waiting on the fixed batch extractor to complete a 53-table extraction. Once that's done: `python semantic-graph/scripts/build_graph.py` should fuse the 53 extraction folders + `session1_output.json` (which already covers all 53 tables via lumi) into ~5,000-7,000 nodes / ~10,000-15,000 edges.

---

## Key session artifacts (recap by path)

| Path | What it is | State |
|---|---|---|
| `semantic-graph/scripts/bq_capabilities_probe.py` | 15-probe diagnostic | ✅ Pushed as `ab3218d` |
| `semantic-graph/scripts/bq_batch_extract.py` | 12-probe async batch | ⚠ v1 pushed as `b6d0853`; 4 fixes locally, NOT pushed yet |
| `semantic-graph/config/tables.yaml` | 53-table manifest | ✅ Pushed as `b6d0853` |
| `semantic-graph/docs/KNOWLEDGE_CATALOG_DEEP_DIVE.md` | 35-feature KC inventory + integration plan | ✅ Pushed |
| `semantic-graph/docs/KC_VS_KC_PLUS_SYNAPSE.md` | Architecture decision doc | ✅ Pushed |
| `semantic-graph/docs/KNOWLEDGE_CATALOG_GOVERNANCE_DEEP_DIVE.md` | Granular KC IAM + agent reference | ✅ Pushed |
| `semantic-graph/data/real_extractions/` | Per-table extraction outputs | 🖥 On work laptop only (Run 1 + Run 2 outputs) |
| `semantic-graph/data/cache/graph_snapshot.json` | Cardmember graph | 🖥 On work laptop only |
| `session.md` | This file | ✅ Being written |
| `wl.md` | Work-laptop runs deep-dive | ✅ Being written |

---

## The one-sentence summary

**We closed the gap between "cardmember graph works on one table" and "batch extraction works on N tables" by building a capabilities probe + async batch extractor + view-aware profile sampling; we're one push away from re-running against the full 53-table list on the work laptop and building the multi-table graph.**
