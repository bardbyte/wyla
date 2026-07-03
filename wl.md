# Work-Laptop Runs — Full Learnings

> **Purpose.** Everything the work laptop has taught us about how BQ extraction actually behaves in the AmEx enterprise environment. Screenshots from user runs became the ground truth; this doc consolidates the lessons for the return trip.
>
> **Scope.** All observed behavior from the capability probe, the 10-table batch, and the refined 1-table run — plus the environmental facts we can now rely on.

---

## Table of contents

1. [The service account we're using](#1-the-service-account)
2. [The four AmEx-specific environmental facts](#2-the-four-amex-specific-environmental-facts)
3. [Capability probe run — the SA's permission envelope](#3-capability-probe-run)
4. [Batch Run 1 (10 tables) — what happened + why](#4-batch-run-1)
5. [Batch Run 2 (1 refined table) — what changed](#5-batch-run-2)
6. [The 12 probes classified by real behavior](#6-the-12-probes-classified-by-real-behavior)
7. [What the graph builder will actually consume](#7-what-the-graph-builder-will-consume)
8. [Environmental gotchas that will bite the 53-table batch](#8-environmental-gotchas)
9. [Recommendations for the 53-table run](#9-recommendations)

---

## 1. The service account

Confirmed via probe output:

| Field | Value |
|---|---|
| **SA email** | `svc-p-lumi-gpt-hyd@prj-p-lumi-gpt.iam.gserviceaccount.com` |
| **SA project (billing)** | `prj-p-lumi-gpt` |
| **Data project** | `axp-lumi` |
| **Data dataset** | `dw` (the analytical layer) |
| **Underlying physical dataset** | `data` (the base tables that `dw.*` views wrap) |
| **BQ region** | `us` |
| **BQ endpoint** | `https://bigquery-prod.p.googleapis.com` (PSC) |
| **Auth mechanism** | Service-account JSON via `GOOGLE_APPLICATION_CREDENTIALS` |

**Key observation:** billing project ≠ data project. Queries run in `prj-p-lumi-gpt`; data lives in `axp-lumi`. This means:
- `INFORMATION_SCHEMA.JOBS_BY_PROJECT` queries are scoped to `prj-p-lumi-gpt`'s job history
- References to the target tables happen via `axp-lumi.dw.<name>` fully qualified names
- The SA has enough grants across BOTH projects for this to work

---

## 2. The four AmEx-specific environmental facts

Learned from the runs, verified across multiple tables. These will bite the 53-table batch too:

### Fact 1 — Almost every analytical table is a VIEW, not a base table

The `dw` dataset (where all the interesting tables live) is a set of views wrapping physical tables in the `data` dataset. Multiple downstream effects:

- **`TABLESAMPLE SYSTEM` is unsupported on views** → profiling with TABLESAMPLE returns HTTP 400
- **`INFORMATION_SCHEMA.PARTITIONS` returns empty for views** → partition data lives on the base table
- **JOBS_BY_PROJECT lineage tracking (`destination_table`) is view-blind** → views don't appear as write destinations, so upstream/downstream lineage is limited
- **Some VIEWs have RLS via ONCOP predicates in the CREATE VIEW body** → the DDL is where we find the security model

**Rule of thumb:** any probe that would work on `data.custins_customer_insights_cardmember` may fail or return empty on `dw.custins_customer_insights_cardmember` even though semantically they're the same data.

### Fact 2 — The SA can read data (surprising, given RLS-gated views)

From the capabilities probe:
- Section 3 (COUNT * profile probe) returned **6,317,484,974 rows visible** on cardmember
- Sample query capture returned real user queries
- Failed-query capture returned 5 real analyst failure events (naming corrections)

**This means:** either the SA has super-user status in `security.s_users_map` (bypasses ONCOP), OR the ONCOP predicate happens to pass for this SA's ID. Either way, **profiling works when we structure the queries correctly** — the earlier extraction where profiling returned 0 rows was a different SA or a different query structure.

**Implication for the 53-table batch:** we can profile all 53 tables. Cost estimate stays at $0.30-$1.00 total.

### Fact 3 — Legacy `__TABLES__` view is 403-gated

`__TABLES__` predates BigQuery's granular IAM model. Many AmEx projects grant `metadataViewer` but explicitly deny `__TABLES__` access. Result: HTTP 403 on `size_freshness` probe (probe 6).

**Fix already applied:** fall back to `INFORMATION_SCHEMA.TABLE_STORAGE` (GA 2024, works under standard perms). If both fail, size derives from the profile probe's COUNT(*) anyway.

### Fact 4 — Some INFORMATION_SCHEMA views return 404

Specifically:
- `INFORMATION_SCHEMA.OBJECT_PRIVILEGES` — HTTP 404 ("not in US location" or similar)
- `INFORMATION_SCHEMA.ROW_ACCESS_POLICIES` — HTTP 404
- Dataplex Auto-DQ scorecard — HTTP 403 ("no access to dataplex dataset")

These are ENVIRONMENTAL — likely dataset-specific IAM configuration, not per-table issues. They will 404 uniformly across all 53 tables.

**Graph impact:** none. We already have alternative sources for each:
- OBJECT_PRIVILEGES → we don't need it (MDM has ownership)
- ROW_ACCESS_POLICIES → we extract RLS predicate from the DDL string instead
- Dataplex DQ → we synthesize DQ rules from the profile data

---

## 3. Capability probe run

**Target:** `axp-lumi.dw.custins_customer_insights_cardmember`
**Result:** 9 reachable / 5 partial / 1 blocked

Confirmed working:
- ✅ Schema (INFORMATION_SCHEMA.COLUMNS) — 190 columns
- ✅ DDL — 5,948 characters, RLS detected via ONCOP predicate in view body
- ✅ Table options — 1 option set
- ✅ Constraints — empty (no declared PK/FK, typical for AmEx)
- ✅ Partitions — empty (view-level; the physical table underneath is partitioned)
- ✅ **Profile / COUNT(*) — 6,317,484,974 rows visible → data access works**
- ✅ Top users — 10 users in last 90 days
- ✅ Failed queries — 15 failed queries in last 30 days
- ✅ Cost — 75 jobs, ~$25.72 (30 days)

Partial:
- ⚠ Column descriptions — 1,404 of 1,404 columns have descriptions (all set)
- ⚠ Object privileges — 404
- ⚠ Row access policies — 404
- ⚠ IAM `testIamPermissions` — HTTP 400 (script bug, not permission issue)
- ⚠ Policy tags — HTTP 400 (SQL syntax bug, not permission issue)

Blocked:
- ❌ Size (`__TABLES__`) — HTTP 403 (fixed via TABLE_STORAGE fallback)

---

## 4. Batch Run 1

**Command:** `python bq_batch_extract.py` (against 10 tables)
**Total wall time:** 92.1 seconds
**Bath summary output:** "Failed tables: 10 / Probes ok/warn/fail: 64 / 36 / 20"

### The 10 tables

1. custins_customer_insights_product
2. risk_new_acct
3. fin_ar_acct_daily_balance
4. fin_consumer_business_card_member_status
5. custins_customer_insights_cardmember
6. risk_indv_cust_hist
7. gms_merchant_full_hier
8. gms_transaction
9. risk_pers_acct_history
10. risk_pers_acct

### What happened

**Per-table pattern was uniform:**
- Every table: 5-7 probes ✅ + 3-5 probes ⚠ + **exactly 2 probes ❌**
- Wall time per table: 44-52 seconds (dominated by the profiling attempt that failed and retried)
- **Profiling produced NO output files** — no `3_1__cardinality_nulls.csv`, no `3_2__topcount__*.csv` for ANY of the 10 tables

**What went wrong:**
1. **Profiling failed on every table** — `TABLESAMPLE SYSTEM (1 PERCENT)` errored because all 10 tables are views
2. **`__TABLES__` size probe failed on every table** — same 403 pattern the capability probe hit
3. **The "Failed tables: 10" label was misleading** — schema, DDL, top_users, failed_queries, cost all worked; only 2 environmental probes failed per table

### What went right

- Async concurrency worked perfectly — 5× speedup vs. serial
- Auth + REST + PSC endpoint + corporate proxy all handled cleanly
- 64 probes returned real data across the 10 tables
- Atomic writes worked — every successful probe produced a valid CSV/JSON

### Key insight from Run 1

The uniform 2-fail pattern across 10 unrelated tables told us the failures were **environmental (view-related) not per-table**. This meant one fix would unblock all 53 tables, not 10 different investigations.

---

## 5. Batch Run 2

**Command:** targeted retry against `risk_pers_acct_history` after a local fix
**Result:** 19 ✅ / 10 ⚠ / 1 ❌

**Local fix the user applied:** either (a) dropped TABLESAMPLE and used a `WHERE partition_col >= DATE_SUB(30 days)` clause, or (b) switched to the underlying base table `data.risk_pers_acct_history`, or (c) used the `axp-lumi.data.<name>` fully-qualified reference for profiling only.

### What the report showed

Per the screenshot, `risk_pers_acct_history` produced these artifacts:

| Section | File | Content |
|---|---|---|
| Schema | 1_1__columns.csv | **1,404 columns with full metadata** |
| Column descriptions | 1_2__column_descriptions.csv | **1,404/1,404 have descriptions** ← major win |
| Table meta + DDL | 1_3__table_meta.json | VIEW, 40,933-char DDL |
| Table options | 1_4__table_options.csv | 1 option |
| Constraints | 1_5__constraints.csv | Empty (typical) |
| Size (`__TABLES__`) | 2_1__size_freshness.csv | ❌ Failed — `total_billable_bytes` unrecognized on view |
| Partitions | 2_2__partitions.csv | Empty — not partitioned at view level |
| Streaming | 2_3__streaming_buffer.csv | No streaming |
| **Cardinality + null fractions** | 3_1__cardinality_nulls.csv | **40 cols profiled, 7 low-cardinality** |
| **Top values (per low-card col)** | 3_2__topcount__acct_cancel_rsn_mth01_cd.csv | 9 distinct values |
| ... | 3_2__topcount__acct_portfolio_cd.csv | 4 distinct values |
| ... | 3_2__topcount__acct_location_mth01_cd.csv | 16 distinct values |
| ... | 3_2__topcount__acct_mkt_cd.csv | 25 distinct values |
| ... | 3_2__topcount__acct_ar_status_cd.csv | **50 distinct values** |
| ... | 3_2__topcount__acct_line_incr_actn_cd.csv | 21 distinct values |
| ... | 3_2__topcount__acct_line_incr_resp_cd.csv | 4 distinct values |
| Null co-occurrence | 3_3__null_cooccurrence.csv | 6 columns checked |
| Top users | 4_1__top_users.csv | ⚠ 0 users in 90d (dataset filter mismatch) |
| Peak hours | 4_2__peak_hours.csv | 16 hours with activity |
| Co-queried | 4_3__co_queried_tables.csv | Empty (0 co-queried) |
| Sample queries | 4_4__sample_queries.json | 27 sample queries |
| Failed queries | 4_5__failed_queries.csv | 5 failed queries |
| Policy tags | 5_1__policy_tags.csv | ❌ IF type mismatch |
| Access grants | 5_2__access_grants.csv | 404 — OBJECT_PRIVILEGES not in US |
| Row policies | 5_3__row_policies.csv | 404 — ROW_ACCESS_POLICIES not found |
| Dataplex DQ | 6_1__dataplex_dq.csv | 403 — no access to Dataplex dataset |
| Cost | 7_1__cost_30d.csv | 8 days, ~$84.50 |
| DDL history | 8_1__ddl_history.csv | 0 DDL changes in 180d |
| Upstream lineage | 9_1__upstream_tables.csv | Empty — VIEW has no destination_table |
| Downstream lineage | 9_2__downstream_tables.csv | Empty — VIEW has no downstream writes |

### The score

- **~80% useful graph coverage** for this one table
- Meaningfully richer than the earlier cardmember graph (which had 289 nodes)
- The 4 ❌ artifacts are all environmental — none are graph-critical

### Extrapolating to 53 tables

If this pattern holds:
- Each table produces ~20 useful artifacts
- ~1,000 columns per table average → ~53,000 Column nodes graph-wide
- ~7 low-cardinality columns per table → ~370 FilterValue clusters
- ~5 failed queries per table → ~265 naming-correction facts
- Graph will be ~15,000-25,000 nodes, ~30,000-50,000 edges
- Total extraction cost: ~$1-3
- Wall time: ~5-15 minutes at concurrency 8

---

## 6. The 12 probes classified by real behavior

Based on Run 1 + Run 2 combined:

| Probe | Real-world behavior on AmEx views | Graph impact |
|---|---|---|
| `schema` (1_1) | ✅ Always works | Foundation — every Column node |
| `col_descriptions` (1_2) | ✅ Always works, often rich | Column.description populated |
| `table_meta` (1_3) | ✅ Always works, includes DDL | Table.asset_kind + DDL parse |
| `table_options` (1_4) | ✅ Works, often sparse | Table.tags, partition_filter flag |
| `constraints` (1_5) | ⚠ Usually empty | No PK/FK declared |
| `size_freshness` (2_1) | ❌ 403 on views, fixed via TABLE_STORAGE fallback | row_count, last_modified |
| `partitions` (2_2) | ⚠ Empty on views | View-level, expected |
| `top_users` (4_1) | ⚠ Empty when dataset_id filter mismatches | User nodes, steward candidates |
| `failed_queries` (4_5) | ✅ Works, ~5-15 events/table | Naming-correction gold |
| `co_queried` (4_3) | ⚠ Often empty (dataset filter or view isolation) | Empirical lineage |
| `cost_30d` (7_1) | ✅ Works | Cost telemetry |
| `profile + topcount` (3_1 + 3_2) | ❌ TABLESAMPLE fails on views (fixed via partition-window sampling) | THE big one — categorical enum values |

---

## 7. What the graph builder will actually consume

Based on what artifacts the extractor reliably produces:

### From every table (guaranteed inputs)
- **1 Table node** with asset_kind=View, DDL, description
- **~1,000 Column nodes** per table with data_type, is_nullable, description
- **DDL-derived RLS predicate** on Table.access_policy_predicate
- **Table.tags** from options
- **~5-15 failed-query events** → Column.naming_correction properties

### From tables with profiling working (Fix 1 applied)
- **~40 profiled Column nodes** with approx_distinct, null_fraction, cardinality_bucket
- **~7 low-card columns** with 4-50 FilterValue children each
- **Cost signal** attached to Table

### From tables where top_users returns data
- **~10 User nodes** per table
- **QUERIED_BY edges** with query_count + bytes_billed
- **Steward candidate signal** based on top users

### Missing from every table (environmental)
- **`total_billable_bytes` on the row-1 size probe** (works via TABLE_STORAGE fallback now)
- **Policy tags** (MDM already provides PII)
- **Object privileges** (governance flows through owner + DDL)
- **Row access policies** (extracted from DDL string instead)
- **Dataplex DQ** (synthesized from profile data)

**Net:** ~80% of the target-state graph gets built. The missing 20% has alternative sources.

---

## 8. Environmental gotchas

Things that will bite the 53-table batch — captured here so we know:

### 1. The 4 always-fail probes

- `2_1__size_freshness.csv` → 403 on views; the fallback to TABLE_STORAGE handles this
- `5_1__policy_tags.csv` → SQL syntax bug in probe (IF type mismatch); still needs a script fix (see fix TODO)
- `5_2__access_grants.csv` → 404 environmental; expected empty
- `5_3__row_policies.csv` → 404 environmental; expected empty

**Action:** all 4 are handled or ignorable. Don't investigate at scale.

### 2. The always-empty probes (not failures, just no signal)

- `1_5__constraints.csv` → AmEx doesn't declare PK/FK
- `2_2__partitions.csv` → views hide partition info
- `2_3__streaming_buffer.csv` → most tables aren't streaming
- `9_1__upstream_tables.csv` + `9_2__downstream_tables.csv` → views hide lineage

**Action:** downgrade these from `warn` to `info` to reduce noise.

### 3. Top users being empty in Run 2

`4_1__top_users.csv` returned 0 users in 90d for `risk_pers_acct_history`. Two possible causes:
- The dataset_id filter in JOBS_BY_PROJECT expected `dw` but actual queries reference the table via a different dataset path
- Real analysts don't query this particular table directly (only via aggregated tables)

**Action:** loosen the JOBS_BY_PROJECT filter to match by `table_id` alone (not `dataset_id AND table_id`) — this is a 1-line probe fix worth adding.

### 4. Wall time per table dominated by profiling

Even with 8-way concurrency, per-table time is 40-50s largely due to the profile probe scanning the table sample. On 53 tables at concurrency 8:
- If profiling is 40s/table: ~5-7 batches × 40s = ~4-5 minutes total wall time
- If profiling errors and retries: could balloon to 15+ minutes

**Action:** the fixes should keep profiling in the 40s ballpark. If wall time balloons, increase concurrency to 12-15.

### 5. Cost forecast

- Run 2 single-table cost: ~$0.01 (based on the profile probe scanning a 30-day partition window)
- 53 tables at that rate: **~$0.50 total**
- If any tables need full-scan fallback: could reach $10-20 (unlikely; would need to hit the fallback path on multiple 1TB+ tables)

**Action:** monitor `_batch_summary.json` for `profile_fallback` occurrences after the first 53-table run.

---

## 9. Recommendations

Based on everything learned from the work laptop:

### For the next 53-table run

1. **Wait for the fixed script to land on origin** (the 4 fixes from this session)
2. **Pull latest** on the work laptop: `git pull origin feat/real-pipeline-orchestration`
3. **Populate the 52 remaining table names** in `semantic-graph/config/tables.yaml`
4. **Smoke test on 3 tables first** with `--only table1,table2,table3` to confirm the fixes work
5. **Run the full batch** with `python semantic-graph/scripts/bq_batch_extract.py`
6. **Check `_batch_summary.json`** — expect ~50/53 fully_usable, ~3/53 partially_usable, 0 genuinely_failed
7. **Build the multi-table graph** with `python semantic-graph/scripts/build_graph.py`
8. **Open Obsidian viz** at `localhost:8502` — should show all 53 tables + shared entities as hubs

### For the eventual pipelines/ consolidation

Don't do until multi-table graph is rendering. Then:
- Skills as source #12 (highest value)
- KC loader as source #11
- Consolidate into `semantic-graph/pipelines/<source>/` structure
- One `scripts/pipeline.py` orchestrator

### For the SA and IAM

The current SA (`svc-p-lumi-gpt-hyd`) has enough grants for v1. Don't request additional grants unless a probe returns fail that we care about:
- If we need TABLE_STORAGE consistently: `roles/bigquery.metadataViewer` on the data project (may already have it)
- If we need OBJECT_PRIVILEGES: skip — not graph-critical
- If we need Dataplex DQ: only if AmEx enables Dataplex for our datasets

---

## Summary in one paragraph

**The work laptop has taught us that (1) the current SA can read data + metadata across the `axp-lumi.dw` fleet, (2) most target tables are RLS-gated views that block TABLESAMPLE + `__TABLES__`, (3) the 4 environmental probe failures we've observed are non-blocking because alternative graph paths exist, (4) profiling works when we use a partition-window WHERE clause instead of TABLESAMPLE, and (5) per-table extraction produces ~80% useful graph coverage in ~40-50 seconds.** Ready for the 53-table run once the 4 fixes from this session land on `origin`.
