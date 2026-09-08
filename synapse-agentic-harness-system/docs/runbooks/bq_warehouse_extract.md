# Runbook · BigQuery warehouse extraction

`scripts/bq_warehouse_extract.py` produces the `real_extractions_production/`
archive that `laptop.py build-graph --bq-archive …` loads. One run, any list
of tables, resumable, budget-guarded, every gap named.

The contract it fulfils is `docs/contracts/bigquery_extraction_run_explained.md`
(the 40-section description of every artifact). This page is how to run it.

---

## 1 · The three identities (read this first)

| | value | role |
|---|---|---|
| billing project | `prj-p-lumi-gpt` | runs and bills every job. **Its `JOBS_BY_PROJECT` holds only our own jobs.** |
| logical dataset | `axp-lumi.dw` | the view layer analysts query |
| physical dataset | `axp-lumi.data` | the storage layer the views wrap: partitions, TABLESAMPLE, real bytes |

`INFORMATION_SCHEMA.JOBS_BY_PROJECT` is scoped to the project that *ran* a
job, not the project whose tables it read. The earlier runs asked the
billing project for history and saw nothing but the service account's own
queries. The analysts' jobs against `axp-lumi.dw.*` were run **in
`axp-lumi`** (Looker, notebooks, scheduled queries) or in projects we cannot
list at all. So the extractor reads history from three places, each with
its own status:

1. `` `axp-lumi`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT `` (and every
   other project under `job_projects`) — needs `bigquery.jobs.listAll` on
   that project.
2. `` `axp-lumi`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION `` — every
   project in the org; needs org-level `bigquery.jobs.listAll`. Carries no
   query text; matched on referenced tables.
3. `axp-lumi.data_backup.cloudaudit_googleapis_com_data_access` — the
   data-access audit sink. Data-access logs land on the project that **owns**
   the table, so this is the one source that sees a query from *any*
   project. Both sink formats (`metadataJson`, legacy `servicedata_v1`) are
   handled; the schema is probed first.

Profiling and value domains read the **physical** table when it is
reachable (the view is resolved to it automatically) and fall back to the
view with a partition window, since `TABLESAMPLE` is illegal on views.

## 2 · Setup

The connection is the one `scripts/bq_check.py` already proves. In
`synapse-agentic-harness-system/.env`:

```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/svc-p-lumi-gpt-hyd.json
BQ_PROJECT_ID=prj-p-lumi-gpt
BIGQUERY_URL=https://bigquery-prod.p.googleapis.com
LUMI_BQ_DATA_PROJECT=axp-lumi
# REQUESTS_CA_BUNDLE=/path/to/corporate-root.pem   (if TLS is intercepted)
```

```
pip install -e ".[enrich]"        # google-auth (+ truststore); pyyaml is core
python scripts/bq_check.py --table dw.gms_transaction
```

Tables live in `config/warehouse_tables.yaml`. Adding one is one line:

```yaml
tables:
  - name: gms_transaction
  - name: some_new_table                 # every default applies
  - name: roll_rate_calc
    logical_dataset: common              # per-table override
  - name: wide_history_table
    profile_budget: 50 GiB
    physical_name: wide_history_table_v3 # when the base table is named differently
```

`--add a,b,c` appends bare names for one run without editing the file;
`--tables a,b` restricts a run to names already there.

## 3 · Run it, in this order

```
# 1. what can this identity see, and WHERE is the history? (metadata + dry runs; one COUNT(*) capped at 1 GiB)
python scripts/bq_warehouse_extract.py probe --table gms_transaction

# 2. price the whole run without billing a byte (writes _plan/<run_id>.json)
python scripts/bq_warehouse_extract.py plan

# 3. smoke on two tables, then everything (both resumable)
python scripts/bq_warehouse_extract.py run --tables gms_transaction,wwcas_authorization
python scripts/bq_warehouse_extract.py run

# 4. after the run — the report, or re-derive the 30-day digests offline
python scripts/bq_warehouse_extract.py status
python scripts/bq_warehouse_extract.py index
```

Read the probe's history rows before anything else. A row that says
*"only OUR jobs — this is the billing project's view"* is the symptom; the
`axp-lumi` row and the `audit_log sink` row are where the analysts are.
If both are `DENIED`, the grant to ask for is `bigquery.jobs.listAll` on
`axp-lumi` (or read access to the audit sink), not more roles on the
billing project.

Exit codes: `0` complete · `1` completed with `ERROR`s · `2` config ·
`3` env/auth · `4` interrupted (checkpoint kept — rerun resumes).

## 4 · What one run does

| phase | what | statements | cost |
|---|---|---|---|
| shared | INFORMATION_SCHEMA TABLES / COLUMNS / COLUMN_FIELD_PATHS / TABLE_OPTIONS / VIEWS / PARTITIONS / constraints / ROUTINES / TABLE_STORAGE — **one statement per view for all tables**, split per table into `01–12` | ~15 | metadata |
| resources | `tables.get` on both layers, the physical table behind each view (the view's own dry run names what it reads; a partition-filter refusal names it too), `rowAccessPolicies.list`, `dw.get_table_metrics('<t>')` | 3–5 / table | metadata |
| profile | per table: **cost plan by dry run** → coverage mode → column statistics in chunks of 80 columns → exact value domains for low-cardinality candidates, over full history when the narrow scan fits `domain_budget` | 2–10 / table | the only real scan |
| history | one statement per source per UTC day into `_history/`, then the local indexer routes the corpus into every table's `17_queries_30d/` | 30 × sources | JOBS views are metadata; the audit sink is a real scan, per-day capped |
| report | `_summary.json` per table, `_batch_summary.*`, `_run_report.json/.md` | — | — |

Coverage modes written to `14_profile_coverage.json` and onto every
profile / domain row: `unpartitioned`, `full_non_null_partition_history`,
`recent_partitions_budgeted`, `single_partition_system_sample_budgeted`,
`system_sample_budgeted`, `full_non_null_partition_history_value_domain`.
A number in the archive always says what slice of the table it describes.

## 5 · Cost guards

* every statement that reads data is **dry-run first**; an estimate over
  its budget is `BUDGET_SKIPPED` and the plan file says why;
* `maximumBytesBilled` caps every real job server-side as well;
* `run_budget` (default 5 TiB) is the ceiling for the whole run; the
  ledger in `_run_report.json` lists every job with bytes processed and
  billed, and an on-demand equivalent at $6.25/TiB (informational: the
  project sits under a reservation);
* the extractor's own jobs are labelled `lumi-extract=warehouse-snapshot`
  and its identity is excluded from every usage digest.

## 6 · Status semantics (say these words to a peer)

`FETCHED` data · `EMPTY` the call succeeded and there is genuinely nothing ·
`DENIED` this identity may not know (a missing `16_row_access_policies.json`
plus a `denied_operations` row means *unknown*, and the graph fails closed
on it) · `NOT_FOUND` the object is not there · `BUDGET_SKIPPED` refused by
the cost guard, recorded · `CACHED` reused from `_state.json` · `ERROR`
something actually broke (exit 1).

## 7 · Resuming, forcing, forgetting

* a rerun reuses every finished task in `_state.json`; today's partial
  history day is always re-fetched;
* `--force a,b` re-extracts named tables; `--fresh` forgets everything;
* `--no-profile` / `--no-history` skip a phase for a metadata-only pass;
* `index` re-derives the digests from `_history/` with no network — change
  a retention limit in the yaml and re-index.

## 8 · Then

```
python scripts/laptop.py build-graph --bq-archive data/real_extractions_production …
```
