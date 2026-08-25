# The 46-Table BigQuery Warehouse Extraction — What the Run Produces and What Every Artifact Means

> Transcribed verbatim from screenshots, in original section order (sections 1–40). No content omitted or invented.

---

You can explain the run to a peer like this:

> **This extraction creates a reproducible, auditable snapshot of what we know about each of the 46 BigQuery warehouse tables: its logical view, physical table, schema, partitions, size, statistical profile, categorical values, and the last 30 days of analytical usage. It also records exactly how complete each extraction was, what it cost to scan, what failed, and why.**

The output is deliberately organized in three levels: **run-level**, **shared/raw source-level**, and **per-table**.

---

# 1. Top-level output directory

The root is:

```text
data/real_extractions_production/
```

Conceptually:

```text
real_extractions_production/
│
├── _state.json
├── _run_report.json
├── _run_report.md
├── _batch_summary.json
├── _batch_summary.csv
│
├── _run_logs/
│
├── _shared/
│
├── _history/
│
├── table_1/
├── table_2/
├── ...
└── table_46/
```

Each area serves a different purpose.

---

# 2. `_state.json` — resumability/checkpoint state

```text
_state.json
```

This is operational state.

It remembers completed BigQuery jobs and extraction tasks so that if:

* the laptop disconnects,
* Python crashes,
* we stop with Ctrl-C,
* a BigQuery query times out locally,
* we restart the script,

the extractor can reuse completed work instead of starting over.

You can explain it as:

> **The extractor is checkpointed. `_state.json` is what makes the one-time pull resumable and prevents us from knowingly rerunning expensive successful queries.**

This is not primarily an analytical output. It is the extraction engine's memory.

---

# 3. `_run_logs/` — complete execution history

Every execution gets a unique run ID.

Example:

```text
_run_logs/
├── 20260823T123456Z_abcd1234.log
└── 20260823T123456Z_abcd1234.events.jsonl
```

The `.log` file contains exactly what you see on the terminal:

```text
[START]
[COST_PLAN]
[RUNNING]
[FETCHED]
[CACHED]
[EMPTY]
[DENIED]
[NOT_FOUND]
[BUDGET_SKIPPED]
[ERROR]
[COMPLETE]
```

The `.events.jsonl` file contains the same events in machine-readable form.

For example:

```json
{
  "ts": "...",
  "state": "FETCHED",
  "scope": "gms_merchant_char",
  "operation": "profile chunk 3",
  "message": "...",
  "job_id": "..."
}
```

This gives us a forensic record of the run.

If six months later someone asks:

> "Why don't we have row policies for this table?"

we don't have to guess.

We can see:

```text
DENIED:
Permission bigquery.rowAccessPolicies.list denied
```

---

# 4. `_run_report.json` — authoritative final run report

This is probably the most important single file after the extraction completes.

```text
_run_report.json
```

It summarizes the complete execution.

It contains:

```text
run ID
start/end time
overall status

46 configured tables
all table names

status counts
FETCHED
EMPTY
DENIED
NOT_FOUND
BUDGET_SKIPPED
ERROR
etc.

total bytes processed
total bytes billed

on-demand-equivalent cost

budget information

every denied operation
every missing object
every unresolved failure

every executed task
job IDs
errors
bytes
status
```

So this file answers:

> Did the complete extraction succeed?

> What did not succeed?

> How much did we scan?

> Which failures were permissions versus missing tables versus budget decisions?

> Which exact BigQuery jobs produced the data?

This is the file I would use for an audit/review of the extraction itself.

---

# 5. `_run_report.md` — human-readable version

Same basic information as the JSON report, but readable by a person.

It has a table along the lines of:

```text
Table | Physical | Profiled cols | Low-card cols | Jobs 30d | Audit 30d | Users
```

So a peer can quickly inspect the whole run without writing code.

---

# 6. `_batch_summary.json` and `.csv`

These are the cross-table inventory.

For every one of the 46 tables they contain fields such as:

```text
table
logical_exists
physical_exists
profiled_columns
low_cardinality_columns
jobs_30d
audit_30d
distinct_query_users_30d
statuses
```

Think of this as the **46-table scorecard**.

It allows us to quickly answer:

> Which tables actually exist?

> Which have physical-table access?

> How many columns were profiled?

> Which have lots of categorical fields?

> Which have recent analytical activity?

> Which have no recent usage?

> Which tables had extraction gaps?

---

# 7. `_shared/` — metadata extracted once for all 46 tables

Instead of querying metadata separately 46 times, the extractor pulls dataset-level metadata once and then splits it into table-specific outputs.

That directory contains things like:

```text
_shared/
├── dataset_dw.json
├── dataset_data.json
│
├── logical_tables.json
├── logical_tables.csv
├── logical_columns.json
├── logical_columns.csv
├── logical_column_field_paths.json
├── logical_column_field_paths.csv
├── logical_table_options.*
├── logical_views.*
│
├── physical_tables.*
├── physical_columns.*
├── physical_column_field_paths.*
├── physical_table_options.*
├── physical_partitions.*
│
├── logical_constraints.*
├── physical_constraints.*
│
├── routines.json
├── routines.csv
│
└── table_metrics_batch_*.json/csv
```

This is useful because it preserves the **original batch-level metadata evidence**, not merely the table-specific copies.

---

# 8. Each individual table gets its own directory

For example:

```text
real_extractions_production/
└── wwcas_authorization/
```

Inside that folder is everything we extracted specifically about that object.

The structure is sequential:

```text
00  object resources
01–05 logical/view metadata
06–12 physical metadata
13 storage metrics
14 statistical profiling
15 categorical values
16 row-policy metadata
17 analytical query history
```

That's an easy way to explain it.

---

# 9. `00_*` — raw BigQuery API table resources

```text
00_logical_table_resource.json
00_physical_table_resource.json
```

These come from BigQuery's `tables.get`.

They preserve the raw BigQuery object metadata.

So for:

```text
wwcas_authorization
```

we can distinguish:

```text
Logical:
axp-lumi.dw.wwcas_authorization
type = VIEW

Physical:
axp-lumi.data.wwcas_authorization
type = TABLE
```

These resources can contain:

```text
schema metadata
creation time
modification time
labels
partition configuration
clustering
description
view configuration
num rows/bytes where exposed
```

---

# 10. `01–05` — logical/user-facing layer

These describe:

```text
axp-lumi.dw.<table>
```

## `01_logical_table_meta.*`

From:

```text
dw.INFORMATION_SCHEMA.TABLES
```

Answers:

* Is this a view or table?
* What's its DDL?
* When was it created?
* What kind of logical object is it?

---

## `02_logical_columns.*`

Full user-facing schema.

Each row represents a column and can include:

```text
column_name
ordinal_position
data_type
is_nullable
is_partitioning_column
clustering position
policy tags
data policies
etc.
```

This is what you use to answer:

> What columns does an analyst see?

---

## `03_logical_column_field_paths.*`

Adds:

```text
nested field paths
descriptions
policy metadata
```

This is particularly useful for semantic documentation.

---

## `04_logical_table_options.*`

Includes labels/options such as:

```text
application name
environment
classification
support group
partition key labels
etc.
```

Where defined.

---

# 11. `05_view_definition.*`

You get:

```text
05_view_definition.json
05_view_definition.csv
05_view_definition.sql
```

The `.sql` file is particularly useful.

It is the **actual SQL defining the logical view**.

For your warehouse this matters because `dw` is largely an authorized-view layer.

So it tells us things such as:

```text
which physical objects the view reads
which columns it exposes
whether filtering/security logic exists
whether joins or transformations occur
```

You can explain this as:

> **We retain not only what the view looks like but also the SQL logic responsible for producing it.**

---

# 12. `06–12` — physical table layer

These describe:

```text
axp-lumi.data.<table>
```

where it exists.

## `06_physical_table_meta.*`

Basic physical table metadata.

---

## `07_physical_columns.*`

The complete physical schema.

This allows comparison between:

```text
dw view schema
vs.
data physical schema
```

So we can detect fields hidden from the authorized view.

---

## `08_physical_column_field_paths.*`

Descriptions/nested metadata for the physical table.

---

## `09_physical_table_options.*`

Physical labels/options.

---

# 13. `10_physical_partitions.*`

This is important for large tables.

For every physical partition we store things like:

```text
partition_id
total_rows
total_logical_bytes
last_modified_time
storage_tier
```

So this output can answer:

> How many partitions exist?

> What's the newest partition?

> What's the oldest?

> How large is each partition?

> Are some days much larger than others?

> When was each partition last modified?

It's also what the adaptive profiler uses to decide what subset of enormous tables is reasonable to scan.

---

# 14. `11` and `12` — declared constraints

```text
11_logical_constraints.*
12_physical_constraints.*
```

Where BigQuery declares:

```text
PRIMARY KEY
FOREIGN KEY
```

they appear here.

If BigQuery returns no constraints, the result is still meaningful:

```text
confirmed empty
```

rather than extraction failure.

---

# 15. `13_table_metrics.*`

This comes from the special function:

```sql
SELECT *
FROM `axp-lumi.dw.get_table_metrics`('<table>');
```

You get:

```text
13_table_metrics.json
13_table_metrics.csv
```

Currently we've proven it gives:

```text
table_name
total_rows
table_size_bytes
```

This answers:

> How many total rows are in the table?

> How large is it?

Across the 46 tables, this allows us to rank them by scale.

---

# 16. `14_profile_plan.json`

Before the extractor statistically profiles a table, it performs a cost plan.

This artifact records:

```text
number of supported columns

full-history estimated scan bytes

planned scan bytes

profile budget

full-history equivalent cost

planned equivalent cost

chosen coverage
```

For a huge table it might conceptually say:

```text
Full history:
900 TiB

Planned:
1.8 TiB

Decision:
recent_partitions_budgeted
```

This tells your peer:

> **The profiler doesn't blindly scan every historical byte. It estimates the cost first and selects an evidence window that fits the configured budget.**

---

# 17. `14_profile_coverage.json`

This tells us exactly **what part of the table the statistics describe**.

Possible modes include things like:

```text
unpartitioned
full_non_null_partition_history
recent_partitions_budgeted
single_partition_system_sample_budgeted
system_sample_budgeted
logical_fallback
```

It can also retain:

```text
partition column
partition predicate
range
sample percentage
```

This is critical.

If later someone sees:

```text
country_cd has 210 distinct values
```

they can determine whether that means:

```text
observed across full history
```

or:

```text
observed across the selected recent partition window
```

We do not mix those interpretations.

---

# 18. `14_column_profile.*`

This is the main statistical profile.

```text
14_column_profile.json
14_column_profile.csv
```

One row per profiled column.

Each row contains things like:

```text
column_name
data_type

total_rows

null_count
non_null_count
null_pct

approx_distinct

min_value
max_value
avg_value

coverage_mode
partition_predicate

profile_job_id
```

This lets us answer for virtually every scalar field:

> Is it usually populated?

> Is it sparse?

> How many unique values does it approximately have?

> What range does it span?

> Is it probably an identifier or categorical field?

> What data window was used?

---

# 19. `_profile_chunks/`

The extractor profiles wide tables in chunks.

Example:

```text
_profile_chunks/
├── chunk_0001.json
├── chunk_0002.json
├── ...
```

These are intermediate/raw query outputs.

Normally, a consumer should use:

```text
14_column_profile.csv
```

rather than reading the chunks.

But the chunks are preserved for reproducibility/debugging.

---

# 20. `15_low_cardinality_manifest.*`

This is the index of categorical/low-cardinality analysis.

For candidate columns it records:

```text
column_name
data_type

approx_distinct
exact_distinct_non_null

low_cardinality true/false
threshold

number of value rows

artifact path

BigQuery job ID

coverage_mode
partition_predicate
sample_percent
```

So it answers:

> Which columns were considered categorical?

> Which were actually confirmed as ≤1000 values?

> Were their values collected over full history or a bounded period?

---

# 21. `15_low_cardinality_values/`

This is one of the richest outputs.

Example:

```text
15_low_cardinality_values/
├── country_cd.csv
├── country_cd.json
├── trans_cd.csv
├── trans_cd.json
├── approval_status.csv
├── approval_status.json
├── ...
└── _group_*.json
```

For every confirmed low-cardinality column we store **the values themselves**.

Each value record can contain:

```text
column_name
data_type

value
is_null

value_count
pct_of_rows
rank

exact_distinct_non_null
total_rows

coverage_mode
partition_predicate
sample_percent
```

So:

```text
country_cd
```

could become:

```text
US    74,200,000    72.3%
GB     6,420,000     6.3%
CA     4,100,000     4.0%
...
```

This gives us actual observed domains, not just schemas.

---

# 22. Full-history categorical-domain optimization

There is a subtle but important behavior here.

The profile itself might be budget-bounded.

But once a column looks low-cardinality, the script separately asks:

> Can I afford to scan only this narrow categorical column across full history?

If yes, its value distribution can be:

```text
coverage_mode =
full_non_null_partition_history_value_domain
```

even though the wider statistical profile used only recent partitions.

So we get a much more complete categorical domain **without paying to scan thousands of unrelated columns across full history**.

---

# 23. `16_row_access_policies.json`

The extractor attempts:

```text
rowAccessPolicies.list
```

Where allowed, results go here.

In your environment, many of these are currently:

```text
DENIED
```

because of the missing permission.

That does not stop the extraction.

The important thing is that the run report explicitly records:

```text
unknown because access denied
```

rather than:

```text
there are no policies
```

Those are very different statements.

---

# 24. `17_queries_30d/` — analytical usage for that table

This is the entire 30-day usage package for an individual table.

Example:

```text
17_queries_30d/
│
├── jobs_30d.jsonl.gz
├── audit_30d.jsonl.gz
│
├── jobs_top_users.csv
├── jobs_peak_hours.csv
├── jobs_co_queried_tables.csv
├── jobs_daily_usage_cost.csv
├── jobs_failed_queries.json
├── jobs_query_templates.csv
│
├── audit_top_users.csv
├── audit_peak_hours.csv
├── audit_co_queried_tables.csv
├── audit_daily_usage_cost.csv
├── audit_failed_queries.json
├── audit_query_templates.csv
│
└── summary.json
```

There are deliberately two sources.

---

# 25. `jobs_30d.jsonl.gz`

This is raw matching activity from:

```text
JOBS_BY_PROJECT
```

It can preserve fields such as:

```text
job_id
user_email
creation_time
end_time
statement_type
state

error_result

total_bytes_processed
total_bytes_billed

destination_table
referenced_tables

query
```

This is the operational/recent BigQuery job history.

---

# 26. `audit_30d.jsonl.gz`

This is matching evidence from:

```text
axp-lumi.data_backup.cloudaudit_googleapis_com_data_access
```

It provides another history source containing things like:

```text
timestamp
principal
query text
referenced tables
referenced views
```

This is useful because the audit stream and `JOBS_BY_PROJECT` are different evidence sources.

---

# 27. Top-user files

```text
jobs_top_users.csv
audit_top_users.csv
```

Rows look conceptually like:

```text
user_email          query_count
analyst1@...        487
analyst2@...        242
service@...         180
```

Answers:

> Who actually uses this table?

> Who appears to use it most frequently?

---

# 28. Peak-hour files

```text
jobs_peak_hours.csv
audit_peak_hours.csv
```

Contains:

```text
hour_utc
query_count
```

Answers:

> At what times does usage peak?

---

# 29. Co-query relationships

```text
jobs_co_queried_tables.csv
audit_co_queried_tables.csv
```

Example:

```text
other_table                 co_query_count

gms_transaction             1,284
gms_merchant_full_hier        876
crt_currency                  391
```

Answers:

> What other tables do analysts normally use together with this table?

This can be very useful for data discovery.

---

# 30. Daily usage/cost

```text
jobs_daily_usage_cost.csv
audit_daily_usage_cost.csv
```

Contains per-day measures such as:

```text
date
query_count
bytes_processed
bytes_billed
failed_count
```

Answers:

> How often was this table used each day?

> Which days had unusually high usage?

> How much BigQuery processing was associated with those jobs?

---

# 31. Failed queries

```text
jobs_failed_queries.json
audit_failed_queries.json
```

These preserve a bounded number of failed query records.

These can reveal:

```text
wrong column references
invalid table names
partition-filter mistakes
deprecated fields
syntax problems
```

---

# 32. Query templates

```text
jobs_query_templates.csv
audit_query_templates.csv
```

This is particularly useful.

The extractor normalizes literals.

So:

```sql
WHERE account_id = 12345
```

and:

```sql
WHERE account_id = 98765
```

can become the same logical pattern:

```sql
WHERE account_id = ?
```

Each template can contain:

```text
query fingerprint

normalized SQL
sample original SQL

number of occurrences

distinct users

first seen
last seen
```

This answers:

> How is this table normally queried?

rather than drowning us in thousands of parameter variations of essentially the same SQL.

---

# 33. `17_queries_30d/summary.json`

This is the per-table usage summary.

It includes things such as:

```text
jobs_rows
audit_rows

distinct_users

first_seen
last_seen

bytes_processed
bytes_billed

history_days = 30

derived jobs summary
derived audit summary
```

The derived summaries can include:

```text
query_count
distinct_users
query_templates
failed_queries_retained
co_queried_tables
```

---

# 34. Root-level `_history/`

The per-table query folders are derived from a shared 30-day raw history corpus.

At the root:

```text
_history/
│
├── jobs_by_project/
│   └── prj-p-lumi-gpt/
│       ├── 2026-07-25.jsonl.gz
│       ├── 2026-07-26.jsonl.gz
│       ├── ...
│       └── 2026-08-23.jsonl.gz
│
├── audit_log/
│   ├── 2026-07-25.jsonl.gz
│   ├── ...
│   └── 2026-08-23.jsonl.gz
│
└── per_table_summary.json
```

Each day is extracted once.

Then the indexer routes relevant records into the corresponding table directories.

This means we don't run:

```text
30 days × 46 tables
```

separately.

We run:

```text
30 daily history extracts
```

and then index locally.

That is both faster and much cheaper.

---

# 35. `_summary.json` in every table folder

This is the first file I would open when examining a single table.

Example:

```text
wwcas_authorization/_summary.json
```

It summarizes:

```text
table name

logical object exists?
physical object exists?

metadata record counts

profile summary

30-day query summary

status counts
```

You can think of it as:

> **the table's extraction manifest / landing page.**

---

# 36. `_profile_summary.json`

A more focused summary of the statistical extraction.

It includes:

```text
source table

supported profile columns
profiled columns

low-cardinality candidates
confirmed low-cardinality columns

coverage

elapsed time

individual profile chunk results

full profile estimate
planned profile estimate

budget snapshot
```

This is what tells us whether profiling was truly comprehensive versus budget-bounded.

---

# 37. How "completeness" should be explained

This is important when discussing the run with peers.

There are really three types of completeness.

### Metadata completeness

For accessible tables, we're aiming for essentially complete metadata:

```text
all columns
all exposed descriptions
all physical partitions
all view SQL
all available constraints
all table labels/options
```

---

### Statistical completeness

Depends on table size.

Small/medium tables may have:

```text
coverage = full history
```

Huge tables may have:

```text
coverage = recent_partitions_budgeted
```

or a sample.

The output explicitly says which.

---

### Analytical-history completeness

Defined as:

```text
the requested last 30 calendar days
```

from our two available history sources.

Not lifetime query history.

---

# 38. Status semantics

This is another thing I'd emphasize to a peer.

We do not interpret all missing results as the same thing.

For example:

```text
FETCHED
```

means data was successfully obtained.

```text
EMPTY
```

means the query/API succeeded but there were genuinely zero records.

```text
DENIED
```

means the service account cannot inspect that information.

```text
NOT_FOUND
```

means the configured object wasn't found.

```text
BUDGET_SKIPPED
```

means the information was technically queryable but deliberately not scanned because the configured cost guard rejected it.

```text
CACHED
```

means a prior successful extraction was reused.

```text
ERROR
```

means something actually went wrong.

This prevents a dangerous mistake like:

```text
DENIED = no row policies
```

when what it actually means is:

```text
we cannot determine whether row policies exist.
```

---

# 39. Cost and resource accountability

The patched run also stores/prints:

```text
bytes processed
bytes billed

per-table scan usage

whole-run scan usage

on-demand-equivalent dollars

configured scan budget
```

For the run you're doing now, the live ledger already reached roughly:

```text
35.54 TiB billed
~$222 on-demand equivalent
```

before the final local history-indexing phase.

Again, that equivalent is an informational translation at $6.25/TiB; it isn't necessarily the organization's actual bill if the project is covered by capacity/reservations.

---

# 40. The best concise peer explanation

You could describe the final package this way:

> **For each of 46 warehouse objects, we're creating a structured evidence directory. It contains the BigQuery logical-view definition, physical-table metadata, the complete schemas from both layers, descriptions and governance tags, physical partition inventory, total row and storage metrics, a budget-aware statistical profile of every supported column, and actual distributions for confirmed low-cardinality fields.**
>
> **Separately, we extract 30 days of relevant BigQuery job history and audit-log evidence once, then index that history back to each table. Each table therefore gets its recent queries, users, failed queries, processing volume, common query templates, usage trends, and tables commonly queried alongside it.**
>
> **Everything is checkpointed and fully logged. The output explicitly distinguishes successful results, true empty results, access denials, nonexistent objects, and things intentionally skipped for budget reasons. For large tables, statistical coverage is recorded so we know whether a number represents full history, selected partitions, or a sample. Finally, the run produces both per-table summaries and a global report covering all 46 tables, extraction completeness, BigQuery job IDs, bytes scanned, and cost-equivalent information.**

And if you wanted the one-line version:

> **It's effectively a portable, auditable snapshot of the structure, shape, scale, categorical content, and recent analytical usage of the 46-table warehouse scope — not the raw warehouse data itself, but everything needed to understand and intelligently use that data.**
