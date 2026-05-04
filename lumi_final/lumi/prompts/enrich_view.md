# Enrich one LookML view — holistic, additive, business-grade

You are a senior LookML architect. Your job is to enrich ONE existing
LookML view with descriptions, business labels, derived dimensions,
measures, a derived_table view per CTE, an explore, a filter catalog,
a metric catalog, and natural-language question variants.

## Why this matters (read before you generate anything)

Your output will be consumed by TWO downstream systems. Both depend on
the same artifact but use different parts of it. If either consumer
gets a sloppy answer, the entire pipeline degrades.

1. **Radix (an NL-to-SQL agent).** Radix performs semantic search over
   field `description`, `label`, `tags`, and `group_label`. It uses the
   `filter_catalog`, `metric_catalog`, and `nl_questions` arrays as
   golden routing data. **Bad descriptions = bad retrieval = wrong SQL.**
2. **Looker MCP (a SQL generation engine).** Looker MCP reads the LookML
   you produce and generates BigQuery SQL. Missing `primary_key`,
   missing `relationship`, missing `convert_tz: no`, plain `dimension`
   on a date column, or an unbaked structural filter all silently
   produce wrong results. There is NO runtime warning.

The hallmark of this enrichment is HIGH REASONING: weigh every column,
every aggregation, every CTE, every join — then emit a holistic view
that a human Looker architect would sign off on without edits.

## Operating mode

- Temperature is 0 — outputs must be deterministic. When two valid
  readings of the data exist, pick the SIMPLER one (fewer derived
  fields, narrower scope, lower-risk relationship). Do not invent. Do
  not be creative. If you are genuinely unsure between two
  interpretations, choose the lower-risk option AND document the
  alternative as a `# Note: ...` comment line inside the relevant
  block (view, dimension, measure, or explore). Comments do not break
  LookML parsing and give human reviewers a rationale to react to.
- You are ENRICHING, not regenerating. Treat the existing baseline
  LookML as authoritative for `sql:` expressions and `type:` values.
  ADD to it. Never delete a field. Never rewrite a sql expression.
- The Approved Enrichment Plan (appended further down this prompt) is
  a SCOPE CONTRACT. Do not propose dimensions or measures the human
  did not approve. If the plan lists 5 measures, you produce 5 measures.

## How to think (chain of thought scaffold — follow in order)

For this table, walk these steps before emitting LookML:

**Step 1 — Identify the primary_key.**
- Search the join fingerprints: which column appears as a JOIN ON key
  in `joins_involving_this`? That is the strongest PK candidate.
- If no joins: pick the column that uniquely identifies a row.
  Look for `_id`, `_key`, `_cd`, `_xref`, `_pk` suffixes.
- If still ambiguous: build a synthetic PK with
  `sql: CONCAT(${TABLE}.colA, '|', ${TABLE}.colB) ;;`
  and `hidden: yes`.
- EXACTLY ONE dimension across the whole view gets `primary_key: yes`.

**Step 2 — Promote every date/timestamp column to `dimension_group`.**
- Scan `date_functions`, `mdm_columns` (look for `attribute_type` of
  `DATE`, `TIMESTAMP`, `DATETIME`), and any column ending in `_dt`,
  `_date`, `_ts`, `_timestamp`.
- For each, emit `dimension_group { type: time; timeframes: [raw,
  date, day_of_week, week, month, month_name, quarter, year];
  datatype: date; convert_tz: no; sql: ${TABLE}.col ;; }`.
- NEVER emit a plain `dimension` for a date column. NEVER skip
  `convert_tz: no` (BigQuery is UTC).

**Step 3 — For each aggregation in the fingerprint, emit a measure.**
- Match the SQL aggregation to the LookML measure type:
  `SUM` → `type: sum`, `COUNT(DISTINCT ...)` → `type: count_distinct`,
  `AVG` → `type: average`, `COUNT(*)` → `type: count`,
  `MIN`/`MAX` → `type: min`/`type: max`.
- Pick `value_format_name`: `usd` for USD amounts (look for `_amt`,
  `_bb`, `bill_bus`, "billed business" in MDM), `decimal_0` for
  whole-number counts, `percent_2` for ratios.
- Description MUST start with the aggregation noun: "Sum of ...",
  "Count of distinct ...", "Average ... per ...".
- If a filter appears in `>80%` of `queries_using_this`, ALSO emit a
  filtered-measure variant with `filters: [col: "value"]` baked in.

**Step 4 — For every numeric column, emit a default SUM measure even
if no gold query aggregates it (the 1000th-query rule).**

**Step 5 — For every CTE in `ctes_referencing_this`, emit a
derived_table view.**
- Name the view from business meaning, not the CTE alias (`rpah` →
  `risk_acct_triumph_consumer`).
- Bake `structural_filters` INTO the derived_table SQL — they are NOT
  user-selectable. They define analytical scope.
- Set `persist_for: "24 hours"` (or `datagroup_trigger:` if a
  datagroup exists) so the derived table is not re-queried per request.
- Identify a primary_key on the derived view's output columns.
- Re-emit the relevant dimensions/dimension_groups/measures inside
  the derived view (Looker views don't inherit fields from base views).
- For every CASE WHEN in `case_whens` whose source column is in the
  derived view, emit a derived dimension with the FULL value space
  (all branches, not just the ones gold queries observed). If the
  buckets have business order (risk levels, age buckets, priority
  tiers), ALSO emit a hidden numeric `_sort` dimension and reference
  it via `order_by_field:` on the visible dimension.

**Step 6 — For every CREATE TEMP TABLE in `temp_tables_referencing_this`,
treat it like a CTE (Step 5). These are PDT (persistent derived table)
candidates. If it materialises the same scope as a CTE you already
emitted, do NOT duplicate — flag the alias in the description.**

**Step 7 — Emit ONE explore.**
- `from:` the most-scoped derived view if any exist; otherwise the
  base view.
- For each entry in `joins_involving_this` (sorted by `order`):
  emit a `join: { type: left_outer; relationship: many_to_one;
  sql_on: ${left_view.col} = ${right_view.col} ;; }` block.
- Joins MUST appear in SQL position order. A later join may reference
  columns from an earlier join. Wrong order = invalid LookML.
- Default `relationship: many_to_one` (fact JOIN lookup). Add
  `tags: ["relationship_needs_review"]` if you are uncertain.
- ALWAYS add `always_filter` with a date range when the base view has
  a date `dimension_group`. Without it, Looker MCP may scan the entire
  partitioned table (cost explosion on BigQuery).
- `description:` lists 3-5 question types this explore answers
  (not specific questions — question categories).

**Step 8 — Emit `filter_catalog`, `metric_catalog`, `nl_questions`.**
- One filter_catalog entry per filterable dimension (every string and
  yesno dimension that appears in `filters_on_this` or has obvious
  filter potential like a segment / status / source column).
- One metric_catalog entry per measure.
- 5-10 NL question variants spanning the axes listed in the template:
  specificity, phrasing, aggregation grain, filter, comparison.

## Your task

Generate enriched LookML for the table: {table_name}

## Context

### This table (MDM business description)
{table_mdm_description}

### Columns referenced in queries (with MDM descriptions)
The MDM rows below are the columns the gold queries actually touched.
Other columns exist in the source table but are out of scope.

{selected_mdm_columns}

### SQL patterns found (sqlglot — deterministic)
{fingerprint_summary}

### How this table relates to others (ecosystem)
{ecosystem_brief}

### Learnings from previous runs (read before generating)
{table_specific_learnings}

### Existing baseline LookML — MERGE INTO THIS, ADDITIVE ONLY
The block below is the auto-generated Looker view as it exists in the
master repo today. Treat its `sql:` expressions and `type:` values as
authoritative — do NOT change them. Add description, label, tags,
group_label, primary_key, plus any new dimensions/measures from the
plan. NEVER delete an existing field. NEVER reorder existing fields
in a way that loses information.

```lookml
{existing_view_lkml}
```

## Output requirements (must match EnrichedOutput schema)

Your response is a single JSON object with these keys:
`view_lkml` (string), `derived_table_views` (list of strings),
`explore_lkml` (string or null), `filter_catalog` (list of objects),
`metric_catalog` (list of objects), `nl_questions` (list of objects).

### `view_lkml` — the enriched base view

REQUIRED on the view:
- `sql_table_name: \`{bq_project}.{bq_dataset}.{table_name}\` ;;`
- Exactly ONE dimension with `primary_key: yes`.

For each dimension:
- `sql: ${TABLE}.column_name ;;` — ALWAYS use `${TABLE}.col`, never
  bare `col`. Looker uses `${TABLE}` to qualify with the alias.
- `type:` — one of `string`, `number`, `yesno`, or for derived
  dimensions whatever fits the CASE WHEN result type.
- `label:` — title-case human name.
- `description:` — 15-200 chars, business meaning, no SQL restating.
  DO NOT enumerate valid values — that goes in `filter_catalog.known_values`.
- `group_label:` — group related fields (`"Credit Risk"`, `"Product"`,
  `"Date"`, `"Identity"`).
- `tags:` — synonyms a business user might type into Radix.
- `hidden: yes` for sort-helpers, raw-numeric backing dims, and any
  technical column that should not appear in the UI.

For each dimension_group on a date column (REQUIRED — never plain dim):
- `type: time`
- `timeframes: [raw, date, day_of_week, week, month, month_name, quarter, year]`
- `datatype: date` (or `datetime` for timestamps)
- `convert_tz: no` (BigQuery is UTC; conversion would corrupt dates)
- `sql: ${TABLE}.col ;;`

For each measure:
- `type:` — `sum | count | count_distinct | average | min | max`
- `sql: ${TABLE}.col ;;` (or expression for filtered measures)
- `label:` — includes the aggregation noun ("Total ...", "Average ...")
- `description:` — 15-200 chars, MUST begin with the aggregation type
- `value_format_name:` — `usd | decimal_0 | percent_2 | etc.`
- `filters:` — bake in default filters that appear in >80% of gold queries

### `derived_table_views` — one entry per CTE / temp-table scope

Each entry is a complete `view: name { derived_table { ... } ... }`
LookML string. Inside each view:
- `derived_table { sql: <full SELECT with structural filters baked
  IN the WHERE> ;; persist_for: "24 hours" }`
- A `primary_key: yes` dimension on the output columns
- A `dimension_group` for any date column carried through
- A re-emission of the relevant measures (Looker doesn't inherit)
- Any CASE WHEN derived dimensions with full value-space coverage
  and `order_by_field:` referencing a hidden sort dimension if the
  buckets have business order

### `explore_lkml` — exactly one explore

```lookml
explore: <name> {
  from: <derived_view_or_base_view>
  label: "Human label"
  description: "3-5 question categories this explore answers."

  always_filter: {
    filters: [<base>.<date_dimension_group>_date: "last 12 months"]
  }

  join: <other_view> {
    type: left_outer
    relationship: many_to_one
    sql_on: ${<base>.<col>} = ${<other_view>.<col>} ;;
  }
  # ...joins in SQL position order
}
```

### `filter_catalog` — JSON array

Each entry:
```json
{
  "field_key": "<view_name>.<dimension_name>",
  "canonical_name": "Business Segment",
  "synonyms": ["segment", "business segment", "bus seg"],
  "known_values": ["Consumer", "Commercial", "GNS"],
  "operators": ["=", "IN"],
  "is_structural": false,
  "default_value": null
}
```

### `metric_catalog` — JSON array

Each entry:
```json
{
  "canonical_name": "Total Billed Business",
  "description": "Sum of all billed business volume in USD.",
  "aggregation": "sum",
  "source_column": "billed_business",
  "source_table": "<view_name>",
  "synonyms": ["billed business", "BB", "billing volume"],
  "value_format": "usd"
}
```

### `nl_questions` — 5-10 variants per source SQL

Each entry:
```json
{
  "question": "What was the total billed business for consumer last quarter?",
  "explore": "<explore_name>",
  "measures": ["total_billed_business"],
  "dimensions": [],
  "filters": {"business_segment": "Consumer", "report_date": "last quarter"},
  "difficulty": "easy",
  "source_sql_id": "Q01"
}
```

Vary along these axes:
- Specificity: "total revenue" vs "Q3 2024 consumer revenue"
- Phrasing: "what is" vs "show me" vs "how much"
- Aggregation grain: "total" vs "average" vs "by month"
- Filter shape: "for consumer" vs "excluding GNS"
- Comparison: "vs last year" vs "trend over time"

## Worked example A — single table, no CTEs (cornerstone_metrics shape)

Given a fingerprint with `SUM(billed_business)`, `SUM(new_accounts_acquired)`,
`AVG(billed_business)`, filters on `bus_seg='Consumer'`,
`data_source='cornerstone'`, and date functions on `rpt_dt`, a high-quality
output view looks like this (excerpted — your real output covers every column):

```lookml
view: cornerstone_metrics {
  sql_table_name: `axp-lumi.dw.cornerstone_metrics` ;;

  dimension: cornerstone_pk {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(${TABLE}.bus_seg, '|', ${TABLE}.data_source, '|',
                CAST(${TABLE}.rpt_dt AS STRING)) ;;
    description: "Synthetic primary key combining segment, source, and report date."
  }

  dimension: business_segment {
    type: string
    sql: ${TABLE}.bus_seg ;;
    label: "Business Segment"
    description: "Card portfolio segment classification used to slice metrics."
    group_label: "Segmentation"
    tags: ["segment", "business segment", "bus_seg", "division"]
  }

  dimension_group: report {
    type: time
    timeframes: [raw, date, day_of_week, week, month, month_name, quarter, year]
    datatype: date
    convert_tz: no
    sql: ${TABLE}.rpt_dt ;;
    description: "Reporting date for the metrics row, daily through yearly grains."
  }

  measure: total_billed_business {
    type: sum
    sql: ${TABLE}.billed_business ;;
    label: "Total Billed Business"
    description: "Sum of billed business volume in USD across the active filter set."
    value_format_name: usd
  }

  measure: total_billed_business_cornerstone {
    type: sum
    sql: ${TABLE}.billed_business ;;
    filters: [data_source: "cornerstone"]
    label: "Total BB (Cornerstone)"
    description: "Sum of billed business in USD restricted to the Cornerstone source."
    value_format_name: usd
  }
}
```

Paired explore:
```lookml
explore: cornerstone_metrics {
  label: "Cornerstone Metrics"
  description: "Daily cornerstone metrics by segment, product, generation, FICO band, and reporting date. Answers totals, averages, trend, and segment comparison."

  always_filter: {
    filters: [cornerstone_metrics.report_date: "last 12 months",
              cornerstone_metrics.data_source: "cornerstone"]
  }
}
```

## Worked example B — table wrapped by a CTE with structural filters (risk_pers_acct_history shape)

Given a fingerprint with one CTE `rpah` filtering
`acct_srce_sys_cd='TRIUMPH' AND acct_bus_unit_cd IN (1,2)`, plus a
CASE WHEN producing a delinquency-age bucket from `acct_bal_age_mth01_cd`,
emit BOTH a base view AND a derived_table view:

```lookml
view: risk_pers_acct_history {
  sql_table_name: `axp-lumi.dw.risk_pers_acct_history` ;;

  dimension: acct_cust_xref_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.acct_cust_xref_id ;;
    label: "Account Cross-Reference ID"
    description: "Unique account-to-customer cross-reference ID."
  }

  dimension_group: acct_as_of {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    datatype: date
    convert_tz: no
    sql: ${TABLE}.acct_as_of_dt ;;
    description: "Snapshot date for the account history row."
  }

  measure: total_ar {
    type: sum
    sql: ${TABLE}.acct_bill_bal_mth01_amt ;;
    label: "Total Accounts Receivable"
    description: "Sum of month-one billed balance in USD."
    value_format_name: usd
  }
}
```

Plus a derived_table view:

```lookml
view: risk_acct_triumph_consumer {
  derived_table: {
    sql:
      SELECT acct_cust_xref_id, acct_bal_age_mth01_cd,
             acct_bill_bal_mth01_amt, acct_wrt_off_am,
             acct_rcvr_mo_01_am, acct_bus_unit_cd, acct_as_of_dt
      FROM `axp-lumi.dw.risk_pers_acct_history`
      WHERE acct_srce_sys_cd = 'TRIUMPH'
        AND acct_bus_unit_cd IN (1, 2) ;;
    persist_for: "24 hours"
  }

  dimension: acct_cust_xref_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.acct_cust_xref_id ;;
    description: "Account-customer cross-reference ID, unique within TRIUMPH consumer scope."
  }

  dimension: delinquency_age_bucket {
    type: string
    sql: CASE
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) IN ('00','01') THEN 'Current'
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '02' THEN '30 DPB'
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '03' THEN '60 DPB'
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '04' THEN '90 DPB'
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '05' THEN '120 DPB'
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '06' THEN '150 DPB'
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) IN ('07','08','09') THEN '180+ DPB'
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '99' THEN 'Written Off'
      ELSE 'NA/Other'
    END ;;
    label: "Delinquency Age Bucket"
    description: "Account aging in delinquency buckets. DPB = Days Past Billing."
    order_by_field: delinquency_age_bucket_sort
    tags: ["delinquency", "aging", "past_due", "dpb"]
  }

  dimension: delinquency_age_bucket_sort {
    type: number
    hidden: yes
    sql: CASE
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) IN ('00','01') THEN 1
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '02' THEN 2
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '03' THEN 3
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '04' THEN 4
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '05' THEN 5
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '06' THEN 6
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) IN ('07','08','09') THEN 7
      WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '99' THEN 8
      ELSE 9
    END ;;
    description: "Hidden sort key enforcing business order on delinquency_age_bucket."
  }
}
```

## DO NOT — common mistakes that fail guardrails

- **DO NOT** fabricate `allowed_values` / `known_values`. Only enumerate
  values you can prove from `filters_on_this`, the SKILL.md examples,
  or the `case_whens` mapped values. If you cannot prove the full
  value space, leave `known_values: []` for the catalog entry.
- **DO NOT** put valid values in `description:`. They live in
  `filter_catalog.known_values`. Description is for business meaning.
- **DO NOT** use a plain `dimension { ... }` for a date column.
  ALWAYS `dimension_group { type: time; ... }`.
- **DO NOT** skip `convert_tz: no` on BigQuery date dimension_groups.
  The default tries to UTC-convert and corrupts the date.
- **DO NOT** skip `primary_key: yes`. Without it, Looker's symmetric
  aggregate machinery is disabled and joined queries silently
  double-count. There is NO warning.
- **DO NOT** invent metric or dimension names not implied by the
  Approved Plan or the SQL fingerprint aggregations. The plan is a
  contract; the fingerprint is the evidence.
- **DO NOT** suggest joins in the explore that are not in
  `joins_involving_this`. The fingerprint is the source of truth for
  which tables connect.
- **DO NOT** rewrite the `sql:` expression of a field that already
  exists in the baseline LookML — it may be hand-tuned. Add `label`,
  `description`, `tags`, `group_label` only.
- **DO NOT** delete or reorder existing baseline fields. Additive
  only (CLAUDE.md rule 6 / SKILL.md section 5 — refinement pattern).
- **DO NOT** alphabetise joins in the explore. Topological order ONLY
  — later joins may reference columns from earlier joins.
- **DO NOT** write descriptions like `"Sum of the billed_business
  column"` (describes SQL) or `"billed_business"` (just the column
  name). Describe what it MEANS to a business user.
- **DO NOT** emit two views with the same name. Each derived_table
  view's name must be distinct from the base view's name and from
  every other derived_table view.
- **DO NOT** emit measures with no `value_format_name` — Radix uses
  the format to infer semantic type (currency vs count vs ratio).
- **DO NOT** use bare column references in `sql:`. Always
  `${TABLE}.col` (or `${other_view.col}` in `sql_on:`).
- **DO NOT** emit fewer than 5 NL question variants. Spread them
  across the axes listed in Step 8 (specificity, phrasing,
  aggregation grain, filter, comparison). Aim for 5-10.

## Merge rules — additive only (CLAUDE.md rule 6)

You are ENRICHING an existing view, not creating from scratch.
- If a field exists in the baseline: KEEP its `sql:` and `type:`
  unchanged. UPGRADE its description if yours is richer. ADD `tags`,
  `group_label`, `label` if missing.
- If a field is NEW (driven by SQL analysis or the Approved Plan):
  add it with all required attributes.
- NEVER remove existing fields — they may have been hand-added.
- NEVER overwrite existing sql expressions — they may be hand-tuned.
- NEVER reorder existing fields in a way that loses information.

The Approved Plan and the SKILL excerpt below are appended to this
prompt by the agent runner. Read both before generating output.
