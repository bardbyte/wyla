# Enrich LookML View

You are a LookML expert building a semantic layer for an enterprise
data platform. Your output will be consumed by an NL-to-SQL system
called Radix that needs to unambiguously select the right dimension,
measure, and filter for any business question.

Your output will also be used by Looker MCP to generate executable SQL.
Every LookML attribute you produce must be complete enough for Looker
to build a valid BigQuery query from it.

## Your task

Generate enriched LookML for the table: {table_name}

## Context

### This table
{table_mdm_description}

### Columns referenced in queries (with MDM descriptions)
{selected_mdm_columns}

### SQL patterns found (from sqlglot analysis)
{fingerprint_summary}

### How this table relates to others
{ecosystem_brief}

### Learnings from previous runs
{table_specific_learnings}

### Existing LookML (current state — merge into this, don't replace)
{existing_view_lkml}

## Output requirements

### View LookML

REQUIRED on every view:
- `sql_table_name: \`{bq_project}.{bq_dataset}.{table_name}\` ;;`

For each column:

**Dimensions:**
- `sql`: the column expression (${TABLE}.column_name)
- `type`: string | number | yesno (infer from data type)
- `label`: human-readable name
- `description`: 15-30 words, front-load business meaning.
  DO NOT list valid values in descriptions — they go in filter_catalog.
- `group_label`: group related fields (e.g. "Credit Risk" for FICO fields)
- `tags`: synonyms that help Radix find this field
- `hidden: yes` for internal/technical fields

**Dimension groups (REQUIRED for ALL date/timestamp columns):**
- `type: time`
- `timeframes: [date, week, month, quarter, year]`
- `datatype: date` (or datetime)
- `convert_tz: no` (BigQuery dates don't need timezone conversion)
- NEVER use a plain dimension for a date column.

**Measures:**
- `type`: sum | count | count_distinct | average (match the SQL aggregation)
- `sql`: the column to aggregate
- `label`: human-readable, includes aggregation ("Total Billed Business")
- `description`: 15-30 words, MUST include aggregation type ("Sum of...")
- `value_format_name`: usd | decimal_0 | percent_2 (as appropriate)

**Primary key:**
- Identify exactly ONE column as `primary_key: yes`
- Look for: columns in JOIN ON conditions, columns ending in _id/_key/_cd
- Without this, Looker silently produces wrong aggregations on joins.

### Derived tables (from CTE patterns)

For each CTE in the SQL patterns:
- Create a SEPARATE view with `derived_table`
- Bake structural filters INTO the derived_table SQL
  (these are NOT user-selectable — they define scope)
- Name the view descriptively (not the CTE alias)
- Include `sql_table_name` is NOT needed (derived_table replaces it)
- Define dimensions on the derived table's output columns
- Identify primary_key on the derived table

Example:
```lookml
view: risk_acct_triumph_consumer {
  derived_table: {
    sql:
      SELECT acct_id, acct_bal, acct_status
      FROM `project.dataset.risk_pers_acct_history`
      WHERE acct_srce_sys_cd = 'TRIUMPH'
        AND acct_bus_unit_cd IN (1, 2)
    ;;
  }
  dimension: acct_id { primary_key: yes type: string sql: ${TABLE}.acct_id ;; }
}
```

### Derived dimensions (from CASE WHEN patterns)

For each CASE WHEN:
- Create a dimension with the full CASE WHEN SQL
- Description MUST explain what each mapped value means in business terms
- Use MDM to interpret the source column's code values
- Include tags with relevant business terms

Example:
```lookml
dimension: delinquency_age_bucket {
  type: string
  sql: CASE
    WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) IN ('00','01') THEN 'Current'
    WHEN TRIM(${TABLE}.acct_bal_age_mth01_cd) = '02' THEN '30 DPB'
    ...
  END ;;
  label: "Delinquency Age Bucket"
  description: "Account balance aging grouped into delinquency buckets.
    Current = no past-due balance. DPB = Days Past Billing.
    Derived from raw balance age code (acct_bal_age_mth01_cd)."
  tags: ["delinquency", "aging", "past_due", "dpb", "age_bucket"]
}
```

### Explore definition

- Use derived_table views as `from:` when CTEs are present
- Joins MUST be in the order specified by position field in the
  SQL patterns. Later joins may reference columns from earlier joins.
  WRONG ORDER = INVALID LOOKML.
- Every join MUST have:
  - `type: left_outer` (or inner, matching the SQL)
  - `relationship: many_to_one` or `one_to_many` (infer from context)
  - `sql_on:` using ${view.field} syntax
- `sql_always_where` for structural filters NOT inside a derived_table
- `description`: list 3-5 example questions this explore answers
- If unsure about relationship, default to `many_to_one` and add
  tag `["relationship_needs_review"]`

### Filter catalog (JSON array)

For each filterable dimension:
```json
{
  "field_key": "cornerstone_metrics.bus_seg",
  "canonical_name": "Business Segment",
  "synonyms": ["segment", "business segment", "bus seg"],
  "known_values": ["Consumer", "Commercial", "GNS"],
  "operators": ["=", "IN"],
  "is_structural": false,
  "default_value": null
}
```

### Metric catalog (JSON array)

For each measure:
```json
{
  "canonical_name": "Total Billed Business",
  "description": "Sum of all billed business volume in USD.",
  "aggregation": "sum",
  "source_column": "billed_business",
  "source_table": "cornerstone_metrics",
  "synonyms": ["billed business", "BB", "billing volume"],
  "value_format": "usd"
}
```

### NL question variants

For each input SQL pattern, generate 5-10 natural language questions
that a business user would ask that this SQL answers.

Vary along these axes:
- Specificity: "total revenue" vs "Q3 2024 consumer revenue"
- Phrasing: "what is" vs "show me" vs "how much"
- Aggregation: "total" vs "average" vs "by month"
- Filter: "for consumer" vs "excluding GNS"
- Comparison: "vs last year" vs "trend over time"

For each question, output the resolution:
```json
{
  "question": "What was the total billed business for consumer segment?",
  "explore": "cornerstone_metrics",
  "measures": ["total_billed_business"],
  "dimensions": [],
  "filters": {"bus_seg": "Consumer"},
  "difficulty": "easy",
  "source_sql_id": "Q1"
}
```

## Examples of good vs bad output

### Good description (measure)
"Sum of total billed business volume in USD across all charge
card transactions for the reporting period."

### Bad description (measure)
"billed_business" ← just the column name
"The sum of the billed_business column" ← describes SQL, not business

### Good description (derived dimension)
"FICO credit score grouped into standard risk bands. Exceptional
(800+), Very Good (740-799), Good (670-739), Fair (580-669),
Poor (below 580)."

### Bad description (derived dimension)
"A case when expression on fico_score" ← describes SQL, not business

## Merge rules

You are ENRICHING an existing view, not creating from scratch.
- If a field exists in the current LookML: upgrade its description
  if yours is richer, add tags/group_label if missing, KEEP its
  sql and type unchanged.
- If a field is NEW (from SQL analysis): add it with all attributes.
- NEVER remove existing fields — they may have been manually added.
- NEVER overwrite existing sql expressions — they may be hand-tuned.
