# LookML Patterns for Semantic Layer Generation

Single source of truth for LookML knowledge in LUMI. The enrichment
prompt injects relevant sections from this file. Claude Code references
this when writing LookML generation code. Don't duplicate these rules
elsewhere.

---

## 1. SQL Pattern → LookML Pattern Map

Every SQL pattern from gold queries maps to a specific LookML construct.
Gemini must know this mapping to produce correct output.

### SUM(column) → measure with type: sum
```sql
SELECT SUM(billed_business) FROM table
```
```lookml
measure: total_billed_business {
  type: sum
  sql: ${TABLE}.billed_business ;;
  label: "Total Billed Business"
  description: "Sum of billed business volume in USD for the reporting period."
  value_format_name: usd
}
```

### COUNT(DISTINCT column) → measure with type: count_distinct
```sql
SELECT COUNT(DISTINCT cm11) FROM table
```
```lookml
measure: unique_cardmembers {
  type: count_distinct
  sql: ${TABLE}.cm11 ;;
  label: "Unique Cardmembers"
  description: "Count of distinct cardmember identifiers (CM11)."
  value_format_name: decimal_0
}
```

### AVG(column) → measure with type: average
```sql
SELECT AVG(billed_business) FROM table
```
```lookml
measure: avg_billed_business {
  type: average
  sql: ${TABLE}.billed_business ;;
  label: "Average Billed Business"
  description: "Average billed business volume per record in USD."
  value_format_name: usd
}
```

### ROUND(SUM(col)/1e9, 2) → measure with custom value_format
```sql
SELECT ROUND(SUM(billed_business) / 1e9, 2) AS bb_billions
```
```lookml
measure: total_billed_business {
  type: sum
  sql: ${TABLE}.billed_business ;;
  value_format: "$#,##0.00,,\"B\""
  # Note: value_format (custom string) not value_format_name (preset)
  # Double comma shifts by millions, triple by billions in Looker format
}
```
When to use `value_format` (custom) vs `value_format_name` (preset):
- `value_format_name: usd` → $1,234.56
- `value_format_name: decimal_0` → 1,235
- `value_format_name: percent_2` → 12.34%
- `value_format: "$#,##0.00,,\"B\""` → $1.23B (custom)
- `value_format: "#,##0.0,\"K\""` → 1,234.5K (custom)

### EXTRACT(YEAR FROM date_col) / EXTRACT(MONTH FROM date_col) → dimension_group
```sql
WHERE EXTRACT(YEAR FROM rpt_dt) = 2024
GROUP BY EXTRACT(MONTH FROM rpt_dt)
```
```lookml
dimension_group: report {
  type: time
  timeframes: [raw, date, day_of_week, week, month, month_name,
               quarter, year]
  datatype: date
  sql: ${TABLE}.rpt_dt ;;
  convert_tz: no
}
```
This auto-generates: `report_raw`, `report_date`, `report_month`,
`report_quarter`, `report_year`, etc. Users filter on `report_year`
or group by `report_month` — Looker generates the correct
`EXTRACT(YEAR FROM ...)` or `DATE_TRUNC(...)` SQL.

CRITICAL: include `raw` in timeframes — some downstream tools need it.
Include `month_name` for display-friendly month names.
ALWAYS set `convert_tz: no` for BigQuery (BQ stores UTC, no conversion).
NEVER use a plain `dimension` for a date column.

### WHERE column = 'value' → filterable dimension
```sql
WHERE bus_seg = 'Consumer'
```
```lookml
dimension: business_segment {
  type: string
  sql: ${TABLE}.bus_seg ;;
  label: "Business Segment"
  description: "Business segment classification: Consumer, Commercial, or GNS."
}
```

### WHERE column IS NULL / IS NOT NULL → yesno dimension
```sql
WHERE apm_flag IS NULL
WHERE cm11 IS NOT NULL
```
```lookml
dimension: has_apm_flag {
  type: yesno
  sql: ${TABLE}.apm_flag IS NOT NULL ;;
  label: "Has APM Flag"
  description: "Yes if account has an APM flag assigned, No otherwise."
}

dimension: has_cardmember_id {
  type: yesno
  sql: ${TABLE}.cm11 IS NOT NULL ;;
  label: "Has Cardmember ID"
  description: "Yes if CM11 identifier exists for this record."
}
```
Use `yesno` for ANY boolean/flag/null-check pattern. Looker renders
these as Yes/No toggles in the filter UI — much better UX than
telling users to type "IS NOT NULL."

### WHERE column BETWEEN date1 AND date2 → dimension_group (already covered)
The dimension_group handles date range filtering automatically.
Looker generates `BETWEEN` when user selects a date range in the UI.

### WHERE column > 0 → dimension with type: number
```sql
WHERE accounts_in_force > 0
```
```lookml
dimension: accounts_in_force_raw {
  type: number
  sql: ${TABLE}.accounts_in_force ;;
  label: "Accounts in Force (Raw)"
  description: "Raw count of active accounts. Filter > 0 to exclude inactive."
  hidden: yes
}
measure: total_accounts_in_force {
  type: sum
  sql: ${TABLE}.accounts_in_force ;;
  label: "Accounts in Force"
  description: "Sum of active accounts in force."
  value_format_name: decimal_0
  filters: [accounts_in_force_raw: ">0"]
}
```
Note the `filters:` parameter on the measure — this bakes the >0
condition into the measure itself, so users don't need to remember
to add it.

### SUM(col) WHERE segment = 'X' → filtered measure
```sql
-- When gold queries frequently filter to one segment
SELECT SUM(billed_business) WHERE data_source = 'cornerstone'
```
```lookml
measure: total_bb_cornerstone {
  type: sum
  sql: ${TABLE}.billed_business ;;
  filters: [data_source: "cornerstone"]
  label: "Total BB (Cornerstone)"
  description: "Sum of billed business from Cornerstone source only."
  value_format_name: usd
}
```
Create filtered measures when a filter appears in >80% of gold queries
for this table. This tells Radix "this is the default slice."

### GROUP BY column ORDER BY measure DESC → dimension + order_by_field
```sql
SELECT fico_band, SUM(accounts_in_force)
GROUP BY fico_band ORDER BY SUM(accounts_in_force) DESC
```
```lookml
dimension: fico_band {
  type: string
  sql: ${TABLE}.fico_band ;;
  label: "FICO Band"
  order_by_field: fico_band_sort_order
}
dimension: fico_band_sort_order {
  type: number
  sql: CASE
    WHEN ${TABLE}.fico_band = 'Exceptional' THEN 1
    WHEN ${TABLE}.fico_band = 'Very Good' THEN 2
    WHEN ${TABLE}.fico_band = 'Good' THEN 3
    WHEN ${TABLE}.fico_band = 'Fair' THEN 4
    WHEN ${TABLE}.fico_band = 'Poor' THEN 5
    ELSE 6
  END ;;
  hidden: yes
}
```
`order_by_field` is CRITICAL for derived dimensions with business
ordering (risk levels, age buckets, priority tiers). Without it,
Looker sorts alphabetically: "Current, 120 DPB, 150 DPB, 180+ DPB,
30 DPB..." which is wrong. The hidden sort dimension enforces
correct business order.

### CTE with structural filters → derived_table view
```sql
WITH rpah AS (
  SELECT ... FROM risk_pers_acct_history
  WHERE acct_srce_sys_cd = 'TRIUMPH'
    AND acct_bus_unit_cd IN (1, 2)
)
```
```lookml
view: risk_acct_triumph_consumer {
  derived_table: {
    sql:
      SELECT acct_cust_xref_id, acct_bal_age_mth01_cd,
             acct_bill_bal_mth01_amt, acct_wrt_off_am,
             acct_rcvr_mo_01_am, acct_bus_unit_cd, acct_as_of_dt
      FROM `axp-lumi`.DATA.risk_pers_acct_history
      WHERE acct_srce_sys_cd = 'TRIUMPH'
        AND acct_bus_unit_cd IN (1, 2) ;;
    persist_for: "24 hours"
  }

  dimension: acct_cust_xref_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.acct_cust_xref_id ;;
  }
}
```
`persist_for` caches the derived table result for 24 hours. Without
it, Looker re-runs the subquery on every explore load. For large
tables this is critical for performance.

Structural filters (acct_srce_sys_cd = 'TRIUMPH') are baked INTO
the derived_table SQL. They are NOT user-selectable. They define
the analytical scope of this view.

### CASE WHEN → derived dimension with order_by_field
```sql
CASE WHEN TRIM(acct_bal_age_mth01_cd) IN ('00','01') THEN 'Current'
     WHEN TRIM(acct_bal_age_mth01_cd) = '02' THEN '30 DPB'
     ...
     WHEN TRIM(acct_bal_age_mth01_cd) = '99' THEN 'Written Off'
     ELSE 'NA/Other' END AS age_bucket
```
```lookml
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
  description: "Account aging in delinquency buckets. Current = no
    past-due. DPB = Days Past Billing. Derived from raw balance
    age code (acct_bal_age_mth01_cd)."
  order_by_field: delinquency_age_bucket_sort
  tags: ["delinquency", "aging", "past_due", "dpb"]
}

dimension: delinquency_age_bucket_sort {
  type: number
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
  hidden: yes
}
```

### Multi-table JOIN with order → explore with topological joins
```sql
FROM risk_acct rpah
JOIN drm_product_member drm_prod ON rpah.acct_ia_pct_cd = drm_prod.prod_cd
JOIN drm_product_hier drm_hier ON drm_prod.mbr_nm = drm_hier.parnt_nm
```
```lookml
explore: risk_product_spend {
  from: risk_acct_triumph_consumer
  label: "Risk Product Spend Analysis"
  description: "Account-level spending by product hierarchy.
    Answers: total spend by product line, spend by product tier,
    product mix for consumer business units."

  always_filter: {
    filters: [risk_acct_triumph_consumer.report_date: "last 12 months"]
  }

  join: product_lookup_us_business {
    type: left_outer
    relationship: many_to_one
    sql_on: ${risk_acct_triumph_consumer.acct_ia_pct_cd}
            = ${product_lookup_us_business.prod_cd} ;;
  }

  # This join MUST come AFTER product_lookup because sql_on
  # references product_lookup.mbr_nm
  join: drm_product_hier {
    type: left_outer
    relationship: many_to_one
    sql_on: ${product_lookup_us_business.mbr_nm}
            = ${drm_product_hier.parnt_nm} ;;
  }
}
```
`always_filter` forces a default filter on the explore. Users can
change the value but can't remove the filter entirely. Use for
date ranges on large tables — prevents full-table scans.

---

## 2. Required Attributes Checklist

### View level
| Attribute | Required | Why |
|-----------|----------|-----|
| `sql_table_name` | YES | Looker can't find BQ table without it |
| (derived_table) | OR sql_table_name | One or the other, never both |

### Dimension level
| Attribute | Required | Why |
|-----------|----------|-----|
| `sql` | YES | The column expression |
| `type` | YES | string/number/date/yesno — affects filtering + SQL casting |
| `primary_key: yes` | YES (on one dim) | Without it: symmetric aggregates break, joins fanout silently |
| `label` | Recommended | Human-readable name for UI + Radix matching |
| `description` | Recommended | 15-200 chars. Radix embeds this for semantic search |
| `group_label` | Recommended | Groups related dims ("Credit Risk", "Product", "Date") |
| `tags` | Recommended | Synonyms for Radix field matching |
| `hidden: yes` | Conditional | For sort-order dims, internal IDs, technical fields |
| `order_by_field` | Conditional | For business-ordered categories (risk bands, age buckets) |

### Dimension group level
| Attribute | Required | Why |
|-----------|----------|-----|
| `type: time` | YES | Tells Looker to generate DATE_TRUNC/EXTRACT |
| `timeframes` | YES | Which grains: [raw, date, week, month, quarter, year] |
| `sql` | YES | The date column |
| `datatype` | YES | "date" or "datetime" |
| `convert_tz: no` | YES for BQ | BigQuery dates are UTC, no conversion needed |

### Measure level
| Attribute | Required | Why |
|-----------|----------|-----|
| `type` | YES | sum/count/count_distinct/average/min/max |
| `sql` | YES | The column or expression to aggregate |
| `value_format_name` or `value_format` | Recommended | Display format. Also tells Radix the semantic type |
| `filters` | Conditional | For segment-specific measures (>80% frequency filters) |
| `drill_fields` | Optional | What to show on click-through |
| `description` | Recommended | MUST include aggregation type: "Sum of...", "Count of..." |

### Explore level
| Attribute | Required | Why |
|-----------|----------|-----|
| `from` | Conditional | Required if base view is a derived_table view |
| `join: view` | Per join | Each joined view |
| `sql_on` | YES per join | Using ${view.field} syntax — Looker emits this verbatim |
| `relationship` | YES per join | many_to_one/one_to_many/many_to_many |
| `type` | YES per join | left_outer/inner/full_outer |
| `sql_always_where` | Conditional | Structural filters not in a derived_table |
| `always_filter` | Recommended | Default date range on large tables |
| `description` | Recommended | 3-5 example questions. Radix uses for explore selection |

---

## 3. The primary_key and Symmetric Aggregates

This is the most commonly missed concept and causes SILENT wrong numbers.

When Looker joins two tables and you SUM a measure, it needs to
know how to avoid double-counting. If table A has 1 row per
customer and table B has 5 rows per customer, a naive JOIN + SUM
multiplies A's values by 5.

Looker's solution: **symmetric aggregates**. When `primary_key: yes`
is set on a dimension, Looker automatically wraps aggregations in
a subquery that deduplicates before summing:

```sql
-- Without primary_key (WRONG — double counts):
SELECT SUM(a.revenue) FROM a JOIN b ON a.id = b.id

-- With primary_key set (Looker generates this automatically):
SELECT SUM(CASE WHEN a.__pk_row = 1 THEN a.revenue END)
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id) as __pk_row FROM a) a
JOIN b ON a.id = b.id
```

Rules for primary_key:
- Exactly ONE dimension per view gets `primary_key: yes`
- It should be the column that uniquely identifies a row
- For derived tables: the column you'd GROUP BY to get unique rows
- For fact tables: often a composite — use `sql: CONCAT(${col1}, ${col2}) ;;`
- If unsure: pick the column that appears in JOIN ON conditions

Without primary_key: Looker CANNOT detect fanout. Measures silently
return wrong numbers. There is NO warning. This is the #1 source
of data quality issues in Looker deployments.

---

## 4. Relationship Inference from SQL Patterns

For each join, the enrichment agent must determine the relationship.
Wrong relationship = wrong aggregation math.

| SQL Pattern | Relationship | Why |
|-------------|-------------|-----|
| Fact table JOIN lookup/dimension table | `many_to_one` | Many fact rows per lookup row |
| Parent JOIN child table | `one_to_many` | One parent, many children |
| CTE that GROUP BYs key → JOIN on that key | `many_to_one` | CTE guarantees one row per key |
| Table with DISTINCT in CTE → JOIN | `many_to_one` | Deduplication = one row per key |
| Neither side is deduplicated | `many_to_many` | DANGEROUS — forces symmetric agg |

From our gold queries:
- Q9: `rpah` (many accounts) JOIN `rich` (one credit score per customer) → `many_to_one`
- Q10: `rpah` JOIN `drm_prod` (one product per code) → `many_to_one`
- Q10: `drm_prod` JOIN `drm_hier` (one hierarchy entry per member) → `many_to_one`

Heuristic: if the joined table is a lookup/reference/dimension table
(product catalog, customer master, geography), it's `many_to_one`
from the fact table's perspective.

If unsure: default to `many_to_one` and add tag `["relationship_needs_review"]`.

---

## 5. Refinements (the correct merge pattern)

LookML refinements are the official way to add business logic on top
of auto-generated views WITHOUT modifying the base file:

```lookml
# In a refinement file (e.g., cornerstone_metrics_refined.view.lkml)
view: +cornerstone_metrics {
  # The + prefix means "refine the existing view"

  dimension: bus_seg {
    # Override just the label and description
    label: "Business Segment"
    description: "Segment classification for the card portfolio."
    tags: ["segment", "business segment", "bus seg"]
  }

  # Add a new measure not in the auto-generated view
  measure: total_billed_business {
    type: sum
    sql: ${TABLE}.billed_business ;;
    label: "Total Billed Business"
    description: "Sum of billed business volume in USD."
    value_format_name: usd
  }
}
```

Benefits:
- Base auto-generated view stays untouched
- Refinements only add or override — never delete (matches our merge strategy)
- Multiple refinement files can layer on top of each other
- If Looker regenerates the base view (schema change), refinements survive

LUMI v1 generates full views with merge logic. v2 should generate
refinement files — it's cleaner and Looker-idiomatic.

---

## 6. Patterns for the 1000th Query

The semantic layer must handle queries it hasn't seen. Design for
generalizability:

### Every date column gets ALL timeframes
Don't just add `[year, month]` because gold queries only use those.
Add `[raw, date, day_of_week, week, month, month_name, quarter, year]`.
The 1000th query might need `day_of_week` for weekday analysis.

### Every numeric column gets a measure even if no gold query aggregates it
If MDM says `acct_spend_mth01_amt` is a numeric amount, create
a SUM measure. A gold query using `AVG()` will still work because
Looker can apply any aggregation at query time.

### Every low-cardinality column gets tags with synonyms
Even if no gold query uses "segment," someone will type "segment"
into Radix. Tags: `["segment", "business segment", "bus seg", "division"]`.

### Explores should describe QUESTION TYPES, not specific questions
Bad: "Shows total billed business for consumer segment in Q3 2024"
Good: "Revenue, volume, and account metrics by segment, product,
      and time period. Supports trend analysis, segment comparison,
      and product mix reporting."

### Derived dimensions need the full value space, not just observed values
If the CASE WHEN has 9 buckets and gold queries only touch 3,
include ALL 9. The 1000th query might filter on bucket 7.

### Filtered measures for high-frequency default filters
If 90% of gold queries filter `data_source = 'cornerstone'`,
create a filtered measure AND a plain measure. Radix then has
a choice: use the filtered measure as the default, or the plain
one when the user explicitly asks for all sources.

---

## 7. Anti-Patterns

- Valid values in descriptions → filter_catalog.json
- "The sum of billed_business column" → describes SQL, not business
- `type: string` for numeric columns → check MDM attribute_type
- Missing `convert_tz: no` on BQ dates → timezone conversion errors
- `relationship: one_to_many` when it should be `many_to_one` → 2x aggregations
- Alphabetical join order → topological only (later may reference earlier)
- Missing `order_by_field` on risk/age/priority buckets → wrong sort order
- Missing `persist_for` on derived tables → re-queries on every explore load
- Missing `always_filter` on large fact tables → full-table scans
- Using `dimension` for dates → must be `dimension_group`
- Missing `hidden: yes` on sort-order helper dimensions → UI clutter
- Missing `primary_key` → silent aggregation errors on joins (the worst bug)

---

## 8. Model File Structure

The `.model.lkml` file ties everything together. Without it,
views and explores exist but Looker can't find or execute them.

```lookml
connection: "axp-lumi-bigquery"   # REQUIRED — the BQ connection name

# Include patterns — what Looker loads
include: "/views/*.view.lkml"     # all base views
include: "/derived_views/*.view.lkml"  # CTE-derived views
include: "/explores/*.explore.lkml"    # if explores in separate files

# Caching strategy — prevents derived tables from rebuilding every query
datagroup: lumi_daily_refresh {
  sql_trigger: SELECT CURRENT_DATE() ;;
  max_cache_age: "24 hours"
}

# Default datagroup for all derived tables in this model
persist_with: lumi_daily_refresh

# Access grants for PII fields (from MDM sensitivity_details)
access_grant: pii_access {
  user_attribute: has_pii_access
  allowed_values: ["yes"]
}
```

### How derived tables use the datagroup
```lookml
view: risk_acct_triumph_consumer {
  derived_table: {
    sql: SELECT ... ;;
    datagroup_trigger: lumi_daily_refresh
    # Rebuilds once per day, not on every query
  }
}
```

### PII protection (from MDM is_pii flags)
```lookml
dimension: ssn_last_four {
  type: string
  sql: ${TABLE}.ssn_last_4 ;;
  required_access_grants: [pii_access]
  # Only visible to users with has_pii_access = "yes"
  tags: ["pii", "restricted"]
}
```
LUMI reads `sensitivity_details.is_pii` from MDM. Fields flagged
as PII get `required_access_grants` and `tags: ["pii"]`.

### Sets (reusable field groups for drill-through)
```lookml
set: customer_detail_fields {
  fields: [customer_id, customer_name, business_segment,
           total_billed_business, accounts_in_force]
}

measure: total_billed_business {
  type: sum
  sql: ${TABLE}.billed_business ;;
  drill_fields: [customer_detail_fields*]
  # Click on the measure → see these fields broken out
}
```

---

## 9. How This Skill Gets Used (Two Consumers)

This skill serves TWO consumers. Both must stay in sync.

### Consumer 1: Claude Code (writing pipeline code)
Claude Code reads this skill when implementing `enrich.py`,
`guardrails.py`, and `validate.py`. It uses the patterns to:
- Write correct LookML validation checks
- Know which attributes are required vs optional
- Understand the SQL → LookML mapping for test assertions

### Consumer 2: Gemini 3.1 Pro (generating LookML at runtime)
The enrichment code (`enrich.py`) reads this skill file and injects
the relevant sections into the Gemini prompt at runtime:

```python
# In enrich.py — load skill patterns for the prompt
from pathlib import Path

def load_lookml_patterns() -> str:
    """Load the SQL→LookML pattern map from the skill file.
    Injected into the Gemini enrichment prompt at runtime."""
    skill_path = Path(".claude/skills/lookml/SKILL.md")
    content = skill_path.read_text()
    # Extract sections 1-4 (patterns, attributes, PK, relationships)
    # Skip sections 5-9 (refinements, 1000th query, anti-patterns, model, this section)
    # Those are for Claude Code, not for Gemini
    return extract_sections(content, sections=[1, 2, 3, 4])
```

This means:
- ONE source of truth (this file)
- Claude Code sees ALL sections when writing code
- Gemini sees sections 1-4 when generating LookML
- No duplication between the prompt and the skill
- Update this file → both consumers get the update

### What goes in the Gemini prompt vs. what stays here

| Section | In Gemini prompt? | Why / why not |
|---------|-------------------|---------------|
| 1. SQL→LookML map | YES | Gemini needs these patterns to generate correct LookML |
| 2. Required attributes | YES | Gemini must know what's mandatory |
| 3. Primary key + symmetric aggs | YES | Gemini must set primary_key correctly |
| 4. Relationship inference | YES | Gemini must set relationship correctly |
| 5. Refinements | NO | Architecture decision for code, not for Gemini |
| 6. 1000th query patterns | Partially | The "add all timeframes" and "every numeric = measure" rules go in prompt |
| 7. Anti-patterns | YES (compressed) | 3-line "never do" list in prompt |
| 8. Model file | NO | Generated by code, not by Gemini per-table call |
| 9. This section | NO | Meta — for Claude Code only |
