# Data Steward Glossary — Synapse Ingest Template

> **For data stewards:** write whatever you know about a term, table, column,
> metric, or business rule. Don't worry about structure — the LLM will parse
> your prose into Synapse graph nodes and edges. The required header is
> the only thing you have to get right.
>
> **For Synapse:** this file becomes a first-class source named
> `steward_glossary` (weight 6 — higher than MDM, lower than human_approval).
> Steward-asserted facts WIN over LLM-inferred facts. They DO NOT
> automatically win over MDM or BQ profiling (the steward might be wrong
> about a partition column; the warehouse is ground truth for that). Conflicts
> are surfaced in the graph, not silently resolved.

---

## How to write an entry

Every entry starts with one of these headers — pick the one that fits the
subject of what you're describing. Order doesn't matter. You can have as
many entries as you want in one file.

```markdown
## Term: <subject>          # an acronym, jargon, or business concept (e.g. "TBB", "Cardmember", "DM")
## Table: <table_name>      # a specific table you know things about
## Column: <table>.<col>    # a specific column
## Metric: <name>           # a calculated business metric
## Rule: <one-line title>   # a business rule, anti-pattern, or known gotcha
```

After the header, **write whatever you know in plain English.** The LLM will
extract the structured pieces. You can write one paragraph or twenty.

If you want to be more structured (optional, not required), use any of these
blocks. The LLM understands them but doesn't require them:

```markdown
- synonyms: TBB, Total BB, Billed Business
- business_unit: Finance
- region: Global
- related_tables: custins_customer_insights_cardmember, pmdl_fin_business_volume_transaction_detail
- formula: SUM(billed_business)
- watch_out_for: refunds, declined transactions, corporate adjustments
- canonical_in: cornerstone metrics model
- pii_level: Sensitive>FinancialAmount
- owner: finance-fpa@example.com
- last_reviewed: 2026-06-04
```

---

## Worked examples — copy / adapt these

### Example 1: an ambiguous acronym

```markdown
## Term: CM

In Finance, CM means "Cardmember" — an Amex customer who holds an active
card. Our internal ID for them is `cm11`, an 11-digit code that lives
on every cardmember-level table. When Marketing uses "CM" they mean
"Communication Module" — totally different thing — so context matters.

When you see a column named `cm11`, that's always a Cardmember ID. When
you see "CM" in a query comment or a metric name, default to Cardmember
unless the query is from the Marketing team's project, in which case
ask first.

- synonyms: Cardmember, Card Member, Customer
- business_unit: Finance, Loyalty, Risk
- region: Global
- conflict_with: CM in Marketing BU means Communication Module
```

### Example 2: a metric

```markdown
## Metric: Total Billed Business

Sum of all eligible spend processed on an Amex card in a given period.
Excludes refunds, declined transactions, and corporate adjustments.
Don't confuse with Total Charge Volume which is broader.

This is one of the cornerstone metrics — it shows up in every executive
deck. The canonical computation lives in our metrics catalog, but a
lot of analysts compute it ad-hoc by summing `billed_business` from
the cardmember insights table. Both are correct as long as you remember
to filter `data_source = 'cornerstone'`.

- synonyms: TBB, Total BB, Billed Business
- formula: SUM(billed_business) WHERE data_source = 'cornerstone'
- canonical_in: cornerstone metrics model
- related_tables: custins_customer_insights_cardmember, pmdl_fin_business_volume_transaction_detail
- watch_out_for: must filter data_source='cornerstone' to avoid double-counting
- business_unit: Finance
```

### Example 3: a table

```markdown
## Table: custins_customer_insights_cardmember

The canonical cardmember-day snapshot fact. One row per cardmember per
calendar day. Use this when you need anything aggregated at the cardmember
or higher level. Don't use this for transaction-level analysis — go to
`pmdl_fin_business_volume_transaction_detail` instead.

The `data_source` column is a structural filter — almost every query
should include `WHERE data_source = 'cornerstone'`. The other data
sources are legacy and partially backfilled.

- owner: cardmember-insights@example.com
- partition_field: rpt_dt
- grain: one row per cardmember per day
- typical_filters: data_source = 'cornerstone'
- related_tables: drm_product_hier (via card_product_id), fin_consumer_business_card_member_status
- watch_out_for: do not use for transaction-level analysis
- pii_columns: cm11, billed_business, fico
```

### Example 4: a column

```markdown
## Column: pmdl_fin_business_volume_transaction_detail.acct_id

Account identifier. One cardmember can have multiple accounts (consumer
card + business card + corporate card). To roll up to cardmember level,
JOIN to `fin_consumer_business_card_member_status` on acct_id, then take
cm11 from there.

- synonyms: Account ID, Account Number
- pii_level: Sensitive>Identifier>AccountNumber
- related_to_column: fin_consumer_business_card_member_status.acct_id (1:1)
- rolls_up_to: cardmember (cm11) via fin_consumer_business_card_member_status
```

### Example 5: a business rule / gotcha

```markdown
## Rule: filter on data_source for cornerstone queries

Any query against `custins_*` tables that doesn't filter `data_source =
'cornerstone'` will silently double-count rows from legacy backfills.
This is the #1 cause of inflated cardmember counts in Q1 dashboards.

- applies_to_tables: custins_customer_insights_cardmember, custins_customer_insights_product
- severity: high
- detection: COUNT(DISTINCT cm11) without data_source filter is ~30% higher than expected
- canonical_filter: data_source = 'cornerstone'
```

### Example 6: minimal entry — just write what you know

```markdown
## Term: AA

AA can mean Account Adjustment in Finance, Account Acquisition in
Marketing (EU), or Adverse Action in Risk. Always ask which.
```

That's it — no structured block, just the prose. The LLM will mint a
`Term` node with three context-keyed synonyms.

---

## What the LLM will do with your entry

For each entry it will:

1. Mint or update the relevant Synapse node (Term → Synonym/Entity,
   Table → Table, Column → Column, Metric → Metric, Rule → Threshold or
   Note).
2. Add edges based on the natural language (e.g. "rolls up to cardmember
   via X" → `RELATES_TO` edge with cardinality hint).
3. Attach your free-form prose to the node's `description` field with
   source = `steward_glossary`.
4. Surface any explicit conflict you flagged (e.g. "CM in Marketing
   means Communication Module") as a `Provenance.conflicts` entry the UI
   will show to all consumers.
5. Leave a back-pointer to this file + the line range so analysts can
   trace the claim.

---

## What NOT to put here

- **Raw SQL.** If you have a canonical query, paste it into a separate
  `gold_queries/` file or reference its ID. This file is for definitions,
  not implementations.
- **PII values.** Don't include actual cardmember IDs, account numbers,
  or names — even as examples. Use synthetic ones if you need.
- **Marketing fluff.** "This is our most strategic data asset" doesn't
  help an analyst write a correct query. Keep it operational.

---

## File metadata (optional but recommended)

```markdown
---
file_version: 1
maintained_by: finance-fpa@example.com, risk-modeling@example.com
last_reviewed: 2026-06-04
review_cadence: quarterly
scope: Finance + Risk consumer cardmember tables
---
```
