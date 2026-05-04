# LUMI Learnings

This file is updated automatically after each pipeline run, and read
back into every enrichment prompt by `_load_learnings()` in
`lumi/enrich.py` (sections whose body mentions the table name get
spliced into the prompt under `### Learnings from previous runs`).

The structure below mirrors `DESIGN.md §4`. Keep one H2 per category.
Each table-specific learning lives as a sub-bullet that mentions the
table name verbatim — the loader does plain-string matching.

---

## Table patterns discovered

These are recurring shapes the pipeline has seen across the gold queries.
They become defaults the LLM can lean on when generating LookML.

- Cornerstone tables (any table with `cornerstone_*` prefix or
  `data_source = 'cornerstone'` in 80%+ of queries): the `cornerstone`
  source is the canonical default. Bake `data_source: "cornerstone"`
  into the explore's `always_filter` and emit a filtered measure
  variant for any SUM aggregation that appears with this filter.
- Risk tables wrapped by a TRIUMPH+consumer CTE (`acct_srce_sys_cd =
  'TRIUMPH' AND acct_bus_unit_cd IN (1, 2)`): the structural-filter
  scope is consumer-only by convention. Name the derived_table view
  `<table>_triumph_consumer` and bake both filters into the SQL.
- Tables with `acct_*` columns: account-level facts. Default
  `relationship: many_to_one` when joining lookup tables (product,
  customer master, geography).
- Snapshot/history tables (column ending `_as_of_dt`, `_snapshot_dt`,
  `_history_dt`): these are time-keyed; always promote to
  `dimension_group` and add `always_filter` on the `_date` timeframe
  (default `last 12 months`).

## Prompt patterns that work

- ALWAYS write `${TABLE}.column_name` in `sql:` expressions, never
  bare `column_name`. Looker uses `${TABLE}` to qualify with the view
  alias — bare references break symmetric aggregates the moment the
  view participates in a join.
- Order the chain-of-thought scaffold (Steps 1-8 in the prompt) by
  data-flow direction: identity (PK) → time (date columns) →
  measures → derivations (CTEs/CASE WHEN) → joins → catalog →
  questions. Reordering hurts coverage on complex tables.
- Include the SKILL.md SQL→LookML map verbatim at the bottom of the
  prompt. Compressing the examples to bullet headlines drops measure
  quality (model defaults to `value_format_name: usd` for everything,
  including counts).
- When the model needs a primary_key for a fact table with no obvious
  unique column, the synthetic PK pattern `CONCAT(${TABLE}.colA, '|',
  ${TABLE}.colB, '|', CAST(${TABLE}.dateCol AS STRING))` works
  reliably and parses cleanly with `lkml.load`.

## Prompt patterns that fail

- Don't ask the model "what fields do you think are PII?" — MDM's
  `sensitivity_details.is_pii` is the source of truth. Asking the
  model invites hallucinated PII tags on harmless columns.
- Don't ask the model to invent NL question variants without a
  scaffold — temperature-0 generations are repetitive (5 variants of
  "What was the total billed business?"). The Step 8 axis list
  (specificity, phrasing, grain, filter, comparison) forces variety.
- Don't put valid values in field descriptions. Even when prompted
  not to, the model leaks values into descriptions when the SKILL.md
  example shows them in a `dimension { description: ... }` block.
  Keep the SKILL examples value-free.

## Common LookML mistakes the model makes

- Plain `dimension` for date columns (instead of `dimension_group`).
  The Step 2 scaffold catches most of these but the model still slips
  on column names that don't end in `_dt` or `_date` (e.g., `period`,
  `effective`). Mitigation: scan `mdm_columns` for
  `attribute_type='DATE'` or `'TIMESTAMP'` and call them out
  explicitly in the prompt.
- Missing `convert_tz: no` on BigQuery dimension_groups. The default
  silently UTC-converts and corrupts dates by 4-7 hours. The DO-NOT
  list flags this; the self-repair loop catches stragglers.
- Missing `value_format_name` on count measures. Model defaults to
  `usd` even when the SKILL example shows `decimal_0`. Mitigation:
  for `type: count` and `type: count_distinct`, the prompt should
  spell out `value_format_name: decimal_0` as the canonical default.
- Joins emitted in alphabetical order in the explore. Topological
  order is REQUIRED — later joins may reference earlier joins'
  columns. The `joins_involving_this` list in TableContext carries an
  `order` field; the prompt must surface it (Step 7 does).
- Missing `order_by_field` on derived dimensions with business
  ordering (risk levels, age buckets, priority tiers). Without it,
  Looker sorts alphabetically: "Current, 120 DPB, 150 DPB, 180+ DPB,
  30 DPB" — wrong. SKILL.md section 1 has the canonical example.

## MDM gaps

- ~30% of columns have no `attribute_desc` populated (e.g.,
  `acct_bal_age_mth01_cd` on `risk_pers_acct_history` is a known
  gap). For these, the prompt instructs the model to derive
  description from SQL context (CASE WHEN body, aggregation usage).
- MDM has no synonyms. Tags on dimensions must come from the model's
  reasoning over the business name + column name (e.g., `bus_seg` →
  `["segment", "business segment", "bus_seg", "division"]`).
- MDM has no `allowed_values`. Low-cardinality enums must come from
  BigQuery `SELECT DISTINCT` (the `check_bq_access.py` probe
  populates `data/bq_cache/<table>.json` with these). Until that
  cache is populated, leave `known_values: []` in `filter_catalog`.

## Evaluator insights

- (populated after the coverage validator runs against gold queries)
