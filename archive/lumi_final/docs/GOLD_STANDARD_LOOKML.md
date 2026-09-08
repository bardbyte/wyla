# Gold-Standard LookML Rubric for Radix Retrieval + Looker MCP

> Synthesis of three deep-research reports (view best practices, explore best practices,
> Looker MCP contract) into one rubric. Every parameter is anchored to (a) Radix's
> multiplicative retrieval score, (b) Looker MCP's deterministic SQL generation, and
> (c) human curator maintainability.

## The three targets every authoring decision trades off against

| Target | What it cares about | Failure mode |
|---|---|---|
| **Radix retrieval (BGE-large-en-v1.5 → pgvector)** | Lexical + semantic surface in `label`, `description`, `tags` (≤8) | Query routes to wrong explore |
| **Looker MCP deterministic SQL** | `type`, `sql`, `primary_key`, `relationship`, `dimension_group`, `convert_tz` correctness | Wrong SQL or wrong numbers (silent) |
| **Human curator maintainability** | `group_label`, refinements (`view: +name`), `sets`, `value_format_name` presets | LookML rot; curators avoid the file |

Radix's score function: `coverage³ × mean_similarity × base_view_bonus × description_similarity × filter_penalty`. Every dim/measure/explore decision should be defensible against at least one of these terms.

---

## Section 1 — View-level rubric

| Parameter | Rule | Source signal | Verdict if missing |
|---|---|---|---|
| `sql_table_name` OR `derived_table` (never both) | Physical table for base views; derived for scope-locked filtered views | MDM `dataset_source_details.table_name` or sql from baseline | BLOCK |
| `description` (view-level) | 3 sentences. Grain + canonical use + scope. ~80-180 chars total | MDM `dataset_details.data_desc` + cluster grain | BLOCK |
| `label` | Title Case, 2-5 words, no DW prefixes (`dim_`, `fct_`, `dw_`) | MDM `dataset_details.business_name` or humanize table name | WARN |
| `extends` / refinement pattern | Emit `view: +name` refinement file, not edited auto-gen file | Always | BLOCK on hand-edit |
| `sets` | Define `<view>_detail` for drill-throughs; `pii_fields` if PII exists | Manual + MDM PII flags | INFO |

**Refinement file pattern (canonical):**
```
views/auto/<table>.view.lkml          ← auto-gen from BQ schema, never edit
views/refinements/<table>.view.lkml   ← include + view: +<table> with enrichment
```

---

## Section 2 — Dimension rubric (the Radix retrieval target)

### Mandatory parameters

| Parameter | Rule | Source signal | Verdict if missing |
|---|---|---|---|
| `name` | lowercase snake_case, no hyphens (parsed as subtraction!) | column name or MDM business_name slug | BLOCK |
| `type` | Match BQ INFORMATION_SCHEMA (string/number/yesno/tier/zipcode/location/int) — never plain `dimension` on dates | MDM `attribute_type` cross-checked vs BQ | BLOCK if wrong |
| `sql` | Always `${TABLE}.col_name`, never bare `col_name` | column name | BLOCK |
| `label` | Title Case, 2-4 words, abbreviations expanded ("FICO Band" not "FB") | MDM business_name or humanize | WARN (HIGH retrieval impact) |
| `description` | **80-180 chars**, no SQL leak, no enum dump | MDM `attribute_desc` + business context | WARN (HIGHEST retrieval impact) |
| `tags` | **3-8 strings**, synonyms from gold queries + MDM + glossary | gold-query SELECT aliases + MDM business_name + ontology synonyms | WARN |
| `group_label` | One of ~6-10 canonical groups: "Identifiers", "Date", "Customer Demographics", "Product", "Credit Risk", "Spend", "Engagement", "Geography", "Source/Audit" | Naming pattern + MDM data_sub_category | INFO |

### Conditional parameters

| Parameter | Rule | When required |
|---|---|---|
| `primary_key: yes` | **Exactly ONE per view.** Composite → synthesize concat dim. | Every view; without it symmetric_aggregates silently fall back |
| `hidden: yes` | For sort-helper dims, raw codes with derived labels, internal surrogates | When name like `*_sort_order` or `*_code` with paired label |
| `order_by_field` | Sort string dim by hidden numeric dim | When dim has implicit business order (risk bands, age buckets, delinquency stages) |
| `required_access_grants: [pii_access]` | Column-level PII gating | When MDM `sensitivity_details.is_pii=true` |
| `value_format_name` | Use preset (decimal_0, usd, usd_0, percent_2) | Only on numeric dims that get displayed in tables |

### Auto-gen → top-0.01% delta on dimensions

Auto-gen produces `dimension: x { type: number; sql: ${TABLE}.x ;; }`. Enrichment must add:
1. `label` (HIGH retrieval impact)
2. `description` 80-180 chars (HIGHEST)
3. `tags` 3-8 synonyms (HIGHEST)
4. `group_label` (MEDIUM)
5. `primary_key: yes` on one dim (CRITICAL — silent fanout otherwise)
6. `required_access_grants` if MDM flags PII (CRITICAL)

---

## Section 3 — Dimension_group rubric (for dates) — ALWAYS use this, never plain `dimension` on dates

| Parameter | Rule | Verdict if missing |
|---|---|---|
| `type: time` | Mandatory | BLOCK |
| `datatype` | Explicit: `date` for BQ DATE; `timestamp` for BQ TIMESTAMP; `datetime` for DATETIME. Don't leave inferred. | WARN |
| `convert_tz: no` | **MANDATORY on BigQuery.** BQ stores UTC; default `yes` double-shifts silently. | BLOCK |
| `timeframes` | **`[raw, date, day_of_week, week, month, month_name, quarter, year, fiscal_year, fiscal_quarter]`** — full list, not what gold queries currently use | WARN |
| `sql` | `${TABLE}.col_name` | BLOCK |
| `group_label: "Date"` | Always | INFO |

**For `type: duration`:** include `intervals: [day, week, month, year]` so all granularities are queryable.

---

## Section 4 — Measure rubric

### Mandatory

| Parameter | Rule |
|---|---|
| `name` | Aggregation verb prefix: `total_*`, `unique_*`, `avg_*`, `pct_*`, `min_*`, `max_*` |
| `type` | sum/count/count_distinct/average/min/max/median/percentile/number/yesno/running_total |
| `sql` | `${TABLE}.col` for direct; `${measure_a} / NULLIF(${measure_b}, 0)` for ratios |
| `value_format_name` | **Mandatory on every numeric measure** — preset (decimal_0, decimal_2, usd_0, usd, percent_2, etc.) |
| `description` | **MUST open with aggregation verb** — "Sum of...", "Count of distinct...", "Average...", "Ratio of..." |

### Conditional

| Parameter | When |
|---|---|
| `filters: [col: "value"]` | Filtered measure when a filter appears in >80% of gold queries on this measure |
| `drill_fields: [view_detail*]` | Reference set, don't enumerate |
| `required_access_grants` | PII-derived measures (rare but exists for amounts on PII rows) |
| `sql_distinct_key` | When measure is `*_distinct` and no PK exists, or PK isn't the right grain |

**Do NOT set `symmetric_aggregates`** — it's ON BY DEFAULT in Looker since v5. The control surface is `primary_key` + `relationship`, not this parameter.

---

## Section 5 — Explore rubric

| Parameter | Rule |
|---|---|
| `explore_name` | snake_case, business concept (not table name). Singular for dim-like (`cardmember`); plural for fact-grained (`card_authorizations`). |
| `description` | **150-250 words.** Template: one-sentence what + 3-5 example questions verbatim + grain + default filters + joins available + synonyms section |
| `label` | Title Case, aligned semantics with description |
| `group_label` | Business domain: "Card Activity", "Cardmember 360", "Merchant Analytics", "Risk & Fraud" |
| `view_name` | When explore name ≠ base view name |
| `from` | For aliasing (typically self-joins) |
| `hidden: yes` | For base explores extended by user-facing ones |
| `extends` | Inherit + override (NOT for grain changes) |

### Filter parameters

| Parameter | Use |
|---|---|
| `always_filter` | User-visible default, changeable — typically default date window |
| `conditionally_filter` | Default that drops if user filters on `unless: [field]` — cost guardrail |
| `sql_always_where` | Hidden, immutable structural invariant (data_source = 'cornerstone', soft-delete = FALSE, partition floor) |
| `sql_always_having` | Post-aggregation hidden filter — rare |
| `access_filter` | Per-user row-level security mapped to user_attribute |

**Anchoring rule (CRITICAL):** every `sql_always_where` / `access_filter` column must live on the base view OR a `required_joins` view. Otherwise auto-pruning changes grain silently.

### Description template (anchored to Radix's +20% description_similarity multiplier)

```
{One sentence: what this explore is, business terms}.

Use this explore to answer questions about {entity 1}, {entity 2}, and {entity 3},
including:
- {Example question 1, verbatim analyst vocabulary}
- {Example question 2}
- {Example question 3}
- {Example question 4}
- {Example question 5}

Grain: one row per {grain unit}.
Default filters: {what sql_always_where enforces}.

Joins available: {entity_a} ({cardinality}), {entity_b} ({cardinality}).
Synonyms: {term} = {synonym 1}, {synonym 2}.
```

BGE rewards: natural sentences, verbatim question phrasings, entity mentions, explicit synonyms for insider terms (NAA, AIF, cornerstone). Anti-patterns: marketing fluff, column-name lists, one-line descriptors, >250 words (BGE truncates at 512 tokens).

---

## Section 6 — Join rubric

| Parameter | Rule | Verdict if wrong |
|---|---|---|
| `join: <alias>` / `from:` | Use `from:` for self-joins (same view twice) | — |
| `relationship` | **MUST be corpus-validated.** one_to_one / many_to_one / one_to_many / many_to_many | BLOCK on wrong → silent fanout |
| `type` | Default `left_outer`. Use `inner` only when join is conceptually inseparable | WARN on misused inner |
| `sql_on` | Always `${alias.field}` substitution, never bare columns. Multi-condition uses AND, not OR | BLOCK on bare-column form |
| `view_label` | **MANDATORY for self-joins.** Recommended when same dim view used across multiple explores | WARN |
| `fields` | Filter exposed fields (especially to hide PII or reduce Radix index noise) | INFO |
| `required_joins` | For transitive multi-hop (B before C when C joins through B) | WARN if missing on multi-hop |

### Wrong-relationship symptom hierarchy (the silent-fail catalog)

1. Declare `many_to_one` when reality is `one_to_many` → sums multiply by right-side cardinality. **No error.**
2. Missing `primary_key` → symmetric aggregates skip → same multiplier bug.
3. Composite key as two `primary_key: yes` dims → Looker uses only the first → wrong dedup.
4. `count_distinct` on left side of `one_to_many` without `sql_distinct_key` → wrong unique count.

**Validation move:** for every declared relationship, run a `SELECT COUNT(*) / COUNT(DISTINCT pk)` ratio probe per join — if >1, you have a fan-out and the relationship is wrong.

---

## Section 7 — Aggregate_table rubric (Tier-3 performance)

```lookml
aggregate_table: <name> {
  query: {
    dimensions: [view.dim, view.dim_group_month]
    measures: [view.total_x, view.count]
    filters: [view.data_source: "cornerstone"]    # only when always-applied
    timezone: "America/New_York"
  }
  materialization: {
    datagroup_trigger: <model_datagroup>           # PREFER datagroup over sql_trigger_value
    # OR: sql_trigger_value: SELECT MAX(updated_at) FROM ... ;;
    # OR: persist_for: "24 hours"
  }
}
```

### 5 routing conditions Looker checks at query time

1. Every requested dim is in the aggregate's dimensions OR derivable (year derivable from month).
2. Every requested measure is in the aggregate's measures.
3. Every filter is on a field present in the aggregate.
4. No `sql_always_where` references a field absent from the aggregate.
5. No `access_filter` references a field absent from the aggregate.

If any condition fails → Looker routes to base table (slow but correct).

### Anti-patterns

- `count_distinct` over a pre-aggregated count → impossible math.
- Timezone mismatch between aggregate's `timezone:` and user's `query_timezone` → bypass to base.
- Aggregating beyond `month` and expecting `week` queries to route — they won't (week is not derivable from month).

---

## Section 8 — Access control rubric

### Row-level: `access_filter`

```lookml
access_filter: {
  field: cardmember.business_unit
  user_attribute: allowed_business_units
}
```

Maps user_attribute to a column. Fail-closed default: user with no attribute value sees no data.

### Column-level: `required_access_grants` + `access_grant` block in model

```lookml
# model file:
access_grant: pii_access {
  user_attribute: has_pii_access
  allowed_values: ["yes"]
}

# view file:
dimension: ssn_last_four {
  required_access_grants: [pii_access]
  tags: ["pii", "pii_role:10"]
}
```

MDM-to-LookML mapping:
- `sensitivity_details.is_pii=true` → `required_access_grants: [pii_access]`, `tags: ["pii"]`
- `is_gdpr=true` → `[gdpr_access]`, `tags: ["gdpr"]`
- `is_critical_data_element=true` → `tags: ["cde"]` (usually informational, not gating)
- `pii_role_id` (10/2/1/FINE/NGBD-SDE) → `tags: ["pii_role:<id>"]`

---

## Section 9 — Looker MCP `run_inline_query` contract

### Tool parameters

```
model      = LookML model name (required)
explore    = explore name within model (required)
fields     = ["view.field", ...] (required)
filters    = {"view.field": "<Looker filter expression>"}
sorts      = ["view.field desc 0", ...]
limit      = int or -1
query_timezone = IANA tz (defaults to UTC)
```

### Filter expression grammar (the hard part)

| Type | Examples |
|---|---|
| String | `FOO`, `FOO,BAR`, `-FOO`, `%FOO%`, `FOO%`, `EMPTY`, `NULL` |
| Number | `5`, `1,3,5`, `>1 AND <100`, `[5, 90]`, `(500, inf)`, `NULL` |
| Date relative | `today`, `last week`, `3 days ago`, `3 days ago for 2 days`, `before 2026-01-01` |
| Date absolute | `2026/05/05`, `2026/05`, `2026-Q4`, `2026-05-18 12:00 to 2026-05-18 14:00` |
| Boolean | `Yes` / `No` |
| Location | `40 miles from 36.97, -122.03`, `inside box from ... to ...` |

### Determinism guarantee

Same `(model, explore, fields, filters, sorts, limit)` + same LookML state → byte-identical SQL. The MCP layer adds NO SQL intelligence; it forwards the structured query, Looker compiles.

### Cross-field OR limitation

MCP toolbox doesn't yet expose `filter_expression` (open issue googleapis/mcp-toolbox#2974). Cross-field OR has to be modeled as a derived LookML dimension. **Implication for our filter_catalog:** `(A OR B) AND C` patterns can't be expressed through MCP today — pre-compute the OR as a yesno derived dim.

### The `query_sql` tool — our validation lever

Returns compiled BigQuery SQL without executing. Use it to:
- Dry-run validate every gold query through enriched LookML
- Cost-estimate via BQ dry_run
- Audit the deterministic SQL output

**This is the validation agent loop we said was missing.** No new infrastructure needed beyond calling `query_sql` per gold query.

---

## Section 10 — Anti-pattern compressed catalog

| Anti-pattern | Symptom | Fix |
|---|---|---|
| `hint:` parameter | Looker parser rejects | Use `tags` + `# RADIX_HINT:` comment |
| Wrong `relationship:` | Silent fanout, wrong sums | Validate with COUNT(*)/COUNT(DISTINCT pk) ratio |
| Missing `primary_key` | Symmetric aggregates skip → wrong sums | Add `primary_key: yes` on one dim or composite |
| Plain `dimension` on date column | One grain only, can't answer "by week/month/year" | Always use `dimension_group: type=time` with full timeframes |
| Missing `convert_tz: no` on BQ dates | Silent timezone double-shift | Add `convert_tz: no` |
| `type: string` on numeric column | Range filters break | Match BQ INFORMATION_SCHEMA |
| Allowed values in `description` | BGE dilutes; recall drops | Put in filter_catalog, not description |
| Description <80 or >220 chars | Embedding suboptimal | Aim 80-180 chars |
| `tags` > 8 items | Mean-pool dilution, precision falls | Cap at 8 |
| Auto-gen label `bus_seg` | Bad embedding match | Title Case, expand abbreviations |
| `sql_always_where` on sometimes-joined view | Grain changes silently | Anchor to base or `required_joins` |
| `access_filter` on absent column | SQL error | Anchor to base view |
| Multi-fact joined directly | Chasm trap, cross-product | Separate explores or `many_to_many` |
| `count_distinct` on pre-aggregated count | Impossible math | Use raw measures, route to base |
| Aggregate `timezone:` mismatch | Routes to base silently | Pin to user query timezone |
| Composite PK as 2 dims | Wrong dedup key | CONCAT into one synthetic dim |
| `type: inner` join user didn't request | Silent row filtering | Default to `left_outer` |
| Hand-editing auto-gen view file | Wiped on schema regen | Use refinement pattern `view: +name` |
| Explore description <100 words | Loses Radix's +20% multiplier | 150-250 words, 5 question patterns |
| Cross-field OR in filters via MCP | Not expressible today | Derived yesno dim |

---

## Section 11 — Signal → parameter mapping (deterministic feed)

For every LookML parameter, which signal we already have feeds it:

| Parameter | Signal source (deterministic) |
|---|---|
| view `sql_table_name` | `ctx.baseline_sql_table_name` or `mdm_dataset_source_details` |
| view `label` | `mdm_dataset_details.business_name` or humanize(table) |
| view `description` | Compose from `mdm_table_description` + cluster grain + scope from `sql_always_where` |
| dim `name` | column name from `ctx.columns_referenced` |
| dim `type` | `mdm_columns[X].type` + naming-pattern + observed-value heuristics |
| dim `sql` | `${TABLE}.<column>` |
| dim `label` | `mdm_columns[X].business_name` or humanize |
| dim `description` | `mdm_columns[X].attribute_desc` (truncated to 180) |
| dim `tags` | Union {MDM business_name, baseline sql_aliases, ontology synonyms, query SELECT aliases} capped at 8 |
| dim `group_label` | MDM `data_sub_category` mapping + naming-pattern fallback |
| dim `primary_key: yes` | `ctx.baseline_primary_key_column` or PK candidate ranking |
| dim `hidden: yes` | Naming pattern (`*_sort`, `*_code` when paired label exists) |
| dim `order_by_field` | Detect ordered-categorical pattern (CASE WHEN with sequential values) |
| dim `required_access_grants` | `mdm_columns[X].is_pii` etc. |
| dim `value_format_name` | MDM type + naming heuristics (amount→usd_0, count→decimal_0, pct→percent_2) |
| dim_group `convert_tz: no` | Always set (BQ default) |
| dim_group `timeframes` | Always full list |
| dim_group `datatype` | From `mdm_columns[X].type` (DATE/TIMESTAMP/DATETIME) |
| measure `name` | `total_<col>` / `unique_<col>` / `avg_<col>` from aggregation type |
| measure `type` | From `fp.aggregations[X].function` |
| measure `description` | "<Agg verb> of <col business name>..." templated |
| measure `value_format_name` | MDM type + agg type heuristic |
| measure `filters` | Detect filter co-occurrence (>80% of queries with this measure) |
| explore `description` | LLM authored per cluster, with template enforced |
| explore `always_filter` | From corpus canonical filters per cluster |
| explore `sql_always_where` | From `is_structural` filters + MDM partition floor |
| explore `access_filter` | From MDM `is_internal` / business_unit gating |
| join `relationship` | From `lumi.joins.infer_join_cardinalities` |
| join `sql_on` | From observed JOIN ON pairs |
| join `view_label` | T4 aliasing logic when same view used in 2+ explores |
| aggregate_table | From `propose_aggregate_tables` (hot GROUP BYs) |

**This table is the contract.** Every gold-standard LookML field has a signal that feeds it. No LLM judgment for anything in this table. The LLM only enters the loop for `description` content quality (within length constraints).

---

## Section 12 — Curator one-screen checklist

### Per dimension
- [ ] `type` matches BQ INFORMATION_SCHEMA
- [ ] `sql: ${TABLE}.col` (curly form)
- [ ] `label` Title Case, abbreviations expanded
- [ ] `description` 80-180 chars, business phrasing, no SQL leak, no enum dump
- [ ] `tags` 3-8 synonyms from gold queries + MDM + ontology
- [ ] `group_label` set
- [ ] `hidden: yes` if sort/code/surrogate
- [ ] `order_by_field` if business-ordered
- [ ] `primary_key: yes` on exactly one dim per view
- [ ] `required_access_grants` if PII

### Per dimension_group
- [ ] `type: time`
- [ ] `datatype` explicit
- [ ] `convert_tz: no` (BQ)
- [ ] `timeframes` full list (10 elements)
- [ ] `group_label: "Date"`

### Per measure
- [ ] `type` matches gold-query aggregation pattern
- [ ] `sql` curly form or `${other_measure}` ratio
- [ ] `value_format_name` preset set
- [ ] `description` opens with aggregation verb
- [ ] `filters` baked if high-frequency segment
- [ ] `drill_fields` references a set

### Per view
- [ ] `sql_table_name` OR `derived_table` (never both)
- [ ] `description` 3 sentences (grain / use / scope)
- [ ] `label` Title Case
- [ ] Refinement pattern (`view: +name`)
- [ ] `set: <view>_detail` defined
- [ ] `set: pii_fields` if PII exists

### Per explore
- [ ] `description` 150-250 words, 5 question patterns, synonyms section
- [ ] `relationship` on every join, corpus-validated
- [ ] `view_label` on self-joins
- [ ] `sql_always_where` anchored to base or `required_joins` view
- [ ] `access_filter` anchored to base
- [ ] `always_filter` for sensible default date window
- [ ] `aggregate_table` for hot GROUP BYs, with `datagroup_trigger`

---

## Section 13 — Bug remediation plan (production-blocking, must fix before next pipeline run)

| Bug | Files affected | Fix |
|---|---|---|
| `hint:` parameter rejected by Looker parser | `lumi/plan_builder.py`, `lumi/enrich.py`, `lumi/critic.py`, `lumi/filter_catalog.py`, SKILL.md, tests | Move `hint` content into `tags` (cap 8) + `# RADIX_HINT:` comment surface |
| `symmetric_aggregates` critic check logic wrong | `lumi/critic.py::_check_symmetric_aggregates` | Replace with `primary_key + relationship` correctness check |
| Description band 15-200 → 80-180 | `lumi/critic.py::_check_disambiguation_completeness`, guardrails.py | Tighten lower bound to 80, upper to 180 |
| `convert_tz: no` not enforced on dim_groups | enrich prompt + critic | Add `partition_freshness` critic check |
| `tags` no cap | enrich prompt + critic | Cap at 8 per field |
| Hand-editing auto-gen views | `lumi/publish.py::additive_merge_view` | Emit refinement files alongside, don't merge into auto-gen |
| `sql_always_where` anchoring not validated | critic | New check: every column in sql_always_where must be on base or required_joins |
| Aggregate_table default `sql_trigger_value` | `lumi/publish.py::_render_aggregate_table` | Use `datagroup_trigger` with model-level datagroup definition |

---

## Section 14 — The validation loop we need (closes the hallmark)

Use Looker MCP's `query_sql` tool against every gold query. Compare generated SQL's BigQuery result against the gold query's BigQuery result on the same data slice. Mismatches:

1. Same result → pass
2. Different result, same shape → wrong relationship or missing primary_key (Tier-A bug)
3. Different result, different shape → wrong explore routing (Tier-B bug)
4. SQL gen error → field/filter mismatch (Tier-C bug)

This is the agentic loop: hypothesis (enriched LookML) → run (Looker MCP query_sql + BQ) → compare → fix. Until we have this, every quality claim is structural.
