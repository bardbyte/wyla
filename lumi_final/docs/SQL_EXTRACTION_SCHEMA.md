# SQL Extraction Schema — what we pull from every gold query

The complete signal surface the sqlglot parser produces per SQL. Combines:
- **Layer 1** — what we extract today (`SQLFingerprint` fields)
- **Layer 3** — gaps to add (ORDER BY, HAVING, window functions, comments, etc.)

Layer 2 (corpus-derived: equivalences, cardinality, clusters, hot GROUP BYs) is computed
across queries — see `docs/GOLD_STANDARD_LOOKML.md` for that layer's contract.
Layer 4 (semantic distillations: metric profiles, entity profiles, question patterns)
is the graph layer built from Layer 1+2+3 outputs.

---

## How to read this template

Every field's value is a placeholder describing its **type** and **allowed enum** (where applicable).
Arrays show one representative element with placeholders inside. Empty arrays `[]` are still
reported by the parser — **absence is itself a signal**.

The template is self-contained: anyone implementing the parser knows exactly what shape
to produce, what enums are allowed, and what the populated form looks like.

---

## The template (single JSON object)

```json
{
  "query_id": "<string — Q01, Q02, ...>",
  "raw_sql": "<string — original SQL>",
  "parse_error": "<string | null>",
  "dialect": "bigquery",

  "tables": ["<canonical_table_name>"],
  "primary_table": "<string — the FROM table>",

  "aggregations": [
    {
      "function": "<SUM|COUNT|AVG|MIN|MAX|MEDIAN|PERCENTILE|STDDEV|VARIANCE|ARRAY_AGG|STRING_AGG>",
      "column": "<string>",
      "alias": "<string | null>",
      "distinct": "<bool>",
      "outer_expr": "<string — wrapping expression, e.g. ROUND(SUM(x), 2)>"
    }
  ],

  "case_whens": [
    {
      "alias": "<string>",
      "source_column": "<string>",
      "sql": "<string — full CASE WHEN expression>",
      "mapped_values": ["<string — the THEN bucket labels>"],
      "boundaries": [
        {
          "op": "<= | != | > | < | >= | <= | IN | IS NULL | ELSE>",
          "value": "<any>",
          "label": "<string — the THEN bucket label>"
        }
      ]
    }
  ],

  "ctes": [
    {
      "alias": "<string>",
      "sql": "<string — CTE body>",
      "source_tables": ["<string>"],
      "structural_filters": [
        {"column": "<string>", "operator": "<string>", "value": "<any>"}
      ],
      "cte_dependencies": ["<string — other CTE aliases this one references>"]
    }
  ],

  "temp_tables": [
    {
      "alias": "<string>",
      "sql": "<string>",
      "source_tables": ["<string>"],
      "structural_filters": [
        {"column": "<string>", "operator": "<string>", "value": "<any>"}
      ],
      "is_temp": "<bool>",
      "is_replace": "<bool>"
    }
  ],

  "joins": [
    {
      "left_table": "<string>",
      "left_alias": "<string | null>",
      "right_table": "<string>",
      "right_alias": "<string | null>",
      "left_key": "<string>",
      "right_key": "<string>",
      "join_type": "<inner | left | right | full | cross>",
      "order": "<int — order in the query>",
      "on_clause": "<string — raw ON expression>",
      "additional_conditions": ["<string — extra AND clauses inside ON>"]
    }
  ],

  "filters": [
    {
      "column": "<string>",
      "table": "<string | null>",
      "operator": "<= | != | > | < | >= | <= | LIKE | IN | NOT_IN | BETWEEN | IS_NULL | IS_NOT_NULL>",
      "value": "<any — string, number, or array for IN/BETWEEN>",
      "is_structural": "<bool — appears in >X% of corpus with same value>",
      "is_negated": "<bool>"
    }
  ],

  "date_functions": [
    {
      "column": "<string>",
      "function": "<DATE_TRUNC | EXTRACT | DATE_ADD | DATE_SUB | DATE_DIFF | FORMAT_DATE | CURRENT_DATE>",
      "granularity": "<DAY | WEEK | MONTH | QUARTER | YEAR | HOUR | MINUTE | SECOND | null>",
      "output_alias": "<string | null>"
    }
  ],

  "select_aliases": [
    {
      "column": "<string — underlying source column>",
      "alias": "<string>",
      "expression": "<string — full SELECT expression>",
      "is_derived": "<bool — true for CASE/ratio/CONCAT/etc>"
    }
  ],

  "group_by": [
    {
      "table": "<string | null>",
      "column": "<string>",
      "via_position": "<int | null — when GROUP BY 1, 2 syntax>",
      "expression": "<string — when GROUP BY is on an expression>"
    }
  ],

  "order_by": [
    {
      "column": "<string>",
      "alias": "<string | null — when ordering by SELECT alias>",
      "direction": "<ASC | DESC>",
      "nulls": "<FIRST | LAST | null>",
      "is_position_ref": "<bool — true for ORDER BY 1, 2 syntax>"
    }
  ],

  "having": [
    {
      "expression": "<string — raw HAVING clause>",
      "aggregation": "<SUM | COUNT | AVG | ...>",
      "source_column": "<string>",
      "operator": "<= | != | > | < | >= | <=>",
      "value": "<any>",
      "semantic_class": "<threshold | count_filter | comparison | null>"
    }
  ],

  "limit": {
    "value": "<int | null>",
    "offset": "<int | null>",
    "is_top_n": "<bool — true when ORDER BY + LIMIT combine into top-N>"
  },

  "distinct_select": "<bool — SELECT DISTINCT at top level>",

  "distinct_aggregations": [
    {
      "column": "<string>",
      "in_function": "<COUNT | SUM | AVG | ...>"
    }
  ],

  "window_functions": [
    {
      "function": "<ROW_NUMBER | RANK | DENSE_RANK | LAG | LEAD | NTILE | FIRST_VALUE | LAST_VALUE | NTH_VALUE>",
      "partition_by": ["<string>"],
      "order_by": [
        {"column": "<string>", "direction": "<ASC | DESC>"}
      ],
      "alias": "<string | null>",
      "expression": "<string — full window expression>"
    }
  ],

  "subqueries": [
    {
      "type": "<IN_WHERE | EXISTS | NOT_EXISTS | SCALAR_SELECT | DERIVED_TABLE | CORRELATED>",
      "context": "<string — clause this subquery lives in>",
      "tables": ["<string>"],
      "is_correlated": "<bool>",
      "sql": "<string — subquery body>"
    }
  ],

  "set_operations": [
    {
      "type": "<UNION | UNION_ALL | INTERSECT | EXCEPT>",
      "branch_count": "<int>",
      "branches": [
        {"primary_table": "<string>", "fields_summary": "<string>"}
      ]
    }
  ],

  "null_handlers": [
    {
      "function": "<COALESCE | IFNULL | NULLIF>",
      "columns_involved": ["<string>"],
      "default_value": "<any | null>",
      "expression": "<string>"
    }
  ],

  "type_casts": [
    {
      "column": "<string>",
      "from_type": "<string | null>",
      "to_type": "<string>",
      "is_safe": "<bool — true for SAFE_CAST>",
      "expression": "<string>"
    }
  ],

  "string_functions": [
    {
      "function": "<CONCAT | SUBSTR | REGEXP_EXTRACT | REGEXP_REPLACE | REGEXP_CONTAINS | UPPER | LOWER | TRIM | SPLIT>",
      "columns": ["<string>"],
      "alias": "<string | null>",
      "expression": "<string>"
    }
  ],

  "math_functions": [
    {
      "function": "<ROUND | FLOOR | CEIL | ABS | MOD | POW | LOG | EXP | SQRT>",
      "column": "<string>",
      "alias": "<string | null>",
      "expression": "<string>"
    }
  ],

  "comments": [
    {
      "type": "<line | block>",
      "position": "<before_select | inline | after_clause | end_of_query>",
      "text": "<string — gold source for NL phrasings>"
    }
  ],

  "parameters": [
    {
      "name": "<string — e.g. @start_date, ${region}>",
      "type_inferred": "<string | null>",
      "used_in_clause": "<WHERE | FROM | SELECT | HAVING>"
    }
  ],

  "qualify_clauses": [
    {
      "expression": "<string — full QUALIFY expression>",
      "window_function": "<string — which window function is filtered>",
      "operator": "<string>",
      "value": "<any>"
    }
  ],

  "array_operations": [
    {
      "operation": "<UNNEST | ARRAY_AGG | ARRAY_LENGTH | EXISTS_ELEMENT>",
      "column": "<string>",
      "context": "<FROM | SELECT | WHERE>",
      "alias": "<string | null>"
    }
  ],

  "struct_access": [
    {
      "path": ["<string — e.g. ['payment', 'method', 'type']>"],
      "root_column": "<string>",
      "alias": "<string | null>"
    }
  ],

  "json_operations": [
    {
      "function": "<JSON_EXTRACT | JSON_VALUE | JSON_QUERY | JSON_EXTRACT_ARRAY>",
      "column": "<string>",
      "path": "<string — e.g. $.payment.amount>",
      "alias": "<string | null>"
    }
  ],

  "self_joins": [
    {
      "table": "<string>",
      "aliases_used": ["<string>"],
      "role_hint": "<string | null — inferred from alias semantics>"
    }
  ],

  "partition_pseudocolumns": [
    {
      "column": "<_PARTITIONTIME | _PARTITIONDATE>",
      "table": "<string>",
      "in_clause": "<WHERE | SELECT>"
    }
  ],

  "sql_hints": [
    {"hint": "<string — optimizer hint inside /*+ */>"}
  ],

  "query_shape_summary": {
    "n_tables": "<int>",
    "n_joins": "<int>",
    "n_aggregations": "<int>",
    "n_filters": "<int>",
    "n_ctes": "<int>",
    "n_group_by": "<int>",
    "n_select_columns": "<int>",
    "has_having": "<bool>",
    "has_order_by": "<bool>",
    "has_limit": "<bool>",
    "has_distinct": "<bool>",
    "has_window_function": "<bool>",
    "has_subquery": "<bool>",
    "has_set_operation": "<bool>",
    "complexity_score": "<int — 0=simple, 1=CTEs, 2=joins, 3=both, +1 per window/set_op>"
  }
}
```

---

## Worked example — the same template filled in

This is what the parser would emit for one moderately-complex AmEx-shaped SQL.

### Input SQL

```sql
WITH active_consumers AS (
  SELECT cm11
  FROM cardmember_dim
  WHERE cm_status = 'Active'
    AND bus_seg = 'Consumer'
)
SELECT
  DATE_TRUNC(c.rpt_dt, MONTH) AS report_month,
  CASE
    WHEN c.fico_score >= 740 THEN 'Prime'
    WHEN c.fico_score >= 670 THEN 'Near-Prime'
    ELSE 'Sub-Prime'
  END AS fico_band,
  SUM(c.billed_business) AS total_bb,
  COUNT(DISTINCT c.cm11) AS unique_cardmembers,
  AVG(c.billed_business) AS avg_bb_per_row
FROM cornerstone_metrics c
JOIN active_consumers a ON c.cm11 = a.cm11
WHERE c.data_source = 'cornerstone'
  AND c.rpt_dt >= '2025-01-01'
GROUP BY 1, 2
HAVING SUM(c.billed_business) > 1000
ORDER BY report_month DESC, total_bb DESC
LIMIT 100;
```

### Extracted output

```json
{
  "query_id": "Q047",
  "raw_sql": "WITH active_consumers AS (SELECT cm11 FROM cardmember_dim WHERE cm_status = 'Active' AND bus_seg = 'Consumer') SELECT DATE_TRUNC(c.rpt_dt, MONTH) AS report_month, CASE WHEN c.fico_score >= 740 THEN 'Prime' WHEN c.fico_score >= 670 THEN 'Near-Prime' ELSE 'Sub-Prime' END AS fico_band, SUM(c.billed_business) AS total_bb, COUNT(DISTINCT c.cm11) AS unique_cardmembers, AVG(c.billed_business) AS avg_bb_per_row FROM cornerstone_metrics c JOIN active_consumers a ON c.cm11 = a.cm11 WHERE c.data_source = 'cornerstone' AND c.rpt_dt >= '2025-01-01' GROUP BY 1, 2 HAVING SUM(c.billed_business) > 1000 ORDER BY report_month DESC, total_bb DESC LIMIT 100",
  "parse_error": null,
  "dialect": "bigquery",

  "tables": ["cornerstone_metrics", "cardmember_dim"],
  "primary_table": "cornerstone_metrics",

  "aggregations": [
    {"function": "SUM",   "column": "billed_business", "alias": "total_bb",           "distinct": false, "outer_expr": ""},
    {"function": "COUNT", "column": "cm11",            "alias": "unique_cardmembers", "distinct": true,  "outer_expr": ""},
    {"function": "AVG",   "column": "billed_business", "alias": "avg_bb_per_row",     "distinct": false, "outer_expr": ""}
  ],

  "case_whens": [
    {
      "alias": "fico_band",
      "source_column": "fico_score",
      "sql": "CASE WHEN fico_score >= 740 THEN 'Prime' WHEN fico_score >= 670 THEN 'Near-Prime' ELSE 'Sub-Prime' END",
      "mapped_values": ["Prime", "Near-Prime", "Sub-Prime"],
      "boundaries": [
        {"op": ">=", "value": 740, "label": "Prime"},
        {"op": ">=", "value": 670, "label": "Near-Prime"},
        {"op": "ELSE", "value": null, "label": "Sub-Prime"}
      ]
    }
  ],

  "ctes": [
    {
      "alias": "active_consumers",
      "sql": "SELECT cm11 FROM cardmember_dim WHERE cm_status = 'Active' AND bus_seg = 'Consumer'",
      "source_tables": ["cardmember_dim"],
      "structural_filters": [
        {"column": "cm_status", "operator": "=", "value": "Active"},
        {"column": "bus_seg",   "operator": "=", "value": "Consumer"}
      ],
      "cte_dependencies": []
    }
  ],

  "temp_tables": [],

  "joins": [
    {
      "left_table": "cornerstone_metrics", "left_alias": "c",
      "right_table": "active_consumers",   "right_alias": "a",
      "left_key": "cm11", "right_key": "cm11",
      "join_type": "inner", "order": 1,
      "on_clause": "c.cm11 = a.cm11",
      "additional_conditions": []
    }
  ],

  "filters": [
    {"column": "data_source", "table": "c", "operator": "=",  "value": "cornerstone", "is_structural": true,  "is_negated": false},
    {"column": "rpt_dt",      "table": "c", "operator": ">=", "value": "2025-01-01",  "is_structural": false, "is_negated": false}
  ],

  "date_functions": [
    {"column": "rpt_dt", "function": "DATE_TRUNC", "granularity": "MONTH", "output_alias": "report_month"}
  ],

  "select_aliases": [
    {"column": "rpt_dt",          "alias": "report_month",       "expression": "DATE_TRUNC(c.rpt_dt, MONTH)",            "is_derived": true},
    {"column": "fico_score",      "alias": "fico_band",          "expression": "CASE WHEN c.fico_score >= 740 ... END",  "is_derived": true},
    {"column": "billed_business", "alias": "total_bb",           "expression": "SUM(c.billed_business)",                 "is_derived": true},
    {"column": "cm11",            "alias": "unique_cardmembers", "expression": "COUNT(DISTINCT c.cm11)",                 "is_derived": true},
    {"column": "billed_business", "alias": "avg_bb_per_row",     "expression": "AVG(c.billed_business)",                 "is_derived": true}
  ],

  "group_by": [
    {"table": "c", "column": "rpt_dt",     "via_position": 1, "expression": "DATE_TRUNC(c.rpt_dt, MONTH)"},
    {"table": "c", "column": "fico_score", "via_position": 2, "expression": "CASE WHEN c.fico_score >= 740 ... END"}
  ],

  "order_by": [
    {"column": "report_month", "alias": "report_month", "direction": "DESC", "nulls": null, "is_position_ref": false},
    {"column": "total_bb",     "alias": "total_bb",     "direction": "DESC", "nulls": null, "is_position_ref": false}
  ],

  "having": [
    {
      "expression": "SUM(c.billed_business) > 1000",
      "aggregation": "SUM",
      "source_column": "billed_business",
      "operator": ">",
      "value": 1000,
      "semantic_class": "threshold"
    }
  ],

  "limit": {"value": 100, "offset": null, "is_top_n": true},

  "distinct_select": false,
  "distinct_aggregations": [{"column": "cm11", "in_function": "COUNT"}],

  "window_functions": [],
  "subqueries": [],
  "set_operations": [],
  "null_handlers": [],
  "type_casts": [],
  "string_functions": [],
  "math_functions": [],
  "comments": [],
  "parameters": [],
  "qualify_clauses": [],
  "array_operations": [],
  "struct_access": [],
  "json_operations": [],
  "self_joins": [],
  "partition_pseudocolumns": [],
  "sql_hints": [],

  "query_shape_summary": {
    "n_tables": 2, "n_joins": 1, "n_aggregations": 3, "n_filters": 2,
    "n_ctes": 1, "n_group_by": 2, "n_select_columns": 5,
    "has_having": true, "has_order_by": true, "has_limit": true,
    "has_distinct": true, "has_window_function": false,
    "has_subquery": false, "has_set_operation": false,
    "complexity_score": 4
  }
}
```

---

## Field legend

| Layer | Fields |
|---|---|
| **Layer 1** (extracted today) | `tables`, `primary_table`, `aggregations`, `case_whens`, `ctes`, `temp_tables`, `joins`, `filters`, `date_functions`, `select_aliases`, `group_by` |
| **Layer 3** (gaps to add) | `order_by`, `having`, `limit`, `distinct_select`, `distinct_aggregations`, `window_functions`, `subqueries`, `set_operations`, `null_handlers`, `type_casts`, `string_functions`, `math_functions`, `comments`, `parameters`, `qualify_clauses`, `array_operations`, `struct_access`, `json_operations`, `self_joins`, `partition_pseudocolumns`, `sql_hints` |
| **Derived** | `query_shape_summary` — deterministically computed from the above |

---

## What each field unlocks downstream

| Field | Powers (LookML render / Radix retrieval / Semantic graph) |
|---|---|
| `tables`, `primary_table` | Explore base_view choice; cluster signature |
| `aggregations` | Measure proposals; metric profile formulas |
| `case_whens` | Derived dimension proposals; analyst bucketing logic |
| `ctes`, `temp_tables` | `derived_table` view candidates; PDT proposals |
| `joins` | Equivalence classes; cardinality inference; explore join graph |
| `filters` | `filter_catalog` values; `sql_always_where`; mandatory partition filters |
| `date_functions` | `dimension_group` timeframes; canonical time granularity per column |
| `select_aliases` | Analyst vocabulary → `tags`, `hint` (via `# RADIX_HINT:` comment), `label` |
| `group_by` | Cluster signatures; hot GROUP BY → aggregate_table proposals |
| `order_by` | `drill_fields` ranking; default sort on explore |
| `having` | Filtered measure candidates; metric threshold semantics |
| `limit` + `order_by` | Top-N question pattern detection |
| `window_functions` | Derived measure candidates (ranks, lags, running totals) |
| `subqueries` | Derived_table candidates; nested business logic |
| `set_operations` | Reconciliation explore candidates |
| `null_handlers` | `default_value` hints; ratio-denominator protection patterns |
| `type_casts` | MDM type correction hints (when analysts repeatedly cast a column) |
| `string_functions` | Derived dim candidates (substring → prefix dim) |
| `math_functions` | `value_format_name` hints (ROUND to cents → usd) |
| `comments` | Question pattern NL phrasings; explore description seeds |
| `parameters` | User-input filter patterns → `always_filter` candidates |
| `qualify_clauses` | Post-window measure logic |
| `array_operations` | Flattened-dim candidates from nested data |
| `struct_access` | Dot-path columns → first-class dim proposals |
| `json_operations` | JSON-path columns → extracted dim proposals |
| `self_joins` | `from:` + `view_label:` aliasing in explore joins |
| `partition_pseudocolumns` | Mandatory `sql_always_where` on partition |
| `sql_hints` | Performance tuning signals (rare but useful) |
| `query_shape_summary` | Cluster ranking; complexity gating; review prioritization |

---

## Where this fits in the larger architecture

```
┌──────────────────────────────────────────────────────────────┐
│   SQL (one of 122 gold queries)                              │
└────────────────────┬─────────────────────────────────────────┘
                     │ sqlglot parser
                     ▼
┌──────────────────────────────────────────────────────────────┐
│   Layer 1 + Layer 3 extraction (this template)               │
│   ~100 K:V pairs per SQL                                     │
└────────────────────┬─────────────────────────────────────────┘
                     │ corpus composition
                     ▼
┌──────────────────────────────────────────────────────────────┐
│   Layer 2 — equivalence classes, join cardinality,           │
│   query clusters, hot GROUP BYs, filter value sets, ...      │
└────────────────────┬─────────────────────────────────────────┘
                     │ semantic distillation
                     ▼
┌──────────────────────────────────────────────────────────────┐
│   Layer 4 — Metric / Entity / QuestionPattern / Filter       │
│   profiles → SEMANTIC GRAPH (the brain)                      │
└────────────────────┬─────────────────────────────────────────┘
                     │ renderers
        ┌────────────┼────────────┬─────────────┐
        ▼            ▼            ▼             ▼
   .view.lkml   .model.lkml  filter_catalog  golden_questions
                                  .json          .json
                     │
                     ▼ consumed by
              ┌──────────────┐
              │   Radix      │  → Looker MCP → BigQuery
              └──────────────┘
```

The template above is the **input layer** that feeds everything else. Get the parser right
and every downstream consumer has the substrate it needs. Get it wrong and the rest of the
system is hallucinating from incomplete signal.
