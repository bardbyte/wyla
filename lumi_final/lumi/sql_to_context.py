"""Stage 1: Parse SQL + Stage 2: Discover tables (MDM + baseline).

All deterministic — no LLM calls. Built in Session 1.

Public API:
    parse_sqls(sqls)                                 → list[SQLFingerprint]
    discover_tables(fps, mdm_client, baseline_dir)   → dict[str, TableContext]
    prepare_enrichment_context(sqls, mdm, baseline)  → dict[str, TableContext]

The fingerprint module is intentionally NOT a Pydantic model — sqlglot output
shapes vary, and we want to surface what we extracted as a flat dataclass that
Stage 2 (discover) consumes. TableContext (in lumi.schemas) is the cross-stage
contract; SQLFingerprint is the intra-Stage-1 representation.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import sqlglot
from sqlglot import exp

from lumi.schemas import TableContext

logger = logging.getLogger("lumi.sql_to_context")

BQ_DIALECT = "bigquery"

# SQL operators we extract from WHERE clauses.
_BINARY_OPS = {
    exp.EQ: "=",
    exp.NEQ: "!=",
    exp.GT: ">",
    exp.GTE: ">=",
    exp.LT: "<",
    exp.LTE: "<=",
}


# ─── MDM client protocol ─────────────────────────────────────


class MDMClientProto(Protocol):
    """Anything with a .fetch(table_name) -> dict method.

    The real MDM client (HTTP-backed) and MockMDMClient (in-process) both
    satisfy this protocol. Tests use the mock; the pipeline uses the real one.
    """

    def fetch(self, table_name: str) -> dict[str, Any]: ...


# ─── SQLFingerprint — Stage 1 output ─────────────────────────


@dataclass
class SQLFingerprint:
    """Everything sqlglot extracts from one SQL string.

    Lives between parse_sqls() (producer) and discover_tables() (consumer).
    Not a Pydantic model: sqlglot output is messy and we'd rather keep this
    flexible than fight pydantic validation on every edge case.
    """

    raw_sql: str
    tables: list[str] = field(default_factory=list)
    primary_table: str | None = None
    aggregations: list[dict[str, Any]] = field(default_factory=list)
    case_whens: list[dict[str, Any]] = field(default_factory=list)
    ctes: list[dict[str, Any]] = field(default_factory=list)
    # CREATE [OR REPLACE] [TEMP] TABLE x AS SELECT ... — semantically a
    # CTE-equivalent (named intermediate result over real source tables).
    # Same shape as a CTE entry: alias, source_tables, structural_filters,
    # sql, plus is_temp/is_replace for future PDT-candidate detection.
    temp_tables: list[dict[str, Any]] = field(default_factory=list)
    joins: list[dict[str, Any]] = field(default_factory=list)
    filters: list[dict[str, Any]] = field(default_factory=list)
    date_functions: list[dict[str, Any]] = field(default_factory=list)
    # SELECT col AS alias — analysts' own glossary. Per-column list of
    # aliases observed in this query's SELECT clause. Only meaningful
    # aliases (passes _alias_quality_filter) are kept; trivial ones
    # (`a`, `t1`, `tmp`) get filtered out at aggregation time but the
    # raw capture here keeps everything for traceability.
    # Format: [{"column": str, "alias": str, "expression": str}]
    select_aliases: list[dict[str, Any]] = field(default_factory=list)
    # GROUP BY columns from the top-level SELECT — signals which side of
    # a join is the dimension (the side we group by) vs the fact (the
    # side carrying aggregations). Format: [{table: str|None, column: str}].
    # Used by lumi.joins to infer JOIN cardinality from query semantics.
    group_by: list[dict[str, Any]] = field(default_factory=list)

    # ─── Layer 3: gaps closed in Phase 0 ─────────────────────
    # Each list is empty when the construct doesn't appear in this SQL —
    # absence IS a signal (e.g., no HAVING → not a filtered-aggregate query).

    # ORDER BY columns — what analysts sort by; drives drill_fields ranking
    # and default sort on the explore. Format:
    # [{column, alias, direction, nulls, is_position_ref}]
    order_by: list[dict[str, Any]] = field(default_factory=list)

    # HAVING clauses — post-aggregation filters. Each entry carries
    # threshold semantics that feed Metric.observed_thresholds in the
    # semantic graph (e.g., "meaningful spender" = >$1000).
    # Format: [{expression, aggregation, source_column, operator, value, semantic_class}]
    having: list[dict[str, Any]] = field(default_factory=list)

    # LIMIT / OFFSET — top-N pattern detection when paired with ORDER BY.
    # Format: {value, offset, is_top_n}
    limit: dict[str, Any] = field(default_factory=dict)

    # SELECT DISTINCT — top-level dedup intent (separate from DISTINCT
    # inside aggregations, which is captured on `aggregations[].distinct`).
    distinct_select: bool = False

    # Window functions — ROW_NUMBER / RANK / LAG / LEAD / NTILE / etc.
    # Format: [{function, partition_by[], order_by:[{column, direction}], alias, expression}]
    window_functions: list[dict[str, Any]] = field(default_factory=list)

    # Subqueries — IN (SELECT …), EXISTS, scalar SELECT, derived tables.
    # Format: [{type, context, tables[], is_correlated, sql}]
    subqueries: list[dict[str, Any]] = field(default_factory=list)

    # Set operations — UNION / UNION ALL / INTERSECT / EXCEPT.
    # Format: [{type, branch_count, branches:[{primary_table, fields_summary}]}]
    set_operations: list[dict[str, Any]] = field(default_factory=list)

    # NULL handlers — COALESCE / IFNULL / NULLIF. Hints for `default_value`
    # and ratio-denominator protection patterns.
    # Format: [{function, columns_involved[], default_value, expression}]
    null_handlers: list[dict[str, Any]] = field(default_factory=list)

    # Type casts — CAST / SAFE_CAST. Repeated cast on the same column hints
    # MDM type is wrong and should be corrected upstream.
    # Format: [{column, from_type, to_type, is_safe, expression}]
    type_casts: list[dict[str, Any]] = field(default_factory=list)

    # String functions — CONCAT / SUBSTR / REGEXP_* / UPPER / LOWER / TRIM.
    # Derived-dim candidates ("first 3 chars of postal = state").
    # Format: [{function, columns[], alias, expression}]
    string_functions: list[dict[str, Any]] = field(default_factory=list)

    # Math functions — ROUND / FLOOR / CEIL / ABS / MOD / etc. Format hint
    # for value_format_name (frequent ROUND to cents → usd).
    # Format: [{function, column, alias, expression}]
    math_functions: list[dict[str, Any]] = field(default_factory=list)

    # Comments — line vs block. Gold source for NL question phrasings
    # mineable into Explore.primary_questions. Format:
    # [{type, position, text}]
    comments: list[dict[str, Any]] = field(default_factory=list)

    # SQL parameters — @param / ${var} placeholders. User-input filter
    # patterns → always_filter candidates.
    # Format: [{name, type_inferred, used_in_clause}]
    parameters: list[dict[str, Any]] = field(default_factory=list)

    # QUALIFY clauses — BigQuery post-window filter; derived-measure logic.
    # Format: [{expression, window_function, operator, value}]
    qualify_clauses: list[dict[str, Any]] = field(default_factory=list)

    # Array operations — UNNEST / ARRAY_AGG / ARRAY_LENGTH. Flattened-dim
    # candidates from nested data.
    # Format: [{operation, column, context, alias}]
    array_operations: list[dict[str, Any]] = field(default_factory=list)

    # STRUCT dot-path access — payment.method.type style. First-class
    # dim proposals from nested fields.
    # Format: [{path[], root_column, alias}]
    struct_access: list[dict[str, Any]] = field(default_factory=list)

    # JSON operations — JSON_EXTRACT / JSON_VALUE / JSON_QUERY.
    # Format: [{function, column, path, alias}]
    json_operations: list[dict[str, Any]] = field(default_factory=list)

    # Self-joins — same canonical table joined to itself via different
    # aliases. Drives `from:` + `view_label:` aliasing in explore joins.
    # Format: [{table, aliases_used[], role_hint}]
    self_joins: list[dict[str, Any]] = field(default_factory=list)

    # BigQuery partition pseudocolumns — _PARTITIONTIME / _PARTITIONDATE.
    # Implicit mandatory sql_always_where.
    # Format: [{column, table, in_clause}]
    partition_pseudocolumns: list[dict[str, Any]] = field(default_factory=list)

    # Optimizer hints inside /*+ */. Format: [{hint}]
    sql_hints: list[dict[str, Any]] = field(default_factory=list)

    # Query shape summary — counts + complexity. Drives cluster ranking
    # and complexity gating in review prioritization.
    query_shape_summary: dict[str, Any] = field(default_factory=dict)

    # ─── Layer 4: semantic-graph-feeding signals (Phase 0d) ─
    # Derived from the above — feed directly into the semantic graph.

    # SHA256 of sqlglot-canonicalized SQL. Dedup key at the graph level
    # so duplicate queries don't double-count evidence.
    query_fingerprint_hash: str = ""

    # Inferred intent class from query shape (deterministic rules over
    # aggregations + group_by + filters + window_functions + limit).
    # Values: single_lookup | aggregate | trend | cohort | attribution
    #       | top_n | comparison | unknown
    inferred_intent_class: str = "unknown"

    # MDM data_category tags collected from every table touched.
    # Format: list of distinct category strings.
    business_domain_tags: list[str] = field(default_factory=list)

    # Implicit grain inferred from GROUP BY + aggregation source tables.
    # Example: "cardmember-day-snapshot", "merchant-month-total".
    # Empty string when grain is undefined (no GROUP BY, no aggregation).
    implicit_grain: str = ""

    # Derived dimension proposals from CASE WHEN.
    # Format: [{output_name, source_column, buckets[], bucket_type}]
    derived_dim_proposals: list[dict[str, Any]] = field(default_factory=list)

    # Cohort scope signal — when a CTE name implies a named cohort
    # (e.g., active_consumers), capture as graph Cohort node candidate.
    # Format: [{cohort_name, definition_filters[], source_cte}]
    cohort_scope_signals: list[dict[str, Any]] = field(default_factory=list)

    parse_error: str | None = None


# ─── Stage 1: parse ─────────────────────────────────────────


def parse_sqls(sqls: list[str]) -> list[SQLFingerprint]:
    """Parse each SQL with sqlglot. Errors don't crash — they land on
    SQLFingerprint.parse_error and discover_tables() can decide what to do.
    """
    return [_parse_one(sql) for sql in sqls]


def _trim_for_parse(sql: str) -> str:
    """Strip trailing junk that confuses sqlglot.

    Handles: trailing semicolons, BOM, surrounding whitespace, and SQL-line
    comments at the very end ('-- some note'). Doesn't try to be clever
    about MIDDLE-of-query problems; that's the user's data.
    """
    s = (sql or "").lstrip("﻿").strip()
    # Drop any pure-whitespace + semicolons at the end (one or many).
    while s.endswith(";"):
        s = s[:-1].rstrip()
    return s


def _parse_one(raw_sql: str) -> SQLFingerprint:
    fp = SQLFingerprint(raw_sql=raw_sql)
    cleaned = _trim_for_parse(raw_sql)
    # Excel exports often produce empty cells or stringified empties ('').
    # Mark these with a distinct sentinel so the guardrail can separate
    # "no SQL to parse" from "real sqlglot error".
    if not cleaned or cleaned in ("''", '""', "``"):
        fp.parse_error = "empty_input"
        return fp

    tree: exp.Expression | None = None
    first_error: Exception | None = None

    try:
        tree = sqlglot.parse_one(cleaned, dialect=BQ_DIALECT)
    except Exception as e:
        first_error = e
        # Cell may have multiple statements (semicolon-separated). Pull the
        # first SELECT/WITH and re-parse that one.
        try:
            statements = sqlglot.parse(cleaned, dialect=BQ_DIALECT)
        except Exception:
            statements = []
        for stmt in statements:
            if stmt is None:
                continue
            if isinstance(stmt, exp.Select | exp.With | exp.Subquery):
                tree = stmt
                break

    if tree is None:
        fp.parse_error = f"{type(first_error).__name__}: {first_error}"
        return fp

    fp.ctes = _extract_ctes(tree)
    cte_aliases = {c["alias"] for c in fp.ctes}
    fp.temp_tables = _extract_temp_tables(tree, cte_aliases)
    create_aliases = {t["alias"] for t in fp.temp_tables}

    fp.tables = _extract_tables(tree, exclude=cte_aliases | create_aliases)
    fp.primary_table = fp.tables[0] if fp.tables else None
    fp.aggregations = _extract_aggregations(tree)
    fp.case_whens = _extract_case_whens(tree)
    fp.joins = _extract_joins(tree)
    fp.filters = _extract_filters(tree)
    fp.date_functions = _extract_date_functions(tree)
    fp.select_aliases = _extract_select_aliases(tree)
    fp.group_by = _extract_group_by(tree)

    # ─── Layer 3 (Phase 0): per-SQL signal expansion ─────────
    # Each extractor is best-effort and returns empty when not applicable.
    # Wrapped in try/except so one malformed clause doesn't kill the whole
    # fingerprint — same policy as the existing extractors.
    for attr, fn in (
        ("order_by", _extract_order_by),
        ("having", _extract_having),
        ("limit", _extract_limit),
        ("distinct_select", _extract_distinct_select),
        ("window_functions", _extract_window_functions),
        ("subqueries", _extract_subqueries),
        ("set_operations", _extract_set_operations),
        ("null_handlers", _extract_null_handlers),
        ("type_casts", _extract_type_casts),
        ("string_functions", _extract_string_functions),
        ("math_functions", _extract_math_functions),
        ("comments", _extract_comments),
        ("parameters", _extract_parameters),
        ("qualify_clauses", _extract_qualify_clauses),
        ("array_operations", _extract_array_operations),
        ("struct_access", _extract_struct_access),
        ("json_operations", _extract_json_operations),
        ("self_joins", _extract_self_joins),
        ("partition_pseudocolumns", _extract_partition_pseudocolumns),
        ("sql_hints", _extract_sql_hints),
    ):
        try:
            setattr(fp, attr, fn(tree))
        except Exception as e:
            logger.debug("layer3 extractor %s failed: %s", attr, e)
            # leave field at default

    # Pair LIMIT with ORDER BY → is_top_n
    if fp.limit and fp.order_by:
        fp.limit["is_top_n"] = bool(fp.limit.get("value")) and bool(fp.order_by)

    # Query shape summary — pure composition.
    fp.query_shape_summary = _build_query_shape_summary(fp)

    # ─── Layer 4 (Phase 0d): semantic-feeding signals ────────
    try:
        fp.query_fingerprint_hash = _compute_query_fingerprint_hash(cleaned)
    except Exception as e:
        logger.debug("fingerprint hash failed: %s", e)
    try:
        fp.inferred_intent_class = _infer_intent_class(fp)
    except Exception as e:
        logger.debug("intent class inference failed: %s", e)
    try:
        fp.implicit_grain = _infer_implicit_grain(fp)
    except Exception as e:
        logger.debug("implicit grain inference failed: %s", e)
    try:
        fp.derived_dim_proposals = _propose_derived_dims(fp)
    except Exception as e:
        logger.debug("derived dim proposals failed: %s", e)
    try:
        fp.cohort_scope_signals = _detect_cohort_scopes(fp)
    except Exception as e:
        logger.debug("cohort scope detection failed: %s", e)
    # business_domain_tags is populated by discover_tables() later when
    # MDM data is hydrated; leave empty here

    return fp


def _extract_tables(tree: exp.Expression, exclude: set[str]) -> list[str]:
    """Real tables (not CTE aliases or CREATE-target tables).
    Preserves first-seen order.
    """
    seen: list[str] = []
    for t in tree.find_all(exp.Table):
        name = t.name
        if name and name not in exclude and name not in seen:
            seen.append(name)
    return seen


def _extract_temp_tables(
    tree: exp.Expression,
    cte_aliases: set[str],
) -> list[dict[str, Any]]:
    """Capture each `CREATE [OR REPLACE] [TEMP] TABLE x AS SELECT ...` as a
    CTE-equivalent.

    Same fields as a CTE entry plus two flags useful downstream:
      - is_temp:    BigQuery TEMP qualifier — purely session-scoped.
      - is_replace: had OR REPLACE — usually means re-run friendliness.

    The alias itself is NOT a real BQ table (Looker can't query a session
    temp table), so it stays out of fp.tables. But we keep the structural
    metadata so:
      - source tables get the temp table's structural filters attributed
        (same path CTEs use)
      - the planner can flag temp tables that get reused as Looker PDT
        (persistent derived table) candidates
      - business-named intermediates ('renewal_fees', 'active_customers')
        feed the NL-question / synonym layer

    Pure CREATE TABLE without a SELECT body (e.g. CREATE TABLE foo (id INT64))
    is skipped — that's a DDL statement, no semantics to extract.
    """
    out: list[dict[str, Any]] = []
    for create in tree.find_all(exp.Create):
        # Skip non-table CREATEs (CREATE FUNCTION, CREATE PROCEDURE, etc.)
        kind = (create.args.get("kind") or "").upper()
        if kind and kind != "TABLE":
            continue

        target = create.this
        alias = getattr(target, "name", None)
        if not isinstance(alias, str) or not alias:
            continue

        # The body of CREATE ... AS SELECT lives in `expression`. CREATE TABLE
        # foo (id INT64) has no expression — pure DDL, skip.
        body = create.args.get("expression")
        if body is None:
            continue

        # sqlglot stores TEMP under .properties as a TemporaryProperty, not a
        # top-level arg. Walk the Properties node to detect it.
        is_temp = False
        props = create.args.get("properties")
        if props is not None:
            for prop in props.expressions or []:
                if isinstance(prop, exp.TemporaryProperty):
                    is_temp = True
                    break
        is_replace = bool(create.args.get("replace"))

        # Structural filters (everything in the inner SELECT's WHERE).
        body_filters: list[dict[str, Any]] = []
        where = body.find(exp.Where) if hasattr(body, "find") else None
        if where is not None:
            body_filters = _flatten_predicates(where.this)
            for f in body_filters:
                f["is_structural"] = True

        # Source tables vs upstream CTE references inside the body.
        source_tables: list[str] = []
        cte_dependencies: list[str] = []
        for t in body.find_all(exp.Table):
            if not t.name:
                continue
            if t.name in cte_aliases:
                if t.name not in cte_dependencies:
                    cte_dependencies.append(t.name)
            else:
                if t.name not in source_tables:
                    source_tables.append(t.name)

        out.append({
            "alias": alias,
            "structural_filters": body_filters,
            "sql": body.sql(dialect=BQ_DIALECT),
            "source_tables": source_tables,
            "cte_dependencies": cte_dependencies,
            "is_temp": is_temp,
            "is_replace": is_replace,
        })
    return out


def _extract_aggregations(tree: exp.Expression) -> list[dict[str, Any]]:
    """Find SUM/COUNT/AVG/MIN/MAX/STDDEV/VAR with column + alias + outer expression."""
    out: list[dict[str, Any]] = []
    agg_classes = (
        exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max, exp.Stddev, exp.Variance,
    )
    for agg in tree.find_all(*agg_classes):
        inner = agg.this
        distinct = False
        if isinstance(inner, exp.Distinct):
            distinct = True
            exprs = inner.expressions or [inner.this]
            inner = exprs[0] if exprs else None
        column = inner.name if isinstance(inner, exp.Column) else None
        # Outer expression (e.g. ROUND(SUM(x)/1e9, 2)) — walk up to find the
        # nearest non-aggregation ancestor.
        outer = agg.parent
        outer_sql = (
            outer.sql(dialect=BQ_DIALECT) if outer is not None else agg.sql(dialect=BQ_DIALECT)
        )
        # Find the alias if any.
        alias = None
        cur = agg
        while cur is not None:
            if isinstance(cur, exp.Alias):
                alias = cur.alias
                break
            cur = cur.parent
        out.append({
            "function": agg.__class__.__name__.upper(),
            "column": column,
            "alias": alias,
            "distinct": distinct,
            "outer_expr": outer_sql,
        })
    return out


def _extract_case_whens(tree: exp.Expression) -> list[dict[str, Any]]:
    """For each CASE WHEN, find the source column + the WHEN→THEN mapping +
    the alias.
    """
    out: list[dict[str, Any]] = []
    for case in tree.find_all(exp.Case):
        # Walk up to find the alias.
        alias = None
        cur = case
        while cur is not None:
            if isinstance(cur, exp.Alias):
                alias = cur.alias
                break
            cur = cur.parent
        # Source column: any Column ref appearing in the conditions.
        source_columns = [
            c.name for c in case.find_all(exp.Column) if c.name
        ]
        source_column = source_columns[0] if source_columns else None
        # Mapped values: walk WHEN → THEN.
        mapped_values: list[dict[str, str]] = []
        for if_clause in case.args.get("ifs") or []:
            cond = if_clause.this
            then = if_clause.args.get("true")
            mapped_values.append({
                "when": cond.sql(dialect=BQ_DIALECT) if cond else "",
                "then": then.sql(dialect=BQ_DIALECT).strip("'\"") if then else "",
            })
        out.append({
            "alias": alias,
            "source_column": source_column,
            "sql": case.sql(dialect=BQ_DIALECT),
            "mapped_values": mapped_values,
        })
    return out


def _extract_ctes(tree: exp.Expression) -> list[dict[str, Any]]:
    """Each WITH ... AS (...) clause. Captures alias, structural filters
    (everything in its WHERE), the real tables it reads from, and any
    upstream CTE aliases it depends on (chained CTEs).

    Chained CTEs are valid SQL — `WITH a AS (...), b AS (SELECT * FROM a)`.
    `b`'s "source" is `a`, not a real BQ table. We track these as
    `cte_dependencies` so downstream guards can distinguish real-table
    references from CTE-internal references.
    """
    out: list[dict[str, Any]] = []
    with_node = tree.find(exp.With)
    if with_node is None:
        return out
    cte_alias_set = {c.alias for c in (with_node.expressions or [])}

    for cte in with_node.expressions or []:
        alias = cte.alias
        body = cte.this  # the SELECT inside
        cte_filters = (
            _flatten_predicates(body.find(exp.Where).this)
            if body.find(exp.Where)
            else []
        )
        for f in cte_filters:
            f["is_structural"] = True

        # Split FROM-table references into real tables vs upstream CTEs.
        source_tables: list[str] = []
        cte_dependencies: list[str] = []
        for t in body.find_all(exp.Table):
            if not t.name:
                continue
            if t.name in cte_alias_set:
                if t.name not in cte_dependencies:
                    cte_dependencies.append(t.name)
            else:
                if t.name not in source_tables:
                    source_tables.append(t.name)

        out.append({
            "alias": alias,
            "structural_filters": cte_filters,
            "sql": body.sql(dialect=BQ_DIALECT),
            "source_tables": source_tables,
            "cte_dependencies": cte_dependencies,
        })
    return out


def _extract_joins(tree: exp.Expression) -> list[dict[str, Any]]:
    """Each JOIN with order preserved + ON condition split into left/right keys.
    Joins inside CTEs are not extracted here (they're inside the CTE's sql).
    """
    # Only top-level joins (the main SELECT, not nested in CTEs).
    top_select = tree.find(exp.Select)
    if top_select is None:
        return []
    out: list[dict[str, Any]] = []
    for order, join in enumerate(top_select.args.get("joins") or [], start=1):
        right_tbl = join.this.name if isinstance(join.this, exp.Table) else None
        right_alias = (
            join.this.alias_or_name if isinstance(join.this, exp.Table) else None
        )
        on = join.args.get("on")
        side = (join.side or "").lower()
        kind_raw = (join.kind or "").lower()
        kind = side or kind_raw or "inner"
        left_key, right_key, left_table = None, None, None
        if isinstance(on, exp.EQ):
            if isinstance(on.left, exp.Column):
                left_table = on.left.table or None
                left_key = on.left.name
            if isinstance(on.right, exp.Column):
                right_key = on.right.name
        out.append({
            "right_table": right_tbl,
            "other_table": right_tbl,  # alias for guardrails compatibility
            "right_alias": right_alias,
            "left_table": left_table,
            "left_key": left_key,
            "right_key": right_key,
            "join_type": kind,
            "order": order,
        })
    return out


def _extract_group_by(tree: exp.Expression) -> list[dict[str, Any]]:
    """Top-level GROUP BY columns. Each item: {table, column}.

    Critical for join cardinality inference: the side whose columns
    appear in GROUP BY is the dimension (one row per group); the side
    with aggregations is the fact. Combined with join_type and
    aggregation source, this yields (table_a, key_a) → (table_b, key_b)
    cardinality with high confidence.
    """
    top_select = tree.find(exp.Select)
    if top_select is None:
        return []
    group = top_select.args.get("group")
    if group is None:
        return []
    # Cache top-SELECT expressions for resolving GROUP BY 1, 2 positional refs.
    select_exprs = list(top_select.expressions or [])
    out: list[dict[str, Any]] = []
    for expr in group.expressions or []:
        if isinstance(expr, exp.Column):
            out.append({
                "table": expr.table or None,
                "column": expr.name,
                "via_position": None,
                "expression": expr.sql(dialect=BQ_DIALECT),
            })
            continue
        # Positional ref: GROUP BY 1, 2 — resolve into the matching SELECT expr.
        if isinstance(expr, exp.Literal) and expr.is_int:
            try:
                pos = int(expr.this)
            except Exception:
                pos = 0
            if 1 <= pos <= len(select_exprs):
                target = select_exprs[pos - 1]
                # Unwrap alias to get the actual expression
                inner = target.this if isinstance(target, exp.Alias) else target
                inner_cols = list(inner.find_all(exp.Column))
                if inner_cols:
                    for col in inner_cols:
                        out.append({
                            "table": col.table or None,
                            "column": col.name,
                            "via_position": pos,
                            "expression": inner.sql(dialect=BQ_DIALECT),
                        })
                else:
                    # Pure literal in SELECT — preserve the positional ref.
                    out.append({
                        "table": None,
                        "column": None,
                        "via_position": pos,
                        "expression": inner.sql(dialect=BQ_DIALECT),
                    })
            continue
        # Expression-style GROUP BY (DATE_TRUNC(x, MONTH), CASE WHEN ...).
        for col in expr.find_all(exp.Column):
            out.append({
                "table": col.table or None,
                "column": col.name,
                "via_position": None,
                "expression": expr.sql(dialect=BQ_DIALECT),
            })
    return out


def _extract_filters(tree: exp.Expression) -> list[dict[str, Any]]:
    """Top-level WHERE predicates only (CTE-internal filters live on the CTE)."""
    top_select = tree.find(exp.Select)
    if top_select is None:
        return []
    where = top_select.args.get("where")
    if where is None:
        return []
    return _flatten_predicates(where.this)


def _flatten_predicates(node: exp.Expression | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if node is None:
        return out
    if isinstance(node, exp.And):
        out.extend(_flatten_predicates(node.left))
        out.extend(_flatten_predicates(node.right))
        return out
    if isinstance(node, exp.Or):
        out.extend(_flatten_predicates(node.left))
        out.extend(_flatten_predicates(node.right))
        return out
    if isinstance(node, exp.Between):
        col = _col_name(node.this)
        if col:
            low = node.args.get("low")
            high = node.args.get("high")
            value = (
                f"{low.sql(dialect=BQ_DIALECT) if low else '?'} AND "
                f"{high.sql(dialect=BQ_DIALECT) if high else '?'}"
            )
            out.append({"column": col, "operator": "BETWEEN", "value": value, "is_structural": False})
        return out
    if isinstance(node, exp.In):
        col = _col_name(node.this)
        if col:
            vals = ", ".join(
                e.sql(dialect=BQ_DIALECT) for e in (node.expressions or [])
            )
            out.append({"column": col, "operator": "IN", "value": f"({vals})", "is_structural": False})
        return out
    if isinstance(node, exp.Is):
        col = _col_name(node.this)
        if col:
            value = node.expression.sql(dialect=BQ_DIALECT) if node.expression else "NULL"
            out.append({"column": col, "operator": "IS", "value": value, "is_structural": False})
        return out
    if isinstance(node, exp.Not):
        # NOT (X IS NULL) → IS NOT NULL — render and recurse on inner
        return _flatten_predicates(node.this)
    for cls, op in _BINARY_OPS.items():
        if isinstance(node, cls):
            col = _col_name(node.left)
            if col:
                out.append({
                    "column": col,
                    "operator": op,
                    "value": node.right.sql(dialect=BQ_DIALECT),
                    "is_structural": False,
                })
            return out
    return out


def _col_name(node: exp.Expression | None) -> str | None:
    if isinstance(node, exp.Column):
        return node.name
    # TRIM(col), LOWER(col), DATE(col), EXTRACT(... FROM col) — peel one layer.
    if isinstance(node, exp.Func):
        for arg in node.args.values():
            if isinstance(arg, exp.Column):
                return arg.name
            if isinstance(arg, list):
                for a in arg:
                    if isinstance(a, exp.Column):
                        return a.name
    return None


def _extract_select_aliases(tree: exp.Expression) -> list[dict[str, Any]]:
    """Capture every ``SELECT col AS alias`` in the query.

    These are the analysts' own glossary — when a query writes
    ``SUM(billed_business) AS total_revenue``, the author is telling us
    "billed_business IS revenue in this domain." For columns with cryptic
    names (cm11, pmdl_*) this is often the most concrete domain signal.

    Captures EVERY alias unfiltered for traceability; the quality filter
    runs later when we aggregate aliases into the narrative section so
    Gemini only sees meaningful ones.

    Returns:
        list of {"column": str | None, "alias": str, "expression": str}
        — column is the source column when the expression is a simple
        Column or a 1-arg function; None for complex expressions.
    """
    out: list[dict[str, Any]] = []
    # Only the OUTER SELECT — alias names inside CTEs are CTE-internal
    # aliases that don't apply to the final output. We get them via
    # ctes[i].sql when that matters.
    top_select = tree.find(exp.Select) if hasattr(tree, "find") else None
    if top_select is None:
        return out
    for proj in top_select.expressions or []:
        if not isinstance(proj, exp.Alias):
            continue
        alias_name = proj.alias
        inner = proj.this
        # Try to identify the source column. Walk through one or two
        # wrapper layers to handle COUNT(DISTINCT col), SUM(col),
        # TRIM(col), DATE(col), etc.
        column = _peel_to_column(inner)
        out.append({
            "column": column,
            "alias": alias_name,
            "expression": inner.sql(dialect=BQ_DIALECT) if inner else "",
        })
    return out


def _peel_to_column(node: exp.Expression | None) -> str | None:
    """Walk through Func / Distinct wrappers to find the underlying column.

    Handles:
      - col                    → col
      - SUM(col)               → col
      - COUNT(DISTINCT col)    → col  (Distinct wraps Column)
      - DATE(col), TRIM(col)   → col
      - SUM(amt) / 1e9         → amt  (the Column inside the Div)
    Returns None for purely literal expressions (1, 'x', etc.).
    """
    if node is None:
        return None
    if isinstance(node, exp.Column):
        return node.name
    if isinstance(node, exp.Distinct):
        # Distinct.expressions is a list when DISTINCT col1, col2 is used;
        # for COUNT(DISTINCT col) it's a single Column inside that list.
        for ex in node.expressions or []:
            col = _peel_to_column(ex)
            if col:
                return col
        # Older sqlglot put it on .this
        return _peel_to_column(node.this)
    # Walk into any sub-expression that has children.
    for arg in node.args.values():
        if isinstance(arg, exp.Expression):
            col = _peel_to_column(arg)
            if col:
                return col
        elif isinstance(arg, list):
            for sub in arg:
                if isinstance(sub, exp.Expression):
                    col = _peel_to_column(sub)
                    if col:
                        return col
    return None


def _extract_date_functions(tree: exp.Expression) -> list[dict[str, Any]]:
    """EXTRACT(YEAR FROM rpt_dt), DATE_TRUNC(rpt_dt, MONTH), DATE(...)."""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str]] = set()

    # EXTRACT
    for ex in tree.find_all(exp.Extract):
        part = ex.this.name if hasattr(ex.this, "name") else str(ex.this)
        col_node = ex.expression
        col = _col_name(col_node)
        key = (col, part.upper())
        if key not in seen:
            seen.add(key)
            out.append({"column": col, "function": part.upper()})

    # DATE_TRUNC
    for dt in tree.find_all(exp.DateTrunc):
        unit_node = dt.args.get("unit")
        unit = unit_node.name if (unit_node and hasattr(unit_node, "name")) else (
            unit_node.sql(dialect=BQ_DIALECT) if unit_node else ""
        )
        col_node = dt.this
        col = _col_name(col_node)
        key = (col, f"DATE_TRUNC_{unit.upper()}")
        if key not in seen:
            seen.add(key)
            out.append({"column": col, "function": f"DATE_TRUNC_{unit.upper()}"})

    # DATE() casts
    for func in tree.find_all(exp.Anonymous):
        if (func.name or "").upper() == "DATE":
            for arg in func.args.get("expressions", []) or []:
                col = _col_name(arg)
                if col:
                    key = (col, "DATE_CAST")
                    if key not in seen:
                        seen.add(key)
                        out.append({"column": col, "function": "DATE_CAST"})

    return out


# ─── Stage 2: discover ──────────────────────────────────────


def discover_tables(
    fingerprints: list[SQLFingerprint],
    mdm_client: MDMClientProto,
    baseline_views_dir: str,
) -> dict[str, TableContext]:
    """Group fingerprints by table, fetch MDM + baseline view per table.

    For each unique table referenced across all SQLs:
      - aggregate columns_referenced, aggregations, case_whens, joins, filters
      - track which queries (by index) touch it
      - capture filters from CTEs scoped to this table as is_structural=True
      - fetch MDM metadata via mdm_client.fetch()
      - load baseline_views_dir/<table>.view.lkml if present
    """
    baseline_dir = Path(baseline_views_dir)

    # Build query identifiers (Qnn) keyed by fingerprint index for traceability.
    contexts: dict[str, dict[str, Any]] = {}

    for q_index, fp in enumerate(fingerprints, start=1):
        qid = f"Q{q_index:02d}"
        if fp.parse_error:
            logger.warning("%s: parse error — %s", qid, fp.parse_error)
            continue

        for table in fp.tables:
            ctx = contexts.setdefault(table, _empty_context(table))
            _accumulate_into_context(ctx, fp, qid, this_table=table)

        # CTE source tables also get TableContexts — they're real tables that
        # need enrichment (the CTE just adds structural filters on top).
        for cte in fp.ctes:
            for src in cte.get("source_tables") or []:
                ctx = contexts.setdefault(src, _empty_context(src))
                # Mark the CTE on the source table.
                if cte not in ctx["ctes_referencing_this"]:
                    ctx["ctes_referencing_this"].append(cte)
                # Bring CTE-internal filters across as structural filters on this table.
                for sf in cte.get("structural_filters") or []:
                    if sf not in ctx["filters_on_this"]:
                        ctx["filters_on_this"].append(sf)
                if qid not in ctx["queries_using_this"]:
                    ctx["queries_using_this"].append(qid)

        # CREATE [TEMP] TABLE bodies — semantically same as CTEs. Attribute
        # back through the same pipeline so source tables pick up the
        # structural filters and the named intermediate is recorded.
        for tt in fp.temp_tables:
            for src in tt.get("source_tables") or []:
                ctx = contexts.setdefault(src, _empty_context(src))
                if tt not in ctx["temp_tables_referencing_this"]:
                    ctx["temp_tables_referencing_this"].append(tt)
                for sf in tt.get("structural_filters") or []:
                    if sf not in ctx["filters_on_this"]:
                        ctx["filters_on_this"].append(sf)
                if qid not in ctx["queries_using_this"]:
                    ctx["queries_using_this"].append(qid)

    # Now hydrate with MDM + baseline.
    for table_name, raw_ctx in contexts.items():
        mdm = mdm_client.fetch(table_name)
        raw_ctx["mdm_columns"] = mdm.get("columns") or []
        raw_ctx["mdm_table_description"] = mdm.get("table_description")
        raw_ctx["mdm_coverage_pct"] = float(mdm.get("mdm_coverage_pct") or 0.0)
        # Table-level + ownership: every dataset_details / source / decommission
        # field MDM exposes, plus *_extra catch-alls for forward-compat.
        raw_ctx["mdm_dataset_details"] = _build_mdm_dataset_details(mdm)
        raw_ctx["mdm_ownership"] = mdm.get("ownership") or {}

        baseline_text = _find_baseline_view(baseline_dir, table_name)
        if baseline_text is not None:
            raw_ctx["existing_view_lkml"] = baseline_text
            # Parse once at discover time so the planner + enricher see
            # structured baseline content instead of having to re-parse it
            # themselves. Auto-generated Looker baselines have terse or
            # missing descriptions; the quality_signals tell the planner
            # exactly which fields need attention.
            parsed = _parse_baseline_view(baseline_text, raw_ctx["date_functions"])
            raw_ctx["baseline_dimensions"] = parsed["dimensions"]
            raw_ctx["baseline_dimension_groups"] = parsed["dimension_groups"]
            raw_ctx["baseline_measures"] = parsed["measures"]
            raw_ctx["baseline_quality_signals"] = parsed["quality_signals"]
            # View-level + structural — preserves every piece of human
            # curation we can find in the existing baseline.
            raw_ctx["baseline_view_description"] = parsed["view_description"]
            raw_ctx["baseline_view_label"] = parsed["view_label"]
            raw_ctx["baseline_sql_table_name"] = parsed["sql_table_name"]
            raw_ctx["baseline_derived_table_sql"] = parsed["derived_table_sql"]
            raw_ctx["baseline_primary_key_column"] = parsed["primary_key_column"]
            raw_ctx["baseline_extends_chain"] = parsed["extends_chain"]
            raw_ctx["baseline_sets"] = parsed["sets"]
            raw_ctx["baseline_parameters"] = parsed["parameters"]
            raw_ctx["baseline_access_filter"] = parsed["access_filter"]
            raw_ctx["baseline_drill_fields_curated"] = parsed["drill_fields_curated"]
            raw_ctx["baseline_filtered_measures"] = parsed["filtered_measures"]
            raw_ctx["baseline_sql_aliases"] = parsed["sql_aliases"]

    return {name: TableContext(**raw) for name, raw in contexts.items()}


# ─── Baseline LookML parser ─────────────────────────────────


# Below this length we treat a description as auto-generated boilerplate.
# 30 chars roughly = "Customer ID" plus a couple words. Anything longer is
# almost always human-edited and worth preserving.
_DESCRIPTION_QUALITY_THRESHOLD = 30


def _parse_baseline_view(
    lkml_text: str,
    date_functions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Parse a baseline .view.lkml file and surface every structured
    signal we can use for grounding the enrichment.

    Beyond just listing dims/measures/dim_groups, this returns:
      view_description     view-level description: (rare but gold)
      view_label           view-level label
      sql_table_name       authoritative BQ FQN if baseline declares one
      derived_table_sql    if the baseline IS a derived_table, its SQL
      primary_key_column   actual NAME of the PK dim (not just bool)
      extends_chain        list of view names this baseline extends
      sets                 named field bundles (set: { ... })
      parameters           user-facing parameters: blocks
      access_filter        security model — must NEVER be touched by enrichment
      drill_fields_curated baseline's curated drill_fields list
      filtered_measures    measures with pre-filtered filters: blocks
                           (canonical slicing patterns Gemini should follow)
      sql_aliases          {dim_name: source_column} — name renames; goldmine
                           for synonym preservation
      quality_signals      counts of gaps for the planner

    All extraction is best-effort: malformed LookML, missing views, or
    unexpected structures collapse to empty defaults so callers can
    rely on field presence.
    """
    empty_result = {
        "dimensions": [],
        "dimension_groups": [],
        "measures": [],
        "view_description": None,
        "view_label": None,
        "sql_table_name": None,
        "derived_table_sql": None,
        "primary_key_column": None,
        "extends_chain": [],
        "sets": [],
        "parameters": [],
        "access_filter": [],
        "drill_fields_curated": [],
        "filtered_measures": [],
        "sql_aliases": {},
        "quality_signals": {},
    }

    try:
        import lkml  # local import to keep module-import cost down
        tree = lkml.load(lkml_text)
    except Exception as e:  # noqa: BLE001
        logger.warning("baseline parse failed; skipping structured fields: %s", e)
        return empty_result

    views = tree.get("views") or []
    if not views:
        return empty_result

    # First view in the file is canonical (Looker-generated baselines have one).
    view = views[0]
    dims = list(view.get("dimensions") or [])
    dgs = list(view.get("dimension_groups") or [])
    msrs = list(view.get("measures") or [])

    # ── View-level grounding ──
    view_description = (view.get("description") or "").strip() or None
    view_label = (view.get("label") or "").strip() or None
    sql_table_name = (view.get("sql_table_name") or "").strip() or None
    derived_table = view.get("derived_table") or {}
    derived_table_sql = (derived_table.get("sql") or "").strip() or None
    # lkml normalizes top-level repeated keys with __all / plural suffixes:
    #   extends:        → "extends__all"
    #   access_filter:  → "access_filters"  (singular block, plural key)
    #   filters: (in measures) → "filters__all"
    # Check both forms so we match whatever lkml gave us.
    extends_chain = (
        view.get("extends__all")
        or view.get("extends")
        or []
    )
    extends_chain = list(extends_chain)
    if extends_chain and isinstance(extends_chain[0], list):
        extends_chain = [item for sub in extends_chain for item in sub]

    # ── Sets, parameters, access_filter (preserve verbatim) ──
    sets = list(view.get("sets") or view.get("set") or [])
    parameters = list(
        view.get("parameters") or view.get("parameter") or []
    )
    access_filter = list(
        view.get("access_filters")
        or view.get("access_filter")
        or []
    )

    # ── Drill fields curated by humans ──
    drill_fields_curated = list(
        view.get("drill_fields")
        or view.get("drill_fields__all")
        or []
    )
    if drill_fields_curated and isinstance(drill_fields_curated[0], list):
        drill_fields_curated = [item for sub in drill_fields_curated for item in sub]

    # ── Pre-filtered measures (canonical slicing patterns) ──
    # lkml stores measure-level `filters: [...]` under `filters__all`.
    filtered_measures: list[dict[str, Any]] = []
    for m in msrs:
        msr_filters = m.get("filters") or m.get("filters__all")
        if msr_filters:
            filtered_measures.append({
                "name": m.get("name"),
                "type": m.get("type"),
                "sql": m.get("sql"),
                "filters": msr_filters,
                "description": m.get("description"),
            })

    # ── Primary-key NAME (not just bool) ──
    primary_key_column: str | None = None
    for d in dims:
        if str(d.get("primary_key") or "").lower() in {"yes", "true"}:
            primary_key_column = d.get("name")
            break

    # ── SQL aliases — when dim NAME differs from the column it sources ──
    # Tells us human-curated synonyms: dimension `customer_segment` sourced
    # from `${TABLE}.bus_seg` means "customer_segment" IS a synonym for bus_seg.
    sql_aliases: dict[str, str] = {}
    for d in dims:
        dim_name = (d.get("name") or "").strip()
        sql = (d.get("sql") or "").strip()
        if not dim_name or not sql:
            continue
        m_match = re.search(r"\$\{TABLE\}\.([\w_]+)", sql, re.IGNORECASE)
        if m_match:
            source_col = m_match.group(1)
            if source_col.lower() != dim_name.lower():
                sql_aliases[dim_name] = source_col

    # ── Quality signals (gap counts for the planner) ──
    dims_missing_desc = sum(1 for d in dims if not (d.get("description") or "").strip())
    dims_short_desc = sum(
        1 for d in dims
        if 0 < len((d.get("description") or "").strip()) < _DESCRIPTION_QUALITY_THRESHOLD
    )
    dims_missing_label = sum(1 for d in dims if not (d.get("label") or "").strip())
    dims_missing_tags = sum(1 for d in dims if not d.get("tags"))
    msrs_missing_vf = sum(
        1 for m in msrs
        if not (m.get("value_format_name") or m.get("value_format"))
    )
    msrs_missing_desc = sum(
        1 for m in msrs if not (m.get("description") or "").strip()
    )
    has_pk = primary_key_column is not None
    hidden_count = sum(
        1 for d in dims if str(d.get("hidden") or "").lower() in {"yes", "true"}
    )

    # Date columns from sqlglot fingerprint that don't appear as dim_groups
    # are "still plain dims" — a SKILL.md violation we want enrichment to fix.
    dg_source_cols: set[str] = set()
    for dg in dgs:
        sql = (dg.get("sql") or "").lower()
        if sql:
            # ${TABLE}.col_name → col_name
            for tok in sql.replace("${TABLE}.", "").replace("${table}.", "").split():
                tok = tok.strip(";`,()").lower()
                if tok and tok.isidentifier():
                    dg_source_cols.add(tok)
        if dg.get("name"):
            dg_source_cols.add(dg["name"].lower())
    date_cols_from_fp = {
        (df.get("column") or "").lower() for df in (date_functions or []) if df.get("column")
    }
    dates_as_plain = len(date_cols_from_fp - dg_source_cols)

    return {
        "dimensions": dims,
        "dimension_groups": dgs,
        "measures": msrs,
        "view_description": view_description,
        "view_label": view_label,
        "sql_table_name": sql_table_name,
        "derived_table_sql": derived_table_sql,
        "primary_key_column": primary_key_column,
        "extends_chain": extends_chain,
        "sets": sets,
        "parameters": parameters,
        "access_filter": access_filter,
        "drill_fields_curated": drill_fields_curated,
        "filtered_measures": filtered_measures,
        "sql_aliases": sql_aliases,
        "quality_signals": {
            "dims_total": len(dims),
            "dims_missing_description": dims_missing_desc,
            "dims_short_description": dims_short_desc,
            "dims_missing_label": dims_missing_label,
            "dims_missing_tags": dims_missing_tags,
            "dims_hidden_count": hidden_count,
            "measures_total": len(msrs),
            "measures_missing_value_format": msrs_missing_vf,
            "measures_missing_description": msrs_missing_desc,
            "dates_as_plain_dim": dates_as_plain,
            "has_primary_key": has_pk,
        },
    }


def _find_baseline_view(baseline_dir: Path, table_name: str) -> str | None:
    """Find a baseline LookML view file for ``table_name``.

    Looker repos use a few different naming conventions in the wild:
      - ``<table>.view.lkml``                (canonical Looker default)
      - ``bq_<table>.view.lkml``             (some teams prefix by source)
      - ``<dataset>_<table>.view.lkml``      (e.g. ``dw_cornerstone_metrics``)
      - ``<table>.view``                     (rare: omitted .lkml)
      - inside subdirs grouped by dataset (``views/dw/<table>.view.lkml``)

    We try them all in order of specificity. First hit wins.

    Returns the file's text content or None if not found.
    """
    if not baseline_dir.exists():
        return None

    # Build candidate filename patterns. Most-specific first so we don't
    # accidentally match a generic prefix when the canonical file exists.
    candidates: list[str] = [
        f"{table_name}.view.lkml",
        f"{table_name}.view",                  # extension variant
    ]
    # Prefix variants: only check these if the bare name didn't match.
    # Common prefixes seen in real Looker repos at AmEx-style data warehouses.
    prefix_variants = ("bq_", "dw_", "edw_", "fact_", "dim_")

    # 1. Quick path: file at root.
    for cand in candidates:
        direct = baseline_dir / cand
        if direct.is_file():
            return direct.read_text(encoding="utf-8")

    # 2. Recursive search for the canonical name; first hit wins.
    for cand in candidates:
        for path in baseline_dir.rglob(cand):
            return path.read_text(encoding="utf-8")

    # 3. Fallback: try common prefixes (only after canonical search misses,
    # so we don't shadow a real <table>.view.lkml elsewhere in the tree).
    for prefix in prefix_variants:
        prefixed_name = f"{prefix}{table_name}.view.lkml"
        direct = baseline_dir / prefixed_name
        if direct.is_file():
            logger.info(
                "matched baseline for %s via prefix variant %s",
                table_name, prefixed_name,
            )
            return direct.read_text(encoding="utf-8")
        for path in baseline_dir.rglob(prefixed_name):
            logger.info(
                "matched baseline for %s via prefix variant %s",
                table_name, prefixed_name,
            )
            return path.read_text(encoding="utf-8")

    # 4. Last-resort fuzzy: scan every .view.lkml under the dir and check
    # whether its declared `view: <name>` matches our table_name. Catches the
    # case where the FILENAME doesn't match but the VIEW NAME inside does
    # (which is what Looker actually resolves explores against). Capped at
    # 500 files to bound cost on huge repos.
    return _fuzzy_match_by_view_name(baseline_dir, table_name)


def _fuzzy_match_by_view_name(
    baseline_dir: Path, table_name: str, *, file_cap: int = 500
) -> str | None:
    """Scan .view.lkml files for a `view: <table_name>` declaration.

    Useful when the filename convention doesn't match our table key but the
    view name inside does. We don't fully parse the LKML here — just scan
    the first ~80 chars of each file for `view: <name> {`.
    """
    needle = f"view: {table_name} ".encode()
    needle_brace = f"view: {table_name}{{".encode()
    count = 0
    for path in baseline_dir.rglob("*.view.lkml"):
        count += 1
        if count > file_cap:
            return None
        try:
            with path.open("rb") as f:
                head = f.read(256)
        except OSError:
            continue
        if needle in head or needle_brace in head:
            logger.info(
                "matched baseline for %s via view-name scan in %s",
                table_name, path.name,
            )
            return path.read_text(encoding="utf-8")
    return None


def _empty_context(table_name: str) -> dict[str, Any]:
    return {
        "table_name": table_name,
        "columns_referenced": [],
        "aggregations": [],
        "case_whens": [],
        "ctes_referencing_this": [],
        "temp_tables_referencing_this": [],
        "joins_involving_this": [],
        "filters_on_this": [],
        "date_functions": [],
        "mdm_columns": [],
        "mdm_table_description": None,
        "mdm_coverage_pct": 0.0,
        "existing_view_lkml": None,
        "queries_using_this": [],
    }


def _accumulate_into_context(
    ctx: dict[str, Any],
    fp: SQLFingerprint,
    qid: str,
    this_table: str,
) -> None:
    """Merge fp's data into ctx for the given table."""
    if qid not in ctx["queries_using_this"]:
        ctx["queries_using_this"].append(qid)

    for agg in fp.aggregations:
        if agg not in ctx["aggregations"]:
            ctx["aggregations"].append(agg)
            if agg.get("column") and agg["column"] not in ctx["columns_referenced"]:
                ctx["columns_referenced"].append(agg["column"])

    for cw in fp.case_whens:
        if cw not in ctx["case_whens"]:
            ctx["case_whens"].append(cw)

    for f in fp.filters:
        if f not in ctx["filters_on_this"]:
            ctx["filters_on_this"].append(f)
            if f.get("column") and f["column"] not in ctx["columns_referenced"]:
                ctx["columns_referenced"].append(f["column"])

    for d in fp.date_functions:
        if d not in ctx["date_functions"]:
            ctx["date_functions"].append(d)

    # Joins: include if this_table is the FROM (left) side OR involves this table.
    for j in fp.joins:
        if j not in ctx["joins_involving_this"]:
            ctx["joins_involving_this"].append(j)


# ─── MDM dataset-level synthesis ────────────────────────────


_DATASET_LEVEL_KEYS = (
    "data_category", "data_sub_category", "data_type", "table_type",
    "feed_type", "is_internal", "is_searchable", "is_sor_certified",
    "is_transactional", "is_history_required", "retention_period",
    "selective_update_required", "enable_sequence_check", "dataset_id",
    "dataset_parent_id", "key_id", "host_region", "status", "version",
    "storage_type", "load_type", "country", "region", "feed_id",
    "base_or_view", "is_decommissioned",
)


def _build_mdm_dataset_details(mdm: dict[str, Any]) -> dict[str, Any]:
    """Collapse every MDM table-level field plus the *_extra catch-alls
    into a single dict the planner / enricher can read uniformly.

    Includes the explicit keys we promoted in :func:`lumi.mdm._digest`
    plus mdm_dataset_extra / mdm_source_extra / mdm_decommission_extra
    so undocumented future MDM keys still flow through.
    """
    out: dict[str, Any] = {}
    for k in _DATASET_LEVEL_KEYS:
        if k in mdm and mdm[k] is not None:
            out[k] = mdm[k]
    for extra_key in ("mdm_dataset_extra", "mdm_source_extra",
                      "mdm_decommission_extra"):
        extras = mdm.get(extra_key) or {}
        if extras:
            out[extra_key] = extras
    return out


# ─── One-call wrapper ───────────────────────────────────────


def prepare_enrichment_context(
    sqls: list[str],
    mdm_client: MDMClientProto,
    baseline_views_dir: str,
) -> dict[str, TableContext]:
    """Wrapper used by tests + the pipeline. Stage 1 then Stage 2."""
    fps = parse_sqls(sqls)
    return discover_tables(fps, mdm_client, baseline_views_dir)


# ─── Layer 3 + 4 extractors (Phase 0 — semantic graph substrate) ──


def _extract_order_by(tree: exp.Expression) -> list[dict[str, Any]]:
    """ORDER BY columns from the top SELECT."""
    top = tree.find(exp.Select)
    if top is None:
        return []
    order_node = top.args.get("order")
    if order_node is None:
        return []
    out: list[dict[str, Any]] = []
    for expr in order_node.expressions or []:
        # exp.Ordered carries .this (the column/expression) + .args["desc"]
        target = getattr(expr, "this", expr)
        direction = "DESC" if getattr(expr, "args", {}).get("desc") else "ASC"
        nulls = None
        if getattr(expr, "args", {}).get("nulls_first"):
            nulls = "FIRST"
        elif getattr(expr, "args", {}).get("nulls_last"):
            nulls = "LAST"
        col_name = _col_name(target) or target.sql(dialect=BQ_DIALECT)
        is_position = isinstance(target, exp.Literal) and target.is_int
        out.append({
            "column": col_name,
            "alias": col_name if not is_position else None,
            "direction": direction,
            "nulls": nulls,
            "is_position_ref": is_position,
        })
    return out


def _extract_having(tree: exp.Expression) -> list[dict[str, Any]]:
    """HAVING clauses with threshold semantics (e.g., SUM(x) > 1000)."""
    top = tree.find(exp.Select)
    if top is None:
        return []
    having = top.args.get("having")
    if having is None:
        return []
    out: list[dict[str, Any]] = []
    # Walk binary-op nodes inside HAVING directly — HAVING typically
    # has function-wrapped LHS (SUM(x), COUNT(*)) so we don't go through
    # the WHERE-style _flatten_predicates which strips function wrappers.
    inner = having.this if hasattr(having, "this") else having

    def walk(node: exp.Expression) -> None:
        if isinstance(node, exp.And | exp.Or):
            walk(node.left)
            walk(node.right)
            return
        if not isinstance(node, tuple(_BINARY_OPS.keys())):
            return
        op_sym = _BINARY_OPS[type(node)]
        lhs = node.this
        rhs = node.expression
        lhs_sql = lhs.sql(dialect=BQ_DIALECT) if lhs is not None else ""
        rhs_sql = rhs.sql(dialect=BQ_DIALECT) if rhs is not None else ""
        agg_func = None
        source_col = None
        # Detect aggregation on the LHS by AST type first, then by regex fallback.
        agg_node = None
        for agg_cls in (
            exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max,
            exp.Stddev, exp.Variance,
        ):
            if isinstance(lhs, agg_cls):
                agg_node = lhs
                break
        if agg_node is not None:
            agg_func = type(agg_node).__name__.upper()
            for c in agg_node.find_all(exp.Column):
                source_col = (c.name or "").lower()
                break
        else:
            m = re.match(
                r"(SUM|COUNT|AVG|MIN|MAX|MEDIAN|STDDEV|VARIANCE)\s*\(\s*(?:DISTINCT\s+)?([^\)]+)\s*\)",
                lhs_sql.upper(),
            )
            if m:
                agg_func = m.group(1)
                source_col = m.group(2).strip().lower()
        semantic_class = None
        if agg_func and op_sym in {">", ">=", "<", "<="}:
            semantic_class = "threshold"
        elif agg_func and op_sym in {"=", "!="}:
            semantic_class = "count_filter"
        out.append({
            "expression": f"{lhs_sql} {op_sym} {rhs_sql}".strip(),
            "aggregation": agg_func,
            "source_column": source_col,
            "operator": op_sym,
            "value": rhs_sql,
            "semantic_class": semantic_class,
        })

    walk(inner)
    return out


def _extract_limit(tree: exp.Expression) -> dict[str, Any]:
    """LIMIT [offset, ]value. is_top_n filled by caller after order_by known."""
    top = tree.find(exp.Select)
    if top is None:
        return {}
    limit_node = top.args.get("limit")
    offset_node = top.args.get("offset")
    if limit_node is None and offset_node is None:
        return {}
    out: dict[str, Any] = {"value": None, "offset": None, "is_top_n": False}
    if limit_node is not None:
        try:
            out["value"] = int(limit_node.expression.sql(dialect=BQ_DIALECT))
        except Exception:
            out["value"] = None
    if offset_node is not None:
        try:
            out["offset"] = int(offset_node.expression.sql(dialect=BQ_DIALECT))
        except Exception:
            pass
    return out


def _extract_distinct_select(tree: exp.Expression) -> bool:
    """SELECT DISTINCT at the top level."""
    top = tree.find(exp.Select)
    if top is None:
        return False
    distinct_node = top.args.get("distinct")
    return distinct_node is not None


def _extract_window_functions(tree: exp.Expression) -> list[dict[str, Any]]:
    """ROW_NUMBER / RANK / LAG / LEAD / NTILE / etc. with PARTITION BY + ORDER BY."""
    out: list[dict[str, Any]] = []
    for win in tree.find_all(exp.Window):
        fn_node = win.this
        fn_name = type(fn_node).__name__.upper() if fn_node else "WINDOW"
        # Some window functions are anonymous funcs — try to get name
        if hasattr(fn_node, "sql_name"):
            try:
                fn_name = fn_node.sql_name().upper()
            except Exception:
                pass
        elif hasattr(fn_node, "name"):
            fn_name = (fn_node.name or fn_name).upper()
        partition_by = []
        for p in win.args.get("partition_by") or []:
            n = _col_name(p)
            if n:
                partition_by.append(n)
        order_by = []
        for o in win.args.get("order") or []:
            if hasattr(o, "expressions"):
                for ex in o.expressions:
                    target = getattr(ex, "this", ex)
                    direction = "DESC" if getattr(ex, "args", {}).get("desc") else "ASC"
                    order_by.append({
                        "column": _col_name(target) or target.sql(dialect=BQ_DIALECT),
                        "direction": direction,
                    })
        out.append({
            "function": fn_name,
            "partition_by": partition_by,
            "order_by": order_by,
            "alias": getattr(win, "alias", None) or None,
            "expression": win.sql(dialect=BQ_DIALECT)[:240],
        })
    return out


def _extract_subqueries(tree: exp.Expression) -> list[dict[str, Any]]:
    """Subqueries: IN_WHERE, EXISTS, scalar SELECT, derived tables, correlated."""
    out: list[dict[str, Any]] = []
    seen: set[int] = set()

    # IN (SELECT ...)
    for in_node in tree.find_all(exp.In):
        inner = in_node.args.get("query")
        if inner is None:
            continue
        if id(inner) in seen:
            continue
        seen.add(id(inner))
        out.append({
            "type": "IN_WHERE",
            "context": "WHERE",
            "tables": [t.name for t in inner.find_all(exp.Table)],
            "is_correlated": False,
            "sql": inner.sql(dialect=BQ_DIALECT)[:240],
        })

    # EXISTS / NOT EXISTS
    for exists_node in tree.find_all(exp.Exists):
        inner = exists_node.this
        if id(inner) in seen:
            continue
        seen.add(id(inner))
        # Detect correlation by checking if inner refs an outer column
        is_correlated = _has_outer_column_refs(inner, tree)
        out.append({
            "type": "EXISTS",
            "context": "WHERE",
            "tables": [t.name for t in inner.find_all(exp.Table)],
            "is_correlated": is_correlated,
            "sql": inner.sql(dialect=BQ_DIALECT)[:240],
        })

    # Subquery (parenthesized SELECT) in SELECT or FROM
    for sq in tree.find_all(exp.Subquery):
        if id(sq) in seen:
            continue
        seen.add(id(sq))
        # Determine context — parent is From → DERIVED_TABLE; parent is
        # SELECT → SCALAR_SELECT.
        parent = sq.parent
        ctx = "FROM" if isinstance(parent, exp.From) else "SELECT"
        sub_type = "DERIVED_TABLE" if ctx == "FROM" else "SCALAR_SELECT"
        inner = sq.this if hasattr(sq, "this") else sq
        out.append({
            "type": sub_type,
            "context": ctx,
            "tables": [t.name for t in inner.find_all(exp.Table)],
            "is_correlated": False,
            "sql": sq.sql(dialect=BQ_DIALECT)[:240],
        })
    return out


def _has_outer_column_refs(inner: exp.Expression, outer: exp.Expression) -> bool:
    """Cheap correlation check: does `inner` reference an alias only declared
    in `outer`? Returns True if so. Conservative — may false-positive."""
    outer_aliases = set()
    for t in outer.find_all(exp.Table):
        if isinstance(outer, exp.Subquery) and outer is t.parent:
            continue
        a = t.alias_or_name
        if a:
            outer_aliases.add(a)
    inner_aliases = {
        t.alias_or_name for t in inner.find_all(exp.Table) if t.alias_or_name
    }
    for col in inner.find_all(exp.Column):
        tbl = col.table
        if tbl and tbl in outer_aliases and tbl not in inner_aliases:
            return True
    return False


def _extract_set_operations(tree: exp.Expression) -> list[dict[str, Any]]:
    """UNION / INTERSECT / EXCEPT (and ALL variants)."""
    out: list[dict[str, Any]] = []
    for node in tree.find_all((exp.Union, exp.Intersect, exp.Except)):
        op_name = type(node).__name__.upper()
        if op_name == "UNION" and getattr(node, "args", {}).get("distinct") is False:
            op_name = "UNION_ALL"
        branches = []
        left = node.left
        right = node.right
        for branch in (left, right):
            primary_t = None
            for t in branch.find_all(exp.Table):
                primary_t = t.name
                break
            branches.append({
                "primary_table": primary_t,
                "fields_summary": branch.sql(dialect=BQ_DIALECT)[:120],
            })
        out.append({
            "type": op_name,
            "branch_count": len(branches),
            "branches": branches,
        })
    return out


def _extract_null_handlers(tree: exp.Expression) -> list[dict[str, Any]]:
    """COALESCE / IFNULL / NULLIF — default-value hints + ratio guards."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for cls, fn_name in (
        (exp.Coalesce, "COALESCE"),
        # IFNULL in sqlglot is often parsed as Coalesce; handle explicit If too
        (exp.Nullif, "NULLIF"),
    ):
        for node in tree.find_all(cls):
            sql_str = node.sql(dialect=BQ_DIALECT)[:240]
            if sql_str in seen:
                continue
            seen.add(sql_str)
            cols = []
            default_val = None
            for child in node.find_all(exp.Column):
                n = _col_name(child)
                if n:
                    cols.append(n)
            # The last argument of COALESCE is typically the default
            if fn_name == "COALESCE":
                args = node.expressions
                if args and isinstance(args[-1], exp.Literal):
                    try:
                        default_val = args[-1].this
                    except Exception:
                        pass
            out.append({
                "function": fn_name,
                "columns_involved": cols,
                "default_value": default_val,
                "expression": sql_str,
            })
    return out


def _extract_type_casts(tree: exp.Expression) -> list[dict[str, Any]]:
    """CAST / SAFE_CAST — type-mismatch hints."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for cast_node in tree.find_all(exp.Cast):
        sql_str = cast_node.sql(dialect=BQ_DIALECT)[:240]
        if sql_str in seen:
            continue
        seen.add(sql_str)
        col = _col_name(cast_node.this)
        to_type = None
        if cast_node.args.get("to"):
            try:
                to_type = cast_node.args["to"].sql(dialect=BQ_DIALECT).upper()
            except Exception:
                pass
        is_safe = "SAFE_CAST" in sql_str.upper() or "SAFE." in sql_str.upper()
        out.append({
            "column": col,
            "from_type": None,
            "to_type": to_type,
            "is_safe": is_safe,
            "expression": sql_str,
        })
    return out


_STRING_FN_CLASSES = (
    exp.Concat, exp.Substring, exp.Upper, exp.Lower, exp.Trim,
)


def _extract_string_functions(tree: exp.Expression) -> list[dict[str, Any]]:
    """CONCAT / SUBSTR / UPPER / LOWER / TRIM / REGEXP_*. Derived-dim candidates."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for node in tree.find_all(_STRING_FN_CLASSES):
        sql_str = node.sql(dialect=BQ_DIALECT)[:240]
        if sql_str in seen:
            continue
        seen.add(sql_str)
        fn_name = type(node).__name__.upper()
        cols = []
        for c in node.find_all(exp.Column):
            n = _col_name(c)
            if n and n not in cols:
                cols.append(n)
        out.append({
            "function": fn_name,
            "columns": cols,
            "alias": None,
            "expression": sql_str,
        })
    # REGEXP_* functions parse as Anonymous func — pick them up separately.
    for anon in tree.find_all(exp.Anonymous):
        name = (anon.name or "").upper()
        if not name.startswith("REGEXP"):
            continue
        sql_str = anon.sql(dialect=BQ_DIALECT)[:240]
        if sql_str in seen:
            continue
        seen.add(sql_str)
        cols = [_col_name(c) for c in anon.find_all(exp.Column) if _col_name(c)]
        out.append({
            "function": name,
            "columns": cols,
            "alias": None,
            "expression": sql_str,
        })
    return out


_MATH_FN_CLASSES = (
    exp.Round, exp.Floor, exp.Ceil, exp.Abs,
)


def _extract_math_functions(tree: exp.Expression) -> list[dict[str, Any]]:
    """ROUND / FLOOR / CEIL / ABS — format hints (frequent ROUND→cents → usd)."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for node in tree.find_all(_MATH_FN_CLASSES):
        sql_str = node.sql(dialect=BQ_DIALECT)[:240]
        if sql_str in seen:
            continue
        seen.add(sql_str)
        fn_name = type(node).__name__.upper()
        col = None
        for c in node.find_all(exp.Column):
            col = _col_name(c)
            break
        out.append({
            "function": fn_name,
            "column": col,
            "alias": None,
            "expression": sql_str,
        })
    return out


_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _extract_comments(tree: exp.Expression) -> list[dict[str, Any]]:
    """Comments in the SQL — line vs block. Mineable for NL phrasings.

    Notes: this extracts from the RAW SQL string when present on the
    Expression; otherwise no-op. sqlglot does carry comments on nodes
    but for simplicity we re-scan the raw SQL.
    """
    out: list[dict[str, Any]] = []
    # sqlglot exposes comments on `tree.comments` for top-level
    for c in (getattr(tree, "comments", None) or []):
        out.append({"type": "block", "position": "before_select", "text": c})
    # Also pull from raw SQL if accessible (the Expression keeps it)
    raw = getattr(tree, "sql", lambda **kw: "")(dialect=BQ_DIALECT)
    for m in _LINE_COMMENT_RE.findall(raw):
        out.append({"type": "line", "position": "inline", "text": m.lstrip("-").strip()})
    for m in _BLOCK_COMMENT_RE.findall(raw):
        out.append({
            "type": "block", "position": "inline",
            "text": m.strip("/*").strip("*/").strip(),
        })
    return out


_PARAM_RE = re.compile(r"@(\w+)|\$\{(\w+)\}")


def _extract_parameters(tree: exp.Expression) -> list[dict[str, Any]]:
    """@param / ${var} placeholders — user-input filter patterns."""
    raw = getattr(tree, "sql", lambda **kw: "")(dialect=BQ_DIALECT)
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in _PARAM_RE.finditer(raw):
        name = m.group(1) or m.group(2)
        if not name or name in seen:
            continue
        seen.add(name)
        # Try to determine which clause it appears in via location heuristic.
        idx = m.start()
        # Find nearest preceding clause keyword.
        clause = "WHERE"
        for kw in ("WHERE", "HAVING", "SELECT", "FROM", "JOIN", "GROUP BY"):
            kw_idx = raw[:idx].upper().rfind(kw)
            if kw_idx != -1:
                clause = kw if kw != "GROUP BY" else "GROUP_BY"
                break
        out.append({
            "name": name, "type_inferred": None, "used_in_clause": clause,
        })
    return out


def _extract_qualify_clauses(tree: exp.Expression) -> list[dict[str, Any]]:
    """BigQuery QUALIFY — post-window filter."""
    top = tree.find(exp.Select)
    if top is None:
        return []
    qualify = top.args.get("qualify")
    if qualify is None:
        return []
    sql_str = qualify.sql(dialect=BQ_DIALECT)[:240]
    win_fn = None
    for w in qualify.find_all(exp.Window):
        win_fn = type(w.this).__name__.upper() if w.this else None
        break
    return [{
        "expression": sql_str,
        "window_function": win_fn,
        "operator": "",
        "value": "",
    }]


def _extract_array_operations(tree: exp.Expression) -> list[dict[str, Any]]:
    """UNNEST / ARRAY_AGG / ARRAY_LENGTH — flattened-dim candidates."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for node in tree.find_all(exp.Unnest):
        sql_str = node.sql(dialect=BQ_DIALECT)[:240]
        if sql_str in seen:
            continue
        seen.add(sql_str)
        col = None
        for c in node.find_all(exp.Column):
            col = _col_name(c)
            break
        out.append({
            "operation": "UNNEST",
            "column": col,
            "context": "FROM",
            "alias": getattr(node, "alias", None) or None,
        })
    for anon in tree.find_all(exp.Anonymous):
        name = (anon.name or "").upper()
        if name not in {"ARRAY_AGG", "ARRAY_LENGTH"}:
            continue
        sql_str = anon.sql(dialect=BQ_DIALECT)[:240]
        if sql_str in seen:
            continue
        seen.add(sql_str)
        col = None
        for c in anon.find_all(exp.Column):
            col = _col_name(c)
            break
        out.append({
            "operation": name, "column": col,
            "context": "SELECT", "alias": None,
        })
    return out


def _extract_struct_access(tree: exp.Expression) -> list[dict[str, Any]]:
    """Dot-path access on nested STRUCT fields (e.g., payment.method.type)."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for dot in tree.find_all(exp.Dot):
        sql_str = dot.sql(dialect=BQ_DIALECT)[:240]
        if sql_str in seen:
            continue
        seen.add(sql_str)
        # Walk the dot chain
        path: list[str] = []
        cur: exp.Expression | None = dot
        while isinstance(cur, exp.Dot):
            right = cur.expression if hasattr(cur, "expression") else None
            if right is not None and hasattr(right, "name"):
                path.insert(0, right.name)
            cur = cur.this
        root = None
        if cur is not None and hasattr(cur, "name"):
            root = cur.name
            path.insert(0, root)
        if len(path) < 2:
            continue
        out.append({
            "path": path,
            "root_column": root,
            "alias": None,
        })
    return out


def _extract_json_operations(tree: exp.Expression) -> list[dict[str, Any]]:
    """JSON_EXTRACT / JSON_VALUE / JSON_QUERY / JSON_EXTRACT_ARRAY."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for anon in tree.find_all(exp.Anonymous):
        name = (anon.name or "").upper()
        if not name.startswith("JSON_"):
            continue
        sql_str = anon.sql(dialect=BQ_DIALECT)[:240]
        if sql_str in seen:
            continue
        seen.add(sql_str)
        col = None
        path = None
        if anon.expressions:
            first = anon.expressions[0]
            col = _col_name(first) or first.sql(dialect=BQ_DIALECT)
            if len(anon.expressions) > 1:
                second = anon.expressions[1]
                if isinstance(second, exp.Literal):
                    path = second.this
        out.append({
            "function": name, "column": col, "path": path, "alias": None,
        })
    return out


def _extract_self_joins(tree: exp.Expression) -> list[dict[str, Any]]:
    """Same canonical table joined to itself via different aliases."""
    top = tree.find(exp.Select)
    if top is None:
        return []
    by_table: dict[str, list[str]] = {}
    # FROM clause table — use exp.From walk (sqlglot uses `from_` key,
    # not `from`, in current versions; find(exp.From) sidesteps that).
    from_node = tree.find(exp.From)
    if from_node is not None:
        for t in from_node.find_all(exp.Table):
            by_table.setdefault(t.name, []).append(t.alias_or_name or t.name)
    # Joined tables
    for j in top.args.get("joins") or []:
        if isinstance(j.this, exp.Table):
            by_table.setdefault(j.this.name, []).append(
                j.this.alias_or_name or j.this.name
            )
    out: list[dict[str, Any]] = []
    for table, aliases in by_table.items():
        if len(set(aliases)) >= 2:
            out.append({
                "table": table,
                "aliases_used": sorted(set(aliases)),
                "role_hint": None,
            })
    return out


def _extract_partition_pseudocolumns(tree: exp.Expression) -> list[dict[str, Any]]:
    """BigQuery _PARTITIONTIME / _PARTITIONDATE references."""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for c in tree.find_all(exp.Column):
        n = (c.name or "").upper()
        if n not in {"_PARTITIONTIME", "_PARTITIONDATE"}:
            continue
        tbl = c.table or ""
        key = (n, tbl)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "column": n, "table": tbl or None, "in_clause": "WHERE",
        })
    return out


_HINT_RE = re.compile(r"/\*\+(.+?)\*/", re.DOTALL)


def _extract_sql_hints(tree: exp.Expression) -> list[dict[str, Any]]:
    """/*+ ... */ optimizer hints."""
    raw = getattr(tree, "sql", lambda **kw: "")(dialect=BQ_DIALECT)
    return [{"hint": m.strip()} for m in _HINT_RE.findall(raw)]


def _build_query_shape_summary(fp: "SQLFingerprint") -> dict[str, Any]:
    """Aggregate counts + complexity score from the populated fingerprint."""
    complexity = 0
    if fp.ctes:
        complexity += 1
    if fp.joins:
        complexity += 2
    if fp.case_whens:
        complexity += 1
    if fp.window_functions:
        complexity += 1
    if fp.subqueries:
        complexity += 1
    if fp.set_operations:
        complexity += 1
    return {
        "n_tables": len(fp.tables),
        "n_joins": len(fp.joins),
        "n_aggregations": len(fp.aggregations),
        "n_filters": len(fp.filters),
        "n_ctes": len(fp.ctes),
        "n_group_by": len(fp.group_by),
        "n_select_columns": len(fp.select_aliases),
        "has_having": bool(fp.having),
        "has_order_by": bool(fp.order_by),
        "has_limit": bool(fp.limit),
        "has_distinct": bool(fp.distinct_select),
        "has_window_function": bool(fp.window_functions),
        "has_subquery": bool(fp.subqueries),
        "has_set_operation": bool(fp.set_operations),
        "dialect": "bigquery",
        "complexity_score": complexity,
    }


# ─── Layer 4: semantic-graph-feeding signals ────────────────


def _compute_query_fingerprint_hash(sql: str) -> str:
    """SHA256 over a canonicalized form of the SQL.

    Canonicalization: lowercased, whitespace-collapsed, comments stripped.
    Two queries with identical semantics modulo formatting hash to the same value.
    """
    import hashlib
    s = _LINE_COMMENT_RE.sub("", sql or "")
    s = _BLOCK_COMMENT_RE.sub("", s)
    s = " ".join(s.split()).lower()
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _infer_intent_class(fp: "SQLFingerprint") -> str:
    """Rule-based intent classification.

    single_lookup: WHERE on PK-shaped column, no GROUP BY, no agg.
    aggregate: has GROUP BY + aggregations, no time grain.
    trend: has aggregations + date_function GROUP BY.
    cohort: has CTE whose name suggests cohort + downstream JOIN.
    attribution: window function detected (ranks/lags).
    top_n: ORDER BY + LIMIT.
    comparison: has set_operations (UNION/INTERSECT/EXCEPT).
    """
    has_agg = bool(fp.aggregations)
    has_gb = bool(fp.group_by)
    # Date-in-GROUP-BY: either the source date column is in group_by, OR
    # the date function's output alias (e.g. `m` in DATE_TRUNC(x,MONTH) AS m
    # when GROUP BY m). The latter requires cross-referencing select_aliases.
    date_cols = {(d.get("column") or "").lower() for d in (fp.date_functions or [])}
    # Date column → its SELECT alias (when one of the date functions
    # wraps this column in a SELECT projection).
    date_aliases: set[str] = set()
    for sa in (fp.select_aliases or []):
        sa_col = (sa.get("column") or "").lower()
        sa_alias = (sa.get("alias") or "").lower()
        expr = (sa.get("expression") or "").upper()
        if sa_col in date_cols or "DATE_TRUNC" in expr or "EXTRACT" in expr:
            if sa_alias:
                date_aliases.add(sa_alias)
    gb_cols = {(g.get("column") or "").lower() for g in (fp.group_by or [])}
    has_date_in_gb = bool((date_cols | date_aliases) & gb_cols)
    has_window = bool(fp.window_functions)
    has_top_n = bool(fp.limit and fp.order_by)
    has_set_ops = bool(fp.set_operations)
    has_cohort_cte = any(
        any(kw in (c.get("alias") or "").lower()
            for kw in ("active", "high_value", "engaged", "cohort", "eligible"))
        for c in (fp.ctes or [])
    )
    if has_set_ops:
        return "comparison"
    if has_window:
        return "attribution"
    if has_top_n:
        return "top_n"
    if has_cohort_cte and has_agg:
        return "cohort"
    if has_agg and has_date_in_gb:
        return "trend"
    if has_agg and has_gb:
        return "aggregate"
    if not has_agg and not has_gb and fp.filters:
        return "single_lookup"
    return "unknown"


def _infer_implicit_grain(fp: "SQLFingerprint") -> str:
    """Derive grain string from GROUP BY + aggregation source table.

    Example: GROUP BY cm11, rpt_dt_month on cardmember_dim → 'cardmember_dim-cm11-month'.
    Empty when no GROUP BY or no aggregation.
    """
    if not fp.group_by:
        return ""
    base = fp.primary_table or ""
    gb_parts: list[str] = []
    for g in fp.group_by:
        col = (g.get("column") or "").lower()
        if not col:
            continue
        gb_parts.append(col)
    # Date granularity from date_functions
    for df in (fp.date_functions or []):
        gran = (df.get("granularity") or "").lower()
        if gran:
            gb_parts.append(gran)
    if not gb_parts:
        return ""
    return f"{base}-" + "-".join(gb_parts)


def _propose_derived_dims(fp: "SQLFingerprint") -> list[dict[str, Any]]:
    """Each CASE WHEN with buckets is a derived-dim proposal."""
    out: list[dict[str, Any]] = []
    for cw in fp.case_whens or []:
        if not cw.get("mapped_values"):
            continue
        out.append({
            "output_name": cw.get("alias"),
            "source_column": cw.get("source_column"),
            "buckets": cw.get("mapped_values"),
            "bucket_type": "categorical",
        })
    return out


_COHORT_KEYWORDS = (
    "active", "high_value", "engaged", "premium", "delinquent", "eligible",
    "cohort", "loyal", "returning", "new", "churned", "at_risk",
)


def _detect_cohort_scopes(fp: "SQLFingerprint") -> list[dict[str, Any]]:
    """CTE names that imply a named cohort → graph Cohort candidates."""
    out: list[dict[str, Any]] = []
    for cte in fp.ctes or []:
        alias = (cte.get("alias") or "").lower()
        if not any(kw in alias for kw in _COHORT_KEYWORDS):
            continue
        out.append({
            "cohort_name": alias,
            "definition_filters": cte.get("structural_filters") or [],
            "source_cte": cte.get("alias"),
        })
    return out


# Suppress unused-import warning for re.
_ = re
