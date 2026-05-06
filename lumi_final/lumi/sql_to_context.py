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
    out: list[dict[str, Any]] = []
    for expr in group.expressions or []:
        if isinstance(expr, exp.Column):
            out.append({
                "table": expr.table or None,
                "column": expr.name,
            })
        else:
            # Could be GROUP BY 1, 2 (positional), or expressions.
            # Walk inner Columns for best-effort capture.
            for col in expr.find_all(exp.Column):
                out.append({
                    "table": col.table or None,
                    "column": col.name,
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


# Suppress unused-import warning for re (helpful even if unused right now).
_ = re
