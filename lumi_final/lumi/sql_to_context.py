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
    """Parse a baseline .view.lkml string with the lkml lib and surface
    structured signals for downstream stages.

    Returns:
        {
            "dimensions": [<lkml dim dict>...],
            "dimension_groups": [...],
            "measures": [...],
            "quality_signals": {
                "dims_total": int,
                "dims_missing_description": int,
                "dims_short_description": int,        # < threshold chars
                "dims_missing_label": int,
                "dims_missing_tags": int,
                "measures_total": int,
                "measures_missing_value_format": int,
                "dates_as_plain_dim": int,            # date col with no dim_group
                "has_primary_key": bool,
            }
        }

    Failures (unparseable LookML, missing views) return empty structures —
    callers must tolerate that since baseline files can drift.
    """
    try:
        import lkml  # local import to keep module-import cost down
        tree = lkml.load(lkml_text)
    except Exception as e:  # noqa: BLE001
        logger.warning("baseline parse failed; skipping structured fields: %s", e)
        return {
            "dimensions": [],
            "dimension_groups": [],
            "measures": [],
            "quality_signals": {},
        }

    views = tree.get("views") or []
    if not views:
        return {
            "dimensions": [],
            "dimension_groups": [],
            "measures": [],
            "quality_signals": {},
        }

    # First view in the file is canonical (Looker-generated baselines have one).
    view = views[0]
    dims = list(view.get("dimensions") or [])
    dgs = list(view.get("dimension_groups") or [])
    msrs = list(view.get("measures") or [])

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
    has_pk = any(
        (d.get("primary_key") or "").lower() in {"yes", "true"} for d in dims
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
        "quality_signals": {
            "dims_total": len(dims),
            "dims_missing_description": dims_missing_desc,
            "dims_short_description": dims_short_desc,
            "dims_missing_label": dims_missing_label,
            "dims_missing_tags": dims_missing_tags,
            "measures_total": len(msrs),
            "measures_missing_value_format": msrs_missing_vf,
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
