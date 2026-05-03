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


def _parse_one(raw_sql: str) -> SQLFingerprint:
    fp = SQLFingerprint(raw_sql=raw_sql)
    try:
        tree = sqlglot.parse_one(raw_sql, dialect=BQ_DIALECT)
    except Exception as e:
        fp.parse_error = f"{type(e).__name__}: {e}"
        return fp

    fp.ctes = _extract_ctes(tree)
    cte_aliases = {c["alias"] for c in fp.ctes}

    fp.tables = _extract_tables(tree, exclude=cte_aliases)
    fp.primary_table = fp.tables[0] if fp.tables else None
    fp.aggregations = _extract_aggregations(tree)
    fp.case_whens = _extract_case_whens(tree)
    fp.joins = _extract_joins(tree)
    fp.filters = _extract_filters(tree)
    fp.date_functions = _extract_date_functions(tree)
    return fp


def _extract_tables(tree: exp.Expression, exclude: set[str]) -> list[str]:
    """Real tables (not CTE aliases). Preserves first-seen order."""
    seen: list[str] = []
    for t in tree.find_all(exp.Table):
        name = t.name
        if name and name not in exclude and name not in seen:
            seen.append(name)
    return seen


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
    (everything in its WHERE), and the source tables it reads from.
    """
    out: list[dict[str, Any]] = []
    with_node = tree.find(exp.With)
    if with_node is None:
        return out
    for cte in with_node.expressions or []:
        alias = cte.alias
        body = cte.this  # the SELECT inside
        cte_filters = _flatten_predicates(body.find(exp.Where).this) if body.find(exp.Where) else []
        # CTE-internal filters are STRUCTURAL by definition — they define the
        # scope of the CTE and aren't user-toggleable.
        for f in cte_filters:
            f["is_structural"] = True
        # Source tables this CTE reads from.
        cte_alias_set = {c.alias for c in (with_node.expressions or [])}
        source_tables = [
            t.name for t in body.find_all(exp.Table)
            if t.name and t.name not in cte_alias_set
        ]
        out.append({
            "alias": alias,
            "structural_filters": cte_filters,
            "sql": body.sql(dialect=BQ_DIALECT),
            "source_tables": source_tables,
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

    # Now hydrate with MDM + baseline.
    for table_name, raw_ctx in contexts.items():
        mdm = mdm_client.fetch(table_name)
        raw_ctx["mdm_columns"] = mdm.get("columns") or []
        raw_ctx["mdm_table_description"] = mdm.get("table_description")
        raw_ctx["mdm_coverage_pct"] = float(mdm.get("mdm_coverage_pct") or 0.0)

        baseline_text = _find_baseline_view(baseline_dir, table_name)
        if baseline_text is not None:
            raw_ctx["existing_view_lkml"] = baseline_text

    return {name: TableContext(**raw) for name, raw in contexts.items()}


def _find_baseline_view(baseline_dir: Path, table_name: str) -> str | None:
    """Find <table>.view.lkml under baseline_dir, searching at the root and
    recursively in subdirs. Returns the file's text content or None if
    not found.

    This lets the same `discover_tables()` work against either layout:
      data/baseline_views/<table>.view.lkml      (flat — fetch_baselines)
      data/looker_master/views/<table>.view.lkml (mirror — fetch_lookml_master)
    """
    if not baseline_dir.exists():
        return None
    target = f"{table_name}.view.lkml"
    # Quick path: file at root
    direct = baseline_dir / target
    if direct.is_file():
        return direct.read_text(encoding="utf-8")
    # Recursive search; first hit wins. Fast for hundreds of files.
    for path in baseline_dir.rglob(target):
        return path.read_text(encoding="utf-8")
    return None


def _empty_context(table_name: str) -> dict[str, Any]:
    return {
        "table_name": table_name,
        "columns_referenced": [],
        "aggregations": [],
        "case_whens": [],
        "ctes_referencing_this": [],
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
