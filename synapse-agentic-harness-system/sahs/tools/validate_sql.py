"""validate_sql — the deterministic pre-flight every query passes
through BEFORE any execution object exists.

Pipeline (pinned): canon-parse → statement class → tables exist in the
compiled schema → columns exist / are unambiguous → metric-contract
conformance (expression containment on canonical text; GROUP BY dims ⊆
approved by token match; grain degeneracy) → access pre-check vs
acl.json (E3: ``policy_unknown`` surfaces HERE as a warning; the
sandbox turns it into a live DENY). Returns ``{ok, violations[],
warnings[]}`` — a violation is a refusal reason, a warning is a
disclosure; every entry carries a ``hint`` that teaches the correct
call.

Violation catalog (codes are contract, tests pin them):
parse_error · statement_not_allowed · not_a_select · unknown_table ·
unknown_column · ambiguous_column · sensitive_column ·
select_star_over_sensitive · cross_join_unconstrained ·
unknown_metric · metric_expression_missing · dim_not_approved

Warning catalog: policy_unknown · restricted_table · select_star ·
no_where_filter · dims_unchecked · sensitive_column_in_filter ·
group_by_at_metric_grain · qualification_partial
"""

from __future__ import annotations

import difflib
from typing import Any

from sahs.canon.canonical import try_canon
from sahs.tools.api import Build, _tokens

_DML_DDL = {"insert", "update", "delete", "merge", "create", "drop",
            "alter", "truncate", "grant"}
_QUERY_KINDS = {"select", "union"}

# pinned time-dimension synonyms: a *_dt / *_date column conforms to an
# approved dimension named time_period / date / period
_TIME_TOKENS = {"dt", "date", "day", "week", "month", "quarter", "year",
                "time"}
_TIME_DIMS = {"time", "period", "date"}


def _entry(code: str, detail: str, hint: str) -> dict[str, str]:
    return {"code": code, "detail": detail, "hint": hint}


def _dim_tokens(name: str) -> set[str]:
    return {t for t in name.lower().replace("-", "_").replace(" ", "_")
            .split("_") if t}


def _column_matches_dim(column: str, dims: list[str]) -> bool:
    col_tokens = _dim_tokens(column)
    for dim in dims:
        dim_tokens = _dim_tokens(dim)
        if col_tokens & dim_tokens:
            return True
        if (col_tokens & _TIME_TOKENS) and (dim_tokens & _TIME_DIMS):
            return True
    return False


def validate_sql(build: Build, sql: str, metric_id: str = "") -> dict:
    """Pre-flight a query against the compiled build. Pass ``metric_id``
    (metric:fp, bare fp, or mgroup id) to additionally check the query
    honors that metric's contract."""
    violations: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []

    result, err = try_canon(sql)
    if err is not None:
        return {"ok": False, "violations": [_entry(
            "parse_error", str(err)[:200],
            "the SQL does not parse as BigQuery standard SQL — fix the "
            "syntax; validate_sql only accepts full SELECT statements")],
            "warnings": []}
    if result.kind in _DML_DDL:
        return {"ok": False, "violations": [_entry(
            "statement_not_allowed", f"statement kind: {result.kind}",
            "only read-only SELECT/UNION queries are allowed — the "
            "warehouse is written by pipelines, never by this tool")],
            "warnings": []}
    if result.kind not in _QUERY_KINDS:
        return {"ok": False, "violations": [_entry(
            "not_a_select", f"parsed as {result.kind}, not a query",
            "send a complete SELECT statement; fragments belong in "
            "search_concepts, not here")], "warnings": []}
    if result.qualified == "partial":
        warnings.append(_entry(
            "qualification_partial",
            "column qualification degraded (no schema at canon time)",
            "unqualified columns are checked against every referenced "
            "table"))

    import sqlglot
    from sqlglot import expressions as exp
    tree = sqlglot.parse_one(sql, read="bigquery")

    # ── tables: resolve every FROM/JOIN target against schema.json ──
    cte_names = {c.alias_or_name.lower()
                 for c in tree.find_all(exp.CTE)}
    alias_to_physical: dict[str, str] = {}
    physicals: list[str] = []
    for table in tree.find_all(exp.Table):
        raw = ".".join(p for p in (table.text("db"), table.name) if p)
        if raw.lower() in cte_names:
            continue
        physical = build.physical_of(raw)
        if physical is None:
            close = difflib.get_close_matches(
                raw.lower().split(".")[-1],
                [build.short_table(t) for t in build.schema], 3)
            violations.append(_entry(
                "unknown_table", f"table {raw!r} not in this build",
                ("did you mean: " + ", ".join(close) + "? "
                 if close else "")
                + "describe_table lists what a table offers"))
            continue
        if physical not in physicals:
            physicals.append(physical)
        for key in (table.alias, raw, raw.split(".")[-1]):
            if key:
                alias_to_physical[key.lower()] = physical

    # ── access pre-check (E3): the acl verdicts, disclosed up front ──
    pii_by_table: dict[str, set[str]] = {}
    for physical in physicals:
        acl = build.acl.get(physical, {})
        pii_by_table[physical] = {c.lower()
                                  for c in acl.get("pii_columns", [])}
        restricted = acl.get("restricted")
        if restricted == "unknown_policy":
            warnings.append(_entry(
                "policy_unknown",
                f"{physical}: row-access policy UNKNOWN (listing denied "
                "at extraction)",
                "execute_sandboxed will DENY live mode for this table "
                "until the policy is resolved; snapshot dry-run is "
                "permitted"))
        elif restricted:
            warnings.append(_entry(
                "restricted_table",
                f"{physical}: row-access policy {restricted}",
                "results are policy-filtered; live mode requires "
                "clearance"))

    # ── projection: SELECT * and sensitive columns ──
    select_aliases = {s.alias.lower() for s in tree.find_all(exp.Alias)
                      if s.alias}
    outer_select = tree if isinstance(tree, exp.Select) else None
    projected: list[exp.Expression] = (
        list(outer_select.expressions) if outer_select is not None else [])
    has_star = any(isinstance(e, exp.Star) or (
        isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
        for e in projected)
    if has_star:
        starred_pii = sorted({c for p in physicals
                              for c in pii_by_table.get(p, ())})
        if starred_pii:
            violations.append(_entry(
                "select_star_over_sensitive",
                "SELECT * would project sensitive columns: "
                + ", ".join(starred_pii),
                "name the columns you need and leave the sensitive "
                "ones out"))
        else:
            warnings.append(_entry(
                "select_star", "SELECT * projects every column",
                "name columns — cards list them; * defeats the "
                "column-level access check on future builds"))

    def _resolve_column(column: exp.Column) -> tuple[str | None, str]:
        """→ (physical_or_None, disposition) where disposition ∈
        ok|unknown|ambiguous|skipped."""
        name = column.name.lower()
        qualifier = column.table.lower() if column.table else ""
        if qualifier:
            if qualifier in cte_names:
                return None, "skipped"
            physical = alias_to_physical.get(qualifier)
            if physical is None:
                return None, "skipped"       # unresolvable qualifier
            if name in build.schema.get(physical, {}):
                return physical, "ok"
            return physical, "unknown"
        if name in select_aliases:
            return None, "skipped"           # GROUP/ORDER BY an alias
        if cte_names:
            hosts = [p for p in physicals
                     if name in build.schema.get(p, {})]
            return (hosts[0], "ok") if hosts else (None, "skipped")
        hosts = [p for p in physicals if name in build.schema.get(p, {})]
        if len(hosts) == 1:
            return hosts[0], "ok"
        if len(hosts) > 1:
            return None, "ambiguous"
        return None, "unknown"

    in_projection: set[int] = {id(n) for e in projected
                               for n in e.walk()}
    where_node = outer_select.args.get("where") if outer_select else None
    in_where: set[int] = ({id(n) for n in where_node.walk()}
                          if where_node is not None else set())
    seen: set[tuple[str, str]] = set()
    for column in tree.find_all(exp.Column):
        if isinstance(column.this, exp.Star):
            continue
        name = column.name.lower()
        physical, disposition = _resolve_column(column)
        key = (column.table.lower() if column.table else "", name)
        if key in seen:
            pass
        elif disposition == "unknown":
            hosts = physicals if physical is None else [physical]
            candidates = sorted({c for p in hosts
                                 for c in build.schema.get(p, {})})
            close = difflib.get_close_matches(name, candidates, 3)
            violations.append(_entry(
                "unknown_column",
                f"column {name!r} not in "
                + (physical or " / ".join(physicals) or "any table"),
                ("did you mean: " + ", ".join(close) + "? "
                 if close else "")
                + "describe_table shows the real columns"))
        elif disposition == "ambiguous":
            hosts = [p for p in physicals
                     if name in build.schema.get(p, {})]
            violations.append(_entry(
                "ambiguous_column",
                f"column {name!r} exists in {', '.join(hosts)}",
                f"qualify it: {hosts[0].split('.')[-1]}.{name}"))
        seen.add(key)
        if physical and name in pii_by_table.get(physical, ()):
            if id(column) in in_projection:
                violations.append(_entry(
                    "sensitive_column",
                    f"{physical}.{name} is sensitive "
                    "(union_most_restrictive)",
                    "drop it from the SELECT list, or route through the "
                    "governed access process — this tool cannot grant "
                    "it"))
            elif id(column) in in_where:
                warnings.append(_entry(
                    "sensitive_column_in_filter",
                    f"{physical}.{name} used as a filter",
                    "filtering by a sensitive column is logged; the "
                    "value never leaves the warehouse"))

    # ── joins: no unconstrained products ──
    for join in tree.find_all(exp.Join):
        if join.args.get("on") is None and join.args.get("using") is None:
            other = join.this
            names = [n.name for n in [other] if isinstance(n, exp.Table)]
            declared = [j for j in build.joins
                        if {j.get("a"), j.get("b")} <= set(physicals)]
            hint = ("declared join path: "
                    + declared[0].get("on", f"{declared[0].get('a')} ↔ "
                                            f"{declared[0].get('b')}")
                    if declared else
                    "add an ON condition; describe_table lists observed "
                    "join partners")
            violations.append(_entry(
                "cross_join_unconstrained",
                "join without ON/USING"
                + (f" (to {names[0]})" if names else ""), hint))

    if outer_select is not None and where_node is None \
            and not tree.find(exp.Group):
        warnings.append(_entry(
            "no_where_filter", "no WHERE clause — full scan",
            "filter on the partition column (see the table card's "
            "grain line) to bound cost"))

    # ── metric contract ──
    if metric_id:
        target = metric_id
        row = next((m for m in build.metrics
                    if m["id"] == target or m["fp"] == target
                    or target in m.get("mgroups", [])
                    or m.get("mgroup") == target), None)
        if row is None:
            violations.append(_entry(
                "unknown_metric", f"no metric {metric_id!r} in build",
                "ids come from search_metrics / resolve"))
        else:
            if row["canonical_sql"] and \
                    row["canonical_sql"] not in result.canonical_sql:
                violations.append(_entry(
                    "metric_expression_missing",
                    f"the certified expression for {row['label']!r} "
                    "does not appear in the query",
                    f"use exactly: {row['canonical_sql']} — variants "
                    "must go through get_definition_line disclosure"))
            group = tree.find(exp.Group)
            group_columns = ([c.name for c in group.find_all(exp.Column)]
                             if group is not None else [])
            approved = row.get("approved_dimensions") or []
            grain = (row.get("grain") or "").lower()
            if group_columns and approved:
                for column in group_columns:
                    if _column_matches_dim(column, approved) or (
                            grain and grain in _dim_tokens(column)):
                        continue
                    violations.append(_entry(
                        "dim_not_approved",
                        f"GROUP BY {column} is not an approved "
                        f"dimension of {row['label']!r} "
                        f"(approved: {', '.join(approved)})",
                        "aggregate only along the contract's approved "
                        "dimensions, or use the metric off-contract "
                        "with disclosure"))
            elif group_columns and not approved:
                warnings.append(_entry(
                    "dims_unchecked",
                    f"{row['label']!r} declares no approved dimensions",
                    "GROUP BY conformance cannot be checked — the "
                    "steward should extend the contract"))
            if grain and any(grain in _dim_tokens(c)
                             for c in group_columns):
                warnings.append(_entry(
                    "group_by_at_metric_grain",
                    f"grouping at the metric's own grain ({grain})",
                    "the aggregate degenerates to row level — usually "
                    "a coarser dimension is meant"))

    return {"ok": not violations, "violations": violations,
            "warnings": warnings}
