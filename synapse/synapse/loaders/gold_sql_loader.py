"""Gold-SQL corpus loader — sqlglot extraction from a folder of .sql files.

The lumi pipeline produces the richest corpus signal (via
``lumi.sql_to_context`` and ``session1_output.json``, consumed by
``lumi_loader``). This loader is the standalone complement: point it at ANY
directory of analyst-written SQL and it emits the same canonical artifacts
— no lumi run, no sys.path reach into the sibling repo:

    out_dir/
      lumi_signals/<table>.json    ← aggregations, joins, filters,
                                     case_whens, columns_referenced,
                                     date_functions  (per table)
      gold_queries/<query>.sql     ← verbatim copies for provenance

The graph builder ingests both via `_ingest_lumi_signals` / `_ingest_corpus`
without changes.

Extraction is deliberately conservative: only facts sqlglot can prove from
the AST are emitted (behavioral witness — "what analysts actually do"), and
literals are treated as query text, never as row data.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from synapse.loaders.types import LoadResult

try:
    import sqlglot
    from sqlglot import expressions as exp
except ImportError:  # pragma: no cover
    sqlglot = None  # type: ignore[assignment]
    exp = None  # type: ignore[assignment]


_AGG_FUNCS = ("Sum", "Count", "Avg", "Min", "Max")


def load_gold_sql_corpus(
    sql_dir: Path,
    *,
    out_dir: Path,
    dialect: str = "bigquery",
    dry_run: bool = False,
) -> LoadResult:
    """Parse every ``*.sql`` under ``sql_dir`` → canonical signal artifacts."""
    started = time.monotonic()
    if sqlglot is None:
        return LoadResult(
            status="error", source="corpus", table_id=str(sql_dir),
            error="sqlglot is required for the gold-SQL loader",
        )
    sql_dir = Path(sql_dir).expanduser()
    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        return LoadResult(
            status="skipped", source="corpus", table_id=str(sql_dir),
            warnings=[f"no *.sql files under {sql_dir}"],
        )

    per_table: dict[str, dict[str, Any]] = {}
    parse_failures: list[str] = []
    parsed_count = 0

    for sql_path in sql_files:
        text = sql_path.read_text(encoding="utf-8", errors="replace")
        query_id = sql_path.stem
        try:
            statements = sqlglot.parse(text, read=dialect)
        except Exception:
            try:  # one dialect-free retry
                statements = sqlglot.parse(text)
            except Exception as exc:
                parse_failures.append(f"{sql_path.name}: {exc}")
                continue
        for stmt in statements:
            if stmt is None:
                continue
            _extract_statement(stmt, query_id, per_table)
        parsed_count += 1

    written: list[Path] = []
    if not dry_run:
        signals_dir = Path(out_dir) / "lumi_signals"
        queries_dir = Path(out_dir) / "gold_queries"
        signals_dir.mkdir(parents=True, exist_ok=True)
        queries_dir.mkdir(parents=True, exist_ok=True)
        for table, blob in sorted(per_table.items()):
            blob["table_name"] = table
            blob["columns_referenced"] = sorted(blob["columns_referenced"])
            out_path = signals_dir / f"{_safe_stem(table)}.json"
            out_path.write_text(
                json.dumps(blob, indent=2, ensure_ascii=False), encoding="utf-8",
            )
            written.append(out_path)
        for sql_path in sql_files:
            target = queries_dir / sql_path.name
            target.write_text(
                sql_path.read_text(encoding="utf-8", errors="replace"),
                encoding="utf-8",
            )
            written.append(target)

    status = "ok" if parsed_count == len(sql_files) else (
        "partial" if parsed_count else "error"
    )
    return LoadResult(
        status=status,
        source="corpus",
        table_id=str(sql_dir),
        artifacts_written=written,
        records_count=parsed_count,
        warnings=parse_failures,
        latency_ms=int((time.monotonic() - started) * 1000),
        metadata={
            "tables_discovered": sorted(per_table),
            "n_queries": len(sql_files),
            "n_parse_failures": len(parse_failures),
            "dialect": dialect,
        },
    )


# ─── per-statement extraction ────────────────────────────────


def _blank_blob() -> dict[str, Any]:
    return {
        "table_name": "",
        "aggregations": [],
        "joins": [],
        "case_whens": [],
        "filters": [],
        "columns_referenced": set(),
        "date_functions": [],
    }


def _extract_statement(
    stmt: "exp.Expression", query_id: str, per_table: dict[str, dict[str, Any]],
) -> None:
    alias_to_table = _alias_map(stmt)
    tables = list(dict.fromkeys(alias_to_table.values()))
    if not tables:
        return
    primary = tables[0]
    for t in tables:
        per_table.setdefault(t, _blank_blob())

    def owner_of(column: exp.Column) -> str | None:
        qualifier = column.table
        if qualifier:
            return alias_to_table.get(qualifier.lower())
        return primary if len(tables) == 1 else None

    # Columns referenced
    for column in stmt.find_all(exp.Column):
        owner = owner_of(column)
        if owner and column.name:
            per_table[owner]["columns_referenced"].add(column.name)

    # Aggregations (with alias if the agg sits under an Alias node)
    for agg_name in _AGG_FUNCS:
        for agg in stmt.find_all(getattr(exp, agg_name)):
            column = agg.find(exp.Column)
            if column is None:
                if isinstance(agg, exp.Count):  # COUNT(*)
                    per_table[primary]["aggregations"].append({
                        "function": "COUNT", "column": "*",
                        "alias": _alias_of(agg), "query_id": query_id,
                    })
                continue
            owner = owner_of(column) or primary
            per_table[owner]["aggregations"].append({
                "function": agg_name.upper(),
                "column": column.name,
                "alias": _alias_of(agg),
                "query_id": query_id,
            })

    # Joins → column equivalences, recorded on BOTH sides
    for join in stmt.find_all(exp.Join):
        on = join.args.get("on")
        join_type = (join.side or join.kind or "INNER").upper()
        if on is None:
            continue
        for eq in on.find_all(exp.EQ):
            left, right = eq.left, eq.right
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                continue
            l_owner, r_owner = owner_of(left), owner_of(right)
            if not (l_owner and r_owner) or l_owner == r_owner:
                continue
            per_table[l_owner]["joins"].append({
                "other_table": r_owner, "left_column": left.name,
                "right_column": right.name, "join_type": join_type,
                "query_id": query_id,
            })
            per_table[r_owner]["joins"].append({
                "other_table": l_owner, "left_column": right.name,
                "right_column": left.name, "join_type": join_type,
                "query_id": query_id,
            })

    # WHERE filters → column vs literal
    for where in stmt.find_all(exp.Where):
        for predicate in where.find_all(exp.EQ, exp.NEQ, exp.GT, exp.GTE,
                                        exp.LT, exp.LTE, exp.In, exp.Like):
            candidates = (
                predicate.this,
                getattr(predicate, "expression", None),  # binary RHS; None for In
            )
            column = next(
                (c for c in candidates if isinstance(c, exp.Column)), None,
            )
            if column is None:
                continue
            owner = owner_of(column)
            if not owner:
                continue
            for literal in predicate.find_all(exp.Literal):
                per_table[owner]["filters"].append({
                    "column": column.name,
                    "value": literal.name,
                    "operator": predicate.key.upper(),
                    "is_negated": isinstance(predicate, exp.NEQ),
                    "query_id": query_id,
                })
                break  # one representative literal per predicate

    # CASE WHEN col = 'X' THEN 'Meaning' → CodeMapping candidates
    for case in stmt.find_all(exp.Case):
        for when in case.args.get("ifs") or []:
            cond, then = when.this, when.args.get("true")
            if not isinstance(cond, exp.EQ):
                continue
            column = cond.find(exp.Column)
            raw = cond.find(exp.Literal)
            if column is None or raw is None:
                continue
            owner = owner_of(column) or primary
            meaning = then.name if isinstance(then, exp.Literal) else ""
            per_table[owner]["case_whens"].append({
                "column": column.name,
                "raw_value": raw.name,
                "human_meaning": meaning,
                "query_id": query_id,
            })

    # Date functions → observed time grain
    for trunc in stmt.find_all(exp.DateTrunc, exp.TimestampTrunc):
        column = trunc.find(exp.Column)
        unit = trunc.args.get("unit")
        if column is None:
            continue
        owner = owner_of(column) or primary
        per_table[owner]["date_functions"].append({
            "column": column.name,
            "granularity": unit.name.upper() if unit is not None else "DATE",
        })


def _alias_map(stmt: "exp.Expression") -> dict[str, str]:
    """alias (or bare name) → real table name.

    Single-table CTEs are resolved to their underlying table so the
    dominant analyst pattern — ``WITH a AS (SELECT … FROM real_1),
    b AS (… FROM real_2) SELECT … FROM a JOIN b ON …`` — still yields a
    join edge between the two real tables. Multi-table CTEs stay
    unresolved (conservative: no guessing).
    """
    cte_names = {
        cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE)
    }
    cte_underlying: dict[str, str] = {}
    for cte in stmt.find_all(exp.CTE):
        inner = {
            _real_name(t) for t in cte.this.find_all(exp.Table)
            if t.name and t.name.lower() not in cte_names
        }
        if len(inner) == 1:
            cte_underlying[cte.alias_or_name.lower()] = next(iter(inner))

    mapping: dict[str, str] = {}
    for table in stmt.find_all(exp.Table):
        name = table.name
        if not name:
            continue
        key = (table.alias or name).lower()
        if name.lower() in cte_names:
            real = cte_underlying.get(name.lower())
            if real:
                mapping.setdefault(key, real)
            continue
        real = _real_name(table)
        mapping[key] = real
        mapping.setdefault(name.lower(), real)
    return mapping


def _real_name(table: "exp.Table") -> str:
    """catalog.db.name, as written."""
    prefix = ".".join(
        part.name for part in (table.args.get("catalog"),
                               table.args.get("db")) if part is not None
    )
    return f"{prefix}.{table.name}" if prefix else table.name


def _alias_of(node: "exp.Expression") -> str:
    parent = node.parent
    if isinstance(parent, exp.Alias):
        return parent.alias or ""
    return ""


def _safe_stem(table: str) -> str:
    return table.replace("/", "_").replace(".", "__")
