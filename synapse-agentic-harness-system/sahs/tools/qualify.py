"""Table qualification: the graph says ``dw.gms_transaction``; the
warehouse wants ``axp-lumi.dw.gms_transaction``.

The cards, the digest, and every tool name tables ``dataset.table``,
and the model writes SQL in that vocabulary. BigQuery resolves a
two-part name against the project that RUNS the query — which on the
laptop is the billing project (prj-p-lumi-gpt), not the one that
hosts the data (axp-lumi). So the sandbox qualifies every table the
build knows with the data project before any dry run or execution.
Deterministic, in code, never a prose rule the model has to follow:
a query written from the cards resolves where the data lives.

Already-qualified names pass through untouched; names the build does
not know are left for the validator to refuse with a real hint.
"""

from __future__ import annotations

import re
from typing import Any

_FROM_JOIN = re.compile(r"(?i)\b(FROM|JOIN)(\s+)(`?)([\w.$-]+)(`?)")


def qualify_tables(sql: str, build: Any, data_project: str
                   ) -> tuple[str, list[dict[str, str]]]:
    """→ (sql to send, [{"from": raw, "to": project.dataset.table}]).
    No data project, or nothing to change: the SQL comes back as it
    was, with an empty change list."""
    data_project = (data_project or "").strip().replace("`", "")
    if not data_project or not sql.strip():
        return sql, []
    try:
        import sqlglot
        from sqlglot import expressions as exp
        tree = sqlglot.parse_one(sql, read="bigquery")
    except Exception:                                   # noqa: BLE001
        return _qualify_textually(sql, build, data_project)
    cte_names = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    changes: list[dict[str, str]] = []
    for table in tree.find_all(exp.Table):
        if table.args.get("catalog") is not None:
            continue                                # project already named
        raw = ".".join(p for p in (table.text("db"), table.name) if p)
        if not raw or raw.lower() in cte_names:
            continue
        physical = build.physical_of(raw)
        if physical is None:
            continue                                # the validator's call
        dataset, _, name = physical.rpartition(".")
        table.set("catalog", exp.to_identifier(data_project, quoted=True))
        table.set("db", exp.to_identifier(dataset) if dataset else None)
        table.set("this", exp.to_identifier(name))
        changes.append({"from": raw, "to": f"{data_project}.{physical}"})
    if not changes:
        return sql, []
    try:
        return tree.sql(dialect="bigquery"), changes
    except Exception:                                   # noqa: BLE001
        return _qualify_textually(sql, build, data_project)


def _qualify_textually(sql: str, build: Any, data_project: str
                       ) -> tuple[str, list[dict[str, str]]]:
    """The fallback when the SQL will not round-trip through the
    parser: rewrite the FROM/JOIN targets in place."""
    changes: list[dict[str, str]] = []

    def swap(match: re.Match) -> str:
        raw = match.group(4)
        if raw.count(".") >= 2:
            return match.group(0)
        physical = build.physical_of(raw)
        if physical is None:
            return match.group(0)
        changes.append({"from": raw, "to": f"{data_project}.{physical}"})
        return (f"{match.group(1)}{match.group(2)}"
                f"`{data_project}.{physical}`")

    return _FROM_JOIN.sub(swap, sql), changes


__all__ = ["qualify_tables"]
