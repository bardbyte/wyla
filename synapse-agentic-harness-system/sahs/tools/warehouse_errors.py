"""Taught warehouse errors — the run_sql post hook (v3 §4).

A failed dry run used to reach the model as BigQuery's raw sentence
and a generic hint. That is enough when the fix is the model's (a
column name, a partition filter, a syntax slip) and useless when it
is not (a wrong project, a permission, the network). This module
classifies the message deterministically and attaches the concrete
fix:

    kind          yours_to_fix   what the hint carries
    sql           yes            the closest real names, the snippet
    cost          yes            narrow the scan
    environment   no             the .env change, the smoke command
    access        no             which table, which permission
    unknown       yes            read it; configuration → say so

No model call, no guessing: everything named comes from the build or
from the error itself.
"""

from __future__ import annotations

import difflib
import re
from typing import Any

KINDS = ("sql", "cost", "environment", "access", "unknown")

_NOT_FOUND_TABLE = re.compile(
    r"Not found: Table\s+(?:([\w-]+):)?([\w$-]+)\.([\w$-]+)"
    r"(?:\s+was not found in location\s+([\w-]+))?", re.I)
_NOT_FOUND_DATASET = re.compile(
    r"Not found: Dataset\s+(?:([\w-]+):)?([\w$-]+)"
    r"(?:\s+was not found in location\s+([\w-]+))?", re.I)
_UNRECOGNIZED = re.compile(
    r"Unrecognized name:\s*`?([\w.]+)`?"
    r"(?:;\s*Did you mean\s+`?([\w.]+)`?)?", re.I)
_NAME_NOT_INSIDE = re.compile(
    r"Name\s+`?(\w+)`?\s+not found inside\s+`?(\w+)`?", re.I)
_PARTITION = re.compile(
    r"Cannot query over table\s+'?([\w.$-]+)'?\s+without a filter over "
    r"column\(s\)\s+'?([\w', ]+?)'?\s+that can be used for partition "
    r"elimination", re.I)
_SYNTAX = re.compile(r"Syntax error:\s*(.+?)(?:\s+at \[(\d+):(\d+)\])?\s*$",
                     re.I | re.S)
_SIGNATURE = re.compile(
    r"No matching signature for (?:aggregate function|function|operator)"
    r"\s+(\S+)", re.I)
_AT = re.compile(r"\bat \[(\d+):(\d+)\]")
_ACCESS = re.compile(
    r"Access Denied|Permission denied|permission .{0,40}denied|"
    r"does not have .{0,60}permission|User does not have|Forbidden|"
    r"\b403\b|IAM", re.I)
_QUOTA = re.compile(
    r"Quota exceeded|rateLimitExceeded|Exceeded rate limits|"
    r"too many (?:requests|jobs)|\b429\b", re.I)
_COST = re.compile(
    r"bytes billed|maximumBytesBilled|Resources exceeded|"
    r"exceeded .{0,30}limit", re.I)
_TRANSPORT = re.compile(
    r"^transport:|proxy|SSL|certificate|TLS|timed out|Connection "
    r"(?:refused|reset|aborted)|Name or service not known|Temporary "
    r"failure in name resolution|EOF occurred|Bad Gateway|Service "
    r"Unavailable|\b50[234]\b|Max retries|urlopen error", re.I)


def _tables_in(build: Any, sql: str) -> list[str]:
    """The build's physical names for the tables the SQL touches."""
    physicals: list[str] = []
    raws: list[str] = []
    try:
        from sahs.canon.canonical import try_canon
        result, err = try_canon(sql)
        if err is None and result is not None:
            raws = list(result.tables)
    except Exception:                                   # noqa: BLE001
        raws = []
    if not raws:
        raws = re.findall(r"(?i)\b(?:FROM|JOIN)\s+`?([\w.$-]+)`?", sql)
    for raw in raws:
        physical = build.physical_of(raw) if hasattr(build, "physical_of") \
            else None
        if physical and physical not in physicals:
            physicals.append(physical)
    return physicals


def _columns_of(build: Any, physicals: list[str]) -> dict[str, str]:
    """column → table, over the referenced tables (all tables when the
    SQL names none the build knows)."""
    schema = getattr(build, "schema", {}) or {}
    tables = physicals or list(schema)
    out: dict[str, str] = {}
    for physical in tables:
        for column in (schema.get(physical) or {}):
            out.setdefault(str(column).lower(), physical)
    return out


def _closest(name: str, candidates: list[str], n: int = 3
             ) -> list[str]:
    name = (name or "").split(".")[-1].lower()
    return difflib.get_close_matches(name, candidates, n=n, cutoff=0.6)


def _snippet(sql: str, line: int, col: int) -> str:
    lines = sql.splitlines() or [sql]
    if not (1 <= line <= len(lines)):
        return ""
    text = lines[line - 1]
    caret = " " * max(0, col - 1) + "^"
    return f"line {line}: {text.rstrip()}\n        {caret}"


def _out(kind: str, hint: str, *, yours: bool, **extra: Any
         ) -> dict[str, Any]:
    out: dict[str, Any] = {"kind": kind, "yours_to_fix": bool(yours),
                           "hint": hint, "source": "warehouse"}
    out.update({k: v for k, v in extra.items() if v})
    return out


def teach_warehouse_error(build: Any, sql: str, message: str, *,
                          data_project: str = "",
                          location: str = "") -> dict[str, Any]:
    """Classify one warehouse failure and attach the concrete fix.
    Pure: the build, the SQL, and the message in; a dict out."""
    message = (message or "").strip()
    physicals = _tables_in(build, sql)
    schema = getattr(build, "schema", {}) or {}
    short = {t.split(".")[-1]: t for t in schema}

    m = _NOT_FOUND_TABLE.search(message)
    if m:
        project, dataset, table, loc = m.groups()
        physical = build.physical_of(f"{dataset}.{table}")
        if physical:
            where = (f"project {project}" if project else "the default "
                     "project") + (f", location {loc}" if loc else "")
            return _out(
                "environment",
                f"the warehouse looked for {dataset}.{table} in {where} "
                f"and the build knows it as {physical}, so the query "
                "was sent to the wrong project: this is configuration, "
                "not your SQL. The sandbox qualifies tables with "
                "LUMI_BQ_DATA_PROJECT (currently "
                f"{data_project or 'unset'}) and runs in BQ_LOCATION "
                f"({location or 'US'}). Tell the user to set "
                "LUMI_BQ_DATA_PROJECT to the project that hosts the "
                "tables"
                + (" and BQ_LOCATION to the dataset's location"
                   if loc else "")
                + " in the silo .env, then prove it with: python "
                f"scripts/bq_check.py --table {physical}. Do not "
                "retry until they have.",
                yours=False,
                fix_env={"LUMI_BQ_DATA_PROJECT":
                         "the project that hosts the tables",
                         "BQ_LOCATION": loc or (location or "US")},
                smoke=f"python scripts/bq_check.py --table {physical}")
        close = _closest(table, list(short))
        return _out(
            "sql",
            f"{dataset}.{table} is not a table this build knows"
            + (": did you mean " + ", ".join(short[c] for c in close)
               + "?" if close else "")
            + " — search(kind=exact) or read the table card before "
            "naming a table",
            yours=True, closest=[short[c] for c in close])

    m = _NOT_FOUND_DATASET.search(message)
    if m:
        project, dataset, loc = m.groups()
        known = any(t.startswith(f"{dataset}.") for t in schema)
        if known:
            return _out(
                "environment",
                f"the warehouse could not find dataset {dataset} in "
                f"project {project or '(default)'}"
                + (f", location {loc}" if loc else "")
                + "; the build's tables live in it, so the query went "
                "to the wrong project or location: configuration, not "
                "your SQL. Tell the user to set LUMI_BQ_DATA_PROJECT "
                "(and BQ_LOCATION if the dataset is regional) in the "
                "silo .env and prove it with scripts/bq_check.py "
                "--table; do not retry until they have.",
                yours=False,
                fix_env={"LUMI_BQ_DATA_PROJECT":
                         "the project that hosts the tables",
                         "BQ_LOCATION": loc or (location or "US")})
        datasets = sorted({t.split(".")[0] for t in schema})
        return _out(
            "sql",
            f"dataset {dataset} is not one this build uses; its tables "
            f"live in: {', '.join(datasets) or 'none'}",
            yours=True, closest=datasets[:3])

    m = _UNRECOGNIZED.search(message) or _NAME_NOT_INSIDE.search(message)
    if m:
        column = m.group(1)
        suggested = m.group(2) if m.re is _UNRECOGNIZED else ""
        candidates = _columns_of(build, physicals)
        close = _closest(column, list(candidates))
        if suggested and suggested.split(".")[-1].lower() in candidates \
                and suggested.split(".")[-1].lower() not in close:
            close.insert(0, suggested.split(".")[-1].lower())
        named = [f"{c} ({candidates[c]})" for c in close]
        scope = ", ".join(physicals) if physicals else "the build"
        return _out(
            "sql",
            f"no column {column!r} on {scope}"
            + ("; closest real columns: " + ", ".join(named)
               if named else "")
            + ". Use one of them, or read the table card "
            "(read(\"table:<name>\")) before naming a column again.",
            yours=True, closest=close)

    m = _PARTITION.search(message)
    if m:
        table, cols = m.groups()
        columns = [c.strip(" '") for c in cols.split(",") if c.strip(" '")]
        col = columns[0] if columns else "the partition column"
        return _out(
            "sql",
            f"{table} requires a filter on {', '.join(columns) or col} "
            "for partition elimination: add WHERE "
            f"{col} BETWEEN <start> AND <end> (a bounded range — the "
            "table card's grain line names the partition), then retry.",
            yours=True, closest=columns)

    m = _SIGNATURE.search(message)
    if m:
        return _out(
            "sql",
            f"type mismatch calling {m.group(1)}: the argument types do "
            "not fit — check the column types on the table card and "
            "CAST or change the function, then retry.",
            yours=True)

    m = _SYNTAX.search(message)
    if m:
        what, line, col = m.groups()
        snippet = _snippet(sql, int(line), int(col)) if line and col \
            else ""
        return _out(
            "sql",
            f"syntax error: {what.strip()[:160]}"
            + (f"\n{snippet}" if snippet else "")
            + "\nfix the SQL at that spot and retry.",
            yours=True)

    if _ACCESS.search(message):
        return _out(
            "access",
            "the service account was refused: a permission, not your "
            "SQL" + (f" (tables: {', '.join(physicals)})" if physicals
                     else "")
            + ". Tell the user which table was refused and that the "
            "SVC-ID needs bigquery.jobs.create on the query project "
            "and bigquery.tables.getData on the data project; do not "
            "retry.",
            yours=False, tables=physicals)

    if _QUOTA.search(message):
        return _out(
            "environment",
            "the warehouse refused for quota or rate reasons; this is "
            "not your SQL. Retry once after the other work; if it "
            "persists, tell the user.",
            yours=False)

    if _COST.search(message):
        return _out(
            "cost",
            "the query is too expensive for the warehouse's limits: "
            "narrow the scan — a partition filter, fewer columns, a "
            "tighter date range — and retry.",
            yours=True)

    if _TRANSPORT.search(message):
        return _out(
            "environment",
            "the warehouse could not be reached (proxy, TLS, or "
            "network): not your SQL. Tell the user to run python "
            "scripts/bq_check.py and stop retrying.",
            yours=False, smoke="python scripts/bq_check.py")

    at = _AT.search(message)
    snippet = _snippet(sql, int(at.group(1)), int(at.group(2))) if at \
        else ""
    return _out(
        "unknown",
        "read the message: if it names a column, a table, a function, "
        "or syntax, fix the SQL and retry; if it names a project, a "
        "dataset, a permission, a location, or the network, it is "
        "configuration — tell the user exactly what it says and stop."
        + (f"\n{snippet}" if snippet else ""),
        yours=True)


__all__ = ["KINDS", "teach_warehouse_error"]
