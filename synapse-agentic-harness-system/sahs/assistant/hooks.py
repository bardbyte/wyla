"""Hooks (Synapse v3 §4): the must-haves, enforced in code, named.

The design's rule: governance is never prose the model may or may not
follow; it is a deterministic check on the way in or out of a tool.
This module is the registry that makes those checks legible, plus the
one new check Stage 1 adds — the literal check on run_sql.

  artifact_schema    pre   artifact      rules 1 and 2 (artifacts.py)
  sql_gates          pre   run_sql       cost + ACL gates (sandbox.py)
  literal_check      post  run_sql       a WHERE literal not among the
                                         column's observed values comes
                                         back as a warning with the
                                         closest real ones — and a
                                         literal that is a MEANING on
                                         record names the stored code
  rows_to_workspace  post  run_sql       q<N>.json for python + check
  warehouse_errors   post  run_sql       a failed dry run or execution
                                         comes back CLASSIFIED: sql or
                                         cost is the model's to fix (the
                                         closest real names ride along);
                                         environment or access is
                                         configuration to report, with
                                         the exact .env change
  clerk_only         —     (no tool)     nothing in a chat writes truth
"""

from __future__ import annotations

import difflib
import re
from typing import Any

from sahs.tools.api import Build, sample_values

HOOKS: tuple[dict[str, str], ...] = (
    {"name": "artifact_schema", "kind": "pre", "tool": "artifact",
     "enforces": "rule 1 (disclosure) and rule 2 (watermark)"},
    {"name": "sql_gates", "kind": "pre", "tool": "run_sql",
     "enforces": "cost gates and ACL before any execution"},
    {"name": "literal_check", "kind": "post", "tool": "run_sql",
     "enforces": "filter literals against observed values"},
    {"name": "rows_to_workspace", "kind": "post", "tool": "run_sql",
     "enforces": "results saved as q<N> for python and check"},
    {"name": "warehouse_errors", "kind": "post", "tool": "run_sql",
     "enforces": "a failure comes back classified — yours to fix "
                 "(sql, cost) or configuration to report (environment, "
                 "access) — with the closest real names and the fix"},
    {"name": "clerk_only", "kind": "absent", "tool": "",
     "enforces": "no chat tool writes to the graph"},
)

_LITERAL = re.compile(
    r"(?:(\w+)\.)?(\w+)\s*(?:=|!=|<>)\s*'([^']*)'", re.IGNORECASE)
_FROM = re.compile(r"\b(?:FROM|JOIN)\s+([\w.]+)", re.IGNORECASE)


def _observed(build: Build, table: str, column: str
              ) -> tuple[list[str], list[dict[str, str]]]:
    """→ (observed values, meanings on record) for one column."""
    try:
        got = sample_values(build, table, column)
    except Exception:                               # noqa: BLE001
        return [], []
    if not isinstance(got, dict):
        return [str(v) for v in (got or [])], []
    values = [str(v.get("value") if isinstance(v, dict) else v)
              for v in (got.get("values") or [])]
    meanings = [m for m in (got.get("meanings") or [])
                if isinstance(m, dict)]
    return values, meanings


def literal_warnings(build: Build, sql: str) -> list[str]:
    """Deterministic: every ``column = 'literal'`` in the SQL whose
    column has observed values on record, but whose literal is not
    among them, yields one warning naming the closest real values.
    Columns with no recorded domain are left alone — no domain, no
    opinion."""
    tables = [build.physical_of(t) or t for t in _FROM.findall(sql)]
    tables = [t for t in tables if t in build.schema]
    warnings = []
    for _alias, column, literal in _LITERAL.findall(sql):
        for table in tables:
            observed, meanings = _observed(build, table, column)
            if not observed and not meanings:
                continue
            known = {o.lower() for o in observed} | {
                str(m.get("value", "")).lower() for m in meanings}
            if literal.lower() in known:
                break
            # the phrase is a MEANING on record, not a stored code:
            # the value lookup says which code to filter on
            meant = [m for m in meanings
                     if str(m.get("synonym", "")).lower()
                     == literal.lower()]
            if meant:
                warnings.append(
                    f"'{literal}' is the meaning of a stored code, not "
                    f"a value: filter with "
                    + " or ".join(f"{column} = '{m.get('value')}'"
                                  for m in meant)
                    + " (value lookup) and say the meaning in the "
                    "answer")
                break
            pool = observed or [str(m.get("value", "")) for m in meanings]
            close = difflib.get_close_matches(
                literal, pool, n=3, cutoff=0.3) or pool[:3]
            line = (f"'{literal}' is not among the {len(pool)} "
                    f"{'observed' if observed else 'known'} values of "
                    f"{table}.{column}; closest: "
                    + ", ".join(repr(c) for c in close))
            if meanings:
                line += "; meanings on record: " + ", ".join(
                    f"'{m.get('value')}' = {m.get('synonym')}"
                    for m in meanings[:6])
            warnings.append(line)
            break
    return warnings


__all__ = ["HOOKS", "literal_warnings"]
