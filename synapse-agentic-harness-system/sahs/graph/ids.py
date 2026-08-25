"""Stable deterministic IDs — the L2 grammar (pinned).

Never auto-increment, never UUID: an ID is a pure function of the entity,
so two runs producing the same fact produce the same line and diffs mean
what they appear to mean.

    table:<dataset>.<table>          project lives in prov, not the id
    col:<dataset>.<table>.<column>
    pred:<fp12>   tmpl:<fp12>        canonical-AST fingerprints
    metric:<fp12>                    fp(expr + grain + entity)
    mgroup:<key>                     dmp/gmns id, else label@entity
    concept:<label_norm>@table:<dataset>.<table>
    term:atlas:<id>
    acr:<symbol>@<bu>@<region>       missing scope → all
    skill:<pack>
    schema:<dataset>.<table>@v<n>
    domain:<dataset>.<table>.<column>   low-cardinality value domain
    status:<state>                   governance lattice states (E7)
    doc:<slug>   run:<run_id>   policy:<slug>   owner:<slug>
"""

from __future__ import annotations

import re

_FP = r"[0-9a-f]{12}"
_NAME = r"[a-z0-9_\-]+"
_DOTTED = rf"{_NAME}(\.{_NAME})+"

ID_PATTERNS: dict[str, re.Pattern] = {
    "table": re.compile(rf"^table:{_DOTTED}$"),
    "col": re.compile(rf"^col:{_NAME}(\.{_NAME}){{2,}}$"),
    "pred": re.compile(rf"^pred:{_FP}$"),
    "tmpl": re.compile(rf"^tmpl:{_FP}$"),
    "metric": re.compile(rf"^metric:{_FP}$"),
    "mgroup": re.compile(r"^mgroup:[a-z0-9_:@\-\. ]+$"),
    "concept": re.compile(rf"^concept:[a-z0-9_ \-]+@table:{_DOTTED}$"),
    "term": re.compile(r"^term:atlas:[a-z0-9_\-]+$"),
    "acr": re.compile(r"^acr:[^@]+@[^@]+@[^@]+$"),
    "skill": re.compile(r"^skill:[A-Za-z0-9_\-]+$"),
    "schema": re.compile(rf"^schema:{_DOTTED}@v\d+$"),
    "domain": re.compile(rf"^domain:{_NAME}(\.{_NAME}){{2,}}$"),
    "status": re.compile(r"^status:[a-z_]+$"),
    "doc": re.compile(r"^doc:[a-z0-9_\-]+$"),
    "run": re.compile(r"^run:[A-Za-z0-9_\-]+$"),
    "policy": re.compile(r"^policy:[a-z0-9_\-]+$"),
    "owner": re.compile(r"^owner:[a-z0-9_\-\.@]+$"),
}

# E7 governance lattice — legal transitions, anything else blocks.
STATUS_STATES = ("mined", "team_candidate", "pending", "certified",
                 "rejected", "deprecated", "retracted")
LEGAL_TRANSITIONS: dict[str, set[str]] = {
    "mined": {"team_candidate", "retracted"},
    "team_candidate": {"certified", "rejected", "retracted"},
    "pending": {"certified", "rejected", "retracted"},
    "certified": {"deprecated", "retracted"},
    "rejected": {"retracted"},
    "deprecated": {"retracted"},
}


def kind_of(node_id: str) -> str | None:
    prefix = node_id.split(":", 1)[0]
    pattern = ID_PATTERNS.get(prefix)
    if pattern and pattern.match(node_id):
        return prefix
    return None


def table_id(physical: str) -> str:
    return f"table:{physical.strip().lower()}"


def col_id(physical_table: str, column: str) -> str:
    return f"col:{physical_table.strip().lower()}.{column.strip().lower()}"


def concept_id(label_norm: str, physical_table: str) -> str:
    return f"concept:{label_norm}@{table_id(physical_table)}"


def acr_id(symbol: str, bu: str, region: str) -> str:
    clean = lambda v, d: (v or d).strip().lower().replace("@", "_") or d
    return (f"acr:{clean(symbol, '?')}@{clean(bu, 'all')}"
            f"@{clean(region, 'all')}")
