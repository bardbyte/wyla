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
    lob:<slug>                       line of business (steward/dmp-declared)
    mdom:<slug>                      metric domain (dmp metricDomain)
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
    "review": re.compile(rf"^review:{_FP}$"),        # E12/A5
    "lob": re.compile(r"^lob:[a-z0-9_\-]+$"),
    "mdom": re.compile(r"^mdom:[a-z0-9_\-]+$"),
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


_ID_SAFE = re.compile(r"[^a-z0-9_ \-]")


def concept_id(label_norm: str, physical_table: str) -> str:
    """The node id is a DERIVATION, not the label: E9 keeps the concept
    label verbatim on the record, while the id slugs every character the
    grammar forbids to ``_`` (real tribal labels carry ``/ . ::``).
    Deterministic; punctuation-only variants may fold — their provs
    both survive the fold."""
    safe = _ID_SAFE.sub("_", label_norm)
    return f"concept:{safe}@{table_id(physical_table)}"


def acr_id(symbol: str, bu: str, region: str) -> str:
    clean = lambda v, d: (v or d).strip().lower().replace("@", "_") or d
    return (f"acr:{clean(symbol, '?')}@{clean(bu, 'all')}"
            f"@{clean(region, 'all')}")


# Ids that EMBED source-provided strings slug grammar-hostile characters
# to `_` at their single mint site — same contract as concept_id: the id
# is a derivation, the verbatim value lives in props, and punctuation
# variants may fold (their provs both survive the fold).
_MGROUP_SAFE = re.compile(r"[^a-z0-9_:@\-\. ]")
_TERM_SAFE = re.compile(r"[^a-z0-9_\-]")
_OWNER_SAFE = re.compile(r"[^a-z0-9_\-\.@]")


def mgroup_id(group_key: str) -> str:
    """A mined catalog id can carry the expression text itself —
    ``…count_distinct_a_hi||a_lo`` is a real one."""
    return f"mgroup:{_MGROUP_SAFE.sub('_', group_key.lower())}"


def term_node_id(term_id: str) -> str:
    """The term node and every mapped_term edge must derive the id
    IDENTICALLY — mint here only."""
    return f"term:atlas:{_TERM_SAFE.sub('_', term_id.strip().lower())}"


def owner_id(owner: str) -> str:
    """MDM ownership values are sometimes display names, not slugs."""
    return f"owner:{_OWNER_SAFE.sub('_', owner.strip().lower())}"


_CLASS_SAFE = re.compile(r"[^a-z0-9_\-]")


def lob_id(code: str) -> str:
    """LOB identity is the slug of its CODE — "GMNS" (a dmp
    lineOfBusiness value) and "gmns" (a steward lob_map code) are the
    same node, so the two declarations corroborate instead of forking.
    A display name used as a code slugs to a DIFFERENT node — divergence
    stays visible in the graph rather than being guessed away."""
    return f"lob:{_CLASS_SAFE.sub('_', code.strip().lower())}"


def mdom_id(name: str) -> str:
    """Metric domain (dmp metricDomain) — same slug contract."""
    return f"mdom:{_CLASS_SAFE.sub('_', name.strip().lower())}"
