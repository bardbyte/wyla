"""Signal-quality primitives for enrichment prioritization.

Pure functions over data we already ingest (graph nodes, usage blobs,
sample queries) — no LLM, no network. They make the priority score
trustworthy by filtering two things the naive approach gets wrong:

  1. **Querier provenance** — only a human corporate analyst's query is
     evidence of analytical salience; a service account running the same
     load a thousand times is not. ``classify_querier`` separates the two;
     operational traffic is kept (lineage / DQ / table-importance), just
     not counted as an analyst vote.

  2. **Identifier inference** — most real tables declare no primary key, so
     we infer key-ness from the data profile (uniqueness ratio + null
     fraction) plus name priors and cross-table co-occurrence. An inferred
     key is never laundered into a declared one.

Everything here is deterministic and cheap; the LLM budget is spent only
on the head these functions surface.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from synapse.graph.store import GraphStore

# American Express corporate domain(s). A human address on one of these is
# an analyst; everything else is operational. Extend via the corp_domains
# argument for other tenants.
_CORP_DOMAINS = frozenset({"axp.com", "aexp.com"})

# service-account-shaped local parts (before the @) — operational even on a
# corp domain (e.g. svc-lumi-etl@axp.com)
_SERVICE_LOCAL = re.compile(
    r"^(svc|sa|srv|prj|etl|batch|job|jobs|pipeline|loader|load|airflow|"
    r"composer|dataflow|dbt|informatica|abinitio|system|automation|bot)"
    r"[-_.0-9]",
    re.IGNORECASE,
)

# identifier name suffixes (keys) — deliberately NOT _cd/_code, which are
# CODED columns (a different priority bucket)
_ID_SUFFIXES = (
    "_id", "_no", "_nbr", "_num", "_key", "_sk", "_xref", "_uuid",
    "_guid", "_hash", "_pk",
)
_ID_EXACT = frozenset({"id", "pk", "cm11"})


def classify_querier(
    email: str | None, *, corp_domains: frozenset[str] = _CORP_DOMAINS,
) -> str:
    """``"analyst"`` (a human corporate email → votes on salience) or
    ``"operational"`` (service account / external / unknown → kept but does
    not vote).

    Rules, strict by default:
      * no ``@`` → operational
      * any ``*.gserviceaccount.com`` (GCP service account) → operational
      * domain not a corporate domain (exact or subdomain) → operational
      * corporate domain but a service-shaped local part → operational
      * otherwise → analyst
    """
    e = (email or "").strip().lower()
    if "@" not in e:
        return "operational"
    local, _, domain = e.partition("@")
    if not local or not domain:
        return "operational"
    if "gserviceaccount.com" in domain:
        return "operational"
    is_corp = any(domain == d or domain.endswith("." + d) for d in corp_domains)
    if not is_corp:
        return "operational"            # external / partner / unknown domain
    if _SERVICE_LOCAL.match(local):
        return "operational"            # svc-*/prj-*/etl-* even on corp domain
    return "analyst"


def is_analyst(email: str | None, **kw: Any) -> bool:
    return classify_querier(email, **kw) == "analyst"


def split_queries_by_querier(
    queries: list[dict[str, Any]] | None,
    *,
    email_key: str = "user_email",
    corp_domains: frozenset[str] = _CORP_DOMAINS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Partition query records into ``(analyst, operational)`` by querier.

    Only the analyst list should feed column-salience; the operational list
    is retained for lineage / DQ / table-importance."""
    analyst: list[dict[str, Any]] = []
    operational: list[dict[str, Any]] = []
    for q in queries or []:
        email = q.get(email_key) if isinstance(q, dict) else None
        target = (analyst if classify_querier(email, corp_domains=corp_domains)
                  == "analyst" else operational)
        target.append(q)
    return analyst, operational


def looks_like_identifier(name: str | None) -> bool:
    """Name-based prior that a column is a key/identifier (not proof — one
    input to ``key_score``)."""
    n = (name or "").strip().lower()
    if not n:
        return False
    if n in _ID_EXACT or n.endswith(_ID_SUFFIXES):
        return True
    # cmNN-class account keys (cm11, cm15, …)
    return bool(re.fullmatch(r"cm\d{1,3}", n))


# an entity key has MANY distinct values (a code has few; this floor keeps
# low-cardinality categoricals out of the key signal)
_ENTITY_KEY_MIN_DISTINCT = 10_000
# continuous-measure types are (almost) never identifiers, however many
# distinct values they carry
_MEASURE_TYPES = ("FLOAT", "NUMERIC", "DECIMAL", "DOUBLE", "REAL", "BIGNUMERIC")


def key_score(
    *,
    approx_distinct: int | None = None,
    row_count: int | None = None,
    null_fraction: float | None = None,
    name: str = "",
    data_type: str = "",
    cross_table_count: int = 0,
    declared: bool = False,
) -> tuple[float, list[str]]:
    """Likelihood (0..1) that a column is an identifier ABSENT a declared
    key, plus the reasons — so the enricher can propose an entity and the
    provenance can say *why* (inferred, not declared).

    A declared key short-circuits to 1.0. Otherwise:
      * ~unique & non-null ⇒ a row-level primary key.
      * many distinct but NOT unique (and not a numeric measure) ⇒ an
        entity key with many rows each — the history-table case; keyed on
        absolute distinct, not ratio, so a low-card code never qualifies.
      * name priors and cross-table co-occurrence reinforce either.
    """
    if declared:
        return 1.0, ["declared key"]
    is_measure = any(t in (data_type or "").upper() for t in _MEASURE_TYPES)
    score = 0.0
    reasons: list[str] = []
    if approx_distinct and row_count and row_count > 0:
        ratio = approx_distinct / row_count
        if ratio >= 0.98:
            score += 0.50
            reasons.append(f"~unique ({ratio:.2f})")
        elif approx_distinct >= _ENTITY_KEY_MIN_DISTINCT and not is_measure:
            score += 0.25
            reasons.append(f"high-cardinality ({approx_distinct} distinct)")
    if null_fraction is not None and null_fraction <= 0.01:
        score += 0.15
        reasons.append("non-null")
    if looks_like_identifier(name):
        score += 0.25
        reasons.append("identifier name")
    if cross_table_count >= 2:
        score += 0.20
        reasons.append(f"shared across {cross_table_count} tables")
    return round(min(score, 1.0), 3), reasons


def cross_table_column_counts(store: GraphStore) -> dict[str, int]:
    """``column_name → number of distinct tables that contain it``.

    A key appearing in several tables is the backbone of an entity; this is
    the cheap cross-table co-occurrence signal ``key_score`` consumes."""
    tables_by_col: dict[str, set[str]] = defaultdict(set)
    for node in store.nodes_by_type("Column"):
        col = node.canonical_uri.rsplit("/", 1)[-1].lower()
        table = str(node.properties.get("table_name") or "").lower()
        if col and table:
            tables_by_col[col].add(table)
    return {col: len(tables) for col, tables in tables_by_col.items()}
