"""Cluster gold queries into explore-candidate groups.

The first-principles observation: an explore is a property of a
question, not a table. The 138 gold queries are evidence of question
patterns. Clustering them by query SHAPE reveals the natural set of
explores the corpus needs.

Each cluster represents a question pattern. The cluster's:
  - **base_view** = the table that's the natural fact (the one whose
    grain the GROUP BY respects + the source of aggregations)
  - **dim_views** = tables joined into the base
  - **canonical_filters** = filters appearing in >50% of the cluster's queries
  - **canonical_groupings** = the GROUP BY columns that recur

This drives Radix's coverage³ × base_view_bonus scoring directly:

  - Per-table explores have small base view, lots of joins for any
    multi-entity question → low base_view_bonus.
  - Per-question-cluster explores choose the natural fact as base →
    high base_view_bonus, fewer joins per question.

Clustering signature (the equivalence we use):

  signature = (
    sorted(tables_in_query),   # which tables touched
    sorted(group_by_keys),     # what we group by — the question's grain
    sorted(structural_filters) # mandatory / canonical filter set
  )

Two queries with the same signature answer the same question shape;
they belong in the same cluster. Frequency = number of queries in the
cluster (signal of importance).

Public API:
    cluster_queries(fingerprints, *, min_cluster_size=1) -> list[QueryCluster]
    propose_explore_for_cluster(cluster, contexts, cardinalities) -> ExploreProposal
    render_clusters_for_prompt(clusters) -> str
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.explore_clusters")


@dataclass
class QueryCluster:
    """One question-pattern cluster."""

    cluster_id: str
    tables: list[str]                # tables in the join chain (sorted)
    group_by_keys: list[str]         # GROUP BY columns (sorted)
    structural_filters: list[dict[str, Any]]   # canonical mandatory filters
    canonical_filters: list[dict[str, Any]]    # filters seen in >50%
    member_query_indices: list[int] = field(default_factory=list)
    frequency: int = 0               # number of member queries
    sample_queries: list[str] = field(default_factory=list)
    # Aggregations observed in the cluster (helps decide base view).
    aggregation_columns: list[str] = field(default_factory=list)


@dataclass
class ExploreProposal:
    """Per-cluster explore design proposal — input to ExplorePlan authoring."""

    cluster_id: str
    base_view: str
    dim_views: list[str]
    joins: list[dict[str, Any]]      # {right_table, left_key, right_key, relationship}
    always_filter: dict[str, Any]    # {col: default} from canonical mandatory filters
    canonical_groupings: list[str]
    primary_question_pattern: str    # one-liner derived from cluster shape
    base_view_bonus_estimate: float  # 1.0 - 2.0 estimate for Radix scoring
    member_frequency: int            # how many queries this serves


# ─── Public API ──────────────────────────────────────────────


def cluster_queries(
    fingerprints: list[SQLFingerprint],
    *,
    min_cluster_size: int = 1,
) -> list[QueryCluster]:
    """Cluster fingerprints by question-shape signature.

    Two queries are in the same cluster when they hit the same set of
    tables, GROUP BY the same keys, and carry the same structural filter
    set. Variability in non-structural filters (date ranges, value
    selection) is intentionally ignored — those are the question's
    parameters, not its shape.
    """
    buckets: dict[_Signature, list[int]] = defaultdict(list)

    for i, fp in enumerate(fingerprints):
        if fp.parse_error:
            continue
        sig = _signature_for_fingerprint(fp)
        if sig is None:
            continue
        buckets[sig].append(i)

    clusters: list[QueryCluster] = []
    for cluster_idx, ((tables, gb, sf), member_indices) in enumerate(
        sorted(buckets.items(), key=lambda kv: -len(kv[1])),
    ):
        if len(member_indices) < min_cluster_size:
            continue
        members = [fingerprints[i] for i in member_indices]

        # Pick the canonical-filter set: filters appearing in > 50% of
        # member queries on the same column.
        canonical = _canonical_filters(members)

        # Collect aggregation source columns for base-view inference.
        agg_cols: set[str] = set()
        for fp in members:
            for a in fp.aggregations or []:
                col = a.get("column")
                if col:
                    agg_cols.add(col)

        clusters.append(QueryCluster(
            cluster_id=f"cluster_{cluster_idx:03d}",
            tables=list(tables),
            group_by_keys=list(gb),
            structural_filters=[
                {"column": col, "value": val} for (col, val) in sf
            ],
            canonical_filters=canonical,
            member_query_indices=member_indices,
            frequency=len(member_indices),
            sample_queries=[
                (members[k].raw_sql or "")[:200]
                for k in range(min(3, len(members)))
            ],
            aggregation_columns=sorted(agg_cols),
        ))
    return clusters


def propose_explore_for_cluster(
    cluster: QueryCluster,
    contexts: dict[str, Any],
    cardinalities: list[Any] | None = None,
) -> ExploreProposal:
    """Design an explore around the cluster's question pattern.

    Base-view selection (in priority order):
      1. The table whose columns appear in cluster aggregations the most
         (the "fact" of the question).
      2. Falls back to the first table alphabetically.

    Joins: for every other cluster table, propose a join with the
    relationship inferred from corpus cardinality (uses A's
    JoinCardinality). Falls back to "many_to_one" when no signal.

    always_filter: from canonical mandatory filters + partition columns
    on the base view.
    """
    base_view = _pick_base_view(cluster, contexts)
    dim_views = [t for t in cluster.tables if t != base_view]

    # Cardinality lookup: (a, b) frozenset → JoinCardinality.
    card_lookup: dict[frozenset[str], Any] = {}
    for c in cardinalities or []:
        key = frozenset({c.left_table.lower(), c.right_table.lower()})
        if key not in card_lookup or c.confidence > card_lookup[key].confidence:
            card_lookup[key] = c

    joins: list[dict[str, Any]] = []
    for dv in dim_views:
        observed = card_lookup.get(frozenset({base_view.lower(), dv.lower()}))
        if observed and observed.cardinality != "unknown":
            # Direction: base → dv
            if observed.left_table.lower() == base_view.lower():
                rel = observed.cardinality
                lk, rk = observed.left_column, observed.right_column
            else:
                rel = {
                    "one_to_many": "many_to_one",
                    "many_to_one": "one_to_many",
                }.get(observed.cardinality, observed.cardinality)
                lk, rk = observed.right_column, observed.left_column
        else:
            rel, lk, rk = "many_to_one", "?", "?"
        joins.append({
            "right_table": dv,
            "left_key": lk,
            "right_key": rk,
            "relationship": rel,
        })

    # always_filter: structural + canonical filters specifically on base_view
    # cols, plus partition columns from the base view's MDM.
    always_filter: dict[str, Any] = {}
    for f in cluster.structural_filters:
        col = f.get("column")
        val = f.get("value")
        if col and val:
            always_filter[col] = val

    base_ctx = contexts.get(base_view)
    if base_ctx is not None:
        for col_dict in base_ctx.mdm_columns or []:
            if col_dict.get("is_partitioned") or col_dict.get(
                "partition_position"
            ):
                col_name = col_dict.get("name")
                if col_name and col_name not in always_filter:
                    # Default partition window — Radix will narrow at query time.
                    always_filter[col_name] = "30 days"

    # Primary question pattern — one-liner derived from cluster shape.
    primary_q = _generate_question_pattern(cluster, base_view)

    # base_view_bonus_estimate: 1.0 + min(1.0, n_aggs_on_base / total_aggs)
    base_aggs = [
        c for c in cluster.aggregation_columns
        if base_ctx and c in (base_ctx.columns_referenced or [])
    ]
    total_aggs = max(1, len(cluster.aggregation_columns))
    bonus = 1.0 + min(1.0, len(base_aggs) / total_aggs)

    return ExploreProposal(
        cluster_id=cluster.cluster_id,
        base_view=base_view,
        dim_views=dim_views,
        joins=joins,
        always_filter=always_filter,
        canonical_groupings=cluster.group_by_keys,
        primary_question_pattern=primary_q,
        base_view_bonus_estimate=round(bonus, 2),
        member_frequency=cluster.frequency,
    )


def build_explore_plans(
    fingerprints: list[SQLFingerprint],
    contexts: dict[str, Any],
    cardinalities: list[Any] | None = None,
    *,
    min_cluster_size: int = 2,
) -> list[Any]:
    """Top-level builder — clusters fingerprints, proposes ExplorePlans.

    Returns a list of ExplorePlan (lumi.schemas) — Pydantic models ready
    to feed into the publish stage.
    """
    from lumi.schemas import ExplorePlan as _ExplorePlan

    clusters = cluster_queries(
        fingerprints, min_cluster_size=min_cluster_size,
    )
    out: list[Any] = []
    for cluster in clusters:
        proposal = propose_explore_for_cluster(
            cluster, contexts, cardinalities=cardinalities,
        )
        explore_name = _name_for_cluster(cluster, proposal.base_view)
        out.append(_ExplorePlan(
            cluster_id=cluster.cluster_id,
            explore_name=explore_name,
            base_view=proposal.base_view,
            dim_views=proposal.dim_views,
            joins=proposal.joins,
            always_filter=proposal.always_filter,
            sql_always_where="",
            description=None,
            member_query_count=cluster.frequency,
            base_view_bonus_estimate=proposal.base_view_bonus_estimate,
        ))
    return out


def _name_for_cluster(cluster: QueryCluster, base_view: str) -> str:
    """Generate a stable, human-readable LookML name for an explore.

    Pattern: <base_view>__<cluster_suffix>. Suffix derived from the
    cluster's dominant grouping or filter when present, else the
    cluster_id.
    """
    suffix = ""
    if cluster.group_by_keys:
        suffix = "_by_" + "_".join(
            k.split(".")[-1] for k in cluster.group_by_keys[:2]
        )
    elif cluster.structural_filters:
        suffix = "_for_" + cluster.structural_filters[0].get("value", "?")
    if not suffix:
        suffix = "_" + cluster.cluster_id
    name = f"{base_view}{suffix}"
    # Sanitize for LookML identifier rules: lowercase, underscore-only.
    return "".join(
        c if c.isalnum() or c == "_" else "_" for c in name.lower()
    )


def propose_aggregate_tables(
    fingerprints: list[SQLFingerprint],
    *,
    min_query_count: int = 3,
    max_proposals: int = 6,
) -> list[dict[str, Any]]:
    """Propose ``aggregate_table:`` definitions from hot GROUP BY patterns.

    Looker's aggregate_table is a materialized rollup that answers
    queries fitting its grain in 5% of the time of the underlying query.
    The corpus tells us which GROUP BY column sets recur — those are
    the rollup candidates.

    Returns a list of proposals, each:
      {
        "name": "agg__<base_view>__<grouping_signature>",
        "base_view": str,
        "group_by": list[str],            # GROUP BY columns (sorted)
        "measures": list[str],            # aggregation source columns
        "frequency": int,
        "filters": dict,                  # canonical filters
      }

    Sorted by frequency descending. Capped at max_proposals.
    """
    pattern_buckets: dict[
        tuple[str, tuple[str, ...]],
        dict[str, Any],
    ] = {}

    for fp in fingerprints:
        if fp.parse_error or not fp.primary_table:
            continue
        # Pattern key: (base_table, sorted GROUP BY columns).
        gb_keys = tuple(sorted(
            (g.get("column") or "").lower()
            for g in (fp.group_by or [])
            if g.get("column")
        ))
        if not gb_keys:
            continue
        key = (fp.primary_table, gb_keys)
        bucket = pattern_buckets.setdefault(key, {
            "name": "",
            "base_view": fp.primary_table,
            "group_by": list(gb_keys),
            "measures": set(),
            "frequency": 0,
            "filters": {},
        })
        bucket["frequency"] += 1
        for a in fp.aggregations or []:
            col = a.get("column")
            if col:
                bucket["measures"].add(col)

    proposals = []
    for (base, gb), bucket in pattern_buckets.items():
        if bucket["frequency"] < min_query_count:
            continue
        slug = "_".join(c[:8] for c in gb[:3]) or "default"
        bucket["name"] = f"agg__{base}__{slug}"
        bucket["measures"] = sorted(bucket["measures"])
        proposals.append(bucket)
    proposals.sort(key=lambda p: -p["frequency"])
    return proposals[:max_proposals]


def render_clusters_for_prompt(
    clusters: list[QueryCluster], *, max_clusters: int = 12,
) -> str:
    """Dense Markdown summary of the corpus clusters for planner prompts.

    Used in the explore planner prompt to show the full set of question
    patterns the planner is designing for.
    """
    if not clusters:
        return ""
    lines = [
        "## Question-pattern clusters (corpus shape)",
        "",
        "These are the K distinct question shapes observed across the gold "
        "queries. Each cluster IS an explore candidate. Higher frequency = "
        "more analyst questions follow this pattern. Author one explore per "
        "cluster (or merge near-identical clusters explicitly if you have "
        "reason).",
        "",
    ]
    for cluster in clusters[:max_clusters]:
        tables_str = ", ".join(f"`{t}`" for t in cluster.tables)
        gb_str = (
            ", ".join(f"`{c}`" for c in cluster.group_by_keys[:5])
            or "(no GROUP BY)"
        )
        sf_str = (
            ", ".join(
                f"`{f['column']}={f['value']}`"
                for f in cluster.structural_filters[:3]
            )
            or "(no structural filters)"
        )
        lines.append(
            f"- **{cluster.cluster_id}** (frequency {cluster.frequency}): "
            f"tables {tables_str}; GROUP BY {gb_str}; "
            f"structural filters {sf_str}"
        )
    if len(clusters) > max_clusters:
        lines.append(f"- … and {len(clusters) - max_clusters} more clusters")
    return "\n".join(lines)


# ─── Internals ───────────────────────────────────────────────


_Signature = tuple[
    tuple[str, ...],
    tuple[str, ...],
    tuple[tuple[str, str], ...],
]


def _signature_for_fingerprint(
    fp: SQLFingerprint,
) -> _Signature | None:
    """Compute the (tables, group_by, structural_filters) signature.

    Tables: deduplicated + sorted.
    GROUP BY: column names sorted (table-qualified when available).
    Structural filters: (column, value) pairs sorted, only `is_structural`.
    """
    tables = sorted(set(fp.tables or []))
    if not tables:
        return None

    gb_keys: set[str] = set()
    for g in fp.group_by or []:
        col = (g.get("column") or "").lower()
        tbl = (g.get("table") or "").lower()
        if col:
            gb_keys.add(f"{tbl}.{col}" if tbl else col)

    sf: set[tuple[str, str]] = set()
    for f in fp.filters or []:
        if not f.get("is_structural"):
            continue
        col = (f.get("column") or "").lower()
        val = str(f.get("value") or "").strip().strip("'\"").lower()
        if col and val:
            sf.add((col, val))

    return (
        tuple(tables),
        tuple(sorted(gb_keys)),
        tuple(sorted(sf)),
    )


def _canonical_filters(members: list[SQLFingerprint]) -> list[dict[str, Any]]:
    """Filters appearing in > 50% of cluster member queries on the same col."""
    filter_counts: Counter[tuple[str, str]] = Counter()
    for fp in members:
        seen_in_query: set[tuple[str, str]] = set()
        for f in fp.filters or []:
            col = (f.get("column") or "").lower()
            val = str(f.get("value") or "").strip().strip("'\"")
            if col and val:
                seen_in_query.add((col, val))
        for k in seen_in_query:
            filter_counts[k] += 1
    threshold = max(1, len(members) // 2)
    return [
        {"column": col, "value": val, "frequency": cnt}
        for (col, val), cnt in filter_counts.items()
        if cnt > threshold
    ]


def _pick_base_view(
    cluster: QueryCluster, contexts: dict[str, Any],
) -> str:
    """Pick the table that's the natural fact for this cluster.

    Heuristic priority:
      1. The table with the most aggregation-source columns from the
         cluster (the fact side of the question).
      2. The table appearing first in the alphabetical signature
         (deterministic tiebreak).
    """
    if not cluster.tables:
        return ""
    if len(cluster.tables) == 1:
        return cluster.tables[0]

    scores: dict[str, int] = {t: 0 for t in cluster.tables}
    agg_cols = set(cluster.aggregation_columns)
    for table in cluster.tables:
        ctx = contexts.get(table)
        if ctx is None:
            continue
        # Aggregation-source columns living on this table.
        for c in (ctx.columns_referenced or []):
            if c in agg_cols:
                scores[table] += 2
        # GROUP BY columns living on this table — slight preference.
        for k in cluster.group_by_keys:
            col = k.split(".")[-1]
            if col in (ctx.columns_referenced or []):
                scores[table] += 1
    # Pick highest-scoring; alphabetical tiebreak.
    return max(scores.items(), key=lambda kv: (kv[1], -ord(kv[0][0])))[0]


def _generate_question_pattern(
    cluster: QueryCluster, base_view: str,
) -> str:
    """Derive a one-liner question pattern from the cluster's shape."""
    if not cluster.aggregation_columns and not cluster.group_by_keys:
        return f"Lookup question against `{base_view}`"
    metrics = (
        ", ".join(f"`{c}`" for c in cluster.aggregation_columns[:2])
        or "rows"
    )
    if cluster.group_by_keys:
        groups = ", ".join(f"`{k.split('.')[-1]}`" for k in cluster.group_by_keys[:2])
        return f"{metrics} by {groups} from `{base_view}`"
    return f"{metrics} from `{base_view}`"
