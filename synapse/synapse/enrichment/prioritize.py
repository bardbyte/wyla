"""Column priority scoring — the triage that decides which columns earn an
LLM call, and which stay grounded-by-BQ/MDM without one.

Pure, deterministic, over graph signals + the S1 primitives (signals.py).
No LLM, no network. This is the front door to enrichment: at 3,000+
columns per table you cannot describe everything, so score every column
from what we already know and spend the budget top-down.

Buckets, highest priority first:
  identifier — a key (declared or inferred) → the entity layer
  salient    — a human analyst actually queries it (corpus usage)
  coded      — low-card categorical whose *values* need decoding
  gap        — MDM-undescribed but with evidence, or computed (derived_logic)
  skip       — a grounded numeric/no-signal column; no LLM, not blank

``select_for_enrichment`` turns a ranking into the columns to send, always
keeping identifiers (few, and they build entities) even under a tight
budget. S3 wires this into the enricher as its ``skip_columns`` complement.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from synapse.enrichment.signals import cross_table_column_counts, key_score
from synapse.graph.store import GraphStore, canonical_uri

_TIER_RANK = {"identifier": 4, "salient": 3, "coded": 2, "gap": 1, "skip": 0}


@dataclass
class ColumnPriority:
    column: str
    score: float
    tier: str
    reasons: list[str] = field(default_factory=list)


def _stringish(data_type: str | None) -> bool:
    dt = (data_type or "").upper()
    return "STRING" in dt or "CHAR" in dt or "TEXT" in dt


def _score_column(
    node, row_count: int | None, cross_counts: dict[str, int],
) -> ColumnPriority:
    p = node.properties
    name = node.canonical_uri.rsplit("/", 1)[-1]
    declared = bool(p.get("is_primary") or p.get("is_foreign_key"))

    bucket: dict[str, float] = {}
    reasons: list[str] = []

    # identifier — declared or inferred (S1 key inference)
    ks, kr = key_score(
        approx_distinct=p.get("approx_distinct"), row_count=row_count,
        null_fraction=p.get("null_fraction"), name=name,
        data_type=p.get("data_type") or "",
        cross_table_count=cross_counts.get(name.lower(), 0),
        declared=declared)
    if ks >= 0.5:
        bucket["identifier"] = round(0.70 + 0.30 * ks, 3)
        reasons.append("identifier (" + "; ".join(kr) + ")")

    # salient — a human analyst queries it (usage set by corpus ingestion;
    # made analyst-only upstream). reference_count + role flags.
    refs = int(p.get("reference_count") or 0)
    is_join = bool(p.get("is_join_key"))
    is_filter = bool(p.get("is_filter"))
    is_group = bool(p.get("is_group_by"))
    if refs or is_join or is_filter or is_group:
        s = (0.50 + min(0.30, 0.04 * refs)
             + (0.08 if is_join else 0.0)
             + (0.05 if (is_filter or is_group) else 0.0))
        bucket["salient"] = round(min(0.95, s), 3)
        bits = ([f"refs={refs}"] if refs else []) \
            + (["join"] if is_join else []) \
            + (["filter"] if is_filter else []) \
            + (["group-by"] if is_group else [])
        reasons.append("analyst-used (" + ", ".join(bits) + ")")

    # coded — a low-card categorical whose values carry meaning; BQ gave it
    # top-values (distinct_sample) or the profile says low cardinality
    coded = (bool(p.get("distinct_sample"))
             or (p.get("cardinality_bucket") == "low"
                 and _stringish(p.get("data_type"))))
    if coded:
        bucket["coded"] = 0.55
        reasons.append("coded/categorical (decode values)")

    # gap — MDM said nothing but there is evidence; or the column is
    # computed and its logic is worth explaining
    described = bool((p.get("description") or "").strip())
    has_evidence = bool(refs or is_join or is_filter or is_group or coded
                        or p.get("derived_logic"))
    if p.get("derived_logic"):
        bucket["gap"] = max(bucket.get("gap", 0.0), 0.50)
        reasons.append("computed (derived_logic → explain)")
    if not described and has_evidence:
        bucket["gap"] = max(bucket.get("gap", 0.0), 0.40)
        reasons.append("MDM-undescribed but has evidence")

    if not bucket:
        return ColumnPriority(
            name, 0.0, "skip",
            ["grounded-only (type/range/nulls; no LLM-worthy signal)"])
    tier = max(bucket, key=lambda b: (_TIER_RANK[b], bucket[b]))
    return ColumnPriority(name, round(max(bucket.values()), 3), tier, reasons)


def prioritize_columns(
    store: GraphStore, table_name: str,
    *, cross_counts: dict[str, int] | None = None,
) -> list[ColumnPriority]:
    """Rank a table's columns by enrichment priority (desc). Reads row_count
    from the table node and cross-table co-occurrence from the whole graph
    (computed once if not supplied)."""
    if cross_counts is None:
        cross_counts = cross_table_column_counts(store)
    t = store.get(canonical_uri("table", table_name))
    row_count = t.properties.get("row_count") if t else None
    out: list[ColumnPriority] = []
    for edge in store.outgoing(canonical_uri("table", table_name), "CONTAINS"):
        node = store.get(edge.to_uri)
        if node is not None:
            out.append(_score_column(node, row_count, cross_counts))
    out.sort(key=lambda pr: (-pr.score, pr.column))
    return out


def select_for_enrichment(
    priorities: list[ColumnPriority],
    *, max_columns: int | None = None, min_score: float = 0.40,
) -> list[str]:
    """The columns to actually enrich: every identifier (always — they build
    entities and there are few), then the highest-scoring remainder above
    ``min_score`` until ``max_columns`` is reached. Returns column names."""
    identifiers = [p for p in priorities if p.tier == "identifier"]
    rest = sorted(
        (p for p in priorities
         if p.tier != "identifier" and p.score >= min_score),
        key=lambda p: -p.score)
    chosen = list(identifiers)
    if max_columns is not None:
        chosen += rest[:max(0, max_columns - len(chosen))]
    else:
        chosen += rest
    return [p.column for p in chosen]
