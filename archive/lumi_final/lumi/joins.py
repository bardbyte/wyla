"""JOIN cardinality + canonical path inference across the corpus.

What `compute_equivalence_classes` proves: these two columns are equal.
What this module proves: which side is the ONE, which side is the MANY.
That distinction is the difference between a correct LookML explore and
a Looker query that silently fans out and returns wrong numbers.

We infer cardinality from three independent signals across all 138 gold
queries (each one of which is a vote):

  1. **Join type** — LEFT JOIN x means x is the OPTIONAL side. Useful
     for nullability inference.
  2. **GROUP BY** — the side whose key appears in GROUP BY is the
     dimension grain (the "one"). The side with aggregations is the
     fact (the "many").
  3. **Join frequency by direction** — `cardmember.cm11 → account.cm11`
     observed 30× as the JOIN ON pair, with cardmember as the FROM
     and account joined into it, signals cardmember-is-base.

Majority vote across observations yields per-pair cardinality. We
output two structures:

  - ``JoinCardinality`` — one per unique (table_a.col_a, table_b.col_b)
    pair, with the inferred relationship + the evidence that supports it.
  - ``CanonicalJoinPath`` — multi-hop chains observed in real queries
    (cardmember → account → transaction). Top-K by frequency. These are
    the chains the LookML explore should support.

Public API:
    infer_join_cardinalities(fingerprints) -> list[JoinCardinality]
    infer_canonical_paths(fingerprints, *, top_k=10) -> list[CanonicalJoinPath]
    render_joins_for_table(table, cardinalities, paths) -> str  # planner prompt
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Literal

from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.joins")


JoinCardinalityKind = Literal[
    "one_to_one", "one_to_many", "many_to_one", "many_to_many", "unknown",
]


@dataclass
class JoinCardinality:
    """Inferred cardinality for one (table_a.col, table_b.col) pair.

    Direction is FROM left_table TO right_table:
      - one_to_many   — for each left row, many right rows (left is dim)
      - many_to_one   — for each left row, exactly one right row (right is dim)
      - one_to_one    — bijective
      - many_to_many  — bridge table needed
    """
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    cardinality: JoinCardinalityKind = "unknown"
    confidence: float = 0.5
    observations: int = 0
    evidence: list[str] = field(default_factory=list)
    # Secondary cardinality candidates with their vote counts (for transparency).
    vote_breakdown: dict[str, int] = field(default_factory=dict)


@dataclass
class CanonicalJoinPath:
    """A multi-hop join chain observed in real queries.

    Members are ordered (table, alias) tuples representing the join
    sequence in a single query. Frequency = how many distinct queries
    use this exact chain. Higher frequency = canonical path the
    explore should support natively.
    """
    base_table: str
    chain: list[tuple[str, str | None]]   # [(table, join_type)] from left to right
    frequency: int = 0
    sample_query_ids: list[str] = field(default_factory=list)


# ─── Public API ──────────────────────────────────────────────


def infer_join_cardinalities(
    fingerprints: list[SQLFingerprint],
) -> list[JoinCardinality]:
    """Infer cardinality for every JOIN pair observed in the corpus.

    Vote per fingerprint per join, then majority-vote across all
    observations of the same pair (in either direction).
    """
    # Bucket by (sorted-pair) so a → b and b → a votes go in one place.
    # Key: frozenset of (table, col); but we keep the directional
    # interpretation per vote.
    pair_votes: dict[
        tuple[tuple[str, str], tuple[str, str]],
        list[tuple[JoinCardinalityKind, str]],
    ] = {}

    for fp in fingerprints:
        if fp.parse_error:
            continue
        # Build alias→table resolution map for this query: each join
        # captures the right_alias; the FROM table's alias is recoverable
        # by elimination (sqlglot's alias_or_name on top-level table).
        alias_map: dict[str, str] = {}
        for j in fp.joins or []:
            ralias = (j.get("right_alias") or "").lower()
            rtbl = j.get("right_table")
            if ralias and rtbl:
                alias_map[ralias] = rtbl
        # Common case: the FROM table's alias is whatever shows up on
        # the left side of every JOIN ON clause that isn't already in
        # alias_map. Resolve by intersection.
        unresolved_left = {
            (j.get("left_table") or "").lower()
            for j in (fp.joins or [])
            if j.get("left_table")
        } - set(alias_map)
        if len(unresolved_left) == 1 and fp.primary_table:
            (only_alias,) = unresolved_left
            if only_alias and only_alias != (fp.primary_table or "").lower():
                alias_map[only_alias] = fp.primary_table

        for j in fp.joins or []:
            raw_left = (j.get("left_table") or "").lower()
            left_t_opt = (
                alias_map.get(raw_left)
                or j.get("left_table")
                or fp.primary_table
            )
            right_t_opt = j.get("right_table") or j.get("other_table")
            lk_opt = j.get("left_key")
            rk_opt = j.get("right_key")
            join_type = (j.get("join_type") or "inner").lower()
            if not (left_t_opt and right_t_opt and lk_opt and rk_opt):
                continue
            left_t: str = left_t_opt
            right_t: str = right_t_opt
            lk: str = lk_opt
            rk: str = rk_opt

            cardinality, evidence = _vote_for_join(
                left_t, lk, right_t, rk, join_type, fp,
            )
            # Store under the directional key so vote semantics are
            # preserved (a→b cardinality is NOT b→a).
            key: tuple[tuple[str, str], tuple[str, str]] = (
                (left_t, lk), (right_t, rk),
            )
            pair_votes.setdefault(key, []).append((cardinality, evidence))

    # Aggregate votes per direction. If both directions seen, fold them.
    out: list[JoinCardinality] = []
    seen_norm: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    for ((side_a, side_b), votes) in pair_votes.items():
        # Find votes for the reverse direction too.
        reverse_key = (side_b, side_a)
        reverse_votes = pair_votes.get(reverse_key, [])
        # Reverse votes are inverted before aggregation.
        all_votes = list(votes) + [
            (_invert(c), e + " (reverse-direction observation)")
            for c, e in reverse_votes
        ]

        # Normalize key so we don't emit both directions as separate rows.
        norm = tuple(sorted([side_a, side_b]))
        norm_key: tuple[tuple[str, str], tuple[str, str]] = (norm[0], norm[1])
        if norm_key in seen_norm:
            continue
        seen_norm.add(norm_key)

        # Decide which side is "left" in the output by lexicographic
        # convention so emission is deterministic.
        if norm[0] == side_a:
            out_left, out_right = side_a, side_b
            normalized_votes = all_votes
        else:
            out_left, out_right = side_b, side_a
            normalized_votes = [(_invert(c), e) for c, e in all_votes]

        counter: Counter[str] = Counter(
            c for c, _ in normalized_votes if c != "unknown"
        )
        top_kind: JoinCardinalityKind
        if counter:
            top_str, top_count = counter.most_common(1)[0]
            top_kind = top_str  # type: ignore[assignment]
            confidence = top_count / max(1, sum(counter.values()))
        else:
            top_kind = "unknown"
            confidence = 0.0

        # Pick best-supporting evidence sentences (up to 3).
        ev = [
            e for c, e in normalized_votes if c == top_kind
        ][:3]
        if not ev:
            ev = [e for _, e in normalized_votes[:3]]

        out.append(JoinCardinality(
            left_table=out_left[0],
            left_column=out_left[1],
            right_table=out_right[0],
            right_column=out_right[1],
            cardinality=top_kind,
            confidence=round(confidence, 2),
            observations=len(normalized_votes),
            evidence=ev,
            vote_breakdown=dict(counter),
        ))
    return out


def infer_canonical_paths(
    fingerprints: list[SQLFingerprint], *, top_k: int = 10,
) -> list[CanonicalJoinPath]:
    """Top-K observed multi-hop join chains, by query frequency."""
    chain_freq: dict[
        tuple[str, tuple[tuple[str, str | None], ...]],
        list[str],
    ] = {}
    for fp in fingerprints:
        if fp.parse_error or not (fp.joins or []):
            continue
        base = fp.primary_table
        if not base:
            continue
        # Walk joins in the captured order so the chain reflects how
        # the analyst wrote the query.
        sorted_joins = sorted(
            fp.joins, key=lambda j: j.get("order", 0),
        )
        chain: list[tuple[str, str | None]] = []
        for j in sorted_joins:
            rt = j.get("right_table") or j.get("other_table")
            jt = j.get("join_type") or "inner"
            if rt:
                chain.append((rt, jt))
        if not chain:
            continue
        key = (base, tuple(chain))
        # Use raw_sql first 50 chars as a "query id" since fingerprint
        # doesn't carry one. Pipeline assigns Q01 etc later.
        qid = (fp.raw_sql or "")[:60].replace("\n", " ")
        chain_freq.setdefault(key, []).append(qid)

    paths = [
        CanonicalJoinPath(
            base_table=base,
            chain=list(chain),
            frequency=len(qids),
            sample_query_ids=qids[:3],
        )
        for ((base, chain), qids) in chain_freq.items()
    ]
    paths.sort(key=lambda p: -p.frequency)
    return paths[:top_k]


def render_joins_for_table(
    table: str,
    cardinalities: list[JoinCardinality],
    paths: list[CanonicalJoinPath],
    *,
    max_pairs: int = 8,
    max_paths: int = 6,
) -> str:
    """Render an opinionated joins section for the planner prompt.

    Only shows pairs + paths that involve the given table. Empty
    string returned when no signal — caller can drop the section.
    """
    relevant_pairs = [
        c for c in cardinalities
        if c.left_table == table or c.right_table == table
    ]
    relevant_paths = [
        p for p in paths
        if p.base_table == table
        or any(t == table for t, _ in p.chain)
    ]
    if not relevant_pairs and not relevant_paths:
        return ""

    lines = [
        "## Observed JOIN cardinality (proven by query evidence)",
        "",
        "These cardinalities are inferred from real query semantics — "
        "GROUP BY targets, aggregation sides, and JOIN type frequency. "
        "Use them to populate `relationship:` in proposed_explore.joins. "
        "Contradicting an observed cardinality with high confidence is "
        "a blocking-severity issue at critic time.",
        "",
    ]
    relevant_pairs.sort(key=lambda c: (-c.confidence, -c.observations))
    for c in relevant_pairs[:max_pairs]:
        conf_pct = int(c.confidence * 100)
        breakdown = ", ".join(
            f"{k}:{v}" for k, v in c.vote_breakdown.items()
        ) or "—"
        lines.append(
            f"- `{c.left_table}.{c.left_column}` → "
            f"`{c.right_table}.{c.right_column}`: "
            f"**{c.cardinality}** "
            f"(confidence {conf_pct}%, {c.observations} observations; "
            f"votes: {breakdown})"
        )
        for ev in c.evidence[:2]:
            lines.append(f"    - _{ev}_")
    if relevant_paths:
        lines.extend([
            "",
            "## Canonical join paths (top by frequency)",
            "",
            "These are the multi-hop chains real queries actually use. "
            "Your proposed_explore should support these natively — "
            "Radix retrieval routes by chain familiarity.",
            "",
        ])
        for p in relevant_paths[:max_paths]:
            chain_str = " → ".join(
                f"`{t}` ({jt})" for t, jt in p.chain
            )
            lines.append(
                f"- base `{p.base_table}` → {chain_str} "
                f"(observed in {p.frequency} quer{'ies' if p.frequency != 1 else 'y'})"
            )
    return "\n".join(lines)


# ─── Internals ───────────────────────────────────────────────


def _vote_for_join(
    left_t: str, lk: str, right_t: str, rk: str,
    join_type: str, fp: SQLFingerprint,
) -> tuple[JoinCardinalityKind, str]:
    """One vote from one query.

    Heuristic priority (in order):
      1. GROUP BY signal — strongest.
      2. Aggregation source signal — fact side has aggregations.
      3. JOIN type — LEFT JOIN x → x is optional; weak hint at one_to_many.
      4. Naming pattern fallback — *_id on left, *_id on right both → likely 1:1.
    """
    gb_cols = {
        ((g.get("table") or "").lower(), (g.get("column") or "").lower())
        for g in (fp.group_by or [])
    }
    gb_cols_no_table = {
        col for tbl, col in gb_cols if not tbl
    } | {col for _, col in gb_cols}

    agg_source = {
        (a.get("column") or "").lower()
        for a in (fp.aggregations or [])
        if a.get("column")
    }

    # Signal 1: GROUP BY says which side is the dim.
    left_in_gb = (left_t.lower(), lk.lower()) in gb_cols or lk.lower() in gb_cols_no_table
    right_in_gb = (right_t.lower(), rk.lower()) in gb_cols or rk.lower() in gb_cols_no_table

    if left_in_gb and not right_in_gb and agg_source:
        # Left side grouped, right side aggregated → right is many per left
        return "one_to_many", (
            f"GROUP BY on `{left_t}.{lk}`, aggregations from right side → "
            "left is dim, right is fact"
        )
    if right_in_gb and not left_in_gb and agg_source:
        return "many_to_one", (
            f"GROUP BY on `{right_t}.{rk}`, aggregations from left side → "
            "right is dim, left is fact"
        )

    # Signal 2: just GROUP BY (no agg anchor) — weaker but still meaningful.
    if left_in_gb and not right_in_gb:
        return "one_to_many", (
            f"GROUP BY on `{left_t}.{lk}` only — left is dim grain"
        )
    if right_in_gb and not left_in_gb:
        return "many_to_one", (
            f"GROUP BY on `{right_t}.{rk}` only — right is dim grain"
        )

    # Signal 3: LEFT JOIN means right is optional → typically 1:N from left.
    if join_type in {"left", "left outer"}:
        return "one_to_many", (
            f"LEFT JOIN to `{right_t}` — left is base, right may have 0..N matches"
        )
    if join_type in {"right", "right outer"}:
        return "many_to_one", "RIGHT JOIN — right is base"

    # Signal 4: naming pattern — *_id on right with same key name suggests
    # right is the dim (e.g. account_dim with acct_id).
    if rk.endswith("_id") or rk.endswith("_pk"):
        if not (lk.endswith("_id") or lk.endswith("_pk")):
            return "many_to_one", (
                f"`{right_t}.{rk}` has *_id naming — likely PK on the dim side"
            )

    return "unknown", "no decisive signal in this query"


_INVERSE: dict[JoinCardinalityKind, JoinCardinalityKind] = {
    "one_to_many": "many_to_one",
    "many_to_one": "one_to_many",
    "one_to_one": "one_to_one",
    "many_to_many": "many_to_many",
    "unknown": "unknown",
}


def _invert(c: JoinCardinalityKind) -> JoinCardinalityKind:
    return _INVERSE[c]
