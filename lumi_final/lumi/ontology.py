"""Cross-table semantic equivalence — the deterministic ontology layer.

The user's first-principles observation: when an analyst writes
``a.cm11 = b.cust_xref_id`` in a JOIN ON clause, that's a literal claim
that the two columns refer to the same business entity. Our existing
fingerprint extraction captures this PER QUERY but never propagates the
equivalence ACROSS THE WHOLE CORPUS.

By taking the transitive closure of every observed JOIN ON pair across
all 138 gold queries, we get equivalence classes for free:

  cm11 = cust_id        (some query)
  cust_id = cust_xref_id (another query)
  ⇒ {cm11, cust_id, cust_xref_id} are the same entity

Surfaced into:
  - ColumnUsageProfile.observed_equivalences in grounding signals
  - TableNarrative.column_equivalences for the prompt
  - The plan-stage prompt's "Cross-table equivalences" section

The LLM-authored Domain Ontology pass (lumi.ontology_builder) builds
on top of these deterministic equivalences with semantic clustering;
this module is the deterministic floor that always works.

Public API:
    compute_equivalence_classes(all_fingerprints) -> EquivalenceMap
    equivalences_for(table, column, eq_map) -> list[(table, column)]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.ontology")


_ColumnRef = tuple[str, str]  # (table_name, column_name)


@dataclass
class EquivalenceClass:
    """One connected component of equivalent columns.

    Built from the transitive closure of JOIN ON pairs. Members are
    (table, column) tuples; the count is how many distinct queries
    contributed to forming this class (signal strength).
    """
    members: frozenset[_ColumnRef]
    query_count: int = 0
    representative: _ColumnRef | None = None  # the most-frequently-joined member

    def __len__(self) -> int:
        return len(self.members)


@dataclass
class EquivalenceMap:
    """All discovered equivalence classes, indexed for lookup."""
    classes: list[EquivalenceClass] = field(default_factory=list)
    column_to_class: dict[_ColumnRef, EquivalenceClass] = field(default_factory=dict)

    def equivalences_for(self, table: str, column: str) -> list[_ColumnRef]:
        """Return all OTHER (table, column) pairs equivalent to this one,
        sorted alphabetically by table then column.
        """
        ec = self.column_to_class.get((table, column))
        if ec is None:
            return []
        others = [m for m in ec.members if m != (table, column)]
        return sorted(others)

    def class_size_for(self, table: str, column: str) -> int:
        ec = self.column_to_class.get((table, column))
        return len(ec) if ec else 1


# ─── Public API ──────────────────────────────────────────────


def compute_equivalence_classes(
    all_fingerprints: list[SQLFingerprint],
) -> EquivalenceMap:
    """Build equivalence classes from every JOIN ON pair across the corpus.

    Algorithm:
      1. For each fingerprint, walk fp.joins and extract pairs of
         (left_table.left_key, right_table.right_key). The "left_table"
         is the FROM-table (fp.primary_table) when the join's left_table
         is just an alias; recover the real name from fp.tables.
      2. Build undirected graph where nodes are (table, column) tuples
         and edges connect each observed pair.
      3. Compute connected components via union-find.
      4. Track which queries produced each pair to weight class strength.

    Returns:
        EquivalenceMap with classes + column→class lookup.
    """
    # Union-find disjoint set, keyed by (table, column).
    parent: dict[_ColumnRef, _ColumnRef] = {}
    rank: dict[_ColumnRef, int] = {}
    edges_observed: set[tuple[_ColumnRef, _ColumnRef]] = set()
    edge_query_count: dict[frozenset[_ColumnRef], int] = {}

    def _find(node: _ColumnRef) -> _ColumnRef:
        # Path-compression union-find.
        while parent.get(node, node) != node:
            parent[node] = parent.get(parent[node], parent[node])
            node = parent[node]
        return node

    def _union(a: _ColumnRef, b: _ColumnRef) -> None:
        ra, rb = _find(a), _find(b)
        if ra == rb:
            return
        # Union by rank for shallow trees.
        if rank.get(ra, 0) < rank.get(rb, 0):
            ra, rb = rb, ra
        parent[rb] = ra
        if rank.get(ra, 0) == rank.get(rb, 0):
            rank[ra] = rank.get(ra, 0) + 1

    for fp in all_fingerprints:
        if fp.parse_error:
            continue
        from_table = fp.primary_table
        # Build a set of real tables present in this fingerprint so we
        # can map alias names (`a`, `b`) to real tables when possible.
        for j in fp.joins or []:
            right_t = j.get("right_table") or j.get("other_table")
            left_k = j.get("left_key")
            right_k = j.get("right_key")
            if not (right_t and left_k and right_k):
                continue
            # Determine the LEFT side's real table:
            # - if left_table is an alias (matches a known table in fp.tables, fine),
            # - if left_table is None or a 1-char alias, fall back to primary_table.
            left_t_alias = j.get("left_table")
            if left_t_alias and left_t_alias in (fp.tables or []):
                left_t = left_t_alias
            elif from_table:
                left_t = from_table
            else:
                continue

            if left_t == right_t:
                # Self-join — record the equivalence anyway; same column
                # on the same table is trivially equivalent.
                continue

            a: _ColumnRef = (left_t, left_k)
            b: _ColumnRef = (right_t, right_k)
            # Initialize union-find nodes.
            for n in (a, b):
                parent.setdefault(n, n)
                rank.setdefault(n, 0)
            _union(a, b)
            edges_observed.add((a, b))
            edge_key = frozenset({a, b})
            edge_query_count[edge_key] = edge_query_count.get(edge_key, 0) + 1

    # Build classes from the union-find result.
    components: dict[_ColumnRef, list[_ColumnRef]] = {}
    for node in parent:
        root = _find(node)
        components.setdefault(root, []).append(node)

    classes: list[EquivalenceClass] = []
    column_to_class: dict[_ColumnRef, EquivalenceClass] = {}

    for root, members in components.items():
        if len(members) < 2:
            continue  # singletons aren't equivalence classes
        # Query strength: sum of edge counts within this component.
        member_set = frozenset(members)
        strength = 0
        for edge_set, count in edge_query_count.items():
            if edge_set <= member_set:
                strength += count
        # Representative: the most-frequently-joined member.
        member_join_counts: dict[_ColumnRef, int] = {m: 0 for m in members}
        for edge_set, count in edge_query_count.items():
            if edge_set <= member_set:
                for m in edge_set:
                    member_join_counts[m] = member_join_counts.get(m, 0) + count
        rep = max(member_join_counts.items(), key=lambda kv: kv[1])[0]

        ec = EquivalenceClass(
            members=member_set,
            query_count=strength,
            representative=rep,
        )
        classes.append(ec)
        for m in members:
            column_to_class[m] = ec

    # Sort by class strength descending (more queries = stronger evidence).
    classes.sort(key=lambda c: -c.query_count)

    logger.info(
        "computed %d cross-table equivalence classes covering %d columns",
        len(classes), len(column_to_class),
    )
    return EquivalenceMap(classes=classes, column_to_class=column_to_class)


def equivalences_for(
    table: str, column: str, eq_map: EquivalenceMap,
) -> list[_ColumnRef]:
    """Convenience wrapper around EquivalenceMap.equivalences_for."""
    return eq_map.equivalences_for(table, column)


# ─── Render to Markdown for the prompt ───────────────────────


def render_equivalence_classes_for_table(
    table_name: str,
    eq_map: EquivalenceMap,
    *,
    max_classes: int = 15,
) -> str:
    """Render equivalence classes that touch the given table as Markdown.

    Used inside the table narrative to show Gemini which columns on
    THIS table are semantically equivalent to columns on OTHER tables.
    Critical for solving the cardmember/customer ontology problem
    when those entities aren't named consistently.
    """
    relevant = [
        ec for ec in eq_map.classes
        if any(m[0] == table_name for m in ec.members)
    ]
    if not relevant:
        return ""

    lines = [
        "### Cross-table semantic equivalences (from observed JOIN ON pairs)",
        "",
        "These columns refer to the SAME business entity across tables, "
        "established by literal `JOIN ... ON x = y` claims in the gold "
        "queries. When you describe one, mention the equivalence so "
        "downstream NL questions can resolve names interchangeably.",
        "",
    ]
    for ec in relevant[:max_classes]:
        # Show our table's column first, then the equivalents on others.
        ours = sorted(m for m in ec.members if m[0] == table_name)
        theirs = sorted(m for m in ec.members if m[0] != table_name)
        if not ours:
            continue
        for our_col in ours:
            equivalents = ", ".join(f"`{t}.{c}`" for t, c in theirs)
            lines.append(
                f"- `{our_col[1]}` ≡ {equivalents} "
                f"_(class strength: {ec.query_count} JOIN ON observation"
                f"{'s' if ec.query_count != 1 else ''})_"
            )
    return "\n".join(lines)
