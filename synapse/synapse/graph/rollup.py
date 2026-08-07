"""Business-unit rollup — the segment level of the graph, derived.

Tables arrive carrying a ``business_unit`` string (MDM ownership is the
authority; the mined-measures and DMP catalogs gap-fill it). That makes
the org hierarchy a GROUP BY, not a graph level: an agent routing a
question has no node to anchor "which part of the company is this about?"

This stage lifts the axis into first-class ``BusinessUnit`` nodes:

    synapse://business_unit/<slug>
        name, description (derived), member_tables, data_domains,
        metric_count, usage totals, top_metrics, join topology,
        vocabulary (how its analysts group/filter)
    BusinessUnit —CONTAINS→ Table   (one edge per member)

Two rules keep it honest:

1. Everything on a BU node is DERIVED from member evidence. The
   description is assembled from counts the graph can prove (member
   tables, dominant data domains, mined usage, join density) — it can
   never say something no witness supports. Authored/steward BU
   descriptions can later overwrite it at human_asserted tier via the
   normal capture path.
2. Witnesses are inherited, not invented. A BU node's provenance tallies,
   per member table, the sources capable of asserting a business unit
   (MDM family, curated catalogs, usage mining, human approval) that
   actually witnessed that table — so a BU asserted by MDM across 20
   tables outranks one gap-filled by usage mining on 2.

Idempotent: previous BusinessUnit nodes and their edges are dropped and
recomputed from the current tables on every run, so append builds stay
consistent with what the tables now say.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from synapse.graph.store import (
    SOURCE_WEIGHTS,
    GraphStore,
    Node,
    canonical_uri,
)

# Sources whose testimony can plausibly have set business_unit on a table.
# bq/corpus profile physical shape, never org ownership — crediting them
# would inflate the BU node's tier with witnesses that said nothing.
_BU_ASSERTING = ("human_approval", "mdm", "collibra", "knowledge_catalog",
                 "dmp", "glossary", "usage_mined")

_TOP_DOMAINS = 5
_TOP_METRICS = 5
_TOP_VOCAB = 12
_TOP_PARTNERS = 3


def _bu_witnesses(table: Node) -> list[str]:
    """The BU-capable sources that witnessed this member table. Falls back
    to the table's single strongest witness when none qualify (synthetic
    or minimal snapshots) — the property came from *somewhere* on it."""
    present = [s for s in _BU_ASSERTING if s in table.provenance.sources]
    if present:
        return present
    if table.provenance.sources:
        return [max(table.provenance.sources,
                    key=lambda s: SOURCE_WEIGHTS.get(s, 0))]
    return []


def _describe(name: str, *, n_tables: int, sample: list[str],
              domains: list[tuple[str, int]], n_metrics: int,
              executions: int, users: int, internal_joins: int,
              external_joins: int, partners: list[str],
              company_domain: str) -> str:
    """Assemble the derived description — every clause backed by a count
    computed above it, no adjectives the evidence doesn't pay for."""
    parts = [f"{name} — {n_tables} table(s)"
             + (f" incl. {', '.join(sample[:3])}" if sample else "")]
    if company_domain:
        parts.append(f"company domain {company_domain}")
    if domains:
        parts.append("dominant data domains: " + ", ".join(
            f"{d} ({c})" for d, c in domains[:3]))
    if n_metrics:
        parts.append(f"{n_metrics} metric(s) defined on member tables")
    if executions or users:
        bits = []
        if executions:
            bits.append(f"{executions:,} mined executions")
        if users:
            bits.append(f"{users} distinct analysts")
        parts.append("usage: " + " by ".join(bits))
    if internal_joins or external_joins:
        j = f"joins: {internal_joins} within the unit"
        if external_joins:
            j += (f", {external_joins} crossing to "
                  + ", ".join(partners[:_TOP_PARTNERS]))
        parts.append(j)
    return "; ".join(parts) + "."


def rollup_business_units(store: GraphStore) -> dict[str, Any]:
    """Recompute BusinessUnit nodes + CONTAINS edges from current tables.

    Returns a report: units minted, tables grouped, tables carrying no
    business_unit (the coverage gap the next MDM crawl should close).
    """
    # 1. drop the previous rollup — derived state, never merged forward
    stale = {u for u, n in store.nodes.items()
             if n.node_type == "BusinessUnit"}
    for uri in stale:
        del store.nodes[uri]
    for edge_uri in [u for u, e in store.edges.items()
                     if e.from_uri in stale or e.to_uri in stale]:
        del store.edges[edge_uri]

    # 2. group tables by their business_unit label (verbatim — aliasing
    #    between spellings is the table-aliases map's job upstream)
    groups: dict[str, list[Node]] = {}
    ungrouped = 0
    for table in store.nodes_by_type("Table"):
        label = str(table.properties.get("business_unit") or "").strip()
        if label:
            groups.setdefault(label, []).append(table)
        else:
            ungrouped += 1

    units: list[dict[str, Any]] = []
    for label, members in sorted(groups.items()):
        members = sorted(members,
                         key=lambda t: str(t.properties.get("table_name")))
        member_uris = {t.canonical_uri for t in members}
        member_names = [str(t.properties.get("table_name") or
                            t.canonical_uri.rsplit("/", 1)[-1])
                        for t in members]

        # ── member-derived profile ──
        domains = Counter()
        for t in members:
            d = str(t.properties.get("data_domain") or "").strip()
            if d:
                domains[d] += 1
        company = Counter(
            str(t.properties.get("company_domain") or "").strip()
            for t in members
            if str(t.properties.get("company_domain") or "").strip())
        company_domain = company.most_common(1)[0][0] if company else ""

        metrics: list[Node] = []
        for t in members:
            for e in store.incoming(t.canonical_uri, "COMPUTED_FROM"):
                m = store.get(e.from_uri)
                if m is not None and m.node_type == "Metric":
                    metrics.append(m)
        executions = sum(int(m.properties.get("execution_count") or 0)
                         for m in metrics)
        users = sum(int(m.properties.get("user_count") or 0)
                    for m in metrics)
        top_metrics = [
            str(m.properties.get("business_name")
                or m.canonical_uri.rsplit("/", 1)[-1])
            for m in sorted(
                metrics,
                key=lambda m: (-int(m.properties.get("execution_count")
                                    or 0), m.canonical_uri))
        ][:_TOP_METRICS]

        vocab = Counter()
        for m in metrics:
            for key in ("group_by_patterns", "common_filters"):
                for term in m.properties.get(key) or []:
                    vocab[str(term)] += 1
        # The questions this unit's metrics answer (DMP curation) — THE
        # routing signal: an incoming question meets the curated phrasing
        # of what the unit already answers, ordered by real usage.
        question_bank = [
            q for q, _ in sorted(
                {str(m.properties.get("question_answered")):
                 int(m.properties.get("execution_count") or 0)
                 for m in metrics
                 if m.properties.get("question_answered")}.items(),
                key=lambda kv: (-kv[1], kv[0]))
        ][:10]

        internal = external = 0
        partner_units: Counter = Counter()
        for t in members:
            for e in store.outgoing(t.canonical_uri, "JOINS_WITH"):
                if e.to_uri in member_uris:
                    internal += 1
                else:
                    external += 1
                    other = store.get(e.to_uri)
                    p = str((other.properties.get("business_unit")
                             if other else "") or "").strip()
                    partner_units[p or "(unlabeled)"] += 1
            for e in store.incoming(t.canonical_uri, "JOINS_WITH"):
                if e.from_uri not in member_uris:
                    external += 1
                    other = store.get(e.from_uri)
                    p = str((other.properties.get("business_unit")
                             if other else "") or "").strip()
                    partner_units[p or "(unlabeled)"] += 1
        partners = [p for p, _ in partner_units.most_common(_TOP_PARTNERS)]

        uri = canonical_uri("business_unit", label)
        node = Node(
            canonical_uri=uri,
            node_type="BusinessUnit",
            properties={
                "name": label,
                "table_count": len(members),
                "member_tables": member_names,
                "data_domains": [
                    {"domain": d, "tables": c}
                    for d, c in domains.most_common(_TOP_DOMAINS)],
                "company_domain": company_domain,
                "metric_count": len(metrics),
                "execution_count": executions,
                "user_count": users,
                "top_metrics": top_metrics,
                "vocabulary": [t for t, _ in vocab.most_common(_TOP_VOCAB)],
                "question_bank": question_bank,
                "internal_join_edges": internal,
                "external_join_edges": external,
                "external_join_partners": partners,
                "description": _describe(
                    label, n_tables=len(members), sample=member_names,
                    domains=domains.most_common(_TOP_DOMAINS),
                    n_metrics=len(metrics), executions=executions,
                    users=users, internal_joins=internal,
                    external_joins=external, partners=partners,
                    company_domain=company_domain),
            },
        )
        # ── inherited witnesses: tally BU-capable sources per member ──
        tally: Counter = Counter()
        edge_sources: dict[str, str] = {}
        for t in members:
            witnesses = _bu_witnesses(t)
            for s in witnesses:
                tally[s] += 1
            edge_sources[t.canonical_uri] = (
                max(witnesses, key=lambda s: SOURCE_WEIGHTS.get(s, 0))
                if witnesses else "llm_generated")
        for src, count in sorted(tally.items(),
                                 key=lambda kv: -SOURCE_WEIGHTS.get(kv[0], 0)):
            node.provenance.record_source(src, count_delta=count)
        store.nodes[uri] = node

        for t in members:
            store.upsert_edge("CONTAINS", uri, t.canonical_uri, {},
                              source=edge_sources[t.canonical_uri])
        units.append({"business_unit": label, "tables": len(members),
                      "metrics": len(metrics),
                      "tier": node.provenance.confidence_tier})

    return {
        "business_units": len(units),
        "tables_grouped": sum(u["tables"] for u in units),
        "tables_without_business_unit": ungrouped,
        "units": units,
    }
