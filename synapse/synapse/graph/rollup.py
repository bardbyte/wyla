"""Company-domain rollup — the domain layer ON TOP of the graph, derived.

Tables arrive carrying org labels (``business_unit`` from MDM ownership —
authoritative; ``company_domain``; catalog gap-fill). Those labels stay
exactly where the witnesses put them. This stage builds the LAYER above:

    synapse://domain/<slug>          node_type "Domain"
        name, description (derived or steward), member profile
    Domain —CONTAINS→ Table          one edge PER MEMBERSHIP

Membership is edge-based, so it can express what a single property never
could: **overlap** — one table in several domains, each membership
independently witnessed. Two witness families coexist on the layer:

  derived   an edge minted here from the label(s) a table carries,
            provenance inherited from the table's BU-capable witnesses.
            Recomputed from current tables on every run.
  steward   an edge minted by the --domain-tags loader at
            ``human_approval``. Survives recomputes verbatim — the
            rollup extracts steward facts before rebuilding and lays
            them back down, so the human map and the machine labels sit
            side by side without either overwriting the other.

Node honesty rules (unchanged from the first rollup): the derived
description is assembled only from counts the graph can prove; steward
prose (``description_by: "steward"``) outranks it and is preserved;
witnesses are inherited, never invented. Idempotent by construction:
every run extracts steward facts → rebuilds the whole layer → same
input, same layer.
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

# Sources whose testimony can plausibly have set an org label on a table.
# bq/corpus profile physical shape, never org ownership — crediting them
# would inflate the domain node's tier with witnesses that said nothing.
_BU_ASSERTING = ("human_approval", "mdm", "collibra", "knowledge_catalog",
                 "dmp", "glossary", "usage_mined")

_TOP_DOMAINS = 5
_TOP_METRICS = 5
_TOP_VOCAB = 12
_TOP_PARTNERS = 3
_TOP_QUESTIONS = 10


def _bu_witnesses(table: Node) -> list[str]:
    """The BU-capable sources that witnessed this member table. Falls back
    to the table's single strongest witness when none qualify (synthetic
    or minimal snapshots) — the label came from *somewhere* on it."""
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
              shared: list[tuple[str, list[str]]]) -> str:
    """Assemble the derived description — every clause backed by a count
    computed above it, no adjectives the evidence doesn't pay for."""
    parts = [f"{name} — {n_tables} table(s)"
             + (f" incl. {', '.join(sample[:3])}" if sample else "")]
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
        j = f"joins: {internal_joins} within the domain"
        if external_joins:
            j += (f", {external_joins} crossing to "
                  + ", ".join(partners[:_TOP_PARTNERS]))
        parts.append(j)
    if shared:
        others = sorted({d for _, ds in shared for d in ds})
        parts.append(f"{len(shared)} table(s) shared with "
                     + ", ".join(others[:3]))
    return "; ".join(parts) + "."


def _extract_steward_facts(store: GraphStore) -> dict[str, dict[str, Any]]:
    """Pull the human-approved facts off the current layer before it is
    rebuilt: per domain, the steward's description (if any) and the set
    of member-table URIs whose edge carries human_approval."""
    facts: dict[str, dict[str, Any]] = {}
    for node in store.nodes_by_type("Domain"):
        name = str(node.properties.get("name", ""))
        if not name:
            continue
        entry: dict[str, Any] = {"description": "", "members": set()}
        if node.properties.get("description_by") == "steward":
            entry["description"] = str(
                node.properties.get("description", ""))
        for e in store.outgoing(node.canonical_uri, "CONTAINS"):
            if "human_approval" in e.provenance.sources:
                entry["members"].add(e.to_uri)
        if entry["description"] or entry["members"] \
                or "human_approval" in node.provenance.sources:
            entry["steward_node"] = (
                "human_approval" in node.provenance.sources)
            facts[name] = entry
    return facts


def rollup_domains(store: GraphStore) -> dict[str, Any]:
    """Rebuild the domain layer: steward facts extracted and laid back
    down verbatim; derived memberships recomputed from the labels tables
    currently carry; per-domain profiles recomputed from the union.

    Returns a report: domains, memberships (derived/steward), tables in
    no domain, and the overlap list — nothing silent."""
    steward = _extract_steward_facts(store)

    # 1. drop the previous layer — steward facts are already extracted
    stale = {u for u, n in store.nodes.items() if n.node_type == "Domain"}
    for uri in stale:
        del store.nodes[uri]
    for edge_uri in [u for u, e in store.edges.items()
                     if e.from_uri in stale or e.to_uri in stale]:
        del store.edges[edge_uri]

    # 2. memberships: domain name → {table_uri: kind}, kind ∈
    #    {"derived", "steward", "both"} — the coexistence, made explicit
    memberships: dict[str, dict[str, str]] = {}
    ungrouped = 0
    for table in store.nodes_by_type("Table"):
        labels = {
            str(table.properties.get(k) or "").strip()
            for k in ("business_unit", "company_domain")
        } - {""}
        for label in labels:
            memberships.setdefault(label, {})[
                table.canonical_uri] = "derived"
        if not labels and not any(
                table.canonical_uri in f["members"]
                for f in steward.values()):
            ungrouped += 1
    for name, facts in steward.items():
        bucket = memberships.setdefault(name, {})
        for t_uri in facts["members"]:
            bucket[t_uri] = "both" if t_uri in bucket else "steward"

    # which tables sit in >1 domain — the overlap the layer exists for
    table_domains: dict[str, list[str]] = {}
    for name, bucket in memberships.items():
        for t_uri in bucket:
            table_domains.setdefault(t_uri, []).append(name)
    overlap = {t: sorted(ds) for t, ds in table_domains.items()
               if len(ds) > 1}

    domains_report: list[dict[str, Any]] = []
    n_derived = n_steward = 0
    for name, bucket in sorted(memberships.items()):
        members: list[tuple[Node, str]] = []
        for t_uri, kind in bucket.items():
            t = store.get(t_uri)
            if t is not None:
                members.append((t, kind))
        if not members and name not in steward:
            continue
        members.sort(key=lambda mk: str(
            mk[0].properties.get("table_name")))
        member_uris = {t.canonical_uri for t, _ in members}
        member_names = [str(t.properties.get("table_name") or
                            t.canonical_uri.rsplit("/", 1)[-1])
                        for t, _ in members]

        # ── member-derived profile (union of both witness families) ──
        data_domains = Counter()
        for t, _ in members:
            d = str(t.properties.get("data_domain") or "").strip()
            if d:
                data_domains[d] += 1

        metrics: list[Node] = []
        for t, _ in members:
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
        question_bank = [
            q for q, _ in sorted(
                {str(m.properties.get("question_answered")):
                 int(m.properties.get("execution_count") or 0)
                 for m in metrics
                 if m.properties.get("question_answered")}.items(),
                key=lambda kv: (-kv[1], kv[0]))
        ][:_TOP_QUESTIONS]

        internal = external = 0
        partner_domains: Counter = Counter()
        for t, _ in members:
            for e in store.outgoing(t.canonical_uri, "JOINS_WITH"):
                if e.to_uri in member_uris:
                    internal += 1
                else:
                    external += 1
                    for p in table_domains.get(e.to_uri, ["(unlabeled)"]):
                        partner_domains[p] += 1
            for e in store.incoming(t.canonical_uri, "JOINS_WITH"):
                if e.from_uri not in member_uris:
                    external += 1
                    for p in table_domains.get(
                            e.from_uri, ["(unlabeled)"]):
                        partner_domains[p] += 1
        partners = [p for p, _ in partner_domains.most_common(
            _TOP_PARTNERS)]

        shared = [(str(t.properties.get("table_name")),
                   [d for d in table_domains[t.canonical_uri]
                    if d != name])
                  for t, _ in members
                  if t.canonical_uri in overlap]

        steward_facts = steward.get(name, {})
        steward_desc = str(steward_facts.get("description") or "")
        derived_desc = _describe(
            name, n_tables=len(members), sample=member_names,
            domains=data_domains.most_common(_TOP_DOMAINS),
            n_metrics=len(metrics), executions=executions, users=users,
            internal_joins=internal, external_joins=external,
            partners=partners, shared=shared)

        uri = canonical_uri("domain", name)
        node = Node(
            canonical_uri=uri,
            node_type="Domain",
            properties={
                "name": name,
                "table_count": len(members),
                "member_tables": member_names,
                "membership": {
                    str(t.properties.get("table_name")): kind
                    for t, kind in members},
                "data_domains": [
                    {"domain": d, "tables": c}
                    for d, c in data_domains.most_common(_TOP_DOMAINS)],
                "metric_count": len(metrics),
                "execution_count": executions,
                "user_count": users,
                "top_metrics": top_metrics,
                "vocabulary": [t for t, _ in vocab.most_common(
                    _TOP_VOCAB)],
                "question_bank": question_bank,
                "internal_join_edges": internal,
                "external_join_edges": external,
                "external_join_partners": partners,
                "shared_tables": [
                    {"table": t, "also_in": ds} for t, ds in shared],
                "description": steward_desc or derived_desc,
                "description_by": "steward" if steward_desc else "rollup",
                "derived_description": derived_desc,
            },
        )
        # ── witnesses: steward ceiling + inherited from members ──
        tally: Counter = Counter()
        edge_sources: dict[str, str] = {}
        for t, kind in members:
            witnesses = _bu_witnesses(t)
            for s in witnesses:
                tally[s] += 1
            edge_sources[t.canonical_uri] = (
                max(witnesses, key=lambda s: SOURCE_WEIGHTS.get(s, 0))
                if witnesses else "llm_generated")
        n_human_edges = sum(1 for _, k in members
                            if k in ("steward", "both"))
        if n_human_edges or steward_facts.get("steward_node"):
            node.provenance.record_source(
                "human_approval", count_delta=max(n_human_edges, 1))
        for src, count in sorted(
                tally.items(),
                key=lambda kv: -SOURCE_WEIGHTS.get(kv[0], 0)):
            if src != "human_approval":
                node.provenance.record_source(src, count_delta=count)
        store.nodes[uri] = node

        for t, kind in members:
            if kind in ("steward", "both"):
                store.upsert_edge("CONTAINS", uri, t.canonical_uri,
                                  {"membership": kind},
                                  source="human_approval")
            if kind in ("derived", "both"):
                store.upsert_edge("CONTAINS", uri, t.canonical_uri,
                                  {"membership": kind},
                                  source=edge_sources[t.canonical_uri])
            n_steward += kind in ("steward", "both")
            n_derived += kind in ("derived", "both")

        domains_report.append({
            "domain": name, "tables": len(members),
            "steward_members": sum(1 for _, k in members
                                   if k in ("steward", "both")),
            "metrics": len(metrics),
            "tier": node.provenance.confidence_tier,
        })

    return {
        "domains": len(domains_report),
        "memberships_derived": n_derived,
        "memberships_steward": n_steward,
        "tables_in_no_domain": ungrouped,
        "overlapping_tables": {
            str((store.get(t) or Node(canonical_uri=t, node_type="Table",
                                      properties={})).properties.get(
                "table_name") or t.rsplit("/", 1)[-1]): ds
            for t, ds in sorted(overlap.items())},
        "units": domains_report,
    }


# Back-compat alias — the pipeline stage and earlier callers used the
# business-unit name before the layer generalized to domains.
rollup_business_units = rollup_domains
