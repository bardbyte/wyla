"""Read-side data for the console API — graph-only, always labeled.

One rule, applied everywhere (the truth-of-state rule): every payload
carries `"live": bool` and a `"source"`. When a compiled snapshot exists
(SYNAPSE_GRAPH_PATH, or the pipeline's default cache path), reads come
from the graph (`source: "graph"`). When none is loaded, every surface
returns an HONEST EMPTY payload (`source: "empty"`) — never fabricated
data. A VP is never shown an invented number; the frontend renders a
"no graph loaded" state instead.

Nothing here mutates the graph. Writes (metric canonicalization,
entity approval) stay on their steward-gated CLI paths.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "synapse") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "synapse"))

_DEFAULT_SNAPSHOT = REPO_ROOT / "synapse" / "data" / "cache" / "graph_snapshot.json"

# Columns whose *values* are governance-guarded never leak metadata
# that looks like data: sample/top values are stripped from payloads.
_VALUE_KEYS = ("sample_values", "top_values", "example_values", "examples")


def _norm(text: str) -> str:
    return "".join(ch for ch in str(text).lower() if ch.isalnum())


class ConsoleData:
    """All read endpoints in one place, so liveness is decided once."""

    def __init__(self, snapshot_path: str | Path | None = None) -> None:
        raw = (snapshot_path
               or os.environ.get("SYNAPSE_GRAPH_PATH")
               or _DEFAULT_SNAPSHOT)
        path = Path(raw).expanduser()
        # a relative SYNAPSE_GRAPH_PATH resolves against the server's
        # cwd; when that misses, fall back to repo-root-relative so
        # `export SYNAPSE_GRAPH_PATH=synapse/data/cache/…` works no
        # matter where uvicorn was launched from
        if not path.is_absolute() and not path.exists():
            alt = REPO_ROOT / str(raw)
            if alt.exists():
                path = alt
        self.snapshot_path = path
        self.store = None
        if self.snapshot_path.exists():
            try:
                from synapse.graph.store import GraphStore
                self.store = GraphStore.load_json(self.snapshot_path)
            except Exception:
                self.store = None                 # unreadable → honest empty

    @property
    def live(self) -> bool:
        return self.store is not None

    def _wrap(self, key: str, value: Any) -> dict[str, Any]:
        # graph-only: no snapshot → an honest empty payload, never invented
        # data. A VP never sees a fabricated number.
        return {"live": self.live,
                "source": "graph" if self.live else "empty",
                key: value}

    # ── data products ────────────────────────────────────────

    def products(self, q: str = "") -> dict[str, Any]:
        rows = self._products_from_graph() if self.live else []
        if q:
            needle = _norm(q)
            rows = [r for r in rows
                    if needle in _norm(r["name"])
                    or needle in _norm(r.get("description", ""))]
        return self._wrap("products", rows)

    def _products_from_graph(self) -> list[dict[str, Any]]:
        from synapse.graph.inspector import context_readiness
        tables = sorted(
            n.properties.get("table_name", n.canonical_uri.rsplit("/", 1)[-1])
            for n in self.store.nodes_by_type("Table"))
        readiness = {r["table"]: r for r in
                     context_readiness(self.store, tables)}
        rows = []
        for node in self.store.nodes_by_type("Table"):
            name = node.properties.get(
                "table_name", node.canonical_uri.rsplit("/", 1)[-1])
            r = readiness.get(name, {})
            rows.append({
                "name": name,
                "ref": node.canonical_uri,
                "description": (node.properties.get("description")
                                or node.properties.get(
                                    "ai_generated_description") or ""),
                "owner": (node.properties.get("business_owner")
                          or node.properties.get("owner") or ""),
                "domain": (node.properties.get("business_unit")
                           or node.properties.get("dataset") or ""),
                "lifecycle": node.properties.get("lifecycle_status", ""),
                "tier": node.provenance.confidence_tier,
                "readiness": {
                    "columns": r.get("n_columns", 0),
                    "meaning_pct": r.get("pct_columns_with_meaning", 0),
                    "related_tables": r.get("n_related_tables", 0),
                    "metrics": r.get("n_metrics", 0),
                    "governance": bool(r.get("has_governance")),
                    "lineage": bool(r.get("has_lineage")),
                },
            })
        rows.sort(key=lambda r: (-r["readiness"]["meaning_pct"], r["name"]))
        return rows

    def products_by_unit(self, q: str = "") -> dict[str, Any]:
        """Data products grouped by MDM business unit — the "we know your
        business" view. Each unit carries its tables plus readiness and
        governance rollups. Graph-only; empty when no snapshot is loaded."""
        if not self.live:
            return self._wrap("units", [])
        rows = self._products_from_graph()
        if q:
            needle = _norm(q)
            rows = [r for r in rows
                    if needle in _norm(r["name"])
                    or needle in _norm(r.get("description", ""))]
        groups: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            node = self.store.get(r["ref"])
            bu = ""
            if node is not None:
                bu = (str(node.properties.get("business_unit") or "").strip()
                      or str(node.properties.get("company_domain") or "").strip())
            groups.setdefault(bu or "Unassigned", []).append(r)

        units: list[dict[str, Any]] = []
        for unit, prod in sorted(groups.items()):
            n = len(prod)
            total_cols = sum(p["readiness"]["columns"] for p in prod)
            mean_meaning = round(
                sum(p["readiness"]["meaning_pct"] for p in prod) / n) if n else 0
            grounded = sum(1 for p in prod
                           if p["tier"] in ("grounded", "human_asserted"))
            pii_tables = sum(1 for p in prod if self._table_has_pii(p["ref"]))
            governed = sum(1 for p in prod if p["readiness"]["governance"])
            units.append({
                "unit": unit,
                "table_count": n,
                "total_columns": total_cols,
                "mean_meaning_pct": mean_meaning,
                "grounded_tables": grounded,
                "pii_tables": pii_tables,
                "governed_tables": governed,
                "products": prod,
            })
        units.sort(key=lambda u: (-u["table_count"], u["unit"]))
        return self._wrap("units", units)

    def _table_has_pii(self, table_ref: str) -> bool:
        for e in self.store.outgoing(table_ref, "CONTAINS"):
            c = self.store.get(e.to_uri)
            if c is not None and bool(c.properties.get("is_pii")):
                return True
        return False

    # ── knowledge graph ──────────────────────────────────────

    def graph_summary(self) -> dict[str, Any]:
        if not self.live:
            return self._wrap("summary", {})
        stats = self.store.stats()
        sources: dict[str, int] = {}
        for n in self.store.nodes.values():
            for s in n.provenance.sources:
                sources[s] = sources.get(s, 0) + 1
        return self._wrap("summary", {
            "nodes": stats["n_nodes"],
            "edges": stats["n_edges"],
            "nodes_by_type": stats["nodes_by_type"],
            "edges_by_type": stats["edges_by_type"],
            "tiers": stats["nodes_by_confidence_tier"],
            "witnesses": dict(sorted(sources.items(),
                                     key=lambda kv: -kv[1])),
            "snapshot_version": getattr(self.store, "snapshot_version",
                                        None),
        })

    def graph_thread(self, table: str = "") -> dict[str, Any]:
        """The curated storyline: entity → identifying columns → join
        evidence → metric → skill. With `table`, the thread anchors on
        that table and explores ITS connections. Falls back hop-by-hop —
        a thin graph still yields a partial, honest thread. A graph that
        yields NO hops returns an empty thread (honest empty state) — never
        a fabricated storyline."""
        if self.live:
            thread = (self._table_thread(table) if table
                      else self._thread_from_graph())
            return self._wrap("thread", thread)
        return self._wrap("thread", {"hops": []})

    def _find_table_node(self, table: str):
        """Resolve a table by canonical URI, then by normalized name —
        snapshots compiled before identity normalization carry qualified
        names in their URIs, and callers pass either form."""
        from synapse.graph.store import canonical_uri, normalize_table_name
        node = self.store.get(canonical_uri("table", table))
        if node is not None:
            return node
        want = normalize_table_name(table)
        for n in self.store.nodes_by_type("Table"):
            name = str(n.properties.get("table_name", ""))
            if name and normalize_table_name(name) == want:
                return n
        return None

    def _table_thread(self, table: str) -> dict[str, Any]:
        """One table's place in the graph: the table, the entity its
        columns identify, its join evidence, a metric computed from it,
        and the playbook that governs it. Every hop is optional."""
        node = self._find_table_node(table)
        if node is None:
            return {"hops": [], "table": table}
        t_uri = node.canonical_uri
        hops: list[dict[str, Any]] = [{
            "kind": "table",
            "label": node.properties.get("table_name", table),
            "ref": t_uri, "tier": node.provenance.confidence_tier,
            "detail": (node.properties.get("description")
                       or node.properties.get("ai_generated_description")
                       or "")[:90],
        }]
        col_uris = [e.to_uri for e in self.store.outgoing(t_uri, "CONTAINS")]
        col_set = set(col_uris)

        # entity identified by one of this table's columns
        for c in col_uris:
            idents = self.store.outgoing(c, "IDENTIFIES")
            if idents:
                ent = self.store.get(idents[0].to_uri)
                if ent is not None:
                    hops.append({
                        "kind": "entity",
                        "label": ent.properties.get(
                            "name", ent.canonical_uri.rsplit("/", 1)[-1]),
                        "ref": ent.canonical_uri,
                        "tier": ent.provenance.confidence_tier,
                        "detail": "identified by "
                                  f"{c.rsplit('/', 1)[-1]}",
                    })
                break

        # join evidence touching this table's columns
        for e in self.store.edges.values():
            if e.edge_type == "EQUIVALENT_TO" and (
                    e.from_uri in col_set or e.to_uri in col_set):
                other = e.to_uri if e.from_uri in col_set else e.from_uri
                hops.append({
                    "kind": "join", "label": "join evidence",
                    "ref": e.canonical_uri,
                    "tier": e.provenance.confidence_tier,
                    "detail": f"{e.from_uri.rsplit('/', 1)[-1]} ≍ "
                              f"{other.rsplit('/', 2)[-2]}."
                              f"{other.rsplit('/', 1)[-1]}",
                })
                break

        # a metric computed from this table's columns
        for e in self.store.edges.values():
            if e.edge_type == "COMPUTED_FROM" and (
                    e.from_uri in col_set or e.to_uri in col_set):
                m_uri = e.to_uri if e.from_uri in col_set else e.from_uri
                m = self.store.get(m_uri)
                if m is not None and m.node_type == "Metric":
                    hops.append({
                        "kind": "metric",
                        "label": m.properties.get(
                            "name", m_uri.rsplit("/", 1)[-1]),
                        "ref": m_uri,
                        "tier": m.provenance.confidence_tier,
                        "detail": str(m.properties.get("formula", ""))[:80],
                    })
                    break

        # the skill/playbook that applies to this table
        for e in list(self.store.incoming(t_uri, "APPLIES_TO")) + \
                list(self.store.outgoing(t_uri, "APPLIES_TO")):
            other = (e.from_uri if e.to_uri == t_uri else e.to_uri)
            s = self.store.get(other)
            if s is not None and s.node_type == "Skill":
                hops.append({
                    "kind": "skill",
                    "label": s.properties.get(
                        "name", other.rsplit("/", 1)[-1]),
                    "ref": other, "tier": s.provenance.confidence_tier,
                    "detail": "expert playbook",
                })
                break
        return {"hops": hops, "table": table}

    def _thread_from_graph(self) -> dict[str, Any]:
        hops: list[dict[str, Any]] = []
        entities = self.store.nodes_by_type("Entity")
        entity = entities[0] if entities else None
        col_uris: list[str] = []
        if entity is not None:
            ident = self.store.incoming(entity.canonical_uri, "IDENTIFIES")
            col_uris = [e.from_uri for e in ident][:2]
            hops.append({
                "kind": "entity",
                "label": entity.properties.get(
                    "name", entity.canonical_uri.rsplit("/", 1)[-1]),
                "ref": entity.canonical_uri,
                "tier": entity.provenance.confidence_tier,
                "detail": f"identified by {len(ident)} column(s)",
            })
        for uri in col_uris:
            node = self.store.get(uri)
            if node is None:
                continue
            hops.append({
                "kind": "column",
                "label": uri.rsplit("/", 2)[-2] + "." + uri.rsplit("/", 1)[-1],
                "ref": uri, "tier": node.provenance.confidence_tier,
                "detail": "identifying column",
            })
        for e in self.store.edges.values():          # first join evidence
            if e.edge_type == "EQUIVALENT_TO":
                hops.append({
                    "kind": "join", "label": "join evidence",
                    "ref": e.canonical_uri,
                    "tier": e.provenance.confidence_tier,
                    "detail": f"{e.from_uri.rsplit('/', 1)[-1]} ≍ "
                              f"{e.to_uri.rsplit('/', 1)[-1]}",
                })
                break
        metrics = self.store.nodes_by_type("Metric")
        if metrics:
            m = metrics[0]
            hops.append({
                "kind": "metric",
                "label": m.properties.get(
                    "name", m.canonical_uri.rsplit("/", 1)[-1]),
                "ref": m.canonical_uri,
                "tier": m.provenance.confidence_tier,
                "detail": m.properties.get("formula", "")[:80],
            })
        skills = self.store.nodes_by_type("Skill")
        if skills:
            s = skills[0]
            hops.append({
                "kind": "skill",
                "label": s.properties.get(
                    "name", s.canonical_uri.rsplit("/", 1)[-1]),
                "ref": s.canonical_uri,
                "tier": s.provenance.confidence_tier,
                "detail": "expert playbook",
            })
        return {"hops": hops}

    def graph_map(self, limit: int = 80) -> dict[str, Any]:
        """The spine of the graph for whole-graph visualization: the
        business-legible nodes (tables, entities, metrics, skills) and the
        structural relationships between them (joins, identification,
        computation, governance). Columns are NOT drawn — each table
        carries its column count instead, so a leader sees the shape of
        the business and drills into a table for detail. Column-level
        edges are rolled up to their owning table. Graph-only; the honest
        empty payload when no snapshot is loaded."""
        empty = {"nodes": [], "edges": [], "truncated": False}
        if not self.live:
            return self._wrap("map", empty)

        spine = ("Table", "Entity", "Metric", "Skill")
        # column URI → owning table URI (roll column edges up to the table)
        col_table: dict[str, str] = {}
        for t in self.store.nodes_by_type("Table"):
            for e in self.store.outgoing(t.canonical_uri, "CONTAINS"):
                col_table[e.to_uri] = t.canonical_uri

        nodes: list[dict[str, Any]] = []
        keep: set[str] = set()
        for kind in spine:
            for n in self.store.nodes_by_type(kind):
                label = (n.properties.get("table_name")
                         or n.properties.get("name")
                         or n.canonical_uri.rsplit("/", 1)[-1])
                node: dict[str, Any] = {
                    "id": n.canonical_uri,
                    "label": str(label),
                    "kind": kind.lower(),
                    "tier": n.provenance.confidence_tier,
                }
                if kind == "Table":
                    node["columns"] = len(
                        self.store.outgoing(n.canonical_uri, "CONTAINS"))
                    node["business_unit"] = (
                        n.properties.get("business_unit")
                        or n.properties.get("company_domain") or "")
                    node["pii"] = self._table_has_pii(n.canonical_uri)
                elif kind == "Metric":
                    node["subtitle"] = str(
                        n.properties.get("formula", ""))[:60]
                nodes.append(node)
                keep.add(n.canonical_uri)

        # roll column-level structural edges up to their spine endpoints
        structural = {"EQUIVALENT_TO", "IDENTIFIES", "COMPUTED_FROM",
                      "APPLIES_TO"}
        seen: set[tuple[str, str, str]] = set()
        edges: list[dict[str, Any]] = []
        for e in self.store.edges.values():
            if e.edge_type not in structural:
                continue
            a = col_table.get(e.from_uri, e.from_uri)
            b = col_table.get(e.to_uri, e.to_uri)
            if a == b or a not in keep or b not in keep:
                continue
            lo, hi = sorted((a, b))
            key = (e.edge_type, lo, hi)
            if key in seen:
                continue
            seen.add(key)
            edges.append({"source": a, "target": b,
                          "kind": e.edge_type.lower(),
                          "tier": e.provenance.confidence_tier})

        truncated = len(nodes) > limit
        if truncated:
            deg: dict[str, int] = {}
            for ed in edges:
                deg[ed["source"]] = deg.get(ed["source"], 0) + 1
                deg[ed["target"]] = deg.get(ed["target"], 0) + 1
            nodes.sort(key=lambda n: -deg.get(n["id"], 0))
            nodes = nodes[:limit]
            kept = {n["id"] for n in nodes}
            edges = [e for e in edges
                     if e["source"] in kept and e["target"] in kept]
        return self._wrap("map", {"nodes": nodes, "edges": edges,
                                  "truncated": truncated})

    def table_insights(self, table: str) -> dict[str, Any]:
        """Everything the graph knows about ONE table — the Insights
        view. Mirrors a catalog's per-dataset insights (description,
        derived relationships, query recommendations) but every
        relationship carries its WITNESSES: who asserted it (declared
        FK, analyst query log, LLM, steward) and at what tier — the
        receipts a catalog's flat "LLM-inferred" label lacks."""
        from synapse.graph.store import canonical_uri
        empty = {"table": table, "found": False, "description": {},
                 "columns": {}, "relationships": [], "recommendations": []}
        if not self.live:
            return self._wrap("insights", empty)
        node = self._find_table_node(table)
        if node is None:
            return self._wrap("insights", empty)
        t_uri = node.canonical_uri
        name = node.properties.get("table_name", table)

        col_uris = [e.to_uri for e in self.store.outgoing(t_uri, "CONTAINS")]
        col_set = set(col_uris)
        n_desc = n_pii = 0
        col_names: list[str] = []
        for c in col_uris:
            cn = self.store.get(c)
            if cn is None:
                continue
            col_names.append(c.rsplit("/", 1)[-1])
            if (cn.properties.get("description")
                    or cn.properties.get("ai_generated_description")):
                n_desc += 1
            if cn.properties.get("is_pii"):
                n_pii += 1

        def _witness(edge) -> str:
            """One legible label per relationship — the curation story."""
            srcs = set(edge.provenance.sources)
            if "human_approval" in srcs:
                return "curated"
            if "bq" in srcs and (edge.properties.get("constraint")
                                 == "foreign_key"):
                return "declared FK"
            if "corpus" in srcs:
                return "query log (analyst)"
            if "llm_generated" in srcs:
                return "LLM-inferred"
            return next(iter(srcs), "unknown")

        def _colname(uri: str) -> str:
            return (uri.rsplit("/", 2)[-2] + "." + uri.rsplit("/", 1)[-1])

        rels: list[dict[str, Any]] = []
        for e in self.store.edges.values():
            row: dict[str, Any] | None = None
            if e.edge_type == "EQUIVALENT_TO" and (
                    e.from_uri in col_set or e.to_uri in col_set):
                other = e.to_uri if e.from_uri in col_set else e.from_uri
                row = {"kind": "join",
                       "predicate": f"{_colname(e.from_uri)} = "
                                    f"{_colname(e.to_uri)}",
                       "other": other.rsplit("/", 2)[-2],
                       "other_ref": canonical_uri(
                           "table", other.rsplit("/", 2)[-2])}
            elif e.edge_type == "IDENTIFIES" and e.from_uri in col_set:
                ent = self.store.get(e.to_uri)
                label = (ent.properties.get("name")
                         if ent else e.to_uri.rsplit("/", 1)[-1])
                row = {"kind": "identifies",
                       "predicate": f"{_colname(e.from_uri)} identifies "
                                    f"{label}",
                       "other": str(label), "other_ref": e.to_uri}
            elif e.edge_type == "COMPUTED_FROM" and e.to_uri in col_set:
                m = self.store.get(e.from_uri)
                label = (m.properties.get("name")
                         if m else e.from_uri.rsplit("/", 1)[-1])
                row = {"kind": "metric",
                       "predicate": f"{label} computed from "
                                    f"{_colname(e.to_uri)}",
                       "other": str(label), "other_ref": e.from_uri}
            elif e.edge_type == "UPSTREAM_OF" and (
                    e.from_uri == t_uri or e.to_uri == t_uri):
                other = e.to_uri if e.from_uri == t_uri else e.from_uri
                direction = ("feeds" if e.from_uri == t_uri else "fed by")
                row = {"kind": "lineage",
                       "predicate": f"{name} {direction} "
                                    f"{other.rsplit('/', 1)[-1]}",
                       "other": other.rsplit("/", 1)[-1],
                       "other_ref": other}
            elif e.edge_type == "VALIDATED_BY" and e.from_uri in (
                    col_set | {t_uri}):
                rule = self.store.get(e.to_uri)
                kindname = (rule.properties.get("rule_kind", "rule")
                            if rule else "rule")
                row = {"kind": "dq",
                       "predicate": f"{_colname(e.from_uri)} validated by "
                                    f"{kindname}",
                       "other": str(kindname), "other_ref": e.to_uri}
            if row is not None:
                row["sources"] = list(e.provenance.sources)
                row["witness"] = _witness(e)
                row["tier"] = e.provenance.confidence_tier
                rels.append(row)
        order = {"join": 0, "identifies": 1, "metric": 2, "lineage": 3,
                 "dq": 4}
        rels.sort(key=lambda r: (order.get(r["kind"], 9), r["predicate"]))

        recs: list[dict[str, str]] = []
        for m_uri in {r["other_ref"] for r in rels if r["kind"] == "metric"}:
            m = self.store.get(m_uri)
            if m is None or len(recs) >= 6:
                continue
            mn = m.properties.get("name", "")
            if mn and not any(mn.lower() in r["question"].lower()
                              for r in recs):
                recs.append({"question": f"What is the {mn} for {name}?",
                             "source": "observed metric"})
        if node.properties.get("business_owner") or any(
                r["kind"] == "lineage" for r in rels):
            recs.append({"question": f"Who owns {name} and what feeds it?",
                         "source": "governance"})
        if any(r["kind"] == "dq" for r in rels):
            recs.append({"question": f"Is {name} trustworthy right now?",
                         "source": "data quality"})
        recs.append({"question": f"How confident are we in {name}, "
                                 "and why?",
                     "source": "witness ledger"})

        return self._wrap("insights", {
            "table": name, "found": True, "ref": t_uri,
            "description": {
                "curated": str(node.properties.get("description") or ""),
                "ai": str(node.properties.get(
                    "ai_generated_description") or ""),
                "tier": node.provenance.confidence_tier,
                "sources": list(node.provenance.sources),
            },
            "columns": {"count": len(col_uris), "described": n_desc,
                        "pii": n_pii},
            "relationships": rels[:40],
            "recommendations": recs[:6],
        })

    # ── metrics ──────────────────────────────────────────────

    def metrics(self, q: str = "") -> dict[str, Any]:
        rows = self._metrics_from_graph() if self.live else []
        if q:
            needle = _norm(q)
            rows = [r for r in rows if needle in _norm(r["name"])]
        return self._wrap("metrics", rows[:200])

    def _metrics_from_graph(self) -> list[dict[str, Any]]:
        rows = []
        for node in self.store.nodes_by_type("Metric"):
            rows.append({
                "name": node.properties.get(
                    "name", node.canonical_uri.rsplit("/", 1)[-1]),
                "ref": node.canonical_uri,
                "formula": str(node.properties.get("formula", ""))[:160],
                "description": str(node.properties.get(
                    "description", ""))[:200],
                "tier": node.provenance.confidence_tier,
                "sources": list(node.provenance.sources),
            })
        rows.sort(key=lambda r: r["name"])
        return rows

    def metric_viability(self, name: str,
                         description: str = "") -> dict[str, Any]:
        """Canon-first check: exact match / near duplicates / clear.
        The copilot MUST run this before drafting — structurally, not by
        prompt discipline."""
        canon = self._metrics_from_graph() if self.live else []
        needle = _norm(name)
        tokens = {t for t in _norm_tokens(name + " " + description)}
        exact = [m for m in canon if _norm(m["name"]) == needle]
        near = []
        if not exact:
            for m in canon:
                m_tokens = set(_norm_tokens(
                    m["name"] + " " + m.get("description", "")))
                overlap = tokens & m_tokens
                # scale by the smaller vocabulary so a verbose draft
                # can't talk its way past a terse canonical name
                bar = max(2, min(len(tokens), len(m_tokens)) // 2)
                if len(overlap) >= bar:
                    near.append({**m, "shared_terms": sorted(overlap)[:6]})
        verdict = ("exact" if exact else
                   "near_duplicate" if near else "clear")
        return self._wrap("viability", {
            "verdict": verdict,
            "exact": exact[:3],
            "near": near[:5],
            "canon_size": len(canon),
        })

    # ── terms ────────────────────────────────────────────────

    def resolve_term(self, term: str) -> dict[str, Any]:
        if not self.live:
            return self._wrap("resolution", {
                "term": term, "canonical": None, "matches": []})
        return self._wrap("resolution", self._resolve_from_graph(term))

    def _resolve_from_graph(self, term: str) -> dict[str, Any]:
        needle = _norm(term)
        matches: list[dict[str, Any]] = []
        for node in self.store.nodes.values():
            name = str(node.properties.get(
                "name", node.properties.get("table_name", "")) or
                node.canonical_uri.rsplit("/", 1)[-1])
            if needle and needle in _norm(name):
                matches.append({
                    "name": name, "kind": node.node_type,
                    "ref": node.canonical_uri,
                    "tier": node.provenance.confidence_tier,
                    "sources": list(node.provenance.sources),
                })
            if len(matches) >= 12:
                break
        best = next((m for m in matches
                     if m["kind"] in ("Metric", "Entity", "Table")),
                    matches[0] if matches else None)
        return {"term": term,
                "canonical": best,
                "matches": matches}

    # ── evidence / witness panel ─────────────────────────────

    def witness(self, ref: str) -> dict[str, Any]:
        """Everything behind one chip: the node, its provenance, and its
        neighborhood — the panel a reviewer opens before citing."""
        if not self.live:
            return self._wrap("witness", {"ref": ref, "found": False})
        node = self.store.get(ref)
        if node is None:
            return self._wrap("witness", {"ref": ref, "found": False})
        props = {k: v for k, v in node.properties.items()
                 if k not in _VALUE_KEYS}
        edges = [{
            "type": e.edge_type,
            "other": (e.to_uri if e.from_uri == ref else e.from_uri),
            "direction": "out" if e.from_uri == ref else "in",
            "tier": e.provenance.confidence_tier,
        } for e in (self.store.outgoing(ref) + self.store.incoming(ref))]
        return self._wrap("witness", {
            "ref": ref, "found": True,
            "kind": node.node_type,
            "properties": props,
            "provenance": {
                "tier": node.provenance.confidence_tier,
                "score": node.provenance.confidence_score,
                "sources": list(node.provenance.sources),
            },
            "ledger": self._witness_ledger(node),
            "edges": edges[:40],
        })

    @staticmethod
    def _witness_ledger(node) -> dict[str, Any]:
        """The confidence arithmetic, shown honestly (mockup 2a): every
        witness with its weight, capped count, and contribution, plus
        the exact tier rule that fired. No hidden math."""
        from synapse.graph.store import SOURCE_WEIGHTS
        prov = node.provenance
        counts = prov.evidence_count_by_source or {}
        cap, denom = 5, 15.0
        rows = []
        for s in prov.sources:
            w = SOURCE_WEIGHTS.get(s, 0)
            n = counts.get(s, 1) or 1
            capped = min(max(n, 1), cap)
            rows.append({
                "source": s, "weight": w, "count": n, "capped": capped,
                "contribution": round(w * capped / denom, 3),
            })
        rows.sort(key=lambda r: -r["contribution"])
        weighted = sum(r["weight"] * r["capped"] for r in rows)
        score = min(0.99, weighted / denom)
        distinct = len(set(prov.sources))
        if "human_approval" in prov.sources:
            rule = ("a human signature sets the ceiling — one witness "
                    "with mass, not proof")
        elif distinct >= 4:
            rule = f"grounded: {distinct} distinct witnesses agree"
        elif distinct >= 3 and score >= 0.70:
            rule = (f"grounded: {distinct} witnesses and score "
                    f"{score:.2f} ≥ 0.70")
        elif score >= 0.90:
            rule = f"grounded: score {score:.2f} ≥ 0.90"
        elif distinct >= 2:
            rule = f"inferred: {distinct} distinct witnesses"
        elif score >= 0.45:
            rule = f"inferred: score {score:.2f} ≥ 0.45"
        else:
            rule = "one weak witness — unverified until corroborated"
        return {"rows": rows, "weighted": weighted,
                "denominator": int(denom), "score": round(score, 3),
                "distinct": distinct, "rule": rule}

    def tier_for(self, ref: str) -> str | None:
        """The current confidence tier at a ref — the pin store's
        resolver, so pinned citations stay honest as the graph moves."""
        if not self.live:
            return None
        node = self.store.get(ref)
        return node.provenance.confidence_tier if node else None

    def lexicon(self) -> dict[str, Any]:
        """Known object names for answer linkification: tables, metrics,
        entities — name, kind, ref. Conservative by construction: names
        under 4 chars dropped, case-insensitive dedupe with tables
        outranking metrics outranking entities, capped at 500."""
        if not self.live:
            return self._wrap("lexicon", [])
        entries = []
        for node in self.store.nodes_by_type("Table"):
            name = str(node.properties.get("table_name", "")).strip()
            if not name:
                continue
            entries.append({"name": name, "kind": "table",
                            "ref": node.canonical_uri})
            tail = name.rsplit(".", 1)[-1]
            if tail != name:                     # qualified-name alias
                entries.append({"name": tail, "kind": "table",
                                "ref": node.canonical_uri})
        for node in self.store.nodes_by_type("Metric"):
            name = str(node.properties.get("name")
                       or node.properties.get("business_name")
                       or node.canonical_uri.rsplit("/", 1)[-1]).strip()
            entries.append({"name": name, "kind": "metric",
                            "ref": node.canonical_uri})
        for node in self.store.nodes_by_type("Entity"):
            name = str(node.properties.get(
                "name", node.canonical_uri.rsplit("/", 1)[-1])).strip()
            entries.append({"name": name, "kind": "entity",
                            "ref": node.canonical_uri})
        return self._wrap("lexicon", _dedupe_lexicon(entries))

    # ── suggested questions ──────────────────────────────────

    def starter_questions(self) -> dict[str, Any]:
        """Starters derived from THIS graph, not a canned list — one per
        capability, each with the why spelled out. The starter page is a
        guided tour of what the system can actually do with the data it
        actually has."""
        if not self.live:
            return self._wrap("starters", [])
        store = self.store
        bare = (lambda s: str(s).split(".")[-1])
        out: list[dict[str, Any]] = []

        def add(category: str, question: str, why: str,
                prefill: bool = False) -> None:
            out.append({"category": category, "question": question,
                        "why": why, "prefill": prefill})

        tables = sorted(
            store.nodes_by_type("Table"),
            key=lambda n: -len(store.outgoing(n.canonical_uri,
                                              "CONTAINS")))
        tname = (lambda n: bare(n.properties.get("table_name")
                                or n.canonical_uri.rsplit("/", 1)[-1]))
        metrics = store.nodes_by_type("Metric")

        # live analysis — REAL analytical questions in natural
        # language, built from a metric plus its table's actual columns
        # (a date column → trend; a low-cardinality column → breakdown)
        def _cols(t_node):
            out = []
            for e in store.outgoing(t_node.canonical_uri, "CONTAINS"):
                c = store.get(e.to_uri)
                if c is not None:
                    out.append(c)
            return out

        def _date_col(cols):
            for c in cols:
                nm = str(c.properties.get("name", "")).lower()
                dt = str(c.properties.get("data_type", "")).upper()
                if dt in ("DATE", "DATETIME", "TIMESTAMP")                         or nm.endswith(("_dt", "_date")):
                    return c.properties.get("name")
            return None

        def _seg_col(cols):
            for c in cols:
                nm = str(c.properties.get("name", "")).lower()
                if c.properties.get("cardinality_bucket") == "low"                         or nm.endswith(("_status", "_type", "_code",
                                        "_segment", "_flag")):
                    return c.properties.get("name")
            return None

        n_live = 0
        for m in metrics:
            tbl = m.properties.get("sourced_from_table")
            name = m.properties.get("name")
            if not (tbl and name):
                continue
            t_node = next((t for t in tables if tname(t) == bare(tbl)),
                          None)
            cols = _cols(t_node) if t_node is not None else []
            date_c, seg_c = _date_col(cols), _seg_col(cols)
            if n_live == 0:
                q = (f"How has {name} moved month over month this year?"
                     if date_c else
                     f"What is {name} right now, exactly?")
                add("Live analysis", q,
                    "Drafts the SQL, prices the scan, waits for your "
                    "signature, runs row-capped on the ledger.")
                n_live += 1
                if seg_c:
                    add("Live analysis",
                        f"Which {seg_c} drives {name} on "
                        f"{bare(tbl)}?",
                        f"A real breakdown: grouped by the observed "
                        f"{seg_c} values, verified before it runs.")
                    n_live += 1
            elif n_live == 2:
                break
            else:
                add("Live analysis",
                    f"Compare {name} against last quarter.",
                    "Time-sliced from the same governed table, on "
                    "the audit ledger.")
                n_live += 1

        # meaning — a definition with provenance
        for m in metrics:
            name = m.properties.get("name")
            if name and (m.properties.get("formula_sql")
                         or m.properties.get("description")):
                add("Meaning",
                    f"What does {name} mean, exactly?",
                    "The canonical definition, with every witness that "
                    "vouches for it and its confidence tier.")
                break

        # ownership + lineage
        for t in tables:
            if t.properties.get("business_owner") or store.outgoing(
                    t.canonical_uri, "UPSTREAM_OF") or store.incoming(
                    t.canonical_uri, "UPSTREAM_OF"):
                add("Ownership & lineage",
                    f"Who owns {tname(t)} and what feeds it?",
                    "Stewardship from the metadata spine plus observed "
                    "lineage, cited.")
                break

        # joins — only observed reality
        join_pair = None
        for e in store.edges.values():
            if e.edge_type != "EQUIVALENT_TO":
                continue
            fa = store.get(e.from_uri)
            fb = store.get(e.to_uri)
            ta = fa.properties.get("table_name") if fa else None
            tb = fb.properties.get("table_name") if fb else None
            if ta and tb and bare(ta) != bare(tb):
                join_pair = (bare(ta), bare(tb))
                break
        if join_pair:
            add("Join paths",
                f"How do I join {join_pair[0]} to {join_pair[1]} safely?",
                "Only joins the corpus has actually observed — the "
                "agent never invents an ON clause.")

        # trust — a fact with real corroboration
        for t in tables:
            if len(set(t.provenance.sources)) >= 3:
                add("Trust",
                    f"How confident are we in {tname(t)}, and why?",
                    "The witness ledger: every source's weight and the "
                    "arithmetic behind the tier.")
                break

        # data quality
        for rule in store.nodes_by_type("DataQualityRule"):
            tbl = rule.properties.get("target_table")
            if tbl:
                add("Data quality",
                    f"Is {bare(tbl)} trustworthy right now?",
                    "Data-quality rules and their latest status, "
                    "disclosed with the answer.")
                break

        # governance — the refusal moment
        pii_col = next(
            (c for c in store.nodes_by_type("Column")
             if c.properties.get("is_pii")), None)
        if pii_col:
            col = bare(pii_col.properties.get("name")
                       or pii_col.canonical_uri.rsplit("/", 1)[-1])
            add("Governance",
                f"Show me raw {col} values",
                "Watch the guardrail refuse — and offer the compliant "
                "alternative instead.")

        # teach it — the signature ceremony
        if tables:
            add("Teach it",
                f"Record that {tname(tables[0])} means ",
                "Finish the sentence and sign it: your assertion "
                "outranks every machine guess, and the sky turns gold.",
                prefill=True)

        return self._wrap("starters", out[:8])

    def questions(self) -> dict[str, Any]:
        """Back-compat shape over the graph-derived starters — nothing
        canned ever reaches a surface."""
        starters = self.starter_questions()
        return {"live": starters["live"], "source": starters["source"],
                "questions": [{
                    "question": st["question"],
                    "archetype": st["category"],
                } for st in starters["starters"]
                    if not st.get("prefill")]}


def _dedupe_lexicon(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rank = {"table": 0, "metric": 1, "entity": 2}
    best: dict[str, dict[str, Any]] = {}
    for e in entries:
        name = e["name"]
        if len(name) < 4:
            continue
        key = name.lower()
        if key not in best or rank[e["kind"]] < rank[best[key]["kind"]]:
            best[key] = e
    out = sorted(best.values(), key=lambda e: (-len(e["name"]), e["name"]))
    return out[:500]


def _norm_tokens(text: str) -> list[str]:
    out, cur = [], []
    for ch in str(text).lower():
        if ch.isalnum():
            cur.append(ch)
        elif cur:
            out.append("".join(cur))
            cur = []
    if cur:
        out.append("".join(cur))
    stop = {"the", "of", "a", "an", "by", "per", "for", "and", "or", "in"}
    return [t for t in out if t not in stop and len(t) > 1]
