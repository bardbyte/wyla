"""Read-side data for the console API — graph-backed, sample-backed,
always labeled.

One rule, applied everywhere (the truth-of-state rule from the design
research): every payload carries `"live": bool` so the frontend can
label real data as live and sample data as sample. When a compiled
snapshot exists (SYNAPSE_GRAPH_PATH, or the pipeline's default cache
path), reads come from the graph; otherwise a coherent sample world —
the same five-table shape the pipeline builds — keeps every surface
functional. The frontend never has to guess which world it is in.

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
_DEFAULT_DEMO = REPO_ROOT / "synapse" / "data" / "cache" / "demo_questions.json"

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
        self.snapshot_path = Path(raw).expanduser()
        self.store = None
        if self.snapshot_path.exists():
            try:
                from synapse.graph.store import GraphStore
                self.store = GraphStore.load_json(self.snapshot_path)
            except Exception:
                self.store = None                 # unreadable → sample world

    @property
    def live(self) -> bool:
        return self.store is not None

    def _wrap(self, key: str, value: Any) -> dict[str, Any]:
        return {"live": self.live,
                "source": "graph" if self.live else "sample",
                key: value}

    # ── data products ────────────────────────────────────────

    def products(self, q: str = "") -> dict[str, Any]:
        rows = (self._products_from_graph() if self.live
                else _SAMPLE["products"])
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

    # ── knowledge graph ──────────────────────────────────────

    def graph_summary(self) -> dict[str, Any]:
        if not self.live:
            return self._wrap("summary", _SAMPLE["summary"])
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

    def graph_thread(self) -> dict[str, Any]:
        """The one curated storyline: entity → identifying columns in two
        tables → join evidence → metric → skill. Falls back hop-by-hop —
        a thin graph still yields a partial, honest thread."""
        if not self.live:
            return self._wrap("thread", _SAMPLE["thread"])
        thread = self._thread_from_graph()
        return self._wrap("thread",
                          thread if thread["hops"] else _SAMPLE["thread"])

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

    # ── metrics ──────────────────────────────────────────────

    def metrics(self, q: str = "") -> dict[str, Any]:
        rows = (self._metrics_from_graph() if self.live
                else _SAMPLE["metrics"])
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
        canon = (self._metrics_from_graph() if self.live
                 else _SAMPLE["metrics"])
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
            hit = next((t for t in _SAMPLE["terms"]
                        if _norm(t["term"]) == _norm(term)), None)
            return self._wrap("resolution", hit or {
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
            return self._wrap("witness", _SAMPLE["witness"].get(
                ref, {"ref": ref, "found": False}))
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
            "edges": edges[:40],
        })

    # ── suggested questions ──────────────────────────────────

    def questions(self) -> dict[str, Any]:
        demo = Path(os.environ.get("SYNAPSE_DEMO_QUESTIONS",
                                   _DEFAULT_DEMO)).expanduser()
        if demo.exists():
            try:
                verified = json.loads(
                    demo.read_text(encoding="utf-8")).get("verified", [])
                if verified:
                    return {"live": True, "source": "graph",
                            "questions": [{
                                "question": q.get("question", ""),
                                "archetype": q.get("archetype", ""),
                            } for q in verified[:12]]}
            except Exception:
                pass
        return {"live": False, "source": "sample",
                "questions": _SAMPLE["questions"]}


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


# ─── the sample world ────────────────────────────────────────
# Mirrors the shape the pipeline compiles for the five onboarded tables,
# so the UI exercised against samples behaves identically against the
# graph. Sample data is always labeled as such by the `live` flag.

_SAMPLE: dict[str, Any] = {
    "products": [
        {"name": "sbs_new_accounts", "ref": "synapse://table/sbs_new_accounts",
         "description": "New account originations with approval outcome, "
                        "product, and channel.",
         "owner": "New Accounts", "domain": "originations",
         "lifecycle": "active", "tier": "grounded",
         "readiness": {"columns": 48, "meaning_pct": 100,
                       "related_tables": 3, "metrics": 12,
                       "governance": True, "lineage": True}},
        {"name": "us_daily_rr_smry_data",
         "ref": "synapse://table/us_daily_rr_smry_data",
         "description": "Daily delinquency roll-rate summary at portfolio "
                        "grain.",
         "owner": "Portfolio Risk", "domain": "risk",
         "lifecycle": "active", "tier": "grounded",
         "readiness": {"columns": 61, "meaning_pct": 99,
                       "related_tables": 2, "metrics": 18,
                       "governance": True, "lineage": False}},
        {"name": "roll_rate_calc", "ref": "synapse://table/roll_rate_calc",
         "description": "Account-level delinquency stage transitions used "
                        "by the roll-rate playbook.",
         "owner": "Portfolio Risk", "domain": "risk",
         "lifecycle": "active", "tier": "grounded",
         "readiness": {"columns": 35, "meaning_pct": 97,
                       "related_tables": 2, "metrics": 9,
                       "governance": True, "lineage": True}},
        {"name": "customer_insights_cardmember",
         "ref": "synapse://table/customer_insights_cardmember",
         "description": "Cardmember profile and engagement attributes.",
         "owner": "Customer Insights", "domain": "customer",
         "lifecycle": "active", "tier": "inferred",
         "readiness": {"columns": 214, "meaning_pct": 92,
                       "related_tables": 1, "metrics": 6,
                       "governance": True, "lineage": False}},
        {"name": "raw_accounts", "ref": "synapse://table/raw_accounts",
         "description": "Account master extract; feeds the summary tables.",
         "owner": "Data Platform", "domain": "core",
         "lifecycle": "active", "tier": "inferred",
         "readiness": {"columns": 72, "meaning_pct": 88,
                       "related_tables": 4, "metrics": 2,
                       "governance": False, "lineage": True}},
    ],
    "summary": {
        "nodes": 14210, "edges": 20964,
        "nodes_by_type": {"Table": 29, "Column": 3184, "Metric": 165,
                          "Synonym": 812, "Skill": 11, "Guardrail": 23,
                          "Entity": 4, "CodeMapping": 96,
                          "FilterValue": 240, "DataQualityRule": 31,
                          "User": 118},
        "edges_by_type": {"CONTAINS": 3184, "RELATES_TO": 402,
                          "EQUIVALENT_TO": 57, "HAS_SYNONYM": 812,
                          "QUERIED_BY": 1240, "IDENTIFIES": 9,
                          "COMPUTED_FROM": 310, "UPSTREAM_OF": 18},
        "tiers": {"human_asserted": 13, "grounded": 211,
                  "inferred": 6728, "guessed": 7258},
        "witnesses": {"mdm": 3120, "bq": 2988, "corpus": 2140,
                      "skills": 690, "llm": 6400, "human_approval": 13},
        "snapshot_version": None,
    },
    "thread": {"hops": [
        {"kind": "entity", "label": "Account",
         "ref": "synapse://entity/account", "tier": "human_asserted",
         "detail": "identified by 2 columns"},
        {"kind": "column", "label": "sbs_new_accounts.acct_id",
         "ref": "synapse://column/sbs_new_accounts/acct_id",
         "tier": "grounded", "detail": "identifying column"},
        {"kind": "column", "label": "roll_rate_calc.acct_id",
         "ref": "synapse://column/roll_rate_calc/acct_id",
         "tier": "grounded", "detail": "identifying column"},
        {"kind": "join", "label": "join evidence",
         "ref": "synapse://edge/equiv", "tier": "grounded",
         "detail": "acct_id ≍ acct_id · observed in 41 analyst queries"},
        {"kind": "metric", "label": "C-30 roll rate",
         "ref": "synapse://metric/c30_roll_rate", "tier": "grounded",
         "detail": "balances rolling current→30+ ÷ prior current balances"},
        {"kind": "skill", "label": "RollRates playbook",
         "ref": "synapse://skill/sbs_rollrates", "tier": "human_asserted",
         "detail": "expert playbook"},
    ]},
    "metrics": [
        {"name": "C-30 roll rate", "ref": "synapse://metric/c30_roll_rate",
         "formula": "sum(bal_roll_c30) / sum(bal_current_prior)",
         "description": "Balances rolling from current to 30+ days past "
                        "due, over prior current balances.",
         "tier": "grounded", "sources": ["skills", "corpus"]},
        {"name": "Approval rate", "ref": "synapse://metric/approval_rate",
         "formula": "count(decision = 'approved') / count(decisioned)",
         "description": "Approved applications over all decisioned "
                        "applications.",
         "tier": "grounded", "sources": ["skills", "corpus", "bq"]},
        {"name": "Net write-off rate",
         "ref": "synapse://metric/net_write_off_rate",
         "formula": "(gross_write_offs - recoveries) / avg_receivables",
         "description": "Write-offs net of recoveries over average "
                        "receivables.",
         "tier": "grounded", "sources": ["skills"]},
        {"name": "Average new-account line",
         "ref": "synapse://metric/avg_new_account_line",
         "formula": "avg(initial_credit_line)",
         "description": "Mean initial line assignment on booked accounts.",
         "tier": "inferred", "sources": ["corpus"]},
    ],
    "terms": [
        {"term": "roll rate", "canonical": {
            "name": "C-30 roll rate", "kind": "Metric",
            "ref": "synapse://metric/c30_roll_rate", "tier": "grounded",
            "sources": ["skills", "corpus"]},
         "matches": [
            {"name": "C-30 roll rate", "kind": "Metric",
             "ref": "synapse://metric/c30_roll_rate", "tier": "grounded",
             "sources": ["skills", "corpus"]},
            {"name": "roll_rate_calc", "kind": "Table",
             "ref": "synapse://table/roll_rate_calc", "tier": "grounded",
             "sources": ["mdm", "bq"]}]},
    ],
    "witness": {
        "synapse://table/sbs_new_accounts": {
            "ref": "synapse://table/sbs_new_accounts", "found": True,
            "kind": "Table",
            "properties": {"table_name": "sbs_new_accounts",
                           "owner": "New Accounts",
                           "lifecycle_status": "active"},
            "provenance": {"tier": "grounded", "score": 0.9,
                           "sources": ["mdm", "bq", "corpus", "skills"]},
            "edges": [{"type": "CONTAINS",
                       "other": "synapse://column/sbs_new_accounts/acct_id",
                       "direction": "out", "tier": "grounded"}]},
    },
    "questions": [
        {"question": "Who owns sbs_new_accounts, and which pipeline feeds "
                     "it?", "archetype": "governance"},
        {"question": "How are new accounts trending month over month?",
         "archetype": "trend"},
        {"question": "What is the C-30 roll rate, and how is it defined?",
         "archetype": "metric"},
        {"question": "Which tables can join to roll_rate_calc, and on what "
                     "keys?", "archetype": "join"},
        {"question": "Show delinquency stage transitions for the latest "
                     "month.", "archetype": "warehouse"},
        {"question": "Why should I trust the approval-rate definition?",
         "archetype": "confidence"},
    ],
}
