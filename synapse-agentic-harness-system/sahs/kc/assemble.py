"""Assemble: every fact the graph holds about one table, as a flat list
the writer, verifier, ledger and exports all key off.

One extractor per coverage family (``coverage.EXTRACTORS``); nothing is
assembled ad hoc. Reads only the promoted build and the graph fold.
Each ``Fact`` carries the Meridian object it came from, the catalog
construct it targets (``kc_target``), the witness families and status
behind it, its provenance, and the include verdict ``include_of``
computes in code: ``copy`` (may be entered into the catalog as is),
``review`` (a human decides first), ``never`` (stays in the graph).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from sahs.graph.quads import GraphDir, NodeRecord, Quad
from sahs.kc import coverage
from sahs.tools.api import Build

COPY, REVIEW, NEVER = "copy", "review", "never"
UNREVIEWED_FLAG = "Suggested — unreviewed"
RESTRICTED_NOTE = "policy unknown — treat as restricted"
PENDING_LABEL = "pending ·"

# join-path tiers: what the evidence lets a reader do with the path
TIER_DECLARED = "declared"      # a constraint the warehouse states
TIER_WITNESSED = "witnessed"    # an ON clause seen in real or published SQL
TIER_SCOPED = "scoped"          # seen between transformed CTEs: not raw-safe
TIER_CANDIDATE = "candidate"    # tables appear together; HOW is unknown

_STATUS_WORD = {
    "certified": "certified", "pending": "pending",
    "pending_certification": "pending", "team_candidate": "team candidate",
    "mined": "mined", "unreviewed": "mined", "rejected": "rejected",
    "deprecated": "deprecated"}


@dataclass
class Fact:
    id: str
    kind: str                 # meridian object kind (table, column, metric…)
    text: str                 # the statement, as a human reads it
    kc_target: str            # construct path, e.g. aspect.join-paths.paths
    representation: str       # coverage vocabulary
    witness: str              # comma-joined witness families
    status: str               # certified | pending | mined | declared | …
    prov: dict[str, Any]      # source, run, evidence, support, agreement…
    subject: str = ""         # the meridian node id
    extractor: str = ""
    include: str = COPY
    reason: str = ""
    showcase: bool = False
    data: Any = None          # structured payload for aspects / exports
    coverage_item: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FactSet:
    table: str
    build_id: str
    graph_run: str
    facts: list[Fact] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)

    def by_target(self, prefix: str) -> list[Fact]:
        return [f for f in self.facts if f.kc_target.startswith(prefix)]

    def get(self, fact_id: str) -> Fact | None:
        return next((f for f in self.facts if f.id == fact_id), None)

    def digest(self) -> str:
        payload = json.dumps([f.to_dict() for f in self.facts],
                             sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


# ── the include rule (code, tested) ──────────────────────────────

def include_of(fact: Fact) -> tuple[str, str]:
    """→ (verdict, reason). The rules of Part II, in order: what never
    leaves, what waits for a human, what may be copied."""
    families = {w.strip() for w in fact.witness.split(",") if w.strip()}
    text = fact.text or ""
    data = fact.data if isinstance(fact.data, dict) else {}
    if fact.representation == coverage.EXCLUDED:
        return NEVER, "no catalog home (see coverage)"
    if "user_variant" in families:
        return NEVER, "a person's on-the-fly variant is not shared truth"
    if fact.kind == "acronym" and data.get("common_word"):
        return NEVER, "anti-alias: a common word flagged as acronym"
    if fact.kind == "query" and data.get("gold") and not data.get("attested"):
        return NEVER, "gold content beyond attested sample queries"
    if fact.kc_target.startswith("aspect.join-paths.paths") \
            and data.get("tier") == TIER_CANDIDATE:
        return NEVER, "a join candidate is never rendered as a join"
    if fact.status in ("rejected", "deprecated"):
        return NEVER, f"{fact.status} definitions do not travel"
    if "llm_enriched" in families:
        if UNREVIEWED_FLAG not in text:
            return NEVER, "model text without its flag"
        return REVIEW, "model-written; a steward has not reviewed it"
    if fact.kc_target.startswith("suggestion."):
        return REVIEW, fact.reason or "a proposal, shown for approval"
    if fact.status == "contested":
        return REVIEW, "contested — steward pending"
    if fact.status == "unknown_policy":
        if RESTRICTED_NOTE not in text:
            return NEVER, "unknown policy without the restricted note"
        return COPY, ""
    if fact.status in ("pending", "team candidate"):
        if fact.representation == coverage.ASPECT \
                or PENDING_LABEL in text:
            return COPY, ""
        return REVIEW, "pending: not governed yet"
    if fact.status == "mined":
        if fact.representation == coverage.ASPECT:
            return COPY, ""
        return REVIEW, "mined only: no catalog or steward has named it"
    return COPY, ""


# ── the fold: sahs/kc/fold.py (indexed, cached per graph state) ──
from sahs.kc.fold import Fold, fold, graph_stamp as _graph_stamp  # noqa: E402


# ── context: one table's slice of everything ─────────────────────

@dataclass
class TableContext:
    build: Build
    graph_root: Path
    physical: str
    nodes: dict[str, NodeRecord]
    edges: dict[tuple, Quad]
    prop_prov: dict[str, dict[str, Any]] = field(default_factory=dict)
    view: Fold | None = None

    @property
    def tid(self) -> str:
        return f"table:{self.physical}"

    def prop_witness(self, node_id: str, prop: str) -> str:
        """The family that wrote this prop, else the node's own."""
        prov = (self.prop_prov.get(node_id) or {}).get(prop)
        if prov is not None and prov.witness:
            return prov.witness
        record = self.nodes.get(node_id)
        return record.prov.witness if record else ""

    def prop_prov_of(self, node_id: str, prop: str) -> dict[str, Any]:
        prov = (self.prop_prov.get(node_id) or {}).get(prop)
        if prov is None:
            record = self.nodes.get(node_id)
            return _prov_of(record) if record else {}
        return {"source": prov.source, "run": prov.run,
                "evidence": prov.evidence, "witness": prov.witness}

    def node(self, node_id: str) -> NodeRecord | None:
        return self.nodes.get(node_id)

    def props(self, node_id: str) -> dict[str, Any]:
        record = self.nodes.get(node_id)
        return dict(record.props) if record else {}

    def out_edges(self, subject: str, relation: str) -> list[Quad]:
        if self.view is not None:
            return self.view.out(subject, relation)
        return [q for (s, r, _o, _w), q in self.edges.items()
                if s == subject and r == relation
                and q.prov.status == "active"]

    def in_edges(self, obj: str, relation: str) -> list[Quad]:
        if self.view is not None:
            return self.view.into(obj, relation)
        return [q for (_s, r, o, _w), q in self.edges.items()
                if o == obj and r == relation and q.prov.status == "active"]

    def nodes_of_kind(self, kind: str) -> list[NodeRecord]:
        if self.view is not None:
            return self.view.of_kind(kind)
        return [n for nid, n in self.nodes.items()
                if nid.split(":", 1)[0] == kind]

    def edges_from(self, subject: str) -> list[Quad]:
        if self.view is not None:
            return self.view.by_s.get(subject, [])
        return [q for (s, _r, _o, _w), q in self.edges.items()
                if s == subject and q.prov.status == "active"]

    def column_ids(self) -> list[str]:
        return sorted({q.o for q in self.out_edges(self.tid, "has_column")})

    def metric_rows(self) -> list[dict[str, Any]]:
        return [m for m in self.build.metrics
                if m.get("table") == self.physical]

    def binding_rows(self) -> list[dict[str, Any]]:
        return [b for b in self.build.bindings
                if b.get("table") == self.physical]

    def join_rows(self) -> list[dict[str, Any]]:
        return [j for j in self.build.joins
                if self.physical in (j.get("a"), j.get("b"))]

    def column_index(self) -> list[dict[str, Any]]:
        return list(self.build.columns.get(self.physical) or [])

    def card(self) -> str:
        path = (self.build.root / "cards" / "tables"
                / f"{self.physical.replace('.', '__')}.md")
        return path.read_text(encoding="utf-8") if path.exists() else ""

    def aux_json(self, name: str) -> Any:
        path = self.build.root / name
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def aux_jsonl(self, name: str) -> list[dict[str, Any]]:
        path = self.build.root / name
        if not path.exists():
            return []
        return [json.loads(line) for line in
                path.read_text(encoding="utf-8").split("\n") if line.strip()]

    def graph_run(self) -> str:
        record = self.nodes.get(self.tid)
        return record.prov.run if record else ""


def _prov_of(quad_or_node: Quad | NodeRecord, **extra: Any) -> dict[str, Any]:
    prov = quad_or_node.prov
    out = {"source": prov.source, "run": prov.run,
           "evidence": prov.evidence, "witness": prov.witness}
    if prov.support is not None:
        out["support"] = prov.support
    if prov.actor:
        out["actor"] = prov.actor
    out.update({k: v for k, v in extra.items() if v not in (None, "", [])})
    return out


def _families(quads: list[Quad]) -> str:
    return ",".join(sorted({q.prov.witness for q in quads if q.prov.witness}))


def _status_of(row: dict[str, Any]) -> str:
    return _STATUS_WORD.get(str(row.get("status_served")
                                or row.get("status") or ""), "mined")


class _Emitter:
    """Collects facts for one extractor; assigns stable ids later."""

    def __init__(self, ctx: TableContext, extractor: str) -> None:
        self.ctx = ctx
        self.extractor = extractor
        self.facts: list[Fact] = []

    def add(self, kind: str, text: str, target: str, *, witness: str,
            status: str, prov: dict[str, Any], subject: str = "",
            data: Any = None, coverage_item: str = "",
            showcase: bool = False, reason: str = "") -> Fact:
        rep = _rep_of_target(target)
        fact = Fact(id="", kind=kind, text=text.strip(), kc_target=target,
                    representation=rep, witness=witness, status=status,
                    prov=prov, subject=subject, extractor=self.extractor,
                    data=data, coverage_item=coverage_item,
                    showcase=showcase, reason=reason)
        self.facts.append(fact)
        return fact


def _rep_of_target(target: str) -> str:
    head = target.split(".", 1)[0]
    return {"entry": coverage.PROSE, "column": coverage.PROSE,
            "glossary": coverage.GLOSSARY, "aspect": coverage.ASPECT,
            "query": coverage.QUERY, "contact": coverage.CONTACT,
            "related": coverage.RELATED, "dq": coverage.DQ,
            "suggestion": coverage.SUGGESTION,
            "excluded": coverage.EXCLUDED}.get(head, coverage.ASPECT)


# ── extractors ───────────────────────────────────────────────────

def _identity(ctx: TableContext, em: _Emitter) -> None:
    record = ctx.node(ctx.tid)
    if record is None:
        return
    p = record.props
    w = lambda prop: ctx.prop_witness(ctx.tid, prop)          # noqa: E731
    pv = lambda prop: ctx.prop_prov_of(ctx.tid, prop)        # noqa: E731
    facts = ctx.build.table_facts(ctx.physical)
    rows = facts.get("total_rows", p.get("total_rows"))
    em.add("table", f"{ctx.physical} is a {p.get('object_type') or 'TABLE'}"
           + (f" with about {rows} rows" if rows is not None else
              " whose row count is not on record"),
           "entry.overview.grain_keys", witness=w("total_rows") or w("object_type"),
           status="declared", prov=pv("total_rows"), subject=ctx.tid,
           data={"object_type": p.get("object_type"), "row_count": rows},
           coverage_item="node:table.total_rows")
    em.add("table", f"object type {p.get('object_type') or 'TABLE'}",
           "aspect.meridian-provenance.object_type",
           witness=w("object_type"), status="declared", prov=pv("object_type"),
           subject=ctx.tid, data=p.get("object_type") or "TABLE",
           coverage_item="node:table.object_type")
    if rows is not None:
        em.add("table", f"row count {rows}",
               "aspect.meridian-provenance.row_count",
               witness=w("total_rows"), status="observed", prov=pv("total_rows"),
               subject=ctx.tid, data=rows, coverage_item="node:table.total_rows")
    if p.get("n_partitions") is not None:
        em.add("table", f"{p['n_partitions']} partitions on record",
               "aspect.meridian-provenance.partitions",
               witness=w("n_partitions"), status="observed", prov=pv("n_partitions"),
               subject=ctx.tid, data=p["n_partitions"],
               coverage_item="node:table.n_partitions")
    latest = facts.get("partition_latest") or p.get("partition_latest")
    if latest:
        em.add("table", f"latest partition {latest}",
               "entry.overview.grain_keys", witness=w("partition_latest"),
               status="observed", prov=pv("partition_latest"), subject=ctx.tid,
               data={"partition_latest": latest},
               coverage_item="node:table.partition_latest")
        em.add("table", f"freshness: a partition for the current period "
               f"exists (latest observed {latest})", "dq.freshness",
               witness=w("partition_latest"), status="observed",
               prov=pv("partition_latest"), subject=ctx.tid,
               data={"rule": "freshness", "column": "partition",
                     "latest": latest},
               coverage_item="node:table.partition_latest")
    lifecycle = p.get("lifecycle_status")
    if lifecycle:
        em.add("table", f"lifecycle status {lifecycle}",
               "aspect.definition-status.lifecycle",
               witness=w("lifecycle_status"), status="declared",
               prov=pv("lifecycle_status"), subject=ctx.tid, data=lifecycle,
               coverage_item="node:table.lifecycle_status")
    for prop, field_name, item in (
            ("schema_fingerprint", "schema_fingerprint", "node:table.schema_fingerprint"),
            ("project", "project", "node:table.project"),
            ("layer_type", "layer", "node:table.layer_type"),
            ("environment", "environment", "node:table.environment"),
            ("appl_id", "application_id", "node:table.appl_id")):
        if p.get(prop) not in (None, ""):
            em.add("table", f"{field_name.replace('_', ' ')} {p[prop]}",
                   f"aspect.meridian-provenance.{field_name}",
                   witness=w(prop), status="declared",
                   prov=pv(prop), subject=ctx.tid, data=p[prop],
                   coverage_item=item)
    if p.get("layer_type"):
        em.add("table", f"{ctx.physical} sits in the {p['layer_type']} layer",
               "entry.overview.purpose", witness=w("layer_type"),
               status="declared", prov=pv("layer_type"), subject=ctx.tid,
               coverage_item="node:table.layer_type")
    if p.get("answerability") not in (None, ""):
        em.add("table", f"answerability {p['answerability']}",
               "aspect.definition-status.answerability",
               witness=w("answerability"), status="observed", prov=pv("answerability"),
               subject=ctx.tid, data=p["answerability"],
               coverage_item="node:table.answerability")
    attributes = {k: v for k, v in p.items()
                  if (k.endswith("_atlas") or k.endswith("_mdm")
                      or k in ("table_meta_logical", "table_metrics",
                               "data_category", "data_sub_category"))
                  and k not in ("description_atlas", "description_mdm",
                                "business_name_atlas", "ownership_atlas",
                                "has_pii_atlas", "has_gdpr_atlas",
                                "has_oncop_atlas", "is_lineage_exist_atlas")}
    if attributes:
        em.add("table", f"{len(attributes)} catalog attributes on record "
               f"({', '.join(sorted(attributes))})",
               "aspect.meridian-provenance.catalog_attributes",
               witness=",".join(sorted({w(k) for k in attributes if w(k)})),
               status="declared", prov={"source": "table node", "run": record.prov.run},
               subject=ctx.tid, data=attributes,
               coverage_item="node:table.*_atlas")
    for q in ctx.out_edges(ctx.tid, "has_schema"):
        sp = ctx.props(q.o)
        em.add("schema", f"schema version {q.o.split('@', 1)[-1]} "
               f"({sp.get('n_columns', '?')} columns, fingerprint "
               f"{sp.get('fingerprint', '?')})",
               "aspect.meridian-provenance.schema_version",
               witness=q.prov.witness, status="observed", prov=_prov_of(q),
               subject=q.o, data={"version": q.o, **sp},
               coverage_item="edge:has_schema")


def _description(ctx: TableContext, em: _Emitter) -> None:
    record = ctx.node(ctx.tid)
    if record is None:
        return
    p = record.props
    for prop, wit in (("description_atlas", "atlas"),
                      ("description_bq", "bq"), ("description_mdm", "lumi")):
        if p.get(prop):
            em.add("table", str(p[prop]), "entry.description",
                   witness=ctx.prop_witness(ctx.tid, prop) or wit,
                   status="declared", prov=ctx.prop_prov_of(ctx.tid, prop),
                   subject=ctx.tid, data={"plane": prop},
                   coverage_item=f"node:table.{prop}")
            em.add("table", str(p[prop]), "entry.overview.purpose",
                   witness=ctx.prop_witness(ctx.tid, prop) or wit,
                   status="declared", prov=ctx.prop_prov_of(ctx.tid, prop),
                   subject=ctx.tid, coverage_item=f"node:table.{prop}")
    meta = p.get("table_meta_logical") or {}
    if isinstance(meta, dict) and meta.get("description") \
            and not p.get("description_atlas"):
        em.add("table", str(meta["description"]), "entry.overview.purpose",
               witness="bq", status="declared", prov=_prov_of(record),
               subject=ctx.tid, coverage_item="node:table.table_meta_logical")
    if p.get("business_name_atlas"):
        em.add("table", f"business name: {p['business_name_atlas']}",
               "glossary.term", witness="atlas", status="declared",
               prov=_prov_of(record), subject=ctx.tid,
               data={"term": p["business_name_atlas"],
                     "definition": p.get("description_atlas") or "",
                     "category": "Tables", "related_entries": [ctx.physical]},
               coverage_item="node:table.business_name_atlas")
    card = ctx.card()
    for line in card.splitlines():
        if line.startswith("- purpose: "):
            em.add("table", "served card purpose line: "
                   + line[len("- purpose: "):].rsplit(" [prov:", 1)[0],
                   "aspect.meridian-provenance.served_purpose",
                   witness="bq,lumi,atlas", status="consensus",
                   prov={"source": "compiled card", "run": ctx.build.version},
                   subject=ctx.tid, coverage_item="card:table.preamble")


def _stewardship(ctx: TableContext, em: _Emitter) -> None:
    record = ctx.node(ctx.tid)
    p = record.props if record else {}
    ownership = p.get("ownership_atlas") or {}
    if isinstance(ownership, dict):
        for role, who in sorted(ownership.items()):
            # a person or team, not an id or a code the catalog keeps
            # alongside (those ride in catalog_attributes)
            if who and any(k in role for k in ("owner", "steward", "vp",
                                                "lead", "contact", "manager",
                                                "custodian", "director")):
                em.add("owner", f"{role.replace('_', ' ')}: {who}", "contact",
                       witness=ctx.prop_witness(ctx.tid, "ownership_atlas") or "atlas",
                       status="declared",
                       prov=ctx.prop_prov_of(ctx.tid, "ownership_atlas"), subject=ctx.tid,
                       data={"role": role, "name": str(who)},
                       coverage_item="node:table.ownership_atlas")
    for q in ctx.out_edges(ctx.tid, "owned_by"):
        op = ctx.props(q.o)
        role = q.props.get("role") or op.get("role") or "owner"
        em.add("owner", f"{role.replace('_', ' ')}: {op.get('owner') or q.o.split(':', 1)[1]}",
               "contact", witness=q.prov.witness, status="declared",
               prov=_prov_of(q), subject=q.o,
               data={"role": role, "name": op.get("owner") or q.o},
               coverage_item="edge:owned_by")
    if p.get("business_unit"):
        em.add("table", f"business unit {p['business_unit']}", "contact",
               witness=ctx.prop_witness(ctx.tid, "business_unit"), status="declared",
               prov=ctx.prop_prov_of(ctx.tid, "business_unit"), subject=ctx.tid,
               data={"role": "business_unit", "name": p["business_unit"]},
               coverage_item="node:table.business_unit")
    for q in ctx.out_edges(ctx.tid, "in_lob"):
        lp = ctx.props(q.o)
        role = q.props.get("role") or ""
        note = q.props.get("note") or ""
        label = lp.get("name") or lp.get("code") or q.o.split(":", 1)[1]
        em.add("lob", f"belongs to {label}"
               + (f" ({role})" if role else "")
               + (f": {note}" if note else ""),
               "entry.overview.ownership_usage", witness=q.prov.witness,
               status="declared", prov=_prov_of(q), subject=q.o,
               data={"lob": q.o, "code": lp.get("code"), "name": lp.get("name"),
                     "kind": lp.get("kind", "lob"), "parent": lp.get("parent", ""),
                     "role": role}, coverage_item="edge:in_lob")
        em.add("lob", f"glossary category {label}"
               + (f" under {lp['parent']}" if lp.get("parent") else ""),
               "glossary.category", witness=q.prov.witness, status="declared",
               prov=_prov_of(q), subject=q.o,
               data={"name": lp.get("code") or label,
                     "description": lp.get("name") or "",
                     "parent": lp.get("parent") or "",
                     "level": lp.get("kind", "lob")},
               coverage_item="node:lob")
        em.add("lob", f"line of business {lp.get('code') or label}", "contact",
               witness=q.prov.witness, status="declared", prov=_prov_of(q),
               subject=q.o, data={"role": "line_of_business",
                                  "name": lp.get("code") or label},
               coverage_item="edge:in_lob")
    for m in ctx.metric_rows():
        for q in ctx.out_edges(m["id"], "in_domain"):
            dp = ctx.props(q.o)
            em.add("mdom", f"glossary category (metric domain) "
                   f"{dp.get('name') or q.o}", "glossary.category",
                   witness=q.prov.witness, status="declared",
                   prov=_prov_of(q), subject=q.o,
                   data={"name": dp.get("name") or q.o.split(":", 1)[1],
                         "description": "metric domain", "parent": "",
                         "level": "domain"}, coverage_item="node:mdom")


def _usage(ctx: TableContext, em: _Emitter) -> None:
    record = ctx.node(ctx.tid)
    p = record.props if record else {}
    users = p.get("top_users") or []
    if users:
        em.add("table", f"{len(users)} distinct heavy users in the 30-day "
               "history (names withheld)", "aspect.usage-profile.distinct_users",
               witness=ctx.prop_witness(ctx.tid, "top_users") or "jobs_30d",
               status="observed", prov=ctx.prop_prov_of(ctx.tid, "top_users"),
               subject=ctx.tid, data=len(users),
               coverage_item="node:table.top_users")
    if p.get("usage_rhythm"):
        em.add("table", "peak hours: " + " · ".join(p["usage_rhythm"]),
               "aspect.usage-profile.peak_hours",
               witness=ctx.prop_witness(ctx.tid, "usage_rhythm") or "jobs_30d",
               status="observed", prov=ctx.prop_prov_of(ctx.tid, "usage_rhythm"),
               subject=ctx.tid,
               data=list(p["usage_rhythm"]),
               coverage_item="node:table.usage_rhythm")
    prior = ctx.build.cost_priors.get(ctx.physical) or p.get("cost_prior")
    if prior:
        em.add("table", f"bytes per query p50 {prior.get('p50_bytes')} · "
               f"p95 {prior.get('p95_bytes')} over {prior.get('n_jobs')} jobs",
               "aspect.usage-profile.bytes_per_query", witness="jobs_30d",
               status="observed", prov={"source": "cost_priors",
                                        "run": ctx.build.version},
               subject=ctx.tid, data=dict(prior),
               coverage_item="index:cost_priors.p50_bytes")
    for q in ctx.out_edges(ctx.tid, "used_by"):
        lp = ctx.props(q.o)
        em.add("lob", f"used by {lp.get('name') or q.o.split(':', 1)[1]} "
               f"({q.prov.support or 1} runs)",
               "aspect.usage-profile.used_by_lob", witness=q.prov.witness,
               status="observed", prov=_prov_of(q), subject=q.o,
               data={"lob": q.o, "name": lp.get("name"),
                     "support": q.prov.support or 1,
                     "first_seen": q.props.get("first_seen"),
                     "last_seen": q.props.get("last_seen")},
               coverage_item="edge:used_by")
        em.add("lob", f"queries run mostly from {lp.get('name') or q.o} "
               "(usage, not ownership)", "entry.overview.ownership_usage",
               witness=q.prov.witness, status="observed", prov=_prov_of(q),
               subject=q.o, coverage_item="edge:used_by")
    templates = [n for n in ctx.nodes_of_kind("tmpl")
                 if n.props.get("table") == ctx.physical]
    if templates:
        em.add("template", f"{len(templates)} recurring query shapes, "
               f"{sum(int(n.props.get('occurrences') or 0) for n in templates)} "
               "runs (shapes are parameterized, never sample queries)",
               "aspect.usage-profile.query_templates", witness="jobs_30d",
               status="observed", prov=_prov_of(templates[0]), subject=ctx.tid,
               data=[{"shape": n.props.get("normalized_sql"),
                      "runs": n.props.get("occurrences")} for n in templates],
               coverage_item="node:tmpl")
    runs = [m for m in ctx.metric_rows() if m.get("execution_count")]
    if runs:
        em.add("metric", f"{len(runs)} metrics carry catalog run counts",
               "aspect.usage-profile.metric_runs", witness="catalog_mined",
               status="observed", prov={"source": "metrics index",
                                        "run": ctx.build.version},
               subject=ctx.tid,
               data=[{"metric": m["id"], "label": m.get("label"),
                      "runs": m["execution_count"]} for m in runs],
               coverage_item="node:metric.execution_count")


def _columns(ctx: TableContext, em: _Emitter) -> None:
    index = {row["name"]: row for row in ctx.column_index()}
    schema = ctx.build.schema.get(ctx.physical) or {}
    tickets = [t for t in ctx.aux_jsonl("tickets.jsonl")
               if t.get("table") == ctx.physical]
    for name, dtype in schema.items():
        row = index.get(name) or {"name": name, "type": dtype}
        cid = f"col:{ctx.physical}.{name}"
        node = ctx.node(cid)
        cp = node.props if node else {}
        description = row.get("description") or ""
        source = row.get("description_source") or ""
        business = row.get("business_name") or cp.get("business_name_atlas") or ""
        pieces = []
        if business:
            pieces.append(business)
        if description:
            pieces.append(description)
        text = ". ".join(pieces) if pieces else ""
        witness = ",".join(sorted(w for w in {
            row.get("type_source") or "", source} if w)) or "bq"
        agreement = int(row.get("agreement") or 1)
        if text:
            em.add("column", f"{name} ({row.get('type') or dtype}): {text}",
                   "column.description", witness=witness,
                   status="consensus" if agreement > 1 else "declared",
                   prov={"source": source or row.get("type_source") or "bq",
                         "run": ctx.build.version, "agreement": agreement},
                   subject=cid, data={"column": name, "type": row.get("type") or dtype,
                                      "description": text, "source": source,
                                      "business_name": business},
                   coverage_item="index:columns.description")
        else:
            em.add("column", f"{name} ({row.get('type') or dtype}): no business "
                   "meaning on record", "suggestion.column_description",
                   witness=witness, status="declared",
                   prov={"source": "columns index", "run": ctx.build.version},
                   subject=cid, data={"column": name, "type": row.get("type") or dtype},
                   coverage_item="index:columns.ungoverned", showcase=True,
                   reason="no witness yet: a column with no meaning on record")
        if row.get("supplementary"):
            em.add("column", f"{name}: {row['supplementary']}",
                   "column.description", witness="lumi", status="declared",
                   prov={"source": "lumi", "run": ctx.build.version},
                   subject=cid, data={"column": name, "supplementary": True,
                                      "description": row["supplementary"]},
                   coverage_item="index:columns.supplementary")
        if cp.get("nested_path"):
            em.add("column", f"{name} is nested at {cp['nested_path']}",
                   "column.description", witness="bq", status="declared",
                   prov=_prov_of(node), subject=cid,
                   data={"column": name, "nested_path": cp["nested_path"]},
                   coverage_item="node:col.nested_path")
        if row.get("sensitive"):
            em.add("column", f"{name} is sensitive "
                   f"(declared by {', '.join(row.get('sensitivity_sources') or ['?'])})",
                   "column.sensitivity",
                   witness=",".join(row.get("sensitivity_sources") or ["bq"]),
                   status="declared", prov={"source": "columns index",
                                            "run": ctx.build.version},
                   subject=cid, data={"column": name, "sensitive": True,
                                      "declared_by": row.get("sensitivity_sources") or [],
                                      "pii_role": cp.get("pii_role_id"),
                                      "sde_group": cp.get("sde_group")},
                   coverage_item="index:columns.sensitive")
        for flag in row.get("flags") or []:
            em.add("column", f"{name} carries structural flag {flag}",
                   "aspect.definition-status.column_flags",
                   witness="bq,lumi,atlas", status="observed",
                   prov={"source": "reconcile", "run": ctx.build.version},
                   subject=cid, data={"column": name, "flag": flag},
                   coverage_item="index:columns.flags")
        if cp.get("null_count") is not None:
            em.add("column", f"{name}: {cp['null_count']} nulls in the profiled "
                   f"rows" + (f" ({cp.get('profile_coverage')})"
                              if cp.get("profile_coverage") else ""),
                   "dq.not_null", witness="bq", status="observed",
                   prov=_prov_of(node), subject=cid,
                   data={"rule": "not_null", "column": name,
                         "null_count": cp["null_count"],
                         "coverage": cp.get("profile_coverage")},
                   coverage_item="node:col.null_count")
        if cp.get("nullable_atlas") is False:
            em.add("column", f"{name} is declared NOT NULL by the catalog",
                   "dq.not_null", witness="atlas", status="declared",
                   prov=_prov_of(node), subject=cid,
                   data={"rule": "not_null", "column": name, "declared": True},
                   coverage_item="node:col.nullable_atlas")
        if cp.get("observed_via"):
            em.add("column", f"{name} observed via {cp['observed_via']}",
                   "aspect.definition-status.columns_observed_via",
                   witness="bq", status="observed", prov=_prov_of(node),
                   subject=cid, data={"column": name, "via": cp["observed_via"]},
                   coverage_item="node:col.observed_via")
        disagreements = {k: v for k, v in cp.items()
                         if k in ("data_type_atlas", "data_type_mdm",
                                  "column_name_atlas")}
        if disagreements:
            em.add("column", f"{name}: the catalogs also say "
                   + ", ".join(f"{k}={v}" for k, v in sorted(disagreements.items())),
                   "aspect.definition-status.type_disagreements",
                   witness="atlas,lumi", status="observed", prov=_prov_of(node),
                   subject=cid, data={"column": name, **disagreements},
                   coverage_item="node:col.data_type_atlas")
        plane = {k: v for k, v in cp.items()
                 if (k.endswith("_atlas") or k.endswith("_mdm"))
                 and k not in ("description_atlas", "description_mdm",
                               "business_name_atlas", "data_type_atlas",
                               "data_type_mdm", "column_name_atlas",
                               "is_pii_mdm", "is_primary_key_atlas",
                               "is_partitioning_atlas", "nullable_atlas",
                               "ordinal_atlas")}
        if plane:
            em.add("column", f"{name}: {len(plane)} catalog attributes",
                   "aspect.meridian-provenance.column_attributes",
                   witness="atlas,lumi", status="declared", prov=_prov_of(node),
                   subject=cid, data={"column": name, **plane},
                   coverage_item="node:col.*_atlas")
    for t in tickets:
        em.add("ticket", f"{t.get('ticket')}: {t.get('column')} — {t.get('detail')}",
               "aspect.definition-status.tickets", witness="bq,lumi,atlas",
               status="observed", prov={"source": "structural census",
                                        "run": ctx.build.version},
               subject=f"col:{ctx.physical}.{t.get('column')}",
               data=dict(t), coverage_item="report:tickets.ticket")
    important = sorted(index.values(),
                       key=lambda r: (-int(r.get("agreement") or 1),
                                      not r.get("description"), r["name"]))[:8]
    for row in important:
        if row.get("description"):
            em.add("column", f"{row['name']}: {row['description']}",
                   "entry.overview.columns_that_matter",
                   witness=row.get("description_source") or "bq",
                   status="consensus" if int(row.get("agreement") or 1) > 1
                   else "declared",
                   prov={"source": row.get("description_source") or "bq",
                         "run": ctx.build.version,
                         "agreement": row.get("agreement")},
                   subject=f"col:{ctx.physical}.{row['name']}",
                   coverage_item="card:table.columns")


def _terms(ctx: TableContext, em: _Emitter) -> None:
    subjects = [ctx.tid] + ctx.column_ids()
    seen_terms: set[str] = set()
    for subject in subjects:
        for q in ctx.out_edges(subject, "mapped_term"):
            tp = ctx.props(q.o)
            name = tp.get("name") or q.o
            target = (subject.split(":", 1)[1])
            note = ", ".join(f"{k} {v}" for k, v in sorted(q.props.items())
                             if k in ("mapping_type", "confidence",
                                      "mapping_source", "matched_on") and v)
            em.add("term", f"{target} is the {name}"
                   + (f" ({note})" if note else ""),
                   "glossary.related_entry", witness=q.prov.witness,
                   status="declared", prov=_prov_of(q), subject=q.o,
                   data={"term": name, "target": target,
                         "mapping": dict(q.props)},
                   coverage_item="edge:mapped_term")
            if q.o not in seen_terms:
                seen_terms.add(q.o)
                status_word = tp.get("status") or ""
                em.add("term", f"{name}"
                       + (f": {tp['description']}" if tp.get("description")
                          else "")
                       + (f" (catalog status {status_word})" if status_word
                          else ""),
                       "glossary.term", witness=q.prov.witness,
                       status="declared", prov=_prov_of(ctx.node(q.o) or q),
                       subject=q.o,
                       data={"term": name,
                             "definition": tp.get("description") or "",
                             "status_note": status_word,
                             "category": "Business terms",
                             "source_id": tp.get("term_id"),
                             "related_entries": [target]},
                       coverage_item="node:term")
            for a in ctx.in_edges(q.o, "alias_of"):
                ap = ctx.props(a.s)
                scope = f"{ap.get('business_unit') or 'All'}/{ap.get('region') or 'All'}"
                em.add("acronym", f"{ap.get('symbol') or a.s} is a synonym of "
                       f"{name} (scope {scope})", "glossary.synonym",
                       witness=a.prov.witness, status="declared",
                       prov=_prov_of(a), subject=a.s,
                       data={"synonym": ap.get("symbol"), "term": name,
                             "expansion": ap.get("definition"),
                             "scope": scope,
                             "common_word": bool(ap.get("common_word"))},
                       coverage_item="edge:alias_of")
    # acronyms whose expansion names this table's business name or a
    # column's business name, without an alias edge: scoped synonyms
    names = {str(v).lower() for v in (
        [ctx.props(ctx.tid).get("business_name_atlas")]
        + [ctx.props(c).get("business_name_atlas") for c in ctx.column_ids()]
        + [r.get("business_name") for r in ctx.column_index()]) if v}
    for node in ctx.nodes_of_kind("acr"):
        nid = node.id
        definition = str(node.props.get("definition") or "").lower()
        if definition and definition in names:
            scope = (f"{node.props.get('business_unit') or 'All'}/"
                     f"{node.props.get('region') or 'All'}")
            em.add("acronym", f"{node.props.get('symbol')} is a synonym of "
                   f"{node.props.get('definition')} (scope {scope})",
                   "glossary.synonym", witness=node.prov.witness,
                   status="declared", prov=_prov_of(node), subject=nid,
                   data={"synonym": node.props.get("symbol"),
                         "term": node.props.get("definition"),
                         "expansion": node.props.get("definition"),
                         "scope": scope,
                         "common_word": bool(node.props.get("common_word"))},
                   coverage_item="node:acr")


def _lineage(ctx: TableContext, em: _Emitter) -> None:
    record = ctx.node(ctx.tid)
    p = record.props if record else {}
    for prop, field_name in (("feed_type", "feed_type"),
                             ("source_system", "source_system"),
                             ("pipeline_name", "pipeline"),
                             ("is_lineage_exist_atlas", "catalog_declares_lineage")):
        if p.get(prop) not in (None, ""):
            em.add("table", f"{field_name.replace('_', ' ')}: {p[prop]}",
                   f"aspect.lineage-notes.{field_name}",
                   witness=ctx.prop_witness(ctx.tid, prop), status="declared",
                   prov=ctx.prop_prov_of(ctx.tid, prop), subject=ctx.tid, data=p[prop],
                   coverage_item=f"node:table.{prop}")
    for q in ctx.in_edges(ctx.tid, "upstream_of"):
        em.add("table", f"fed from {q.s.split(':', 1)[1]}",
               "aspect.lineage-notes.upstream", witness=q.prov.witness,
               status="declared", prov=_prov_of(q), subject=q.s,
               data=q.s.split(":", 1)[1], coverage_item="edge:upstream_of")
        em.add("table", f"upstream: {q.s.split(':', 1)[1]} (lineage as the "
               "MDM records it; the catalog's lineage API needs process "
               "events, so this stays a note)", "entry.overview.provenance",
               witness=q.prov.witness, status="declared", prov=_prov_of(q),
               subject=q.s, coverage_item="edge:upstream_of")
    for q in ctx.out_edges(ctx.tid, "upstream_of"):
        em.add("table", f"feeds {q.o.split(':', 1)[1]}",
               "aspect.lineage-notes.downstream", witness=q.prov.witness,
               status="declared", prov=_prov_of(q), subject=q.o,
               data=q.o.split(":", 1)[1], coverage_item="edge:upstream_of")
    for cid in ctx.column_ids():
        for q in ctx.out_edges(cid, "derived_from"):
            logic = q.props.get("derivation_logic") or ""
            em.add("column", f"{cid.split('.')[-1]} derives from "
                   f"{q.o.split(':', 1)[1]}" + (f" as {logic}" if logic else ""),
                   "aspect.lineage-notes.column_derivations",
                   witness=q.prov.witness, status="declared", prov=_prov_of(q),
                   subject=cid, data={"column": cid.split(".")[-1],
                                      "from": q.o.split(":", 1)[1],
                                      "logic": logic},
                   coverage_item="edge:derived_from")
        cp = ctx.props(cid)
        if cp.get("derived_logic"):
            em.add("column", f"{cid.split('.')[-1]}: derived logic "
                   f"{cp['derived_logic']}",
                   "aspect.lineage-notes.column_derivations",
                   witness="lumi", status="declared",
                   prov=_prov_of(ctx.node(cid)), subject=cid,
                   data={"column": cid.split(".")[-1],
                         "logic": cp["derived_logic"]},
                   coverage_item="node:col.derived_logic")


def _keys(ctx: TableContext, em: _Emitter) -> None:
    facts = ctx.build.table_facts(ctx.physical)
    pk = list(facts.get("primary_key") or [])
    if pk:
        em.add("table", f"primary key ({', '.join(pk)})",
               "entry.overview.grain_keys", witness="bq", status="declared",
               prov={"source": "tables index", "run": ctx.build.version},
               subject=ctx.tid, data={"primary_key": pk},
               coverage_item="index:tables.primary_key")
        em.add("table", f"uniqueness: ({', '.join(pk)}) is unique",
               "dq.uniqueness", witness="bq", status="declared",
               prov={"source": "tables index", "run": ctx.build.version},
               subject=ctx.tid, data={"rule": "uniqueness", "columns": pk},
               coverage_item="index:tables.primary_key")
    for cid in ctx.column_ids():
        cp = ctx.props(cid)
        name = cid.split(".")[-1]
        if cp.get("is_primary_key_atlas") and name not in pk:
            em.add("column", f"uniqueness: {name} is a catalog-declared key",
                   "dq.uniqueness", witness="atlas", status="declared",
                   prov=_prov_of(ctx.node(cid)), subject=cid,
                   data={"rule": "uniqueness", "columns": [name],
                         "declared_by": "atlas"},
                   coverage_item="node:col.is_primary_key_atlas")
        if cp.get("is_partitioning"):
            em.add("column", f"partitioned by {name}",
                   "entry.overview.grain_keys", witness="bq", status="declared",
                   prov=_prov_of(ctx.node(cid)), subject=cid,
                   data={"partition_column": name},
                   coverage_item="node:col.is_partitioning")
        for q in ctx.out_edges(cid, "fk_references"):
            other = ".".join(q.o.split(":", 1)[1].split(".")[:2])
            em.add("join", f"{name} references {q.o.split(':', 1)[1]}"
                   + (f" (constraint {q.props['constraint']})"
                      if q.props.get("constraint") else ""),
                   "aspect.join-paths.paths", witness=q.prov.witness,
                   status="declared", prov=_prov_of(q), subject=cid,
                   data={"tier": TIER_DECLARED, "other": other,
                         "on": [f"{cid.split(':', 1)[1]} = {q.o.split(':', 1)[1]}"],
                         "constraint": q.props.get("constraint"),
                         "source": "constraints"},
                   coverage_item="edge:fk_references")
            em.add("join", f"referential integrity: every {name} exists in "
                   f"{q.o.split(':', 1)[1]}", "dq.referential",
                   witness=q.prov.witness, status="declared", prov=_prov_of(q),
                   subject=cid, data={"rule": "referential", "column": name,
                                      "references": q.o.split(":", 1)[1]},
                   coverage_item="edge:fk_references")


def _metrics(ctx: TableContext, em: _Emitter) -> None:
    rows = sorted(ctx.metric_rows(),
                  key=lambda m: (-int(m.get("authority") or 0),
                                 -int(m.get("support") or 0), m["id"]))
    for m in rows:
        status = _status_of(m)
        label = m.get("label") or m["id"]
        origin = m.get("evidence_origin") or m.get("source") or ""
        witness = ",".join(sorted(m.get("support_by_witness") or {"?": 1}))
        support = int(m.get("support") or 0)
        agreement = int(m.get("witness_agreement") or 1)
        prov = {"source": m.get("source"), "run": ctx.build.version,
                "support": support, "agreement": agreement,
                "evidence_origin": origin, "fp": m.get("fp")}
        parts = []
        if m.get("question"):
            parts.append(m["question"])
        if m.get("grain"):
            parts.append(f"grain: {m['grain']}")
        if m.get("expression_prose"):
            parts.append(f"calculation: {m['expression_prose']}")
        elif m.get("description"):
            parts.append(m["description"])
        definition = " · ".join(parts)
        # facts that repeat the model's words carry its witness; a fact
        # that only names the metric and its status does not
        base_witness = witness
        if m.get("question_source") == "llm_enriched" \
                or m.get("grain_source") == "llm_enriched":
            definition = f"{UNREVIEWED_FLAG}: {definition}"
            witness = witness + ",llm_enriched"
        status_note = ""
        if status in ("pending", "team candidate"):
            status_note = f"{PENDING_LABEL} {origin} ×{support}"
            definition = f"{status_note} — {definition}" if definition \
                else status_note
        elif status == "mined":
            status_note = f"mined · {origin} ×{support}"
        em.add("metric", f"metric {label} ({status}): {definition or 'no definition text on record'}",
               "aspect.governed-metrics.metrics", witness=witness,
               status=status, prov=prov, subject=m["id"],
               data={"id": m["id"], "label": label, "status": status,
                     "status_served": m.get("status_served"),
                     "canonical_sql": m.get("canonical_sql"),
                     "fingerprint": m.get("fp"),
                     "fingerprint_version": ctx.props(m["id"]).get("canon_version"),
                     "evidence_origin": origin,
                     "witnesses": m.get("support_by_witness") or {},
                     "agreement": agreement, "support": support,
                     "grain": m.get("grain"), "grain_source": m.get("grain_source"),
                     "grain_observed": m.get("grain_observed"),
                     "question": m.get("question"),
                     "question_source": m.get("question_source"),
                     "approved_dimensions": m.get("approved_dimensions") or [],
                     "sign_convention": m.get("sign_convention"),
                     "filters": m.get("common_filters") or [],
                     "families": m.get("mgroups") or [],
                     "catalog_id": m.get("mgroup"),
                     "domain": m.get("domain"),
                     "line_of_business": m.get("line_of_business"),
                     "business_unit": m.get("business_unit"),
                     "data_category": m.get("data_category"),
                     "scope": m.get("scope"),
                     "products": m.get("product_ids") or ctx.props(m["id"]).get("products") or [],
                     "base_tables": m.get("base_tables") or [],
                     "tables_associated": m.get("tables_associated_not_referenced") or [],
                     "join_condition": m.get("join_condition"),
                     "join_conditions": m.get("join_conditions") or [],
                     "approval": m.get("approval") or {},
                     "miner_confidence": m.get("confidence"),
                     "sql_source": m.get("sql_source"),
                     "group_by_patterns": m.get("group_by_patterns") or [],
                     "query_shape": m.get("query_shape") or [],
                     "author": m.get("author"),
                     "requestor": ctx.props(m["id"]).get("requestor"),
                     "label_usage": ctx.props(m["id"]).get("label_usage"),
                     "skill_pack": m.get("source") if m.get("source") == "skill_contract" else "",
                     "last_seen": m.get("last_seen"),
                     "seen_by_witness": m.get("seen_by_witness") or {},
                     "status_note": status_note},
               coverage_item="index:metrics.status_served")
        if status == "certified":
            em.add("metric", f"{label}: {definition or 'certified metric'}",
                   "glossary.term", witness=witness, status=status, prov=prov,
                   subject=m["id"],
                   data={"term": label, "definition": definition,
                         "category": (m.get("domain") or m.get("line_of_business")
                                      or "Metrics"),
                         "status_note": "certified",
                         "related_entries": [ctx.physical],
                         "synonyms": [], "related_terms": []},
                   coverage_item="node:metric")
            em.add("metric", f"{label} is measured on {ctx.physical}",
                   "glossary.related_entry", witness=base_witness, status=status,
                   prov=prov, subject=m["id"],
                   data={"term": label, "target": ctx.physical},
                   coverage_item="edge:measured_on")
            if m.get("canonical_sql"):
                em.add("query", f"{label} (certified): {m['canonical_sql']}",
                       "query", witness=witness, status=status, prov=prov,
                       subject=m["id"],
                       data={"sql": m["canonical_sql"], "description": label,
                             "kind": "certified_metric", "source": "User"},
                       coverage_item="node:metric.canonical_sql")
            em.add("metric", f"{label} is certified: {definition or label}",
                   "entry.overview.metrics", witness=witness, status=status,
                   prov=prov, subject=m["id"], coverage_item="card:table.metrics_available")
        elif status in ("pending", "team candidate"):
            em.add("metric", f"{label}: {definition}", "glossary.term",
                   witness=witness, status=status, prov=prov, subject=m["id"],
                   data={"term": label, "definition": definition,
                         "category": "Candidate terms",
                         "status_note": status_note,
                         "related_entries": [ctx.physical],
                         "synonyms": [], "related_terms": []},
                   coverage_item="node:metric")
            em.add("metric", f"{label} is {status} ({origin}, support {support})",
                   "entry.overview.metrics", witness=base_witness, status=status,
                   prov=prov, subject=m["id"], coverage_item="card:table.metrics_available")
            em.add("metric", f"proposed metric {label}: {definition}",
                   "suggestion.metric", witness=witness, status=status, prov=prov,
                   subject=m["id"], showcase=True,
                   reason="pending: needs a steward's decision before it is governed",
                   data={"label": label, "support": support,
                         "agreement": agreement, "rank": support * agreement,
                         "question_enriched": m.get("question_source") == "llm_enriched",
                         "grain_enriched": m.get("grain_source") == "llm_enriched"},
                   coverage_item="node:metric")
        else:
            em.add("metric", f"mined metric {label} ({origin}, support {support}): "
                   f"{m.get('canonical_sql') or ''}", "suggestion.metric",
                   witness=witness, status=status, prov=prov, subject=m["id"],
                   showcase=True,
                   reason="mined only: no catalog or steward has named it",
                   data={"label": label, "support": support,
                         "agreement": agreement, "rank": support * agreement},
                   coverage_item="node:metric")
        for who, role in ((m.get("author"), "metric author"),
                          (ctx.props(m["id"]).get("requestor"), "requestor")):
            if who and status == "certified":
                em.add("owner", f"{role} of {label}: {who}", "contact",
                       witness=witness, status=status, prov=prov,
                       subject=m["id"], data={"role": role, "name": str(who),
                                              "metric": label},
                       coverage_item="node:metric.author")
        for owners, wit in ((m.get("data_owners") or [], "studio"),
                            (m.get("data_owners_dmp") or [], "dmp")):
            for who in owners:
                em.add("owner", f"data owner ({label}): {who}", "contact",
                       witness=wit, status="declared", prov=prov,
                       subject=m["id"], data={"role": "data_owner",
                                              "name": str(who), "metric": label},
                       coverage_item="node:metric.data_owners")
        for q in ctx.out_edges(m["id"], "variant_of"):
            parent = next((r for r in ctx.build.metrics if r["id"] == q.o), None)
            parent_label = (parent or {}).get("label") or q.o
            if parent_label == label:
                continue
            em.add("metric", f"{label} is a variant of {parent_label}"
                   + (f": {m.get('sign_convention')}" if m.get("sign_convention") else ""),
                   "glossary.related_term", witness=q.prov.witness, status=status,
                   prov=_prov_of(q), subject=m["id"],
                   data={"term": label, "related": parent_label,
                         "relation": "variant of",
                         "delta": m.get("sign_convention") or m.get("scope") or ""},
                   coverage_item="edge:variant_of")
        for q in ctx.out_edges(m["id"], "certified_as"):
            if q.props.get("note"):
                em.add("metric", f"{label} status note: {q.props['note']}",
                       "aspect.governed-metrics.status_notes",
                       witness=q.prov.witness, status=status, prov=_prov_of(q),
                       subject=m["id"], data={"metric": label,
                                              "note": q.props["note"]},
                       coverage_item="edge:certified_as.note")


def _concepts(ctx: TableContext, em: _Emitter) -> None:
    rows = ctx.binding_rows()
    by_label: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for b in rows:
        by_label[b["label"]].append(b)
    for label, group in sorted(by_label.items()):
        classes = {b["fp"] for b in group}
        contested = len(classes) > 1
        for b in sorted(group, key=lambda r: (-int(r.get("authority") or 0),
                                              -int(r.get("support") or 0))):
            witness = ",".join(sorted(b.get("support_by_witness") or {b.get("source", "?")}))
            status = "contested" if contested else "witnessed"
            em.add("concept", f"'{label}' on {ctx.physical} means "
                   f"{b['canonical_sql']}" + (" (contested — steward pending)"
                                             if contested else ""),
                   "aspect.concept-bindings.bindings", witness=witness,
                   status=status,
                   prov={"source": b.get("source"), "run": ctx.build.version,
                         "support": b.get("support"), "agreement": b.get("agreement"),
                         "fp": b["fp"]},
                   subject=f"concept:{label}@table:{ctx.physical}",
                   data={"label": label, "predicate_sql": b["canonical_sql"],
                         "fingerprint": b["fp"], "support": b.get("support"),
                         "witnesses": b.get("support_by_witness") or {},
                         "agreement": b.get("agreement"), "contested": contested,
                         "last_seen": b.get("last_seen"),
                         "recency_source": b.get("recency_source")},
                   coverage_item="index:bindings.label")
        best = group[0]
        node = ctx.node(f"concept:{label}@table:{ctx.physical}")
        enriched = (node.props.get("description_enriched") if node else "") or ""
        if not contested and int(best.get("support") or 0) >= 1 \
                and best.get("source") not in ("blue_insights",):
            em.add("concept", f"{label}: filter meaning {best['canonical_sql']}",
                   "glossary.term", witness=",".join(sorted(best.get("support_by_witness") or {})),
                   status="witnessed", prov={"source": best.get("source"),
                                             "run": ctx.build.version},
                   subject=f"concept:{label}@table:{ctx.physical}",
                   data={"term": label, "definition": f"filter: {best['canonical_sql']}",
                         "category": "Concepts", "status_note": "witnessed",
                         "related_entries": [ctx.physical], "synonyms": [],
                         "related_terms": []},
                   coverage_item="node:concept")
            em.add("concept", f"{label} applies to {ctx.physical}",
                   "glossary.related_entry", witness=best.get("source", ""),
                   status="witnessed", prov={"source": best.get("source"),
                                             "run": ctx.build.version},
                   subject=f"concept:{label}@table:{ctx.physical}",
                   data={"term": label, "target": ctx.physical},
                   coverage_item="edge:bound_to")
        else:
            em.add("concept", f"proposed concept term {label} "
                   + ("(contested: several definitions on this table)"
                      if contested else "(snippet-witnessed only)"),
                   "suggestion.glossary_term", witness=best.get("source", ""),
                   status="contested" if contested else "witnessed",
                   prov={"source": best.get("source"), "run": ctx.build.version},
                   subject=f"concept:{label}@table:{ctx.physical}", showcase=True,
                   reason=("competing definitions: a steward picks one"
                           if contested else "one snippet is thin evidence for a governed term"),
                   data={"label": label, "classes": sorted(classes)},
                   coverage_item="node:concept")
        em.add("concept", f"filter '{label}': {best['canonical_sql']}"
               + (" (contested)" if contested else ""),
               "entry.overview.concepts_filters",
               witness=",".join(sorted(best.get("support_by_witness") or {})),
               status="contested" if contested else "witnessed",
               prov={"source": best.get("source"), "run": ctx.build.version},
               subject=f"concept:{label}@table:{ctx.physical}",
               coverage_item="card:table.common_filters")
        if enriched:
            em.add("concept", f"{UNREVIEWED_FLAG}: {label} — {enriched}",
                   "suggestion.glossary_term", witness="llm_enriched",
                   status="unreviewed", prov=_prov_of(node),
                   subject=f"concept:{label}@table:{ctx.physical}", showcase=True,
                   reason="model-written meaning; a steward has not reviewed it",
                   data={"label": label, "description": enriched,
                         "disambiguation": node.props.get("disambiguation_enriched", "")},
                   coverage_item="node:concept.description_enriched")


def _joins(ctx: TableContext, em: _Emitter) -> None:
    for j in ctx.join_rows():
        other = j["b"] if j["a"] == ctx.physical else j["a"]
        source = j.get("source") or ""
        on = j.get("on")
        on_list = on if isinstance(on, list) else ([on] if on else [])
        if source == "constraints":
            tier = TIER_DECLARED
        elif source in ("jobs_30d", "studio") and on_list:
            tier = TIER_SCOPED if j.get("scope") == "scoped_only" else TIER_WITNESSED
        else:
            tier = TIER_CANDIDATE
        witness = {"constraints": "bq", "co_query": "bq",
                   "catalog": "catalog_mined"}.get(source, source)
        support = int(j.get("support") or 1)
        text = (f"joins {other}"
                + (f" on {' AND '.join(on_list)}" if on_list else "")
                + (f" ({j.get('join_type')})" if j.get("join_type") else "")
                + (" — CTE-scoped, not raw-safe" if tier == TIER_SCOPED else "")
                + f" [{tier}; {source}; support {support}]")
        data = {"tier": tier, "other": other, "on": on_list, "source": source,
                "support": support, "scope": j.get("scope"),
                "join_type": j.get("join_type"), "how": j.get("how"),
                "confidence": j.get("confidence"),
                "seen_in_metrics": j.get("witness_metrics") or [],
                "seen_in_measures": j.get("measures") or [],
                "corroborated_by": j.get("also_witnessed_by") or [],
                "catalog": j.get("also_in_catalog"), "note": j.get("note")}
        prov = {"source": source, "run": ctx.build.version, "support": support}
        if tier == TIER_CANDIDATE:
            em.add("join", text, "aspect.join-paths.paths", witness=witness,
                   status="observed", prov=prov, subject=f"table:{other}",
                   data=data, coverage_item="index:joins.source")
            em.add("join", f"proposed join with {other}: the tables appear "
                   f"together ({source}, support {support}) but no ON clause is "
                   "on record", "suggestion.join", witness=witness,
                   status="observed", prov=prov, subject=f"table:{other}",
                   showcase=True, reason="co-usage alone is not a join",
                   data=data, coverage_item="edge:co_queried_with")
        else:
            em.add("join", text, "aspect.join-paths.paths", witness=witness,
                   status="declared" if tier == TIER_DECLARED else "observed",
                   prov=prov, subject=f"table:{other}", data=data,
                   coverage_item="index:joins.on")
            em.add("join", text, "entry.overview.joins", witness=witness,
                   status="declared" if tier == TIER_DECLARED else "observed",
                   prov=prov, subject=f"table:{other}",
                   coverage_item="card:table.joined_with")
    for q in ctx.out_edges(ctx.tid, "joins_via"):
        if q.props.get("purpose") or q.props.get("preconditions"):
            em.add("join", f"join with {q.o.split(':', 1)[1]}: "
                   + (f"purpose {q.props.get('purpose')}" if q.props.get("purpose") else "")
                   + (f" requires {'; '.join(q.props['preconditions'])}"
                      if q.props.get("preconditions") else ""),
                   "aspect.join-paths.paths", witness=q.prov.witness,
                   status="observed", prov=_prov_of(q), subject=q.o,
                   data={"tier": TIER_SCOPED if q.props.get("scope") == "scoped_only"
                         else TIER_WITNESSED, "other": q.o.split(":", 1)[1],
                         "purpose": q.props.get("purpose"),
                         "preconditions": q.props.get("preconditions") or []},
                   coverage_item="edge:joins_via.purpose")


def _sensitivity(ctx: TableContext, em: _Emitter) -> None:
    acl = ctx.build.acl.get(ctx.physical) or {}
    restricted = acl.get("restricted")
    prov = {"source": "acl", "run": ctx.build.version}
    if restricted == "unknown_policy":
        em.add("policy", f"row-access {RESTRICTED_NOTE} (listing was denied)",
               "aspect.sensitivity-consensus.policy", witness="bq",
               status="unknown_policy", prov=prov, subject=ctx.tid,
               data={"policy": "unknown", "treat_as": "restricted"},
               coverage_item="index:acl.restricted")
        em.add("policy", f"row-access {RESTRICTED_NOTE}",
               "entry.overview.sensitivity", witness="bq",
               status="unknown_policy", prov=prov, subject=ctx.tid,
               coverage_item="card:table.access")
    elif restricted:
        em.add("policy", f"row-access policy {restricted}",
               "aspect.sensitivity-consensus.policy", witness="bq",
               status="declared", prov=prov, subject=ctx.tid,
               data={"policy": restricted}, coverage_item="index:acl.restricted")
        em.add("policy", f"row-access policy {restricted}",
               "entry.overview.sensitivity", witness="bq", status="declared",
               prov=prov, subject=ctx.tid, coverage_item="card:table.access")
    else:
        em.add("policy", "no row-access restriction on record",
               "aspect.sensitivity-consensus.policy", witness="bq",
               status="observed", prov=prov, subject=ctx.tid,
               data={"policy": "none_on_record"}, coverage_item="index:acl.restricted")
    pii = list(acl.get("pii_columns") or [])
    if pii:
        em.add("column", f"sensitive columns (most restrictive of every "
               f"declaration): {', '.join(pii)}",
               "aspect.sensitivity-consensus.columns", witness="bq,lumi,atlas",
               status="consensus", prov=prov, subject=ctx.tid,
               data=[{"column": c, "sensitive": True} for c in pii],
               coverage_item="index:acl.pii_columns")
        em.add("column", f"sensitive columns: {', '.join(pii)}",
               "entry.overview.sensitivity", witness="bq,lumi,atlas",
               status="consensus", prov=prov, subject=ctx.tid,
               coverage_item="card:table.access")
    for q in ctx.out_edges(ctx.tid, "has_policy"):
        em.add("policy", f"policy {q.o.split(':', 1)[1]} declared by {q.prov.witness}",
               "aspect.sensitivity-consensus.declared_policies",
               witness=q.prov.witness, status="declared", prov=_prov_of(q),
               subject=q.o, data={"policy": q.o.split(":", 1)[1],
                                  "by": q.prov.witness},
               coverage_item="edge:has_policy")
    record = ctx.node(ctx.tid)
    p = record.props if record else {}
    for prop, key in (("has_pii_atlas", "pii"), ("has_gdpr_atlas", "gdpr"),
                      ("has_oncop_atlas", "oncop")):
        if p.get(prop) is not None:
            em.add("table", f"catalog declares {key}: {p[prop]}",
                   f"aspect.sensitivity-consensus.declared.{key}",
                   witness=ctx.prop_witness(ctx.tid, prop) or "atlas",
                   status="declared", prov=ctx.prop_prov_of(ctx.tid, prop),
                   subject=ctx.tid, data=p[prop], coverage_item=f"node:table.{prop}")
    for cid in ctx.column_ids():
        cp = ctx.props(cid)
        name = cid.split(".")[-1]
        for q in ctx.out_edges(cid, "has_policy"):
            em.add("column", f"{name} carries policy {q.o.split(':', 1)[1]} "
                   f"({q.prov.witness})", "aspect.sensitivity-consensus.columns",
                   witness=q.prov.witness, status="declared", prov=_prov_of(q),
                   subject=cid, data={"column": name,
                                      "policy": q.o.split(":", 1)[1],
                                      "declared_by": [q.prov.witness]},
                   coverage_item="edge:has_policy")
        if cp.get("is_pii_mdm"):
            em.add("column", f"{name} is PII per the MDM"
                   + (f" (role {cp.get('pii_role_id')})" if cp.get("pii_role_id") else "")
                   + (f", SDE group {cp.get('sde_group')}" if cp.get("sde_group") else ""),
                   "aspect.sensitivity-consensus.columns", witness="lumi",
                   status="declared", prov=_prov_of(ctx.node(cid)), subject=cid,
                   data={"column": name, "declared_by": ["lumi"],
                         "pii_role": cp.get("pii_role_id"),
                         "sde_group": cp.get("sde_group")},
                   coverage_item="node:col.is_pii_mdm")
    census = ctx.aux_json("census.json") or {}
    d5 = ((census.get("structural") or {}).get("per_table") or {}).get(
        ctx.physical, {}).get("D5")
    if d5:
        em.add("table", f"{d5} sensitivity disagreement(s) resolved to the most "
               "restrictive reading", "aspect.sensitivity-consensus.disagreements",
               witness="bq,lumi,atlas", status="consensus", prov=prov,
               subject=ctx.tid, data=d5, coverage_item="report:census.structural.per_table")


def _status(ctx: TableContext, em: _Emitter) -> None:
    census = ctx.aux_json("census.json") or {}
    structural = census.get("structural") or {}
    cell = (structural.get("per_table") or {}).get(ctx.physical) or {}
    handlers = structural.get("handlers") or {}
    prov = {"source": "structural census", "run": ctx.build.version}
    if cell:
        em.add("census", "structural census: " + ", ".join(
            f"{k} {v}" for k, v in sorted(cell.items())),
            "aspect.definition-status.d_counts", witness="bq,lumi,atlas",
            status="observed", prov=prov, subject=ctx.tid,
            data={"counts": cell, "handlers": handlers},
            coverage_item="report:census.structural.per_table")
        for code, n in sorted(cell.items()):
            if n:
                em.add("census", f"{n} {code} finding(s): {handlers.get(code, code)}",
                       "entry.overview.unknowns", witness="bq,lumi,atlas",
                       status="observed", prov=prov, subject=ctx.tid,
                       coverage_item="card:table.conflicts")
    summary = census.get("summary") or {}
    if summary:
        em.add("census", "build-wide census: " + ", ".join(
            f"{k} {v}" for k, v in sorted(summary.items())),
            "aspect.definition-status.census", witness="bq,lumi,atlas",
            status="observed", prov=prov, subject=ctx.tid, data=summary,
            coverage_item="report:census.summary.concept_cells")
    metric_ids = {m["id"] for m in ctx.metric_rows()}
    subjects = {ctx.tid, *ctx.column_ids(), *metric_ids}
    for node in ctx.nodes_of_kind("review"):
        nid = node.id
        if node.props.get("subject") in subjects \
                and node.props.get("status") == "open":
            em.add("review", f"open review ({node.props.get('kind')}): "
                   f"{node.props.get('proposal')}",
                   "aspect.definition-status.open_reviews",
                   witness=node.prov.witness, status="observed", prov=_prov_of(node),
                   subject=nid, data={k: v for k, v in node.props.items()
                                      if k != "agent_recommendation"},
                   coverage_item="node:review")
            em.add("review", f"a steward item is open: {node.props.get('proposal')}",
                   "entry.overview.unknowns", witness=node.prov.witness,
                   status="observed", prov=_prov_of(node), subject=nid,
                   coverage_item="node:review")
    labels = defaultdict(set)
    for b in ctx.binding_rows():
        labels[b["label"]].add(b["fp"])
    contested = sorted(l for l, fps in labels.items() if len(fps) > 1)
    if contested:
        em.add("concept", f"contested concept definitions: {', '.join(contested)}",
               "aspect.definition-status.concept_conflicts",
               witness="snippet,jobs_30d", status="contested", prov=prov,
               subject=ctx.tid, data=contested, coverage_item="card:concept.conflicts")
    groups = defaultdict(set)
    for m in ctx.metric_rows():
        for g in m.get("mgroups") or []:
            groups[g].add(m["id"])
    conflicts = sorted(g for g, ids in groups.items() if len(ids) > 1)
    if conflicts:
        em.add("metric", f"metric families with competing definitions: "
               f"{', '.join(conflicts)}", "aspect.definition-status.metric_conflicts",
               witness="dmp,gmns,catalog_mined", status="contested", prov=prov,
               subject=ctx.tid, data=conflicts, coverage_item="card:metric.conflicts")
    ungoverned = [r["name"] for r in ctx.column_index() if r.get("ungoverned")]
    if ungoverned:
        em.add("column", f"columns with no business meaning on record: "
               f"{', '.join(ungoverned)}", "entry.overview.unknowns",
               witness="bq", status="observed", prov=prov, subject=ctx.tid,
               coverage_item="card:table.conflicts")


def _queries(ctx: TableContext, em: _Emitter) -> None:
    for q in ctx.out_edges(ctx.tid, "described_by"):
        dp = ctx.props(q.o)
        if dp.get("kind") == "view_sql" and dp.get("sql"):
            em.add("query", f"view definition: {dp['sql']}", "query",
                   witness=q.prov.witness, status="declared", prov=_prov_of(q),
                   subject=q.o, data={"sql": dp["sql"], "kind": "view_sql",
                                      "description": f"view definition of {ctx.physical}",
                                      "source": "User"},
                   coverage_item="edge:described_by")
            em.add("query", f"defined as a view: {dp['sql']}",
                   "aspect.lineage-notes.view_sql", witness=q.prov.witness,
                   status="declared", prov=_prov_of(q), subject=q.o,
                   data=dp["sql"], coverage_item="node:doc.sql")
        elif dp.get("text") or dp.get("sql"):
            em.add("doc", f"{dp.get('kind') or 'note'}: {dp.get('text') or dp.get('sql')}",
                   "aspect.lineage-notes.notes", witness=q.prov.witness,
                   status="declared", prov=_prov_of(q), subject=q.o,
                   data={"kind": dp.get("kind"), "text": dp.get("text") or dp.get("sql")},
                   coverage_item="node:doc.text")
    for m in ctx.metric_rows():
        for q in ctx.out_edges(m["id"], "evidenced_by"):
            dp = ctx.props(q.o)
            if not dp.get("sql"):
                continue
            gold = q.prov.witness == "gold_attested"
            attested = gold or q.prov.witness == "studio"
            em.add("query", f"{m.get('label') or m['id']} as observed "
                   f"({q.prov.witness}): {dp['sql']}", "query",
                   witness=q.prov.witness,
                   status="observed" if attested else _status_of(m),
                   prov=_prov_of(q), subject=q.o,
                   data={"sql": dp["sql"], "kind": dp.get("kind"),
                         "description": f"{m.get('label') or m['id']} "
                                        f"(observed {q.props.get('first_seen', '')}"
                                        f"–{q.props.get('last_seen', '')})",
                         "source": "User", "gold": gold, "attested": attested},
                   coverage_item="edge:evidenced_by")


def _domains(ctx: TableContext, em: _Emitter) -> None:
    prefix = f"{ctx.physical}."
    for d in ctx.build.domains:
        if not str(d.get("key", "")).startswith(prefix):
            continue
        column = d["key"][len(prefix):]
        values = d.get("values") or []
        meanings = {m["value"]: m["synonym"] for m in d.get("meanings") or []
                    if "value" in m}
        shown = [{"value": v["value"], "share_pct": v.get("pct"),
                  "meaning": meanings.get(v["value"], "")} for v in values]
        for value, meaning in meanings.items():
            if value not in {v["value"] for v in values}:
                shown.append({"value": value, "share_pct": None,
                              "meaning": meaning})
        node = ctx.node(f"domain:{d['key']}")
        witness = "bq" + (",lumi" if meanings else "")
        em.add("domain", f"{column} takes values "
               + ", ".join(f"{s['value']}" + (f" ({s['meaning']})" if s["meaning"] else "")
                           for s in shown[:12])
               + (f"; about {d.get('distinct_estimate')} distinct"
                  if d.get("distinct_estimate") else ""),
               "aspect.value-domain.values", witness=witness,
               status="observed", prov=_prov_of(node) if node else
               {"source": "domains index", "run": ctx.build.version},
               subject=f"col:{d['key']}",
               data={"column": column, "values": shown,
                     "distinct_estimate": d.get("distinct_estimate"),
                     "profiled": bool(values)},
               coverage_item="index:domains.values")
        if values:
            em.add("domain", f"set membership: {column} in "
                   f"({', '.join(v['value'] for v in values[:20])})",
                   "dq.set_membership", witness="bq", status="observed",
                   prov=_prov_of(node) if node else {"source": "domains index"},
                   subject=f"col:{d['key']}",
                   data={"rule": "set_membership", "column": column,
                         "values": [v["value"] for v in values]},
                   coverage_item="node:domain")
    for cid in ctx.column_ids():
        cp = ctx.props(cid)
        if cp.get("approx_distinct") is not None:
            em.add("column", f"{cid.split('.')[-1]}: about {cp['approx_distinct']} "
                   "distinct values", "aspect.value-domain.distinct_estimate",
                   witness="bq", status="observed", prov=_prov_of(ctx.node(cid)),
                   subject=cid, data={"column": cid.split(".")[-1],
                                      "distinct_estimate": cp["approx_distinct"],
                                      "profile_coverage": cp.get("profile_coverage")},
                   coverage_item="node:col.approx_distinct")


def _provenance(ctx: TableContext, em: _Emitter) -> None:
    prov = {"source": "build manifest", "run": ctx.build.version}
    em.add("build", f"build {ctx.build.version}", "aspect.meridian-provenance.build_id",
           witness="steward", status="observed", prov=prov, subject=ctx.tid,
           data=ctx.build.version, coverage_item="report:manifest.build_id")
    graph_hash = ctx.build.manifest.get("graph_hash")
    if graph_hash:
        em.add("build", f"graph hash {graph_hash}",
               "aspect.meridian-provenance.graph_hash", witness="steward",
               status="observed", prov=prov, subject=ctx.tid, data=graph_hash,
               coverage_item="report:manifest.graph_hash")
    run = ctx.graph_run()
    if run:
        em.add("build", f"graph run {run}", "aspect.meridian-provenance.graph_run",
               witness="steward", status="observed", prov=prov, subject=ctx.tid,
               data=run, coverage_item="report:run.run_id")
    families = defaultdict(int)
    for q in ctx.edges_from(ctx.tid):
        if q.prov.witness:
            families[q.prov.witness] += 1
    for cid in ctx.column_ids():
        record = ctx.node(cid)
        if record and record.prov.witness:
            families[record.prov.witness] += 1
    sources = ctx.aux_json("indexes/sources.json") or {}
    display = {s.get("source"): s.get("display") for s in sources.get("sources") or []}
    em.add("build", "witnesses on this table: " + ", ".join(
        f"{w} ×{n}" for w, n in sorted(families.items())),
        "aspect.meridian-provenance.witnesses", witness=",".join(sorted(families)),
        status="observed", prov=prov, subject=ctx.tid,
        data=[{"family": w, "records": n, "display": display.get(w, "")}
              for w, n in sorted(families.items())],
        coverage_item="prov:witness")
    em.add("build", f"generated from build {ctx.build.version}, graph run "
           f"{run or '?'}; every fact carries its witness and status",
           "entry.overview.provenance", witness="steward", status="observed",
           prov=prov, subject=ctx.tid, coverage_item="report:manifest.build_id")


def _enrichment(ctx: TableContext, em: _Emitter) -> None:
    for m in ctx.metric_rows():
        node = ctx.node(m["id"])
        if node is None:
            continue
        p = node.props
        for prop, what in (("question_enriched", "question"),
                           ("grain_enriched", "grain")):
            if p.get(prop):
                em.add("metric", f"{UNREVIEWED_FLAG}: {m.get('label') or m['id']} "
                       f"{what} — {p[prop]}"
                       + (f" (confidence {p.get('enrich_confidence')})"
                          if p.get("enrich_confidence") is not None else ""),
                       "suggestion.metric_definition", witness="llm_enriched",
                       status="unreviewed", prov=_prov_of(node), subject=m["id"],
                       showcase=True,
                       reason="model-written; a steward has not reviewed it",
                       data={"metric": m.get("label"), "field": what,
                             "text": p[prop],
                             "confidence": p.get("enrich_confidence"),
                             "caveat": p.get("enrich_caveat"),
                             "prompt_version": p.get("enrich_prompt_version")},
                       coverage_item=f"node:metric.{prop}")
    reports = sorted(ctx.graph_root.glob("runs/*/enrich_report.json"))
    if reports:
        latest = json.loads(reports[-1].read_text(encoding="utf-8"))
        blind = latest.get("blind") or {}
        em.add("build", f"latest enrichment run {reports[-1].parent.name}: prompt "
               f"{latest.get('prompt_version')}, blind gate "
               f"{blind.get('tier', 'not run')}", "suggestion.enrichment_run",
               witness="llm_enriched", status="observed",
               prov={"source": "enrich_report", "run": reports[-1].parent.name},
               subject=ctx.tid, showcase=True,
               reason="context for every model suggestion above",
               data={"run": reports[-1].parent.name,
                     "prompt_version": latest.get("prompt_version"),
                     "blind": blind}, coverage_item="report:enrich.blind")


EXTRACTOR_FUNCS: dict[str, Callable[[TableContext, _Emitter], None]] = {
    "identity": _identity, "description": _description,
    "stewardship": _stewardship, "usage": _usage, "columns": _columns,
    "terms": _terms, "lineage": _lineage, "keys": _keys, "metrics": _metrics,
    "concepts": _concepts, "joins": _joins, "sensitivity": _sensitivity,
    "status": _status, "queries": _queries, "domains": _domains,
    "provenance": _provenance, "enrichment": _enrichment}


# ── the assembly ─────────────────────────────────────────────────

def context(build: Build, graph_root: Path, physical: str) -> TableContext:
    view = fold(graph_root)
    return TableContext(build=build, graph_root=Path(graph_root),
                        physical=physical, nodes=view.nodes, edges=view.edges,
                        prop_prov=view.prop_prov, view=view)


def assemble(build: Build, graph_root: Path, physical: str) -> FactSet:
    """Every fact about ``physical``, in a stable order with stable ids.
    Deterministic: the same build and graph produce byte-identical
    output, so a bundle's digest is a digest of the knowledge."""
    if physical not in build.schema:
        raise KeyError(f"{physical} is not in build {build.version}")
    ctx = context(build, graph_root, physical)
    facts: list[Fact] = []
    for name in coverage.EXTRACTORS:
        em = _Emitter(ctx, name)
        EXTRACTOR_FUNCS[name](ctx, em)
        facts.extend(em.facts)
    # de-duplicate exact repeats (a fact reached through two paths)
    seen: set[tuple] = set()
    unique: list[Fact] = []
    for f in facts:
        key = (f.kc_target, f.subject, f.text)
        if key in seen:
            continue
        seen.add(key)
        unique.append(f)
    order = {name: i for i, name in enumerate(coverage.EXTRACTORS)}
    unique.sort(key=lambda f: (order[f.extractor], f.kc_target, f.subject,
                               f.text))
    for i, f in enumerate(unique, 1):
        f.id = f"f{i}"
        verdict, reason = include_of(f)
        f.include = verdict
        if reason and not f.reason:
            f.reason = reason
        elif reason and verdict == NEVER:
            f.reason = reason
    counts = defaultdict(int)
    for f in unique:
        counts[f.include] += 1
        counts[f"kind:{f.kind}"] += 1
    return FactSet(table=physical, build_id=build.version,
                   graph_run=ctx.graph_run(), facts=unique,
                   counts=dict(sorted(counts.items())))
