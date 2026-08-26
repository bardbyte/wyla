"""Semantic sources → quads (the P0 adapters' P2 upgrade).

Predicates become ``pred:`` nodes bound to ``concept:`` subjects;
metric expressions become ``metric:`` nodes (id = fp(expr ⊕ grain ⊕
entity), pinned) grouped under ``mgroup:`` identities with initial
governance states seeded per source (dmp → certified, gmns → pending,
skills → team_candidate, mined → mined); vocabulary becomes ``term:`` /
``acr:`` nodes; Atlas std_tech entries land their DECLARED column↔term
links as ``mapped_term`` edges plus atlas-attributed props — the E1
merge policy arbitrates against BQ/Lumi at compile time, not here.

Table identity resolves through the crosswalk; a semantic record whose
table can't resolve is SKIPPED with a count (unlike archives, semantic
sources legitimately mention out-of-scope tables — skipping is honest,
blocking would be wrong).
"""

from __future__ import annotations

from collections import defaultdict

from sahs.canon.authority import Authority
from sahs.canon.canonical import CANON_VERSION, CanonResult
from sahs.canon.census import norm_label
from sahs.canon.fingerprint import fingerprint
from sahs.graph.crosswalk import Crosswalk
from sahs.graph.ids import acr_id, col_id, concept_id, table_id
from sahs.graph.quads import SOURCE_WITNESS, GraphDir, NodeRecord, Prov, Quad
from sahs.loaders.records import (
    ExpressionRecord,
    StdTechEntry,
    TermRecord,
    VocabRecord,
)

_INITIAL_STATE = {
    Authority.CERTIFIED: "certified",
    Authority.PENDING: "pending",
    Authority.SKILL_CONTRACT: "team_candidate",
    Authority.MINED: "mined",
    Authority.SNIPPET: None,             # predicates carry no governance
}


_STATE_RANK = {"mined": 1, "team_candidate": 2, "pending": 3,
               "certified": 4}


def emit_expressions(pairs: list[tuple[ExpressionRecord, CanonResult]],
                     graph: GraphDir, crosswalk: Crosswalk,
                     run_id: str) -> dict:
    """Aggregate-then-write: sources sharing a fingerprint fuse into one
    node, one edge per (s,r,o,WITNESS) with support summed WITHIN the
    family only (E12/A1 — cross-family aggregation is compiler output,
    never store state), and ONE governance seed per metric — the
    highest-authority initial state — so the E7 transition history never
    starts with an illegal sequence."""
    report: dict = defaultdict(int)
    nodes: dict[str, dict] = {}
    node_prov: dict[str, tuple[str, int, str]] = {}
    edges: dict[tuple[str, str, str, str], dict] = {}
    states: dict[str, str] = {}

    def witness_of(record: ExpressionRecord) -> str:
        return record.witness or SOURCE_WITNESS.get(record.source, "")

    def merge_node(node_id: str, props: dict, record: ExpressionRecord
                   ) -> None:
        held = nodes.setdefault(node_id, {})
        for key, value in props.items():
            if value not in (None, "", []):
                current = held.get(key)
                if current in (None, "", []):
                    held[key] = value
        prev = node_prov.get(node_id)
        if prev is None or int(record.authority) > prev[1]:
            node_prov[node_id] = (record.source, int(record.authority),
                                  record.evidence_ref)

    def merge_edge(s: str, r: str, o: str, record: ExpressionRecord
                   ) -> None:
        entry = edges.setdefault((s, r, o, witness_of(record)), {
            "source": record.source, "authority": int(record.authority),
            "support": 0, "evidence": record.evidence_ref,
            "first_seen": record.first_seen, "last_seen": record.last_seen,
            "run_count": 0})
        entry["support"] += max(record.support, 1)
        entry["run_count"] += int(record.extra.get("run_count") or 0)
        if record.first_seen and (not entry["first_seen"]
                                  or record.first_seen
                                  < entry["first_seen"]):
            entry["first_seen"] = record.first_seen
        if record.last_seen and record.last_seen > entry["last_seen"]:
            entry["last_seen"] = record.last_seen
        if int(record.authority) > entry["authority"]:
            entry.update(source=record.source,
                         authority=int(record.authority),
                         evidence=record.evidence_ref)

    for record, canon in pairs:
        raw_table = record.table_hint or (
            canon.tables[0] if canon.tables else "")
        physical = (crosswalk.physical_for_atlas(raw_table)
                    or crosswalk.physical_for_lumi(raw_table)
                    if raw_table else None)
        if physical is None:
            report["skipped_unresolvable_table"] += 1
            continue
        tid = table_id(physical)
        # the catalog attesting a metric/predicate attests the table
        # exists — mint the endpoint (fold merges with archive detail)
        merge_node(tid, {}, record)

        if record.kind in ("predicate", "case") and record.concept_label:
            pred = f"pred:{canon.fp_expr}"
            merge_node(pred, {"canonical_sql": canon.canonical_sql,
                              "canon_version": CANON_VERSION,
                              "kind": record.kind}, record)
            concept = concept_id(norm_label(record.concept_label), physical)
            merge_node(concept, {"label": record.concept_label}, record)
            merge_edge(concept, "bound_to", pred, record)
            report["predicates"] += 1

        elif record.kind == "metric_expr":
            grain = str(record.extra.get("metric_grain")
                        or record.extra.get("grain") or "")
            metric_fp = fingerprint(
                canon.canonical_sql + "\x00" + grain + "\x00" + physical,
                "bigquery", CANON_VERSION)
            metric = f"metric:{metric_fp}"
            merge_node(metric, {
                "canonical_sql": canon.canonical_sql,
                "canon_version": CANON_VERSION,
                # a usage alias is a WEAK name: it groups (mgroup key)
                # but never claims the metric node's label — a fold is
                # last-wins on props, and a jobs alias must not clobber
                # a catalog name
                "label": ("" if record.extra.get("label_is_weak")
                          else record.concept_label or ""),
                "label_usage": (record.concept_label or ""
                                if record.extra.get("label_is_weak")
                                else ""),
                "grain": grain,
                "question_answered":
                    record.extra.get("question_answered") or "",
                "approved_dimensions":
                    record.extra.get("approved_dimensions") or [],
                "sign_convention": record.extra.get("calculation") or "",
            }, record)
            group_key = (record.metric_ref
                         or f"{norm_label(record.concept_label or '?')}"
                            f"@{physical}").lower()
            mgroup = f"mgroup:{group_key}"
            merge_node(mgroup, {"label": record.concept_label or ""},
                       record)
            merge_edge(metric, "member_of", mgroup, record)
            merge_edge(metric, "measured_on", tid, record)
            state = _INITIAL_STATE.get(record.authority)
            if state and _STATE_RANK[state] > _STATE_RANK.get(
                    states.get(metric, ""), 0):
                states[metric] = state
            report["metrics"] += 1

    for node_id in sorted(nodes):
        source, _authority, evidence = node_prov[node_id]
        graph.append_node(NodeRecord(
            id=node_id, props=nodes[node_id],
            prov=Prov(source=source, run=run_id, evidence=evidence)))
    for (s, r, o, witness) in sorted(edges):
        entry = edges[(s, r, o, witness)]
        props = {k: v for k, v in (
            ("first_seen", entry["first_seen"]),
            ("last_seen", entry["last_seen"]),
            ("run_count", entry["run_count"] or None)) if v}
        graph.append_edge(Quad(s=s, r=r, o=o, props=props, prov=Prov(
            source=entry["source"], run=run_id, witness=witness,
            support=entry["support"], evidence=entry["evidence"])))
    # seed-aware across CALLS: a metric already governed in the graph
    # (an earlier emit or a clerk edge) is never re-seeded — a second
    # witness of a metric is testimony, not a governance transition,
    # and a late lower seed would forge an illegal E7 sequence
    already_governed = {q.s for q in graph.iter_edges("certified_as")}
    for metric in sorted(states):
        if metric in already_governed:
            continue
        source, _authority, evidence = node_prov[metric]
        graph.append_edge(Quad(
            s=metric, r="certified_as", o=f"status:{states[metric]}",
            prov=Prov(source=source, run=run_id, evidence=evidence)))

    # variant_of: same (label, table), different fingerprints — children
    # hang off the highest-authority member (the meridian line when one
    # exists); "off-meridian by: <expression delta>" renders from these
    by_intent: dict[tuple[str, str], list[str]] = defaultdict(list)
    for (s, r, o, _witness) in edges:
        if r == "measured_on" and s.startswith("metric:"):
            label = norm_label(str(nodes.get(s, {}).get("label") or ""))
            if label:
                by_intent[(label, o)].append(s)
    for (label, tid_o), members in sorted(by_intent.items()):
        unique = sorted(set(members))
        if len(unique) < 2:
            continue
        parent = max(unique, key=lambda m: (node_prov[m][1], m))
        for child in unique:
            if child == parent:
                continue
            source, _authority, evidence = node_prov[child]
            graph.append_edge(Quad(
                s=child, r="variant_of", o=parent,
                prov=Prov(source=source, run=run_id, evidence=evidence)))
            report["variant_edges"] = report.get("variant_edges", 0) + 1
    return dict(report)


def emit_vocab(glossary: list[VocabRecord], terms: list[TermRecord],
               graph: GraphDir, run_id: str) -> dict:
    report: dict = defaultdict(int)
    for v in glossary:
        graph.append_node(NodeRecord(
            id=acr_id(v.symbol, v.business_unit, v.region),
            props={"symbol": v.symbol, "definition": v.definition,
                   "entry_type": v.entry_type,
                   "business_unit": v.business_unit, "region": v.region},
            prov=Prov(source="glossary", run=run_id,
                      evidence=v.evidence_ref)))
        report["acronyms"] += 1
    for t in terms:
        graph.append_node(NodeRecord(
            id=f"term:atlas:{t.term_id}",
            props={"name": t.name, "status": t.status},
            prov=Prov(source="atlas", run=run_id, evidence=t.evidence_ref)))
        report["terms"] += 1
    return dict(report)


def emit_std_tech(entries: list[StdTechEntry], terms: list[TermRecord],
                  graph: GraphDir, crosswalk: Crosswalk,
                  run_id: str) -> dict:
    report: dict = defaultdict(int)
    term_by_name = {t.name.strip().lower(): t.term_id for t in terms}
    for entry in entries:
        physical = crosswalk.physical_for_atlas(entry.table)
        if physical is None:
            report["skipped_unresolvable_table"] += 1
            continue
        tid = table_id(physical)
        graph.append_node(NodeRecord(
            id=tid,
            props={"description_atlas": entry.description,
                   "business_name_atlas": entry.business_name,
                   "data_category": entry.data_category,
                   "layer_type": entry.layer_type,
                   "has_pii_atlas": entry.has_pii,
                   "ownership_atlas": entry.ownership},
            prov=Prov(source="atlas", run=run_id,
                      evidence=entry.evidence_ref)))
        report["tables"] += 1
        for column in entry.columns:
            cid = col_id(physical, column.name)
            graph.append_node(NodeRecord(
                id=cid,
                props={"description_atlas": column.description,
                       "business_name_atlas": column.business_name,
                       "data_type_atlas": column.data_type,
                       "pii_role_id": column.pii_role_id,
                       "sde_group": column.sde_group},
                prov=Prov(source="atlas", run=run_id,
                          evidence=entry.evidence_ref)))
            graph.append_edge(Quad(
                s=tid, r="has_column", o=cid,
                prov=Prov(source="atlas", run=run_id,
                          evidence=entry.evidence_ref)))
            report["columns"] += 1
            if column.pii_role_id:
                graph.append_edge(Quad(
                    s=cid, r="has_policy", o="policy:pii",
                    prov=Prov(source="atlas", run=run_id,
                              evidence=entry.evidence_ref)))
            for link in column.linked_terms:
                name = str(link.get("businessTermName") or "").strip()
                term_id = term_by_name.get(name.lower())
                if term_id is None:
                    report["term_links_unmatched"] += 1
                    continue
                graph.append_edge(Quad(
                    s=cid, r="mapped_term", o=f"term:atlas:{term_id}",
                    props={"mapping_source": link.get("sourceName", ""),
                           "mapping_type": link.get("sourceType", "")},
                    prov=Prov(source="atlas", run=run_id,
                              evidence=entry.evidence_ref)))
                report["term_links"] += 1
    return dict(report)
