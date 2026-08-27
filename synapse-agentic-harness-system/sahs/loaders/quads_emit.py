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
from sahs.graph.ids import (
    acr_id,
    col_id,
    concept_id,
    lob_id,
    mdom_id,
    mgroup_id,
    table_id,
    term_node_id,
)
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
                     run_id: str,
                     known_lobs: set[str] | None = None) -> dict:
    """Aggregate-then-write: sources sharing a fingerprint fuse into one
    node, one edge per (s,r,o,WITNESS) with support summed WITHIN the
    family only (E12/A1 — cross-family aggregation is compiler output,
    never store state), and ONE governance seed per metric — the
    highest-authority initial state — so the E7 transition history never
    starts with an illegal sequence.

    ``known_lobs``: lob node ids the steward map already minted. LOB
    minting is an authority privilege — a certified/pending catalog's
    declared lineOfBusiness mints the lob node; a MINED business_unit
    only ever corroborates an existing one (steward or catalog-declared)
    and anything else is counted ``lob_unmatched``, never guessed into
    the graph."""
    report: dict = defaultdict(int)
    nodes: dict[str, dict] = {}
    node_prov: dict[str, tuple[str, int, str]] = {}
    edges: dict[tuple[str, str, str, str], dict] = {}
    states: dict[str, str] = {}

    # authority pre-scan so a mined record processed BEFORE the catalog
    # that declares its LOB still corroborates deterministically
    declared_lobs: set[str] = set()
    for record, _canon in pairs:
        if record.kind == "metric_expr" \
                and int(record.authority) >= int(Authority.PENDING):
            raw = str(record.extra.get("line_of_business") or "").strip()
            if raw:
                declared_lobs.add(lob_id(raw))
    corroboratable = (known_lobs or set()) | declared_lobs

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

    def _resolve(raw: str) -> tuple[str | None, bool]:
        direct = (crosswalk.physical_for_atlas(raw)
                  or crosswalk.physical_for_lumi(raw))
        if direct:
            return direct, False
        hit = crosswalk.physical_for_alias(raw)
        return (hit, True) if hit else (None, False)

    for record, canon in pairs:
        # attribution chain: the declared hint (catalogs attribute by
        # data-product/display names — the alias sidecar maps those),
        # then the tables the metric's OWN SQL references. Nothing
        # resolvable → counted skip, never a guess.
        physical, via_alias, via_sql = None, False, False
        if record.table_hint:
            physical, via_alias = _resolve(record.table_hint)
        if physical is None:
            for t in canon.tables:
                physical, via_alias = _resolve(t)
                if physical:
                    via_sql = bool(record.table_hint)
                    break
        if physical is None:
            report["skipped_unresolvable_table"] += 1
            continue
        if via_alias:
            report["resolved_via_alias"] += 1
        if via_sql:
            report["resolved_via_sql_table"] += 1
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
                # full-utilization props pass — everything the source
                # catalogs say about a metric rides on the node (first
                # non-empty wins per key; a fused metric keeps both its
                # certified pedigree and its mined usage texture)
                "author": record.extra.get("author") or "",
                "domain": record.extra.get("domain") or "",
                "line_of_business":
                    record.extra.get("line_of_business") or "",
                "scope": record.extra.get("metric_scope") or "",
                "requestor": record.extra.get("requestor") or "",
                "products": record.extra.get("products") or [],
                "confidence": record.extra.get("confidence"),
                "execution_count": record.extra.get("execution_count"),
                "group_by_patterns":
                    record.extra.get("group_by_patterns") or [],
                "common_filters":
                    record.extra.get("common_filters") or [],
                "joined_tables":
                    record.extra.get("joined_tables") or [],
                "business_unit": record.extra.get("business_unit") or "",
                "data_category": record.extra.get("data_category") or "",
            }, record)
            # ── LOB layer (witnessed classification, never guessed) ──
            lob_raw = str(record.extra.get("line_of_business")
                          or "").strip()
            dom_raw = str(record.extra.get("domain") or "").strip()
            bu_raw = str(record.extra.get("business_unit") or "").strip()
            if int(record.authority) >= int(Authority.PENDING):
                if lob_raw:
                    lid = lob_id(lob_raw)
                    merge_node(lid, {"code": lob_raw}, record)
                    merge_edge(tid, "in_lob", lid, record)
                    report["lob_edges"] += 1
                if dom_raw:
                    mid = mdom_id(dom_raw)
                    merge_node(mid, {"name": dom_raw}, record)
                    merge_edge(metric, "in_domain", mid, record)
                    if lob_raw:
                        merge_edge(mid, "in_lob", lob_id(lob_raw), record)
                    report["domain_edges"] += 1
            elif bu_raw:
                lid = lob_id(bu_raw)
                if lid in corroboratable:
                    merge_edge(tid, "in_lob", lid, record)
                    report["lob_corroborated_mined"] += 1
                else:
                    report["lob_unmatched"] += 1
            group_key = (record.metric_ref
                         or f"{norm_label(record.concept_label or '?')}"
                            f"@{physical}").lower()
            mgroup = mgroup_id(group_key)
            merge_node(mgroup, {"label": record.concept_label or "",
                                "group_key": group_key},
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
            id=term_node_id(t.term_id),
            props={"name": t.name, "status": t.status,
                   "term_id": t.term_id},
            prov=Prov(source="atlas", run=run_id, evidence=t.evidence_ref)))
        report["terms"] += 1
    return dict(report)


def emit_std_tech(entries: list[StdTechEntry], terms: list[TermRecord],
                  graph: GraphDir, crosswalk: Crosswalk,
                  run_id: str) -> dict:
    """The real feed registers the SAME table more than once — one
    envelope's tech_metadata_list can hold several registrations (46
    files yielded 70 entries on the first real run). A registration is
    testimony about the same facts, not new facts: nodes and edges emit
    ONCE (document order, first registration wins), repeats are counted,
    and the dedup keeps validator check [8] meaningful."""
    report: dict = defaultdict(int)
    term_by_name = {t.name.strip().lower(): t.term_id for t in terms}
    seen_nodes: set[str] = set()
    seen_edges: set[tuple[str, str, str]] = set()

    def put_node(node_id: str, props: dict, evidence: str) -> bool:
        if node_id in seen_nodes:
            return False
        seen_nodes.add(node_id)
        graph.append_node(NodeRecord(
            id=node_id, props=props,
            prov=Prov(source="atlas", run=run_id, evidence=evidence)))
        return True

    def put_edge(s: str, r: str, o: str, evidence: str,
                 props: dict | None = None) -> bool:
        if (s, r, o) in seen_edges:
            return False
        seen_edges.add((s, r, o))
        graph.append_edge(Quad(
            s=s, r=r, o=o, props=props or {},
            prov=Prov(source="atlas", run=run_id, evidence=evidence)))
        return True

    for entry in entries:
        report["entries"] += 1
        physical = crosswalk.physical_for_atlas(entry.table)
        if physical is None:
            report["skipped_unresolvable_table"] += 1
            continue
        tid = table_id(physical)
        if put_node(tid, {"description_atlas": entry.description,
                          "business_name_atlas": entry.business_name,
                          "data_category": entry.data_category,
                          "layer_type": entry.layer_type,
                          "has_pii_atlas": entry.has_pii,
                          "has_oncop_atlas": entry.has_oncop,
                          "has_gdpr_atlas": entry.has_gdpr,
                          "ownership_atlas": entry.ownership},
                    entry.evidence_ref):
            report["tables"] += 1
        else:
            report["repeat_registrations"] += 1
        # sensitivity is union-most-restrictive (E1): every compliance
        # flag the feed asserts becomes an explicit policy edge
        for flag, policy in ((entry.has_oncop, "policy:oncop"),
                             (entry.has_gdpr, "policy:gdpr")):
            if flag and put_edge(tid, "has_policy", policy,
                                 entry.evidence_ref):
                report["policy_flags"] += 1
        for column in entry.columns:
            cid = col_id(physical, column.name)
            if put_node(cid, {"description_atlas": column.description,
                              "business_name_atlas": column.business_name,
                              "data_type_atlas": column.data_type,
                              "pii_role_id": column.pii_role_id,
                              "sde_group": column.sde_group},
                        entry.evidence_ref):
                report["columns"] += 1
            else:
                report["columns_repeated"] += 1
            put_edge(tid, "has_column", cid, entry.evidence_ref)
            if column.pii_role_id:
                put_edge(cid, "has_policy", "policy:pii",
                         entry.evidence_ref)
            for link in column.linked_terms:
                name = str(link.get("businessTermName") or "").strip()
                term_id = term_by_name.get(name.lower())
                if term_id is None:
                    report["term_links_unmatched"] += 1
                    continue
                if put_edge(cid, "mapped_term", term_node_id(term_id),
                            entry.evidence_ref,
                            props={"mapping_source":
                                   link.get("sourceName", ""),
                                   "mapping_type":
                                   link.get("sourceType", "")}):
                    report["term_links"] += 1
    return dict(report)
