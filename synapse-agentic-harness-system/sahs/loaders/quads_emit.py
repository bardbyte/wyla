"""Semantic sources → quads (the P0 adapters' P2 upgrade).

Predicates become ``pred:`` nodes bound to ``concept:`` subjects;
metric expressions become ``metric:`` nodes (id = fp(expr ⊕ grain ⊕
entity), pinned) grouped under ``mgroup:`` identities with initial
governance states seeded per source (dmp → certified, gmns → pending,
skills → team_candidate, mined → mined); vocabulary becomes ``term:`` /
``acr:`` nodes; Atlas std_tech entries land EVERY documented field —
column↔term links as ``mapped_term`` edges (resolved on
``businessTermId`` first), ownership as ``owned_by`` edges, the three
compliance flags and the table-level ``pii_columns`` declaration as
``has_policy`` edges, computed-column ``derived_logic`` as doc
evidence, and the rest as atlas-attributed props — the E1 merge policy
arbitrates against BQ/Lumi at compile time, not here.

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
    owner_id,
    table_id,
    term_node_id,
)
from sahs.graph.quads import SOURCE_WITNESS, GraphDir, NodeRecord, Prov, Quad
from sahs.loaders.sources.vocab import ownership_key_is_person
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
                     lob_aliases: dict[str, str] | None = None,
                     usage_targets: dict[str, str] | None = None) -> dict:
    """Aggregate-then-write: sources sharing a fingerprint fuse into one
    node, one edge per (s,r,o,WITNESS) with support summed WITHIN the
    family only (E12/A1 — cross-family aggregation is compiler output,
    never store state), and ONE governance seed per metric — the
    highest-authority initial state — so the E7 transition history never
    starts with an illegal sequence.

    LOB discipline: a certified/pending catalog's declared
    lineOfBusiness mints/corroborates OWNERSHIP (``in_lob``), resolved
    through the steward's ``lob_aliases``. A MINED business_unit names
    who RUNS the queries — it resolves through ``usage_targets`` (LOB +
    org-unit codes and aliases) to a ``used_by`` edge, never an
    ownership edge; anything outside the map is counted
    ``usage_unmatched``, never guessed into the graph."""
    report: dict = defaultdict(int)
    nodes: dict[str, dict] = {}
    node_prov: dict[str, tuple[str, int, str]] = {}
    edges: dict[tuple[str, str, str, str], dict] = {}
    states: dict[str, str] = {}

    # steward-declared equivalence: a DMP display name and a steward
    # code resolving to the same lob node (lob_alias_map) — applied
    # BEFORE minting or corroborating, so parallel nodes never fork
    aliases = lob_aliases or {}

    def canonical_lob(raw: str) -> str:
        lid = lob_id(raw)
        return aliases.get(lid, lid)

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
                "author_id": record.extra.get("author_id") or "",
                "description": record.extra.get("description") or "",
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
                # Studio-export texture (observed SQL shape + lineage
                # mismatch + stewardship contacts). grain_observed is
                # DELIBERATELY outside the identity fingerprint: the
                # certified catalog carries no grain, so an observed
                # grain entering identity would fork every fusion.
                "query_shape": record.extra.get("query_shape") or [],
                "grain_observed":
                    record.extra.get("grain_observed") or "",
                "data_owners": record.extra.get("data_owners") or [],
                "join_condition":
                    record.extra.get("join_condition") or "",
                "tables_associated_not_referenced":
                    record.extra.get("tables_associated_not_referenced")
                    or [],
            }, record)
            # full referenced SQL rides WHOLE as a doc node (the
            # view-SQL pattern) — "the full SQL is deliberately
            # retained because future extraction can derive more".
            # Source-neutral: dmp's referencedSqlQuery and the studio
            # export land on the SAME doc when they fuse (same fp).
            referenced_query = str(
                record.extra.get("referenced_query") or "").strip()
            if referenced_query:
                doc = f"doc:referenced_sql_{metric_fp}"
                merge_node(doc, {"kind": "referenced_sql",
                                 "sql": referenced_query}, record)
                merge_edge(metric, "evidenced_by", doc, record)
            # ── LOB layer (witnessed classification, never guessed) ──
            lob_raw = str(record.extra.get("line_of_business")
                          or "").strip()
            dom_raw = str(record.extra.get("domain") or "").strip()
            bu_raw = str(record.extra.get("business_unit") or "").strip()
            if int(record.authority) >= int(Authority.PENDING):
                if lob_raw:
                    lid = canonical_lob(lob_raw)
                    if lid == lob_id(lob_raw):
                        # novel lob — mint; an ALIASED value must not
                        # clobber the steward node's code on fold
                        merge_node(lid, {"code": lob_raw}, record)
                    merge_edge(tid, "in_lob", lid, record)
                    report["lob_edges"] += 1
                if dom_raw:
                    mid = mdom_id(dom_raw)
                    merge_node(mid, {"name": dom_raw}, record)
                    merge_edge(metric, "in_domain", mid, record)
                    if lob_raw:
                        merge_edge(mid, "in_lob",
                                   canonical_lob(lob_raw), record)
                    report["domain_edges"] += 1
            elif bu_raw:
                # usage, never ownership: the mined business_unit names
                # who RUNS the queries — resolved through the steward's
                # usage-target map (LOB + org-unit codes and aliases)
                # to a used_by edge; anything outside the map is
                # counted, never guessed
                target = (usage_targets or {}).get(lob_id(bu_raw))
                if target:
                    merge_edge(tid, "used_by", target, record)
                    report["used_by_edges"] += 1
                else:
                    report["usage_unmatched"] += 1
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


def _kept(props: dict) -> dict:
    """Empty is ABSENT, never an assertion. A fold is last-wins per
    key, so writing ``""``/``None`` for a field this registration
    happens not to carry would erase a value another registration (or
    another witness) did carry. False and 0 ARE assertions and stay."""
    return {k: v for k, v in props.items()
            if v not in (None, "", [], {})}


def emit_std_tech(entries: list[StdTechEntry], terms: list[TermRecord],
                  graph: GraphDir, crosswalk: Crosswalk,
                  run_id: str) -> dict:
    """The real feed registers the SAME table more than once — one
    envelope's tech_metadata_list can hold several registrations (46
    files yielded 70 entries on the first real run). A registration is
    testimony about the same facts, not new facts: nodes and edges emit
    ONCE (document order, first registration wins), repeats are counted,
    and the dedup keeps validator check [8] meaningful.

    FULL UTILIZATION (E12/A2 at field grain): every field the loader
    parses lands as a node prop or an edge here. What that buys, beyond
    the props:

    - ``ownership`` becomes ``owned_by`` edges per role (any key naming
      an owner or a VP), so Atlas and Lumi corroborate on the same
      owner nodes instead of Atlas ownership sitting inert in a dict;
    - table-level ``has_pii`` finally emits its ``has_policy`` edge —
      only oncop/gdpr did before, so the strongest of the three flags
      was the one the ACL never saw;
    - ``pii_columns[]`` is a SECOND, independent PII witness: it
      corroborates a column's own ``pii_role_id``, supplies one where
      the pde listing carried none (union-most-restrictive, E1), and
      mints a column the pde listing missed entirely;
    - ``derived_logic`` rides WHOLE as a doc node (the view-SQL
      pattern) behind a ``described_by`` edge — retained for a future
      canon pass, readable now;
    - a business term resolves by ``businessTermId`` FIRST and by name
      only as a fallback, and ``businessTermDescription`` lands on the
      term node — business_terms.csv carries no definition text, so
      this is the only place the meaning of a term exists at all.
    """
    report: dict = defaultdict(int)
    term_by_name = {t.name.strip().lower(): t.term_id for t in terms}
    known_term_ids = {t.term_id for t in terms}
    seen_nodes: set[str] = set()
    seen_edges: set[tuple[str, str, str]] = set()
    # unmatched term declarations accumulate per column and emit ONCE
    # at the end: a fold is last-wins per key, so appending them one at
    # a time would leave only the last one standing
    declared_terms: dict[str, list[dict]] = defaultdict(list)

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
        table_props = _kept({
            "description_atlas": entry.description,
            "business_name_atlas": entry.business_name,
            "data_category": entry.data_category,
            "data_sub_category": entry.data_sub_category,
            "layer_type": entry.layer_type,
            "has_pii_atlas": entry.has_pii,
            "has_oncop_atlas": entry.has_oncop,
            "has_gdpr_atlas": entry.has_gdpr,
            "ownership_atlas": entry.ownership,
            # ── envelope + Layer 2: where this table LIVES ──
            "appl_id": entry.appl_id,
            "project_atlas": entry.datasource,
            "dataset_group_atlas": entry.dataset_group,
            "data_server_atlas": entry.data_server,
            "data_system_atlas": entry.data_system,
            "technology_atlas": entry.technology,
            "is_active_atlas": entry.is_active,
            "is_latest_atlas": entry.is_latest,
            "is_lineage_exist_atlas": entry.is_lineage_exist,
            # ── Layer 3: how it is BUILT ──
            "table_name_atlas": entry.table_name,
            "table_type_atlas": entry.table_type,
            "load_type_atlas": entry.load_type,
            "is_partitioned_atlas": entry.is_partitioned,
            "target_system_atlas": entry.target_system,
        })
        if put_node(tid, table_props, entry.evidence_ref):
            report["tables"] += 1
        else:
            report["repeat_registrations"] += 1
        # sensitivity is union-most-restrictive (E1): every compliance
        # flag the feed asserts becomes an explicit policy edge
        for flag, policy in ((entry.has_pii, "policy:pii"),
                             (entry.has_oncop, "policy:oncop"),
                             (entry.has_gdpr, "policy:gdpr")):
            if flag and put_edge(tid, "has_policy", policy,
                                 entry.evidence_ref):
                report["policy_flags"] += 1
        # ownership → owned_by edges, one per role. A key naming an
        # owner or a VP is a person; everything else in the dict (a
        # CAR id, a cost centre) stays a prop — an id is not an owner.
        for role, value in sorted(entry.ownership.items()):
            name = str(value or "").strip().lower()
            if not name or not ownership_key_is_person(role):
                continue
            put_node(owner_id(name), {"role": role, "owner": name},
                     entry.evidence_ref)
            if put_edge(tid, "owned_by", owner_id(name),
                        entry.evidence_ref, props={"role": role}):
                report["ownership_edges"] += 1

        # table-level PII declaration, indexed for the column pass
        declared_pii: dict[str, str | None] = {
            str(cell["column"]): cell.get("pii_role_id")
            for cell in entry.pii_columns if cell.get("column")}

        for column in entry.columns:
            cid = col_id(physical, column.name)
            role = column.pii_role_id
            table_role = declared_pii.get(column.name)
            if table_role and not role:
                # the pde listing carried no role and the table-level
                # declaration does: union-most-restrictive applies
                role = table_role
                report["pii_from_table_declaration"] += 1
            elif table_role and role and table_role != role:
                # two Atlas-internal declarations disagree — keep both,
                # visibly; E1's D5 handler arbitrates at compile time
                report["pii_role_disagreements"] += 1
            props = _kept({
                "description_atlas": column.description,
                "business_name_atlas": column.business_name,
                "data_type_atlas": column.data_type,
                "pii_role_id": role,
                "sde_group": column.sde_group,
                "column_name_atlas": column.column_name,
                "ordinal_atlas": column.position,
                "column_length": column.column_length,
                "nullable_atlas": column.nullable,
                "is_primary_key_atlas": column.primary_key,
                "is_partitioning_atlas": column.partition_key,
                "derived_logic": column.derived_logic,
            })
            if table_role and role and table_role != role:
                props["pii_role_id_table_declared"] = table_role
            if put_node(cid, props, entry.evidence_ref):
                report["columns"] += 1
            else:
                report["columns_repeated"] += 1
            put_edge(tid, "has_column", cid, entry.evidence_ref)
            if role:
                put_edge(cid, "has_policy", "policy:pii",
                         entry.evidence_ref)
            if column.derived_logic:
                # the computed column's own SQL rides WHOLE, the way
                # view SQL does — unparsed today, readable now, and
                # ready for the canon pass the contract anticipates
                doc = ("doc:derived_logic_"
                       + fingerprint(column.derived_logic, "bigquery",
                                     CANON_VERSION))
                put_node(doc, {"kind": "derived_logic",
                               "sql": column.derived_logic},
                         entry.evidence_ref)
                if put_edge(cid, "described_by", doc,
                            entry.evidence_ref):
                    report["derived_logic_docs"] += 1
            for link in column.linked_terms:
                name = str(link.get("businessTermName") or "").strip()
                description = str(
                    link.get("businessTermDescription") or "").strip()
                # the id is identity; the name is a spelling. Resolve
                # on the id FIRST — matching on name alone lost every
                # link whose spelling drifted from the glossary export
                declared_id = str(link.get("businessTermId")
                                  or "").strip()
                term_id = declared_id or term_by_name.get(name.lower())
                if not term_id:
                    # no id and no glossary match: nothing to mint an
                    # identity from (a slugged name would fork the node
                    # the moment the id arrives). The declaration is
                    # kept on the column so the text is not lost.
                    report["term_links_unmatched"] += 1
                    cell = {"name": name, "description": description}
                    if (name or description) and \
                            cell not in declared_terms[cid]:
                        declared_terms[cid].append(cell)
                    continue
                if declared_id and declared_id not in known_term_ids:
                    # Atlas declaring a term with an id attests it
                    # exists — mint it, counted, so a column↔term link
                    # is never dropped for the glossary export lagging
                    report["terms_minted_from_link"] += 1
                # businessTermDescription is the ONLY definition text
                # in the whole feed: business_terms.csv is id+name+
                # status. Write it even on a term node the glossary
                # already minted — a fold merges by key.
                graph.append_node(NodeRecord(
                    id=term_node_id(term_id),
                    props=_kept({"name": name, "term_id": term_id,
                                 "description": description}),
                    prov=Prov(source="atlas", run=run_id,
                              evidence=entry.evidence_ref)))
                if put_edge(cid, "mapped_term", term_node_id(term_id),
                            entry.evidence_ref,
                            props=_kept({
                                "mapping_source":
                                    link.get("sourceName", ""),
                                "mapping_type":
                                    link.get("sourceType", ""),
                                "confidence":
                                    link.get("confidenceScore"),
                                "matched_on": ("id" if declared_id
                                               else "name")})):
                    report["term_links"] += 1

        # a column the pde listing missed but the table-level PII
        # declaration names: the declaration attests it exists — mint
        # the endpoint, counted, so the 02-vs-catalog drift stays
        # visible instead of the PII flag vanishing with the column
        listed = {c.name for c in entry.columns}
        for cname, cell_role in sorted(declared_pii.items()):
            if cname in listed:
                continue
            cid = col_id(physical, cname)
            if put_node(cid, _kept({"pii_role_id": cell_role,
                                    "observed_via":
                                        "table_pii_declaration"}),
                        entry.evidence_ref):
                report["columns_from_pii_declaration"] += 1
            put_edge(tid, "has_column", cid, entry.evidence_ref)
            put_edge(cid, "has_policy", "policy:pii", entry.evidence_ref)

    # a business term Atlas names with neither an id nor a spelling the
    # glossary knows: no identity can be minted from that without
    # forking the node the moment the id arrives — but the TEXT is
    # still evidence, so it rides on the column it was declared for
    for cid, cells in sorted(declared_terms.items()):
        graph.append_node(NodeRecord(
            id=cid, props={"declared_terms": cells},
            prov=Prov(source="atlas", run=run_id,
                      evidence="std_tech_metadata")))
    return dict(report)
