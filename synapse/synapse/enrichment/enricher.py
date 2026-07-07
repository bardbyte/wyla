"""Enrichment pipeline — runs the LLM pass + reduces memory to entity proposals.

Two public functions:
    enrich_graph(store, llm_client)
        Per-table LLM enrichment. Writes back as `llm_generated` facts.
        Mutates the store in place.

    propose_entities(observations)
        Pure reduction over ColumnObservation memory → EntityProposals.
        No LLM call here; just clustering by candidate_entity_name and
        cross-table corroboration thresholds.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Protocol

from synapse.enrichment.schemas import (
    ColumnObservation,
    EnrichmentBundle,
    EntityProposal,
    RelationProposal,
)
from synapse.graph.inspector import inspect_table
from synapse.graph.store import GraphStore, canonical_uri

# Path the skill.md lives at — loaded once per run
_SKILL_MD_PATH = Path(__file__).resolve().parent / "skill.md"


class LLMClient(Protocol):
    """The minimal interface the enricher needs from an LLM client.

    Real implementation: a thin Vertex-Gemini wrapper that calls
    `generate_content(model='gemini-3.1-pro-preview', response_schema=...)`.
    Test implementation: returns a canned EnrichmentBundle.
    """

    def enrich(
        self, *, skill_md: str, context: dict[str, Any], table_name: str,
    ) -> EnrichmentBundle: ...


def enrich_graph(
    store: GraphStore,
    llm_client: LLMClient,
    *,
    only_tables: list[str] | None = None,
    memory_out: Path | None = None,
    column_batch_size: int = 40,
    max_calls: int | None = None,
    grounding_reports: dict[str, dict] | None = None,
    evidence_dir: Path | None = None,
    demo_out: Path | None = None,
) -> dict[str, EnrichmentBundle]:
    """Run the LLM enrichment pass over every Table in the store — BATCHED.

    Wide tables (the warehouse has 1,400-column tables) cannot ship their
    whole inspection in one prompt, so columns are chunked: one LLM call
    per ≤column_batch_size columns, table-level context repeated per
    chunk, partial bundles merged before a single apply per table.

    Args:
        store: the built GraphStore (mutated in place).
        llm_client: implementor of LLMClient — called once per column batch.
        only_tables: restrict enrichment to these table names.
        memory_out: dump the merged EnrichmentBundles as JSON for steward
            audit + entity-proposal input.
        column_batch_size: columns per LLM call (0/None → one call per
            table regardless of width — legacy behavior).
        max_calls: hard budget across the whole run; tables that don't fit
            are skipped and reported (never silently).
        evidence_dir: staged per-table signal files (the session split's
            ``mdm_cache/<table>.json``) — real SQL snippets from
            ``queries_using_this`` are fed to the LLM as primary evidence.
        demo_out: where to write the demo pack (a ``.json`` path; a
            rehearsal-ready ``.md`` script lands alongside it). Only
            questions that survived BOTH gates — grounded references AND
            every claimed capability present in the built graph — reach
            the script.

    Returns:
        dict mapping table_name → merged EnrichmentBundle (also written
        into the graph as `llm_generated` provenance). Skipped tables are
        listed in each bundle's self_assessment when the budget ran out.
    """
    skill_md = _SKILL_MD_PATH.read_text(encoding="utf-8")
    bundles: dict[str, EnrichmentBundle] = {}
    calls_made = 0
    skipped_for_budget: list[str] = []
    demo_pack: list[dict[str, Any]] = []
    demo_pack_held: list[dict[str, Any]] = []

    table_nodes = [
        n for n in store.nodes_by_type("Table")
        if n.properties.get("table_name")
    ]
    # cross-table awareness: every call sees the names+columns of ALL
    # in-graph tables (not just the enrich scope) so relates_to proposals
    # can target real siblings instead of hallucinated table names
    scope_digest = _scope_digest(store, table_nodes)
    if only_tables:
        wanted = {t.lower() for t in only_tables}
        table_nodes = [
            n for n in table_nodes
            if str(n.properties.get("table_name", "")).lower() in wanted
        ]

    for node in table_nodes:
        table_name = node.properties["table_name"]
        context = _build_context_for_table(
            store, table_name,
            evidence_dir=evidence_dir, scope_digest=scope_digest)
        all_columns = (context.get("inspection") or {}).get("columns") or []
        if column_batch_size and len(all_columns) > column_batch_size:
            chunks = [
                all_columns[i:i + column_batch_size]
                for i in range(0, len(all_columns), column_batch_size)
            ]
        else:
            chunks = [all_columns]

        # Budget: a wide table that doesn't fully fit gets its FIRST
        # remaining chunks enriched (partial coverage beats none — a
        # 4,400-column table must not be skippable forever); only a
        # fully-spent budget skips a table outright.
        partial_note = None
        if max_calls is not None:
            remaining = max_calls - calls_made
            if remaining <= 0:
                skipped_for_budget.append(table_name)
                continue
            if len(chunks) > remaining:
                partial_note = (
                    f"partial enrichment: first {remaining} of "
                    f"{len(chunks)} column chunks (call budget)")
                chunks = chunks[:remaining]

        parts: list[EnrichmentBundle] = []
        for chunk_no, chunk in enumerate(chunks):
            chunk_context = dict(context)
            inspection = dict(context.get("inspection") or {})
            inspection["columns"] = chunk
            chunk_context["inspection"] = inspection
            chunk_context["batch"] = {
                "chunk": chunk_no + 1, "of": len(chunks),
                "columns_in_chunk": len(chunk),
                "total_columns": len(all_columns),
            }
            parts.append(llm_client.enrich(
                skill_md=skill_md, context=chunk_context,
                table_name=table_name,
            ))
            calls_made += 1

        bundle = parts[0] if len(parts) == 1 else _merge_bundles(
            table_name, parts)
        if partial_note:
            bundle.self_assessment.requires_steward_attention.append(
                partial_note)
        table_report = _apply_bundle(store, bundle)
        # demo packs are payloads, not counters — pull them out so
        # grounding_reports stays int-only for the pipeline's totals
        demo_pack.extend(table_report.pop("demo_pack", []))
        demo_pack_held.extend(table_report.pop("demo_pack_held", []))
        if grounding_reports is not None:
            grounding_reports[table_name] = table_report
        bundles[table_name] = bundle

    if skipped_for_budget and bundles:
        # surface the budget skip in-band, never silently
        first = next(iter(bundles.values()))
        first.self_assessment.tables_skipped_for_lack_of_signal.extend(
            f"{t} (enrichment budget exhausted)" for t in skipped_for_budget
        )

    if memory_out is not None:
        memory_out.parent.mkdir(parents=True, exist_ok=True)
        memory_out.write_text(
            json.dumps(
                {tn: b.model_dump() for tn, b in bundles.items()},
                indent=2, default=str,
            ),
            encoding="utf-8",
        )
    if demo_out is not None:
        demo_out.parent.mkdir(parents=True, exist_ok=True)
        demo_out.write_text(
            json.dumps({"verified": demo_pack, "held": demo_pack_held},
                       indent=2, default=str),
            encoding="utf-8",
        )
        demo_out.with_suffix(".md").write_text(
            _render_demo_script(demo_pack, n_held=len(demo_pack_held)),
            encoding="utf-8",
        )
    return bundles


def _merge_bundles(
    table_name: str, parts: list[EnrichmentBundle],
) -> EnrichmentBundle:
    """Column-batch partials → one table bundle (dedupe by natural keys)."""
    from synapse.enrichment.schemas import SelfAssessment

    description = next(
        (p.table_description_proposal for p in parts
         if p.table_description_proposal), None)
    observations, seen_cols = [], set()
    for p in parts:
        for obs in p.column_observations:
            if obs.column_name not in seen_cols:
                seen_cols.add(obs.column_name)
                observations.append(obs)
    synonyms, seen_syn = [], set()
    for p in parts:
        for syn in p.candidate_synonyms:
            key = (syn.surface_form.lower(), syn.canonical_form.lower())
            if key not in seen_syn:
                seen_syn.add(key)
                synonyms.append(syn)
    resolutions, seen_res = [], set()
    for p in parts:
        for cr in p.candidate_code_resolutions:
            key = (cr.column.lower(), cr.raw_value)
            if key not in seen_res:
                seen_res.add(key)
                resolutions.append(cr)
    filters = [f for p in parts for f in p.candidate_filter_rationale]
    questions, seen_q = [], set()
    for p in parts:
        for q in p.candidate_demo_questions:
            key = _norm(q.question)
            if key and key not in seen_q:
                seen_q.add(key)
                questions.append(q)
    attention = [a for p in parts
                 for a in p.self_assessment.requires_steward_attention]
    return EnrichmentBundle(
        table_name=table_name,
        table_description_proposal=description,
        column_observations=observations,
        candidate_synonyms=synonyms,
        candidate_code_resolutions=resolutions,
        candidate_filter_rationale=filters,
        candidate_demo_questions=questions,
        self_assessment=SelfAssessment(
            tables_skipped_for_lack_of_signal=[],
            columns_marked_ambiguous=sum(
                1 for o in observations if o.ambiguity_flag),
            proposed_entities_with_low_evidence=[],
            requires_steward_attention=attention,
        ),
    )


def propose_entities(
    bundles: dict[str, EnrichmentBundle],
    *,
    min_supporting_tables: int = 3,
    min_aggregate_confidence: float = 0.7,
) -> list[EntityProposal]:
    """Reduce ColumnObservations across tables into EntityProposals.

    Clustering rule: group observations by (candidate_entity_name,
    column_name). An entity is proposed when:
      - it appears in ≥ min_supporting_tables distinct tables, AND
      - the geometric mean of self_confidence across observations
        is ≥ min_aggregate_confidence.
    Below these thresholds the cluster is held back for steward review
    via the review_queue rather than promoted to a proposal.
    """
    # Group: entity_name → list[(table_name, obs)]
    by_entity: dict[str, list[tuple[str, ColumnObservation]]] = defaultdict(list)
    for table_name, bundle in bundles.items():
        for obs in bundle.column_observations:
            if obs.candidate_entity_name and obs.candidate_role == "identifier":
                by_entity[obs.candidate_entity_name].append((table_name, obs))

    proposals: list[EntityProposal] = []
    for entity_name, observations in by_entity.items():
        distinct_tables = sorted({t for t, _ in observations})
        if len(distinct_tables) < min_supporting_tables:
            continue

        # Geometric mean of self_confidence (penalizes low-confidence outliers)
        product = 1.0
        for _, obs in observations:
            product *= max(obs.self_confidence, 1e-6)
        agg_conf = product ** (1.0 / len(observations))
        if agg_conf < min_aggregate_confidence:
            continue

        identifier_columns = sorted({obs.column_name for _, obs in observations})
        all_relations: list[RelationProposal] = []
        for _, obs in observations:
            all_relations.extend(obs.relates_to)
        # Dedupe relations by (target_table, target_column, verb)
        seen: set[tuple[str, str, str]] = set()
        deduped: list[RelationProposal] = []
        for r in all_relations:
            key = (r.target_table, r.target_column, r.verb)
            if key not in seen:
                deduped.append(r)
                seen.add(key)

        conflicts: list[str] = []
        for _, obs in observations:
            if obs.ambiguity_flag:
                conflicts.append(f"{obs.column_name}: {obs.ambiguity_flag}")

        proposals.append(EntityProposal(
            proposed_name=entity_name,
            identified_by_columns=identifier_columns,
            materialized_in_tables=distinct_tables,
            relationships=deduped,
            conflict_signals=conflicts,
            evidence_packet_refs=[
                f"{t}::{obs.column_name}" for t, obs in observations
            ],
            aggregate_self_confidence=round(agg_conf, 3),
            n_supporting_observations=len(observations),
            requires_steward_review=True,
        ))
    return sorted(proposals, key=lambda p: -p.aggregate_self_confidence)


# ─── Internals ───────────────────────────────────────────────


def _build_context_for_table(
    store: GraphStore,
    table_name: str,
    *,
    evidence_dir: Path | None = None,
    scope_digest: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """The JSON payload handed to the LLM per table.

    Gemini's accuracy is bounded by the evidence it sees, so this packs
    ALL primary sources — the fused inspector view, the real SQL that
    analysts ran against this table (session signals), the credit-risk
    skill knowledge that governs it, and a digest of every sibling table
    in the graph (the only legal relates_to targets). The LLM consumes
    ONLY this payload — it does not see the raw graph."""
    inspection = inspect_table(store, table_name)
    return {
        "inspection": inspection,
        "corpus_evidence": {
            "related_tables": inspection.get("related_tables", []),
            "metrics": inspection.get("metrics", []),
            "code_resolutions": inspection.get("code_resolutions", []),
        },
        # real analyst SQL — the strongest grounding signal we own
        "corpus_sql_evidence": _corpus_sql_evidence(evidence_dir, table_name),
        # curated skill knowledge that APPLIES_TO this table
        "skills_evidence": _skills_evidence(store, table_name),
        # sibling tables: names + columns; relates_to may ONLY target these
        "tables_in_scope": scope_digest or [],
        # Steward glossary entries — the loader will populate this once
        # we ingest the steward glossary.md. For v1, empty.
        "steward_glossary": [],
    }


def _corpus_sql_evidence(
    evidence_dir: Path | None, table_name: str,
    *, max_queries: int = 15, max_chars: int = 1500,
) -> dict[str, Any]:
    """Real SQL snippets from the staged session signals for this table.

    Tolerates both entry shapes (dict with sql/query/text keys, or a bare
    string) and trims hard — evidence, not a transcript dump."""
    if evidence_dir is None:
        return {}
    path = Path(evidence_dir) / f"{table_name}.json"
    if not path.exists():
        return {}
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    queries: list[str] = []
    for entry in (blob.get("queries_using_this") or [])[:max_queries]:
        if isinstance(entry, dict):
            text = str(entry.get("sql") or entry.get("query")
                       or entry.get("query_text") or entry.get("text") or "")
        else:
            text = str(entry)
        text = text.strip()
        if text:
            queries.append(text[:max_chars])
    return {
        "queries": queries,
        "aggregations": (blob.get("aggregations") or [])[:30],
        "n_queries_total": len(blob.get("queries_using_this") or []),
    }


def _skills_evidence(
    store: GraphStore, table_name: str, *, max_excerpt: int = 2000,
) -> list[dict[str, Any]]:
    """Curated skill packages that APPLIES_TO this table — steward-grade
    domain knowledge the LLM should treat as high-authority evidence."""
    t_uri = canonical_uri("table", table_name)
    out: list[dict[str, Any]] = []
    for edge in store.incoming(t_uri, "APPLIES_TO"):
        skill = store.get(edge.from_uri)
        if skill is None or skill.node_type != "Skill":
            continue
        props = skill.properties
        out.append({
            "skill_id": props.get("skill_id"),
            "domain": props.get("domain"),
            "description": props.get("description"),
            "knowledge_excerpt": str(
                props.get("knowledge_excerpt") or "")[:max_excerpt],
            "metrics_defined": props.get("metrics_defined") or [],
        })
    return out


def _scope_digest(
    store: GraphStore, table_nodes: list, *, max_tables: int = 40,
    max_columns: int = 80,
) -> list[dict[str, Any]]:
    """Compact per-table digest of the whole graph scope — lets Gemini
    ground cross-table relates_to proposals in real sibling schemas."""
    digest: list[dict[str, Any]] = []
    for node in table_nodes[:max_tables]:
        cols = sorted(
            e.to_uri.rsplit("/", 1)[-1]
            for e in store.outgoing(node.canonical_uri, "CONTAINS")
        )
        digest.append({
            "table": node.properties.get("table_name"),
            "description": str(node.properties.get("description") or "")[:200],
            "columns": cols[:max_columns],
            "n_columns": len(cols),
        })
    return digest


def _grounding_index(store: GraphStore) -> set[str]:
    """Every name the graph actually knows, normalized — the reference
    set the grounding gate checks LLM claims against."""
    names: set[str] = set()

    def add(value: Any) -> None:
        text = str(value or "").strip().lower()
        if text:
            names.add(re.sub(r"[^a-z0-9]", "", text))

    for node in store.nodes.values():
        add(node.canonical_uri.rsplit("/", 1)[-1])
        for key in ("table_name", "business_name", "surface_form",
                    "canonical_entity", "skill_id", "entity_name"):
            add(node.properties.get(key))
    names.discard("")
    return names


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _has_signal(value: Any) -> bool:
    """True when a chunk of inspector output carries real content."""
    if isinstance(value, dict):
        return any(_has_signal(v) for v in value.values())
    if isinstance(value, (list, tuple, set)):
        return len(value) > 0
    return value not in (None, "", 0, False)


def _capabilities_present(store: GraphStore, table_name: str) -> dict[str, bool]:
    """Which demo capabilities the BUILT graph actually has for this
    table — the answerability check behind the demo-question gate."""
    inspection = inspect_table(store, table_name)
    norm_table = _norm(table_name)
    return {
        "column_semantics": _has_signal(inspection.get("columns")),
        "metrics": _has_signal(inspection.get("metrics")),
        "code_resolutions": _has_signal(inspection.get("code_resolutions")),
        "related_tables": _has_signal(inspection.get("related_tables")),
        "lineage": _has_signal(inspection.get("lineage")),
        "governance": _has_signal(inspection.get("governance")),
        "usage": _has_signal(inspection.get("usage")),
        "guardrails": any(
            e.edge_type == "CONSTRAINS" and norm_table in _norm(e.to_uri)
            for e in store.edges.values()),
        # verifiable only against live BQ at demo time — allowed through,
        # flagged in the rendered script
        "warehouse_sql": True,
    }


def _apply_bundle(
    store: GraphStore, bundle: EnrichmentBundle,
) -> dict[str, Any]:
    """Fold the LLM bundle back into the graph as `llm_generated` facts —
    through the GROUNDING GATE. skill.md's anti-patterns are enforced
    here as code, not prose:

      * observations for columns the graph doesn't have → dropped
      * descriptions with self_confidence < 0.3 OR empty evidence_used
        → HELD (ambiguity/role still recorded; text never written)
      * synonyms whose canonical_form matches nothing the graph knows
        (any column/metric/table/entity name or business name) → dropped
      * code resolutions for columns that don't exist → dropped
      * relates_to targets that don't exist as real columns → dropped;
        relations the graph already witnessed (corpus joins) → skipped —
        the LLM only echoes corpus evidence, it is not an independent
        witness, so re-asserting would inflate confidence

    Every drop/hold is counted in the returned grounding report so the
    run is auditable — accuracy is enforced, not assumed. Tag every
    write with source='llm_generated'; the tier machinery keeps these
    facts below `grounded` until real witnesses corroborate."""
    report = {
        "applied_descriptions": 0,
        "held_low_confidence": 0,
        "held_no_evidence": 0,
        "dropped_imagined_columns": 0,
        "dropped_ungrounded_synonyms": 0,
        "dropped_ungrounded_code_resolutions": 0,
        "applied_relations": 0,
        "skipped_existing_relations": 0,
        "dropped_ungrounded_relations": 0,
        "applied_demo_questions": 0,
        "held_unanswerable_demo_questions": 0,
        "dropped_ungrounded_demo_questions": 0,
        "ambiguity_flags": 0,
        # non-counter payloads — popped by enrich_graph before the report
        # joins grounding_reports (which stay int-only for aggregation)
        "demo_pack": [],
        "demo_pack_held": [],
    }
    known = _grounding_index(store)
    t_uri = canonical_uri("table", bundle.table_name)

    # Table description (only if sparse — see skill.md decision rules)
    if bundle.table_description_proposal:
        existing = store.get(t_uri)
        if existing is not None:
            existing_desc = (existing.properties.get("description") or "").strip()
            if len(existing_desc) < 40:
                store.upsert_node(
                    "Table", t_uri,
                    properties={
                        "table_name": bundle.table_name,
                        "ai_generated_description": bundle.table_description_proposal,
                    },
                    source="llm_generated",
                )

    # Column observations → ai_generated_description + role hint
    for obs in bundle.column_observations:
        c_uri = canonical_uri("column", bundle.table_name, obs.column_name)
        if c_uri not in store.nodes:
            report["dropped_imagined_columns"] += 1
            continue  # don't mint columns the LLM imagined
        update: dict[str, Any] = {
            "table_name": bundle.table_name,
            "candidate_role": obs.candidate_role,
            "llm_self_confidence": obs.self_confidence,
        }
        if obs.proposed_description:
            # anti-patterns 7 + calibration: no evidence or low confidence
            # → the TEXT is held; role/ambiguity still land for audit
            if not obs.evidence_used:
                report["held_no_evidence"] += 1
            elif obs.self_confidence < 0.3:
                report["held_low_confidence"] += 1
            else:
                update["ai_generated_description"] = obs.proposed_description
                report["applied_descriptions"] += 1
        if obs.candidate_entity_name:
            update["candidate_entity_name"] = obs.candidate_entity_name
        if obs.ambiguity_flag:
            update["ambiguity_flag"] = obs.ambiguity_flag
            report["ambiguity_flags"] += 1
        store.upsert_node(
            "Column", c_uri, properties=update, source="llm_generated",
        )

        # relates_to → EQUIVALENT_TO edges, gap-fill only. The target
        # must exist as a real column, and an edge the corpus already
        # observed is skipped — the LLM read that corpus, so re-asserting
        # the same join is not independent corroboration.
        for rel in obs.relates_to:
            target_uri = canonical_uri(
                "column", rel.target_table, rel.target_column)
            if target_uri == c_uri or target_uri not in store.nodes:
                report["dropped_ungrounded_relations"] += 1
                continue
            fwd = f"{c_uri}::EQUIVALENT_TO::{target_uri}"
            rev = f"{target_uri}::EQUIVALENT_TO::{c_uri}"
            if fwd in store.edges or rev in store.edges:
                report["skipped_existing_relations"] += 1
                continue
            store.upsert_edge(
                "EQUIVALENT_TO", c_uri, target_uri,
                properties={
                    "verb": rel.verb,
                    "llm_evidence_count": rel.evidence_count,
                },
                source="llm_generated",
            )
            report["applied_relations"] += 1

    # Candidate synonyms — ONLY when the canonical form grounds to
    # something the graph actually knows (anti-pattern 3 enforced)
    for syn in bundle.candidate_synonyms:
        if _norm(syn.canonical_form) not in known:
            report["dropped_ungrounded_synonyms"] += 1
            continue
        s_uri = canonical_uri(
            "synonym", syn.surface_form,
            syn.scope_business_unit or "global",
            syn.scope_region or "global",
        )
        store.upsert_node(
            "Synonym", s_uri,
            properties={
                "surface_form": syn.surface_form,
                "canonical_entity": syn.canonical_form,
                "business_unit": syn.scope_business_unit or "",
                "region": syn.scope_region or "",
                "entry_type": "LLM_Inferred",
            },
            source="llm_generated",
        )

    # Candidate code resolutions — the column must exist (anti-pattern 5)
    for cr in bundle.candidate_code_resolutions:
        if _norm(cr.column) not in known:
            report["dropped_ungrounded_code_resolutions"] += 1
            continue
        cm_uri = canonical_uri("codemapping", cr.column, cr.raw_value)
        store.upsert_node(
            "CodeMapping", cm_uri,
            properties={
                "column": cr.column,
                "raw_value": cr.raw_value,
                "human_meaning": cr.proposed_meaning,
                "source": "llm_inferred",
                "llm_confidence": cr.confidence,
            },
            source="llm_generated",
        )

    # Demo questions — never written to the graph (they're a demo
    # artifact, not facts). Survive only when (a) at least one grounding
    # reference names something the graph knows, and (b) every claimed
    # capability is PRESENT in the built graph for this table — the
    # demo script must not contain a question the graph can't answer.
    if bundle.candidate_demo_questions:
        caps = _capabilities_present(store, bundle.table_name)
        for q in bundle.candidate_demo_questions:
            if not any(_norm(g) in known for g in q.grounding):
                report["dropped_ungrounded_demo_questions"] += 1
                continue
            missing = [c for c in q.answered_by if not caps.get(c)]
            entry = {**q.model_dump(), "table": bundle.table_name}
            if not q.answered_by or missing:
                entry["missing_capabilities"] = missing or ["(none claimed)"]
                report["held_unanswerable_demo_questions"] += 1
                report["demo_pack_held"].append(entry)
                continue
            report["applied_demo_questions"] += 1
            report["demo_pack"].append(entry)
    return report


def collect_enrichment_failures(
    bundles: dict[str, EnrichmentBundle],
) -> dict[str, Any]:
    """Post-run diagnosis: which bundles came back empty, and why.

    Failures are in-band by design (a bad chunk never kills the run) —
    but in-band must not mean invisible. The pipeline prints this digest
    so '79 calls, 0 observations' always arrives WITH its reasons."""
    from collections import Counter

    empty = 0
    notes: Counter[str] = Counter()
    for bundle in bundles.values():
        has_content = bool(
            bundle.column_observations or bundle.candidate_synonyms
            or bundle.candidate_code_resolutions
            or bundle.table_description_proposal)
        if not has_content:
            empty += 1
        for note in bundle.self_assessment.requires_steward_attention:
            notes[note[:160]] += 1
    return {
        "empty_bundles": empty,
        "n_bundles": len(bundles),
        "notes": notes.most_common(8),
    }


_AUDIENCE_ORDER = [
    ("c_suite", "C-Suite — the 30-second wins"),
    ("vp", "VP — governance, lineage, and trust"),
    ("analyst", "Analyst — depth on demand"),
]


def _render_demo_script(
    pack: list[dict[str, Any]], *, n_held: int = 0,
) -> str:
    """The verified demo pack → a rehearsal-ready markdown script.

    Every question in here passed BOTH gates: its grounding references
    resolve against the graph, and every capability its answer needs is
    present in the compiled snapshot. Ask any of these live."""
    lines = [
        "# Demo script — questions this graph provably answers",
        "",
        f"{len(pack)} verified question(s)"
        + (f" · {n_held} held back (capability not yet in this compile — "
           "see demo_questions.json)" if n_held else "") + ".",
        "Every entry passed the grounding gate: real entities, and the",
        "graph sections its answer needs are populated in THIS snapshot.",
        "",
    ]
    for audience, heading in _AUDIENCE_ORDER:
        rows = [q for q in pack if q.get("audience") == audience]
        if not rows:
            continue
        lines += [f"## {heading}", ""]
        for q in rows:
            caps = ", ".join(q.get("answered_by") or [])
            grounding = ", ".join(f"`{g}`" for g in (q.get("grounding") or []))
            lines += [
                f"### “{q.get('question', '').strip()}”",
                "",
                f"- **Table:** `{q.get('table', '')}` · **Uses:** {caps}",
                f"- **Grounded in:** {grounding}",
                f"- **Expected answer:** {q.get('expected_answer_sketch', '')}",
                f"- **Why it lands:** {q.get('wow_factor', '')}",
            ]
            if "warehouse_sql" in (q.get("answered_by") or []):
                lines.append(
                    "- ⚠ needs the live gated BigQuery path at demo time")
            lines.append("")
    return "\n".join(lines) + "\n"


# ─── Convenience factory: a mock LLM client for tests ───────


class MockLLMClient:
    """Returns a canned EnrichmentBundle. Lets the pipeline run with no
    external API. The real client lives in synapse.enrichment.vertex_client."""

    def __init__(self, response_factory: Callable[[str, dict], EnrichmentBundle]) -> None:
        self._response_factory = response_factory

    def enrich(
        self, *, skill_md: str, context: dict[str, Any], table_name: str,
    ) -> EnrichmentBundle:
        return self._response_factory(table_name, context)
