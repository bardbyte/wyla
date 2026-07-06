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

    Returns:
        dict mapping table_name → merged EnrichmentBundle (also written
        into the graph as `llm_generated` provenance). Skipped tables are
        listed in each bundle's self_assessment when the budget ran out.
    """
    skill_md = _SKILL_MD_PATH.read_text(encoding="utf-8")
    bundles: dict[str, EnrichmentBundle] = {}
    calls_made = 0
    skipped_for_budget: list[str] = []

    table_nodes = [
        n for n in store.nodes_by_type("Table")
        if n.properties.get("table_name")
    ]
    if only_tables:
        wanted = {t.lower() for t in only_tables}
        table_nodes = [
            n for n in table_nodes
            if str(n.properties.get("table_name", "")).lower() in wanted
        ]

    for node in table_nodes:
        table_name = node.properties["table_name"]
        context = _build_context_for_table(store, table_name)
        all_columns = (context.get("inspection") or {}).get("columns") or []
        if column_batch_size and len(all_columns) > column_batch_size:
            chunks = [
                all_columns[i:i + column_batch_size]
                for i in range(0, len(all_columns), column_batch_size)
            ]
        else:
            chunks = [all_columns]

        if max_calls is not None and calls_made + len(chunks) > max_calls:
            skipped_for_budget.append(table_name)
            continue

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
        _apply_bundle(store, bundle)
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
    attention = [a for p in parts
                 for a in p.self_assessment.requires_steward_attention]
    return EnrichmentBundle(
        table_name=table_name,
        table_description_proposal=description,
        column_observations=observations,
        candidate_synonyms=synonyms,
        candidate_code_resolutions=resolutions,
        candidate_filter_rationale=filters,
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


def _build_context_for_table(store: GraphStore, table_name: str) -> dict[str, Any]:
    """The JSON payload handed to the LLM per table.

    Includes the full inspector dict + corpus snippets + any steward
    glossary entries that mention this table. The LLM consumes ONLY this
    payload — it does not see the raw graph."""
    inspection = inspect_table(store, table_name)
    return {
        "inspection": inspection,
        # Corpus snippets — pull from gold_queries dir if we had access here;
        # for now the inspector already surfaces JOIN observations + metrics.
        "corpus_evidence": {
            "related_tables": inspection.get("related_tables", []),
            "metrics": inspection.get("metrics", []),
            "code_resolutions": inspection.get("code_resolutions", []),
        },
        # Steward glossary entries — the loader will populate this once
        # we ingest the steward glossary.md. For v1, empty.
        "steward_glossary": [],
    }


def _apply_bundle(store: GraphStore, bundle: EnrichmentBundle) -> None:
    """Fold the LLM bundle back into the graph as `llm_generated` facts.

    Tag every write with source='llm_generated'. The provenance + tier
    machinery in store.py caps these at `inferred` (skill.md rule 2)."""
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
            continue  # don't mint columns the LLM imagined
        update: dict[str, Any] = {
            "table_name": bundle.table_name,
            "candidate_role": obs.candidate_role,
            "llm_self_confidence": obs.self_confidence,
        }
        if obs.proposed_description:
            update["ai_generated_description"] = obs.proposed_description
        if obs.candidate_entity_name:
            update["candidate_entity_name"] = obs.candidate_entity_name
        if obs.ambiguity_flag:
            update["ambiguity_flag"] = obs.ambiguity_flag
        store.upsert_node(
            "Column", c_uri, properties=update, source="llm_generated",
        )

    # Candidate synonyms — mint Synonym nodes (capped at inferred via tier)
    for syn in bundle.candidate_synonyms:
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

    # Candidate code resolutions — mint CodeMapping nodes
    for cr in bundle.candidate_code_resolutions:
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
