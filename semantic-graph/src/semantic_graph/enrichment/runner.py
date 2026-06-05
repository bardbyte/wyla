"""Enrichment runner — batches columns, calls Gemini, folds results back."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

from semantic_graph.config import Config
from semantic_graph.enrichment.vertex_client import VertexEnrichmentClient

# Reach into sibling synapse package
_SYNAPSE_ROOT = Path(__file__).resolve().parents[4] / "synapse"
if str(_SYNAPSE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SYNAPSE_ROOT))

from synapse.enrichment.enricher import _apply_bundle, propose_entities  # noqa: E402
from synapse.enrichment.schemas import EnrichmentBundle, SelfAssessment  # noqa: E402
from synapse.graph.inspector import inspect_table                        # noqa: E402
from synapse.graph.store import GraphStore                               # noqa: E402

_console = Console()


def enrich_table(cfg: Config, store: GraphStore) -> dict[str, Any]:
    """Run the LLM enrichment for the configured table. Mutates store in place;
    writes the memory + proposals to disk."""
    skill_md = cfg.enrichment_skill_path.read_text(encoding="utf-8")
    client = VertexEnrichmentClient(
        model=cfg.gemini_model,
        skill_md=skill_md,
        dry_run=cfg.enrichment_dry_run,
    )

    inspection = inspect_table(store, cfg.table_name)
    if "error" in inspection:
        raise RuntimeError(
            f"inspect_table failed for {cfg.table_name}: {inspection['error']}"
        )

    all_columns = inspection["columns"]
    total = len(all_columns)
    batch_size = max(1, cfg.enrichment_batch_size)
    batches = [
        all_columns[i:i + batch_size] for i in range(0, total, batch_size)
    ]
    if len(batches) > cfg.enrichment_max_calls:
        _console.print(
            f"  [yellow]![/] {len(batches)} batches > max_calls={cfg.enrichment_max_calls}; "
            f"truncating to first {cfg.enrichment_max_calls}"
        )
        batches = batches[:cfg.enrichment_max_calls]

    _console.print(
        f"[bold cyan]── enrichment: {total} columns in {len(batches)} batches "
        f"({'DRY-RUN' if cfg.enrichment_dry_run else 'real Gemini'}) ──[/]"
    )

    merged_observations: list[Any] = []
    merged_synonyms: list[Any] = []
    merged_codes: list[Any] = []
    merged_filters: list[Any] = []
    aggregate_attention: list[str] = []
    table_description_proposal: str | None = None

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=_console,
    ) as progress:
        task = progress.add_task(
            f"[cyan]Gemini → {cfg.table_name}", total=len(batches),
        )
        for i, batch in enumerate(batches):
            batch_context = _build_batch_context(inspection, batch)
            bundle = client.enrich_batch(
                table_name=cfg.table_name, batch_context=batch_context,
            )
            merged_observations.extend(bundle.column_observations)
            merged_synonyms.extend(bundle.candidate_synonyms)
            merged_codes.extend(bundle.candidate_code_resolutions)
            merged_filters.extend(bundle.candidate_filter_rationale)
            aggregate_attention.extend(bundle.self_assessment.requires_steward_attention)
            if bundle.table_description_proposal and not table_description_proposal:
                table_description_proposal = bundle.table_description_proposal
            progress.advance(task)

    full_bundle = EnrichmentBundle(
        table_name=cfg.table_name,
        table_description_proposal=table_description_proposal,
        column_observations=merged_observations,
        candidate_synonyms=merged_synonyms,
        candidate_code_resolutions=merged_codes,
        candidate_filter_rationale=merged_filters,
        self_assessment=SelfAssessment(
            columns_marked_ambiguous=sum(1 for o in merged_observations if o.ambiguity_flag),
            requires_steward_attention=aggregate_attention,
        ),
    )

    _apply_bundle(store, full_bundle)
    _console.print(
        f"  [green]✓[/] enrichment applied: "
        f"{len(merged_observations)} observations, "
        f"{len(merged_synonyms)} synonyms, "
        f"{len(merged_codes)} code resolutions, "
        f"{len(merged_filters)} filter rationales"
    )

    # Save memory
    cfg.enrichment_memory_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.enrichment_memory_path.write_text(
        full_bundle.model_dump_json(indent=2), encoding="utf-8",
    )
    _console.print(f"  [green]✓[/] memory → {cfg.enrichment_memory_path}")

    # Entity proposals
    proposals = propose_entities(
        {cfg.table_name: full_bundle},
        min_supporting_tables=1,    # single-table demo
        min_aggregate_confidence=0.6,
    )
    cfg.entity_proposals_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.entity_proposals_path.write_text(
        json.dumps(
            [p.model_dump() for p in proposals], indent=2, default=str,
        ),
        encoding="utf-8",
    )
    _console.print(
        f"  [green]✓[/] {len(proposals)} entity proposal(s) → {cfg.entity_proposals_path}"
    )

    # Re-snapshot the now-enriched graph
    snapshot_path = cfg.graph_cache_dir / "graph_snapshot.json"
    snapshot_path.write_text(store.model_dump_json(indent=2), encoding="utf-8")
    _console.print(f"  [green]✓[/] enriched graph → {snapshot_path}")

    return {
        "bundle": full_bundle,
        "proposals": proposals,
    }


def _build_batch_context(inspection: dict, columns: list[dict]) -> dict[str, Any]:
    """The context object the LLM sees for one batch."""
    return {
        "table_identity": inspection.get("identity", {}),
        "table_fused_view": inspection.get("fused_view", {}),
        "table_governance": inspection.get("governance", {}),
        "table_usage": inspection.get("usage", {}),
        "columns_in_batch": columns,
        "metrics_known": inspection.get("metrics", []),
        "related_tables_observed": inspection.get("related_tables", []),
        "code_resolutions_known": inspection.get("code_resolutions", []),
        "per_source_view": inspection.get("per_source_view", {}),
    }
