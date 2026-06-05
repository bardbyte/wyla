"""Single-table loader orchestrator.

Reads from the .env-configured input paths, calls the existing synapse
loaders, and lays out the canonical sources_dir under graph_cache_dir/sources/."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Any

from rich.console import Console

from semantic_graph.config import Config

# Reach into the sibling synapse package (no duplication of 3000 lines)
_SYNAPSE_ROOT = Path(__file__).resolve().parents[4] / "synapse"
if str(_SYNAPSE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SYNAPSE_ROOT))

from synapse.loaders import load_bq_for_table, load_mdm_for_table  # noqa: E402, F401

_console = Console()


def load_all_sources(cfg: Config) -> Path:
    """Run all loaders for the one configured table. Returns the canonical
    sources_dir the graph builder reads from."""
    sources_dir = cfg.graph_cache_dir / "sources"
    sources_dir.mkdir(parents=True, exist_ok=True)

    _console.print(f"[bold cyan]── loading sources for {cfg.table_name} ──[/]")

    # 1. BQ extraction → bq_cache/, usage_history/, dq_rules/, lineage/, gold_queries/
    bq_result = load_bq_for_table(
        cfg.table_name,
        source_dir=cfg.bq_extraction_dir.parent if cfg.bq_extraction_dir.name == cfg.table_name
                   else cfg.bq_extraction_dir.parent,
        out_dir=sources_dir,
    )
    # If the user pointed BQ_EXTRACTION_DIR directly at the per-table folder,
    # use it as-is; otherwise the synapse loader expects parent/<table>/...
    if bq_result.status == "error" and "source dir not found" in (bq_result.error or ""):
        # User's BQ_EXTRACTION_DIR IS the per-table folder. Adapt by passing parent.
        actual_parent = cfg.bq_extraction_dir.parent
        # If the per-table folder is named differently from table_name, symlink
        # it under sources_dir/__bq_input__/<table_name>/
        staging = sources_dir / "__bq_input__"
        staging.mkdir(parents=True, exist_ok=True)
        target = staging / cfg.table_name
        if target.exists() or target.is_symlink():
            target.unlink()
        target.symlink_to(cfg.bq_extraction_dir.resolve())
        bq_result = load_bq_for_table(
            cfg.table_name, source_dir=staging, out_dir=sources_dir,
        )
    _print_result("BQ", bq_result)

    # 2. MDM single JSON file → mdm_cache/<table>.json
    #    The synapse mdm_loader accepts source_dir + uses cached file matching
    #    "<table_id>__mdm_raw.json" naming, OR hits the network. We stage the
    #    user's mdm.json under that expected name.
    mdm_staging = sources_dir / "__mdm_input__"
    mdm_staging.mkdir(parents=True, exist_ok=True)
    staged_mdm = mdm_staging / f"{cfg.table_name}__mdm_raw.json"
    if staged_mdm.exists() or staged_mdm.is_symlink():
        staged_mdm.unlink()
    if cfg.mdm_json_path.exists():
        try:
            staged_mdm.symlink_to(cfg.mdm_json_path.resolve())
        except OSError:
            # Filesystem rejected the symlink — copy instead
            shutil.copy2(cfg.mdm_json_path, staged_mdm)
    mdm_result = load_mdm_for_table(
        cfg.table_name,
        source_dir=mdm_staging,
        out_dir=sources_dir,
    )
    _print_result("MDM", mdm_result)

    # 3. SQL queries → gold_queries/
    gold_dir = sources_dir / "gold_queries"
    gold_dir.mkdir(parents=True, exist_ok=True)
    n_copied = 0
    if cfg.sql_queries_dir.exists():
        for sql_path in sorted(cfg.sql_queries_dir.rglob("*.sql")):
            target = gold_dir / sql_path.name
            if target.exists():
                target.unlink()
            shutil.copy2(sql_path, target)
            n_copied += 1
    _console.print(f"  [green]✓[/] corpus: {n_copied} .sql files copied → {gold_dir}")

    # 4. Empty placeholders for sources we don't have (graph builder
    #    treats missing dirs as no-op; we just need the dir tree to exist)
    for sub in (
        "registries/raw", "ai_descriptions", "baseline_views",
    ):
        (sources_dir / sub).mkdir(parents=True, exist_ok=True)

    # Synthesize a minimal table_catalog with just our one table so the
    # graph builder seeds the Table node from the catalog source too
    catalog_path = sources_dir / "registries" / "raw" / "table_catalog.csv"
    if not catalog_path.exists():
        catalog_path.write_text(
            "table_name,IS IN DMP,company_domain,data_domain\n"
            f"{cfg.table_name},Yes,Finance,Cardmember\n",
            encoding="utf-8",
        )
        _console.print("  [green]✓[/] table_catalog: seeded single-row catalog")

    _console.print(f"[bold green]── sources staged at {sources_dir} ──[/]\n")
    return sources_dir


def _print_result(label: str, result: Any) -> None:
    if result.status == "ok":
        _console.print(
            f"  [green]✓[/] {label}: {result.records_count} records, "
            f"{len(result.artifacts_written)} files, "
            f"{result.latency_ms} ms"
        )
    elif result.status == "partial":
        _console.print(
            f"  [yellow]~[/] {label}: partial — {len(result.warnings)} warnings"
        )
        for w in result.warnings:
            _console.print(f"      [dim]{w}[/]")
    else:
        _console.print(f"  [red]✗[/] {label}: {result.error}")
