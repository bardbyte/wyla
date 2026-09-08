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

from synapse.loaders import (  # noqa: E402, F401
    load_bq_for_table,
    load_lumi_for_table,
    load_mdm_for_table,
)

_console = Console()


def load_all_sources(cfg: Config) -> Path:
    """Run every configured loader and stage outputs under sources_dir.

    Sources are ADDITIVE — each configured loader fires. The graph builder
    consumes the union and the Provenance envelope on every node/edge
    tracks which sources contributed. The full graph reflects every
    source that was wired:

      LUMI_OUTPUT_PATH    → MDM digest + baseline LookML + the ~35 queries
      BQ_EXTRACTION_DIR   → BQ schema/profiling, usage telemetry, lineage, DQ
      MDM_JSON_PATH       → only used if LUMI not set (lumi already has MDM)
      SQL_QUERIES_DIR     → corpus queries; additive to lumi's queries_using_this

    The same one table appears in both lumi and BQ outputs — that's the
    point: every fact gets two independent witnesses and the graph's
    confidence calibration reflects that.
    """
    sources_dir = cfg.graph_cache_dir / "sources"
    sources_dir.mkdir(parents=True, exist_ok=True)

    _console.print(
        f"[bold cyan]── loading sources for {cfg.table_name} ──[/]"
    )

    sources_fired: list[str] = []

    # ── 1. LUMI fused snapshot — MDM + baseline LookML + corpus
    if cfg.lumi_output_path is not None:
        _load_lumi_source(cfg, sources_dir)
        sources_fired.append("lumi")

    # ── 2. BQ extraction — schema, profile, lineage, usage, DQ
    if cfg.bq_extraction_dir is not None:
        _load_bq_source(cfg, sources_dir)
        sources_fired.append("bq")

    # ── 3. Standalone MDM (skip if lumi already provided MDM digest)
    if cfg.mdm_json_path is not None and cfg.lumi_output_path is None:
        _load_standalone_mdm(cfg, sources_dir)
        sources_fired.append("mdm")
    elif cfg.mdm_json_path is not None:
        _console.print(
            "  [dim]MDM_JSON_PATH ignored — lumi already provides MDM digest[/]"
        )

    # ── 4. Standalone SQL corpus folder (additive to lumi's queries)
    if cfg.sql_queries_dir is not None:
        n_copied = _copy_sql_corpus(cfg, sources_dir)
        _console.print(
            f"  [green]✓[/] corpus folder: {n_copied} .sql files copied"
        )
        sources_fired.append("sql_dir")

    # ── 5. Empty stub dirs for sources we don't have
    for sub in (
        "registries/raw", "ai_descriptions", "baseline_views",
        "bq_cache", "usage_history", "dq_rules", "lineage", "mdm_cache",
        "gold_queries",
    ):
        (sources_dir / sub).mkdir(parents=True, exist_ok=True)

    # Single-row catalog seed (only if no other source created one)
    catalog_path = sources_dir / "registries" / "raw" / "table_catalog.csv"
    if not catalog_path.exists():
        catalog_path.write_text(
            "table_name,IS IN DMP,company_domain,data_domain\n"
            f"{cfg.table_name},Yes,Finance,Cardmember\n",
            encoding="utf-8",
        )
        _console.print("  [green]✓[/] table_catalog: seeded single-row catalog")

    _console.print(
        f"[bold green]── {len(sources_fired)} source(s) fused at {sources_dir}: "
        f"{', '.join(sources_fired)} ──[/]\n"
    )
    return sources_dir


# ─── Per-source helpers ──────────────────────────────────────


def _load_lumi_source(cfg: Config, sources_dir: Path) -> None:
    assert cfg.lumi_output_path is not None
    result = load_lumi_for_table(
        cfg.table_name,
        lumi_path=cfg.lumi_output_path,
        out_dir=sources_dir,
    )
    _print_result("LUMI", result)
    if result.status == "error":
        raise RuntimeError(
            f"Lumi loader failed: {result.error}. "
            "Check LUMI_OUTPUT_PATH and TABLE_NAME in .env."
        )
    _console.print(
        f"  [dim]    {result.metadata.get('n_columns_mdm', 0)} MDM cols · "
        f"{result.metadata.get('n_queries', 0)} corpus queries · "
        f"LookML={result.metadata.get('has_lkml', False)}[/]"
    )


def _load_bq_source(cfg: Config, sources_dir: Path) -> None:
    """Run BQ loader. Handles the user's likely folder layout (BQ_EXTRACTION_DIR
    pointing directly at the per-table folder vs. its parent)."""
    assert cfg.bq_extraction_dir is not None
    # The synapse bq_loader expects source_dir to be the PARENT containing a
    # subfolder named <table_id>/. If the user pointed at the per-table folder
    # itself, we symlink it under a staging parent to satisfy that contract
    # without renaming anything on disk.
    if cfg.bq_extraction_dir.name == cfg.table_name:
        # Already correctly shaped — but we still need a parent that ONLY contains
        # this table to avoid name collisions
        staging = sources_dir / "__bq_input__"
        staging.mkdir(parents=True, exist_ok=True)
        target = staging / cfg.table_name
        if target.exists() or target.is_symlink():
            target.unlink()
        try:
            target.symlink_to(cfg.bq_extraction_dir.resolve())
        except OSError:
            shutil.copytree(cfg.bq_extraction_dir, target, dirs_exist_ok=True)
        bq_source_dir = staging
    else:
        bq_source_dir = cfg.bq_extraction_dir

    result = load_bq_for_table(
        cfg.table_name, source_dir=bq_source_dir, out_dir=sources_dir,
    )
    _print_result("BQ", result)


def _load_standalone_mdm(cfg: Config, sources_dir: Path) -> None:
    assert cfg.mdm_json_path is not None
    mdm_staging = sources_dir / "__mdm_input__"
    mdm_staging.mkdir(parents=True, exist_ok=True)
    staged_mdm = mdm_staging / f"{cfg.table_name}__mdm_raw.json"
    if staged_mdm.exists() or staged_mdm.is_symlink():
        staged_mdm.unlink()
    if cfg.mdm_json_path.exists():
        try:
            staged_mdm.symlink_to(cfg.mdm_json_path.resolve())
        except OSError:
            shutil.copy2(cfg.mdm_json_path, staged_mdm)
    result = load_mdm_for_table(
        cfg.table_name, source_dir=mdm_staging, out_dir=sources_dir,
    )
    _print_result("MDM", result)


def _copy_sql_corpus(cfg: Config, sources_dir: Path) -> int:
    """Copy raw .sql files from SQL_QUERIES_DIR into gold_queries/."""
    assert cfg.sql_queries_dir is not None
    gold_dir = sources_dir / "gold_queries"
    gold_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    if cfg.sql_queries_dir.exists():
        for sql_path in sorted(cfg.sql_queries_dir.rglob("*.sql")):
            target = gold_dir / sql_path.name
            if target.exists():
                continue  # don't overwrite — lumi-derived queries take precedence
            shutil.copy2(sql_path, target)
            n += 1
    return n


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
