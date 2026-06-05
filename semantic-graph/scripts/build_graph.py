"""End-to-end: load sources → build graph → run LLM enrichment → save.

Run from the semantic-graph/ folder:

    python scripts/build_graph.py

Reads .env. Writes:
    data/cache/sources/...        — canonical source artifacts
    data/cache/graph_snapshot.json — the full enriched graph
    data/cache/enrichment_memory.json
    data/cache/entity_proposals.json
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make src/ importable without `pip install -e .`
_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from rich.console import Console
from rich.panel import Panel

from semantic_graph.config import load_config
from semantic_graph.loaders import load_all_sources
from semantic_graph.graph import build_and_save_graph
from semantic_graph.enrichment import enrich_table

_console = Console()


def main() -> int:
    cfg = load_config()
    _console.print(Panel.fit(
        f"[bold]Semantic Graph build[/]\n"
        f"  table     : [cyan]{cfg.fqn}[/]\n"
        f"  model     : [cyan]{cfg.gemini_model}[/]\n"
        f"  vertex prj: [cyan]{cfg.google_project}[/]\n"
        f"  dry-run   : [cyan]{cfg.enrichment_dry_run}[/]\n"
        f"  cache dir : [cyan]{cfg.graph_cache_dir.resolve()}[/]"
    ))

    sources_dir = load_all_sources(cfg)
    store = build_and_save_graph(cfg, sources_dir)
    result = enrich_table(cfg, store)

    _console.print(Panel.fit(
        f"[bold green]Build complete[/]\n"
        f"  graph      : {(cfg.graph_cache_dir / 'graph_snapshot.json').resolve()}\n"
        f"  memory     : {cfg.enrichment_memory_path.resolve()}\n"
        f"  proposals  : {cfg.entity_proposals_path.resolve()}\n"
        f"  entities   : {len(result['proposals'])}\n"
        f"  next       : [cyan]adk web apps/[/]  →  open http://localhost:8000"
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
