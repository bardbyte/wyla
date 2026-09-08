"""Graph builder + persistence for the demo table."""

from __future__ import annotations

import sys
from pathlib import Path

from rich.console import Console

from semantic_graph.config import Config

# Reach into sibling synapse package
_SYNAPSE_ROOT = Path(__file__).resolve().parents[4] / "synapse"
if str(_SYNAPSE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SYNAPSE_ROOT))

from synapse.graph import build_graph_from_sources  # noqa: E402
from synapse.graph.store import GraphStore           # noqa: E402

_console = Console()


def build_and_save_graph(cfg: Config, sources_dir: Path) -> GraphStore:
    """Build the graph from the staged sources_dir and persist it."""
    _console.print(f"[bold cyan]── building graph from {sources_dir} ──[/]")
    store = build_graph_from_sources(sources_dir)
    stats = store.stats()
    _console.print(
        f"  [green]✓[/] {stats['n_nodes']} nodes, {stats['n_edges']} edges"
    )
    for nt, n in sorted(stats["nodes_by_type"].items(), key=lambda kv: -kv[1]):
        _console.print(f"      [dim]{nt}: {n}[/]")

    snapshot_path = cfg.graph_cache_dir / "graph_snapshot.json"
    snapshot_path.write_text(store.model_dump_json(indent=2), encoding="utf-8")
    _console.print(f"  [green]✓[/] snapshot saved → {snapshot_path}")
    return store


def load_cached_graph(cfg: Config) -> GraphStore:
    """Load the most recent persisted graph (used by the ADK agent at runtime)."""
    snapshot_path = cfg.graph_cache_dir / "graph_snapshot.json"
    if not snapshot_path.exists():
        raise RuntimeError(
            f"No cached graph at {snapshot_path}. Run "
            f"`python scripts/build_graph.py` first."
        )
    return GraphStore.model_validate_json(snapshot_path.read_text(encoding="utf-8"))
