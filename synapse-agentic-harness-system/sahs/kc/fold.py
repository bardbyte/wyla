"""The graph as this module reads it: one fold per graph state, indexed
so a table's slice costs what the table has, not what the graph has.

Two caches, both keyed on what they read:

* ``fold(graph_root)`` — nodes, per-witness edges, per-prop provenance,
  and the indexes every extractor needs (edges by subject and relation,
  by object and relation, by subject alone; nodes by kind). Keyed on the
  store files' names, sizes and mtimes, so an unchanged graph is never
  re-read and a changed one is never served stale.
* ``open_build(builds_root)`` — the promoted ``Build``, keyed on the
  CURRENT pointer and the build's manifest mtime. ``Build.open`` parses
  every index from disk; a page that calls it three times pays three
  times for the same bytes.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sahs.graph.quads import GraphDir, NodeRecord, Quad
from sahs.tools.api import Build


def graph_stamp(graph_root: Path) -> tuple:
    stamp = []
    for sub in ("nodes", "edges"):
        folder = Path(graph_root) / sub
        if folder.exists():
            for path in sorted(folder.glob("*.jsonl")):
                st = path.stat()
                stamp.append((path.name, st.st_mtime_ns, st.st_size))
    return tuple(stamp)


@dataclass
class Fold:
    nodes: dict[str, NodeRecord]
    edges: dict[tuple, Quad]
    prop_prov: dict[str, dict[str, Any]]
    by_s_r: dict[tuple[str, str], list[Quad]] = field(default_factory=dict)
    by_o_r: dict[tuple[str, str], list[Quad]] = field(default_factory=dict)
    by_s: dict[str, list[Quad]] = field(default_factory=dict)
    by_kind: dict[str, list[NodeRecord]] = field(default_factory=dict)

    def out(self, subject: str, relation: str) -> list[Quad]:
        return self.by_s_r.get((subject, relation), [])

    def into(self, obj: str, relation: str) -> list[Quad]:
        return self.by_o_r.get((obj, relation), [])

    def of_kind(self, kind: str) -> list[NodeRecord]:
        return self.by_kind.get(kind, [])


_FOLD: dict[tuple, Fold] = {}


def fold(graph_root: Path) -> Fold:
    key = (str(graph_root), graph_stamp(graph_root))
    held = _FOLD.get(key)
    if held is not None:
        return held
    _FOLD.clear()
    graph = GraphDir(graph_root)
    prop_prov: dict[str, dict[str, Any]] = defaultdict(dict)
    # the fold keeps the LAST writer's prov for a node; a table node is
    # written by several loaders, so each prop remembers its own writer
    for record in graph.iter_nodes():
        if record.prov.status == "retracted":
            prop_prov.pop(record.id, None)
            continue
        for prop in record.props:
            prop_prov[record.id][prop] = record.prov
    nodes = graph.fold_nodes()
    edges = graph.fold_edges()
    by_s_r: dict[tuple[str, str], list[Quad]] = defaultdict(list)
    by_o_r: dict[tuple[str, str], list[Quad]] = defaultdict(list)
    by_s: dict[str, list[Quad]] = defaultdict(list)
    for (s, r, o, _w), quad in edges.items():
        if quad.prov.status != "active":
            continue
        by_s_r[(s, r)].append(quad)
        by_o_r[(o, r)].append(quad)
        by_s[s].append(quad)
    by_kind: dict[str, list[NodeRecord]] = defaultdict(list)
    for node_id, record in nodes.items():
        by_kind[node_id.split(":", 1)[0]].append(record)
    view = Fold(nodes=nodes, edges=edges, prop_prov=dict(prop_prov),
                by_s_r=dict(by_s_r), by_o_r=dict(by_o_r), by_s=dict(by_s),
                by_kind=dict(by_kind))
    _FOLD[key] = view
    return view


_BUILDS: dict[tuple, Build] = {}


def build_stamp(builds_root: Path) -> tuple:
    root = Path(builds_root)
    current = root / "CURRENT"
    if current.exists():
        target = current.read_text(encoding="utf-8").strip()
        manifest = root / target / "manifest.json"
    else:
        target = ""
        manifest = root / "manifest.json"
    return (str(root), target,
            manifest.stat().st_mtime_ns if manifest.exists() else 0)


def open_build(builds_root: Path) -> Build:
    """``Build.open`` once per promoted build; a re-promotion (CURRENT
    moves) or a recompiled manifest opens it again."""
    key = build_stamp(builds_root)
    held = _BUILDS.get(key)
    if held is not None:
        return held
    build = Build.open(Path(builds_root))
    _BUILDS.clear()
    _BUILDS[key] = build
    return build
