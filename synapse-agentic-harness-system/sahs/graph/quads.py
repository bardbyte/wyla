"""The quad store — append-only JSONL in git, fold for current state.

Layout (pinned):

    graph/
      nodes/<kind>.jsonl        {"id", "props", "prov"}
      edges/<relation>.jsonl    {"s", "r", "o", "props"?, "prov"}
      runs/<run_id>/manifest.json
      identity/crosswalk.jsonl  (E1 — owned by the compiler side)

Discipline replaces the database: single writer, append-only (nobody
edits history), current state = a fold where the LAST line wins per
identity — (id) for nodes, (s, r, o) for edges. The relation registry
types every edge's endpoints; the validator (validate.py) is the
foreign-key story.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel, Field

from sahs.graph.ids import kind_of

QUAD_STATUSES = ("active", "superseded", "retracted")

# relation → (allowed subject kinds, allowed object kinds)
RELATIONS: dict[str, tuple[set[str], set[str]]] = {
    "has_column":      ({"table"}, {"col"}),
    "has_schema":      ({"table"}, {"schema"}),
    "bound_to":        ({"concept"}, {"pred"}),
    "defines_metric":  ({"mgroup"}, {"metric"}),
    "measured_on":     ({"metric"}, {"table"}),
    "variant_of":      ({"metric"}, {"metric"}),
    "mapped_term":     ({"col", "table"}, {"term"}),
    "alias_of":        ({"acr"}, {"term", "concept", "mgroup"}),
    "joins_via":       ({"table"}, {"table"}),
    "co_queried_with": ({"table"}, {"table"}),
    "derived_from":    ({"col"}, {"col"}),
    "upstream_of":     ({"table"}, {"table"}),
    "owned_by":        ({"table"}, {"owner"}),
    "certified_as":    ({"metric", "pred", "mgroup"}, {"status"}),
    "has_policy":      ({"table", "col"}, {"policy"}),
    "has_domain":      ({"col"}, {"domain"}),
    "evidenced_by":    (set(ID_KINDS := {
        "table", "col", "pred", "tmpl", "metric", "mgroup", "concept",
        "term", "acr", "skill", "schema", "domain", "doc"}), {"doc", "run"}),
    "valid_in":        ({"pred", "metric", "col"}, {"schema"}),
    "member_of":       ({"metric"}, {"mgroup"}),
    "described_by":    ({"table", "col"}, {"doc"}),
}


class Prov(BaseModel):
    source: str
    run: str
    retrieved: str = ""
    valid_for: list[str] = Field(default_factory=lambda: ["unversioned"])
    status: str = "active"
    support: int | None = None
    evidence: str = ""
    actor: str | None = None       # REQUIRED when source == "clerk" (E7)


class NodeRecord(BaseModel):
    id: str
    props: dict[str, Any] = Field(default_factory=dict)
    prov: Prov


class Quad(BaseModel):
    s: str
    r: str
    o: str
    props: dict[str, Any] = Field(default_factory=dict)
    prov: Prov


class GraphDir:
    """The single-writer handle. Appends only; never rewrites."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        (self.root / "nodes").mkdir(parents=True, exist_ok=True)
        (self.root / "edges").mkdir(parents=True, exist_ok=True)
        (self.root / "runs").mkdir(parents=True, exist_ok=True)

    # ── writes ──

    def append_node(self, record: NodeRecord) -> None:
        kind = kind_of(record.id)
        if kind is None:
            raise ValueError(f"node id fails grammar: {record.id!r}")
        self._append(self.root / "nodes" / f"{kind}.jsonl",
                     record.model_dump(exclude_none=True))

    def append_edge(self, quad: Quad) -> None:
        if quad.r not in RELATIONS:
            raise ValueError(f"unregistered relation: {quad.r!r}")
        self._append(self.root / "edges" / f"{quad.r}.jsonl",
                     quad.model_dump(exclude_none=True))

    @staticmethod
    def _append(path: Path, payload: dict) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False,
                               sort_keys=True) + "\n")

    # ── reads ──

    def iter_nodes(self) -> Iterator[NodeRecord]:
        for path in sorted((self.root / "nodes").glob("*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    yield NodeRecord.model_validate(json.loads(line))

    def iter_edges(self, relation: str | None = None) -> Iterator[Quad]:
        paths = (sorted((self.root / "edges").glob("*.jsonl"))
                 if relation is None
                 else [self.root / "edges" / f"{relation}.jsonl"])
        for path in paths:
            if not path.exists():
                continue
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    yield Quad.model_validate(json.loads(line))

    # ── fold: current state ──

    def fold_nodes(self) -> dict[str, NodeRecord]:
        state: dict[str, NodeRecord] = {}
        for record in self.iter_nodes():
            if record.prov.status == "retracted":
                state.pop(record.id, None)
            else:
                previous = state.get(record.id)
                if previous is not None:
                    merged = dict(previous.props)
                    merged.update(record.props)
                    record = record.model_copy(update={"props": merged})
                state[record.id] = record
        return state

    def fold_edges(self) -> dict[tuple[str, str, str], Quad]:
        state: dict[tuple[str, str, str], Quad] = {}
        for quad in self.iter_edges():
            key = (quad.s, quad.r, quad.o)
            if quad.prov.status == "retracted":
                state.pop(key, None)
            else:
                state[key] = quad
        return state

    def governance_history(self) -> dict[str, list[str]]:
        """Per subject, the ordered certified_as state sequence (E7)."""
        history: dict[str, list[str]] = defaultdict(list)
        for quad in self.iter_edges("certified_as"):
            history[quad.s].append(quad.o.split(":", 1)[1])
        return dict(history)
