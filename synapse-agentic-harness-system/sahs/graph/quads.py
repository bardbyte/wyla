"""The quad store — append-only JSONL in git, fold for current state.

Layout (pinned):

    graph/
      nodes/<kind>.jsonl        {"id", "props", "prov"}
      edges/<relation>.jsonl    {"s", "r", "o", "props"?, "prov"}
      runs/<run_id>/manifest.json
      identity/crosswalk.jsonl  (E1 — owned by the compiler side)

Discipline replaces the database: single writer, append-only (nobody
edits history), current state = a fold where the LAST line wins per
identity — (id) for nodes, (s, r, o, witness) for edges (E12/A1: one
quad per witness family; independent testimony never collapses at the
store). The relation registry types every edge's endpoints; the
validator (validate.py) is the foreign-key story.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel, Field, model_validator

from sahs.graph.ids import kind_of

QUAD_STATUSES = ("active", "superseded", "retracted")

# ── witness families (E12/A1, pinned) ──────────────────────────
# A witness is WHO SAW IT — the independent evidence family behind an
# assertion. One quad per (s, r, o, witness); per-witness support
# arrays exist only as compiler output (fold), never mutated in place.
WITNESSES = (
    "catalog_mined",    # upstream measures_catalog mining
    "jobs_30d",         # in-silo mining of raw 30-day job history
    "audit_30d",        # audit-log corroboration (never a feature source)
    "dmp",
    "gmns",
    "skill_contract",
    "snippet",          # blue insights fragments
    "atlas",            # governed vocabulary (atlas/business_terms/std_tech/glossary)
    "lumi",
    "bq",
    "steward",          # clerk-written human decisions
    "user_variant",     # Alice/Bob on-the-fly variants
    "llm_enriched",     # enricher output (Part B)
    "gold_attested",    # the 158 gold pairs — the answer key
)

# Gold contamination guard (pinned): gold_attested is a full graph
# citizen — census, cards, steward evidence — but the gold pairs ARE the
# eval answer key, so they must never feed a feature the resolver ranks
# on (support_effective, witness_agreement, recency). The SUT must not
# contain its own test set. audit_30d likewise corroborates but never
# votes (two witnesses of the same events don't count twice).
RANKING_WITNESSES = tuple(w for w in WITNESSES
                          if w not in ("gold_attested", "audit_30d"))

# prov.source → default witness family; writers may set witness
# explicitly (jobs_30d/audit_30d/user_variant/llm_enriched always do).
SOURCE_WITNESS = {
    "measures_catalog": "catalog_mined",
    "blue_insights": "snippet",
    "metrics_dmp": "dmp",
    "extended_gmns": "gmns",
    "skill_contract": "skill_contract",
    "gold_queries": "gold_attested",
    "bq": "bq",
    "lumi": "lumi",
    "atlas": "atlas",
    "business_terms": "atlas",
    "std_tech_metadata": "atlas",
    "glossary": "atlas",
    "clerk": "steward",
    "jobs_30d": "jobs_30d",
}

# ReviewItem lattice (E12/A5, pinned) — schemas land BEFORE the first
# real build so day-one quads never remint; the queue tooling is Part B.
REVIEW_KINDS = ("naming", "metric_conflict", "structural_d1", "structural_d2",
                "structural_d3", "structural_d4", "structural_d5", "variant",
                "witness_divergence", "enrichment_correction")
REVIEW_STATUSES = ("open", "decided", "spawned_task")

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
    # E12/A5: ReviewItem → the node it is about (any kind incl. review)
    "concerns":        ({"review"}, ID_KINDS | {"review", "run"}),
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
    witness: str = ""              # ∈ WITNESSES (E12/A1); derived from
                                   # source when a writer doesn't set it

    @model_validator(mode="after")
    def _default_witness(self) -> "Prov":
        if not self.witness:
            self.witness = SOURCE_WITNESS.get(self.source, "")
        return self


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
    """The single-writer handle. Appends only; never rewrites.

    Crash hygiene: a run killed mid-append can leave a torn final line
    (no trailing newline). Before the first append to a file this
    process, the tail is checked — torn bytes move to a ``.torn``
    sidecar (evidence, never deleted) and the file truncates back to
    its last complete line, so the next append never glues onto a
    fragment. Corruption anywhere ELSE is refused loudly by the readers
    with file+line named — the store is derivable from the archives, so
    the honest recovery is rebuild, never silent skipping."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        (self.root / "nodes").mkdir(parents=True, exist_ok=True)
        (self.root / "edges").mkdir(parents=True, exist_ok=True)
        (self.root / "runs").mkdir(parents=True, exist_ok=True)
        self._tail_checked: set[Path] = set()

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

    def _append(self, path: Path, payload: dict) -> None:
        if path not in self._tail_checked:
            self._repair_torn_tail(path)
            self._tail_checked.add(path)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False,
                               sort_keys=True) + "\n")

    @staticmethod
    def _repair_torn_tail(path: Path) -> None:
        if not path.exists() or path.stat().st_size == 0:
            return
        data = path.read_bytes()
        if data.endswith(b"\n"):
            return
        cut = data.rfind(b"\n") + 1          # 0 when no newline at all
        torn = data[cut:]
        with path.with_suffix(path.suffix + ".torn").open("ab") as f:
            f.write(torn + b"\n")
        with path.open("wb") as f:
            f.write(data[:cut])
        print(f"graph store: repaired torn tail in {path.name} "
              f"({len(torn)} bytes -> {path.name}.torn)", file=sys.stderr)

    # ── reads ──

    @staticmethod
    def _lines(path: Path):
        for n, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"graph store corrupt at {path.parent.name}/"
                    f"{path.name}:{n} ({e.msg}) — the append-only store "
                    "is derivable: remove graph/nodes and graph/edges "
                    "(KEEP graph/identity and graph/runs) and re-run "
                    "build-graph") from e

    def iter_nodes(self) -> Iterator[NodeRecord]:
        for path in sorted((self.root / "nodes").glob("*.jsonl")):
            for payload in self._lines(path):
                yield NodeRecord.model_validate(payload)

    def iter_edges(self, relation: str | None = None) -> Iterator[Quad]:
        paths = (sorted((self.root / "edges").glob("*.jsonl"))
                 if relation is None
                 else [self.root / "edges" / f"{relation}.jsonl"])
        for path in paths:
            if not path.exists():
                continue
            for payload in self._lines(path):
                yield Quad.model_validate(payload)

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

    def fold_edges(self) -> dict[tuple[str, str, str, str], Quad]:
        """Current state per (s, r, o, WITNESS) — E12/A1: each witness
        family testifies independently, and a retraction by one witness
        never erases another's testimony. Aggregation across witnesses
        (support arrays, max-combiner) is compiler output, never fold
        state."""
        state: dict[tuple[str, str, str, str], Quad] = {}
        for quad in self.iter_edges():
            key = (quad.s, quad.r, quad.o, quad.prov.witness)
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
