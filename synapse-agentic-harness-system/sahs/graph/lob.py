"""LOB map — steward-declared line-of-business membership (E1 sidecar).

``graph/identity/lob_map.jsonl`` sits beside the crosswalk and carries
one row per (line of business, table):

    {"lob_code": "gmns",
     "lob_name": "Global Merchant & Network Services",
     "physical": "dw.gms_transaction",
     "verified_by": "...", "verified_on": "YYYY-MM-DD", "notes": ""}

Strict like the alias sidecar: a row whose ``physical`` is not a
crosswalk row refuses to load — classification never mints identity.
Multi-membership is legal and deliberate (a table serving two LOBs gets
two rows; the graph holds one ``in_lob`` edge per witness family each).
The dmp catalog and mined measures corroborate these edges with their
own witnesses at emit time (quads_emit); this module only carries the
HUMAN declaration.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

from sahs.graph.crosswalk import Crosswalk
from sahs.graph.ids import lob_id, table_id
from sahs.graph.quads import GraphDir, NodeRecord, Prov, Quad

SOURCE = "lob_map"                      # → witness "steward"


class LobRow(BaseModel):
    lob_code: str                       # short code — the identity slug
    lob_name: str = ""                  # display name, verbatim
    physical: str                       # dataset.table — MUST crosswalk
    verified_by: str
    verified_on: str
    notes: str = ""


def load_lob_map(path: Path, crosswalk: Crosswalk) -> list[LobRow]:
    rows: list[LobRow] = []
    for line in Path(path).read_text(encoding="utf-8").split("\n"):
        if not line.strip():
            continue
        row = LobRow.model_validate(json.loads(line))
        physical = row.physical.strip().lower()
        if physical not in crosswalk.by_physical:
            raise ValueError(
                f"lob_map.jsonl: {row.lob_code!r} -> {row.physical!r} is "
                "not a crosswalk row — fix the physical name or add the "
                "table to the crosswalk first")
        rows.append(row.model_copy(update={"physical": physical}))
    return rows


def emit_lob_map(rows: list[LobRow], graph: GraphDir,
                 run_id: str) -> dict:
    """lob nodes + steward ``in_lob`` edges. Duplicate (table, lob)
    rows collapse (validator [8] stays meaningful); repeated lob codes
    keep the FIRST row's display name."""
    report = {"lobs": 0, "memberships": 0, "duplicate_rows": 0}
    seen_lobs: set[str] = set()
    seen_edges: set[tuple[str, str]] = set()
    for row in rows:
        lid = lob_id(row.lob_code)
        if lid not in seen_lobs:
            seen_lobs.add(lid)
            graph.append_node(NodeRecord(
                id=lid,
                props={"code": row.lob_code, "name": row.lob_name},
                prov=Prov(source=SOURCE, run=run_id,
                          evidence="identity/lob_map.jsonl")))
            report["lobs"] += 1
        tid = table_id(row.physical)
        if (tid, lid) in seen_edges:
            report["duplicate_rows"] += 1
            continue
        seen_edges.add((tid, lid))
        # the steward attesting membership attests the table exists —
        # mint the endpoint (fold merges with archive detail)
        graph.append_node(NodeRecord(
            id=tid, props={},
            prov=Prov(source=SOURCE, run=run_id,
                      evidence="identity/lob_map.jsonl")))
        graph.append_edge(Quad(
            s=tid, r="in_lob", o=lid,
            props={"note": row.notes} if row.notes else {},
            prov=Prov(source=SOURCE, run=run_id,
                      actor=row.verified_by,
                      evidence="identity/lob_map.jsonl")))
        report["memberships"] += 1
    return report
