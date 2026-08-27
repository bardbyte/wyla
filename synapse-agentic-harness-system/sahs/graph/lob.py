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
    # run-2 finding: DMP declares LOBs by DISPLAY NAME ("Global
    # Merchant & Network Svcs") while codes are short (GMNS) — without
    # declared equivalence the two slug to PARALLEL lob nodes and the
    # corroboration splits. Aliases are that human declaration: every
    # listed spelling resolves onto THIS code's node.
    aliases: list[str] = []


def lob_alias_map(rows: list[LobRow]) -> dict[str, str]:
    """slugged-alias lob id → canonical lob id, for every alias on
    every row. Emitters resolve declared/mined LOB values through this
    before minting or corroborating — equivalence is declared by the
    steward, never guessed from string similarity."""
    out: dict[str, str] = {}
    for row in rows:
        canonical = lob_id(row.lob_code)
        for alias in row.aliases:
            if alias.strip():
                out[lob_id(alias)] = canonical
    return out


class OrgRow(BaseModel):
    """Sub-LOB / org unit (graph/identity/org_map.jsonl) — WHO QUERIES,
    as distinct from who owns. The mined ``business_unit`` names the
    org that runs the queries (CFR's 1,062 patterns sit 96% on GMNS
    tables) — so org units feed ``used_by`` edges, never ``in_lob``
    ownership. Each org has a parent LOB (must be a lob_map code)."""

    org_code: str
    org_name: str = ""
    parent_lob: str                     # MUST be a lob_map code
    aliases: list[str] = []
    verified_by: str
    verified_on: str
    notes: str = ""


def load_org_map(path: Path, lob_rows: list[LobRow]) -> list[OrgRow]:
    lob_codes = {r.lob_code for r in lob_rows}
    rows: list[OrgRow] = []
    for line in Path(path).read_text(encoding="utf-8").split("\n"):
        if not line.strip():
            continue
        row = OrgRow.model_validate(json.loads(line))
        if row.parent_lob not in lob_codes:
            raise ValueError(
                f"org_map.jsonl: {row.org_code!r} -> parent_lob "
                f"{row.parent_lob!r} is not a lob_map code "
                f"({sorted(lob_codes)}) — fix the parent or add the "
                "LOB first")
        rows.append(row)
    return rows


def emit_org_map(rows: list[OrgRow], graph: GraphDir,
                 run_id: str) -> dict:
    """Org-unit nodes (lob kind, ``kind: org_unit``) + a steward
    ``in_lob`` edge to the parent LOB. Usage edges (``used_by``) come
    from the mined witness at emit time, resolved through
    ``usage_target_map``."""
    report = {"org_units": 0}
    seen: set[str] = set()
    for row in rows:
        oid = lob_id(row.org_code)
        if oid in seen:
            continue
        seen.add(oid)
        graph.append_node(NodeRecord(
            id=oid,
            props={"code": row.org_code, "name": row.org_name,
                   "kind": "org_unit", "parent": row.parent_lob},
            prov=Prov(source="lob_map", run=run_id,
                      actor=row.verified_by,
                      evidence="identity/org_map.jsonl")))
        graph.append_edge(Quad(
            s=oid, r="in_lob", o=lob_id(row.parent_lob),
            prov=Prov(source="lob_map", run=run_id,
                      actor=row.verified_by,
                      evidence="identity/org_map.jsonl")))
        report["org_units"] += 1
    return report


def usage_target_map(lob_rows: list[LobRow],
                     org_rows: list["OrgRow"]) -> dict[str, str]:
    """slug → node id for every spelling a mined ``business_unit`` may
    legally resolve to: LOB codes + aliases (usage BY the LOB's own
    org) and org codes + aliases. Org entries win on collision (more
    specific). Anything outside this map is counted, never guessed."""
    out: dict[str, str] = {}
    for row in lob_rows:
        canonical = lob_id(row.lob_code)
        out[canonical] = canonical
        for alias in row.aliases:
            if alias.strip():
                out[lob_id(alias)] = canonical
    for row in org_rows:
        oid = lob_id(row.org_code)
        out[oid] = oid
        for alias in row.aliases:
            if alias.strip():
                out[lob_id(alias)] = oid
    return out


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
