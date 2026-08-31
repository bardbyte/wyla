"""The cosmos projection — ``indexes/graph_map.json`` (E17).

The graph explorer renders THIS file; it never lays out at runtime.
Positions are computed here, seeded from stable identifiers (sha256 —
no RNG), so the same build always renders the same sky and a diff in
the map is a diff in the data.

Shape (consumed by <synapse-cosmos>):
  nodes: [{id, label, kind: table|metric, tier, usage, well, star,
           pos: [x,y,z], columns?, metrics_here?, status?}]
  edges: [{a, b, kind: joins|computed-from, source?, scope?, on?}]
  wells: [{id, label, sub, center: [x,y,z]}]
  meta:  {layout, truncated, encoding}

Scope honesty carries through: a scoped_only join stays labeled — the
sky shows the relationship exists, never that raw tables join safely.
Mined metrics (~3k unnamed) are counted in meta.truncated, not drawn:
41 named stars are a legible sky; 3,000 anonymous ones are noise.
"""

from __future__ import annotations

import hashlib
import math
from typing import Any

from sahs.compiler.display import tier_of_metric, tier_of_table

_WELL_RING = 16.0          # well centers sit on this ring (xz plane)
_WELL_RADIUS = (4.0, 8.5)  # table shells around a well center
_METRIC_ORBIT = (1.2, 2.2)  # metrics orbit their table


def _floats(seed: str, n: int) -> list[float]:
    """n deterministic floats in [0,1) from a stable seed."""
    out: list[float] = []
    counter = 0
    while len(out) < n:
        digest = hashlib.sha256(
            f"{seed}#{counter}".encode("utf-8")).digest()
        for i in range(0, 32, 4):
            out.append(int.from_bytes(digest[i:i + 4], "big")
                       / 2 ** 32)
            if len(out) >= n:
                break
        counter += 1
    return out


def _sphere(seed: str, center: tuple[float, float, float],
            rmin: float, rmax: float) -> list[float]:
    u, v, w = _floats(seed, 3)
    radius = rmin + u * (rmax - rmin)
    theta = v * 2 * math.pi
    phi = math.acos(2 * w - 1)
    return [round(center[0] + radius * math.sin(phi) * math.cos(theta), 3),
            round(center[1] + radius * math.cos(phi) * 0.7, 3),
            round(center[2] + radius * math.sin(phi) * math.sin(theta), 3)]


def build_graph_map(consensus: dict[str, Any],
                    nodes: dict[str, Any],
                    metric_rows: list[dict[str, Any]],
                    join_rows: list[dict[str, Any]],
                    lob_rows: list[dict[str, Any]]) -> dict[str, Any]:
    physicals = sorted(c.physical for c in consensus.values())
    metrics_by_table: dict[str, list[dict[str, Any]]] = {}
    for row in metric_rows:
        if row.get("table"):
            metrics_by_table.setdefault(row["table"], []).append(row)

    # wells: one per steward-mapped LOB, on a ring; unmapped tables
    # get their own honest well rather than a fake home
    lobs = sorted(lob_rows, key=lambda r: str(r.get("code")
                                              or r.get("lob") or ""))
    table_wells: dict[str, list[str]] = {}
    for row in lobs:
        code = str(row.get("code") or row.get("lob") or "?")
        for physical in row.get("tables", []):
            table_wells.setdefault(physical, []).append(code)
    well_ids = [str(r.get("code") or r.get("lob") or "?")
                for r in lobs]
    if any(p not in table_wells for p in physicals):
        well_ids.append("UNMAPPED")
    wells = []
    centers: dict[str, tuple[float, float, float]] = {}
    for i, well_id in enumerate(well_ids):
        angle = (2 * math.pi * i) / max(len(well_ids), 1)
        lift = (_floats(f"well:{well_id}", 1)[0] - 0.5) * 6
        center = (round(_WELL_RING * math.cos(angle), 3),
                  round(lift, 3),
                  round(_WELL_RING * math.sin(angle), 3))
        centers[well_id] = center
        members = ([p for p, w in table_wells.items()
                    if w[0] == well_id]
                   if well_id != "UNMAPPED" else
                   [p for p in physicals if p not in table_wells])
        witnessed = sum(1 for p in members if metrics_by_table.get(p))
        pct = round(100 * witnessed / len(members)) if members else 0
        row = next((r for r in lobs
                    if str(r.get("code") or r.get("lob")) == well_id),
                   {})
        label = well_id if not row.get("name") \
            else f"{well_id} — {row['name']}"
        wells.append({"id": well_id, "label": label,
                      "sub": f"{len(members)} tables · "
                             f"{pct}% witnessed",
                      "center": list(center)})

    map_nodes: list[dict[str, Any]] = []
    table_pos: dict[str, list[float]] = {}
    for physical in physicals:
        my_wells = table_wells.get(physical, ["UNMAPPED"])
        well = my_wells[0]
        record = nodes.get(f"table:{physical}")
        props = record.props if record is not None else {}
        here = metrics_by_table.get(physical, [])
        usage = len(here) + sum(int(r.get("support") or 0)
                                for r in here)
        pos = _sphere(f"table:{physical}", centers[well],
                      *_WELL_RADIUS)
        table_pos[physical] = pos
        map_nodes.append({
            "id": f"table:{physical}",
            "label": physical.split(".")[-1], "kind": "table",
            "tier": tier_of_table(
                str(props.get("purpose") or ""), len(here),
                physical in table_wells),
            "usage": usage, "well": well,
            "star": len(set(my_wells)) >= 2,
            "pos": pos,
            "columns": len(getattr(consensus.get(
                f"table:{physical}", None), "columns", {}) or {})
            or None,
            "metrics_here": len(here)})

    shown = 0
    for row in sorted(metric_rows, key=lambda r: r["id"]):
        if row.get("status") not in ("certified", "pending"):
            continue
        table = row.get("table") or ""
        anchor = table_pos.get(table)
        well = (table_wells.get(table, ["UNMAPPED"]) or ["UNMAPPED"])[0]
        pos = (_sphere(f"metric:{row['fp']}", tuple(anchor),
                       *_METRIC_ORBIT) if anchor else
               _sphere(f"metric:{row['fp']}", centers[well], 2.0, 5.0))
        map_nodes.append({
            "id": row["id"], "label": row.get("label") or row["fp"][:8],
            "kind": "metric", "tier": tier_of_metric(row),
            "usage": int(row.get("support") or 1), "well": well,
            "star": False, "pos": pos,
            "status": row.get("status_served") or row.get("status")})
        shown += 1

    map_edges: list[dict[str, Any]] = []
    for row in join_rows:
        if row.get("source") == "co_query":
            continue           # digests say "together", not HOW — noise
        if row["a"] not in table_pos or row["b"] not in table_pos:
            continue           # an end outside the map cannot render
        edge = {"a": f"table:{row['a']}", "b": f"table:{row['b']}",
                "kind": "joins", "source": row.get("source", "")}
        for key in ("scope", "on"):
            if row.get(key):
                edge[key] = row[key]
        map_edges.append(edge)
    for node in map_nodes:
        if node["kind"] == "metric":
            row = next(r for r in metric_rows if r["id"] == node["id"])
            if row.get("table"):
                map_edges.append({"a": node["id"],
                                  "b": f"table:{row['table']}",
                                  "kind": "computed-from"})

    return {
        "schema": "meridian.graph_map/1",
        "nodes": map_nodes, "edges": map_edges, "wells": wells,
        "meta": {
            "layout": "seeded-v1 (sha256 positions — same build, "
                      "same sky)",
            "truncated": {
                "mined_metrics": sum(
                    1 for r in metric_rows
                    if r.get("status") not in ("certified", "pending"))},
            "encoding": "size = usage · glow = trust tier · gold "
                        "star = held by multiple domains · "
                        "scoped_only joins are CTE-scoped, not "
                        "raw-safe",
        },
    }
