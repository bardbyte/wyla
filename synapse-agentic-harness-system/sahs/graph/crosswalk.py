"""E1 — the identity crosswalk: Atlas ↔ Lumi ↔ BQ, human-verified once.

46 rows, one per table:
    {"physical": "<dataset>.<table>", "lumi_asset_id": "...",
     "atlas_entity_id": "...", "verified_by": "...",
     "verified_on": "YYYY-MM-DD", "notes": ""}

Every archive-derived quad's table subject MUST resolve through this file
— an unresolvable source record BLOCKS the build (never quarantines):
identity confusion is the one error class that corrupts everything
downstream, so it fails loudly at the door.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel


class CrosswalkRow(BaseModel):
    physical: str                  # dataset.table (project in prov)
    lumi_asset_id: str = ""
    atlas_entity_id: str = ""
    verified_by: str
    verified_on: str
    notes: str = ""


class Crosswalk:
    def __init__(self, rows: list[CrosswalkRow]) -> None:
        self.rows = rows
        self.by_physical = {r.physical.lower(): r for r in rows}
        self.by_lumi = {r.lumi_asset_id: r for r in rows if r.lumi_asset_id}
        self.by_atlas = {r.atlas_entity_id.lower(): r
                         for r in rows if r.atlas_entity_id}
        self.by_short = {}
        for r in rows:
            short = r.physical.split(".")[-1].lower()
            self.by_short.setdefault(short, []).append(r)

    @classmethod
    def load(cls, path: Path) -> "Crosswalk":
        rows = [CrosswalkRow.model_validate(json.loads(line))
                for line in Path(path).read_text(
                    encoding="utf-8").splitlines() if line.strip()]
        return cls(rows)

    def physical_for_bq(self, dataset: str, table: str) -> str | None:
        hit = self.by_physical.get(f"{dataset}.{table}".lower())
        return hit.physical.lower() if hit else None

    def physical_for_lumi(self, table_name: str,
                          asset_id: str = "") -> str | None:
        if asset_id and asset_id in self.by_lumi:
            return self.by_lumi[asset_id].physical.lower()
        hits = self.by_short.get(table_name.lower(), [])
        return hits[0].physical.lower() if len(hits) == 1 else None

    def physical_for_atlas(self, entity: str) -> str | None:
        hit = self.by_atlas.get(entity.lower())
        if hit:
            return hit.physical.lower()
        hits = self.by_short.get(entity.lower(), [])
        return hits[0].physical.lower() if len(hits) == 1 else None
