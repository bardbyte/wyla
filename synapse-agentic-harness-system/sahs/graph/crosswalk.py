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
    def __init__(self, rows: list[CrosswalkRow],
                 aliases: dict[str, str] | None = None) -> None:
        self.rows = rows
        self.by_physical = {r.physical.lower(): r for r in rows}
        self.by_lumi = {r.lumi_asset_id: r for r in rows if r.lumi_asset_id}
        self.by_atlas = {r.atlas_entity_id.lower(): r
                         for r in rows if r.atlas_entity_id}
        self.by_short = {}
        for r in rows:
            short = r.physical.split(".")[-1].lower()
            self.by_short.setdefault(short, []).append(r)
        self.aliases: dict[str, str] = {}
        for alias, physical in (aliases or {}).items():
            physical = physical.strip().lower()
            if physical not in self.by_physical:
                # an alias RESOLVES an identity, it never mints one —
                # a typo here corrupts attribution, so it dies loudly
                raise ValueError(
                    f"aliases.jsonl: {alias!r} -> {physical!r} is not a "
                    "crosswalk row — fix the alias or add the table")
            self.aliases[alias.strip().lower()] = physical

    @classmethod
    def load(cls, path: Path) -> "Crosswalk":
        path = Path(path)
        rows = [CrosswalkRow.model_validate(json.loads(line))
                for line in path.read_text(
                    encoding="utf-8").split("\n") if line.strip()]
        aliases: dict[str, str] = {}
        sidecar = path.parent / "aliases.jsonl"
        if sidecar.exists():
            for line in sidecar.read_text(
                    encoding="utf-8").split("\n"):
                if line.strip():
                    row = json.loads(line)
                    aliases[str(row["alias"])] = str(row["physical"])
        return cls(rows, aliases)

    def physical_for_bq(self, dataset: str, table: str) -> str | None:
        hit = self.by_physical.get(f"{dataset}.{table}".lower())
        return hit.physical.lower() if hit else None

    def physical_for_lumi(self, table_name: str,
                          asset_id: str = "") -> str | None:
        if asset_id and asset_id in self.by_lumi:
            return self.by_lumi[asset_id].physical.lower()
        hits = self.by_short.get(table_name.lower(), [])
        return hits[0].physical.lower() if len(hits) == 1 else None

    def physical_for_alias(self, name: str) -> str | None:
        """Human-verified alternative names — data-product display
        names, skill-pack table nicknames — mapping onto crosswalked
        identities (graph/identity/aliases.jsonl)."""
        return self.aliases.get((name or "").strip().lower())

    def physical_for_short(self, table_name: str) -> str | None:
        """UNIQUE short-name resolution — the fallback identity when an
        archive artifact names its table without a dataset (e.g. a
        missing 00 resource file). Ambiguity returns None: two crosswalk
        rows sharing a short name are never guessed between."""
        hits = self.by_short.get(table_name.lower(), [])
        return hits[0].physical.lower() if len(hits) == 1 else None

    def physical_for_atlas(self, entity: str) -> str | None:
        hit = self.by_atlas.get(entity.lower())
        if hit:
            return hit.physical.lower()
        hits = self.by_short.get(entity.lower(), [])
        return hits[0].physical.lower() if len(hits) == 1 else None
