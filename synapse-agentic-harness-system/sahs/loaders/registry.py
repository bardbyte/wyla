"""The extracted-table registry — bare table names resolve against it.

blue_business_insights rows carry `table_name` values with no dataset
qualifier and occasional truncation. Resolution rule (pinned): exact match
first, else UNIQUE suffix match; an ambiguous suffix quarantines the row —
guessing a table is how wrong numbers get born.

The registry's authoritative feed is the BQ archive's `_batch_summary.csv`
(one row per extracted table); a plain newline list works for fixtures and
ad-hoc scopes.
"""

from __future__ import annotations

import csv
from pathlib import Path


class TableRegistry:
    def __init__(self, names: list[str]) -> None:
        self.names = sorted({n.strip().lower() for n in names if n.strip()})

    @classmethod
    def from_batch_summary(cls, path: Path) -> "TableRegistry":
        with Path(path).open(encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        return cls([str(r.get("table") or r.get("table_name") or "")
                    for r in rows])

    @classmethod
    def from_list_file(cls, path: Path) -> "TableRegistry":
        return cls(Path(path).read_text(encoding="utf-8").splitlines())

    def resolve(self, raw: str) -> tuple[str | None, str]:
        """→ (resolved_name | None, reason). Reasons: exact | qualified |
        suffix | ambiguous | unknown.

        Real query references arrive FULLY QUALIFIED
        (``project.dataset.table``) while this registry holds bare
        extracted-table names — a qualified reference resolves by its
        table component (real identity is the crosswalk's job, E1).
        Ambiguity still quarantines: two registry tables sharing the
        short name never get guessed between."""
        name = (raw or "").strip().lower().replace("`", "")
        if not name:
            return None, "unknown"
        if name in self.names:
            return name, "exact"
        short = name.split(".")[-1].strip()
        if not short:
            return None, "unknown"
        if short != name and short in self.names:
            return short, "qualified"
        hits = [n for n in self.names
                if n.endswith(short) or n.split(".")[-1] == short]
        uniq = sorted(set(hits))
        if len(uniq) == 1:
            return uniq[0], "suffix"
        if len(uniq) > 1:
            return None, "ambiguous"
        return None, "unknown"
