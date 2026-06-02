"""Glossary loader — acronyms / synonyms with disambiguation context.

The CSV is expected to have at minimum:
    Symbol, Definition, BusinessUnit (or BU), Region, EntryType

Column name matching is case-insensitive and tolerates a handful of
common aliases (Symbol/Acronym/Abbreviation, BU/Business Unit, etc.).
Forward-compat: every original row is preserved in `raw_row`.

Same symbol can appear multiple times with different (BU, region) —
that's the disambiguation context. The loader does NOT dedup; the
multiplicity IS the signal.
"""

from __future__ import annotations

import csv
from pathlib import Path

from synapse.registry.schemas import GlossaryEntry


# Column-name aliases we accept (case-insensitive)
_SYMBOL_KEYS = ("symbol", "acronym", "abbreviation", "term", "code")
_DEFINITION_KEYS = ("definition", "meaning", "expansion", "description")
_BU_KEYS = ("business_unit", "businessunit", "bu", "business unit", "business unit/division", "division")
_REGION_KEYS = ("region", "market", "geo", "geography")
_TYPE_KEYS = ("entry_type", "entrytype", "type", "category", "kind")


def _pick(row: dict, candidates: tuple[str, ...]) -> str | None:
    """Return the first non-empty value matching any candidate column name."""
    lower = {(k or "").strip().lower(): v for k, v in row.items()}
    for c in candidates:
        v = lower.get(c)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def load_glossary(path: Path) -> list[GlossaryEntry]:
    """Load every row of the glossary CSV into GlossaryEntry list.

    Raises FileNotFoundError if path is missing. Returns [] if the file
    is empty (header only)."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Glossary CSV not found: {p}")

    out: list[GlossaryEntry] = []
    with p.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = _pick(row, _SYMBOL_KEYS)
            defn = _pick(row, _DEFINITION_KEYS)
            if not symbol or not defn:
                continue  # require at minimum symbol + definition
            out.append(GlossaryEntry(
                symbol=symbol,
                definition=defn,
                business_unit=_pick(row, _BU_KEYS),
                region=_pick(row, _REGION_KEYS),
                entry_type=_pick(row, _TYPE_KEYS),
                raw_row=dict(row),
            ))
    return out


def index_by_symbol(
    entries: list[GlossaryEntry],
) -> dict[str, list[GlossaryEntry]]:
    """Group entries by lowercased symbol. Multiple defs per symbol
    are the disambiguation context — keep them grouped."""
    out: dict[str, list[GlossaryEntry]] = {}
    for e in entries:
        out.setdefault(e.symbol.lower(), []).append(e)
    return out


def ambiguous_symbols(
    entries: list[GlossaryEntry], min_defs: int = 2,
) -> dict[str, list[GlossaryEntry]]:
    """Return only the symbols that have ≥ min_defs distinct meanings."""
    grouped = index_by_symbol(entries)
    return {
        sym: items for sym, items in grouped.items()
        if len({i.definition for i in items}) >= min_defs
    }
