"""Low-cardinality value synonyms — what a stored code MEANS.

The warehouse profile (BQ `15_low_cardinality_values`) tells us which
values a column holds and how often. It never says what they mean, so
a card could only ever offer ``3 known values (1 72%, 0 26%, N 2%)``.
This index supplies the other half: ``1 = KYC done``.

Shape (the real export)::

    {"value lookup": {"<schema.table>": {"<stored value>":
        [{"column": "kyc_check_confirmed__c", "synonym": "KYC done"}]}}}

Three properties of the file drive every decision here:

- **Values are STRINGS and must stay strings.** ``"0"``, ``"001"``,
  ``"01"``, ``"Y"`` and ``"None"`` are five distinct stored values.
  Coercing any of them to a number or to null destroys the mapping —
  and ``"None"`` is a value a column really holds, not an absence.
- **One value may carry several entries**, because the same value
  occurs in several columns of one table, and because one column's
  value can have more than one reading. Grouping is by COLUMN; within
  a column every reading is kept, in document order.
- **It is MINED.** Nothing here outranks an authored description. The
  records carry that provenance so the graph can rank it correctly.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from sahs.loaders.records import Quarantined

# the export's root key contains a SPACE; accept the plausible
# spellings rather than making the file conform to the loader
_ROOT_KEYS = ("value lookup", "value_lookup", "valueLookup", "lookup")


def load_value_synonyms(path: Path) -> tuple[list[dict],
                                             list[Quarantined]]:
    """Returns one record per (table, column): ``{table, column,
    synonyms: {value: [reading, …]}}``. Table names are returned as
    the file spells them; the emitter resolves identity."""
    records: list[dict] = []
    quarantined: list[Quarantined] = []
    path = Path(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        quarantined.append(Quarantined(
            source="value_synonyms", category="missing_field",
            detail=f"unreadable JSON: {e}", evidence_ref=path.name))
        return records, quarantined

    lookup = None
    if isinstance(payload, dict):
        for key in _ROOT_KEYS:
            if isinstance(payload.get(key), dict):
                lookup = payload[key]
                break
        # a bare mapping of table → values parses too
        if lookup is None and payload and all(
                isinstance(v, dict) for v in payload.values()):
            lookup = payload
    if not isinstance(lookup, dict):
        quarantined.append(Quarantined(
            source="value_synonyms", category="schema_mismatch",
            detail="no value-lookup object found (expected a root key "
                   f"in {_ROOT_KEYS} holding table → value → "
                   "[{column, synonym}]) — send the file's shape",
            evidence_ref=path.name))
        return records, quarantined

    for table, values in lookup.items():
        if not isinstance(values, dict):
            quarantined.append(Quarantined(
                source="value_synonyms", category="schema_mismatch",
                detail=f"{table}: value map is not an object",
                evidence_ref=path.name))
            continue
        # column → value → readings, preserving document order
        by_column: dict[str, dict[str, list[str]]] = defaultdict(
            lambda: defaultdict(list))
        for value, entries in values.items():
            # the KEY is the stored value and stays verbatim: "0",
            # "001" and "01" are different values, and "None" is a
            # value the column holds, never an absence
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                column = str(entry.get("column") or "").strip().lower()
                synonym = str(entry.get("synonym") or "").strip()
                if not column or not synonym:
                    continue
                readings = by_column[column][value]
                if synonym not in readings:
                    readings.append(synonym)
        for column, synonyms in by_column.items():
            records.append({
                "table": str(table).strip().lower(),
                "column": column,
                "synonyms": {v: list(r) for v, r in synonyms.items()},
                "evidence_ref": f"{path.name}#{table}.{column}"})
    return records, quarantined
