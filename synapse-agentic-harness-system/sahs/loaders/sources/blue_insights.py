"""blue_business_insights.csv — ~35.7K tribal SQL fragments.

Columns: insight_name, sql_logic, table_name. Each fragment is wrapped so
one canon pipeline serves everything: predicates become
`SELECT 1 FROM <t> WHERE <pred>`, CASE expressions become
`SELECT <case> FROM <t>`. Bare table names resolve through the registry;
ambiguity quarantines (never guesses). The label is kept verbatim as the
concept label — census normalization is whitespace/case only (E9)."""

from __future__ import annotations

import csv
from pathlib import Path

from sahs.canon.authority import Authority
from sahs.canon.canonical import wrap_case, wrap_predicate
from sahs.loaders.records import ExpressionRecord, Quarantined
from sahs.loaders.registry import TableRegistry

SOURCE = "blue_insights"


def load_blue_insights(path: Path, registry: TableRegistry
                       ) -> tuple[list[ExpressionRecord], list[Quarantined]]:
    records: list[ExpressionRecord] = []
    quarantined: list[Quarantined] = []
    with Path(path).open(encoding="utf-8-sig", newline="") as f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            ref = f"{Path(path).name}#L{i}"
            label = str(row.get("insight_name") or "").strip()
            logic = str(row.get("sql_logic") or "").strip()
            raw_table = str(row.get("table_name") or "").strip()
            if not label or not logic or not raw_table:
                quarantined.append(Quarantined(
                    source=SOURCE, category="missing_field",
                    detail=f"row missing {'label' if not label else 'sql' if not logic else 'table'}",
                    evidence_ref=ref))
                continue
            table, reason = registry.resolve(raw_table)
            if table is None:
                quarantined.append(Quarantined(
                    source=SOURCE,
                    category="ambiguous_table" if reason == "ambiguous"
                    else "missing_field",
                    detail=f"table {raw_table!r} → {reason}",
                    evidence_ref=ref))
                continue
            is_case = logic.upper().lstrip().startswith("CASE")
            wrapped = (wrap_case(logic, table) if is_case
                       else wrap_predicate(logic, table))
            records.append(ExpressionRecord(
                raw_sql=wrapped,
                kind="case" if is_case else "predicate",
                source=SOURCE, authority=Authority.SNIPPET,
                concept_label=label, table_hint=table,
                evidence_ref=ref))
    return records, quarantined
