"""blue_business_insights.csv — ~35.7K tribal SQL fragments.

Columns: insight_name, sql_logic, table_name. Each fragment is wrapped so
one canon pipeline serves everything: predicates become
`SELECT 1 FROM <t> WHERE <pred>`, CASE expressions become
`SELECT <case> FROM <t>`. Bare table names resolve through the registry;
ambiguity quarantines (never guesses). The label is kept verbatim as the
concept label — census normalization is whitespace/case only (E9).

The real corpus is enterprise-wide, so the quarantine ledger must tell
the truth about WHY a row sat out: a table outside the run's registry is
`out_of_scope` (nothing is missing — the row belongs to a table this run
does not carry), while prose in the sql_logic column ("Cheque Cashing")
is `not_sql`, caught BEFORE canon so `blue_canon_rate` measures the
pipeline on rows that claim to be SQL. The prose test is deliberately
conservative — borderline rows still go to the parser and fail there
honestly, where the gate can see them."""

from __future__ import annotations

import csv
import re
from pathlib import Path

from sahs.canon.authority import Authority
from sahs.canon.canonical import wrap_case, wrap_predicate
from sahs.loaders.records import ExpressionRecord, Quarantined
from sahs.loaders.registry import TableRegistry

SOURCE = "blue_insights"

# Characters and keywords SQL cannot be written without. Comma is NOT
# signal (prose has commas; a bare comma is never a valid predicate);
# hyphen and period ARE (subtraction, qualified columns) — that lets
# "Non-Sufficient Funds" through to the parser, which is the point.
_SQL_SIGNAL = re.compile(
    r"[=<>!()'\"`+*/.%-]"
    r"|\b(?:case|when|then|else|end|and|or|not|in|like|between|is|null|"
    r"exists|select|from|where|distinct|cast|coalesce|nullif|if|count|"
    r"sum|avg|min|max)\b",
    re.IGNORECASE)


def _is_prose(logic: str) -> bool:
    """True only for TWO or more bare words carrying zero SQL signal.
    A single bare word may be a boolean column (`WHERE is_active_flag`)
    — the parser judges those, not a heuristic."""
    return not _SQL_SIGNAL.search(logic) and len(logic.split()) >= 2


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
                # scope before content: an enterprise-wide table outside
                # the run registry is the whole story for this run
                quarantined.append(Quarantined(
                    source=SOURCE,
                    category="ambiguous_table" if reason == "ambiguous"
                    else "out_of_scope",
                    detail=f"table {raw_table!r} → {reason}",
                    evidence_ref=ref))
                continue
            if _is_prose(logic):
                quarantined.append(Quarantined(
                    source=SOURCE, category="not_sql",
                    detail=f"prose in sql_logic: {logic[:80]!r}",
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
