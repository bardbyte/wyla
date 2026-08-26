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
honestly, where the gate can see them.

Some export rows arrive with the two columns SWAPPED — SQL in
insight_name, the label in sql_logic. Detection is deterministic (this
side fails to parse, that side parses) and the row is recovered with
`extra.column_swap=True`; a row broken on BOTH sides still reaches
canon so the gate counts it."""

from __future__ import annotations

import csv
import re
from pathlib import Path

import sqlglot

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


def _wrap(logic: str, table: str) -> tuple[str, str]:
    """CASE-expression vs predicate wrapping. `CASE ` with the space —
    a predicate on a column NAMED case_* is not a CASE expression."""
    if logic.upper().lstrip().startswith("CASE "):
        return wrap_case(logic, table), "case"
    return wrap_predicate(logic, table), "predicate"


def _parses(sql: str) -> bool:
    try:
        sqlglot.parse_one(sql, read="bigquery")
        return True
    except Exception:               # every flavor of "won't parse"
        return False


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
            wrapped, kind = _wrap(logic, table)
            if not _parses(wrapped) and _SQL_SIGNAL.search(label):
                # the export swaps the two columns in some rows: SQL in
                # insight_name, the human label in sql_logic. Detection
                # is deterministic — this side fails to parse, that side
                # parses — and lives in the loader so it survives every
                # re-export. Rows broken on BOTH sides fall through to
                # canon: the gate must keep seeing rows that claim to be
                # SQL and are not.
                alt, alt_kind = _wrap(label, table)
                if _parses(alt):
                    records.append(ExpressionRecord(
                        raw_sql=alt, kind=alt_kind, source=SOURCE,
                        authority=Authority.SNIPPET,
                        concept_label=logic, table_hint=table,
                        evidence_ref=ref,
                        extra={"column_swap": True}))
                    continue
            records.append(ExpressionRecord(
                raw_sql=wrapped, kind=kind,
                source=SOURCE, authority=Authority.SNIPPET,
                concept_label=label, table_hint=table,
                evidence_ref=ref))
    return records, quarantined
