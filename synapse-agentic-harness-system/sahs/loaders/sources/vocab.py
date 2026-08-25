"""Vocabulary + catalog sources — parsed shapes, no canon.

data_cleaned.csv        ~12.3K acronyms/terms, BU+region scoped
business_terms.csv      ~4.4K Atlas terms with governance status
std_tech_metadata/      46 Atlas catalog entries (column→term links)

These skip c(sql) — they are not SQL-shaped — but P0 still parses and
counts them (fixture CI runs every branch), and P2's quad emission builds
on exactly these records."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from sahs.loaders.records import (
    Quarantined,
    StdTechColumn,
    StdTechEntry,
    TermRecord,
    VocabRecord,
)

_TERM_STATUSES = {"Approved", "Candidate", "Under Review", "Rejected"}


def load_glossary(path: Path) -> tuple[list[VocabRecord], list[Quarantined]]:
    records, quarantined = [], []
    with Path(path).open(encoding="utf-8-sig", newline="") as f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            low = {(k or "").strip().lower(): (v or "").strip()
                   for k, v in row.items()}
            symbol = low.get("symbol", "")
            definition = low.get("definition", "")
            if not symbol or not definition:
                quarantined.append(Quarantined(
                    source="glossary", category="missing_field",
                    detail="row without symbol/definition",
                    evidence_ref=f"{Path(path).name}#L{i}"))
                continue
            records.append(VocabRecord(
                symbol=symbol, definition=definition,
                business_unit=low.get("business_unit") or "All",
                region=low.get("global_region") or low.get("region") or "All",
                entry_type=low.get("entry_type") or "Acronym",
                evidence_ref=f"{Path(path).name}#L{i}"))
    return records, quarantined


def load_business_terms(path: Path) -> tuple[list[TermRecord],
                                             list[Quarantined]]:
    records, quarantined = [], []
    payload_path = Path(path)
    if payload_path.suffix.lower() == ".json":
        rows = json.loads(payload_path.read_text(encoding="utf-8"))
        items = [(i + 1, r) for i, r in enumerate(rows)]
    else:
        with payload_path.open(encoding="utf-8-sig", newline="") as f:
            items = [(i, r) for i, r in enumerate(csv.DictReader(f), start=2)]
    for i, row in items:
        term_id = str(row.get("businessTermId") or "").strip()
        name = str(row.get("businessTermName") or "").strip()
        status = str(row.get("businessTermStatus") or "").strip()
        ref = f"{payload_path.name}#row={i}"
        if not term_id or not name:
            quarantined.append(Quarantined(
                source="business_terms", category="missing_field",
                detail="term without id/name", evidence_ref=ref))
            continue
        if status not in _TERM_STATUSES:
            quarantined.append(Quarantined(
                source="business_terms", category="missing_field",
                detail=f"term {term_id}: unknown status {status!r}",
                evidence_ref=ref))
            continue
        records.append(TermRecord(term_id=term_id, name=name,
                                  status=status, evidence_ref=ref))
    return records, quarantined


def load_std_tech_metadata(root: Path) -> tuple[list[StdTechEntry],
                                                list[Quarantined]]:
    records, quarantined = [], []
    for path in sorted(Path(root).glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            quarantined.append(Quarantined(
                source="std_tech_metadata", category="missing_field",
                detail=f"unreadable JSON: {e}", evidence_ref=path.name))
            continue
        entries = payload if isinstance(payload, list) else [payload]
        for entry in entries:
            table = str(entry.get("dataset") or "").strip().lower()
            if not table:
                quarantined.append(Quarantined(
                    source="std_tech_metadata", category="missing_field",
                    detail="entry without dataset", evidence_ref=path.name))
                continue
            attr = entry.get("datasetAttribute") or {}
            columns = []
            for pde in entry.get("pde") or []:
                pattr = pde.get("pdeAttribute") or {}
                columns.append(StdTechColumn(
                    name=str(pde.get("pdeRelPath") or "").lower(),
                    description=str(pattr.get("description") or ""),
                    business_name=str(pattr.get("business_name") or ""),
                    data_type=str(pattr.get("data_type_name") or ""),
                    pii_role_id=pattr.get("pii_role_id"),
                    sde_group=pattr.get("sde_group"),
                    linked_terms=list(pde.get("businessMetadata") or [])))
            records.append(StdTechEntry(
                table=table,
                description=str(attr.get("description") or ""),
                business_name=str(attr.get("business_name") or ""),
                data_category=str(attr.get("data_category") or ""),
                data_sub_category=str(attr.get("data_sub_category") or ""),
                layer_type=str(attr.get("data_type_name") or ""),
                has_pii=bool(attr.get("has_pii")),
                ownership=dict(attr.get("ownership") or {}),
                columns=columns,
                evidence_ref=path.name))
    return records, quarantined
