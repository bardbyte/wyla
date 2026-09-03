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
    ValueMeaning,
    VocabRecord,
)

_TERM_STATUSES = {"Approved", "Candidate", "Under Review", "Rejected"}


def _opt_str(value) -> str | None:
    """Loose-feed normalizer: None / False / "" mean ABSENT; True means
    a bare flag; anything else is the value as text. The real Atlas
    export sends ``sde_group: false`` for "not in an SDE group"."""
    if value is None or value is False or value == "":
        return None
    if value is True:
        return "true"
    return str(value)


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


def load_common_words(path: Path) -> tuple[set[str], list[Quarantined]]:
    """potential_common_word_acronyms.csv — acronyms whose symbols are
    also ordinary words (CARE, FIRST, REST, YES). A GUARD LIST, never a
    second vocabulary: every symbol already lives in data_cleaned.csv;
    this only flags which of them must not be expanded unless the ask
    writes them as acronyms."""
    symbols: set[str] = set()
    quarantined: list[Quarantined] = []
    with Path(path).open(encoding="utf-8-sig", newline="") as f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            low = {(k or "").strip().lower(): (v or "").strip()
                   for k, v in row.items()}
            symbol = low.get("symbol", "")
            if not symbol:
                quarantined.append(Quarantined(
                    source="common_words", category="missing_field",
                    detail="row without symbol",
                    evidence_ref=f"{Path(path).name}#L{i}"))
                continue
            symbols.add(symbol)
    return symbols, quarantined


def load_value_lookup(path: Path) -> tuple[list[ValueMeaning],
                                           list[Quarantined]]:
    """value_lookup.json — ``{"value lookup": {table: {value: [{column,
    synonym}]}}}``: the business meaning of low-cardinality stored
    values, per table and column. One record per (table, column,
    value); an entry without a column or a synonym is quarantined,
    never guessed."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    root = payload
    if isinstance(payload, dict):
        for key in ("value lookup", "value_lookup", "lookup"):
            if isinstance(payload.get(key), dict):
                root = payload[key]
                break
    records: list[ValueMeaning] = []
    quarantined: list[Quarantined] = []
    name = Path(path).name
    for table, by_value in (root or {}).items():
        if not isinstance(by_value, dict):
            quarantined.append(Quarantined(
                source="value_lookup", category="malformed",
                detail=f"{table}: values are not an object",
                evidence_ref=f"{name}#{table}"))
            continue
        for value, entries in by_value.items():
            for n, entry in enumerate(entries if isinstance(entries, list)
                                      else [entries]):
                column = str((entry or {}).get("column") or "").strip() \
                    if isinstance(entry, dict) else ""
                synonym = str((entry or {}).get("synonym") or "").strip() \
                    if isinstance(entry, dict) else ""
                ref = f"{name}#{table}/{value}/{n}"
                if not column or not synonym:
                    quarantined.append(Quarantined(
                        source="value_lookup", category="missing_field",
                        detail=f"{table} value {value!r}: entry without "
                               "column/synonym", evidence_ref=ref))
                    continue
                records.append(ValueMeaning(
                    table=str(table).strip().lower(),
                    column=column.lower(), value=str(value),
                    synonym=synonym, evidence_ref=ref))
    return records, quarantined


def _norm(text: str) -> str:
    return " ".join((text or "").replace("\xa0", " ").split()).lower()


def glossary_drift(view_path: Path,
                   corpus: list[VocabRecord]) -> dict:
    """glossary_terms.csv is a generated VIEW of the corpus (Entry Type
    = Glossary Term). It is never loaded twice; instead the drift
    between the two exports is counted: rows matching exactly, rows
    matching only after normalization (non-breaking spaces, whitespace,
    a blank business unit read as All), and names the corpus lacks."""
    corpus_rows = [r for r in corpus if r.entry_type == "Glossary Term"]
    exact = {(r.symbol, r.definition, r.business_unit, r.region)
             for r in corpus_rows}
    normalized = {(_norm(r.symbol), _norm(r.definition),
                   _norm(r.business_unit or "All"), _norm(r.region or "All"))
                  for r in corpus_rows}
    names = {_norm(r.symbol) for r in corpus_rows}
    view, _q = load_glossary(view_path)
    report = {"view_rows": len(view), "matched_exact": 0, "drifted": 0,
              "missing": 0, "examples": []}
    for r in view:
        if (r.symbol, r.definition, r.business_unit, r.region) in exact:
            report["matched_exact"] += 1
        elif (_norm(r.symbol), _norm(r.definition),
              _norm(r.business_unit or "All"),
              _norm(r.region or "All")) in normalized:
            report["drifted"] += 1
            if len(report["examples"]) < 5:
                report["examples"].append(
                    f"{r.symbol}: normalization only ({r.evidence_ref})")
        elif _norm(r.symbol) in names:
            report["drifted"] += 1
            if len(report["examples"]) < 5:
                report["examples"].append(
                    f"{r.symbol}: definition or scope differs "
                    f"({r.evidence_ref})")
        else:
            report["missing"] += 1
            if len(report["examples"]) < 5:
                report["examples"].append(
                    f"{r.symbol}: not in the corpus ({r.evidence_ref})")
    return report


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


def _harvest_std_tech_entries(payload) -> list[dict]:
    """Find entry-shaped objects ANYWHERE in the export — the loader
    adapts to the file, never the file to the loader. Two shapes match
    (see docs/contracts/std_tech_metadata_layout.md):

    - a FLAT entry: dict carrying ``dataset`` + ``pde``/
      ``datasetAttribute`` in one object;
    - the REAL per-table ENVELOPE: ``{dataset, appl_id, page_info,
      tech_metadata_list: [{datasource…, datasetAttribute, pde}]}`` —
      the table name lives at the envelope, the payload one level
      down; each qualifying list item is flattened with the
      envelope's ``dataset``.

    Every wrapper Atlas might add on top — a plain list, ``{"data":
    [...]}``, a dict keyed by table — parses identically,
    deterministically (document order). A matched entry is collected
    whole, never descended into."""
    found: list[dict] = []

    def walk(node) -> None:
        if isinstance(node, dict):
            if "dataset" in node and ("pde" in node
                                      or "datasetAttribute" in node):
                found.append(node)
                return
            if "dataset" in node and isinstance(
                    node.get("tech_metadata_list"), list):
                for item in node["tech_metadata_list"]:
                    if isinstance(item, dict) and (
                            "pde" in item or "datasetAttribute" in item):
                        found.append({**item, "dataset":
                                      item.get("dataset")
                                      or node["dataset"]})
                return
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return found


def load_std_tech_metadata(root: Path) -> tuple[list[StdTechEntry],
                                                list[Quarantined]]:
    """``root`` is either the per-table directory of JSONs or the single
    combined export (``std_tech_metadata_all.json``) — the Atlas feed
    ships both shapes, in whatever wrapper; entries are harvested by
    signature (see ``_harvest_std_tech_entries``)."""
    records, quarantined = [], []
    root = Path(root)
    paths = [root] if root.is_file() else sorted(root.glob("*.json"))
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            quarantined.append(Quarantined(
                source="std_tech_metadata", category="missing_field",
                detail=f"unreadable JSON: {e}", evidence_ref=path.name))
            continue
        entries = _harvest_std_tech_entries(payload)
        if not entries:
            quarantined.append(Quarantined(
                source="std_tech_metadata", category="missing_field",
                detail="no entry-shaped objects found (a std-tech entry "
                       "is a dict with 'dataset' + 'pde'/"
                       "'datasetAttribute') — send the file's shape",
                evidence_ref=path.name))
            continue
        for entry in entries:
            table = str(entry.get("dataset") or "").strip().lower()
            if not table:
                quarantined.append(Quarantined(
                    source="std_tech_metadata", category="missing_field",
                    detail="entry without dataset", evidence_ref=path.name))
                continue
            # the real feed types loosely: sde_group / pii_role_id come
            # back as `false` for "none" — normalize, never crash
            try:
                attr = entry.get("datasetAttribute") or {}
                columns = []
                for pde in entry.get("pde") or []:
                    pattr = pde.get("pdeAttribute") or {}
                    columns.append(StdTechColumn(
                        name=str(pde.get("pdeRelPath") or "").lower(),
                        description=str(pattr.get("description") or ""),
                        business_name=str(
                            pattr.get("business_name") or ""),
                        data_type=str(pattr.get("data_type_name") or ""),
                        pii_role_id=_opt_str(pattr.get("pii_role_id")),
                        sde_group=_opt_str(pattr.get("sde_group")),
                        linked_terms=[t for t in
                                      (pde.get("businessMetadata") or [])
                                      if isinstance(t, dict)]))
                ownership = attr.get("ownership")
                records.append(StdTechEntry(
                    table=table,
                    description=str(attr.get("description") or ""),
                    business_name=str(attr.get("business_name") or ""),
                    data_category=str(attr.get("data_category") or ""),
                    data_sub_category=str(
                        attr.get("data_sub_category") or ""),
                    layer_type=str(attr.get("data_type_name") or ""),
                    has_pii=bool(attr.get("has_pii")),
                    has_oncop=bool(attr.get("has_oncop")),
                    has_gdpr=bool(attr.get("has_gdpr")),
                    ownership=(ownership
                               if isinstance(ownership, dict) else {}),
                    columns=columns,
                    evidence_ref=path.name))
            except Exception as e:      # one weird entry ≠ a dead run
                quarantined.append(Quarantined(
                    source="std_tech_metadata",
                    category="schema_mismatch",
                    detail=f"{table}: {str(e)[:180]}",
                    evidence_ref=path.name))
    return records, quarantined
