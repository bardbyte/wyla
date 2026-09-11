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


def _yn(value) -> bool | None:
    """Atlas types its flags three ways in the same export: real
    booleans, ``"Y"``/``"N"`` strings, and absent. ABSENT IS NOT FALSE
    — an unknown active-flag must stay unknown, so None survives all
    the way to the graph rather than being flattened to a lie."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("y", "yes", "true", "1"):
        return True
    if text in ("n", "no", "false", "0"):
        return False
    return None


def _opt_int(value) -> int | None:
    """Positions and lengths arrive as ints, as digit strings, and as
    ``""``. Anything that is not a whole number is ABSENT, never 0."""
    if value is None or value is False or value == "":
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _kept_cell(cell: dict) -> dict:
    """Empty is ABSENT: a sensitivity cell carries only what the feed
    actually said about that column."""
    return {k: v for k, v in cell.items() if v not in (None, "")}


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


STD_TECH_CONSUMED_KEYS: dict[str, frozenset[str]] = {
    "envelope": frozenset({"dataset", "appl_id", "tech_metadata_list"}),
    "entry": frozenset({
        "dataset", "appl_id", "datasource", "datasetGroup", "dataserver",
        "datasystem", "technology", "isActive", "isLatest",
        "isLineageExist", "datasetAttribute", "pde"}),
    "datasetAttribute": frozenset({
        "description", "business_name", "data_category",
        "data_sub_category", "data_type_name", "has_pii", "has_oncop",
        "has_gdpr", "ownership", "table_name", "type", "load_type",
        "is_partitioned", "target_system", "pii_columns",
        "gdpr_columns", "oncop_columns", "business_unit",
        "data_classification", "host_region", "decommissioned",
        "partitioned_columns"}),
    "pii_columns[]": frozenset({"column", "column_name", "pii_role_id",
                                "data_type_name", "is_mandatory"}),
    "pde": frozenset({"pdeRelPath", "pdeAttribute", "businessMetadata"}),
    "pdeAttribute": frozenset({
        "column_name", "description", "business_name", "data_type_name",
        "pii_role_id", "sde_group", "position", "column_length_number",
        "nullable_indicator", "primary_key_indicator",
        "partition_indicator", "derived_logic", "is_clustered",
        "cluster_position", "partition_position", "attribute_scale",
        "publish_code"}),
    "businessMetadata[]": frozenset({
        "businessTermId", "businessTermName", "businessTermDescription",
        "sourceName", "sourceType", "confidenceScore"}),
}
# keys read and deliberately NOT carried, with the reason pinned


# Read and deliberately NOT carried, each with the reason. A key here
# is a DECISION on record; a key in neither table is an UNCONSUMED
# finding the census reports until someone decides.
STD_TECH_DEFERRED_KEYS: dict[str, dict[str, str]] = {
    "envelope": {"page_info": "pagination bookkeeping about the API "
                              "call, not a fact about the table"},
    "entry": {
        "applId": "the envelope's appl_id under a second spelling; "
                  "the same value, already carried",
        "updatedTime": "catalog row mtime: when the CATALOG changed, "
                       "not when the table did — the warehouse's own "
                       "last_modified is the freshness fact",
        "version": "catalog row version: internal optimistic-locking "
                   "bookkeeping",
    },
    "datasetAttribute": {
        "dataset_parent_id": "catalog-internal parent pointer; "
                             "identity comes from the crosswalk",
        "schema_id": "catalog-internal schema row id",
        "schema_parent_id": "catalog-internal schema parent pointer",
        "dataset_source_details": "catalog-internal source blob",
        "table_grouping": "catalog-internal grouping key",
        "version": "catalog row version: internal bookkeeping",
        "lumi_first_table_in": "catalog onboarding marker: when the "
                               "table entered the catalog, not a fact "
                               "about the data",
        "ownership_id": "an id for the ownership record; the people "
                        "themselves ride on owned_by edges",
        "data_type": "the attribute's own type word, duplicated by "
                     "data_type_name which the layer prop already reads",
    },
    "pde": {
        "isActive": "per-column activity flag; the TABLE's isActive is "
                    "the governed signal and a column's own is unused "
                    "by any surface",
        "isLatest": "per-column latest flag, empty across the feed",
        "isLineageExist": "per-column lineage flag; lineage edges come "
                          "from the MDM plane, which states them",
        "type": "the pde's own kind word (column vs nested field); the "
                "dotted path in the name already says which",
        "updatedTime": "catalog row mtime for the column",
    },
    "businessMetadata[]": {
        "businessTermAssetId": "catalog-internal asset pointer",
        "businessTermDomainName": "the term's domain in the glossary; "
                                  "served when a term card exists",
        "businessTermStatus": "the term's governance status, already "
                              "carried on the term node from the "
                              "glossary export which is its registry",
        "activationIndicator": "catalog-internal activation flag",
        "aemp70Indicator": "internal compliance-programme marker",
        "confidenceLevel": "a word form of confidenceScore, which is "
                           "already read as a number on the edge",
        "informationClassification": "the TERM's classification; the "
                                     "column's own sensitivity is the "
                                     "governed fact and is read",
        "metadataInfoClass": "catalog-internal class marker",
        "scopeSegmentName": "catalog-internal scope marker",
        "sdeNames": "the sde group under a second spelling; sde_group "
                    "on the column is already read",
        "dataCustodianEmailAddress": "the TERM's custodian, not the "
                                     "table's owner; served when a "
                                     "term card exists",
        "dataCustodianFullName": "see dataCustodianEmailAddress",
        "dataCustodianUserId": "see dataCustodianEmailAddress",
        "userList": "empty across the feed; a list of viewers, not a "
                    "fact about the term",
    },
}
# ``ownership`` is consumed WHOLE as the ``ownership_atlas`` prop; a key
# that names a person (owner / VP) ALSO becomes an ``owned_by`` edge.
# The census reports which of the two each real key got, so a role the
# heuristic does not recognise (a steward, a custodian) is visible as
# "prop only" instead of quietly failing to reach the owner nodes.


OWNERSHIP_EDGE_MARKERS = ("owner", "vp")


def ownership_key_is_person(key: str) -> bool:
    k = key.lower()
    return any(marker in k for marker in OWNERSHIP_EDGE_MARKERS)


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
                        # the envelope carries the table name AND the
                        # application id; both belong to every entry
                        # flattened out of its list
                        found.append({**item, "dataset":
                                      item.get("dataset")
                                      or node["dataset"],
                                      "appl_id": item.get("appl_id")
                                      or node.get("appl_id")})
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
                        name=str(pde.get("pdeRelPath")
                                 or pattr.get("column_name")
                                 or "").lower(),
                        description=str(pattr.get("description") or ""),
                        business_name=str(
                            pattr.get("business_name") or ""),
                        data_type=str(pattr.get("data_type_name") or ""),
                        pii_role_id=_opt_str(pattr.get("pii_role_id")),
                        sde_group=_opt_str(pattr.get("sde_group")),
                        column_name=str(pattr.get("column_name") or ""),
                        position=_opt_int(pattr.get("position")),
                        column_length=_opt_int(
                            pattr.get("column_length_number")),
                        nullable=_yn(pattr.get("nullable_indicator")),
                        primary_key=_yn(
                            pattr.get("primary_key_indicator")),
                        partition_key=_yn(
                            pattr.get("partition_indicator")),
                        is_clustered=_yn(pattr.get("is_clustered")),
                        cluster_position=_opt_int(
                            pattr.get("cluster_position")),
                        partition_position=_opt_int(
                            pattr.get("partition_position")),
                        attribute_scale=_opt_int(
                            pattr.get("attribute_scale")),
                        publish_code=str(
                            pattr.get("publish_code") or ""),
                        derived_logic=str(
                            pattr.get("derived_logic") or "").strip(),
                        linked_terms=[t for t in
                                      (pde.get("businessMetadata") or [])
                                      if isinstance(t, dict)]))
                ownership = attr.get("ownership")
                # normalized at the parse boundary: the emitter should
                # never have to know that Atlas writes `false` for "no
                # role" (same loose typing as sde_group/pii_role_id)
                # the column key has TWO spellings across exports:
                # `column` in the documented contract, `column_name` in
                # the feed itself. Reading one meant every row of the
                # real feed's list was skipped and the second
                # sensitivity witness produced nothing at all.

                def _sensitive(key: str) -> list[dict]:
                    out = []
                    for c in (attr.get(key) or []):
                        if not isinstance(c, dict):
                            continue
                        name = str(c.get("column")
                                   or c.get("column_name") or "").strip()
                        if not name:
                            continue
                        out.append(_kept_cell({
                            "column": name.lower(),
                            "pii_role_id": _opt_str(c.get("pii_role_id")),
                            "data_type": str(
                                c.get("data_type_name") or ""),
                            "mandatory": _yn(c.get("is_mandatory"))}))
                    return out

                pii_columns = _sensitive("pii_columns")
                gdpr_columns = _sensitive("gdpr_columns")
                oncop_columns = _sensitive("oncop_columns")
                records.append(StdTechEntry(
                    table=table,
                    description=str(attr.get("description") or ""),
                    business_name=str(attr.get("business_name") or ""),
                    data_category=str(attr.get("data_category") or ""),
                    data_sub_category=str(
                        attr.get("data_sub_category") or ""),
                    layer_type=str(attr.get("data_type_name") or ""),
                    has_pii=_yn(attr.get("has_pii")),
                    has_oncop=_yn(attr.get("has_oncop")),
                    has_gdpr=_yn(attr.get("has_gdpr")),
                    ownership=(ownership
                               if isinstance(ownership, dict) else {}),
                    columns=columns,
                    evidence_ref=path.name,
                    appl_id=str(entry.get("appl_id") or ""),
                    datasource=str(entry.get("datasource") or ""),
                    dataset_group=str(entry.get("datasetGroup") or ""),
                    data_server=str(entry.get("dataserver") or ""),
                    data_system=str(entry.get("datasystem") or ""),
                    technology=str(entry.get("technology") or ""),
                    is_active=_yn(entry.get("isActive")),
                    is_latest=_yn(entry.get("isLatest")),
                    is_lineage_exist=_yn(entry.get("isLineageExist")),
                    table_name=str(attr.get("table_name") or ""),
                    table_type=str(attr.get("type") or ""),
                    load_type=str(attr.get("load_type") or ""),
                    is_partitioned=_yn(attr.get("is_partitioned")),
                    target_system=str(attr.get("target_system") or ""),
                    pii_columns=pii_columns,
                    gdpr_columns=gdpr_columns,
                    oncop_columns=oncop_columns,
                    business_unit=str(attr.get("business_unit") or ""),
                    data_classification=str(
                        attr.get("data_classification") or ""),
                    host_region=str(attr.get("host_region") or ""),
                    decommissioned=_yn(attr.get("decommissioned")),
                    partitioned_columns=[
                        str(c.get("column") or c.get("column_name") or c)
                        if isinstance(c, dict) else str(c)
                        for c in (attr.get("partitioned_columns") or [])
                        if c]))
            except Exception as e:      # one weird entry ≠ a dead run
                quarantined.append(Quarantined(
                    source="std_tech_metadata",
                    category="schema_mismatch",
                    detail=f"{table}: {str(e)[:180]}",
                    evidence_ref=path.name))
    return records, quarantined
