"""Render: the deterministic bundle — sections in the order a person
enters them into the catalog, the translation ledger, the aspect
payloads, the suggestions, and the right-rail guide.

Everything here is a pure function of the ``FactSet`` (plus the config
and, when present, the verified model output). Nothing is looked up
again; if a section is empty it says why."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any

from sahs.kc import coverage
from sahs.kc.assemble import (COPY, NEVER, REVIEW, TIER_CANDIDATE, Fact,
                              FactSet)
from sahs.kc.config import ASPECT_TYPES, KcConfig

SECTION_ORDER: tuple[tuple[str, str], ...] = (
    ("description", "Description"),
    ("overview", "Overview"),
    ("columns", "Column descriptions"),
    ("glossary", "Glossary terms"),
    ("related_entries", "Related entries"),
    ("aspects", "Aspects"),
    ("dq", "DQ rules"),
    ("queries", "Queries"),
    ("contacts", "Contacts"),
    ("review", "Needs review"),
    ("push", "Push record"),
)

OVERVIEW_SECTIONS: tuple[tuple[str, str], ...] = (
    ("purpose", "Purpose"),
    ("grain_keys", "Grain & keys"),
    ("ownership_usage", "Ownership & usage"),
    ("columns_that_matter", "Columns that matter"),
    ("metrics", "Metrics on this table"),
    ("concepts_filters", "Concepts & filters"),
    ("joins", "Joins"),
    ("sensitivity", "Sensitivity"),
    ("unknowns", "What we don't know yet"),
    ("provenance", "Provenance & build"),
)

# the catalog construct each section fills, in the catalog's words, and
# the click path a person follows for it
GUIDE: tuple[dict[str, str], ...] = (
    {"section": "description", "construct": "Entry description",
     "path": "Descriptions update through the Dataplex API "
             "(entries.patch on the BigQuery entry) — use the JSON export; "
             "the console edits the overview, not the description."},
    {"section": "overview", "construct": "Entry overview (rich text)",
     "path": "Catalog → search the table → entry page → Overview → Edit → "
             "enter the overview text → Save."},
    {"section": "columns", "construct": "Column descriptions",
     "path": "Column descriptions are API-only (entries.patch, schema "
             "aspect) — use the JSON export."},
    {"section": "glossary", "construct": "Glossary term",
     "path": "Glossaries → your glossary → category → Add term → display "
             "name, description, overview → Save. Many terms: a metadata "
             "import job with the glossary JSONL export."},
    {"section": "related_entries", "construct": "Related entry (term ↔ asset)",
     "path": "Glossary term page → Related entries → Add → pick the table "
             "or column. Bulk: entryLinks JSONL (type definition) in an "
             "import job."},
    {"section": "aspects", "construct": "Aspect (custom aspect type)",
     "path": "Entry page → Aspects → Add aspect → choose the aspect type → "
             "fill the fields from the JSON → Save. Create the aspect types "
             "once from the panel on the right."},
    {"section": "dq", "construct": "Data quality rule",
     "path": "Attach the rules to the glossary term (term page → Data "
             "quality rules) or create a data quality scan on the table "
             "with these rules."},
    {"section": "queries", "construct": "Queries aspect",
     "path": "Entry page → Queries → Add query → SQL + one-line "
             "description → set source to User → Save."},
    {"section": "contacts", "construct": "Contacts aspect",
     "path": "Entry page → Contacts → Edit → add each identity with its "
             "role → Save."},
    {"section": "review", "construct": "(none: a human decides)",
     "path": "Decide each item: approve moves it to a copy block on the "
             "next load; hold leaves it here."},
    {"section": "push", "construct": "(Synapse record)",
     "path": "Tick the sections you entered, name yourself, Mark pushed."},
)


@dataclass
class Section:
    key: str
    title: str
    text: str = ""                    # copy-ready text
    items: list[Any] = field(default_factory=list)
    facts_used: list[str] = field(default_factory=list)
    status_counts: dict[str, int] = field(default_factory=dict)
    witnesses: list[str] = field(default_factory=list)
    review: list[dict[str, str]] = field(default_factory=list)
    llm: bool = False
    empty_reason: str = ""
    gate_tier: str = ""
    paragraphs: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _cite(fact_ids: list[str]) -> str:
    return "[" + ",".join(fact_ids) + "]" if fact_ids else ""


def _disclose(f: Fact) -> str:
    return f"[{f.witness or '?'} · {f.status}]"


def _tally(section: Section, facts: list[Fact]) -> None:
    section.facts_used = [f.id for f in facts]
    counts: dict[str, int] = defaultdict(int)
    witnesses: set[str] = set()
    for f in facts:
        counts[f.status] += 1
        witnesses.update(w for w in f.witness.split(",") if w)
    section.status_counts = dict(sorted(counts.items()))
    section.witnesses = sorted(witnesses)


def _copyable(facts: list[Fact]) -> list[Fact]:
    return [f for f in facts if f.include == COPY]


# ── sections ─────────────────────────────────────────────────────

def _description(fs: FactSet, llm: dict | None) -> Section:
    sec = Section("description", "Description")
    facts = _copyable(fs.by_target("entry.description"))
    grain = _copyable(fs.by_target("entry.overview.grain_keys"))
    status = _copyable([f for f in fs.by_target("aspect.definition-status.lifecycle")])
    if llm and llm.get("description"):
        sec.text = llm["description"]
        sec.llm = True
        sec.paragraphs = [{"text": llm["description"],
                           "fact_ids": llm.get("description_fact_ids", [])}]
    elif facts:
        lead = facts[0].text.rstrip(".")
        bits = [lead]
        if grain:
            bits.append(grain[0].text.split(" is a ", 1)[-1])
        if status:
            bits.append(f"lifecycle {status[0].data}")
        sec.text = ". ".join(bits) + "."
        sec.paragraphs = [{"text": sec.text,
                           "fact_ids": [f.id for f in facts + grain[:1] + status[:1]]}]
    else:
        sec.empty_reason = ("no description on record from any plane: the "
                            "catalog entry keeps its own text")
    _tally(sec, facts + grain[:1] + status[:1])
    return sec


def _overview(fs: FactSet, llm: dict | None, gate_tier: str) -> Section:
    sec = Section("overview", "Overview")
    used: list[Fact] = []
    lines: list[str] = []
    for key, title in OVERVIEW_SECTIONS:
        facts = _copyable(fs.by_target(f"entry.overview.{key}"))
        review = [f for f in fs.by_target(f"entry.overview.{key}")
                  if f.include == REVIEW]
        lines.append(f"## {title}")
        if llm and (llm.get("overview_sections") or {}).get(key):
            para = llm["overview_sections"][key]
            lines.append(para["text"])
            sec.paragraphs.append({"section": key, **para, "llm": True})
            used.extend(f for f in facts if f.id in para.get("fact_ids", []))
        elif facts:
            for f in facts:
                lines.append(f"- {f.text} {_disclose(f)} [{f.id}]")
            sec.paragraphs.append({"section": key,
                                   "text": "\n".join(f.text for f in facts),
                                   "fact_ids": [f.id for f in facts],
                                   "llm": False})
            used.extend(facts)
        else:
            lines.append("- nothing on record yet"
                         + (f" ({len(review)} item(s) need review first)"
                            if review else ""))
        lines.append("")
    sec.text = "\n".join(lines).strip()
    sec.llm = bool(llm and llm.get("overview_sections"))
    sec.gate_tier = gate_tier
    _tally(sec, used)
    if not used:
        sec.empty_reason = "no overview facts on record"
    return sec


def _columns(fs: FactSet, llm: dict | None) -> Section:
    sec = Section("columns", "Column descriptions")
    facts = fs.by_target("column.description") + fs.by_target("column.sensitivity")
    by_col: dict[str, dict[str, Any]] = {}
    for f in facts:
        data = f.data if isinstance(f.data, dict) else {}
        name = data.get("column") or f.subject.split(".")[-1]
        row = by_col.setdefault(name, {
            "column": name, "type": data.get("type", ""), "description": "",
            "supplementary": "", "sensitive": False, "term": "", "status": f.status,
            "witness": set(), "fact_ids": [], "include": COPY})
        row["witness"].update(w for w in f.witness.split(",") if w)
        row["fact_ids"].append(f.id)
        if f.kc_target == "column.sensitivity":
            row["sensitive"] = True
        elif data.get("supplementary"):
            row["supplementary"] = data.get("description", "")
        elif data.get("nested_path"):
            row["nested_path"] = data["nested_path"]
        elif f.include == COPY:
            row["description"] = row["description"] or data.get("description", "")
            row["type"] = row["type"] or data.get("type", "")
        if f.include != COPY:
            row["include"] = f.include
    for f in fs.by_target("glossary.related_entry"):
        data = f.data if isinstance(f.data, dict) else {}
        target = str(data.get("target") or "")
        if target.startswith(fs.table + "."):
            name = target[len(fs.table) + 1:]
            if name in by_col:
                by_col[name]["term"] = data.get("term", "")
    llm_cols = {c.get("name"): c for c in (llm or {}).get("columns") or []}
    for name, row in by_col.items():
        if name in llm_cols and llm_cols[name].get("text"):
            row["description_llm"] = llm_cols[name]["text"]
            row["llm_fact_ids"] = llm_cols[name].get("fact_ids", [])
    needs = [f for f in fs.by_target("suggestion.column_description")]
    rows = sorted((r for r in by_col.values()
                   if r["description"] or r["supplementary"] or r["sensitive"]
                   or r.get("nested_path") or r.get("description_llm")),
                  key=lambda r: r["column"])
    for r in rows:
        r["witness"] = sorted(r["witness"])
    sec.items = rows
    header = "column · type · description · term link · sensitivity · status"
    body = [header, "-" * len(header)]
    for r in rows:
        text = r.get("description_llm") or r["description"]
        if not text and r.get("nested_path"):
            text = f"nested at {r['nested_path']}"
        if r.get("supplementary"):
            text = f"{text} | {r['supplementary']}" if text else r["supplementary"]
        body.append(f"{r['column']} · {r['type']} · {text} · "
                    f"{r['term'] or '-'} · {'SENSITIVE' if r['sensitive'] else '-'} · "
                    f"{r['status']} [{','.join(r['witness'])}] {_cite(r['fact_ids'])}")
    if needs:
        body.append("")
        body.append(f"needs review ({len(needs)} column(s) with no witness yet): "
                    + ", ".join((f.data or {}).get("column", "?") for f in needs
                                if isinstance(f.data, dict)))
    sec.text = "\n".join(body) if rows else ""
    sec.llm = bool(llm_cols)
    sec.review = [{"id": f.id, "text": f.text, "reason": f.reason} for f in needs]
    _tally(sec, facts)
    if not rows:
        sec.empty_reason = "no column has a business meaning on record"
    return sec


def _glossary(fs: FactSet, llm: dict | None) -> Section:
    sec = Section("glossary", "Glossary terms")
    terms = fs.by_target("glossary.term")
    categories = fs.by_target("glossary.category")
    synonyms = fs.by_target("glossary.synonym")
    related = fs.by_target("glossary.related_term")
    by_term: dict[str, dict[str, Any]] = {}
    for f in terms:
        if f.include != COPY:
            continue
        data = dict(f.data or {})
        key = str(data.get("term") or f.text)
        row = by_term.setdefault(key, {
            "term": key, "definition": data.get("definition", ""),
            "category": data.get("category", ""), "status_note": data.get("status_note", ""),
            "synonyms": [], "related_terms": [], "related_entries": [],
            "contacts": [], "fact_ids": [], "witness": f.witness, "status": f.status})
        row["fact_ids"].append(f.id)
        for e in data.get("related_entries") or []:
            if e not in row["related_entries"]:
                row["related_entries"].append(e)
    for f in synonyms:
        if f.include != COPY:
            continue
        data = f.data or {}
        term = str(data.get("term") or "")
        row = by_term.get(term)
        if row is None:
            row = by_term.setdefault(term, {
                "term": term, "definition": data.get("expansion", ""),
                "category": "Business terms", "status_note": "", "synonyms": [],
                "related_terms": [], "related_entries": [], "contacts": [],
                "fact_ids": [], "witness": f.witness, "status": f.status})
        entry = f"{data.get('synonym')} (scope {data.get('scope')})"
        if entry not in row["synonyms"]:
            row["synonyms"].append(entry)
        row["fact_ids"].append(f.id)
    for f in related:
        if f.include != COPY:
            continue
        data = f.data or {}
        row = by_term.get(str(data.get("term")))
        if row is not None:
            row["related_terms"].append(
                f"{data.get('relation')} {data.get('related')}"
                + (f": {data['delta']}" if data.get("delta") else ""))
            row["fact_ids"].append(f.id)
    llm_terms = {g.get("term"): g for g in (llm or {}).get("glossary") or []}
    for term, row in by_term.items():
        if term in llm_terms and llm_terms[term].get("definition"):
            row["definition_llm"] = llm_terms[term]["definition"]
            row["llm_fact_ids"] = llm_terms[term].get("fact_ids", [])
    cats = []
    seen = set()
    for f in categories:
        data = f.data or {}
        key = (data.get("name"), data.get("parent"))
        if key in seen:
            continue
        seen.add(key)
        cats.append({**data, "fact_ids": [f.id]})
    rows = sorted(by_term.values(), key=lambda r: (r["category"], r["term"]))
    sec.items = [{"categories": cats, "terms": rows}]
    lines = ["# categories"]
    for c in cats:
        lines.append(f"- {c.get('name')}" + (f" (under {c['parent']})" if c.get("parent") else "")
                     + (f": {c['description']}" if c.get("description") else "")
                     + f" {_cite(c['fact_ids'])}")
    lines.append("")
    lines.append("# terms (category › term: definition · synonyms · related terms · related entries)")
    for r in rows:
        lines.append(f"- {r['category']} › {r['term']}: "
                     f"{r.get('definition_llm') or r['definition']}"
                     + (f" · synonyms: {'; '.join(r['synonyms'])}" if r["synonyms"] else "")
                     + (f" · related: {'; '.join(r['related_terms'])}" if r["related_terms"] else "")
                     + (f" · entries: {', '.join(r['related_entries'])}" if r["related_entries"] else "")
                     + f" [{r['witness']} · {r['status']}] {_cite(r['fact_ids'])}")
    sec.text = "\n".join(lines) if (rows or cats) else ""
    sec.llm = bool(llm_terms)
    review = [f for f in terms + synonyms + related if f.include == REVIEW]
    sec.review = [{"id": f.id, "text": f.text, "reason": f.reason} for f in review]
    _tally(sec, [f for f in terms + synonyms + related + categories if f.include == COPY])
    if not rows and not cats:
        sec.empty_reason = "no certified metric, mapped business term or witnessed concept on this table"
    return sec


def _related_entries(fs: FactSet) -> Section:
    sec = Section("related_entries", "Related entries")
    facts = fs.by_target("glossary.related_entry")
    rows = [{"term": (f.data or {}).get("term"), "target": (f.data or {}).get("target"),
             "note": (f.data or {}).get("mapping", {}), "fact_id": f.id,
             "witness": f.witness, "status": f.status}
            for f in facts if f.include == COPY]
    sec.items = rows
    sec.text = "\n".join(f"- {r['term']} → {r['target']} [{r['witness']} · {r['status']}] [{r['fact_id']}]"
                         for r in rows)
    sec.review = [{"id": f.id, "text": f.text, "reason": f.reason}
                  for f in facts if f.include == REVIEW]
    _tally(sec, [f for f in facts if f.include == COPY])
    if not rows:
        sec.empty_reason = "no certified or witnessed term ↔ asset link on record"
    return sec


def aspects_payload(fs: FactSet, cfg: KcConfig) -> dict[str, dict[str, Any]]:
    """The ``aspects`` map of an entries.patch body: one entry per
    aspect type, list fields appended, scalar fields set. Only facts
    with the copy verdict enter; pending statuses ride as fields."""
    out: dict[str, dict[str, Any]] = {}
    for f in fs.by_target("aspect."):
        if f.include != COPY:
            continue
        _a, atype, *rest = f.kc_target.split(".")
        field_path = rest
        data = out.setdefault(cfg.aspect_id(atype), {"data": {}})["data"]
        node = data
        for part in field_path[:-1]:
            node = node.setdefault(part, {})
        leaf = field_path[-1]
        value = f.data if f.data is not None else f.text
        listy = leaf in ("metrics", "bindings", "paths", "columns", "witnesses",
                         "tickets", "open_reviews", "upstream", "downstream",
                         "column_derivations", "used_by_lob", "query_templates",
                         "metric_runs", "declared_policies", "column_flags",
                         "type_disagreements", "columns_observed_via",
                         "status_notes", "notes", "values", "distinct_estimate",
                         "column_attributes")
        if listy:
            node.setdefault(leaf, [])
            if isinstance(value, list) and leaf in ("values", "columns", "witnesses"):
                node[leaf].extend(value)
            else:
                node[leaf].append(value)
        else:
            node[leaf] = value
    return out


def aspect_type_templates(cfg: KcConfig) -> list[dict[str, Any]]:
    """The create-once payloads: one ``metadataTemplate`` per custom
    aspect type, in the shape ``aspectTypes.create`` takes (record type
    with named fields). Field lists mirror the coverage rows."""
    fields: dict[str, list[tuple[str, str]]] = {
        "meridian-provenance": [
            ("build_id", "string"), ("graph_hash", "string"), ("graph_run", "string"),
            ("object_type", "string"), ("row_count", "int"), ("partitions", "int"),
            ("schema_fingerprint", "string"), ("schema_version", "string"),
            ("project", "string"), ("layer", "string"), ("environment", "string"),
            ("application_id", "string"), ("served_purpose", "string"),
            ("catalog_attributes", "map"), ("column_attributes", "array"),
            ("witnesses", "array")],
        "governed-metrics": [("metrics", "array"), ("status_notes", "array")],
        "concept-bindings": [("bindings", "array")],
        "join-paths": [("paths", "array")],
        "usage-profile": [
            ("distinct_users", "int"), ("peak_hours", "array"),
            ("bytes_per_query", "map"), ("used_by_lob", "array"),
            ("query_templates", "array"), ("metric_runs", "array")],
        "sensitivity-consensus": [
            ("policy", "map"), ("columns", "array"), ("declared_policies", "array"),
            ("declared", "map"), ("disagreements", "int")],
        "definition-status": [
            ("lifecycle", "string"), ("answerability", "string"), ("d_counts", "map"),
            ("census", "map"), ("tickets", "array"), ("open_reviews", "array"),
            ("concept_conflicts", "array"), ("metric_conflicts", "array"),
            ("column_flags", "array"), ("type_disagreements", "array"),
            ("columns_observed_via", "array")],
        "lineage-notes": [
            ("feed_type", "string"), ("source_system", "string"), ("pipeline", "string"),
            ("catalog_declares_lineage", "bool"), ("upstream", "array"),
            ("downstream", "array"), ("column_derivations", "array"),
            ("view_sql", "string"), ("notes", "array")],
        "value-domain": [("values", "array"), ("distinct_estimate", "array")],
    }
    out = []
    for name in ASPECT_TYPES:
        record_fields = []
        for i, (fname, ftype) in enumerate(fields[name], 1):
            spec: dict[str, Any] = {"name": fname, "index": i,
                                    "annotations": {"displayName": fname.replace("_", " ")}}
            if ftype == "array":
                spec["type"] = "array"
                spec["arrayItems"] = {"name": f"{fname}_item", "type": "string"}
            elif ftype == "map":
                spec["type"] = "map"
                spec["mapItems"] = {"name": f"{fname}_value", "type": "string"}
            else:
                spec["type"] = ftype
            record_fields.append(spec)
        out.append({
            "aspect_type_id": cfg.aspect_types.get(name, name),
            "display_name": name.replace("-", " "),
            "description": f"Synapse enrichment: {name.replace('-', ' ')} facts, "
                           "each carrying witness and status",
            "metadataTemplate": {"name": name.replace("-", "_"), "type": "record",
                                 "recordFields": record_fields}})
    return out


def _aspects(fs: FactSet, cfg: KcConfig) -> Section:
    sec = Section("aspects", "Aspects")
    payload = aspects_payload(fs, cfg)
    facts = [f for f in fs.by_target("aspect.") if f.include == COPY]
    sec.items = [{"aspect": k, **v} for k, v in payload.items()]
    sec.text = json.dumps({"aspects": payload}, indent=1, ensure_ascii=False,
                          sort_keys=True) if payload else ""
    sec.review = [{"id": f.id, "text": f.text, "reason": f.reason}
                  for f in fs.by_target("aspect.") if f.include == REVIEW]
    _tally(sec, facts)
    if not payload:
        sec.empty_reason = "no structured fact on record"
    return sec


def _dq(fs: FactSet) -> Section:
    sec = Section("dq", "DQ rules")
    facts = [f for f in fs.by_target("dq.") if f.include == COPY]
    rows = [{"rule": f.kc_target.split(".", 1)[1], "text": f.text,
             "evidence": f.text, **(f.data or {}), "fact_id": f.id,
             "witness": f.witness, "status": f.status} for f in facts]
    sec.items = rows
    sec.text = "\n".join(f"- {r['rule']}: {r['text']} [{r['witness']} · {r['status']}] [{r['fact_id']}]"
                         for r in rows)
    _tally(sec, facts)
    if not rows:
        sec.empty_reason = "no declared key, profile, domain or partition evidence to propose a rule from"
    return sec


def _queries(fs: FactSet) -> Section:
    sec = Section("queries", "Queries")
    facts = fs.by_target("query")
    rows = [{"description": (f.data or {}).get("description"),
             "sql": (f.data or {}).get("sql"), "source": "User",
             "kind": (f.data or {}).get("kind"), "fact_id": f.id,
             "witness": f.witness, "status": f.status}
            for f in facts if f.include == COPY]
    sec.items = rows
    sec.text = "\n\n".join(f"-- {r['description']} [{r['witness']} · {r['status']}] [{r['fact_id']}]\n{r['sql']}"
                           for r in rows)
    sec.review = [{"id": f.id, "text": f.text, "reason": f.reason}
                  for f in facts if f.include == REVIEW]
    _tally(sec, [f for f in facts if f.include == COPY])
    if not rows:
        sec.empty_reason = "no certified metric SQL, view definition or attested query on record"
    return sec


def _contacts(fs: FactSet) -> Section:
    sec = Section("contacts", "Contacts")
    facts = [f for f in fs.by_target("contact") if f.include == COPY]
    seen = set()
    rows = []
    for f in facts:
        data = f.data or {}
        key = (data.get("role"), data.get("name"))
        if key in seen:
            continue
        seen.add(key)
        rows.append({"role": data.get("role"), "name": data.get("name"),
                     "metric": data.get("metric", ""), "fact_id": f.id,
                     "witness": f.witness, "status": f.status})
    sec.items = rows
    sec.text = "\n".join(f"- {r['role']}: {r['name']}"
                         + (f" (for {r['metric']})" if r.get("metric") else "")
                         + f" [{r['witness']} · {r['status']}] [{r['fact_id']}]" for r in rows)
    _tally(sec, facts)
    if not rows:
        sec.empty_reason = "no owner, steward or line of business on record"
    return sec


def _review(fs: FactSet) -> Section:
    sec = Section("review", "Needs review")
    facts = [f for f in fs.facts if f.include in (REVIEW, NEVER)
             and not f.kc_target.startswith("suggestion.")]
    rows = [{"id": f.id, "verdict": f.include, "target": f.kc_target,
             "text": f.text, "reason": f.reason, "witness": f.witness,
             "status": f.status} for f in facts]
    sec.items = rows
    sec.text = "\n".join(f"- [{r['verdict']}] {r['target']}: {r['text']} — {r['reason']} [{r['id']}]"
                         for r in rows)
    sec.facts_used = [f.id for f in facts]
    counts: dict[str, int] = defaultdict(int)
    for f in facts:
        counts[f.include] += 1
    sec.status_counts = dict(counts)
    if not rows:
        sec.empty_reason = "nothing withheld: every fact on this table may be copied"
    return sec


def suggestions(fs: FactSet) -> list[dict[str, Any]]:
    """The showcase: proposals grouped by kind, each with rationale,
    evidence and why it is not in a copy block by default."""
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    titles = {"metric": "Proposed metrics (pending and mined, ranked by support × agreement)",
              "glossary_term": "Proposed glossary terms",
              "join": "Proposed joins (candidates: the tables appear together)",
              "column_description": "Columns with no meaning on record",
              "metric_definition": "Model-suggested definitions (unreviewed)",
              "enrichment_run": "Enrichment context"}
    for f in fs.facts:
        if not f.kc_target.startswith("suggestion."):
            continue
        kind = f.kc_target.split(".", 1)[1]
        groups[kind].append({
            "id": f.id, "text": f.text, "why_not_default": f.reason,
            "witness": f.witness, "status": f.status, "evidence": f.prov,
            "data": f.data, "showcase": True,
            "rank": (f.data or {}).get("rank", 0) if isinstance(f.data, dict) else 0})
    out = []
    for kind, items in groups.items():
        items.sort(key=lambda i: (-i["rank"], i["text"]))
        out.append({"kind": kind, "title": titles.get(kind, kind), "items": items})
    out.sort(key=lambda g: list(titles).index(g["kind"]) if g["kind"] in titles else 99)
    return out


def ledger(fs: FactSet) -> list[dict[str, Any]]:
    """The translation ledger: Meridian object (kind + count) → catalog
    construct → statuses flowing → representation → translated count →
    withheld count with reasons. Every fact lands in exactly one row,
    so the rows sum to the fact count."""
    cells: dict[tuple[str, str], dict[str, Any]] = {}
    for f in fs.facts:
        construct = construct_of(f.kc_target)
        row = cells.setdefault((f.kind, construct), {
            "meridian_kind": f.kind, "kc_construct": construct,
            "representation": f.representation, "statuses": set(),
            "translated": 0, "review": 0, "withheld": 0,
            "reasons": defaultdict(int), "fact_ids": [], "coverage_items": set()})
        row["statuses"].add(f.status)
        row["fact_ids"].append(f.id)
        if f.coverage_item:
            row["coverage_items"].add(f.coverage_item)
        if f.include == COPY:
            row["translated"] += 1
        elif f.include == REVIEW:
            row["review"] += 1
            row["reasons"][f.reason or "review"] += 1
        else:
            row["withheld"] += 1
            row["reasons"][f.reason or "withheld"] += 1
    out = []
    for (_kind, _construct), row in sorted(cells.items()):
        out.append({**row, "count": len(row["fact_ids"]),
                    "statuses": sorted(row["statuses"]),
                    "reasons": dict(row["reasons"]),
                    "coverage_items": sorted(row["coverage_items"])})
    return out


def construct_of(target: str) -> str:
    head, *rest = target.split(".")
    if head == "entry" and rest:
        return "Entry description" if rest[0] == "description" \
            else f"Entry overview § {dict(OVERVIEW_SECTIONS).get(rest[1], rest[1]) if len(rest) > 1 else ''}".strip()
    if head == "column":
        return "Column description" if rest and rest[0] == "description" else "Column sensitivity"
    if head == "glossary":
        return {"term": "Glossary term", "category": "Glossary category",
                "synonym": "Synonym", "related_term": "Related term",
                "related_entry": "Related entry"}.get(rest[0] if rest else "", "Glossary")
    if head == "aspect" and len(rest) >= 2:
        return f"Aspect {rest[0]}.{rest[1]}"
    if head == "dq":
        return f"DQ rule ({rest[0]})" if rest else "DQ rule"
    if head == "query":
        return "Query"
    if head == "contact":
        return "Contact"
    if head == "suggestion":
        return f"Suggestion ({rest[0]})" if rest else "Suggestion"
    return target


def push_section(records: list[dict[str, Any]], sections: dict[str, Section],
                 build_id: str) -> Section:
    sec = Section("push", "Push record")
    hashes = {k: hashlib.sha256((s.text or "").encode("utf-8")).hexdigest()[:12]
              for k, s in sections.items() if k not in ("push",)}
    manifest = {"build_id": build_id, "sections": sorted(hashes),
                "hashes": hashes}
    sec.items = [manifest] + records
    sec.text = json.dumps(manifest, indent=1, sort_keys=True)
    if records:
        latest = records[-1]
        sec.status_counts = {"pushed": len(records)}
        sec.paragraphs = [{"text": f"last pushed {latest.get('ts', '')} by "
                                   f"{latest.get('actor', '?')} against build "
                                   f"{latest.get('build', '?')}", "fact_ids": []}]
    else:
        sec.empty_reason = "nothing recorded as entered into the catalog yet"
    return sec


def render_sections(fs: FactSet, cfg: KcConfig, llm: dict | None = None,
                    gate_tier: str = "", push_records: list[dict] | None = None
                    ) -> dict[str, Section]:
    sections = {
        "description": _description(fs, llm),
        "overview": _overview(fs, llm, gate_tier),
        "columns": _columns(fs, llm),
        "glossary": _glossary(fs, llm),
        "related_entries": _related_entries(fs),
        "aspects": _aspects(fs, cfg),
        "dq": _dq(fs),
        "queries": _queries(fs),
        "contacts": _contacts(fs),
        "review": _review(fs),
    }
    sections["push"] = push_section(push_records or [], sections, fs.build_id)
    return sections


def guide(cfg: KcConfig) -> list[dict[str, str]]:
    out = []
    for g in GUIDE:
        entry = dict(g)
        if g["section"] == "glossary" and cfg.glossary_path():
            entry["path"] = entry["path"].replace("your glossary", cfg.glossary_path())
        out.append(entry)
    return out
