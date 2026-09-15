"""Exports: the bundle in the shapes a person or an import job takes.

Formats: ``md`` (every copy-ready section as one document), ``json``
(facts, sections, ledger, aspect payloads and the import-shaped files),
``csv`` (the flat review sheet: one fact per row), ``sheet`` (the
glossary in the columns of the catalog's glossary import sheet), and
``zip`` (all of the above plus the JSONL import files).

Import shapes encoded here, from the catalog's metadata-import format:

* Glossary import file: one JSON object per line, ``{"entry": {...}}``
  for a term or category — ``name`` (the full resource name under the
  glossary's entry group), ``entryType`` (``projects/dataplex-types/
  locations/global/entryTypes/glossary-term`` or ``glossary-category``),
  ``parentEntry``, ``entrySource`` (``displayName``, ``description``),
  and ``aspects`` keyed ``dataplex-types.global.glossary-term-aspect``
  (or ``glossary-category-aspect``), plus ``overview`` and ``contacts``
  system aspects.
* Entry links: ``{"entryLink": {"name", "entryLinkType", "entryReferences":
  [{"name", "type": "SOURCE"|"TARGET"}]}}`` with link types ``definition``
  (term → asset), ``synonym`` and ``related`` under
  ``projects/dataplex-types/locations/global/entryLinkTypes/``.
* Aspects on an entry: the ``aspects`` map of an ``entries.patch`` body,
  keyed ``PROJECT.LOCATION.ASPECT_TYPE_ID`` with a ``data`` object.
* Glossary sheet import: the columns ``Term``, ``Description``,
  ``Overview``, ``Category``, ``Synonyms``, ``Related Terms``,
  ``Contacts``, ``Related Entries`` (semicolon-separated lists).

The documentation pages were not reachable from the environment this
was written in, so these shapes follow the documented format as of
the last read and every file carries a header naming the page to
verify against before the first import:
docs.cloud.google.com/dataplex/docs/import-glossaries-entrylinks-json,
.../manage-glossaries, .../create-aspect-types, .../import-metadata.
"""

from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from typing import TYPE_CHECKING, Any

from sahs.kc.assemble import COPY
from sahs.kc.config import KcConfig

if TYPE_CHECKING:
    from sahs.kc.bundle import Bundle

DOCS = {
    "glossary_json": "https://docs.cloud.google.com/dataplex/docs/import-glossaries-entrylinks-json",
    "glossary_sheet": "https://docs.cloud.google.com/dataplex/docs/manage-glossaries",
    "aspect_types": "https://docs.cloud.google.com/dataplex/docs/create-aspect-types",
    "import_jobs": "https://docs.cloud.google.com/dataplex/docs/import-metadata",
}
_TYPES = "projects/dataplex-types/locations/global"
SHEET_COLUMNS = ("Term", "Description", "Overview", "Category", "Synonyms",
                 "Related Terms", "Contacts", "Related Entries")


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(text).lower()).strip("-")[:63] or "term"


def _glossary_rows(bundle: "Bundle") -> tuple[list[dict], list[dict]]:
    sec = bundle.sections["glossary"]
    if not sec.items:
        return [], []
    return sec.items[0].get("categories", []), sec.items[0].get("terms", [])


def bigquery_entry_name(cfg: KcConfig, physical: str) -> str:
    """The catalog's name for a BigQuery table entry: the system entry
    group ``@bigquery`` in the table's project. ``physical`` is
    dataset.table; the project is the configured one when set."""
    dataset, table = (physical.split(".", 1) + [""])[:2]
    project = cfg.project or "PROJECT"
    return (f"projects/{project}/locations/{cfg.location}/entryGroups/@bigquery/"
            f"entries/bigquery.googleapis.com/projects/{project}/datasets/"
            f"{dataset}/tables/{table}")


def glossary_import_jsonl(bundle: "Bundle", cfg: KcConfig) -> str:
    glossary = cfg.glossary_path() or "projects/PROJECT/locations/LOCATION/glossaries/GLOSSARY"
    group = glossary.replace("/glossaries/", "/entryGroups/@dataplex/entries/")
    categories, terms = _glossary_rows(bundle)
    lines = [json.dumps({"_comment": f"verify the shape against {DOCS['glossary_json']} "
                                      "before the first import job",
                         "build": bundle.build_id, "table": bundle.table})]
    seen_cats = set()
    for c in categories + [{"name": t["category"]} for t in terms]:
        name = c.get("name")
        if not name or name in seen_cats:
            continue
        seen_cats.add(name)
        entry = {"entry": {
            "name": f"{group}/categories/{_slug(name)}",
            "entryType": f"{_TYPES}/entryTypes/glossary-category",
            "parentEntry": (f"{group}/categories/{_slug(c['parent'])}"
                            if c.get("parent") else group),
            "entrySource": {"displayName": name,
                            "description": c.get("description", "")},
            "aspects": {"dataplex-types.global.glossary-category-aspect": {"data": {}}}}}
        lines.append(json.dumps(entry, ensure_ascii=False))
    for t in terms:
        overview = t.get("definition_llm") or ""
        entry = {"entry": {
            "name": f"{group}/terms/{_slug(t['term'])}",
            "entryType": f"{_TYPES}/entryTypes/glossary-term",
            "parentEntry": f"{group}/categories/{_slug(t['category'])}",
            "entrySource": {"displayName": t["term"],
                            "description": t.get("definition", "")},
            "aspects": {
                "dataplex-types.global.glossary-term-aspect": {"data": {}},
                **({"dataplex-types.global.overview": {"data": {"content": overview}}}
                   if overview else {}),
                **({"dataplex-types.global.contacts": {"data": {"identities": [
                    {"role": "steward", "name": c} for c in t.get("contacts", [])]}}}
                   if t.get("contacts") else {})}}}
        lines.append(json.dumps(entry, ensure_ascii=False))
    return "\n".join(lines) + "\n"


def entry_links_jsonl(bundle: "Bundle", cfg: KcConfig) -> str:
    glossary = cfg.glossary_path() or "projects/PROJECT/locations/LOCATION/glossaries/GLOSSARY"
    group = glossary.replace("/glossaries/", "/entryGroups/@dataplex/entries/")
    link_group = glossary.rsplit("/glossaries/", 1)[0] + "/entryGroups/@dataplex"
    lines = [json.dumps({"_comment": f"verify against {DOCS['glossary_json']}",
                         "build": bundle.build_id, "table": bundle.table})]
    n = 0
    for r in bundle.sections["related_entries"].items:
        n += 1
        target = str(r.get("target") or "")
        physical, _dot, column = target.partition(".") if target.count(".") >= 2 \
            else (target, "", "")
        if target.count(".") >= 2:
            physical = ".".join(target.split(".")[:2])
            column = ".".join(target.split(".")[2:])
        asset = bigquery_entry_name(cfg, physical)
        lines.append(json.dumps({"entryLink": {
            "name": f"{link_group}/entryLinks/{_slug(r['term'])}-{_slug(target)}-{n}",
            "entryLinkType": f"{_TYPES}/entryLinkTypes/definition",
            "entryReferences": [
                {"name": f"{group}/terms/{_slug(r['term'])}", "type": "SOURCE"},
                {"name": asset, "type": "TARGET",
                 **({"path": column} if column else {})}]}}, ensure_ascii=False))
    _cats, terms = _glossary_rows(bundle)
    for t in terms:
        for s in t.get("synonyms", []):
            n += 1
            lines.append(json.dumps({"entryLink": {
                "name": f"{link_group}/entryLinks/syn-{_slug(t['term'])}-{n}",
                "entryLinkType": f"{_TYPES}/entryLinkTypes/synonym",
                "entryReferences": [
                    {"name": f"{group}/terms/{_slug(t['term'])}", "type": "SOURCE"},
                    {"name": f"{group}/terms/{_slug(s.split(' (scope')[0])}",
                     "type": "TARGET"}]}}, ensure_ascii=False))
        for rel in t.get("related_terms", []):
            n += 1
            other = rel.split(" ", 2)[-1].split(":")[0]
            lines.append(json.dumps({"entryLink": {
                "name": f"{link_group}/entryLinks/rel-{_slug(t['term'])}-{n}",
                "entryLinkType": f"{_TYPES}/entryLinkTypes/related",
                "entryReferences": [
                    {"name": f"{group}/terms/{_slug(t['term'])}", "type": "SOURCE"},
                    {"name": f"{group}/terms/{_slug(other)}", "type": "TARGET"}]}},
                ensure_ascii=False))
    return "\n".join(lines) + "\n"


def entry_patch_payload(bundle: "Bundle", cfg: KcConfig) -> dict[str, Any]:
    """The ``entries.patch`` body for the table entry: description,
    the overview and contacts system aspects, and every custom aspect.
    Column descriptions ride as ``schema`` aspect fields."""
    sections = bundle.sections
    columns = [{"name": r["column"], "description":
                (r.get("description_llm") or r.get("description") or "")}
               for r in sections["columns"].items
               if (r.get("description_llm") or r.get("description"))]
    aspects = dict(bundle.aspects)
    if sections["overview"].text:
        aspects["dataplex-types.global.overview"] = {
            "data": {"content": sections["overview"].text}}
    if sections["contacts"].items:
        aspects["dataplex-types.global.contacts"] = {"data": {"identities": [
            {"role": r["role"], "name": r["name"]} for r in sections["contacts"].items]}}
    if sections["queries"].items:
        aspects["dataplex-types.global.queries"] = {"data": {"queries": [
            {"sql": r["sql"], "description": r["description"], "source": "User"}
            for r in sections["queries"].items]}}
    return {
        "_comment": f"verify against {DOCS['aspect_types']} and {DOCS['import_jobs']}",
        "name": bigquery_entry_name(cfg, bundle.table),
        "updateMask": "entrySource.description,aspects",
        "entrySource": {"description": sections["description"].text},
        "schema_column_descriptions": columns,
        "aspects": aspects,
    }


def markdown(bundle: "Bundle") -> str:
    lines = [f"# {bundle.table} — Knowledge Catalog enrichment bundle",
             f"build {bundle.build_id} · graph run {bundle.graph_run} · "
             f"prompt {bundle.prompt_version} · digest {bundle.digest}", ""]
    for key, sec in bundle.sections.items():
        lines.append(f"## {sec.title}")
        if sec.gate_tier:
            lines.append(f"_gate: {sec.gate_tier}_")
        if sec.text:
            lines.append(sec.text)
        else:
            lines.append(f"_(empty: {sec.empty_reason})_")
        if sec.review:
            lines.append("")
            lines.append(f"needs review: {len(sec.review)}")
        lines.append("")
    lines.append("## Facts")
    for f in bundle.facts:
        lines.append(f"- [{f['id']}] ({f['include']}) {f['text']} "
                     f"[{f['witness']} · {f['status']}]")
    return "\n".join(lines) + "\n"


def review_csv(bundle: "Bundle") -> str:
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["fact_id", "kind", "kc_target", "include", "reason", "status",
                     "witness", "text", "evidence"])
    for f in bundle.facts:
        writer.writerow([f["id"], f["kind"], f["kc_target"], f["include"],
                         f["reason"], f["status"], f["witness"], f["text"],
                         json.dumps(f["prov"], ensure_ascii=False)])
    return out.getvalue()


def glossary_sheet_csv(bundle: "Bundle") -> str:
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(SHEET_COLUMNS)
    _cats, terms = _glossary_rows(bundle)
    for t in terms:
        writer.writerow([t["term"], t.get("definition", ""),
                         t.get("definition_llm", ""), t.get("category", ""),
                         "; ".join(t.get("synonyms", [])),
                         "; ".join(t.get("related_terms", [])),
                         "; ".join(t.get("contacts", [])),
                         "; ".join(t.get("related_entries", []))])
    return out.getvalue()


def as_json(bundle: "Bundle", cfg: KcConfig) -> dict[str, Any]:
    payload = bundle.to_dict()
    payload["imports"] = {
        "glossary_jsonl": glossary_import_jsonl(bundle, cfg),
        "entry_links_jsonl": entry_links_jsonl(bundle, cfg),
        "entry_patch": entry_patch_payload(bundle, cfg),
        "docs": DOCS}
    return payload


def export(bundle: "Bundle", fmt: str, cfg: KcConfig
           ) -> tuple[bytes, str, str]:
    """→ (bytes, media type, filename)."""
    slug = bundle.table.replace(".", "__")
    if fmt == "md":
        return markdown(bundle).encode("utf-8"), "text/markdown", f"{slug}.kc.md"
    if fmt == "json":
        return (json.dumps(as_json(bundle, cfg), indent=1, ensure_ascii=False,
                           sort_keys=True).encode("utf-8"),
                "application/json", f"{slug}.kc.json")
    if fmt == "csv":
        return review_csv(bundle).encode("utf-8"), "text/csv", f"{slug}.kc.csv"
    if fmt == "sheet":
        return (glossary_sheet_csv(bundle).encode("utf-8"), "text/csv",
                f"{slug}.glossary.csv")
    if fmt == "zip":
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{slug}/bundle.md", markdown(bundle))
            zf.writestr(f"{slug}/bundle.json", json.dumps(
                as_json(bundle, cfg), indent=1, ensure_ascii=False, sort_keys=True))
            zf.writestr(f"{slug}/facts.csv", review_csv(bundle))
            zf.writestr(f"{slug}/glossary_sheet.csv", glossary_sheet_csv(bundle))
            zf.writestr(f"{slug}/glossary_import.jsonl", glossary_import_jsonl(bundle, cfg))
            zf.writestr(f"{slug}/entry_links.jsonl", entry_links_jsonl(bundle, cfg))
            zf.writestr(f"{slug}/entry_patch.json", json.dumps(
                entry_patch_payload(bundle, cfg), indent=1, ensure_ascii=False))
            zf.writestr(f"{slug}/aspect_types.json", json.dumps(
                bundle.aspect_types, indent=1, ensure_ascii=False))
        return buffer.getvalue(), "application/zip", f"{slug}.kc.zip"
    raise ValueError(f"unknown export format {fmt!r}: md, json, csv, sheet, zip")
