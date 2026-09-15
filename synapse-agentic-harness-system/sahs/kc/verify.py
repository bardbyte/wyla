"""The deterministic verifier — no model, no memory, one pass.

A sentence the model wrote survives only when: it cites at least one
fact id that exists; every identifier it uses (table, column, metric
name, code) appears in the cited facts or the fact set; every number it
uses appears there too; a status word it uses (certified, pending,
mined, contested, unreviewed) matches the status of a cited fact; and
it carries no forbidden content (a person's variant, an anti-alias, a
gold prompt). A failing sentence drops to "needs review" with the
reason; nothing unverified reaches a copy block."""

from __future__ import annotations

import re
from typing import Any

from sahs.kc.assemble import FactSet

_CITE = re.compile(r"\[(f\d+(?:\s*,\s*f\d+)*)\]")
_SENTENCE = re.compile(r"(?<=[.!?])\s+(?=[A-Z\[`(\"'])")
_IDENT = re.compile(r"\b[a-z][a-z0-9]*(?:[_.][a-z0-9]+)+\b")
_NUMBER = re.compile(r"(?<![\w.])\d[\d,]*(?:\.\d+)?%?(?![\w])")
_STATUS_WORDS = {
    "certified": {"certified"}, "pending": {"pending", "team candidate"},
    "mined": {"mined"}, "contested": {"contested"},
    "unreviewed": {"unreviewed"}, "deprecated": {"deprecated"},
    "rejected": {"rejected"}}
_FORBIDDEN = ("user_variant", "anti-alias", "gold prompt", "answer key")


def _cites(sentence: str) -> list[str]:
    ids: list[str] = []
    for m in _CITE.finditer(sentence):
        ids.extend(x.strip() for x in m.group(1).split(","))
    return ids


def _strip_cites(sentence: str) -> str:
    return _CITE.sub("", sentence)


def _corpus(fs: FactSet, ids: list[str]) -> tuple[str, str]:
    cited = " ".join(f.text for f in fs.facts if f.id in ids)
    everything = " ".join(f.text for f in fs.facts) + " " + fs.table
    return cited.lower(), everything.lower()


def verify_text(text: str, fs: FactSet, forbidden: tuple[str, ...] = ()
                ) -> dict[str, Any]:
    """→ {kept: [sentence], dropped: [{sentence, reason}], text, fact_ids}."""
    kept: list[str] = []
    dropped: list[dict[str, str]] = []
    used: list[str] = []
    banned = tuple(_FORBIDDEN) + tuple(forbidden)
    for raw in _SENTENCE.split((text or "").strip()):
        sentence = raw.strip()
        if not sentence:
            continue
        ids = _cites(sentence)
        known = [i for i in ids if fs.get(i) is not None]
        if not ids:
            dropped.append({"sentence": sentence, "reason": "no fact citation"})
            continue
        if len(known) != len(ids):
            dropped.append({"sentence": sentence,
                            "reason": f"cites unknown fact id(s): "
                                      f"{', '.join(i for i in ids if i not in known)}"})
            continue
        body = _strip_cites(sentence)
        lower = body.lower()
        cited, everything = _corpus(fs, known)
        bad = next((b for b in banned if b.lower() in lower), None)
        if bad:
            dropped.append({"sentence": sentence, "reason": f"forbidden content: {bad}"})
            continue
        foreign = [i for i in _IDENT.findall(lower)
                   if i not in everything and not i.startswith("f")]
        if foreign:
            dropped.append({"sentence": sentence,
                            "reason": f"identifier not in the facts: {', '.join(sorted(set(foreign))[:4])}"})
            continue
        numbers = [n.rstrip("%").replace(",", "") for n in _NUMBER.findall(body)]
        foreign_n = [n for n in numbers
                     if n not in everything.replace(",", "") and n.lstrip("0") not in everything]
        if foreign_n:
            dropped.append({"sentence": sentence,
                            "reason": f"number not in the facts: {', '.join(sorted(set(foreign_n)))}"})
            continue
        cited_statuses = {f.status for f in fs.facts if f.id in known}
        mismatch = ""
        for word, statuses in _STATUS_WORDS.items():
            if re.search(rf"\b{word}\b", lower) and not (statuses & cited_statuses):
                # the word may describe another cited fact's text (e.g. a
                # certified metric named in the fact itself)
                if word not in cited:
                    mismatch = word
                    break
        if mismatch:
            dropped.append({"sentence": sentence,
                            "reason": f"status word '{mismatch}' does not match the cited facts"})
            continue
        kept.append(sentence)
        used.extend(i for i in known if i not in used)
    return {"kept": kept, "dropped": dropped, "text": " ".join(kept),
            "fact_ids": used}


def verify_output(raw: dict[str, Any], fs: FactSet) -> dict[str, Any]:
    """Verify a full writer output; keep the schema, drop what fails.
    ``dropped`` collects every rejected sentence with its reason."""
    out: dict[str, Any] = {"overview_sections": {}, "columns": [], "glossary": [],
                           "description": "", "description_fact_ids": [],
                           "dropped": [], "confidence": raw.get("confidence"),
                           "caveat": raw.get("caveat", "")}
    desc = verify_text(str(raw.get("description") or ""), fs)
    out["description"] = desc["text"]
    out["description_fact_ids"] = desc["fact_ids"]
    out["dropped"] += [{"section": "description", **d} for d in desc["dropped"]]
    for key, para in (raw.get("overview_sections") or {}).items():
        text = para.get("text") if isinstance(para, dict) else str(para or "")
        checked = verify_text(str(text or ""), fs)
        if checked["text"]:
            out["overview_sections"][key] = {"text": checked["text"],
                                             "fact_ids": checked["fact_ids"]}
        out["dropped"] += [{"section": f"overview.{key}", **d} for d in checked["dropped"]]
    for col in raw.get("columns") or []:
        if not isinstance(col, dict) or not col.get("name"):
            continue
        checked = verify_text(str(col.get("text") or ""), fs)
        if checked["text"]:
            out["columns"].append({"name": col["name"], "text": checked["text"],
                                   "fact_ids": checked["fact_ids"]})
        out["dropped"] += [{"section": f"columns.{col['name']}", **d}
                           for d in checked["dropped"]]
    known_synonyms = {str((f.data or {}).get("synonym") or "").lower()
                      for f in fs.by_target("glossary.synonym")
                      if isinstance(f.data, dict)}
    for term in raw.get("glossary") or []:
        if not isinstance(term, dict) or not term.get("term"):
            continue
        checked = verify_text(str(term.get("definition") or ""), fs)
        synonyms = [s for s in term.get("synonyms") or []
                    if str(s).lower() in known_synonyms]
        if checked["text"]:
            out["glossary"].append({"term": term["term"], "definition": checked["text"],
                                    "synonyms": synonyms, "fact_ids": checked["fact_ids"],
                                    "status_note": term.get("status_note", "")})
        out["dropped"] += [{"section": f"glossary.{term['term']}", **d}
                           for d in checked["dropped"]]
    return out
