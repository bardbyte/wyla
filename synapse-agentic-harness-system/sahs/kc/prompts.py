"""Prompt posture ``kc.1`` — the model writes prose from facts and adds
nothing. Versioned so a wording change is a diff and a gate re-run.

Every prompt lists the facts as ``[fN] text [witness · status]`` and
demands ONE strict JSON object. Rules the verifier enforces afterwards
are stated here too, so the model and the check agree on the contract:
every sentence ends with the ids of the facts it rests on; no table,
column, metric, value or number that is not in the facts; a pending or
unreviewed status is said inside the sentence; an empty string when
the facts do not support a section."""

from __future__ import annotations

from typing import Iterable

from sahs.kc.assemble import Fact

PROMPT_VERSION = "kc.1"

SYSTEM = (
    "You are a careful data steward writing catalog documentation for a "
    "BigQuery table. You write ONLY from the numbered facts you are given. "
    "Every sentence you write ends with the ids of the facts it rests on, "
    "in square brackets, like [f12,f40]. You never introduce a table, "
    "column, metric, value or number that does not appear in the facts. "
    "When a fact is marked pending, mined, contested or unreviewed, you say "
    "so inside the sentence that uses it. When the facts are insufficient "
    "for a section you return an empty string for it. You answer with ONE "
    "JSON object and nothing else.")

OUTPUT_SCHEMA = {
    "overview_sections": {"<section_key>": {"text": "prose with [fN] citations",
                                            "fact_ids": ["f1"]}},
    "description": "one or two sentences with citations",
    "description_fact_ids": ["f1"],
    "columns": [{"name": "column_name", "text": "one sentence [fN]",
                 "fact_ids": ["f1"]}],
    "glossary": [{"term": "display name", "definition": "one or two sentences [fN]",
                  "synonyms": [], "fact_ids": ["f1"], "status_note": ""}],
    "confidence": 0.0,
    "caveat": "",
}


def fact_lines(facts: Iterable[Fact]) -> str:
    return "\n".join(f"[{f.id}] {f.text} [{f.witness or '?'} · {f.status}]"
                     for f in facts)


def overview_prompt(table: str, facts: list[Fact],
                    sections: list[tuple[str, str]]) -> str:
    keys = "\n".join(f"- {k}: {title}" for k, title in sections)
    return (
        f"Table: {table}\n\nFacts (the only source you may use):\n"
        f"{fact_lines(facts)}\n\n"
        "Write the catalog entry's DESCRIPTION (at most two sentences: purpose, "
        "grain, and status posture) and an OVERVIEW with these sections, in "
        f"this order:\n{keys}\n\n"
        "Rules: one short paragraph per section; every sentence ends with its "
        "fact ids like [f3,f9]; skip a section (empty string) when no fact "
        "supports it; never restate a number or a name that is not in the "
        "facts; say 'pending', 'mined', 'contested' or 'unreviewed' inside "
        "any sentence that uses such a fact; write plain prose, no lists.\n\n"
        "Return exactly this JSON object:\n"
        '{"description": "...", "description_fact_ids": ["f1"], '
        '"overview_sections": {"purpose": {"text": "...", "fact_ids": ["f1"]}, '
        '...}, "confidence": 0.0, "caveat": ""}')


def columns_prompt(table: str, facts: list[Fact]) -> str:
    return (
        f"Table: {table}\n\nColumn facts (the only source you may use):\n"
        f"{fact_lines(facts)}\n\n"
        "For each column named in the facts, write ONE sentence describing "
        "what the column holds, in the register of a data catalog. Use the "
        "column's business name and its meaning when both are present. End "
        "every sentence with its fact ids like [f5]. Skip a column when no "
        "fact gives its meaning. Never invent a type, value or code.\n\n"
        "Return exactly this JSON object:\n"
        '{"columns": [{"name": "col", "text": "... [f5]", "fact_ids": ["f5"]}], '
        '"confidence": 0.0, "caveat": ""}')


def glossary_prompt(table: str, facts: list[Fact]) -> str:
    return (
        f"Table: {table}\n\nGlossary facts (the only source you may use):\n"
        f"{fact_lines(facts)}\n\n"
        "For each term named in the facts, write a definition of one or two "
        "sentences a business reader understands: what question it answers, "
        "at what grain, and how it is calculated when the facts say. Keep the "
        "term's status inside the definition when it is pending or "
        "candidate. End every sentence with its fact ids like [f7,f8]. "
        "Never add a synonym that is not in the facts.\n\n"
        "Return exactly this JSON object:\n"
        '{"glossary": [{"term": "...", "definition": "... [f7]", "synonyms": [], '
        '"fact_ids": ["f7"], "status_note": ""}], "confidence": 0.0, "caveat": ""}')


def gate_prompt(table: str, facts: list[Fact], targets: list[str]) -> str:
    """The blind exam: describe the table and the named columns from the
    facts alone; the human descriptions were withheld from ``facts``."""
    return (
        f"Table: {table}\n\nFacts (the only source you may use):\n"
        f"{fact_lines(facts)}\n\n"
        "Write a one-sentence description of the table and a one-sentence "
        f"description of each of these columns: {', '.join(targets)}. End "
        "each sentence with its fact ids. Use only the facts.\n\n"
        "Return exactly this JSON object:\n"
        '{"description": "...", "columns": [{"name": "col", "text": "..."}], '
        '"confidence": 0.0, "caveat": ""}')
