"""Skills and knowledge files, authored by a person with the model's
help (Synapse v3): the raw material a person has — notes, a runbook,
a pasted email, a converted workbook — is rewritten by the model into
the house format, previewed, and saved where the agent reads it.

Two formats, one discipline:

  * **a skill** is doctrine: the moves for a kind of ask and the
    checks each move must run. It steers where the agent looks; it
    never asserts a fact. Saved under ``<graph>/skills/users/<owner>/``
    it loads for that owner alone, labelled unreviewed.
  * **a knowledge file** is reference: definitions, tables and
    columns named exactly, metrics in words, caveats, the owner to
    ask. Staged under ``sources/artifacts/`` it enters the graph on
    the next build, with its provenance.

The model rewrites; it does not invent. The prompt says so, the
draft carries the model's own notes on what the material left
unclear, and the person reads the preview before anything is saved.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from sahs.loop.skills import MAX_SKILL_CHARS, Skill, _parse

from .skills_loader import BUILTIN, builtin_skills, owner_slug, user_root

KINDS = ("skill", "knowledge")
MAX_MATERIAL_CHARS = 60_000
MAX_SAVE_CHARS = 12_000

SKILL_FORMAT = """\
# <Title: the kind of ask this is for, in four words or fewer>

<One line: what this pack is for and when it applies. This line is
the description the agent reads when deciding to load the pack.>

## <A move: the first thing to do for this kind of ask>
1. <A step, naming the tool when one is implied: search(...) to find
   a definition, run_sql(mode="dry_run") to prove a query, check(kind=
   ...) to hold a claim to the rows, propose_sql to hand a query over,
   artifact for a chart or a document.>
2. <The next step.>

## <Another move>
1. ...

## Never
- <A rule the material implies, stated as a prohibition.>
"""

KNOWLEDGE_FORMAT = """\
# <Title: the subject, as the business names it>

## What this is
<A paragraph: what the subject is, who uses it, for what.>

## Definitions
- <term>: <meaning, in the material's own words>

## Tables and columns
- <physical table name exactly as written in the material>: <what
  it holds>; columns: <column: meaning> …

## Metrics
- <metric name>: <the calculation in words, or the SQL if the
  material gives it>; grain <…>; usual dimensions <…>

## Caveats
- <a known trap, a partial period, a join that is not safe, a
  definition still pending>

## Owner
<who to ask, as the material names them; "not stated" when it does
not>
"""

RULES = """\
You rewrite material into the house format. Rules:
- Use only what the material says. Nothing is invented: no table,
  column, metric, number, owner or date that the material does not
  give. Where the format asks for something the material lacks,
  write "not stated".
- Keep the person's meaning; tighten the words. Short sentences.
- Name tables and columns exactly as the material writes them.
- Stay under {cap} characters of output text.
- Answer as one JSON object: {{"name": "<a slug of lowercase words and
  dashes, at most 40 characters>", "title": "<the title>",
  "description": "<the one line>", "text": "<the whole file, in the
  format, markdown>", "notes": ["<what the material left unclear, one
  item each; empty when nothing did>"]}}
"""


def format_for(kind: str) -> str:
    return SKILL_FORMAT if kind == "skill" else KNOWLEDGE_FORMAT


def prompt_for(kind: str, title: str, material: str,
               hint: str = "") -> tuple[str, str]:
    """(system, user) for the drafting call."""
    if kind not in KINDS:
        raise ValueError(f"kind is skill or knowledge, not {kind!r}")
    cap = MAX_SKILL_CHARS - 200 if kind == "skill" else MAX_SAVE_CHARS - 500
    what = ("a SKILL: doctrine for the agent — the moves for a kind of "
            "ask and the checks each move must run. It steers where the "
            "agent looks; it never asserts a fact about the data."
            if kind == "skill" else
            "a KNOWLEDGE FILE: reference the agent reads at build time — "
            "definitions, tables and columns, metrics, caveats, the "
            "owner. Facts, each traceable to the material.")
    system = (f"You are Synapse's editor. The person is writing {what}\n\n"
              f"The house format:\n\n{format_for(kind)}\n\n"
              + RULES.format(cap=cap))
    material = (material or "").strip()[:MAX_MATERIAL_CHARS]
    user = (f"Title the person gave: {title.strip() or 'not stated'}\n"
            f"What it is for, in their words: {hint.strip() or 'not stated'}"
            f"\n\nThe material:\n<<<\n{material}\n>>>")
    return system, user


def slug(name: str) -> str:
    out = re.sub(r"[^a-z0-9]+", "-", (name or "").strip().lower())
    return out.strip("-")[:40]


def draft(agent: Any, kind: str, title: str, material: str,
          hint: str = "") -> dict[str, Any]:
    """The model's draft, checked: a name, a title, a description, the
    text in the format, and the notes. A missing or malformed answer
    is a reason, never an empty file."""
    if not (material or "").strip():
        return {"ok": False, "reason": "no material to draft from: paste "
                                       "the notes or add a file"}
    system, user = prompt_for(kind, title, material, hint)
    answer = agent.json(user, system=system, temperature=0.2,
                        max_tokens=6144)
    if not isinstance(answer, dict) or not str(
            answer.get("text") or "").strip():
        return {"ok": False, "reason": "the model returned no draft; "
                                       "try again or shorten the material"}
    text = str(answer["text"]).strip()
    if not text.startswith("#"):
        text = f"# {answer.get('title') or title or 'Untitled'}\n\n{text}"
    name = slug(str(answer.get("name") or title or "")) or "draft"
    parsed = _parse_text(name, text)
    notes = answer.get("notes") if isinstance(answer.get("notes"), list) \
        else []
    return {"ok": True, "kind": kind, "name": name,
            "title": str(answer.get("title") or parsed.title),
            "description": str(answer.get("description")
                               or parsed.description),
            "text": text, "chars": len(text),
            "notes": [str(n) for n in notes][:8]}


def _parse_text(name: str, text: str) -> Skill:
    title, description = name, ""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#") and title == name:
            title = stripped.lstrip("#").strip() or name
            continue
        description = stripped[:160]
        break
    return Skill(name=name, title=title, description=description, text=text)


def save_skill(graph_root: Path, owner: str, name: str,
               text: str) -> dict[str, Any]:
    """A person's own pack, on disk where the loader reads it for
    them. Refuses a built-in name (it would never load), an empty
    owner, an empty text, and a text over the cap."""
    name = slug(name)
    if not name:
        return {"ok": False, "reason": "the skill needs a name"}
    if not owner_slug(owner):
        return {"ok": False, "reason": "no owner to save the skill for"}
    if name in {p.name for p in builtin_skills()}:
        return {"ok": False, "reason": f"{name} is a built-in skill's "
                                       "name: pick another"}
    text = (text or "").strip() + "\n"
    if len(text.strip()) < 20:
        return {"ok": False, "reason": "the skill is empty"}
    if len(text) > MAX_SAVE_CHARS:
        return {"ok": False, "reason": f"over {MAX_SAVE_CHARS:,} characters: "
                                       "a skill is a briefing, not a book"}
    root = user_root(Path(graph_root), owner)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{name}.md"
    existed = path.exists()
    path.write_text(text, encoding="utf-8")
    parsed = _parse(path)
    return {"ok": True, "name": parsed.name, "title": parsed.title,
            "description": parsed.description, "owner": owner_slug(owner),
            "origin": "unreviewed", "replaced": existed,
            "truncated": len(text) > MAX_SKILL_CHARS,
            "path": str(path)}


def delete_skill(graph_root: Path, owner: str, name: str) -> bool:
    path = user_root(Path(graph_root), owner) / f"{slug(name)}.md"
    if not path.exists():
        return False
    path.unlink()
    return True


__all__ = ["KINDS", "SKILL_FORMAT", "KNOWLEDGE_FORMAT", "RULES",
           "MAX_MATERIAL_CHARS", "MAX_SAVE_CHARS", "format_for",
           "prompt_for", "slug", "draft", "save_skill", "delete_skill",
           "BUILTIN"]
