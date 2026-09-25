"""Skill packs (Synapse v2 §7/§13.3): doctrine on demand.

Two shelves, one index:

  * **built-in packs** ship with the assistant (``sahs/assistant/
    skills/*.md``) — method notes written against the real toolkit:
    the search doctrine, the analysis playbooks, dashboard grammar,
    the executive-summary shape. Origin ``built-in``.
  * **user packs** are the analyst's own briefings in
    ``<graph>/skills/`` — the same files the v1 picker serves. They
    enter through the E14 door: usable immediately, labeled
    ``unreviewed`` everywhere they appear. A user pack cannot shadow
    a built-in name — the built-in wins and the user copy is ignored,
    so nobody smuggles new doctrine under a trusted label.
  * **a person's own packs** live in ``<graph>/skills/users/<owner>/``
    and load only for that owner (the runtime names the owner; today
    the configured user, tomorrow the signed-in one). They are
    unreviewed like any user pack, carry ``owner``, and shadow a
    shared user pack of the same name for their owner alone — never
    a built-in.

Progressive disclosure is the point: the system prompt carries only
names and one-liners (``render_skill_index``); the full text enters a
turn only when the model calls ``load_skill`` — the tool result IS
the injection — or when the user preloads packs via the session
picker (``load_packs``). A pack over the whole-load ceiling
(``SAHS_MAX_SKILL_CHARS``) loads as a LIBRARY instead: its table of
contents and the passages matching the ask, fitted to the engine's
budget (``skill_context``), with the kit's ``skill_toc`` /
``skill_search`` / ``skill_read`` for the rest (docs/skill-retrieval.md).
Either way the pins from v1 hold: skills steer, they never assert
facts, and every load is disclosed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sahs.loop.skills import (
    SearchableSkill,
    Skill,
    _parse,
    max_loaded,
    max_skill_chars,
    render_searchable_skills,
    skills_root,
    split_by_ceiling,
)

BUILTIN = "built-in"
UNREVIEWED = "unreviewed"      # the E14 door: usable now, labeled


@dataclass(frozen=True)
class Pack(Skill):
    origin: str = BUILTIN
    owner: str = ""             # a person's own pack: the owner's slug
    updated: str = ""           # the file's last write, ISO, for the shelf


def builtin_root() -> Path:
    return Path(__file__).parent / "skills"


def author_of(text: str) -> str:
    """The author a markdown file names for itself: a YAML front
    matter ``author:`` or an ``Author:`` line in the first thirty
    lines; '' when it names none."""
    import re
    for line in (text or "").splitlines()[:30]:
        m = re.match(r"^\s*(?:[-*]\s*)?\*{0,2}author\*{0,2}\s*:\*{0,2}\s*(.+?)\s*$",
                     line, re.I)
        if m:
            return m.group(1).strip().strip("'\"")[:60]
    return ""


def owner_slug(owner: str) -> str:
    """The folder a person's packs live in: letters, digits, dashes."""
    import re
    slug = re.sub(r"[^a-z0-9]+", "-", (owner or "").strip().lower())
    return slug.strip("-")[:60]


def user_root(graph_root: Path, owner: str) -> Path:
    return skills_root(Path(graph_root)) / "users" / owner_slug(owner)


def _updated(path: Path) -> str:
    import datetime as _dt
    try:
        stamp = path.stat().st_mtime
    except OSError:
        return ""
    return _dt.datetime.fromtimestamp(
        stamp, tz=_dt.timezone.utc).isoformat(timespec="seconds")


def _packs(root: Path, origin: str, owner: str = "") -> list[Pack]:
    if not root.exists():
        return []
    out = []
    for path in sorted(root.glob("*.md")):
        s = _parse(path)
        out.append(Pack(name=s.name, title=s.title,
                        description=s.description, text=s.text,
                        origin=origin, owner=owner, updated=_updated(path)))
    return out


def builtin_skills() -> list[Pack]:
    return _packs(builtin_root(), BUILTIN)


def all_skills(graph_root: Path | None = None,
               owner: str = "") -> list[Pack]:
    """Built-in packs first, then the owner's own, then the shared
    user packs; built-in names win every collision, an own pack wins
    over a shared one for its owner (see the module docstring)."""
    merged: dict[str, Pack] = {p.name: p for p in builtin_skills()}
    if graph_root is not None:
        if owner_slug(owner):
            for pack in _packs(user_root(Path(graph_root), owner),
                               UNREVIEWED, owner_slug(owner)):
                merged.setdefault(pack.name, pack)
        for pack in _packs(skills_root(Path(graph_root)), UNREVIEWED):
            merged.setdefault(pack.name, pack)
    return list(merged.values())


def get_skill(graph_root: Path | None, name: str,
              owner: str = "") -> Pack | None:
    for pack in all_skills(graph_root, owner):
        if pack.name == name:
            return pack
    return None


def load_packs(graph_root: Path | None,
               names: list[str],
               owner: str = "") -> tuple[list[Pack], list[str]]:
    """(loaded, missing) across the shelves — the session-preload
    resolver. Missing names are reported, never invented. A pack over
    the size ceiling loads too: the turn splits the list with
    ``split_by_ceiling`` and the oversized ones enter the prompt as a
    searchable library (``skill_context``), never as a cut of
    themselves."""
    available = {p.name: p for p in all_skills(graph_root, owner)}
    loaded, missing = [], []
    for name in names[:max_loaded()]:
        pack = available.get(name)
        if pack is None:
            missing.append(name)
        else:
            loaded.append(pack)
    return loaded, missing


# ─── the searchable library: catalogue + pages, under a budget ──

TOC_SHARE = 0.35        # of the budget: the contents; the rest, pages
TOC_TOOL_CAP = 15_000   # chars a skill_toc result lists before folding


def toc_lines(toc: list[dict], budget: int,
              max_level: int = 0) -> tuple[list[str], int]:
    """The contents as prompt lines under ``budget`` characters,
    folding to shallower headings when the full list does not fit,
    then cutting with a count of what was left out. Deterministic:
    (lines, omitted)."""
    def _line(s: dict) -> str:
        return (f"{s['section_id']} · {s['heading_path']} "
                f"({s['chunks']} chunk{'s' if s['chunks'] != 1 else ''}, "
                f"{s['chars']:,} chars)")
    levels = sorted({int(s["level"]) for s in toc}, reverse=True)
    deepest = levels[0] if levels else 0
    for level in ([max_level] if max_level else [deepest, 3, 2, 1]):
        if level > deepest:
            continue
        rows = [s for s in toc if int(s["level"]) <= level]
        lines = [_line(s) for s in rows]
        if sum(len(x) + 3 for x in lines) <= budget:
            return lines, len(toc) - len(rows)
    rows = [s for s in toc if int(s["level"]) <= (max_level or 1)]
    kept: list[str] = []
    used = 0
    for s in rows:
        line = _line(s)
        if used + len(line) + 3 > budget:
            break
        kept.append(line)
        used += len(line) + 3
    return kept, len(toc) - len(kept)


@dataclass
class SkillContext:
    """One turn's skills, split: the packs pasted whole, the packs
    loaded as a library (with the rendered block for the prompt), the
    index the tools read from, and the budget that shaped it."""

    whole: list[Pack] = field(default_factory=list)
    searchable: list[Pack] = field(default_factory=list)
    block: str = ""
    index: Any = None                    # sahs.loop.skill_index.SkillIndex
    views: list[SearchableSkill] = field(default_factory=list)
    chunks: int = 0                      # the fold: passages allowed
    budget: int = 0                      # the fold: characters allowed

    @property
    def searchable_names(self) -> list[str]:
        return [p.name for p in self.searchable]


def skill_context(graph_root: Path | None, packs: list[Pack],
                  query: str, model_name: str = "",
                  stop: str = "medium", *,
                  index: Any = None,
                  limit: int | None = None) -> SkillContext:
    """Split the loaded packs at the ceiling and, for the searchable
    ones, build the prompt block: each pack's contents (fitted) and
    the top passages for ``query`` at this engine's fold for ``stop``
    (``profiles.skill_retrieval_for``). The index lives at
    ``<graph>/runs/skill_index.sqlite3`` and is built lazily: an
    unchanged pack costs a hash, nothing more."""
    from sahs.util.profiles import skill_retrieval_for

    from sahs.loop.skill_index import open_index

    limit = max_skill_chars() if limit is None else limit
    whole, searchable = split_by_ceiling(list(packs), limit)
    ctx = SkillContext(whole=whole, searchable=searchable)
    if not searchable:
        return ctx
    k, budget = skill_retrieval_for(model_name, stop)
    ctx.chunks, ctx.budget = k, budget
    ctx.index = index if index is not None else open_index(graph_root)
    ctx.index.ensure(searchable)
    names = [p.name for p in searchable]
    hits = ctx.index.search(query, skills=names, k=k) if query.strip() \
        else []
    header = len(render_searchable_skills(
        [SearchableSkill(p.name, p.title, p.chars, 0, 0)
         for p in searchable], limit))
    toc_budget = max(0, int(budget * TOC_SHARE) - header) // len(searchable)
    views: list[SearchableSkill] = []
    for pack in searchable:
        info = ctx.index.overview(pack.name) or {}
        lines, omitted = toc_lines(ctx.index.toc(pack.name), toc_budget)
        views.append(SearchableSkill(
            pack.name, pack.title, pack.chars,
            int(info.get("sections") or 0), int(info.get("chunks") or 0),
            tuple(lines), omitted, (), 0))
    # the pages: in rank order, each whole or not at all, while the
    # rendered block stays under the budget
    by_name = {v.name: i for i, v in enumerate(views)}
    passages: dict[str, list[dict]] = {v.name: [] for v in views}
    matched: dict[str, int] = {v.name: 0 for v in views}
    for hit in hits:
        matched[hit.skill] = matched.get(hit.skill, 0) + 1
    def _render() -> str:
        return render_searchable_skills(
            [SearchableSkill(v.name, v.title, v.chars, v.sections, v.chunks,
                             v.toc, v.toc_omitted, tuple(passages[v.name]),
                             matched.get(v.name, 0)) for v in views], limit)
    block = _render()
    for hit in hits:
        if hit.skill not in by_name:
            continue
        chunk = ctx.index.chunk(hit.skill, hit.chunk_id) or {}
        if not chunk:
            continue
        passages[hit.skill].append(
            {"chunk_id": chunk["chunk_id"], "heading_path": chunk["heading_path"],
             "start": chunk["start"], "end": chunk["end"],
             "text": chunk["text"]})
        candidate = _render()
        if len(candidate) > budget:
            passages[hit.skill].pop()
            continue
        block = candidate
    ctx.views = [SearchableSkill(v.name, v.title, v.chars, v.sections,
                                 v.chunks, v.toc, v.toc_omitted,
                                 tuple(passages[v.name]),
                                 matched.get(v.name, 0)) for v in views]
    ctx.block = block
    return ctx


def render_skill_index(packs: list[Pack],
                       exclude: frozenset[str] = frozenset()) -> str:
    """The names-only system-prompt section. Excluded names (packs
    already preloaded in full) are not re-offered; empty in, empty
    out — a shelf-less prompt stays byte-identical."""
    rows = [p for p in packs if p.name not in exclude]
    if not rows:
        return ""
    lines = ["## Skills on demand",
             "Doctrine packs by name. Loading them is YOUR job, not "
             "the user's: when the task matches a line below, call "
             "load_skill(name) before doing the work — a \"why did "
             "it change\" loads analysis-playbooks unprompted. "
             "Unreviewed packs are the analyst's own words: they "
             "steer where you look, they never assert facts."]
    for pack in rows:
        tag = "" if pack.origin == BUILTIN else f" [{pack.origin}]"
        lines.append(f"- {pack.name}{tag} — {pack.description}")
    return "\n".join(lines)


__all__ = ["BUILTIN", "UNREVIEWED", "Pack", "builtin_root", "author_of",
           "owner_slug",
           "user_root", "builtin_skills", "all_skills", "get_skill",
           "load_packs", "render_skill_index", "SkillContext",
           "skill_context", "toc_lines", "TOC_TOOL_CAP"]
