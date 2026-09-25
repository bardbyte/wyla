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
    a built-in. Under ``SAHS_STORE=spanner|sqlite`` the runtime binds
    a store-backed shelf for the owner (``bind_own_skills``: the
    ``UserSkills`` rows, ``sahs/assistant/content_store.py``) and the
    folder is not read for them; under ``local`` nothing is bound and
    the folder is the shelf, as before.

Progressive disclosure is the point: the system prompt carries only
names and one-liners (``render_skill_index``); the full text enters a
turn only when the model calls ``load_skill`` — the tool result IS
the injection — or when the user preloads packs via the session
picker (``load_packs``). A pack over the whole-load ceiling
(``SAHS_MAX_SKILL_CHARS``, under the engine's own window) loads as a
LIBRARY instead: its table of contents and the passages matching the
ask, fitted to the engine's budget (``skill_context``), with the kit's
``skill_toc`` / ``skill_search`` / ``skill_read`` for the rest
(docs/skill-retrieval.md). Whole when it fits, a library when it does
not, never a refusal: a frontmatter that asks for the whole file is
a preference the block and the loader record disclose (``preferred:
whole``) when the pack does not fit. Either way the pins from v1
hold: skills steer, they never assert facts, and every load is
disclosed.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from sahs.loop.skills import (
    PREFERRED_SECTIONED,
    PREFERRED_WHOLE,
    SearchableSkill,
    Skill,
    _parse,
    check_readable,
    library_reason,
    max_loaded,
    max_skill_chars,
    policy_of,
    render_searchable_skills,
    skills_root,
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
                        error=s.error,
                        origin=origin, owner=owner, updated=_updated(path)))
    return out


def builtin_skills() -> list[Pack]:
    return _packs(builtin_root(), BUILTIN)


# the store-backed shelves, one per owner slug: a callable returning the
# owner's rows ({name, title, description, text, updated}); bound by
# the runtime when a store is on, never under SAHS_STORE=local
_OWN_SHELVES: dict[str, Callable[[], list[dict[str, Any]]]] = {}


def bind_own_skills(owner: str,
                    source: Callable[[], list[dict[str, Any]]]) -> None:
    """Read this owner's own packs from ``source()`` (the store's
    ``UserSkills`` rows) instead of their folder, in every reader —
    ``all_skills``, ``get_skill``, ``load_packs``, the loop's index and
    the ``load_skill`` tool — with no other call site changed."""
    slug = owner_slug(owner)
    if slug:
        _OWN_SHELVES[slug] = source


def unbind_own_skills(owner: str) -> None:
    _OWN_SHELVES.pop(owner_slug(owner), None)


def own_skills(graph_root: Path | None, owner: str) -> list[Pack]:
    """The owner's own packs: the bound store shelf when one is bound
    for them, else their folder under the graph's skills tree."""
    slug = owner_slug(owner)
    if not slug:
        return []
    source = _OWN_SHELVES.get(slug)
    if source is not None:
        return [Pack(name=str(row["name"]), title=str(row.get("title") or row["name"]),
                     description=str(row.get("description") or ""),
                     text=str(row.get("text") or ""), origin=UNREVIEWED,
                     owner=slug, updated=str(row.get("updated") or ""))
                for row in source()]
    if graph_root is None:
        return []
    return _packs(user_root(Path(graph_root), owner), UNREVIEWED, slug)


def all_skills(graph_root: Path | None = None,
               owner: str = "") -> list[Pack]:
    """Built-in packs first, then the owner's own, then the shared
    user packs; built-in names win every collision, an own pack wins
    over a shared one for its owner (see the module docstring)."""
    merged: dict[str, Pack] = {p.name: p for p in builtin_skills()}
    if graph_root is not None:
        for pack in own_skills(graph_root, owner):
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
    resolver. Missing names are reported, never invented. Size never
    refuses: a pack over the ceiling pins too, and the turn splits
    the list with ``split_by_policy`` so the oversized ones enter the
    prompt as a searchable library (``skill_context``), never as a
    cut of themselves. The one refusal is a file that cannot be read
    (``SkillUnreadable``, by name — the pickers show the reason)."""
    available = {p.name: p for p in all_skills(graph_root, owner)}
    loaded, missing = [], []
    for name in names[:max_loaded()]:
        pack = available.get(name)
        if pack is None:
            missing.append(name)
            continue
        loaded.append(check_readable(pack))
    return loaded, missing


def whole_load_limit(model_name: str = "",
                     env: Mapping[str, str] | None = None) -> int:
    """The longest skill this turn loads whole: the engine's share of
    its context window (``profiles.whole_load_chars_for``) under the
    global ceiling (``SAHS_MAX_SKILL_CHARS``)."""
    from sahs.util.profiles import whole_load_chars_for
    return min(max_skill_chars(env), whole_load_chars_for(
        model_name, dict(env) if env is not None else None))


def split_by_policy(packs: list[Pack], limit: int, model_name: str = ""
                    ) -> tuple[list[Pack], list[Pack]]:
    """(whole, searchable) at this turn's whole-load limit: whole
    when it fits, a library when it does not — never a refusal. The
    frontmatter's ``runtime_loading`` / ``truncation_allowed`` do not
    move a pack between the two lists; they are the preference
    ``preference_of`` reports and the block discloses."""
    whole = [p for p in packs if p.chars <= limit]
    searchable = [p for p in packs if p.chars > limit]
    return whole, searchable


def preference_of(pack: Skill) -> tuple[str, str]:
    """(preferred, why) from the frontmatter: ``whole`` with the keys
    that asked for it (``runtime_loading: full_file_required``,
    ``truncation_allowed: false``), else ``sectioned`` and ''."""
    policy = policy_of(pack.text)
    if policy.requires_whole:
        return PREFERRED_WHOLE, policy.why
    return PREFERRED_SECTIONED, ""


MODE_WHOLE = "whole"
MODE_LIBRARY = "library"


def loader_record(pack: Pack, mode: str, *, rendered: int = 0,
                  sent: int = 0, limit: int = 0,
                  model_name: str = "") -> dict[str, Any]:
    """One line of the turn's loader record (the ``skills_loaded``
    event): what the skill is, what the renderer made of it, what
    reached the prompt, how (``whole`` or ``library``), what its
    frontmatter preferred, and — for a library — the sizes that put
    it there. ``truncated`` says the prompt holds less than the file;
    it is always false for a whole load, and for a library the whole
    text stays reachable by section, so nothing is cut."""
    preferred, why = preference_of(pack)
    reason = (library_reason(pack, limit, model_name, why)
              if mode == MODE_LIBRARY else "")
    return {"skill_name": pack.name, "source_chars": pack.chars,
            "rendered_chars": int(rendered), "sent_chars": int(sent),
            "mode": mode, "preferred": preferred, "reason": reason,
            "truncated": mode == MODE_LIBRARY and sent < pack.chars}


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
    index the tools read from, the budgets that shaped it, and the
    loader record the turn emits (``skills_loaded``)."""

    whole: list[Pack] = field(default_factory=list)
    searchable: list[Pack] = field(default_factory=list)
    block: str = ""
    index: Any = None                    # sahs.loop.skill_index.SkillIndex
    views: list[SearchableSkill] = field(default_factory=list)
    chunks: int = 0                      # the fold: passages allowed
    budget: int = 0                      # the fold: characters allowed
    limit: int = 0                       # this turn's whole-load limit
    records: list[dict[str, Any]] = field(default_factory=list)
    likely: tuple[str, ...] = ()         # the routing hint's order

    @property
    def searchable_names(self) -> list[str]:
        return [p.name for p in self.searchable]

    @property
    def aggregate_skill_chars(self) -> int:
        return sum(int(r.get("sent_chars") or 0) for r in self.records)

    def event(self) -> dict[str, Any]:
        """The ``skills_loaded`` event's fields."""
        return {"skills": list(self.records),
                "skills_loaded": [r["skill_name"] for r in self.records],
                "aggregate_skill_chars": self.aggregate_skill_chars,
                "whole_load_limit": self.limit,
                "retrieval_budget": self.budget,
                "retrieval_chunks": self.chunks}


def skill_context(graph_root: Path | None, packs: list[Pack],
                  query: str, model_name: str = "",
                  stop: str = "medium", *,
                  index: Any = None,
                  limit: int | None = None,
                  shelf: list[Pack] | None = None,
                  tools: bool = True) -> SkillContext:
    """Split the loaded packs at this engine's whole-load limit
    (``whole_load_limit``): whole when it fits, a library when it
    does not, never a refusal. For the searchable ones, build the
    prompt block: each pack's contents (fitted) and the top passages
    for ``query`` at this engine's fold for ``stop``
    (``profiles.skill_retrieval_for``), each block saying ``mode:
    library``, the frontmatter's preference and the sizes. With
    ``shelf``, the routing hint ranks the shelf's packs for the query
    (``likely``). ``tools=False`` renders the v1 navigator's static
    block (no lookup tool named). The index lives at
    ``<graph>/runs/skill_index.sqlite3`` and is built lazily: an
    unchanged pack costs a hash, nothing more; it never fails a turn
    (memory when the file cannot be used, one chunk when the chunker
    cannot read a pack)."""
    from sahs.util.profiles import skill_retrieval_for

    from sahs.loop.skill_index import open_index

    limit = whole_load_limit(model_name) if limit is None else limit
    ctx = SkillContext(limit=limit)
    whole, searchable = split_by_policy(list(packs), limit, model_name)
    ctx.whole, ctx.searchable = whole, searchable
    for pack in whole:
        # what render_skills adds for this pack: the heading, the text
        shown = len(f"### {pack.title}\n{pack.text.strip()}\n")
        ctx.records.append(loader_record(pack, MODE_WHOLE, rendered=shown,
                                         sent=shown, limit=limit,
                                         model_name=model_name))
    if shelf:
        ctx.index = index if index is not None else open_index(graph_root)
        ctx.likely = rank_shelf(ctx.index, list(shelf), query)
    if not searchable:
        return ctx
    k, budget = skill_retrieval_for(model_name, stop)
    ctx.chunks, ctx.budget = k, budget
    if ctx.index is None:
        ctx.index = index if index is not None else open_index(graph_root)
    ctx.index.ensure(searchable)
    names = [p.name for p in searchable]
    hits = ctx.index.search(query, skills=names, k=k) if query.strip() \
        else []
    # what each pack's frontmatter preferred, and the sizes that put
    # it in the library: disclosed on the block and in the record
    marks: dict[str, tuple[str, str]] = {}
    for pack in searchable:
        preferred, why = preference_of(pack)
        marks[pack.name] = (preferred,
                            library_reason(pack, limit, model_name, why))
    header = len(render_searchable_skills(
        [SearchableSkill(p.name, p.title, p.chars, 0, 0,
                         preferred=marks[p.name][0], reason=marks[p.name][1])
         for p in searchable], limit, tools=tools))
    toc_budget = max(0, int(budget * TOC_SHARE) - header) // len(searchable)
    views: list[SearchableSkill] = []
    for pack in searchable:
        info = ctx.index.overview(pack.name) or {}
        lines, omitted = toc_lines(ctx.index.toc(pack.name), toc_budget)
        views.append(SearchableSkill(
            pack.name, pack.title, pack.chars,
            int(info.get("sections") or 0), int(info.get("chunks") or 0),
            tuple(lines), omitted, (), 0, *marks[pack.name]))
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
                             matched.get(v.name, 0), v.preferred, v.reason)
             for v in views], limit, tools=tools)
    block = _render()
    found: dict[str, list[dict]] = {v.name: [] for v in views}
    for hit in hits:
        if hit.skill not in by_name:
            continue
        chunk = ctx.index.chunk(hit.skill, hit.chunk_id) or {}
        if not chunk:
            continue
        page = {"chunk_id": chunk["chunk_id"],
                "heading_path": chunk["heading_path"],
                "start": chunk["start"], "end": chunk["end"],
                "text": chunk["text"]}
        found[hit.skill].append(page)
        passages[hit.skill].append(page)
        candidate = _render()
        if len(candidate) > budget:
            passages[hit.skill].pop()
            continue
        block = candidate
    ctx.views = [SearchableSkill(v.name, v.title, v.chars, v.sections,
                                 v.chunks, v.toc, v.toc_omitted,
                                 tuple(passages[v.name]),
                                 matched.get(v.name, 0), v.preferred,
                                 v.reason) for v in views]
    ctx.block = block
    by_pack = {p.name: p for p in searchable}
    for view in ctx.views:
        # rendered: the contents plus every passage the search found;
        # sent: the same after the budget fitted it
        full = SearchableSkill(view.name, view.title, view.chars,
                               view.sections, view.chunks, view.toc,
                               view.toc_omitted, tuple(found[view.name]),
                               view.matched, view.preferred, view.reason)
        ctx.records.append(loader_record(
            by_pack[view.name], MODE_LIBRARY,
            rendered=len(render_searchable_skills([full], limit,
                                                  header=False, tools=tools)),
            sent=len(render_searchable_skills([view], limit,
                                              header=False, tools=tools)),
            limit=limit, model_name=model_name))
    return ctx


def render_skill_index(packs: list[Pack],
                       exclude: frozenset[str] = frozenset(),
                       likely: tuple[str, ...] = ()) -> str:
    """The names-only system-prompt section. Excluded names (packs
    already preloaded in full) are not re-offered; empty in, empty
    out — a shelf-less prompt stays byte-identical. ``likely`` (the
    routing hint's order, ``rank_shelf``) lists those packs first,
    marked; without it the section is byte-identical to before."""
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
    by_name = {p.name: p for p in rows}
    first = [by_name[n] for n in likely if n in by_name]
    if first:
        lines[-1] += (" The packs marked (likely) matched this message's "
                      "words by their description, aliases or headings "
                      "— a hint, not a verdict.")
    rest = [p for p in rows if p.name not in set(likely)]
    for pack in first + rest:
        tag = "" if pack.origin == BUILTIN else f" [{pack.origin}]"
        mark = " (likely)" if pack in first else ""
        lines.append(f"- {pack.name}{tag}{mark} — {pack.description}")
    return "\n".join(lines)


def rank_shelf(index: Any, packs: list[Pack], question: str,
               k: int = 3) -> tuple[str, ...]:
    """The routing hint: the shelf's packs a question likely wants,
    best first (``SkillIndex.rank_skills`` over every pack's
    description, aliases and headings). Cheap and deterministic; the
    model still decides what to load."""
    if index is None or not packs or not (question or "").strip():
        return ()
    index.ensure_routing(packs)
    names = {p.name for p in packs}
    return tuple(r["skill"] for r in index.rank_skills(question, k)
                 if r["skill"] in names)


__all__ = ["BUILTIN", "UNREVIEWED", "Pack", "builtin_root", "author_of",
           "owner_slug", "bind_own_skills", "unbind_own_skills", "own_skills",
           "user_root", "builtin_skills", "all_skills", "get_skill",
           "load_packs", "render_skill_index", "SkillContext",
           "skill_context", "toc_lines", "TOC_TOOL_CAP",
           "whole_load_limit", "split_by_policy", "preference_of",
           "loader_record", "MODE_WHOLE", "MODE_LIBRARY", "rank_shelf"]
