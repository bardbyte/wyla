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
picker (``load_packs``). Either way the pins from v1 hold: skills
steer, they never assert facts, and every load is disclosed.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from sahs.loop.skills import (
    Skill,
    _parse,
    check_size,
    max_loaded,
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
    resolver. Missing names are reported, never invented; a pack over
    the size ceiling raises ``SkillTooLarge`` (see sahs.loop.skills)
    rather than reaching the model as a cut of itself."""
    available = {p.name: p for p in all_skills(graph_root, owner)}
    loaded, missing = [], []
    for name in names[:max_loaded()]:
        pack = available.get(name)
        if pack is None:
            missing.append(name)
        else:
            loaded.append(check_size(pack))
    return loaded, missing


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
           "owner_slug", "bind_own_skills", "unbind_own_skills", "own_skills",
           "user_root", "builtin_skills", "all_skills", "get_skill",
           "load_packs", "render_skill_index"]
