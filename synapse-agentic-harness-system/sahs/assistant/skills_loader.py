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

from sahs.loop.skills import MAX_LOADED, Skill, _parse, skills_root

BUILTIN = "built-in"
UNREVIEWED = "unreviewed"      # the E14 door: usable now, labeled


@dataclass(frozen=True)
class Pack(Skill):
    origin: str = BUILTIN


def builtin_root() -> Path:
    return Path(__file__).parent / "skills"


def _packs(root: Path, origin: str) -> list[Pack]:
    if not root.exists():
        return []
    return [Pack(name=s.name, title=s.title,
                 description=s.description, text=s.text, origin=origin)
            for s in (_parse(p) for p in sorted(root.glob("*.md")))]


def builtin_skills() -> list[Pack]:
    return _packs(builtin_root(), BUILTIN)


def all_skills(graph_root: Path | None = None) -> list[Pack]:
    """Built-in packs first, then the analyst's own; built-in names
    win collisions (see the module docstring for why)."""
    merged: dict[str, Pack] = {p.name: p for p in builtin_skills()}
    if graph_root is not None:
        for pack in _packs(skills_root(Path(graph_root)), UNREVIEWED):
            merged.setdefault(pack.name, pack)
    return list(merged.values())


def get_skill(graph_root: Path | None, name: str) -> Pack | None:
    for pack in all_skills(graph_root):
        if pack.name == name:
            return pack
    return None


def load_packs(graph_root: Path | None,
               names: list[str]) -> tuple[list[Pack], list[str]]:
    """(loaded, missing) across both shelves — the session-preload
    resolver. Missing names are reported, never invented."""
    available = {p.name: p for p in all_skills(graph_root)}
    loaded, missing = [], []
    for name in names[:MAX_LOADED]:
        pack = available.get(name)
        if pack is None:
            missing.append(name)
        else:
            loaded.append(pack)
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
             "Doctrine packs by name; load_skill(name) pulls the "
             "full text into this turn when the task matches. "
             "Unreviewed packs are the analyst's own words: they "
             "steer where you look, they never assert facts."]
    for pack in rows:
        tag = "" if pack.origin == BUILTIN else f" [{pack.origin}]"
        lines.append(f"- {pack.name}{tag} — {pack.description}")
    return "\n".join(lines)


__all__ = ["BUILTIN", "UNREVIEWED", "Pack", "builtin_root",
           "builtin_skills", "all_skills", "get_skill", "load_packs",
           "render_skill_index"]
