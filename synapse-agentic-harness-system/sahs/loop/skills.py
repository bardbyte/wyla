"""Session skills (Agent Loop v1 §2/§4): analyst-authored context the
loop carries, coding-agent style.

A skill is a markdown file in ``<graph>/skills/`` — the analyst's own
words about how THEY read the data (a fiscal-calendar note, a team's
metric preferences, a market's naming habits). The user selects which
skills a session loads; the selected text enters the navigator's
system prompt verbatim and is disclosed on ``loop_started`` and in
the "what the model saw" panel.

Three pins:
  * **skills steer, they never assert facts.** A skill cannot add a
    table, metric, or number to the world — the tools still serve
    only the compiled build, and the verifier still holds every claim
    to it. A skill that says "spend means gross" changes where the
    model LOOKS first, not what exists.
  * **selection is explicit and visible.** Nothing loads by default;
    the session stores the chosen names; every surface that shows the
    model's context shows the loaded skills.
  * **a skill loads whole or not at all.** The size ceiling and the
    count ceiling come from the silo ``.env`` (``SAHS_MAX_SKILL_CHARS``,
    ``SAHS_MAX_LOADED_SKILLS``); the defaults keep a skill a briefing
    and a session to a few. A skill over the ceiling is still LISTED
    — the shelf never hides it — but refuses to LOAD, by name, with
    the number and the variable that raises it. Governed knowledge is
    never truncated in silence: a cut briefing reads as a different
    briefing, and nobody would know.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MAX_LOADED = 4          # chips, not a library: choose what matters
DEFAULT_MAX_SKILL_CHARS = 4000  # each skill is a briefing, not a book
LOADED_VAR = "SAHS_MAX_LOADED_SKILLS"
CHARS_VAR = "SAHS_MAX_SKILL_CHARS"


def _env_int(name: str, default: int,
             env: Mapping[str, str] | None = None) -> int:
    """A positive integer from the env (``128_000`` and ``1e5`` both
    read), or the default — an unreadable or non-positive value falls
    back rather than silently disabling the ceiling."""
    source: Mapping[str, str] = os.environ if env is None else env
    raw = str(source.get(name, "")).strip()
    try:
        value = int(float(raw)) if raw else default
    except ValueError:
        return default
    return value if value > 0 else default


def max_loaded(env: Mapping[str, str] | None = None) -> int:
    """How many skills one session loads (``SAHS_MAX_LOADED_SKILLS``)."""
    return _env_int(LOADED_VAR, DEFAULT_MAX_LOADED, env)


def max_skill_chars(env: Mapping[str, str] | None = None) -> int:
    """The longest skill a session loads (``SAHS_MAX_SKILL_CHARS``)."""
    return _env_int(CHARS_VAR, DEFAULT_MAX_SKILL_CHARS, env)


@dataclass(frozen=True)
class Skill:
    name: str               # the file stem: the stable id
    title: str              # first "# " heading, or the name
    description: str        # first prose line after the title
    text: str               # full markdown, whole — never truncated

    @property
    def chars(self) -> int:
        return len(self.text)


class SkillTooLarge(ValueError):
    """A skill over the ceiling: named, sized, and told how to fix —
    raise the variable or split the skill. Never a silent cut."""

    def __init__(self, skill: Skill, limit: int) -> None:
        self.skill, self.limit = skill, limit
        super().__init__(
            f"skill {skill.name!r} has {skill.chars:,} characters; the "
            f"limit is {limit:,} ({CHARS_VAR}). Refusing to truncate "
            f"governed knowledge — raise {CHARS_VAR} in the silo .env "
            "or split the skill.")


def check_size(skill: Skill, limit: int | None = None) -> Skill:
    """The skill, whole, or ``SkillTooLarge``."""
    limit = max_skill_chars() if limit is None else limit
    if skill.chars > limit:
        raise SkillTooLarge(skill, limit)
    return skill


def skills_root(graph_root: Path) -> Path:
    return Path(graph_root) / "skills"


def _parse(path: Path) -> Skill:
    raw = path.read_text(encoding="utf-8")
    title, description = path.stem, ""
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#") and title == path.stem:
            title = stripped.lstrip("#").strip() or path.stem
            continue
        description = stripped[:160]
        break
    return Skill(name=path.stem, title=title,
                 description=description, text=raw)


def list_skills(graph_root: Path) -> list[Skill]:
    """Every skill on the shelf, oversized ones included — the shelf
    shows what exists; the ceiling applies at load."""
    root = skills_root(graph_root)
    if not root.exists():
        return []
    return [_parse(p) for p in sorted(root.glob("*.md"))]


def load_skills(graph_root: Path,
                names: list[str]) -> tuple[list[Skill], list[str]]:
    """(loaded, missing) — missing names are reported, never invented
    into empty skills; a skill over the size ceiling raises
    ``SkillTooLarge`` rather than loading a cut of itself."""
    cap, limit = max_loaded(), max_skill_chars()
    available = {s.name: s for s in list_skills(graph_root)}
    loaded, missing = [], []
    for name in names[:cap]:
        skill = available.get(name)
        if skill is None:
            missing.append(name)
        else:
            loaded.append(check_size(skill, limit))
    return loaded, missing


def render_skills(skills: list[Skill]) -> str:
    """The prompt section. Empty when nothing is loaded — the prompt
    stays byte-identical for skill-less sessions (cacheable)."""
    if not skills:
        return ""
    parts = ["## Skills the analyst loaded",
             "These steer where you look first. They cannot add "
             "tables, metrics, or numbers to the world: the tools "
             "still serve only the compiled build, and the verifier "
             "still checks every claim against it.", ""]
    for skill in skills:
        parts.append(f"### {skill.title}")
        parts.append(skill.text.strip())
        parts.append("")
    return "\n".join(parts)
