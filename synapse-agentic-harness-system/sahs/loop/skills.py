"""Session skills (Agent Loop v1 §2/§4): analyst-authored context the
loop carries, Claude-Code style.

A skill is a markdown file in ``<graph>/skills/`` — the analyst's own
words about how THEY read the data (a fiscal-calendar note, a team's
metric preferences, a market's naming habits). The user selects which
skills a session loads; the selected text enters the navigator's
system prompt verbatim (bounded) and is disclosed on ``loop_started``
and in the "what the model saw" panel.

Two pins:
  * **skills steer, they never assert facts.** A skill cannot add a
    table, metric, or number to the world — the tools still serve
    only the compiled build, and the verifier still holds every claim
    to it. A skill that says "spend means gross" changes where the
    model LOOKS first, not what exists.
  * **selection is explicit and visible.** Nothing loads by default;
    the session stores the chosen names; every surface that shows the
    model's context shows the loaded skills.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

MAX_LOADED = 4              # chips, not a library: choose what matters
MAX_SKILL_CHARS = 4000      # each skill is a briefing, not a book


@dataclass(frozen=True)
class Skill:
    name: str               # the file stem: the stable id
    title: str              # first "# " heading, or the name
    description: str        # first prose line after the title
    text: str               # full markdown, truncated at the cap


def skills_root(graph_root: Path) -> Path:
    return Path(graph_root) / "skills"


def _parse(path: Path) -> Skill:
    raw = path.read_text(encoding="utf-8")
    if len(raw) > MAX_SKILL_CHARS:
        raw = raw[:MAX_SKILL_CHARS - 20] + "\n… skill truncated."
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
    root = skills_root(graph_root)
    if not root.exists():
        return []
    return [_parse(p) for p in sorted(root.glob("*.md"))]


def load_skills(graph_root: Path,
                names: list[str]) -> tuple[list[Skill], list[str]]:
    """(loaded, missing) — missing names are reported, never invented
    into empty skills."""
    available = {s.name: s for s in list_skills(graph_root)}
    loaded, missing = [], []
    for name in names[:MAX_LOADED]:
        skill = available.get(name)
        if skill is None:
            missing.append(name)
        else:
            loaded.append(skill)
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
