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
  * **a skill loads whole, or as a library.** The size ceiling and
    the count ceiling come from the silo ``.env``
    (``SAHS_MAX_SKILL_CHARS``, ``SAHS_MAX_LOADED_SKILLS``); the
    defaults keep a whole-loaded skill a briefing and a session to a
    few. A skill within the ceiling enters the prompt whole, verbatim.
    A skill over it is still LISTED — the shelf never hides it — and
    in the assistant it loads as a SEARCHABLE pack: its table of
    contents plus the passages that match the ask, under a per-model
    budget, with tools to read any section (``sahs.loop.skill_index``,
    ``render_searchable_skills``). The v1 navigator, which has no
    such tools, still refuses by name (``SkillTooLarge``). Governed
    knowledge is never truncated in silence: what the model holds is
    labeled as the catalogue and the pages it asked for, never as the
    whole book.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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


class SkillRefused(SkillTooLarge):
    """A skill whose frontmatter demands the whole file
    (``runtime_loading: full_file_required`` or ``truncation_allowed:
    false``) that does not fit the whole-load budget: refused by name
    with the reason. It never loads in part and never falls back to
    search — fail closed. ``model`` names the engine when the budget
    is the engine's; empty when it is the global ceiling."""

    def __init__(self, skill: Skill, limit: int, model: str = "",
                 why: str = "") -> None:
        self.skill, self.limit, self.model = skill, limit, model
        whose = (f"this model's ({model}) whole-load budget" if model
                 else f"the whole-load ceiling ({CHARS_VAR})")
        fix = ("switch to a model with a larger window or mark the "
               "skill sectioned" if model else
               f"raise {CHARS_VAR} or mark the skill sectioned")
        ValueError.__init__(
            self,
            f"skill {skill.name!r} needs {skill.chars:,} chars whole "
            f"({why or 'its frontmatter requires the full file'}), "
            f"{whose} is {limit:,}; {fix}.")


# ─── frontmatter: the skill's own word on how it may load ────

LOADING_SECTIONED = "sectioned"
LOADING_FULL = "full_file_required"
LOADING_MODES = (LOADING_SECTIONED, LOADING_FULL)


def frontmatter(text: str) -> dict[str, Any]:
    """The leading ``---`` block as a flat mapping, stdlib only: scalar
    values (quoted or bare; true/false/yes/no fold to bool), inline
    lists ``[a, b]`` and block lists (``- item`` lines). Anything
    else is kept as its raw string. {} when there is none."""
    lines = (text or "").splitlines()
    if not lines or lines[0].strip() != "---":
        return {}
    out: dict[str, Any] = {}
    key = ""
    for line in lines[1:]:
        if line.strip() in ("---", "..."):
            break
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        item = re.match(r"^\s+-\s*(.*)$", line)
        if item and key and isinstance(out.get(key), list):
            out[key].append(_scalar(item.group(1)))
            continue
        m = re.match(r"^([A-Za-z_][\w-]*)\s*:\s*(.*)$", line)
        if not m:
            continue
        key, raw = m.group(1).strip(), m.group(2).strip()
        if raw == "":
            out[key] = []                 # a block list may follow
        elif raw.startswith("[") and raw.endswith("]"):
            out[key] = [_scalar(v) for v in raw[1:-1].split(",")
                        if v.strip()]
        else:
            out[key] = _scalar(raw)
    return out


def _scalar(raw: str) -> Any:
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    low = value.lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    return value


def strip_frontmatter(text: str) -> str:
    """The text after the frontmatter block (the text itself when
    there is none)."""
    lines = (text or "").splitlines(keepends=True)
    if not lines or lines[0].strip() != "---":
        return text
    for i, line in enumerate(lines[1:], 1):
        if line.strip() in ("---", "..."):
            return "".join(lines[i + 1:])
    return text


@dataclass(frozen=True)
class LoadPolicy:
    """How a skill may load, from its frontmatter: ``runtime_loading``
    (sectioned, the default, or full_file_required) and
    ``truncation_allowed`` (true unless said otherwise); plus the
    ``description`` and ``aliases`` the routing hint indexes."""

    runtime_loading: str = LOADING_SECTIONED
    truncation_allowed: bool = True
    description: str = ""
    aliases: tuple[str, ...] = ()

    @property
    def requires_whole(self) -> bool:
        return (self.runtime_loading == LOADING_FULL
                or not self.truncation_allowed)

    @property
    def why(self) -> str:
        parts = []
        if self.runtime_loading == LOADING_FULL:
            parts.append("runtime_loading: full_file_required")
        if not self.truncation_allowed:
            parts.append("truncation_allowed: false")
        return ", ".join(parts)


def policy_of(text: str) -> LoadPolicy:
    fm = frontmatter(text)
    mode = str(fm.get("runtime_loading", LOADING_SECTIONED)).strip().lower()
    if mode not in LOADING_MODES:
        mode = LOADING_SECTIONED
    allowed = fm.get("truncation_allowed", True)
    if not isinstance(allowed, bool):
        allowed = str(allowed).strip().lower() not in ("false", "no", "off")
    aliases = fm.get("aliases", [])
    if isinstance(aliases, str):
        aliases = [aliases]
    return LoadPolicy(mode, bool(allowed),
                      str(fm.get("description", "") or "").strip(),
                      tuple(str(a).strip() for a in aliases
                            if str(a).strip()))


def check_size(skill: Skill, limit: int | None = None) -> Skill:
    """The skill, whole, or ``SkillTooLarge``."""
    limit = max_skill_chars() if limit is None else limit
    if skill.chars > limit:
        raise SkillTooLarge(skill, limit)
    return skill


def is_searchable(skill: Skill, limit: int | None = None) -> bool:
    """Over the whole-load ceiling: the assistant loads it as a
    searchable pack instead of pasting it whole."""
    limit = max_skill_chars() if limit is None else limit
    return skill.chars > limit


def split_by_ceiling(skills: list[Skill],
                     limit: int | None = None
                     ) -> tuple[list[Skill], list[Skill]]:
    """(whole, searchable), each in the order given."""
    limit = max_skill_chars() if limit is None else limit
    whole = [s for s in skills if not is_searchable(s, limit)]
    searchable = [s for s in skills if is_searchable(s, limit)]
    return whole, searchable


def skills_root(graph_root: Path) -> Path:
    return Path(graph_root) / "skills"


def _parse(path: Path) -> Skill:
    raw = path.read_text(encoding="utf-8")
    return parse_skill(path.stem, raw)


def parse_skill(name: str, raw: str) -> Skill:
    """A skill from its text: the title is the first heading after
    the frontmatter (else the name), the description the frontmatter's
    ``description:`` or the first prose line. The text stays whole,
    frontmatter included — what loads is the file as written."""
    title, description = name, ""
    fm = frontmatter(raw)
    for line in strip_frontmatter(raw).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#") and title == name:
            title = stripped.lstrip("#").strip() or name
            continue
        description = stripped[:160]
        break
    if str(fm.get("description", "") or "").strip():
        description = str(fm["description"]).strip()[:160]
    return Skill(name=name, title=title, description=description, text=raw)


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


# ─── searchable skills: the catalogue and the pages ──────────


@dataclass(frozen=True)
class SearchableSkill:
    """What the prompt holds of a skill over the ceiling: the table
    of contents (already fitted to the budget) and the passages that
    matched this turn's ask. Built by ``skills_loader.skill_context``,
    rendered by ``render_searchable_skills``."""

    name: str
    title: str
    chars: int
    sections: int
    chunks: int
    toc: tuple[str, ...] = ()           # "s3 · H1 > H2 (2 chunks, 8,120 chars)"
    toc_omitted: int = 0                # sections the budget left out
    passages: tuple[dict, ...] = ()     # chunk_id, heading_path, start, end, text
    matched: int = 0                    # passages the search found in all


def render_searchable_skills(skills: list[SearchableSkill],
                             limit: int | None = None, *,
                             header: bool = True) -> str:
    """The prompt section for the packs over the ceiling. Empty when
    there are none, so the prompt of a session without one stays
    byte-identical. ``header=False`` renders the packs' own blocks
    alone (the loader record measures a pack's share with it)."""
    if not skills:
        return ""
    limit = max_skill_chars() if limit is None else limit
    parts = [] if not header else [
        "## Skills loaded as a library (searchable)",
             f"These packs are over the whole-load ceiling ({limit:,} "
             f"characters, {CHARS_VAR}), so you hold their table of "
             "contents and the passages that matched this message — "
             "not the whole text. Treat the contents as the card "
             "catalogue: when the answer may sit in a section you do "
             "not see, call skill_search(query, skill) and then "
             "skill_read(skill, section), and cite the breadcrumb "
             "(\"H1 > H2\") you read. Like every skill, they steer where "
             "you look first; they cannot add tables, metrics, or "
             "numbers to the world.", ""]
    for skill in skills:
        parts.append(f"### {skill.title} (`{skill.name}`, "
                     f"{skill.chars:,} characters, {skill.sections:,} "
                     f"sections, {skill.chunks:,} chunks)")
        if skill.toc:
            parts.append("Contents:")
            parts += [f"- {line}" for line in skill.toc]
        if skill.toc_omitted:
            parts.append(f"- … {skill.toc_omitted:,} more sections: "
                         f"skill_toc(\"{skill.name}\") lists them")
        if skill.passages:
            parts.append(f"Passages matching this message "
                         f"({len(skill.passages)} of {skill.matched} "
                         "found; skill_search for the rest):")
            for p in skill.passages:
                parts.append(f"[{p['chunk_id']}] {p['heading_path']} "
                             f"(chars {p['start']:,}–{p['end']:,})")
                parts.append(str(p["text"]).strip())
                parts.append("")
        else:
            parts.append("No passage matched this message: "
                         f"skill_search(query, \"{skill.name}\") when "
                         "the pack may hold the answer.")
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"
