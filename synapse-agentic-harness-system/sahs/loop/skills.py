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
  * **a skill loads whole when it fits, as a library when it does
    not — never a refusal.** The size ceiling and the count ceiling
    come from the silo ``.env`` (``SAHS_MAX_SKILL_CHARS``,
    ``SAHS_MAX_LOADED_SKILLS``); the defaults keep a whole-loaded
    skill a briefing and a session to a few. A skill within the
    ceiling enters the prompt whole, verbatim. A skill over it is
    still LISTED — the shelf never hides it — and loads as a LIBRARY:
    its table of contents plus the passages that match the ask, under
    a per-model budget (``sahs.loop.skill_index``,
    ``render_searchable_skills``). The assistant adds tools to read
    any section; the v1 navigator, which has none, holds the same
    block as static retrieval. A frontmatter that asks for the whole
    file (``runtime_loading: full_file_required``) is a preference:
    honoured whenever the pack fits, and disclosed as ``preferred:
    whole`` on the block and the loader record when it does not.
    Governed knowledge is never truncated in silence: what the model
    holds is labeled as the catalogue and the pages it asked for,
    never as the whole book. The only load that refuses is a file
    that cannot be read (``SkillUnreadable``) — broken input, never
    size.
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
    error: str = ""         # why the file could not be read; '' when it could

    @property
    def chars(self) -> int:
        return len(self.text)


class SkillUnreadable(ValueError):
    """The one load that refuses: a skill file that cannot be read or
    decoded (broken input, never size). Named with the reason so the
    picker shows it; the shelf still lists the file, so nothing is
    hidden. A large-but-valid pack never raises this — it loads as a
    library."""

    def __init__(self, skill: Skill) -> None:
        self.skill = skill
        super().__init__(
            f"skill {skill.name!r} cannot be loaded: {skill.error}. "
            "Fix the file (it must be UTF-8 markdown) and load it "
            "again.")


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


def check_readable(skill: Skill) -> Skill:
    """The skill, or ``SkillUnreadable`` when its file could not be
    read — the only refusal left in skill loading."""
    if skill.error:
        raise SkillUnreadable(skill)
    return skill


def is_searchable(skill: Skill, limit: int | None = None) -> bool:
    """Over the whole-load ceiling: the skill loads as a searchable
    library instead of being pasted whole."""
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
    """The file as a skill. A file that cannot be read or is not
    UTF-8 still LISTS — name, an empty text and the reason in
    ``error`` — and refuses to LOAD by name (``SkillUnreadable``);
    it is never decoded with substitutions, which would change the
    words in silence."""
    try:
        raw = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as e:
        reason = f"not UTF-8 ({e.reason} at byte {e.start})"
        return Skill(name=path.stem, title=path.stem,
                     description=f"unreadable: {reason}", text="",
                     error=reason)
    except OSError as e:
        reason = f"cannot read {path.name}: {e.strerror or e}"
        return Skill(name=path.stem, title=path.stem,
                     description=f"unreadable: {reason}", text="",
                     error=reason)
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
    into empty skills. Size never refuses: every skill comes back
    whole, and the prompt builder splits the list at the whole-load
    limit (``split_by_ceiling``) — whole when it fits, a library when
    it does not. A file that cannot be read raises
    ``SkillUnreadable`` (broken input, by name)."""
    available = {s.name: s for s in list_skills(graph_root)}
    loaded, missing = [], []
    for name in names[:max_loaded()]:
        skill = available.get(name)
        if skill is None:
            missing.append(name)
        else:
            loaded.append(check_readable(skill))
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


PREFERRED_WHOLE = "whole"          # the frontmatter asked for the file
PREFERRED_SECTIONED = "sectioned"  # the default: a library is fine


@dataclass(frozen=True)
class SearchableSkill:
    """What the prompt holds of a skill over the ceiling: the table
    of contents (already fitted to the budget) and the passages that
    matched this turn's ask. Built by ``skills_loader.skill_context``,
    rendered by ``render_searchable_skills``. ``preferred`` is the
    frontmatter's word (whole or sectioned) and ``reason`` the sizes
    that put it in the library — both disclosed on the block."""

    name: str
    title: str
    chars: int
    sections: int
    chunks: int
    toc: tuple[str, ...] = ()           # "s3 · H1 > H2 (2 chunks, 8,120 chars)"
    toc_omitted: int = 0                # sections the budget left out
    passages: tuple[dict, ...] = ()     # chunk_id, heading_path, start, end, text
    matched: int = 0                    # passages the search found in all
    preferred: str = PREFERRED_SECTIONED
    reason: str = ""


def library_reason(skill: Skill, limit: int, model_name: str = "",
                   why: str = "") -> str:
    """Why a pack is a library this turn, in sizes: its characters
    against the limit that bound — the global ceiling
    (``SAHS_MAX_SKILL_CHARS``) when the limit is that, else the
    engine's own whole-load window — and the frontmatter's word when
    it asked for the whole file."""
    if limit >= max_skill_chars():
        whose = f"the whole-load ceiling ({CHARS_VAR})"
    else:
        whose = (f"this model's ({model_name or 'unnamed engine'}) "
                 "whole-load limit")
    asked = f" ({why})" if why else ""
    return (f"{skill.chars:,} characters{asked} over {whose} of "
            f"{limit:,}: loaded as a library, the whole text reachable "
            "by section, nothing cut")


def render_searchable_skills(skills: list[SearchableSkill],
                             limit: int | None = None, *,
                             header: bool = True,
                             tools: bool = True) -> str:
    """The prompt section for the packs over the ceiling. Empty when
    there are none, so the prompt of a session without one stays
    byte-identical. ``header=False`` renders the packs' own blocks
    alone (the loader record measures a pack's share with it).
    ``tools=False`` is the v1 navigator's static retrieval: the same
    contents and passages, with no lookup tool to name."""
    if not skills:
        return ""
    limit = max_skill_chars() if limit is None else limit
    if tools:
        follow = ("Treat the contents as the card catalogue: when the "
                  "answer may sit in a section you do not see, call "
                  "skill_search(query, skill) and then skill_read(skill, "
                  "section), and cite the breadcrumb (\"H1 > H2\") you "
                  "read.")
    else:
        follow = ("Treat the contents as the card catalogue: this lane "
                  "has no lookup tool, so when the answer may sit in a "
                  "section you do not hold, name that section by its "
                  "breadcrumb (\"H1 > H2\") rather than guessing its "
                  "words.")
    parts = [] if not header else [
        "## Skills loaded as a library (searchable)",
             f"These packs are over the whole-load ceiling ({limit:,} "
             f"characters, {CHARS_VAR}), so you hold their table of "
             "contents and the passages that matched this message — "
             f"not the whole text. {follow} Like every skill, they "
             "steer where you look first; they cannot add tables, "
             "metrics, or numbers to the world.", ""]
    for skill in skills:
        parts.append(f"### {skill.title} (`{skill.name}`, "
                     f"{skill.chars:,} characters, {skill.sections:,} "
                     f"sections, {skill.chunks:,} chunks)")
        parts.append(f"mode: library · preferred: {skill.preferred}"
                     + (f" — {skill.reason}" if skill.reason else ""))
        if skill.toc:
            parts.append("Contents:")
            parts += [f"- {line}" for line in skill.toc]
        if skill.toc_omitted:
            parts.append(f"- … {skill.toc_omitted:,} more sections"
                         + (f": skill_toc(\"{skill.name}\") lists them"
                            if tools else " (not listed here)"))
        if skill.passages:
            parts.append(f"Passages matching this message "
                         f"({len(skill.passages)} of {skill.matched} "
                         + ("found; skill_search for the rest):" if tools
                            else "found):"))
            for p in skill.passages:
                parts.append(f"[{p['chunk_id']}] {p['heading_path']} "
                             f"(chars {p['start']:,}–{p['end']:,})")
                parts.append(str(p["text"]).strip())
                parts.append("")
        else:
            parts.append("No passage matched this message"
                         + (f": skill_search(query, \"{skill.name}\") "
                            "when the pack may hold the answer."
                            if tools else "."))
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"
