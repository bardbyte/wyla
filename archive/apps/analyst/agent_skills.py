"""Agent-skill loader — progressive disclosure, the way Claude loads skills.

The instruction carries only the skill INDEX (name + when-to-use, from
SKILL.md frontmatter). Bodies load on demand via the load_agent_skill
tool, so context is spent only on the skill the question actually needs.

These are the agent's OWN craft skills (how to respond); the domain
skills (roll rates, approval rates) live in the graph via the skills
loader — different layer, deliberately.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

_SKILLS_DIR = Path(__file__).parent / "skills"


def _parse_frontmatter(text: str) -> tuple[dict[str, str], str]:
    """Minimal ----delimited YAML frontmatter → (meta, body)."""
    if not text.startswith("---"):
        return {}, text
    try:
        _, fm, body = text.split("---", 2)
    except ValueError:
        return {}, text
    meta: dict[str, str] = {}
    key = None
    for line in fm.splitlines():
        if not line.strip():
            continue
        if ":" in line and not line.startswith((" ", "\t")):
            key, _, value = line.partition(":")
            meta[key.strip()] = value.strip()
        elif key:  # folded continuation line
            meta[key] = (meta[key] + " " + line.strip()).strip()
    return meta, body.lstrip("\n")


def list_agent_skills() -> dict[str, Any]:
    """Index of the agent's response-craft skills: name + when to use
    each. Cheap — call once per conversation, then load_agent_skill for
    the one the current answer needs."""
    skills = []
    for skill_md in sorted(_SKILLS_DIR.glob("*/SKILL.md")):
        meta, _ = _parse_frontmatter(
            skill_md.read_text(encoding="utf-8", errors="replace"))
        skills.append({
            "name": meta.get("name") or skill_md.parent.name,
            "description": meta.get("description", ""),
        })
    return {"status": "ok", "skills": skills}


def load_agent_skill(name: str) -> dict[str, Any]:
    """Full body of one response-craft skill (decision procedures, spec
    shapes, structure rules). Load BEFORE composing a data answer:
    response-design to pick the form, visualization to build chart specs,
    executive-communication for VP/C-suite framing."""
    wanted = name.strip().lower()
    for skill_md in sorted(_SKILLS_DIR.glob("*/SKILL.md")):
        meta, body = _parse_frontmatter(
            skill_md.read_text(encoding="utf-8", errors="replace"))
        skill_name = (meta.get("name") or skill_md.parent.name).lower()
        if skill_name == wanted or skill_md.parent.name.lower() == wanted:
            return {"status": "ok", "name": skill_name, "body": body}
    available = [s["name"] for s in list_agent_skills()["skills"]]
    return {"status": "error",
            "error": f"no skill named {name!r}; available: {available}"}
