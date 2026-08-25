"""The skills registry — business logic the agent consults, kept OUT of the
graph.

Two sources feed the agent: the GRAPH (warehouse truth — MDM + BQ + entities
+ metrics + lineage: *what exists and what a table is about*) and this
REGISTRY (business logic — definitions, metric contracts, analytical SQL,
and guardrails: *how to reason about it well*). The graph never carries a
skill-derived node; enforcement of skill guardrails is sourced from here so
the ``cm11_encrypted`` gate works with zero skill nodes in the data graph.

Bundles are the ``<skill_id>.json`` artifacts the skills loader writes; the
registry loads a directory of them (default ``<cache>/sources/skills``).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from synapse.graph.store import normalize_table_name


def load_registry(snapshot_path: "str | Path | None" = None) -> "SkillsRegistry":
    """Load the registry for a running service. Prefers ``SYNAPSE_SKILLS_DIR``,
    else the ``sources/skills`` dir next to the snapshot (where the loader
    stages the bundles). Empty when neither exists — callers that enforce
    guardrails MUST ensure the skills were staged."""
    d = os.environ.get("SYNAPSE_SKILLS_DIR")
    if not d and snapshot_path:
        d = str(Path(snapshot_path).parent / "sources" / "skills")
    return SkillsRegistry.from_dir(d)


class SkillsRegistry:
    """A read-only view over the loaded skill bundles + their guardrails."""

    def __init__(self, bundles: list[dict[str, Any]]) -> None:
        self.skills: list[dict[str, Any]] = [
            b for b in bundles if isinstance(b, dict) and b.get("skill_id")
        ]
        self.guardrails: list[dict[str, Any]] = self._flatten_guardrails(self.skills)

    # ── loading ──

    @classmethod
    def from_dir(cls, skills_dir: "str | Path | None") -> "SkillsRegistry":
        """Load every ``*.json`` skill bundle under ``skills_dir``. Absent or
        empty dir → an empty registry (no skills, no guardrails), never a
        crash — but note: an empty registry enforces NO guardrails, so the
        loader must have staged the skills for the gate to bite."""
        bundles: list[dict[str, Any]] = []
        if skills_dir:
            d = Path(skills_dir)
            if d.exists():
                for p in sorted(d.glob("*.json")):
                    try:
                        blob = json.loads(p.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        continue
                    if isinstance(blob, dict) and blob.get("skill_id"):
                        bundles.append(blob)
        return cls(bundles)

    @staticmethod
    def _flatten_guardrails(bundles: list[dict]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for b in bundles:
            tables = b.get("tables_used") or []
            for g in b.get("guardrails") or []:
                if isinstance(g, dict) and g.get("rule"):
                    out.append({
                        **g,
                        "skill_id": b.get("skill_id"),
                        "skill_tables": tables,
                    })
        return out

    # ── enforcement (security) ──

    def guardrails_for(self, target: str) -> list[dict[str, Any]]:
        """Guardrails relevant to a table, matched by NAME (no graph nodes).

        A guardrail is returned when the target table is one the guardrail's
        skill covers (``skill_tables``), OR an ``applies_to`` entry names the
        table (``table.column`` or ``table``), OR the rule is a
        machine-checkable rule with no table scope at all — a global
        never-expose that must fire wherever its column appears. Erring
        toward returning MORE is safe: validate_sql_plan re-checks each
        rule's banned column against the actual SQL, so an extra guardrail
        that doesn't match the columns simply never fires.
        """
        t = normalize_table_name(target)
        out: list[dict[str, Any]] = []
        for g in self.guardrails:
            skill_tables = {normalize_table_name(x)
                            for x in (g.get("skill_tables") or [])}
            applies_tables: set[str] = set()
            for a in g.get("applies_to") or []:
                a = str(a)
                if "." in a:
                    applies_tables.add(normalize_table_name(a.rsplit(".", 1)[0]))
            is_global = (bool(g.get("machine_checkable"))
                         and not skill_tables and not applies_tables)
            if t in skill_tables or t in applies_tables or is_global:
                out.append(g)
        return out

    def all_guardrails(self) -> list[dict[str, Any]]:
        return list(self.guardrails)

    # ── knowledge (get_skill) ──

    def find_skill(self, topic: str) -> dict[str, Any] | None:
        """Best skill bundle for a topic — token overlap over skill_id,
        domain, description, and covered tables. Returns the full bundle
        (the agent reads it all: knowledge, metric contracts, guardrails)."""
        want = {w for w in _tokens(topic) if w}
        if not want:
            return self.skills[0] if self.skills else None
        best: tuple[float, dict] | None = None
        for b in self.skills:
            hay = " ".join([
                str(b.get("skill_id", "")), str(b.get("domain", "")),
                str(b.get("company_domain", "")),
                str(b.get("description", "")),
                " ".join(str(t) for t in b.get("tables_used") or []),
            ]).lower()
            hay_tokens = set(_tokens(hay))
            score = len(want & hay_tokens)
            if str(b.get("skill_id", "")).lower().replace("_", " ") in topic.lower():
                score += 5
            if score and (best is None or score > best[0]):
                best = (score, b)
        return best[1] if best else None


def _tokens(text: str) -> list[str]:
    return [w for w in "".join(
        c if c.isalnum() else " " for c in str(text).lower()).split()
        if len(w) > 2]
