"""Domain sub-agents — one specialist per company domain, self-identified.

The graph's Domain nodes (built by the rollup stage from MDM
ownership + the mined/DMP catalogs, or the steward's --domain-tags map)
define WHO the specialists are; this module turns each into a scoped ADK
sub-agent whose instruction embeds only what the graph can prove about
its segment: the derived unit profile, its member tables with trust
tiers, its most-used metrics, and the skill playbooks that cover it.

The root analyst stays the generalist front door: with sub-agents
attached (SYNAPSE_DOMAIN_AGENTS=1), ADK's transfer mechanism lets it
first identify the company domain a question belongs to — route_question
is the tool-shaped version of the same decision — and hand the thread to
the specialist that knows that segment's tables and playbooks.

Arranging skills per domain (for stewards):
  - explicit: add `company_domain: Credit Risk` to a skill.yaml — the
    bundle pins to that unit regardless of tables;
  - implicit: leave it out — the skill attaches to any unit whose member
    tables intersect its tables_used.

``build_domain_agent_specs`` is pure (no ADK import) so the composition
is testable anywhere; ``build_domain_agents`` adapts specs to real ADK
Agents at the edge.
"""

from __future__ import annotations

import re
from typing import Any


def _slug(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return f"{s or 'unit'}_specialist"


def _spec_for_unit(service: Any, unit: Any) -> dict[str, Any]:
    props = unit.properties
    name = str(props.get("name", ""))
    members = [
        service.store.get(e.to_uri)
        for e in service.store.outgoing(unit.canonical_uri, "CONTAINS")
    ]
    members = [m for m in members if m is not None]
    table_lines = [
        f"  - {m.properties.get('table_name')}"
        + (f" ({m.properties.get('business_name')})"
           if m.properties.get("business_name") else "")
        + f" — tier {m.provenance.confidence_tier}"
        for m in sorted(members,
                        key=lambda t: str(t.properties.get("table_name")))
    ]
    member_names = {
        str(m.properties.get("table_name", "")).lower() for m in members}
    skills = service._skills_for_unit(name, member_names)
    skill_lines = [
        f"  - {s['skill_id']}"
        + (f" [{s['domain']}]" if s.get("domain") else "")
        + (f": {s['description']}" if s.get("description") else "")
        for s in skills
    ]
    top_metrics = props.get("top_metrics") or []
    lines: list[str | None] = [
        f"You are the {name} domain specialist over the semantic "
        "knowledge graph.",
        "",
        f"Your segment, as the evidence describes it: "
        f"{props.get('description', '')}",
        "",
        "Tables you own (stay inside them unless a join edge leads out):",
        *table_lines,
        f"Most-used metrics here: {', '.join(map(str, top_metrics))}."
        if top_metrics else None,
        "",
        "Skill playbooks that govern this segment — load with get_skill "
        "BEFORE improvising an approach:" if skill_lines else None,
        *skill_lines,
        "",
        "Operating rules, same as the general analyst: ground every "
        "claim in graph tools and cite trust tiers; guardrails bind; "
        "validate_sql_plan before showing SQL; prefer "
        "search_entities(query, domain=" + repr(name) + ") so "
        "results stay inside your segment. If the question clearly "
        "belongs to another company domain, say so and transfer back to "
        "the root analyst instead of guessing.",
    ]
    instruction = "\n".join(p for p in lines if p is not None)
    return {
        "name": _slug(name),
        "domain": name,
        "description": (
            f"Specialist for the {name} company domain "
            f"({props.get('table_count', len(members))} tables; "
            f"knows its tables, metrics, and playbooks)."),
        "instruction": instruction,
        "n_tables": len(members),
        "n_skills": len(skills),
    }


def build_domain_agent_specs(service: Any) -> list[dict[str, Any]]:
    """One spec per Domain node, largest domain first. Pure
    composition — returns plain dicts, imports nothing from ADK."""
    units = service.store.nodes_by_type("Domain")
    specs = [_spec_for_unit(service, u) for u in units]
    specs.sort(key=lambda s: (-s["n_tables"], s["name"]))
    return specs


def build_domain_agents(model: str, tools: list[Any],
                        service: Any) -> list[Any]:
    """Adapt specs to google-adk Agents (call only when ADK is present).
    Each specialist carries the same tool roster as the root — scoping
    lives in the instruction + domain filters, not in capability
    removal, so a specialist can still follow a join across segments
    when the graph proves one."""
    from google.adk import Agent

    return [
        Agent(
            model=model,
            name=spec["name"],
            description=spec["description"],
            instruction=spec["instruction"],
            tools=tools,
        )
        for spec in build_domain_agent_specs(service)
    ]
