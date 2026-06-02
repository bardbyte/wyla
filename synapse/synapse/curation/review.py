"""Render ENTITIES_FOR_REVIEW.md — the human-readable approval surface.

Layout (per entity):
    ## Entity: <CanonicalName>     <confidence pill>
    - description
    - source evidence (tables, columns, metrics, acronyms, data_categories)
    - properties
    - parent / relationships
    - LLM notes
    - [ ] approve   [ ] modify   [ ] reject

Plus top-of-file ambiguities + scope observations the human must
resolve before approving.
"""

from __future__ import annotations

from synapse.registry import CurationProposal


def _confidence_pill(conf: float) -> str:
    if conf >= 0.9:
        return "🟢 high"
    if conf >= 0.7:
        return "🟡 medium"
    return "🔴 low"


def _list_or_dash(items: list[str], max_items: int = 12) -> str:
    if not items:
        return "—"
    shown = items[:max_items]
    suffix = f" (+{len(items) - max_items} more)" if len(items) > max_items else ""
    return ", ".join(f"`{i}`" for i in shown) + suffix


def render_review_markdown(proposal: CurationProposal) -> str:
    """Render the proposal as a markdown document with approval checkboxes."""
    lines: list[str] = []
    lines.append("# Synapse — Entity Registry Proposal\n")
    lines.append(f"_Generated_: `{proposal.generated_at}`")
    lines.append(f"_Model_: `{proposal.model_used or 'unknown'}`")
    lines.append(f"_Prompt SHA256_: `{proposal.prompt_sha256[:16] or 'n/a'}…`")
    lines.append(f"_Proposed entities_: **{len(proposal.proposed_entities)}**\n")

    if proposal.scope_observations:
        lines.append("## Scope observations\n")
        for o in proposal.scope_observations:
            lines.append(f"- {o}")
        lines.append("")

    if proposal.ambiguities_flagged:
        lines.append("## ⚠ Ambiguities to resolve\n")
        for a in proposal.ambiguities_flagged:
            lines.append(f"- {a}")
        lines.append("")

    lines.append("## Proposed entities\n")
    lines.append(
        "_Tick `[x]` on `approve` / `modify` / `reject` per entity. "
        "Edit names/descriptions inline if needed; the registry script "
        "reads back the modified file._\n",
    )

    for i, e in enumerate(proposal.proposed_entities, start=1):
        lines.append(
            f"### {i}. `{e.canonical_name}`  {_confidence_pill(e.llm_confidence)} "
            f"(`{e.llm_confidence:.2f}`)\n",
        )
        lines.append(f"**Description**  {e.description}\n")

        ev = e.source_evidence or {}
        lines.append("**Source evidence**")
        lines.append(f"- tables: {_list_or_dash(ev.get('tables', []))}")
        lines.append(f"- columns: {_list_or_dash(ev.get('columns', []))}")
        lines.append(f"- metrics: {_list_or_dash(ev.get('metrics', []))}")
        lines.append(f"- acronyms: {_list_or_dash(ev.get('acronyms', []))}")
        lines.append(
            f"- data_categories: {_list_or_dash(ev.get('data_categories', []))}\n",
        )

        props = e.properties or {}
        if props:
            lines.append("**Properties**")
            for k, v in props.items():
                if isinstance(v, list):
                    lines.append(f"- {k}: {_list_or_dash(v)}")
                else:
                    lines.append(f"- {k}: `{v}`")
            lines.append("")

        if e.parent_entity:
            lines.append(f"**Parent entity**  `{e.parent_entity}`\n")
        if e.relationships:
            lines.append("**Relationships**")
            for r in e.relationships:
                via = f" via `{r.via_column}`" if r.via_column else ""
                ev_note = f" — {r.cardinality_evidence}" if r.cardinality_evidence else ""
                lines.append(
                    f"- {r.type} → `{r.target_entity}`{via}{ev_note}",
                )
            lines.append("")

        if e.human_review_notes:
            lines.append(f"**LLM review notes**  {e.human_review_notes}\n")

        lines.append("**Decision**  `[ ] approve`  `[ ] modify`  `[ ] reject`\n")
        lines.append("---\n")

    lines.append("## Approval workflow\n")
    lines.append(
        "1. For each entity above, tick exactly one of `approve` / "
        "`modify` / `reject`.\n"
        "2. If `modify`, edit `canonical_name`, `description`, "
        "`source_evidence`, or `properties` inline.\n"
        "3. Resolve every ambiguity listed at the top.\n"
        "4. Run `python -m synapse.scripts.finalize_entities "
        "review_queue/ENTITIES_FOR_REVIEW.md` (next step — to be built) "
        "to commit the approved registry to "
        "`data/registries/entities.yaml`.\n",
    )

    return "\n".join(lines)
