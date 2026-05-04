"""Stage 5: Enrich one table via Gemini 3.1 Pro (Vertex direct, no SafeChain).

One :class:`google.adk.agents.LlmAgent` per APPROVED table. Each call:

  1. Loads ``lumi/prompts/enrich_view.md`` (the canonical enrichment prompt).
  2. Interpolates context: TableContext, ecosystem brief, learnings, the
     approved plan as scope contract, BQ project/dataset. MDM columns are
     trimmed to a relevant subset by :func:`_select_relevant_mdm_columns`
     (cap ~50) so wide tables do not blow the prompt budget.
  3. Appends sections 1-5 of ``.claude/skills/lookml/SKILL.md`` verbatim
     plus a compressed view of sections 6-7 (1000th-query rules +
     anti-patterns). Section 5 (Refinements / additive merge pattern) is
     non-optional — CLAUDE.md rule 6 ("merge into existing LookML, never
     regenerate, additive only") depends on it.
  4. Runs the agent with ``output_schema=EnrichedOutput`` and ``temperature=0``.
  5. Validates the response against :func:`lumi.guardrails.check_enrichment`.
     If the gate FAILS (blocking), retries ONCE with the failure messages
     appended to the prompt as a self-repair loop. Cap is 2 attempts total.
  6. Returns the parsed :class:`EnrichedOutput`.

The agent does NOT regenerate the view from scratch. The prompt is explicit:
the existing baseline LookML is included and only ADDITIVE merges are
allowed (rule 6 in CLAUDE.md). The approved plan acts as scope contract —
do not invent dimensions/measures the human did not approve.

Tests mock the LLM call by monkey-patching :func:`_invoke_enrichment_agent`
to return a fixture :class:`EnrichedOutput`; the rest of the pipeline
(prompt assembly, guardrail checks, self-repair) runs unchanged.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from google.adk.agents import LlmAgent
from google.genai.types import GenerateContentConfig

from lumi.config import LumiConfig
from lumi.schemas import EnrichedOutput, EnrichmentPlan, TableContext

logger = logging.getLogger("lumi.enrich")

# ─── Module-level paths ─────────────────────────────────────────────

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parent.parent  # lumi_final/
_PROMPT_PATH = _THIS_FILE.parent / "prompts" / "enrich_view.md"
_SKILL_PATH = _REPO_ROOT / ".claude" / "skills" / "lookml" / "SKILL.md"
_LEARNINGS_PATH_DEFAULT = _REPO_ROOT / "data" / "learnings.md"


# ─── Public API ─────────────────────────────────────────────────────


def enrich_table(
    table_context: TableContext,
    approved_plan: EnrichmentPlan,
    model: str | None = None,
    config: LumiConfig | None = None,
    max_attempts: int = 2,
) -> EnrichedOutput:
    """Run one enrichment LLM call for ``table_context.table_name``.

    On a blocking guardrail failure the call is retried ONCE with the failure
    messages appended to the prompt (self-repair loop). The retry uses a
    fresh agent built from the augmented prompt.

    Args:
        table_context: Output of ``sql_to_context.discover_tables``.
        approved_plan: The plan the human approved. Treated as scope contract.
        model: Override the model id (default: ``LumiConfig.model_name``).
        config: Optional :class:`LumiConfig` for project/dataset/temperature.
        max_attempts: Total invocations allowed (default 2 = first try +
            one self-repair attempt). Set to 1 to disable repair.

    Returns:
        :class:`EnrichedOutput` with view_lkml, derived tables, explore,
        catalogs, and NL questions. The returned output is the LAST attempt
        even if it still has blocking failures — the caller can re-check.
    """
    # Imported here to avoid a hard cycle at import time.
    from lumi.guardrails import check_enrichment

    cfg = config or LumiConfig()
    model_id = model or cfg.model_name
    base_prompt = build_enrichment_prompt(table_context, approved_plan, config=cfg)
    logger.info(
        "Enriching %s — prompt %d chars, plan dims=%d measures=%d",
        table_context.table_name,
        len(base_prompt),
        len(approved_plan.proposed_dimensions),
        len(approved_plan.proposed_measures),
    )

    last_result: EnrichedOutput | None = None
    prompt = base_prompt
    for attempt in range(1, max_attempts + 1):
        agent = _build_agent_with_instruction(
            table_context.table_name, prompt, model_id, cfg
        )
        last_result = _invoke_enrichment_agent(agent, prompt, table_context.table_name)
        gate = check_enrichment(table_context.table_name, last_result, table_context)
        if gate.status != "fail":
            if attempt > 1:
                logger.info(
                    "Self-repair succeeded for %s on attempt %d",
                    table_context.table_name,
                    attempt,
                )
            return last_result

        if attempt < max_attempts:
            logger.warning(
                "Enrichment for %s failed guardrail on attempt %d (%d blocking) — "
                "retrying with self-repair appendix",
                table_context.table_name,
                attempt,
                len(gate.blocking_failures),
            )
            prompt = _append_repair_instructions(base_prompt, gate.blocking_failures)
        else:
            logger.error(
                "Enrichment for %s exhausted %d attempts; returning last result "
                "with %d blocking failures so the caller can decide",
                table_context.table_name,
                max_attempts,
                len(gate.blocking_failures),
            )

    # Defensive — loop always assigns last_result, but mypy needs the guard.
    assert last_result is not None
    return last_result


def _append_repair_instructions(base_prompt: str, blocking: list[str]) -> str:
    """Glue a self-repair appendix onto the base prompt.

    The appendix is intentionally short and imperative: list the exact
    blocking failures and tell the model to fix them while keeping every
    other field of the previous response intact (we cannot show the model
    its previous response without doubling the prompt size, but the gate
    failure messages already pinpoint the exact field/check).
    """
    bullets = "\n".join(f"  - {b}" for b in blocking)
    appendix = (
        "\n\n## SELF-REPAIR — your previous attempt failed these blocking checks\n"
        f"{bullets}\n\n"
        "Re-emit the FULL EnrichedOutput JSON. Fix each blocking check above. "
        "Keep everything else exactly as you had it. Pay particular attention "
        "to: primary_key on every view, dimension_group (not dimension) on every "
        "date column, structural filters baked INTO derived_table SQL, joins in "
        "topological order. Do not invent new dimensions or measures."
    )
    return base_prompt + appendix


def _build_agent_with_instruction(
    table_name: str,
    instruction: str,
    model_id: str,
    cfg: LumiConfig,
) -> LlmAgent:
    """Build an :class:`LlmAgent` with a pre-rendered instruction string.

    Used by both the first-try path and the self-repair path so the agent
    always carries the exact prompt the LLM saw.
    """
    return LlmAgent(
        name=f"enrich_{_safe_agent_name(table_name)}",
        model=model_id,
        instruction=instruction,
        output_schema=EnrichedOutput,
        generate_content_config=GenerateContentConfig(temperature=cfg.temperature),
    )


def build_enrich_agent(
    table_context: TableContext,
    plan: EnrichmentPlan,
    model: str | None = None,
    config: LumiConfig | None = None,
) -> LlmAgent:
    """Construct the :class:`LlmAgent` for one table.

    The full interpolated prompt becomes the agent's ``instruction`` so the
    agent can be re-used (or rendered for debugging) without re-assembling
    context. Output schema is :class:`EnrichedOutput`; temperature is 0 per
    the project rule.
    """
    cfg = config or LumiConfig()
    model_id = model or cfg.model_name
    instruction = build_enrichment_prompt(table_context, plan, config=cfg)
    return _build_agent_with_instruction(
        table_context.table_name, instruction, model_id, cfg
    )


# ─── Prompt assembly ────────────────────────────────────────────────


def build_enrichment_prompt(
    table_context: TableContext,
    plan: EnrichmentPlan,
    config: LumiConfig | None = None,
) -> str:
    """Render the enrichment prompt with all placeholders interpolated.

    The base template lives at ``lumi/prompts/enrich_view.md``. After
    interpolation we APPEND the LookML SKILL excerpt + the approved plan
    contract. The plan goes near the top of the appended block because the
    LLM tends to weight late-prompt content highly when generating long
    structured output.
    """
    cfg = config or LumiConfig()
    template = _PROMPT_PATH.read_text(encoding="utf-8")

    placeholders = {
        "{table_name}": table_context.table_name,
        "{table_mdm_description}": table_context.mdm_table_description
        or "(no MDM description available)",
        "{selected_mdm_columns}": _render_mdm_columns(table_context),
        "{fingerprint_summary}": _render_fingerprint_summary(table_context),
        "{ecosystem_brief}": _render_ecosystem_brief(table_context),
        "{table_specific_learnings}": _load_learnings(table_context.table_name),
        "{existing_view_lkml}": table_context.existing_view_lkml
        or "(no existing baseline view — generate a fresh view)",
        "{bq_project}": cfg.bq_project,
        "{bq_dataset}": cfg.bq_dataset,
    }
    rendered = _interpolate(template, placeholders)

    return "\n\n".join(
        [
            rendered,
            "## Approved enrichment plan (scope contract — do not exceed)",
            _render_plan_contract(plan),
            "## Baseline gap analysis (drives surgical enrichment scope)",
            _render_baseline_gaps(table_context),
            "## LookML patterns reference (from .claude/skills/lookml/SKILL.md)",
            _load_skill_excerpt(),
        ]
    )


def _render_baseline_gaps(ctx: TableContext) -> str:
    """Surface what the baseline lacks so the LLM enriches surgically.

    Auto-generated Looker baselines are full of one-line "Customer ID"-style
    descriptions and missing labels. Telling the LLM which fields are stubs
    vs already-curated lets it spend tokens where it matters and stay out of
    fields a human already wrote good copy for.
    """
    sig = ctx.baseline_quality_signals or {}
    if not sig:
        return (
            "(no baseline view available — produce a complete enriched view "
            "from scratch using the fingerprint + MDM context above)"
        )

    parts: list[str] = []
    parts.append(
        f"Baseline shape: {sig.get('dims_total', 0)} dimensions, "
        f"{sig.get('measures_total', 0)} measures, "
        f"primary_key={'yes' if sig.get('has_primary_key') else 'NO — must add one'}"
    )
    parts.append("")
    gaps: list[str] = []
    if sig.get("dims_missing_description", 0):
        gaps.append(
            f"- {sig['dims_missing_description']} dimensions have NO description"
        )
    if sig.get("dims_short_description", 0):
        gaps.append(
            f"- {sig['dims_short_description']} dimensions have a stub "
            "description (< 30 chars — likely auto-generated; fine to enrich)"
        )
    if sig.get("dims_missing_label", 0):
        gaps.append(f"- {sig['dims_missing_label']} dimensions have NO label")
    if sig.get("dims_missing_tags", 0):
        gaps.append(
            f"- {sig['dims_missing_tags']} dimensions have NO tags "
            "(add Radix-friendly synonyms)"
        )
    if sig.get("measures_missing_value_format", 0):
        gaps.append(
            f"- {sig['measures_missing_value_format']} measures have no "
            "value_format_name"
        )
    if sig.get("dates_as_plain_dim", 0):
        gaps.append(
            f"- {sig['dates_as_plain_dim']} date column(s) are still plain "
            "dimensions — must be promoted to dimension_group"
        )
    if not sig.get("has_primary_key"):
        gaps.append(
            "- NO primary_key dimension — pick one (look for *_id, *_xref_id, "
            "or the JOIN-on column)"
        )
    if gaps:
        parts.append("Gaps to fix (surgical enrichment scope):")
        parts.extend(gaps)
    else:
        parts.append("No obvious gaps — only enrich if you can materially improve.")

    parts.append("")
    parts.append(
        "MERGE POLICY: descriptions ≥ 30 chars are assumed human-curated — "
        "DO NOT touch them. If you genuinely think one is wrong (not just terse), "
        "put your alternative on `proposed_overwrites` (do NOT silently overwrite). "
        "Tags are cumulative — always safe to add useful synonyms."
    )
    return "\n".join(parts)


def _interpolate(template: str, placeholders: dict[str, str]) -> str:
    """Replace ``{placeholder}`` tokens in the template.

    We use literal string replacement (not :meth:`str.format`) because the
    template body contains JSON examples with their own ``{...}`` braces —
    ``str.format`` would choke on them.
    """
    out = template
    for needle, value in placeholders.items():
        out = out.replace(needle, value)
    return out


def _select_relevant_mdm_columns(
    ctx: TableContext,
    cap: int = 50,
) -> list[str]:
    """Return the union of "interesting" column names, capped at ``cap``.

    Wide MDM responses (the largest table we have indexed has 193 columns)
    would blow the prompt budget if we dumped every row. The relevant set is
    the union of:

      - columns referenced by the input SQLs
      - columns that appear in JOIN ON clauses (left or right key)
      - columns that appear in any WHERE filter on this table
      - columns that appear in any aggregation on this table
      - columns referenced by CASE WHEN derivations on this table
      - date columns (always interesting — promoted to dimension_group)

    The cap is a soft guardrail. If the union exceeds ``cap`` we keep
    columns_referenced first (they are the strongest signal), then add the
    rest in priority order until full. The cap defaults to 50 — large
    enough to fit complex tables, small enough to keep prompts under
    ~30K tokens of MDM context.
    """
    relevant: list[str] = []
    seen: set[str] = set()

    def _add(col: str | None) -> None:
        if not col or col in seen:
            return
        seen.add(col)
        relevant.append(col)

    for col in ctx.columns_referenced:
        _add(col)
    for j in ctx.joins_involving_this:
        # Either side of the join key may live on this table; the discoverer
        # does not always normalise. Add both — _render_mdm_columns will only
        # surface ones MDM actually has.
        _add(j.get("left_key"))
        _add(j.get("right_key"))
    for f in ctx.filters_on_this:
        _add(f.get("column"))
    for a in ctx.aggregations:
        _add(a.get("column"))
    for cw in ctx.case_whens:
        _add(cw.get("source_column"))
    for d in ctx.date_functions:
        _add(d.get("column"))

    if len(relevant) > cap:
        logger.info(
            "MDM column set for %s capped from %d to %d",
            ctx.table_name,
            len(relevant),
            cap,
        )
        relevant = relevant[:cap]
    return relevant


def _render_mdm_columns(ctx: TableContext) -> str:
    """Markdown table of relevant columns plus their MDM rows.

    Wraps :func:`_select_relevant_mdm_columns` so wide tables don't blow the
    prompt budget. Columns the relevant filter picked up but MDM has nothing
    for show as ``(no MDM metadata)`` — the LLM can flag them or generate
    descriptions from the SQL context alone.
    """
    relevant = _select_relevant_mdm_columns(ctx)
    if not relevant:
        return "(no relevant columns identified for this table)"

    by_name: dict[str, dict[str, Any]] = {
        c.get("name", ""): c for c in ctx.mdm_columns if isinstance(c, dict)
    }

    lines = ["| Column | Type | Business name | Description |", "|---|---|---|---|"]
    for col in relevant:
        meta = by_name.get(col, {})
        c_type = meta.get("type") or meta.get("attribute_type") or "—"
        bname = meta.get("business_name") or "—"
        desc = meta.get("description") or meta.get("attribute_desc") or "(no MDM metadata)"
        # Squash newlines/pipes that would corrupt the table.
        desc_safe = desc.replace("|", "\\|").replace("\n", " ").strip()
        lines.append(f"| `{col}` | {c_type} | {bname} | {desc_safe} |")
    return "\n".join(lines)


def _render_fingerprint_summary(ctx: TableContext) -> str:
    """Compact bullet list of what sqlglot extracted, grouped by kind."""
    parts: list[str] = []

    if ctx.aggregations:
        agg_lines = [
            f"  - {a.get('function', '?')}({a.get('column', '?')})"
            + (f" → alias `{a['alias']}`" if a.get("alias") else "")
            for a in ctx.aggregations
        ]
        parts.append("**Aggregations:**\n" + "\n".join(agg_lines))

    if ctx.case_whens:
        cw_lines = []
        for cw in ctx.case_whens:
            mapped = cw.get("mapped_values") or []
            preview = ", ".join(
                f"{m.get('then', '?')}" for m in mapped[:5]
            )
            cw_lines.append(
                f"  - alias `{cw.get('alias', '?')}` "
                f"on column `{cw.get('source_column', '?')}` "
                f"→ buckets: {preview}"
            )
        parts.append("**CASE WHEN derivations:**\n" + "\n".join(cw_lines))

    if ctx.date_functions:
        df_lines = [
            f"  - {d.get('function', '?')} on `{d.get('column', '?')}`"
            for d in ctx.date_functions
        ]
        parts.append("**Date functions:**\n" + "\n".join(df_lines))

    if ctx.filters_on_this:
        f_lines = []
        for f in ctx.filters_on_this[:30]:  # cap to keep prompt bounded
            tag = " [structural]" if f.get("is_structural") else ""
            f_lines.append(
                f"  - `{f.get('column', '?')}` {f.get('operator', '?')} "
                f"{f.get('value', '?')}{tag}"
            )
        parts.append("**Filters:**\n" + "\n".join(f_lines))

    if ctx.ctes_referencing_this:
        cte_lines = [
            f"  - `{c.get('alias', '?')}` "
            f"({len(c.get('structural_filters') or [])} structural filter(s))"
            for c in ctx.ctes_referencing_this
        ]
        parts.append("**CTEs reading this table:**\n" + "\n".join(cte_lines))

    if ctx.joins_involving_this:
        j_lines = []
        for j in sorted(ctx.joins_involving_this, key=lambda x: x.get("order", 0)):
            j_lines.append(
                f"  - position {j.get('order', '?')}: "
                f"{j.get('join_type', 'inner')} JOIN `{j.get('other_table', '?')}` "
                f"ON {j.get('left_table') or '?'}.{j.get('left_key', '?')} "
                f"= {j.get('right_table', '?')}.{j.get('right_key', '?')}"
            )
        parts.append("**Joins (in SQL position order):**\n" + "\n".join(j_lines))

    if ctx.queries_using_this:
        parts.append(
            "**Source queries:** " + ", ".join(ctx.queries_using_this)
        )

    return "\n\n".join(parts) if parts else "(no SQL fingerprints — table only present via baseline)"


def _render_ecosystem_brief(ctx: TableContext) -> str:
    """3-5 line summary of how this table connects to its neighbours.

    Pulls from ``joins_involving_this`` (sibling tables) and
    ``ctes_referencing_this`` (CTE aliases that wrap this table). The brief
    is intentionally short — the full join graph already lives in the
    fingerprint summary above.
    """
    lines: list[str] = []

    join_targets = sorted(
        {j.get("other_table") for j in ctx.joins_involving_this if j.get("other_table")}
    )
    if join_targets:
        lines.append(
            f"- Joins to: {', '.join(f'`{t}`' for t in join_targets)}"
        )

    cte_aliases = [c.get("alias") for c in ctx.ctes_referencing_this if c.get("alias")]
    if cte_aliases:
        lines.append(
            f"- Wrapped by CTE alias(es): {', '.join(f'`{a}`' for a in cte_aliases)}"
        )

    temp_aliases = [
        t.get("alias") for t in ctx.temp_tables_referencing_this if t.get("alias")
    ]
    if temp_aliases:
        lines.append(
            f"- Materialised as temp/PDT candidate(s): "
            f"{', '.join(f'`{a}`' for a in temp_aliases)}"
        )

    if ctx.queries_using_this:
        lines.append(
            f"- Touched by {len(ctx.queries_using_this)} input "
            f"{'query' if len(ctx.queries_using_this) == 1 else 'queries'}: "
            f"{', '.join(ctx.queries_using_this)}"
        )

    if not lines:
        return "(this table has no upstream/downstream relationships in scope)"
    return "\n".join(lines)


def _render_plan_contract(plan: EnrichmentPlan) -> str:
    """Render the approved plan as a scope contract for the LLM.

    The contract enumerates the dimensions/measures/derived tables the human
    approved and explicitly tells the model not to invent more. Without this
    framing, Gemini tends to over-produce on complex tables.
    """
    lines = [
        f"Table: `{plan.table_name}` — complexity={plan.complexity}",
        "",
        "Build EXACTLY these LookML constructs — do not invent additional "
        "dimensions/measures beyond the plan:",
        "",
    ]

    if plan.proposed_dimensions:
        lines.append(f"### Approved dimensions ({len(plan.proposed_dimensions)})")
        for d in plan.proposed_dimensions:
            lines.append(
                f"- `{d.get('name', '?')}` ({d.get('type', '?')}) "
                f"← `{d.get('source_column', '?')}` "
                f"— {d.get('description_summary', '(no summary)')}"
            )
        lines.append("")

    if plan.proposed_dimension_groups:
        lines.append(
            f"### Approved dimension_groups ({len(plan.proposed_dimension_groups)})"
        )
        for dg in plan.proposed_dimension_groups:
            lines.append(
                f"- `{dg.get('name', '?')}` on `{dg.get('source_column', '?')}`"
            )
        lines.append("")

    if plan.proposed_measures:
        lines.append(f"### Approved measures ({len(plan.proposed_measures)})")
        for m in plan.proposed_measures:
            lines.append(
                f"- `{m.get('name', '?')}` ({m.get('type', '?')}) "
                f"← `{m.get('source_column', '?')}` "
                f"— {m.get('description_summary', '(no summary)')}"
            )
        lines.append("")

    if plan.proposed_derived_tables:
        lines.append(
            f"### Approved derived_tables ({len(plan.proposed_derived_tables)})"
        )
        for dt in plan.proposed_derived_tables:
            sf = dt.get("structural_filters") or []
            lines.append(
                f"- view `{dt.get('name', '?')}` from CTE "
                f"`{dt.get('source_cte', '?')}` "
                f"with primary_key `{dt.get('primary_key', '?')}` "
                f"— bake {len(sf)} structural filter(s) into derived_table SQL"
            )
        lines.append("")

    if plan.proposed_explore:
        lines.append("### Approved explore")
        lines.append(
            "```json\n"
            + json.dumps(plan.proposed_explore, indent=2, default=str)
            + "\n```"
        )
        lines.append("")

    if plan.risks:
        lines.append("### Risks the reviewer flagged — mitigate explicitly:")
        for r in plan.risks:
            lines.append(f"- {r}")
        lines.append("")

    if plan.questions_for_reviewer:
        lines.append(
            "### Reviewer questions (treat answers as approved if not addressed):"
        )
        for q in plan.questions_for_reviewer:
            lines.append(f"- {q}")
        lines.append("")

    lines.append(
        f"Targets: {plan.proposed_filter_catalog_count} filter catalog entries, "
        f"{plan.proposed_metric_catalog_count} metric catalog entries, "
        f"{plan.proposed_nl_question_count} NL questions."
    )
    return "\n".join(lines)


def _load_learnings(table_name: str, learnings_path: Path | None = None) -> str:
    """Pull the section(s) of ``data/learnings.md`` relevant to this table.

    Heuristic: a section is "relevant" if its heading line OR the first 200
    chars of body mention the table name. Returns joined sections, or the
    sentinel "(no prior learnings)" if nothing matches / file missing.
    """
    path = learnings_path or _LEARNINGS_PATH_DEFAULT
    if not path.exists():
        return "(no prior learnings)"
    text = path.read_text(encoding="utf-8")
    if table_name not in text:
        return "(no prior learnings)"

    # Split on H2 headings — that's how learnings.md is structured.
    sections = re.split(r"\n(?=##\s)", text)
    matching = [s.strip() for s in sections if table_name in s]
    if not matching:
        return "(no prior learnings)"
    return "\n\n".join(matching)


def _load_skill_excerpt(skill_path: Path | None = None) -> str:
    """Return SKILL.md sections 1-5 verbatim plus compressed 6-7.

    The full skill file lives at ``.claude/skills/lookml/SKILL.md``. Sections
    are delimited by ``## N. <heading>`` lines. We inject:

      - Sections 1-5 verbatim (SQL→LookML map, required attributes,
        primary_key, relationship inference, refinements/additive merge).
        Section 5 is REQUIRED because CLAUDE.md rule 6 ("merge into
        existing LookML — never regenerate. Additive only.") binds the
        model's behaviour to the refinement pattern.
      - A compressed view of sections 6 (1000th-query patterns) and 7
        (anti-patterns) — bullet headlines only, full body trimmed.

    Sections 8 (model file structure) and 9 (meta — how this skill is
    consumed) are intentionally excluded. The model file is generated by
    code, not by Gemini per-table; section 9 is documentation for Claude
    Code only.
    """
    path = skill_path or _SKILL_PATH
    if not path.exists():
        logger.warning("SKILL.md not found at %s — using inline fallback", path)
        return _INLINE_SKILL_FALLBACK

    text = path.read_text(encoding="utf-8")
    sections = _split_skill_sections(text)
    parts: list[str] = []

    for idx in (1, 2, 3, 4, 5):
        body = sections.get(idx)
        if body:
            parts.append(body.rstrip())
        else:
            logger.warning("SKILL.md missing section %d", idx)

    for idx in (6, 7):
        body = sections.get(idx)
        if body:
            parts.append(_compress_section(body))

    return "\n\n".join(parts)


def _split_skill_sections(text: str) -> dict[int, str]:
    """Parse SKILL.md into ``{section_number: body_with_heading}``.

    Section headings look like ``## 3. The primary_key and Symmetric ...``.
    The body runs up to the next ``## N.`` heading or end-of-file.
    """
    out: dict[int, str] = {}
    pattern = re.compile(r"^##\s+(\d+)\.\s+(.+?)\s*$", re.MULTILINE)
    matches = list(pattern.finditer(text))
    for i, m in enumerate(matches):
        num = int(m.group(1))
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        out[num] = text[start:end].strip()
    return out


def _compress_section(body: str) -> str:
    """Keep the heading + bullet headlines (drop code blocks and prose).

    Used for SKILL.md sections 6 and 7 so we keep the rules without burning
    prompt tokens on examples Gemini doesn't strictly need.
    """
    lines = body.splitlines()
    out: list[str] = []
    in_code = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        # Keep section heading, sub-headings, and bullet headlines.
        if (
            stripped.startswith("##")
            or stripped.startswith("###")
            or stripped.startswith("- ")
            or stripped.startswith("* ")
        ):
            out.append(line)
    return "\n".join(out)


# ─── ADK invocation seam (mocked in tests) ──────────────────────────


def _invoke_enrichment_agent(
    agent: LlmAgent,
    prompt: str,
    table_name: str,
) -> EnrichedOutput:
    """Run ``agent`` against ``prompt`` via ADK's ``InMemoryRunner``.

    Tests monkey-patch this function (or pass their own invoker) to return a
    fixture :class:`EnrichedOutput`; the prompt-assembly path stays under
    test even when no real Gemini call is made.

    This function is intentionally tiny — keep all heavy lifting in
    :func:`build_enrichment_prompt` so the seam stays clean.
    """
    # Lazy imports so test runs don't pay the ADK runner import cost when
    # the invoker is mocked.
    from google.adk.runners import InMemoryRunner
    from google.genai.types import Content, Part

    runner = InMemoryRunner(agent=agent, app_name=f"enrich_{table_name}")
    session = runner.session_service.create_session_sync(
        app_name=runner.app_name,
        user_id="lumi",
    )
    user_message = Content(role="user", parts=[Part(text=prompt)])

    final_text: str | None = None
    for event in runner.run(
        user_id="lumi",
        session_id=session.id,
        new_message=user_message,
    ):
        if event.is_final_response() and event.content and event.content.parts:
            for part in event.content.parts:
                if getattr(part, "text", None):
                    final_text = part.text
                    break

    if final_text is None:
        raise RuntimeError(
            f"Enrichment agent for {table_name} returned no final response"
        )
    return EnrichedOutput.model_validate_json(final_text)


# ─── Helpers ────────────────────────────────────────────────────────


def _safe_agent_name(table_name: str) -> str:
    """ADK agent names must be valid Python identifiers."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", table_name) or "table"


# Conservative inline fallback — only used if SKILL.md is missing on disk.
# Keep this in sync with the real skill file's invariants (rule 7 in
# CLAUDE.md says SKILL.md is the single source — this is a safety net for
# unusual deployment topologies, not a duplicate).
_INLINE_SKILL_FALLBACK = """\
## LookML invariants (fallback — SKILL.md not found)

- Every view needs `sql_table_name` OR a `derived_table { sql: ... ;; }`.
- Every view needs exactly one dimension with `primary_key: yes`.
- Every date/timestamp column → `dimension_group { type: time; ... }`,
  never a plain dimension. Include `convert_tz: no` on BigQuery.
- SUM → `measure { type: sum; sql: ${TABLE}.col ;; value_format_name: usd }`.
- COUNT(DISTINCT) → `measure { type: count_distinct; sql: ${TABLE}.col ;; }`.
- CTE with structural filters → derived_table view with filters baked in.
- CASE WHEN → derived dimension; if buckets have business order, add a
  hidden sort dimension and `order_by_field`.
- Join order in explores must follow SQL position order (later joins may
  reference columns from earlier joins). Default `relationship: many_to_one`.
- Descriptions: 15-200 chars, business meaning, no SQL restating.
"""
