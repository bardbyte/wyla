"""Plan builder — produces the EnrichmentPlan that scopes Stage 5 (enrich).

Two paths:

1. ``build_enrichment_plan_skeleton(ctx)`` — pure deterministic from
   sqlglot fingerprint + MDM digest + baseline parse. Cheap,
   reproducible, no LLM. Always works as a fallback.

2. ``build_enrichment_plan(ctx, all_fingerprints, contexts_by_table,
   with_llm=True)`` — runs the deterministic skeleton, then calls
   Gemini with the FULL context (TableContext + grounding signals +
   table narrative + baseline gaps) to produce a refined plan with
   substantive reasoning, risk identification, and questions for
   the human reviewer. Falls back to the skeleton on any LLM error.

The LLM-authored path is what makes the plan a meaningful review
artifact: humans see actual reasoning + grounded confidence per
proposal, not just deterministic tallies.

Public API:
    build_enrichment_plan_skeleton(ctx) -> EnrichmentPlan
    build_enrichment_plan(ctx, *, all_fingerprints=None,
        contexts_by_table=None, with_llm=False, ...) -> EnrichmentPlan
    format_enrichment_plan_markdown(plan, ctx) -> str
    save_plan_json / load_plan_json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from lumi.config import LumiConfig
from lumi.schemas import EnrichmentPlan, TableContext

logger = logging.getLogger("lumi.plan_builder")


# ─── Public API ──────────────────────────────────────────────


def build_enrichment_plan_skeleton(ctx: TableContext) -> EnrichmentPlan:
    """Deterministic plan from sqlglot + MDM + baseline only.

    This is the fallback when LLM authoring is disabled or fails.
    Always succeeds, always reproducible.
    """
    proposed_dimensions = _propose_dimensions(ctx)
    proposed_dim_groups = _propose_dimension_groups(ctx)
    proposed_measures = _propose_measures(ctx)
    proposed_derived = _propose_derived_tables(ctx)
    proposed_explore = _propose_explore(ctx)
    fields_to_enrich = _build_fields_to_enrich(ctx)
    risks = _identify_risks(ctx)
    complexity = _classify_complexity(
        ctx, proposed_derived=proposed_derived, joins=ctx.joins_involving_this
    )
    reasoning = _deterministic_reasoning(
        ctx,
        n_dims=len(proposed_dimensions),
        n_measures=len(proposed_measures),
        n_derived=len(proposed_derived),
    )

    estimated_input_tokens = _estimate_input_tokens(ctx)
    estimated_output_tokens = (
        len(proposed_dimensions) * 60
        + len(proposed_measures) * 80
        + len(proposed_derived) * 200
        + 500
    )

    return EnrichmentPlan(
        table_name=ctx.table_name,
        proposed_dimensions=proposed_dimensions,
        proposed_measures=proposed_measures,
        proposed_dimension_groups=proposed_dim_groups,
        proposed_derived_tables=proposed_derived,
        proposed_explore=proposed_explore,
        proposed_filter_catalog_count=len(
            [f for f in (ctx.filters_on_this or []) if not f.get("is_structural")]
        ),
        proposed_metric_catalog_count=len(proposed_measures),
        proposed_nl_question_count=max(5, len(ctx.queries_using_this or [])),
        complexity=complexity,
        estimated_input_tokens=estimated_input_tokens,
        estimated_output_tokens=estimated_output_tokens,
        reasoning=reasoning,
        risks=risks,
        questions_for_reviewer=_questions_for_reviewer(ctx, risks),
        fields_to_enrich=fields_to_enrich,
    )


def build_enrichment_plan(
    ctx: TableContext,
    *,
    all_fingerprints: list | None = None,
    contexts_by_table: dict | None = None,
    with_llm: bool = False,
    config: LumiConfig | None = None,
) -> EnrichmentPlan:
    """Build an EnrichmentPlan, optionally Gemini-authored.

    Args:
        ctx: TableContext for the table being planned.
        all_fingerprints: every parsed SQL — needed to compute grounding
            signals + narrative for the LLM context.
        contexts_by_table: all discovered TableContexts — for cross-table
            join inference.
        with_llm: when True, calls Gemini to author the plan with full
            context. When False, returns the deterministic skeleton.
        config: optional LumiConfig (model, project, etc.).

    Returns:
        EnrichmentPlan. On LLM error or missing prerequisites, falls
        back to the deterministic skeleton — caller never gets None.
    """
    skeleton = build_enrichment_plan_skeleton(ctx)
    if not with_llm:
        return skeleton
    if all_fingerprints is None:
        logger.warning(
            "build_enrichment_plan(with_llm=True) needs all_fingerprints "
            "to compute grounding/narrative; falling back to skeleton",
        )
        return skeleton
    try:
        return _author_plan_with_llm(
            ctx, skeleton, all_fingerprints, contexts_by_table or {},
            config=config or LumiConfig(),
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "LLM plan authoring failed for %s, using skeleton: %s",
            ctx.table_name, e,
        )
        return skeleton


def _author_plan_with_llm(
    ctx: TableContext,
    skeleton: EnrichmentPlan,
    all_fingerprints: list,
    contexts_by_table: dict,
    config: LumiConfig,
) -> EnrichmentPlan:
    """Run the planning LlmAgent with the full deterministic context.

    The LLM gets:
      - The deterministic skeleton (proposed dims/measures/derived_tables
        as a scope hint — NOT a strict contract; LLM can refine)
      - TableNarrative (holistic understanding)
      - Grounding signals (per-column evidence)
      - Baseline gap analysis
      - Confidence-labeling rules

    It MUST return an EnrichmentPlan. The agent's output_schema enforces
    Pydantic validation; we run check_planning after as a safety net
    and self-repair retry once on failure.
    """
    # Lazy imports — only paid when LLM is actually used.
    from lumi.grounding import build_grounding_signals
    from lumi.narrative import build_table_narrative

    grounding = build_grounding_signals(ctx, all_fingerprints, contexts_by_table)
    narrative = build_table_narrative(ctx, all_fingerprints=all_fingerprints)

    prompt = _build_plan_prompt(ctx, skeleton, narrative, grounding)
    logger.info(
        "Planning %s with Gemini — prompt %d chars, skeleton %d dims / %d measures",
        ctx.table_name, len(prompt),
        len(skeleton.proposed_dimensions), len(skeleton.proposed_measures),
    )

    refined = _invoke_plan_agent(prompt, ctx.table_name, config)
    if refined is None:
        logger.info("LLM returned no usable plan; using skeleton")
        return skeleton

    # Sanity guard: if the LLM dropped substantive proposals, fall back.
    # The skeleton's count is a floor — Gemini can refine but shouldn't
    # silently empty out the plan.
    if not refined.proposed_dimensions and not refined.proposed_measures:
        logger.warning(
            "LLM-authored plan has no dims/measures, falling back to skeleton",
        )
        return skeleton

    # Preserve fields the LLM is unlikely to refine well.
    refined.estimated_input_tokens = (
        refined.estimated_input_tokens or skeleton.estimated_input_tokens
    )
    refined.estimated_output_tokens = (
        refined.estimated_output_tokens or skeleton.estimated_output_tokens
    )
    refined.fields_to_enrich = (
        refined.fields_to_enrich or skeleton.fields_to_enrich
    )
    return refined


def _build_plan_prompt(
    ctx: TableContext,
    skeleton: EnrichmentPlan,
    narrative,
    grounding,
) -> str:
    """Compose the plan-stage prompt.

    Heavily structured: identity → narrative → grounding → skeleton →
    instructions. The LLM authors a refined EnrichmentPlan on top.
    """
    # Lazy imports for renderers.
    from lumi.grounding import render_grounding_signals
    from lumi.narrative import render_table_narrative

    parts: list[str] = [
        "# Planning task",
        "",
        f"You are reviewing the deterministic enrichment skeleton for "
        f"`{ctx.table_name}` and authoring the FINAL plan. The plan is "
        "what a human reviewer reads + approves before we spend tokens "
        "on full LookML enrichment. Make it substantive: real reasoning, "
        "real risks, real questions.",
        "",
        "## Output contract",
        "Return an EnrichmentPlan (Pydantic). You may refine the skeleton's "
        "proposals — add missing fields, remove redundant ones, sharpen "
        "names — but DO NOT silently drop the structural categories "
        "(dimensions, measures, dimension_groups, derived_tables, explore). "
        "The `reasoning` field MUST be 2-3 substantive sentences (not a "
        "tally) explaining what this table IS and what the plan changes "
        "vs the existing baseline. The `risks` field lists real concerns "
        "not generic boilerplate. The `questions_for_reviewer` field asks "
        "directed questions only when there's genuine ambiguity.",
        "",
        render_table_narrative(narrative),
        "",
        render_grounding_signals(grounding),
        "",
        "## Deterministic skeleton (your starting point — refine, don't replace wholesale)",
        _render_skeleton_for_prompt(skeleton),
        "",
        "## Authoring rules",
        "1. PRESERVE the structure — every category in the skeleton must "
        "appear in your output unless you have explicit reasoning to drop it.",
        "2. REFINE descriptions — when grounding signals + narrative give "
        "you better domain context than the skeleton's auto-generated text, "
        "use it. e.g. cm11 with pii_role_id=NGBD-SDE-CM11 should describe "
        "as 'Cardmember-grain identifier (PII role: SDE-CM11)' not "
        "'Customer Member 11'.",
        "3. WRITE the reasoning field as if explaining to a senior data "
        "engineer: what the table represents, what the plan adds vs the "
        "baseline, the single biggest risk.",
        "4. SURFACE real risks — sparse MDM, no PK candidate, complex "
        "CTE chains, high-cardinality categoricals — not 'caution: change'.",
        "5. ASK directed questions only for true ambiguity (e.g. when "
        "two PK candidates tie, or join cardinality is unclear).",
        "6. KEEP estimated_input_tokens / estimated_output_tokens from "
        "the skeleton (they're already computed).",
    ]
    return "\n\n".join(parts)


def _render_skeleton_for_prompt(skeleton: EnrichmentPlan) -> str:
    """Compact skeleton view for the prompt context."""
    out: list[str] = [
        f"Complexity: {skeleton.complexity}",
        f"Estimated input tokens: {skeleton.estimated_input_tokens}",
        "",
    ]
    if skeleton.proposed_dimensions:
        out.append(f"Proposed dimensions ({len(skeleton.proposed_dimensions)}):")
        for d in skeleton.proposed_dimensions:
            out.append(
                f"  - `{d.get('name')}` ({d.get('type')}) ← "
                f"`{d.get('source_column')}`"
            )
    if skeleton.proposed_dimension_groups:
        out.append(
            f"Proposed dimension_groups ({len(skeleton.proposed_dimension_groups)}):"
        )
        for dg in skeleton.proposed_dimension_groups:
            out.append(f"  - `{dg.get('name')}` on `{dg.get('source_column')}`")
    if skeleton.proposed_measures:
        out.append(f"Proposed measures ({len(skeleton.proposed_measures)}):")
        for m in skeleton.proposed_measures:
            out.append(
                f"  - `{m.get('name')}` ({m.get('type')}) ← "
                f"`{m.get('source_column')}`"
            )
    if skeleton.proposed_derived_tables:
        out.append(
            f"Proposed derived_tables ({len(skeleton.proposed_derived_tables)}):"
        )
        for dt in skeleton.proposed_derived_tables:
            out.append(f"  - `{dt.get('name')}` (kind: {dt.get('kind')})")
    if skeleton.risks:
        out.append("Skeleton-detected risks:")
        for r in skeleton.risks:
            out.append(f"  - {r}")
    return "\n".join(out)


def _maybe_disable_tls() -> None:
    """When ``LUMI_INSECURE_TLS=1`` is set, patch every Python TLS layer
    so corporate-MITM proxies don't kill the Vertex call.

    Mirrors the comprehensive bypass used by check_bq_access.py and
    check_vertex_gemini.py. Idempotent — patches won't double-stack.
    Truststore (injected at import) is the preferred path; this is the
    fallback for when Keychain doesn't carry the corp root CA.
    """
    import os
    if os.environ.get("LUMI_INSECURE_TLS") not in {"1", "true", "yes"}:
        return
    if getattr(_maybe_disable_tls, "_applied", False):
        return
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[assignment]
    os.environ["PYTHONHTTPSVERIFY"] = "0"
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass
    try:
        import google.auth.transport.requests as gat
        _orig_init = gat.AuthorizedSession.__init__

        def _patched(self, *a, **kw):  # type: ignore[no-untyped-def]
            _orig_init(self, *a, **kw)
            self.verify = False
        gat.AuthorizedSession.__init__ = _patched  # type: ignore[method-assign]
    except ImportError:
        pass
    try:
        import requests
        _orig_req_init = requests.Session.__init__

        def _patched_req(self, *a, **kw):  # type: ignore[no-untyped-def]
            _orig_req_init(self, *a, **kw)
            self.verify = False
        requests.Session.__init__ = _patched_req  # type: ignore[method-assign]
    except ImportError:
        pass
    try:
        import httpx
        _orig_client = httpx.Client
        _orig_async = httpx.AsyncClient

        def _client_no_verify(*args, **kwargs):  # type: ignore[no-untyped-def]
            kwargs.setdefault("verify", False)
            return _orig_client(*args, **kwargs)

        def _async_no_verify(*args, **kwargs):  # type: ignore[no-untyped-def]
            kwargs.setdefault("verify", False)
            return _orig_async(*args, **kwargs)
        httpx.Client = _client_no_verify  # type: ignore[misc]
        httpx.AsyncClient = _async_no_verify  # type: ignore[misc]
    except ImportError:
        pass
    _maybe_disable_tls._applied = True  # type: ignore[attr-defined]
    logger.warning(
        "LUMI_INSECURE_TLS is set — disabled TLS verification across "
        "stdlib/requests/httpx/google-auth. Only safe on networks you trust.",
    )


def _invoke_plan_agent(
    prompt: str, table_name: str, config: LumiConfig,
) -> EnrichmentPlan | None:
    """Build the LlmAgent + Runner, run it, return the parsed EnrichmentPlan.

    Mirrors the pattern in lumi.enrich._invoke_enrichment_agent. Returns
    None on any error so the caller can fall back to the skeleton.
    """
    _maybe_disable_tls()
    try:
        from google.adk.agents import LlmAgent
        from google.adk.runners import InMemoryRunner
        from google.adk.sessions import InMemorySessionService
        from google.genai import types as genai_types
    except ImportError as e:
        logger.warning("ADK not importable, skipping LLM planning: %s", e)
        return None

    # ADK treats `instruction` as a template with `{var}` substitution.
    # Any literal `{` `}` in our prompt (from MDM descriptions, filter
    # values like `WHERE x = '{N}'`, JSON snippets, etc.) gets parsed
    # as a state-variable reference. Escape them by doubling.
    safe_prompt = prompt.replace("{", "{{").replace("}", "}}")
    agent = LlmAgent(
        name=f"plan_{table_name}".replace(".", "_"),
        model=config.model_name,
        description="Authoring an EnrichmentPlan from grounded context.",
        instruction=safe_prompt,
        output_schema=EnrichmentPlan,
        generate_content_config=genai_types.GenerateContentConfig(
            temperature=config.temperature,
            max_output_tokens=4000,
        ),
    )
    runner = InMemoryRunner(agent=agent, app_name="lumi_plan")

    session_service: InMemorySessionService = runner.session_service  # type: ignore[assignment]
    import asyncio
    user_id, session_id = "lumi_planner", f"plan_{table_name}"

    async def _run() -> str | None:
        await session_service.create_session(
            app_name="lumi_plan", user_id=user_id, session_id=session_id,
        )
        last_text: str | None = None
        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="Author the plan.")],
            ),
        ):
            if (event.content and event.content.parts
                    and event.content.parts[0].text):
                last_text = event.content.parts[0].text
        return last_text

    try:
        text = asyncio.run(_run())
    except Exception as e:  # noqa: BLE001
        logger.warning("plan agent invocation failed: %s", e)
        return None

    if not text:
        return None
    try:
        data = json.loads(text)
        return EnrichmentPlan(**data)
    except Exception as e:  # noqa: BLE001
        logger.warning("plan agent returned non-JSON or invalid plan: %s", e)
        return None


# Back-compat: callers that still import build_enrichment_plan with no
# kwargs get the deterministic skeleton (same as before this commit).
# Tests that mock the LlmAgent layer can pass with_llm=True.


# ─── Persistence ─────────────────────────────────────────────


def save_plan_json(plan: EnrichmentPlan, plans_dir: Path) -> Path:
    """Persist the structured plan so Phase 2 can load it after approval."""
    plans_dir.mkdir(parents=True, exist_ok=True)
    target = plans_dir / f"{plan.table_name}.plan.json"
    target.write_text(
        json.dumps(plan.model_dump(), indent=2, default=str), encoding="utf-8"
    )
    return target


def load_plan_json(plans_dir: Path, table_name: str) -> EnrichmentPlan | None:
    """Load a saved EnrichmentPlan, or None if missing / unparseable."""
    target = plans_dir / f"{table_name}.plan.json"
    if not target.exists():
        return None
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
        return EnrichmentPlan(**data)
    except Exception as e:  # noqa: BLE001
        logger.warning("could not load plan for %s: %s", table_name, e)
        return None


# ─── Markdown render ─────────────────────────────────────────


def format_enrichment_plan_markdown(
    plan: EnrichmentPlan,
    ctx: TableContext,
    *,
    rank: int | None = None,
) -> str:
    """Human-readable review markdown for one EnrichmentPlan.

    Includes the reviewer-decision footer (`[ ] APPROVED / [ ] REJECTED`)
    parsed by ``lumi.approval.collect_approvals``.
    """
    sig = ctx.baseline_quality_signals or {}
    rank_line = f" — rank #{rank}" if rank is not None else ""

    lines: list[str] = [
        f"# Enrichment plan: {plan.table_name}{rank_line}",
        "",
        f"- complexity: **{plan.complexity}**",
        f"- queries using this table: {len(ctx.queries_using_this)}",
        f"- est. input/output tokens: {plan.estimated_input_tokens} / "
        f"{plan.estimated_output_tokens}",
        "- baseline: "
        + (
            f"{sig.get('dims_total', 0)} dims, "
            f"{sig.get('measures_total', 0)} measures, "
            f"primary_key={'yes' if sig.get('has_primary_key') else 'NO'}"
            if sig
            else "(no baseline view found — generating fresh)"
        ),
        "",
        "## Reasoning",
        plan.reasoning or "(none)",
        "",
    ]

    if plan.proposed_dimensions:
        lines.append(f"## Proposed dimensions ({len(plan.proposed_dimensions)})")
        for d in plan.proposed_dimensions:
            lines.append(
                f"- `{d.get('name', '?')}` "
                f"({d.get('type', '?')}) ← `{d.get('source_column', '?')}` "
                f"— {d.get('description_summary', '')}"
            )
        lines.append("")

    if plan.proposed_dimension_groups:
        lines.append(
            f"## Proposed dimension_groups ({len(plan.proposed_dimension_groups)})"
        )
        for dg in plan.proposed_dimension_groups:
            lines.append(
                f"- `{dg.get('name', '?')}` on `{dg.get('source_column', '?')}`"
            )
        lines.append("")

    if plan.proposed_measures:
        lines.append(f"## Proposed measures ({len(plan.proposed_measures)})")
        for m in plan.proposed_measures:
            lines.append(
                f"- `{m.get('name', '?')}` "
                f"({m.get('type', '?')}) ← `{m.get('source_column', '?')}` "
                f"— {m.get('description_summary', '')}"
            )
        lines.append("")

    if plan.proposed_derived_tables:
        lines.append(
            f"## Proposed derived_tables ({len(plan.proposed_derived_tables)})"
        )
        for dt in plan.proposed_derived_tables:
            lines.append(
                f"- `{dt.get('name', '?')}` "
                f"({'TEMP' if dt.get('is_temp') else 'CTE'}) — "
                f"sources: {', '.join(dt.get('source_tables', []) or ['?'])}"
            )
        lines.append("")

    if plan.proposed_explore:
        e = plan.proposed_explore
        lines.append("## Proposed explore")
        lines.append(f"- base_view: `{e.get('base_view', plan.table_name)}`")
        joins = e.get("joins") or []
        if joins:
            lines.append(f"- joins ({len(joins)}, in topological order):")
            for j in joins:
                lines.append(
                    f"    - `{j.get('right_table', '?')}` "
                    f"({j.get('join_type', 'inner')}) "
                    f"on {j.get('left_key', '?')} = {j.get('right_key', '?')}"
                )
        if e.get("always_filter"):
            lines.append(f"- always_filter: {e['always_filter']}")
        lines.append("")

    if plan.fields_to_enrich:
        lines.append(f"## Fields to enrich ({len(plan.fields_to_enrich)})")
        for f in plan.fields_to_enrich[:20]:
            lines.append(
                f"- {f.get('kind', 'field')} `{f.get('name', '?')}` — "
                f"{f.get('gap', '?')}"
            )
        if len(plan.fields_to_enrich) > 20:
            lines.append(f"- … and {len(plan.fields_to_enrich) - 20} more")
        lines.append("")

    if plan.risks:
        lines.append("## Risks")
        for r in plan.risks:
            lines.append(f"- ⚠ {r}")
        lines.append("")

    if plan.questions_for_reviewer:
        lines.append("## Questions for reviewer")
        for q in plan.questions_for_reviewer:
            lines.append(f"- {q}")
        lines.append("")

    lines.extend([
        "---",
        "## Reviewer decision",
        "",
        "Mark ONE:",
        "",
        "- [ ] ✅ APPROVED",
        "- [ ] ❌ REJECTED",
        "",
        "Feedback (required if rejected):",
        "",
    ])
    return "\n".join(lines)


# ─── Helpers (deterministic propose_*) ───────────────────────


def _propose_dimensions(ctx: TableContext) -> list[dict[str, Any]]:
    """Every column referenced in WHERE / GROUP BY / SELECT that isn't a
    date (those become dim_groups) or an aggregation source (those become
    measures).
    """
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    date_cols = {
        (df.get("column") or "").lower()
        for df in (ctx.date_functions or [])
        if df.get("column")
    }
    agg_source_cols = {
        (a.get("column") or "").lower()
        for a in (ctx.aggregations or [])
        if a.get("column")
    }

    # MDM gives us the column type; use it to set type=string|number|yesno.
    mdm_by_col = {
        (c.get("name") or "").lower(): c for c in (ctx.mdm_columns or [])
    }

    candidates = list(ctx.columns_referenced or [])
    # Add filter columns (some only appear in WHERE).
    for f in ctx.filters_on_this or []:
        col = f.get("column")
        if col and col not in candidates:
            candidates.append(col)

    for col in candidates:
        if not col:
            continue
        c_lower = col.lower()
        if c_lower in date_cols or c_lower in agg_source_cols:
            continue
        if c_lower in seen:
            continue
        seen.add(c_lower)

        mdm = mdm_by_col.get(c_lower) or {}
        mdm_type = (mdm.get("type") or mdm.get("data_type") or "").upper()
        if mdm_type in {"NUMERIC", "FLOAT64", "INT64", "INTEGER", "NUMBER"}:
            lk_type = "number"
        elif mdm_type in {"BOOL", "BOOLEAN"}:
            lk_type = "yesno"
        else:
            lk_type = "string"

        desc = (mdm.get("description") or mdm.get("attribute_desc") or "").strip()
        out.append({
            "name": col,
            "type": lk_type,
            "source_column": col,
            "description_summary": desc[:120] if desc else "",
        })

    # Append CASE WHEN derived dimensions.
    for cw in ctx.case_whens or []:
        alias = cw.get("alias") or cw.get("source_column")
        if not alias or alias.lower() in seen:
            continue
        seen.add(alias.lower())
        out.append({
            "name": alias,
            "type": "string",
            "source_column": cw.get("source_column"),
            "description_summary": "Derived from CASE WHEN — see sql",
            "is_derived": True,
            "case_when_sql": cw.get("sql"),
        })

    return out


def _propose_dimension_groups(ctx: TableContext) -> list[dict[str, Any]]:
    """Every column appearing in fp.date_functions becomes a dim_group."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for df in ctx.date_functions or []:
        col = df.get("column")
        if not col or col.lower() in seen:
            continue
        seen.add(col.lower())
        out.append({
            "name": col.replace("_dt", "").replace("_date", "")
                       .replace("_ts", "").rstrip("_") or col,
            "source_column": col,
            "type": "time",
            "timeframes": ["date", "week", "month", "quarter", "year"],
            "datatype": "date" if "dt" in col.lower() or "date" in col.lower()
                       else "datetime",
        })
    return out


def _propose_measures(ctx: TableContext) -> list[dict[str, Any]]:
    """One measure per (function, column) pair from fp.aggregations."""
    fn_to_lkml = {
        "SUM": ("sum", "decimal_2"),
        "COUNT": ("count", "decimal_0"),
        "AVG": ("average", "decimal_2"),
        "MIN": ("min", "decimal_2"),
        "MAX": ("max", "decimal_2"),
        "STDDEV": ("number", "decimal_4"),
        "VARIANCE": ("number", "decimal_4"),
    }

    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for agg in ctx.aggregations or []:
        fn = (agg.get("function") or "").upper()
        col = agg.get("column")
        if not col:
            continue
        if agg.get("distinct") and fn == "COUNT":
            lk_type, vfmt = "count_distinct", "decimal_0"
        else:
            lk_type, vfmt = fn_to_lkml.get(fn, ("number", "decimal_2"))
        key = (lk_type, col.lower())
        if key in seen:
            continue
        seen.add(key)
        # Reasonable measure name.
        name_prefix = {
            "sum": "total_", "count": "count_", "count_distinct": "unique_",
            "average": "avg_", "min": "min_", "max": "max_",
        }.get(lk_type, "")
        out.append({
            "name": f"{name_prefix}{col}",
            "type": lk_type,
            "source_column": col,
            "value_format_name": vfmt,
            "description_summary": f"{lk_type.replace('_', ' ').title()} of {col}",
        })

    return out


def _propose_derived_tables(ctx: TableContext) -> list[dict[str, Any]]:
    """Each CTE / temp_table referencing this table → one derived_table view.

    Structural filters from the CTE body get baked into the derived_table
    sql so they're never user-selectable (per CLAUDE.md rule 9).
    """
    out: list[dict[str, Any]] = []
    for cte in ctx.ctes_referencing_this or []:
        out.append({
            "name": f"{cte.get('alias', 'cte')}_dt",
            "source_alias": cte.get("alias"),
            "source_tables": cte.get("source_tables") or [],
            "structural_filters": cte.get("structural_filters") or [],
            "is_temp": False,
            "kind": "cte",
        })
    for tt in ctx.temp_tables_referencing_this or []:
        out.append({
            "name": f"{tt.get('alias', 'temp')}_pdt",
            "source_alias": tt.get("alias"),
            "source_tables": tt.get("source_tables") or [],
            "structural_filters": tt.get("structural_filters") or [],
            "is_temp": bool(tt.get("is_temp")),
            "kind": "temp_table",
        })
    return out


def _propose_explore(ctx: TableContext) -> dict[str, Any] | None:
    """Build an explore proposal — joins in topological order from the
    fingerprint, plus an always_filter on the dim_group date if present.
    """
    joins = ctx.joins_involving_this or []
    date_cols = [df.get("column") for df in (ctx.date_functions or []) if df.get("column")]

    # Sort joins by their captured 'order' field if present.
    sorted_joins = sorted(joins, key=lambda j: j.get("order", 0))

    out: dict[str, Any] = {
        "base_view": ctx.table_name,
        "joins": [
            {
                "right_table": j.get("right_table") or j.get("other_table"),
                "left_table": j.get("left_table"),
                "left_key": j.get("left_key"),
                "right_key": j.get("right_key"),
                "join_type": j.get("join_type", "inner"),
            }
            for j in sorted_joins
            if j.get("right_table") or j.get("other_table")
        ],
    }

    if date_cols:
        primary_date = date_cols[0]
        out["always_filter"] = {primary_date: "last 90 days"}

    return out if (out["joins"] or out.get("always_filter")) else {"base_view": ctx.table_name, "joins": []}


def _build_fields_to_enrich(ctx: TableContext) -> list[dict[str, Any]]:
    """Surgical scope from baseline_quality_signals — what's missing."""
    out: list[dict[str, Any]] = []
    for d in ctx.baseline_dimensions or []:
        name = d.get("name") or "?"
        if not (d.get("description") or "").strip():
            out.append({"kind": "dim", "name": name, "gap": "missing_description"})
        elif len((d.get("description") or "").strip()) < 30:
            out.append({"kind": "dim", "name": name, "gap": "short_description"})
        if not (d.get("label") or "").strip():
            out.append({"kind": "dim", "name": name, "gap": "missing_label"})
        if not d.get("tags"):
            out.append({"kind": "dim", "name": name, "gap": "missing_tags"})
    for m in ctx.baseline_measures or []:
        name = m.get("name") or "?"
        if not (m.get("value_format_name") or m.get("value_format")):
            out.append({
                "kind": "measure", "name": name, "gap": "missing_value_format",
            })
    sig = ctx.baseline_quality_signals or {}
    if sig.get("dates_as_plain_dim", 0):
        out.append({
            "kind": "view", "name": ctx.table_name,
            "gap": "promote_to_dim_group",
        })
    if sig.get("dims_total", 0) > 0 and not sig.get("has_primary_key"):
        out.append({
            "kind": "view", "name": ctx.table_name, "gap": "missing_primary_key",
        })
    return out


def _identify_risks(ctx: TableContext) -> list[str]:
    """Surface things the reviewer should look at carefully."""
    risks: list[str] = []
    sig = ctx.baseline_quality_signals or {}
    if not sig.get("has_primary_key", True):
        risks.append(
            "Baseline has NO primary_key dimension — Looker will silently produce "
            "wrong aggregations across joins. Pick one (look for *_id, *_xref_id)."
        )
    if ctx.ctes_referencing_this:
        risks.append(
            f"{len(ctx.ctes_referencing_this)} CTE-derived view(s) need structural "
            "filters baked into the derived_table SQL."
        )
    if ctx.temp_tables_referencing_this:
        risks.append(
            f"{len(ctx.temp_tables_referencing_this)} CREATE TEMP TABLE(s) found — "
            "these are PDT (persistent derived table) candidates. Reviewer to "
            "confirm whether to materialise as Looker PDT vs derived_table."
        )
    structural = [f for f in (ctx.filters_on_this or []) if f.get("is_structural")]
    if structural:
        risks.append(
            f"{len(structural)} structural filter(s) — these must be baked into "
            "derived_table SQL or sql_always_where, NOT exposed as user filters."
        )
    if sig.get("dates_as_plain_dim", 0):
        risks.append(
            f"{sig['dates_as_plain_dim']} date column(s) currently a plain "
            "dimension — must be promoted to dimension_group."
        )
    return risks


def _classify_complexity(
    ctx: TableContext,
    *,
    proposed_derived: list[dict[str, Any]],
    joins: list[dict[str, Any]],
) -> str:
    score = (
        len(proposed_derived) * 2
        + len(joins)
        + (1 if (ctx.case_whens or []) else 0)
    )
    if score == 0:
        return "simple"
    if score <= 3:
        return "medium"
    return "complex"


def _questions_for_reviewer(
    ctx: TableContext, risks: list[str]
) -> list[str]:
    """A few directed questions the human should answer in their feedback."""
    questions: list[str] = []
    if ctx.temp_tables_referencing_this:
        questions.append(
            "Should the temp tables become Looker PDTs (persistent) or "
            "non-persistent derived_tables?"
        )
    if (ctx.joins_involving_this or []):
        questions.append(
            "Confirm the join relationship for each proposed explore join "
            "(many_to_one vs many_to_many)."
        )
    sig = ctx.baseline_quality_signals or {}
    if not sig.get("has_primary_key", True):
        questions.append(
            "Which column is the primary_key? Best candidates: "
            + ", ".join(_pk_candidates(ctx)[:5])
        )
    return questions


def _pk_candidates(ctx: TableContext) -> list[str]:
    cands: list[str] = []
    for col in (ctx.columns_referenced or []):
        cl = col.lower()
        if cl.endswith(("_id", "_xref_id", "_uuid", "_key", "_cd")):
            cands.append(col)
    return cands or ["(no obvious *_id columns — manual choice)"]


def _estimate_input_tokens(ctx: TableContext) -> int:
    """Rough token budget for the enrichment prompt — used by the planning
    guardrail to flag tables whose context would overflow Gemini's limit.
    """
    approx = 4000  # base prompt + SKILL excerpt
    approx += len(ctx.mdm_columns or []) * 25
    approx += len(ctx.existing_view_lkml or "") // 4
    approx += len(ctx.aggregations or []) * 30
    approx += sum(
        len(c.get("sql", "")) // 4 for c in (ctx.ctes_referencing_this or [])
    )
    return approx


def _deterministic_reasoning(
    ctx: TableContext, *, n_dims: int, n_measures: int, n_derived: int
) -> str:
    """Computed summary of why this plan was constructed — fallback when
    --with-gemini is not used."""
    parts = [
        f"This table is referenced by {len(ctx.queries_using_this)} gold "
        f"query/queries and has {ctx.mdm_coverage_pct * 100:.0f}% MDM coverage."
    ]
    sig = ctx.baseline_quality_signals or {}
    if sig:
        parts.append(
            f"Baseline view has {sig.get('dims_total', 0)} dimensions and "
            f"{sig.get('measures_total', 0)} measures; "
            f"{sig.get('dims_missing_description', 0)} dim(s) lack a "
            f"description and {sig.get('measures_missing_value_format', 0)} "
            f"measure(s) lack value_format_name."
        )
    parts.append(
        f"Plan proposes {n_dims} dimension(s), {n_measures} measure(s), "
        f"and {n_derived} derived_table(s) to close those gaps."
    )
    if ctx.temp_tables_referencing_this:
        parts.append(
            f"{len(ctx.temp_tables_referencing_this)} CREATE TEMP TABLE block(s) "
            "are flagged as Looker PDT candidates."
        )
    return " ".join(parts)


