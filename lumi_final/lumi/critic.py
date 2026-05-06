"""Critic agent — the plan quality gate.

Critiques an EnrichmentPlan against 11 categories with a Radix-aware lens
(would the downstream NL2SQL retrieval system find this view for natural
analyst questions?). Outputs a CritiqueReport. Severity = block forces a
self-repair retry of the plan stage.

Two-layer architecture:

1. Deterministic pre-flight (free, fast) — catches the issues you can
   prove without an LLM: placeholder names, missing PK when the table
   has dim, structural filters exposed, ontology canonical name absent
   from synonyms when the table maps to that entity. Always runs.

2. LLM critic (Gemini, temp 0) — catches the substantive issues only a
   model with domain understanding can see: vague reasoning, ambiguous
   names, Radix retrieval misalignment, ontology vocabulary drift.
   Combined with the deterministic findings; LLM may upgrade severity
   or add new issues.

Public API:
    critique_plan(ctx, plan, ontology, *, all_fingerprints, with_llm=True,
                  config=None) -> CritiqueReport
    render_critique_markdown(report) -> str
    format_issues_for_repair(report) -> str
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Literal

from lumi.config import LumiConfig
from lumi.schemas import (
    CritiqueIssue,
    CritiqueReport,
    DomainOntology,
    EnrichmentPlan,
    TableContext,
)

logger = logging.getLogger("lumi.critic")


_PLACEHOLDER_TOKENS = frozenset({
    None, "", "?", "??", "TBD", "todo", "TODO",
    "unknown", "Unknown", "UNKNOWN", "n/a", "N/A",
})


# ─── Public API ──────────────────────────────────────────────


def critique_plan(
    ctx: TableContext,
    plan: EnrichmentPlan,
    *,
    ontology: DomainOntology | None = None,
    all_fingerprints: list[Any] | None = None,
    with_llm: bool = True,
    config: LumiConfig | None = None,
) -> CritiqueReport:
    """Critique a plan; return CritiqueReport.

    Always runs deterministic pre-flight. If ``with_llm=True`` and ADK
    is importable, also runs the LLM critic and merges issues. Never
    raises — on LLM error, returns the deterministic-only report.
    """
    cfg = config or LumiConfig()
    # Compute equivalence classes + JOIN cardinality + paths once —
    # feeds the equivalence preservation check, the join cardinality
    # check, and the LLM critic prompt.
    eq_map = None
    cardinalities: list[Any] = []
    canonical_paths: list[Any] = []
    if all_fingerprints:
        try:
            from lumi.ontology import compute_equivalence_classes
            eq_map = compute_equivalence_classes(all_fingerprints)
        except Exception as e:  # noqa: BLE001
            logger.debug("eq_map unavailable: %s", e)
        try:
            from lumi.joins import (
                infer_canonical_paths, infer_join_cardinalities,
            )
            cardinalities = infer_join_cardinalities(all_fingerprints)
            canonical_paths = infer_canonical_paths(
                all_fingerprints, top_k=20,
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("join inference unavailable: %s", e)

    det_issues = _deterministic_findings(
        ctx, plan, ontology, eq_map=eq_map,
        cardinalities=cardinalities, canonical_paths=canonical_paths,
    )

    if with_llm:
        try:
            llm_report = _llm_critique(
                ctx, plan, ontology, all_fingerprints or [], cfg,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("LLM critic failed for %s: %s", ctx.table_name, e)
            llm_report = None
    else:
        llm_report = None

    if llm_report is None:
        report = CritiqueReport(
            table_name=ctx.table_name,
            issues=det_issues,
            overall_verdict=_verdict_from_issues(det_issues),
            radix_retrieval_score=_deterministic_retrieval_score(ctx, plan),
            summary=_deterministic_summary(ctx, plan, det_issues),
        )
    else:
        # Merge: keep both, dedupe by (category, locus, finding[:60]).
        merged = _merge_issues(det_issues, llm_report.issues)
        # ALWAYS re-derive verdict from issues — never trust the LLM's
        # claimed verdict against the actual severity counts. This is
        # how we prevent "verdict=approve" with 5 blocking issues, which
        # an unconstrained model will absolutely emit.
        derived = _verdict_from_issues(merged)
        verdict = _stricter_verdict(
            llm_report.overall_verdict, derived,
        )
        report = CritiqueReport(
            table_name=ctx.table_name,
            issues=merged,
            overall_verdict=verdict,
            radix_retrieval_score=llm_report.radix_retrieval_score,
            summary=llm_report.summary or _deterministic_summary(ctx, plan, merged),
        )
    report.recompute_counts()

    # Critic findings feed back into the ontology. Synonym + equivalence
    # findings become events the next promotion folds into the canonical
    # ontology. Best-effort — never blocks the critique.
    try:
        _emit_refinement_events(ctx, report)
    except Exception as e:  # noqa: BLE001
        logger.debug("could not emit critic refinement events: %s", e)

    return report


def _emit_refinement_events(
    ctx: TableContext, report: CritiqueReport,
) -> None:
    """Mine the critique for ontology learnings and record them.

    Patterns we extract:
      - equivalence_preservation findings → entity_refinement event with
        the suggested winner entity name (mined from `recommendation`).
      - vocabulary_completeness findings citing an entity → synonym
        candidate from the cited entity.
    """
    from lumi.ontology_store import OntologyStore
    from lumi.schemas import OntologyEvent

    store = OntologyStore()
    for issue in report.issues:
        if issue.category == "equivalence_preservation":
            # Mine the recommendation for "Suggested winner: X" pattern.
            rec = issue.recommendation
            m = re.search(r"winner:?\s*\(?\s*([a-z_][a-z0-9_]*)", rec)
            ent_name = m.group(1) if m else None
            if ent_name:
                store.record(OntologyEvent(
                    event_type="entity_refinement",
                    source="critic",
                    table_name=ctx.table_name,
                    entity_name=ent_name,
                    payload={"reason": "equivalence_preservation_finding"},
                    confidence=0.7,
                    evidence=issue.evidence[:200],
                ))
        elif issue.category in {
            "vocabulary_completeness", "ontology_consistency",
        }:
            # Mine for canonical entity names in the recommendation.
            for cand in re.findall(r"`([a-z_][a-z0-9_]*)`", issue.recommendation):
                if cand in {"name", "label", "tags"}:
                    continue
                store.record(OntologyEvent(
                    event_type="entity_refinement",
                    source="critic",
                    table_name=ctx.table_name,
                    entity_name=cand,
                    payload={"reason": issue.category},
                    confidence=0.55,
                    evidence=issue.evidence[:200],
                ))


def format_issues_for_repair(report: CritiqueReport) -> str:
    """Render the critic's issues for appending to the planner's retry prompt.

    The planner reads these and rewrites the plan to address them. Only
    ``block`` and ``warn`` issues go to repair; ``info`` is reviewer-only
    so we don't waste LLM tokens on stylistic nits.
    """
    actionable = [i for i in report.issues if i.severity in {"block", "warn"}]
    if not actionable:
        return ""
    lines = [
        "## Critic feedback — fix these on retry",
        "",
        f"The previous plan was critiqued. Verdict: **{report.overall_verdict}**, "
        f"Radix-retrieval score: {report.radix_retrieval_score}/10. "
        f"{report.block_count} blocking, {report.warn_count} warning.",
        "",
        "Each issue carries category + locus + evidence + recommendation. "
        "Address EVERY blocking issue and the warnings you can. Re-emit the "
        "full EnrichmentPlan with fixes applied — DO NOT just acknowledge.",
        "",
    ]
    for i, issue in enumerate(actionable, start=1):
        sev_marker = "🛑" if issue.severity == "block" else "⚠"
        lines.append(f"{i}. {sev_marker} **[{issue.category}] {issue.locus}**")
        lines.append(f"   - finding: {issue.finding}")
        if issue.evidence:
            lines.append(f"   - evidence: {issue.evidence}")
        lines.append(f"   - fix: {issue.recommendation}")
        lines.append("")
    return "\n".join(lines)


def render_critique_markdown(report: CritiqueReport) -> str:
    """Human-readable critique block for review_queue/<table>.plan.md."""
    if not report.issues:
        return (
            f"## Critic\n\n_No issues raised. Radix retrieval score: "
            f"{report.radix_retrieval_score}/10._\n"
        )
    lines = [
        "## Critic",
        "",
        f"**Verdict**: `{report.overall_verdict}` · "
        f"Radix retrieval: **{report.radix_retrieval_score}/10** · "
        f"{report.block_count} block · {report.warn_count} warn · "
        f"{report.info_count} info",
        "",
    ]
    if report.summary:
        lines.append(f"_{report.summary}_")
        lines.append("")
    by_sev: dict[str, list[CritiqueIssue]] = {"block": [], "warn": [], "info": []}
    for issue in report.issues:
        by_sev.setdefault(issue.severity, []).append(issue)
    for sev, marker, label in (
        ("block", "🛑", "Blocking"),
        ("warn", "⚠", "Warnings"),
        ("info", "ℹ", "Info"),
    ):
        items = by_sev.get(sev, [])
        if not items:
            continue
        lines.append(f"### {marker} {label} ({len(items)})")
        for issue in items:
            lines.append(
                f"- **[{issue.category}]** `{issue.locus}` — {issue.finding}"
            )
            if issue.evidence:
                lines.append(f"  - _evidence_: {issue.evidence}")
            lines.append(f"  - _fix_: {issue.recommendation}")
        lines.append("")
    return "\n".join(lines)


# ─── Deterministic pre-flight ────────────────────────────────


def _deterministic_findings(
    ctx: TableContext,
    plan: EnrichmentPlan,
    ontology: DomainOntology | None,
    *,
    eq_map: Any = None,
    cardinalities: list[Any] | None = None,
    canonical_paths: list[Any] | None = None,
) -> list[CritiqueIssue]:
    """Issues the LLM doesn't need to find — proven by the data."""
    issues: list[CritiqueIssue] = []

    # 1. Placeholder names — never acceptable.
    for cat, items in (
        ("dim", plan.proposed_dimensions or []),
        ("measure", plan.proposed_measures or []),
    ):
        for idx, item in enumerate(items):
            name = item.get("name")
            src = item.get("source_column")
            if name in _PLACEHOLDER_TOKENS:
                issues.append(CritiqueIssue(
                    category="reasoning_grounding",
                    severity="block",
                    locus=f"proposed_{cat}s[{idx}].name={name!r}",
                    finding=(
                        f"{cat} #{idx} has a placeholder name. "
                        "The plan is not reviewable."
                    ),
                    evidence=f"name={name!r}, source_column={src!r}",
                    recommendation=(
                        f"Replace the placeholder with a real {cat} name "
                        "derived from the source column or MDM business_name."
                    ),
                ))
            if src in _PLACEHOLDER_TOKENS and not item.get("is_derived"):
                issues.append(CritiqueIssue(
                    category="reasoning_grounding",
                    severity="block",
                    locus=f"proposed_{cat}s[{idx}].source_column={src!r}",
                    finding=(
                        f"{cat} `{name}` has no source column. "
                        "Without grounding, this is invented content."
                    ),
                    evidence=f"source_column={src!r}",
                    recommendation=(
                        f"Set source_column to a real column from `{ctx.table_name}` "
                        "or mark `is_derived=true` and supply case_when_sql."
                    ),
                ))

    # 2. PK rationality — when baseline declares a PK, the plan must mention it.
    baseline_pk = ctx.baseline_primary_key_column
    if baseline_pk:
        plan_dims = {(d.get("name") or "").lower() for d in (plan.proposed_dimensions or [])}
        plan_dims |= {
            (d.get("source_column") or "").lower()
            for d in (plan.proposed_dimensions or [])
        }
        if baseline_pk.lower() not in plan_dims and (plan.proposed_dimensions or []):
            issues.append(CritiqueIssue(
                category="pk_rationality",
                severity="warn",
                locus=f"proposed_dimensions (missing baseline PK `{baseline_pk}`)",
                finding=(
                    f"Baseline declares `{baseline_pk}` as primary_key but the "
                    "plan's dimensions don't include it."
                ),
                evidence=f"baseline_primary_key_column={baseline_pk!r}",
                recommendation=(
                    f"Add `{baseline_pk}` as a dimension with primary_key=yes "
                    "and preserve the baseline declaration."
                ),
            ))

    # 3. Structural-filter baking — every is_structural filter must be baked.
    structural = [
        f for f in (ctx.filters_on_this or []) if f.get("is_structural")
    ]
    if structural:
        explore = plan.proposed_explore or {}
        sql_always_where = (explore.get("sql_always_where") or "").lower()
        always_filter = explore.get("always_filter") or {}
        baked_in_dt = any(
            (dt.get("structural_filters") or [])
            for dt in (plan.proposed_derived_tables or [])
        )
        for f in structural:
            col = (f.get("column") or "").lower()
            val = str(f.get("value") or "").lower()
            covered = (
                col in sql_always_where
                or col in {k.lower() for k in always_filter}
                or baked_in_dt
            )
            if not covered:
                issues.append(CritiqueIssue(
                    category="structural_filter_baking",
                    severity="block",
                    locus=f"filters_on_this[{col}={val}]",
                    finding=(
                        f"Structural filter `{col} = {val}` appears in queries "
                        "but the plan exposes no derived_table or "
                        "sql_always_where to bake it in. Users would be able "
                        "to remove it and silently get wrong rows."
                    ),
                    evidence=(
                        f"filter is_structural=true; observed in "
                        f"{len(ctx.queries_using_this or [])} quer(y/ies)"
                    ),
                    recommendation=(
                        f"Add `{col} = {val}` to proposed_explore.sql_always_where "
                        "OR push it into a derived_table.structural_filters."
                    ),
                ))

    # 4. Partition / freshness coherence — if MDM marks a column as
    #    partitioned, it MUST surface as a dim_group.
    partition_cols: list[str] = []
    for mc in (ctx.mdm_columns or []):
        if mc.get("is_partitioned") or mc.get("partition_position"):
            n = mc.get("name") or ""
            if n:
                partition_cols.append(n)
    if partition_cols:
        dim_group_sources = {
            (dg.get("source_column") or "").lower()
            for dg in (plan.proposed_dimension_groups or [])
        }
        for pc in partition_cols:
            if pc.lower() not in dim_group_sources:
                issues.append(CritiqueIssue(
                    category="partition_freshness",
                    severity="warn",
                    locus=f"proposed_dimension_groups (missing partition `{pc}`)",
                    finding=(
                        f"MDM marks `{pc}` as the partition column but the plan "
                        "doesn't surface it as a dimension_group. Looker will "
                        "scan full partitions instead of pruning."
                    ),
                    evidence=f"mdm_columns[{pc}].is_partitioned=true",
                    recommendation=(
                        f"Add `{pc}` as a dimension_group (type=time, "
                        "timeframes=[date, week, month, quarter, year])."
                    ),
                ))

    # 5. Logical-type correctness — yesno columns proposed as string.
    type_issues = _check_logical_types(ctx, plan)
    issues.extend(type_issues)

    # 6. Ontology consistency — primary entity vocabulary.
    if ontology is not None:
        primary = ontology.primary_entity_for_table(ctx.table_name)
        if primary is not None:
            # Check whether the plan reasoning actually mentions the entity.
            blob = (plan.reasoning or "").lower()
            mentions_entity = (
                primary.name in blob
                or any(s.lower() in blob for s in (primary.synonyms or []))
            )
            if not mentions_entity:
                issues.append(CritiqueIssue(
                    category="vocabulary_completeness",
                    severity="warn",
                    locus="reasoning",
                    finding=(
                        f"Plan reasoning never names the primary entity "
                        f"`{primary.name}` (a.k.a. {', '.join(primary.synonyms[:3])}). "
                        "Radix retrieval relies on entity vocabulary alignment."
                    ),
                    evidence=f"ontology.table_to_primary_entity[{ctx.table_name}]={primary.name!r}",
                    recommendation=(
                        f"Rewrite `reasoning` to explicitly state that this "
                        f"table represents the `{primary.name}` entity at "
                        f"`{primary.grain_description}` grain."
                    ),
                ))

    # 7. Reasoning grounding — must be substantive (>= 80 chars, not boilerplate).
    reasoning = (plan.reasoning or "").strip()
    if len(reasoning) < 80:
        issues.append(CritiqueIssue(
            category="reasoning_grounding",
            severity="warn",
            locus="reasoning",
            finding=(
                f"`reasoning` is only {len(reasoning)} chars — too thin for a "
                "human reviewer to validate the plan against."
            ),
            evidence=f"reasoning={reasoning[:60]!r}",
            recommendation=(
                "Write 2-3 sentences citing what this table represents (entity + "
                "grain), what the plan changes vs the baseline, and the single "
                "biggest risk."
            ),
        ))

    # 7b. Equivalence preservation — life-or-death. If two columns are
    #     proven equivalent via JOIN ON closure, they MUST be described
    #     as the same entity in this plan (or, when the equivalent
    #     column lives on another table, this plan must not contradict).
    if eq_map is not None and ontology is not None:
        eq_issues = _check_equivalence_preservation(
            ctx, plan, ontology, eq_map,
        )
        issues.extend(eq_issues)

    # 7c. JOIN cardinality correctness — block when proposed_explore.joins
    #     declare a relationship that contradicts strong corpus evidence.
    if cardinalities:
        issues.extend(_check_join_cardinality(ctx, plan, cardinalities))

    # 7d. JOIN path grounding — warn when explore proposes a join that
    #     no real query has used (might be invented).
    if canonical_paths is not None:
        issues.extend(_check_join_path_grounding(
            ctx, plan, canonical_paths or [],
        ))

    # 8. Risk acknowledgement — sparse contexts must list risks.
    sparse = (
        (ctx.mdm_coverage_pct or 0) < 0.30
        or not ctx.existing_view_lkml
        or len(ctx.queries_using_this or []) < 3
    )
    if sparse and not (plan.risks or []):
        issues.append(CritiqueIssue(
            category="risk_acknowledgement",
            severity="warn",
            locus="risks",
            finding=(
                "Sparse-source table (low MDM, no baseline, or few queries) but "
                "the plan lists no risks."
            ),
            evidence=(
                f"mdm_coverage_pct={ctx.mdm_coverage_pct}, "
                f"baseline={'yes' if ctx.existing_view_lkml else 'no'}, "
                f"queries={len(ctx.queries_using_this or [])}"
            ),
            recommendation=(
                "Surface the sparse-source risk explicitly so the reviewer "
                "can decide whether to provide more context before enrichment."
            ),
        ))

    return issues


def _check_logical_types(
    ctx: TableContext, plan: EnrichmentPlan,
) -> list[CritiqueIssue]:
    """Yesno on flags, number on amounts — wrong type breaks SQL."""
    out: list[CritiqueIssue] = []
    mdm_by_col: dict[str, dict[str, Any]] = {
        (c.get("name") or "").lower(): c for c in (ctx.mdm_columns or [])
    }
    for idx, d in enumerate(plan.proposed_dimensions or []):
        src = (d.get("source_column") or "").lower()
        if not src:
            continue
        proposed_type = (d.get("type") or "").lower()
        mdm = mdm_by_col.get(src) or {}
        mdm_type = (mdm.get("type") or mdm.get("data_type") or "").upper()

        # Boolean signal: name patterns OR MDM type.
        is_flag = (
            mdm_type in {"BOOL", "BOOLEAN"}
            or src.startswith(("is_", "has_", "flag_"))
            or src.endswith(("_flag", "_ind", "_indicator"))
        )
        if is_flag and proposed_type not in {"yesno", ""}:
            out.append(CritiqueIssue(
                category="logical_type",
                severity="warn",
                locus=f"proposed_dimensions[{idx}].type",
                finding=(
                    f"`{src}` looks like a boolean flag but proposed as "
                    f"type={proposed_type!r}. Looker filtering will be wrong."
                ),
                evidence=(
                    f"mdm_type={mdm_type!r}, naming pattern matches flag/ind"
                ),
                recommendation="Set type=yesno.",
            ))

        # Numeric signal.
        is_numeric = mdm_type in {
            "NUMERIC", "FLOAT64", "INT64", "INTEGER", "NUMBER", "BIGNUMERIC",
        }
        if is_numeric and proposed_type in {"string"}:
            out.append(CritiqueIssue(
                category="logical_type",
                severity="warn",
                locus=f"proposed_dimensions[{idx}].type",
                finding=(
                    f"`{src}` is numeric in MDM but proposed as type=string. "
                    "Aggregations will fail."
                ),
                evidence=f"mdm_type={mdm_type!r}",
                recommendation="Set type=number.",
            ))
    return out


def _check_join_cardinality(
    ctx: TableContext,
    plan: EnrichmentPlan,
    cardinalities: list[Any],
) -> list[CritiqueIssue]:
    """Block plans whose proposed_explore.joins[i].relationship contradicts
    a strongly-supported corpus observation.

    Builds an index of (table_a, table_b) -> inferred cardinality + confidence,
    then walks each proposed_explore.join checking against the index. Direction
    is normalized: a `many_to_one` from base→joined matches a `one_to_many`
    inferred from joined→base.
    """
    out: list[CritiqueIssue] = []
    explore = plan.proposed_explore or {}
    proposed_joins = explore.get("joins") or []
    if not proposed_joins:
        return out

    base = (explore.get("base_view") or ctx.table_name).lower()

    # Index by unordered table pair.
    by_pair: dict[frozenset[str], Any] = {}
    for c in cardinalities:
        key: frozenset[str] = frozenset(
            {c.left_table.lower(), c.right_table.lower()}
        )
        # Keep the highest-confidence entry for each pair.
        existing = by_pair.get(key)
        if existing is None or c.confidence > existing.confidence:
            by_pair[key] = c

    for idx, j in enumerate(proposed_joins):
        right = (j.get("right_table") or j.get("other_table") or "").lower()
        if not right:
            continue
        proposed_rel = (j.get("relationship") or j.get("join_type") or "").lower()
        if not proposed_rel:
            # Missing relationship is its own warning — Looker defaults to
            # many_to_one which is often wrong.
            out.append(CritiqueIssue(
                category="join_cardinality_correctness",
                severity="warn",
                locus=f"proposed_explore.joins[{idx}]",
                finding=(
                    f"Join to `{right}` has no `relationship` field. Looker "
                    "will default to many_to_one — but corpus evidence "
                    "may say otherwise."
                ),
                evidence="missing relationship: field",
                recommendation=(
                    "Set `relationship:` to one of one_to_one, one_to_many, "
                    "many_to_one, many_to_many based on corpus cardinality."
                ),
            ))
            continue
        observed = by_pair.get(frozenset({base, right}))
        if observed is None or observed.cardinality == "unknown":
            continue
        if observed.confidence < 0.6:
            continue

        # Normalize observed cardinality to base→right direction.
        if observed.left_table.lower() == base:
            expected = observed.cardinality
        else:
            expected = {
                "one_to_many": "many_to_one",
                "many_to_one": "one_to_many",
                "one_to_one": "one_to_one",
                "many_to_many": "many_to_many",
            }.get(observed.cardinality, observed.cardinality)
        if proposed_rel != expected:
            out.append(CritiqueIssue(
                category="join_cardinality_correctness",
                severity="block",
                locus=f"proposed_explore.joins[{idx}].relationship={proposed_rel}",
                finding=(
                    f"Join `{base} → {right}` declared as `{proposed_rel}` "
                    f"but corpus evidence (confidence "
                    f"{int(observed.confidence * 100)}%, "
                    f"{observed.observations} observations) says `{expected}`. "
                    "Wrong relationship → silent fan-out → wrong numbers."
                ),
                evidence=(
                    f"observed cardinality {observed.cardinality} via "
                    f"{', '.join(observed.evidence[:1])}"
                ),
                recommendation=(
                    f"Change relationship to `{expected}`. If you have evidence "
                    "the corpus inference is wrong, surface it in `risks` so "
                    "the human reviewer can decide."
                ),
            ))
    return out


def _check_join_path_grounding(
    ctx: TableContext,
    plan: EnrichmentPlan,
    canonical_paths: list[Any],
) -> list[CritiqueIssue]:
    """Warn when proposed_explore.joins propose a (base → joined) edge
    that NO canonical path uses. Inventing joins misroutes Radix retrieval.

    INFO when no observed paths exist at all (no signal); WARN when there
    ARE observed paths but the proposed join isn't one of them.
    """
    out: list[CritiqueIssue] = []
    explore = plan.proposed_explore or {}
    proposed_joins = explore.get("joins") or []
    if not proposed_joins:
        return out

    base = (explore.get("base_view") or ctx.table_name).lower()
    paths_for_base = [
        p for p in canonical_paths if p.base_table.lower() == base
    ]
    if not paths_for_base:
        # No paths observed for this base — info only.
        return out

    observed_edges: set[tuple[str, str]] = set()
    for p in paths_for_base:
        prev = p.base_table.lower()
        for tbl, _jt in p.chain:
            observed_edges.add((prev, tbl.lower()))
            prev = tbl.lower()

    for idx, j in enumerate(proposed_joins):
        right = (j.get("right_table") or j.get("other_table") or "").lower()
        if not right:
            continue
        if (base, right) not in observed_edges:
            out.append(CritiqueIssue(
                category="join_path_grounding",
                severity="warn",
                locus=f"proposed_explore.joins[{idx}].right_table={right}",
                finding=(
                    f"Join `{base} → {right}` is not part of any canonical "
                    "path observed in the gold queries. The explore may "
                    "answer questions no analyst is actually asking."
                ),
                evidence=(
                    f"observed canonical paths from {base}: "
                    + ", ".join(
                        " → ".join(t for t, _ in p.chain[:3])
                        for p in paths_for_base[:3]
                    )
                ),
                recommendation=(
                    f"Either (a) drop the join to `{right}` if no query "
                    "needs it, or (b) cite a query in `risks` showing the "
                    "join is needed but wasn't captured by the corpus."
                ),
            ))
    return out


_VerdictLiteral = Literal["approve", "approve_with_warnings", "retry", "reject"]


def _check_equivalence_preservation(
    ctx: TableContext,
    plan: EnrichmentPlan,
    ontology: DomainOntology,
    eq_map: Any,
) -> list[CritiqueIssue]:
    """Enforce that proven JOIN-equivalent columns map to the same entity.

    For each equivalence class containing a column on this table, find
    which entity that column belongs to per the ontology, and verify
    every other member of the class belongs to the SAME entity. If a
    class spans multiple entities → that's an ontology consistency
    violation. If the plan describes our column without referencing the
    proven equivalence, that's still loggable as info.
    """
    out: list[CritiqueIssue] = []
    if not getattr(eq_map, "classes", None):
        return out

    table = ctx.table_name
    # Build column → entity lookup from ontology grain_columns.
    col_to_entity: dict[tuple[str, str], str] = {}
    for ent in ontology.entities:
        for tbl, cols in ent.grain_columns.items():
            for col in cols:
                col_to_entity[(tbl, col)] = ent.name

    # Plan dimension source columns, lowercased.
    plan_sources_on_this = {
        (d.get("source_column") or "").lower()
        for d in (plan.proposed_dimensions or [])
        if d.get("source_column")
    }

    for ec in eq_map.classes:
        # Which members live on this table?
        on_this = [
            (t, c) for (t, c) in ec.members if t == table
        ]
        if not on_this:
            continue

        # Map each member to its ontology entity (if any).
        member_entities: dict[str, list[tuple[str, str]]] = {}
        for (t, c) in ec.members:
            mapped_ent = col_to_entity.get((t, c))
            if mapped_ent:
                member_entities.setdefault(mapped_ent, []).append((t, c))

        # Multi-entity class → ontology contradiction. Block.
        if len(member_entities) >= 2:
            offenders = " vs ".join(
                f"{e}({len(cols)} members)"
                for e, cols in member_entities.items()
            )
            out.append(CritiqueIssue(
                category="equivalence_preservation",
                severity="block",
                locus=f"equivalence_class[{','.join(f'{t}.{c}' for t,c in sorted(ec.members)[:4])}]",
                finding=(
                    "Proven JOIN-equivalent columns are split across "
                    f"different entities in the ontology: {offenders}. "
                    "Same data, different names — Radix retrieval will be "
                    "inconsistent."
                ),
                evidence=(
                    f"equivalence class observed in {ec.query_count} quer(y/ies); "
                    f"members: {sorted(ec.members)}"
                ),
                recommendation=(
                    "Merge these entities in the ontology, OR re-classify "
                    f"the columns under one entity. Suggested winner: the "
                    f"entity with the most members ("
                    f"{max(member_entities, key=lambda k: len(member_entities[k]))})."
                ),
            ))
            continue

        # Single-entity class but our column isn't in the plan's
        # dimensions → info only. We don't block; the human may have
        # a reason. But we surface it so they can verify.
        for (t, c) in on_this:
            if c.lower() not in plan_sources_on_this:
                continue
            # Plan references this column. Does its description tie
            # back to the equivalence?
            ent_name = col_to_entity.get((t, c))
            if not ent_name:
                # Column on this table is in an equivalence class but
                # the ontology hasn't classified it — INFO so the next
                # ontology refresh can pick it up.
                out.append(CritiqueIssue(
                    category="equivalence_preservation",
                    severity="info",
                    locus=f"proposed_dimensions[source_column={c}]",
                    finding=(
                        f"`{c}` is JOIN-equivalent to "
                        f"{[m for m in ec.members if m != (t, c)][:3]} "
                        "across other tables but the ontology hasn't "
                        "classified it under any entity yet."
                    ),
                    evidence=(
                        f"equivalence class with {len(ec.members)} members, "
                        f"observed in {ec.query_count} quer(y/ies)"
                    ),
                    recommendation=(
                        f"Describe `{c}` consistently with the cross-table "
                        "members so the next ontology promotion folds them "
                        "into one entity."
                    ),
                ))
                break  # one info per class is enough
    return out


def _verdict_from_issues(
    issues: list[CritiqueIssue],
) -> _VerdictLiteral:
    has_block = any(i.severity == "block" for i in issues)
    has_warn = any(i.severity == "warn" for i in issues)
    if has_block:
        return "retry"
    if has_warn:
        return "approve_with_warnings"
    return "approve"


def _stricter_verdict(
    a: _VerdictLiteral, b: _VerdictLiteral,
) -> _VerdictLiteral:
    """Return the stricter of two verdicts."""
    rank = {"approve": 0, "approve_with_warnings": 1, "retry": 2, "reject": 3}
    return a if rank.get(a, 0) >= rank.get(b, 0) else b


def _deterministic_retrieval_score(
    ctx: TableContext, plan: EnrichmentPlan,
) -> int:
    """Heuristic 0-10 score: would Radix find this view?"""
    score = 5
    if ctx.mdm_table_description:
        score += 1
    if (plan.reasoning or "").strip() and len(plan.reasoning) >= 80:
        score += 1
    if plan.proposed_nl_question_count >= 5:
        score += 1
    if plan.proposed_dimensions and not any(
        d.get("name") in _PLACEHOLDER_TOKENS for d in plan.proposed_dimensions
    ):
        score += 1
    if not (ctx.queries_using_this or []):
        score -= 2
    return max(0, min(10, score))


def _deterministic_summary(
    ctx: TableContext, plan: EnrichmentPlan, issues: list[CritiqueIssue],
) -> str:
    n_block = sum(1 for i in issues if i.severity == "block")
    n_warn = sum(1 for i in issues if i.severity == "warn")
    if n_block:
        return (
            f"{n_block} blocking issue(s) prevent approval. "
            "Self-repair will retry; reviewer should still check the result."
        )
    if n_warn:
        return (
            f"{n_warn} warning(s) — plan is approvable but reviewer should "
            "verify the flagged items."
        )
    return "No issues raised by deterministic checks."


def _merge_issues(
    a: list[CritiqueIssue], b: list[CritiqueIssue],
) -> list[CritiqueIssue]:
    """Dedupe by (category, locus, finding[:60])."""
    seen: set[tuple[str, str, str]] = set()
    out: list[CritiqueIssue] = []
    for issue in a + b:
        key = (issue.category, issue.locus, issue.finding[:60])
        if key in seen:
            continue
        seen.add(key)
        out.append(issue)
    return out


# ─── LLM critic ──────────────────────────────────────────────


def _llm_critique(
    ctx: TableContext,
    plan: EnrichmentPlan,
    ontology: DomainOntology | None,
    all_fingerprints: list[Any],
    cfg: LumiConfig,
) -> CritiqueReport | None:
    """Run the Gemini-backed critic agent and return its CritiqueReport."""
    try:
        from google.adk.agents import LlmAgent
        from google.adk.runners import InMemoryRunner
        from google.adk.sessions import InMemorySessionService
        from google.genai import types as genai_types
    except ImportError as e:
        logger.warning("ADK not importable, deterministic critique only: %s", e)
        return None

    prompt = _build_critic_prompt(ctx, plan, ontology, all_fingerprints)
    safe_prompt = prompt.replace("{", "{{").replace("}", "}}")

    agent = LlmAgent(
        name=f"critic_{ctx.table_name}".replace(".", "_"),
        model=cfg.model_name,
        description="Critiques an EnrichmentPlan against 11 quality categories.",
        instruction=safe_prompt,
        output_schema=CritiqueReport,
        generate_content_config=genai_types.GenerateContentConfig(
            temperature=cfg.temperature,
            max_output_tokens=8000,
            response_mime_type="application/json",
        ),
    )
    runner = InMemoryRunner(agent=agent, app_name="lumi_critic")
    session_service: InMemorySessionService = runner.session_service  # type: ignore[assignment]

    import asyncio
    user_id, session_id = "lumi_critic", f"crit_{ctx.table_name}"

    async def _run() -> str | None:
        await session_service.create_session(
            app_name="lumi_critic", user_id=user_id, session_id=session_id,
        )
        last_text: str | None = None
        async for event in runner.run_async(
            user_id=user_id, session_id=session_id,
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="Critique the plan now.")],
            ),
        ):
            if (event.content and event.content.parts
                    and event.content.parts[0].text):
                last_text = event.content.parts[0].text
        return last_text

    try:
        text = asyncio.run(_run())
    except Exception as e:  # noqa: BLE001
        logger.warning("critic agent invocation failed: %s", e)
        return None

    if not text:
        return None
    return _parse_critique_response(text, ctx.table_name)


def _build_critic_prompt(
    ctx: TableContext,
    plan: EnrichmentPlan,
    ontology: DomainOntology | None,
    all_fingerprints: list[Any],
) -> str:
    """Compose the critic prompt — Radix-aware, evidence-anchored.

    The critic gets the same grounded context the planner had (so it
    can verify against evidence, not vibes) plus the plan it's
    critiquing.
    """
    # Lazy imports for renderers.
    from lumi.grounding import build_grounding_signals, render_grounding_signals
    from lumi.narrative import build_table_narrative, render_table_narrative

    parts: list[str] = [
        "# Critic task",
        "",
        f"You are the QUALITY GATE for the enrichment plan of `{ctx.table_name}`. "
        "The plan you're reviewing was authored by another agent. Your job is "
        "to find what's wrong before it goes to a human reviewer and before "
        "it spends ~10K tokens of enrichment budget. Be ruthless on substance, "
        "polite on form.",
        "",
        "## Critic mission",
        "This view will be served to **Radix**, a downstream NL→SQL retrieval "
        "system. An analyst will type 'How many active cardmembers spent more "
        "than $1K last quarter?' — the retrieval layer reads view + dim + "
        "measure descriptions and synonyms to decide WHICH view to load. "
        "If our descriptions don't match how analysts speak, Radix sends the "
        "wrong view, the LLM writes the wrong SQL, and the answer is wrong.",
        "",
        "## Output contract",
        "Return a CritiqueReport (Pydantic). Every issue MUST carry:",
        "- `category`: one of the 11 categories below",
        "- `severity`: 'block' (must fix; pipeline retries), 'warn' (reviewer "
        "should verify), 'info' (nit).",
        "- `locus`: the exact field reference — e.g. "
        "'proposed_dimensions[3].name=cm11' or 'reasoning' — so the planner "
        "knows where to fix.",
        "- `finding`: ONE sentence on what's wrong. No hedging.",
        "- `evidence`: cite the specific MDM column / baseline / query / "
        "ontology entry that proves the finding.",
        "- `recommendation`: concrete fix the planner can apply on retry.",
        "",
        "Aim for 2-5 substantive issues. If the plan is clean, return zero "
        "issues with verdict='approve' — do NOT manufacture findings.",
        "",
        "## 11 critique categories",
        "1. **radix_retrieval_alignment**: would Radix find this view for "
        "natural questions an analyst would ask? Names + descriptions must "
        "use the analyst's vocabulary, not raw column codes.",
        "2. **vocabulary_completeness**: does the plan's reasoning name the "
        "primary entity (e.g. cardmember) and surface the canonical synonyms?",
        "3. **disambiguation**: when two columns mean different things "
        "(cm11=cardmember PK vs cm15=spouse FK), do descriptions disambiguate?",
        "4. **logical_type**: yesno on flags, number on amounts, string on "
        "categories, dimension_group on dates. Wrong type → wrong SQL.",
        "5. **ontology_consistency**: same entity must use the same canonical "
        "name across tables. If the ontology says cm11≡cust_xref_id are "
        "cardmember, both must be described as cardmember.",
        "6. **equivalence_preservation**: JOIN-proven equivalent columns "
        "must be described as the same entity in the plan.",
        "7. **partition_freshness**: partition columns must be dim_groups; "
        "freshness claims must match MDM hints.",
        "8. **pk_rationality**: the PK candidate must actually identify "
        "one row, not be a high-cardinality FK.",
        "9. **structural_filter_baking**: model invariants (data_source = "
        "'cornerstone') must NOT be exposed as user filters.",
        "10. **risk_acknowledgement**: sparse MDM, missing baseline, complex "
        "CTE chains must show up in `risks`, not be silently glossed.",
        "11. **reasoning_grounding**: `reasoning` must cite real evidence "
        "(MDM, baseline, query usage). No vague hand-waving.",
        "",
        "## What to score (radix_retrieval_score, 0-10)",
        "Imagine 10 typical analyst questions about this table's domain. "
        "How many would Radix correctly route to this view based on the "
        "plan's vocabulary alone? That's the score.",
        "",
    ]

    # Domain ontology context.
    if ontology is not None:
        from lumi.ontology_builder import render_ontology_for_table
        ontology_md = render_ontology_for_table(ontology, ctx.table_name)
        if ontology_md:
            parts.extend([ontology_md, ""])

    # Grounding context — same evidence the planner saw.
    if all_fingerprints:
        try:
            grounding = build_grounding_signals(ctx, all_fingerprints, {})
            narrative = build_table_narrative(
                ctx, all_fingerprints=all_fingerprints, eq_map=None,
            )
            parts.extend([
                render_table_narrative(narrative),
                "",
                render_grounding_signals(grounding),
                "",
            ])
        except Exception as e:  # noqa: BLE001
            logger.debug("could not build grounding for critic prompt: %s", e)

    # The plan being reviewed.
    parts.append("## Plan under review")
    parts.append("")
    parts.append("```json")
    parts.append(json.dumps(plan.model_dump(), indent=2, default=str))
    parts.append("```")

    parts.append("")
    parts.append("Now critique it.")
    return "\n\n".join(parts)


def _parse_critique_response(text: str, table_name: str) -> CritiqueReport | None:
    """Tolerant parser, same pattern as plan_builder."""
    # Strategy 1: as-is.
    try:
        return CritiqueReport(**json.loads(text))
    except Exception:  # noqa: BLE001
        pass

    # Strategy 2: strip code fence.
    s = text.strip()
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl + 1:]
        if s.endswith("```"):
            s = s[:-3]
        s = s.strip()
        try:
            return CritiqueReport(**json.loads(s))
        except Exception:  # noqa: BLE001
            pass

    # Strategy 3: balanced object extract.
    extracted = _extract_first_json_object(s)
    if extracted:
        try:
            return CritiqueReport(**json.loads(extracted))
        except Exception:  # noqa: BLE001
            repaired = re.sub(r",(\s*[}\]])", r"\1", extracted)
            try:
                return CritiqueReport(**json.loads(repaired))
            except Exception:  # noqa: BLE001
                pass

    logger.warning(
        "critic for %s returned unparseable output. First 200 chars: %r",
        table_name, text[:200],
    )
    return None


def _extract_first_json_object(text: str) -> str | None:
    start = -1
    depth = 0
    in_string = False
    escape_next = False
    for i, ch in enumerate(text):
        if escape_next:
            escape_next = False
            continue
        if in_string:
            if ch == "\\":
                escape_next = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            if start == -1:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start != -1:
                return text[start:i + 1]
    return None


