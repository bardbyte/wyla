"""Stage 2: Plan — LLM expert assessment + change plan per table.

Generates a review queue with one plan per table, sorted by priority.
Low-risk tables (description-only changes) auto-approve.
Structural changes (new PK, derived tables, type changes) require human review.

Usage:
    plans = generate_plans(table_contexts, ecosystem_brief, model)
    review = build_review_queue(plans)
    # Human reviews review_queue/REVIEW.md
    # Then: approved = load_approved_plans("review_queue/")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from lumi.schemas import TableContext

logger = logging.getLogger("lumi.planner")


@dataclass
class PlannedChange:
    """One planned change to a view/explore."""
    action: str             # ADD, CHANGE, REMOVE, UPGRADE
    target: str             # "dimension rpt_dt", "measure total_bb", "primary_key"
    description: str        # what specifically changes
    reason: str             # WHY, referencing LookML skill section
    risk: str               # low, medium, high
    skill_reference: str    # "Skill §1: EXTRACT → dimension_group"


@dataclass
class TablePlan:
    """Complete plan for one table's enrichment."""
    table_name: str
    priority_score: float
    priority_rank: int
    query_count: int

    # LLM expert assessment (from Gemini)
    llm_understanding: str          # what the LLM thinks this table IS
    llm_existing_assessment: str    # what the LLM thinks about current LookML quality

    # Deterministic diff (no LLM)
    existing_dimensions: int
    existing_measures: int
    existing_dim_groups: int
    has_primary_key: bool
    has_sql_table_name: bool
    mdm_coverage_pct: float

    # What needs to change
    new_measures_needed: list[str]
    new_derived_tables: list[str]
    new_derived_dimensions: list[str]
    description_upgrades_needed: int
    dimension_group_conversions: list[str]  # date dims → dim_groups
    filtered_measures_needed: list[str]

    # Planned changes with expert reasoning
    changes: list[PlannedChange]

    # Auto-approval logic
    has_structural_changes: bool    # new PK, derived tables, type changes, joins
    auto_approved: bool             # True if description-only changes
    human_approved: bool | None     # None = pending, True/False = reviewed

    # Human notes (filled during review)
    human_notes: str = ""


@dataclass
class ReviewQueue:
    """The full review queue presented to the human."""
    total_tables: int
    auto_approved_count: int
    needs_review_count: int
    plans: list[TablePlan]


def compute_priority(ctx: TableContext) -> float:
    """Priority = query_count × (1 + new_changes_needed).
    Higher = process first."""
    query_count = len(ctx.queries_using_this)
    new_measures = len(ctx.aggregations)
    new_derived = len(ctx.ctes_referencing_this) + len(ctx.case_whens)
    return query_count * (1 + new_measures + new_derived)


def compute_deterministic_diff(ctx: TableContext) -> dict:
    """What the existing view has vs what gold queries need.
    Pure set comparison, no LLM."""
    import lkml

    existing = {"dimensions": 0, "measures": 0, "dim_groups": 0,
                "has_pk": False, "has_sql_table_name": False}

    if ctx.existing_view_lkml:
        try:
            parsed = lkml.load(ctx.existing_view_lkml)
            views = parsed.get("views", [])
            if views:
                v = views[0]
                existing["dimensions"] = len(v.get("dimensions", []))
                existing["measures"] = len(v.get("measures", []))
                existing["dim_groups"] = len(v.get("dimension_groups", []))
                existing["has_pk"] = any(
                    d.get("primary_key") == "yes"
                    for d in v.get("dimensions", [])
                )
                existing["has_sql_table_name"] = bool(v.get("sql_table_name"))
        except Exception:
            pass

    # What gold queries need that doesn't exist
    needed_measures = [
        f"{a.get('function', '?')}({a.get('column', '?')})"
        for a in ctx.aggregations
    ]

    needed_dim_groups = [
        d["column"] for d in ctx.date_functions
    ]

    needed_derived_tables = [
        c.get("alias", "?") for c in ctx.ctes_referencing_this
    ]

    needed_derived_dims = [
        c.get("alias", "?") for c in ctx.case_whens
    ]

    # Filtered measures: filters appearing in >80% of queries for this table
    filter_freq: dict[str, int] = {}
    total_queries = len(ctx.queries_using_this)
    for f in ctx.filters_on_this:
        col = f.get("column", "")
        if not f.get("is_structural"):
            filter_freq[col] = filter_freq.get(col, 0) + 1

    high_freq_filters = [
        col for col, count in filter_freq.items()
        if total_queries > 0 and count / total_queries >= 0.8
    ]

    return {
        "existing": existing,
        "needed_measures": needed_measures,
        "needed_dim_groups": needed_dim_groups,
        "needed_derived_tables": needed_derived_tables,
        "needed_derived_dims": needed_derived_dims,
        "high_freq_filters": high_freq_filters,
        "description_upgrades": existing["dimensions"],  # all dims need desc review
    }


def classify_risk(diff: dict, ctx: TableContext) -> tuple[bool, list[PlannedChange]]:
    """Determine if changes are structural (need review) or safe (auto-approve).

    Structural changes:
    - Adding/changing primary_key
    - Creating derived_table views (CTEs)
    - Changing dimension type (string → number, dim → dim_group)
    - New explore joins
    - New sql_always_where

    Safe changes (auto-approve):
    - Description upgrades
    - Adding tags/synonyms
    - Adding value_format to existing measures
    - Adding group_label
    """
    changes = []
    has_structural = False

    # Primary key
    if not diff["existing"]["has_pk"]:
        has_structural = True
        changes.append(PlannedChange(
            action="ADD", target="primary_key",
            description="Set primary_key on the identity column",
            reason="Without primary_key, Looker's symmetric aggregate "
                   "protection is disabled. Joins will silently double-count.",
            risk="high",
            skill_reference="Skill §3: primary_key and symmetric aggregates"
        ))

    # sql_table_name
    if not diff["existing"]["has_sql_table_name"]:
        has_structural = True
        changes.append(PlannedChange(
            action="ADD", target="sql_table_name",
            description="Add sql_table_name pointing to BigQuery table",
            reason="Looker can't find the table without this. "
                   "Looker MCP can't generate FROM clause.",
            risk="high",
            skill_reference="Skill §2: required attributes"
        ))

    # Dimension group conversions
    for col in diff["needed_dim_groups"]:
        has_structural = True
        changes.append(PlannedChange(
            action="CHANGE", target=f"dimension {col} → dimension_group",
            description=f"Convert plain dimension {col} to dimension_group with timeframes",
            reason=f"Gold queries use YEAR/MONTH on {col}. As a plain dimension, "
                   "Looker generates string equality filters instead of DATE_TRUNC. "
                   "Wrong filter syntax = wrong results.",
            risk="medium",
            skill_reference="Skill §1: EXTRACT → dimension_group"
        ))

    # Derived tables
    for cte_alias in diff["needed_derived_tables"]:
        has_structural = True
        changes.append(PlannedChange(
            action="ADD", target=f"derived_table view from CTE '{cte_alias}'",
            description=f"Create new view with derived_table from CTE {cte_alias}",
            reason="CTE has structural filters that define analytical scope. "
                   "These must be baked into a derived_table, not exposed as "
                   "user-selectable filters.",
            risk="medium",
            skill_reference="Skill §1: CTE → derived_table"
        ))

    # Derived dimensions
    for case_alias in diff["needed_derived_dims"]:
        has_structural = True
        changes.append(PlannedChange(
            action="ADD", target=f"derived dimension '{case_alias}'",
            description=f"Create dimension with CASE WHEN SQL for {case_alias}",
            reason="Gold query computes this derived category. Making it a "
                   "named dimension means Radix can select it directly.",
            risk="medium",
            skill_reference="Skill §1: CASE WHEN → derived dimension + order_by_field"
        ))

    # New measures (low risk — additive)
    for measure_expr in diff["needed_measures"]:
        changes.append(PlannedChange(
            action="ADD", target=f"measure: {measure_expr}",
            description=f"Add measure for {measure_expr}",
            reason="Gold queries aggregate this column. Named measure is "
                   "unambiguous for Radix selection.",
            risk="low",
            skill_reference="Skill §1: SUM/COUNT/AVG → measure"
        ))

    # Filtered measures
    for col in diff["high_freq_filters"]:
        changes.append(PlannedChange(
            action="ADD", target=f"filtered measure (default: {col})",
            description=f"Add filtered measure baking in {col} default",
            reason="This filter appears in >80% of queries. A filtered "
                   "measure gives Radix a default slice.",
            risk="low",
            skill_reference="Skill §1: >80% frequency → filtered measure"
        ))

    # Description upgrades (always low risk)
    if diff["description_upgrades"] > 0:
        changes.append(PlannedChange(
            action="UPGRADE",
            target=f"{diff['description_upgrades']} field descriptions",
            description="Upgrade descriptions using MDM + gold query context",
            reason="Current descriptions are empty or just column names. "
                   "Rich descriptions enable Radix semantic matching.",
            risk="low",
            skill_reference="Skill §2: description 15-200 chars"
        ))

    return has_structural, changes


def format_plan_markdown(plan: TablePlan) -> str:
    """Format a single table plan as readable markdown."""
    status = "AUTO-APPROVED (description-only)" if plan.auto_approved \
        else "⚠ NEEDS REVIEW (structural changes)"

    lines = [
        f"## {plan.table_name}",
        f"**Priority:** {plan.priority_rank}/{plan.priority_rank} | "
        f"**Queries:** {plan.query_count} | **Status:** {status}",
        "",
        "### What this table is",
        plan.llm_understanding,
        "",
        "### Current view assessment",
        plan.llm_existing_assessment,
        "",
        "### Existing LookML",
        f"- {plan.existing_dimensions} dimensions, "
        f"{plan.existing_measures} measures, "
        f"{plan.existing_dim_groups} dimension_groups",
        f"- Primary key: {'✓' if plan.has_primary_key else '✗ MISSING'}",
        f"- sql_table_name: {'✓' if plan.has_sql_table_name else '✗ MISSING'}",
        f"- MDM coverage: {plan.mdm_coverage_pct:.0%}",
        "",
        "### Planned changes",
    ]

    for i, change in enumerate(plan.changes, 1):
        risk_icon = {"low": "●", "medium": "◐", "high": "○"}[change.risk]
        lines.extend([
            "",
            f"**{i}. {change.action}: {change.target}** [{change.risk} risk {risk_icon}]",
            f"  {change.description}",
            f"  *Why:* {change.reason}",
            f"  *Ref:* {change.skill_reference}",
        ])

    if not plan.auto_approved:
        lines.extend([
            "",
            "### Your decision",
            "- [ ] Approve all changes as planned",
            "- [ ] Approve with modifications (edit notes below)",
            "- [ ] Skip this table for now",
            "",
            "**Notes:**",
            "```",
            plan.human_notes or "(write any modifications here)",
            "```",
        ])

    return "\n".join(lines)


def format_review_document(queue: ReviewQueue) -> str:
    """Format the full review queue as a single markdown document."""
    lines = [
        "# LUMI enrichment review",
        "",
        f"**Tables:** {queue.total_tables} | "
        f"**Auto-approved:** {queue.auto_approved_count} | "
        f"**Needs review:** {queue.needs_review_count}",
        "",
        "---",
        "",
        "## Tables needing review",
        "",
    ]

    # Review-needed tables first
    for plan in queue.plans:
        if not plan.auto_approved:
            lines.append(format_plan_markdown(plan))
            lines.append("\n---\n")

    lines.append("## Auto-approved tables (description-only changes)\n")

    for plan in queue.plans:
        if plan.auto_approved:
            lines.append(format_plan_markdown(plan))
            lines.append("\n---\n")

    return "\n".join(lines)
