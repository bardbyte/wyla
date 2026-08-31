"""The answer serializer (E18): A2UI-shaped, and it REFUSES.

An answer without its meridian line or without a grain is not a
formatting problem, it is an ungoverned answer. The serializer raises
rather than emitting one, so the reality law is enforced at the last
gate rather than trusted upstream.
"""

from __future__ import annotations

from typing import Any

from sahs.tools.api import Build

from .contract import Contract
from .generate import Generation
from .plan import Plan

SCHEMA = "a2ui.answer/1"


class RenderRefused(ValueError):
    """The payload would have been ungoverned; nothing was rendered."""


def _tier(build: Build, metric_id: str) -> tuple[str, str]:
    for row in build.metrics:
        if row.get("id") == metric_id:
            try:
                from sahs.compiler.display import tier_of_metric
                return tier_of_metric(row), (row.get("status_served")
                                             or row.get("status") or "")
            except Exception:
                return "gu", row.get("status_served", "")
    return "gu", ""


def render_answer(build: Build, plan: Plan, gen: Generation,
                  contract: Contract) -> dict[str, Any]:
    if not gen.definition_line:
        raise RenderRefused(
            "no meridian line for this metric: an answer that cannot "
            "say where its definition comes from is not servable")
    if not plan.grain:
        raise RenderRefused(
            "no grain on this plan: an answer that cannot say what one "
            "row means is not servable")

    tier, status = _tier(build, plan.metric_id)
    limits: list[str] = []
    if not gen.rows:
        denied = (gen.execution.get("meta") or {}).get("live_denied")
        limits.append(
            "validated by dry run, not executed live"
            + (f" ({denied})" if denied else ""))
    if gen.execution.get("withheld_rows"):
        limits.append(f"{gen.execution['withheld_rows']} further rows "
                      "withheld from this view")
    for warning in gen.warnings:
        limits.append(f"{warning.get('code')}: {warning.get('detail')}")
    for failure in contract.failures():
        limits.append(f"unverified — {failure.text}: {failure.evidence}")
    if gen.repaired:
        limits.append("the first composed query was refused by the "
                      "validator and repaired before running")

    return {
        "schema": SCHEMA,
        "meridian_line": gen.definition_line,
        "grain": plan.grain,
        "metric": {"id": plan.metric_id, "label": plan.metric_label,
                   "fp": plan.metric_fp, "tier": tier, "status": status,
                   "table": plan.table},
        "prose": gen.prose,
        "sql": gen.sql,
        "why": gen.why,
        "rows": gen.rows,
        "result_schema": gen.result_schema,
        "bytes_processed": gen.bytes_processed,
        "verdict": contract.to_dict(),
        "plan": plan.to_dict(),
        "build_id": build.version,
        "limits": limits,
        "actions": [
            {"id": "open_metric", "label": "open the metric profile",
             "href": f"#/metric/{plan.metric_id}", "enabled": True},
            {"id": "open_table", "label": "open the table profile",
             "href": f"#/table/{plan.table}", "enabled": True},
            {"id": "promote", "label": "propose as a certified metric",
             "enabled": False, "note": "arrives with the steward loop (B2)"},
        ],
    }
