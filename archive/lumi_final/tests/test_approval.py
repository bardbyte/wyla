"""Session 3 tests for ``lumi.approval``.

The approval gate parses human decisions out of ``review_queue/<table>.plan.md``
files. Tests cover the three states a plan can be in:

  - APPROVED (with several markdown variants)
  - REJECTED (with feedback captured)
  - PENDING (no decision yet — blocks the gate)

These tests are pure file-IO + regex — no LLM, no network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lumi.approval import (
    collect_approvals,
    parse_approval_file,
    parse_approval_text,
)
from lumi.guardrails import check_approvals
from lumi.schemas import EnrichmentPlan, PlanApproval


# ─── Plan fixture (shared) ───────────────────────────────────────


def _make_plan(name: str) -> EnrichmentPlan:
    return EnrichmentPlan(
        table_name=name,
        proposed_dimensions=[
            {"name": "x", "type": "string", "source_column": "x", "description_summary": "x"}
        ],
        proposed_measures=[],
        reasoning="Test plan with sufficient reasoning to clear the warning threshold "
        "for downstream guardrail checks.",
    )


def _write_plan_md(path: Path, body: str) -> None:
    """Helper: write a minimal plan.md skeleton + the variant body under test."""
    skeleton = (
        "---\n"
        "table_name: example\n"
        "complexity: medium\n"
        "---\n\n"
        "# Enrichment plan: example\n\n"
        "## Reasoning\nDoes the thing.\n\n"
        "## Proposed dimensions (1)\n- x (string)\n\n"
        "---\n"
        "APPROVAL (append below):\n"
    )
    path.write_text(skeleton + body, encoding="utf-8")


# ─── Required tests (per LUMI_BUILD_PLAN.md Session 3) ──────────


def test_approval_parses_approved(tmp_path: Path) -> None:
    """Recognise ``✅ APPROVED`` plus the common checkbox variants."""
    variants = {
        "plain": "✅ APPROVED\n",
        "ticked_lower": "- [x] ✅ APPROVED\n",
        "ticked_upper": "- [X] ✅ APPROVED\n",
        "no_emoji": "[x] APPROVED\n",
        "with_note": "✅ APPROVED — yes to filtered measure on data_source\n",
        "no_marker": "APPROVED\n",
    }

    approvals: list[PlanApproval] = []
    for name, body in variants.items():
        plan_md = tmp_path / f"{name}.plan.md"
        _write_plan_md(plan_md, body)
        approval = parse_approval_file(plan_md)
        approvals.append(approval)
        assert approval.approved, f"variant {name!r} not recognised as approved"
        assert approval.table_name == name

    # The "with_note" variant should round-trip the trailing note as feedback.
    with_note = next(a for a in approvals if a.table_name == "with_note")
    assert with_note.feedback is not None
    assert "filtered measure" in with_note.feedback


def test_approval_parses_rejected_with_feedback(tmp_path: Path) -> None:
    """Rejection plus the optional ``Feedback:`` block becomes a structured row."""
    plan_md = tmp_path / "cornerstone_metrics.plan.md"
    _write_plan_md(
        plan_md,
        "- [x] ❌ REJECTED\n\n"
        "**Feedback:**\n"
        "primary_key should be the compound (bus_seg, rpt_dt, sub_product_group),\n"
        "not just the bus_seg column. Also rename `total_bb` → `total_billed_business`.\n",
    )

    approval = parse_approval_file(plan_md)
    assert approval.table_name == "cornerstone_metrics"
    assert approval.approved is False
    assert approval.feedback is not None
    assert "primary_key should be the compound" in approval.feedback
    assert "total_billed_business" in approval.feedback


def test_approval_parses_rejected_inline_feedback() -> None:
    """A rejection with the reason on the SAME line should still capture feedback."""
    text = (
        "Some preamble.\n\n"
        "❌ REJECTED — fix the join order, drop the synthetic primary key\n"
    )
    approval = parse_approval_text("explore_x", text)
    assert approval.approved is False
    assert approval.feedback is not None
    assert "join order" in approval.feedback


def test_approval_blocks_on_pending(tmp_path: Path) -> None:
    """Files with neither box ticked → guardrail blocks with a named offender."""
    decided = tmp_path / "approved_table.plan.md"
    _write_plan_md(decided, "✅ APPROVED\n")

    pending = tmp_path / "pending_table.plan.md"
    _write_plan_md(
        pending,
        # The reviewer left the empty-checkbox template alone.
        "- [ ] ✅ APPROVED\n"
        "- [ ] ❌ REJECTED\n\n"
        "**Feedback:**\n"
        "(write any modifications here)\n",
    )

    approvals = collect_approvals(str(tmp_path))
    by_name = {a.table_name: a for a in approvals}
    assert by_name["approved_table"].approved is True
    pending_row = by_name["pending_table"]
    assert pending_row.approved is False
    assert pending_row.approver == "pending"
    assert pending_row.feedback and "pending" in pending_row.feedback.lower()

    plans = [_make_plan("approved_table"), _make_plan("pending_table")]
    gate = check_approvals(approvals, plans)
    # Pending row was rejected without real feedback → guardrail blocks.
    assert gate.status == "fail", gate.blocking_failures
    assert any("pending_table" in b for b in gate.blocking_failures)


def test_approval_collect_returns_empty_for_missing_dir(tmp_path: Path) -> None:
    """Non-existent queue dir returns ``[]`` rather than raising."""
    assert collect_approvals(str(tmp_path / "does_not_exist")) == []


def test_approval_latest_decision_wins(tmp_path: Path) -> None:
    """If the file has both APPROVED and REJECTED markers, the LATER one wins.

    Reviewers sometimes leave the template's example "✅ APPROVED" line above
    their real "❌ REJECTED" decision; we treat the bottom-most as authoritative.
    """
    plan_md = tmp_path / "x.plan.md"
    _write_plan_md(
        plan_md,
        "Earlier in the file: ✅ APPROVED (template example)\n\n"
        "- [x] ❌ REJECTED\n\n"
        "**Feedback:**\nbad join order\n",
    )
    approval = parse_approval_file(plan_md)
    assert approval.approved is False
    assert "join order" in (approval.feedback or "")


@pytest.mark.parametrize(
    "marker",
    [
        "  ✅ APPROVED",
        "> ✅ APPROVED",  # someone quoted it as a markdown blockquote
        "* [x] APPROVED",  # asterisk list bullet
    ],
)
def test_approval_marker_indentation(marker: str) -> None:
    """Marker recognition tolerates leading whitespace and quoting."""
    approval = parse_approval_text("t", f"some context\n{marker}\n")
    assert approval.approved
