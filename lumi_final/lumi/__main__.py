"""LUMI CLI — entry points for the two-phase pipeline.

Phase 1 (cheap, deterministic + sqlglot + lkml):
  python -m lumi plan
    → Stages 1-4: Parse → Discover → Stage → Plan
    → writes review_queue/<table>.plan.md + data/plans/<table>.plan.json
    → user reviews and ticks `[x] ✅ APPROVED` or `[x] ❌ REJECTED`

  python -m lumi status
    → prints the 7-stage progress table

Phase 2 (expensive, Gemini-driven):
  python -m lumi execute
    → Stages 5-8: Enrich → Validate → Publish
    → only runs for tables with PlanApproval(approved=True)
    → resumable: re-running skips tables already in data/enriched/

  python -m lumi execute --table cornerstone_metrics
    → single-table execute (iteration on prompts)

  python -m lumi execute --dry-run
    → uses fixture EnrichedOutputs from tests/fixtures/llm_responses/
    → no Vertex tokens spent
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any


def _print_result(label: str, result: Any) -> None:
    print()
    print("=" * 78)
    print(f"  {label} — {result.elapsed_s()}s elapsed")
    print("=" * 78)
    print(
        f"  tables: {result.tables_total} total, "
        f"{result.tables_succeeded} ok, "
        f"{result.tables_failed} failed"
        + (f", {result.tables_skipped_resume} skipped (resume)"
           if result.tables_skipped_resume else "")
    )
    if result.coverage_pct is not None:
        print(f"  coverage: {result.coverage_pct:.1f}%")
    for k, v in (result.extra or {}).items():
        print(f"  {k}: {v}")
    if result.failures:
        print(f"\n  {len(result.failures)} failure(s):")
        for f in result.failures[:10]:
            print(f"    - [{f['stage']}] {f['table']}: {f['error']}")
        if len(result.failures) > 10:
            print(f"    … and {len(result.failures) - 10} more")
    if result.files_written:
        print(f"\n  wrote {len(result.files_written)} files")


def _cmd_plan(args: argparse.Namespace) -> int:
    """Phase 1: deterministic Parse → Discover → Stage → Plan."""
    from lumi.config import LumiConfig
    from lumi.pipeline import PipelineHaltError, run_plan_phase

    cfg = LumiConfig()
    if args.input:
        cfg.gold_queries_dir = args.input

    print(f"Phase 1: planning from {cfg.gold_queries_dir}")
    only = args.table or None
    try:
        result = run_plan_phase(cfg, only_tables=only)
    except PipelineHaltError as e:
        print(f"\nHALT: {e}", file=sys.stderr)
        return 2
    _print_result("Phase 1 — plan", result)
    print(
        "\nNext: open review_queue/<table>.plan.md, tick a ✅/❌ box, "
        "then `python -m lumi execute`."
    )
    return 0 if result.tables_failed == 0 else 1


def _cmd_status(args: argparse.Namespace) -> int:
    """Print 7-stage progress for the current run."""
    from lumi.config import LumiConfig
    from lumi.pipeline import LumiPipeline

    LumiPipeline(LumiConfig()).print_status()
    return 0


def _cmd_approve(args: argparse.Namespace) -> int:
    """Auto-approve description-only plans, or report pending."""
    from pathlib import Path

    from lumi.approval import collect_approvals

    queue_dir = Path(args.queue)
    approvals = collect_approvals(str(queue_dir))
    if not approvals:
        print(f"No plan files found under {queue_dir}/")
        return 1

    print(f"{'TABLE':<48} {'APPROVED?':<10} {'BY':<14} FEEDBACK")
    print("-" * 100)
    pending = 0
    approved = 0
    rejected = 0
    for a in approvals:
        flag = "✓" if a.approved else ("✗" if a.approver != "pending" else "·")
        print(f"{a.table_name[:47]:<48} {flag:<10} {a.approver:<14} "
              f"{(a.feedback or '')[:50]}")
        if a.approver == "pending":
            pending += 1
        elif a.approved:
            approved += 1
        else:
            rejected += 1
    print()
    print(f"Summary: {approved} approved, {rejected} rejected, {pending} pending")
    return 0 if pending == 0 else 2


def _cmd_execute(args: argparse.Namespace) -> int:
    """Phase 2: Enrich → Validate → Publish for approved plans only."""
    from lumi.config import LumiConfig
    from lumi.pipeline import PipelineHaltError, run_execute_phase

    cfg = LumiConfig()
    if args.max_concurrent:
        cfg.max_concurrent_enrichments = args.max_concurrent

    print(
        f"Phase 2: executing approved plans"
        f"{' (DRY RUN — no Vertex calls)' if args.dry_run else ''}"
        f"{' (FORCE — re-enriching cached)' if args.force else ''}"
    )
    only = args.table or None
    try:
        result = run_execute_phase(
            cfg,
            only_tables=only,
            force=args.force,
            dry_run=args.dry_run,
        )
    except PipelineHaltError as e:
        print(f"\nHALT: {e}", file=sys.stderr)
        return 2

    _print_result("Phase 2 — execute", result)

    # Exit status logic: failures or coverage below target → 1.
    if result.tables_failed:
        return 1
    if result.coverage_pct is not None and result.coverage_pct < cfg.coverage_target_pct:
        print(
            f"\n⚠  Coverage {result.coverage_pct:.1f}% is below target "
            f"{cfg.coverage_target_pct:.0f}% — see "
            f"{cfg.output_dir}/coverage_report.json for top_gaps."
        )
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="lumi",
        description="LUMI — LookML Understanding and Metric Intelligence",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="DEBUG-level logs to stderr",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_plan = sub.add_parser(
        "plan", help="Phase 1: parse → discover → stage → plan",
    )
    p_plan.add_argument(
        "--input", default=None,
        help="Override gold_queries_dir (default: from LumiConfig)",
    )
    p_plan.add_argument(
        "--table", action="append",
        help="Plan for one table only; repeat for multiple",
    )
    p_plan.set_defaults(func=_cmd_plan)

    p_status = sub.add_parser("status", help="Show 7-stage progress")
    p_status.set_defaults(func=_cmd_status)

    p_approve = sub.add_parser(
        "approve",
        help="Show approval state for the queue (no auto-mutation; tick "
             "checkboxes manually in your editor)",
    )
    p_approve.add_argument("--queue", default="review_queue", help="Plan queue dir")
    p_approve.set_defaults(func=_cmd_approve)

    p_execute = sub.add_parser(
        "execute", help="Phase 2: enrich → validate → publish",
    )
    p_execute.add_argument(
        "--table", action="append",
        help="Execute for one table only; repeat for multiple",
    )
    p_execute.add_argument(
        "--dry-run", action="store_true",
        help="Use fixture EnrichedOutputs (no Vertex calls)",
    )
    p_execute.add_argument(
        "--force", action="store_true",
        help="Re-enrich even if data/enriched/<table>.json exists",
    )
    p_execute.add_argument(
        "--max-concurrent", type=int, default=None,
        help="Override LumiConfig.max_concurrent_enrichments",
    )
    p_execute.set_defaults(func=_cmd_execute)

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
