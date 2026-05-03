"""LUMI CLI — 4 subcommands matching the 7-stage flow.

Phase 1 (cheap, automated):
  python -m lumi plan --input data/gold_queries/
    → Stages 1-4: Parse → Discover → Stage → Plan
    → writes review_queue/<table>.plan.md per table
    → user reviews and appends "✅ APPROVED" / "❌ REJECTED — <feedback>"

  python -m lumi status
    → prints the 7-stage progress table (matches the sketch in the design)
    → ✓ Parse  ✓ Discover  ✓ Stage  ● Plan: 3/6  ○ Enrich  ○ Validate  ○ Publish

  python -m lumi approve <table>
    → validates the appended approval block in <table>.plan.md
    → writes <table>.approval.json

Phase 2 (expensive, automated):
  python -m lumi execute
    → Stages 5-7: Enrich → Validate → Publish
    → only runs for tables with approved plans
    → opens GitHub PR at the end (unless --dry-run)
"""

from __future__ import annotations

import argparse
import sys


def _cmd_plan(args: argparse.Namespace) -> int:
    """Run Phase 1 (Parse → Discover → Stage → Plan)."""
    from lumi.config import LumiConfig
    from lumi.pipeline import LumiPipeline

    cfg = LumiConfig()
    pipeline = LumiPipeline(cfg)
    sql_inputs = _load_sql_files(args.input)
    if not sql_inputs:
        print(f"ERROR: no .sql files found in {args.input}", file=sys.stderr)
        return 2
    print(f"Phase 1: planning from {len(sql_inputs)} SQLs in {args.input}")
    pipeline.run_plan_phase(sql_inputs)  # writes review_queue/
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    """Print 7-stage progress for the current run."""
    from lumi.pipeline import LumiPipeline
    from lumi.config import LumiConfig

    pipeline = LumiPipeline(LumiConfig())
    pipeline.print_status()  # ✓/●/○ indicators per stage
    return 0


def _cmd_approve(args: argparse.Namespace) -> int:
    """Parse <table>.plan.md's approval block and write <table>.approval.json."""
    from lumi.approval import write_approval_json

    written = write_approval_json(args.table, args.queue)
    print(f"Wrote approval: {written}")
    return 0


def _cmd_execute(args: argparse.Namespace) -> int:
    """Run Phase 2 (Enrich → Validate → Publish) for approved plans only."""
    from lumi.config import LumiConfig
    from lumi.pipeline import LumiPipeline

    pipeline = LumiPipeline(LumiConfig())
    print("Phase 2: executing approved plans...")
    pipeline.run_execute_phase(dry_run=args.dry_run)
    return 0


def _load_sql_files(input_path: str) -> list[str]:
    """Load every .sql file under input_path (or the single file)."""
    from pathlib import Path

    p = Path(input_path)
    if p.is_file():
        return [p.read_text(encoding="utf-8")]
    return [f.read_text(encoding="utf-8") for f in sorted(p.glob("*.sql"))]


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="lumi",
        description="LUMI — LookML Understanding and Metric Intelligence",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_plan = sub.add_parser("plan", help="Phase 1: parse → discover → stage → plan")
    p_plan.add_argument("--input", default="data/gold_queries/", help="SQL dir or file")
    p_plan.set_defaults(func=_cmd_plan)

    p_status = sub.add_parser("status", help="Show 7-stage progress")
    p_status.set_defaults(func=_cmd_status)

    p_approve = sub.add_parser("approve", help="Parse approval block for one table")
    p_approve.add_argument("table", help="Table name (matches review_queue/<table>.plan.md)")
    p_approve.add_argument("--queue", default="review_queue/", help="Plan queue dir")
    p_approve.set_defaults(func=_cmd_approve)

    p_execute = sub.add_parser("execute", help="Phase 2: enrich → validate → publish")
    p_execute.add_argument("--dry-run", action="store_true", help="Skip git push")
    p_execute.set_defaults(func=_cmd_execute)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
