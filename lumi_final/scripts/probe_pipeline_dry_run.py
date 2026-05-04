#!/usr/bin/env python3
"""Phase 1 dry-run — Parse + Discover + Stage + Plan with mocked LLM.

Reads gold queries from ``data/gold_queries/`` (or ``--input``), runs Stage 1
(Parse + Discover) for real (deterministic — no LLM), then synthesises a plan
per table using:

  - ``lumi.planner.compute_priority``                (deterministic)
  - ``lumi.planner.compute_deterministic_diff``      (deterministic)
  - ``lumi.planner.classify_risk``                   (deterministic)
  - ``lumi.planner.format_plan_markdown``            (deterministic)

The "LLM understanding / assessment" fields are taken from a fixture under
``tests/fixtures/llm_responses/`` when one exists for the table; otherwise a
sensible placeholder is used so the markdown is still readable.

Per-stage progress is printed plus the parse/discover guardrail. One
``review_queue/<table>.plan.md`` is written per table.

Usage:
    python scripts/probe_pipeline_dry_run.py
    python scripts/probe_pipeline_dry_run.py --input data/gold_queries/
    python scripts/probe_pipeline_dry_run.py --single-table cornerstone_metrics
    python scripts/probe_pipeline_dry_run.py --queue-dir review_queue/

Exit codes:
    0  all stages passed
    1  parse/discover guardrail flagged blocking failures
    2  inputs missing
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumi import guardrails  # noqa: E402
from lumi.config import LumiConfig  # noqa: E402
from lumi.mdm import CachedMDMClient  # noqa: E402
from lumi.planner import (  # noqa: E402
    PlannedChange,
    TablePlan,
    classify_risk,
    compute_deterministic_diff,
    compute_priority,
    format_plan_markdown,
)
from lumi.schemas import TableContext  # noqa: E402
from lumi.sql_to_context import parse_sqls, prepare_enrichment_context  # noqa: E402

logger = logging.getLogger("probe.pipeline_dry_run")

_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "llm_responses"


def _refuse_in_repo_sa_key() -> None:
    """Refuse to run if GOOGLE_APPLICATION_CREDENTIALS points inside the repo."""
    import os

    val = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not val:
        return
    p = Path(val).resolve()
    try:
        p.relative_to(REPO_ROOT.parent)
        print(
            f"ERROR: GOOGLE_APPLICATION_CREDENTIALS points inside the repo: {p}\n"
            "Move SA JSON to ~/Downloads or another path outside the repo.",
            file=sys.stderr,
        )
        sys.exit(2)
    except ValueError:
        return


def _fixture_understanding(table_name: str) -> tuple[str, str]:
    """Best-effort ``(llm_understanding, llm_existing_assessment)`` pair.

    If a fixture EnrichedOutput exists for the table the explore description
    is reused as the understanding. Otherwise return generic placeholders.
    """
    fx = _FIXTURE_DIR / f"enrich_{table_name}.json"
    if not fx.exists():
        return (
            f"(dry-run) Table `{table_name}` — LLM call mocked. Run "
            "`python -m lumi plan` for real Gemini reasoning.",
            "(dry-run) Existing baseline LookML assessment skipped — no "
            "fixture available for this table.",
        )
    try:
        raw = json.loads(fx.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return ("(dry-run) fixture present but unparseable", "(dry-run)")
    explore = raw.get("explore_lkml") or ""
    desc = ""
    for line in explore.splitlines():
        s = line.strip()
        if s.startswith("description:"):
            desc = s.removeprefix("description:").strip().strip('"').rstrip(";").strip()
            break
    if not desc:
        desc = f"Fixture-derived description for {table_name}."
    return (
        f"(fixture) {desc}",
        "(fixture) Baseline view appears reasonable; structural changes "
        "noted in plan diff below.",
    )


def _build_plan(ctx: TableContext, rank: int, total_ranks: int) -> TablePlan:
    """Synthesise a TablePlan from deterministic helpers + fixture text."""
    priority = compute_priority(ctx)
    diff = compute_deterministic_diff(ctx)
    has_struct, changes = classify_risk(diff, ctx)
    understanding, assessment = _fixture_understanding(ctx.table_name)

    auto_approved = (not has_struct) and all(c.risk == "low" for c in changes)

    return TablePlan(
        table_name=ctx.table_name,
        priority_score=priority,
        priority_rank=rank,
        query_count=len(ctx.queries_using_this),
        llm_understanding=understanding,
        llm_existing_assessment=assessment,
        existing_dimensions=diff["existing"]["dimensions"],
        existing_measures=diff["existing"]["measures"],
        existing_dim_groups=diff["existing"]["dim_groups"],
        has_primary_key=diff["existing"]["has_pk"],
        has_sql_table_name=diff["existing"]["has_sql_table_name"],
        mdm_coverage_pct=ctx.mdm_coverage_pct,
        new_measures_needed=diff["needed_measures"],
        new_derived_tables=diff["needed_derived_tables"],
        new_derived_dimensions=diff["needed_derived_dims"],
        description_upgrades_needed=diff["description_upgrades"],
        dimension_group_conversions=diff["needed_dim_groups"],
        filtered_measures_needed=diff["high_freq_filters"],
        changes=changes,
        has_structural_changes=has_struct,
        auto_approved=auto_approved,
        human_approved=None,
    )


def _approval_template(plan: TablePlan) -> str:
    """Append a ticked / unticked decision template to the plan markdown.

    Auto-approved plans are pre-ticked APPROVED with an ``auto_low_risk``
    marker so :func:`lumi.approval.collect_approvals` infers the source
    correctly. Structural plans are LEFT pending so the human must tick.
    """
    if plan.auto_approved:
        return (
            "\n\n## Decision\n\n"
            "- [x] ✅ APPROVED — auto_low_risk (description-only, additive measures)\n"
        )
    return (
        "\n\n## Decision\n\n"
        "- [ ] ✅ APPROVED\n"
        "- [ ] ❌ REJECTED\n\n"
        "**Feedback:**\n```\n(write feedback here if rejecting)\n```\n"
    )


def _print_progress(stage: str, detail: str) -> None:
    print(f"[{stage:<10}] {detail}")


def _print_guardrail(gate) -> None:
    icon = {"pass": "✓", "warn": "⚠", "fail": "✗"}[gate.status]
    print(f"\n{icon} guardrail [{gate.stage}] — {gate.status.upper()}")
    for c in gate.checks:
        ci = "✓" if c["passed"] else "✗"
        print(f"   {ci} {c['name']:<28} {c.get('message', '')}")
    for w in gate.warnings:
        print(f"   ⚠  {w}")
    for b in gate.blocking_failures:
        print(f"   ✗ BLOCKING: {b}")


def main() -> int:
    parser = argparse.ArgumentParser(prog="probe_pipeline_dry_run")
    parser.add_argument(
        "--input",
        default=None,
        help="Directory of .sql files (default: from LumiConfig.gold_queries_dir)",
    )
    parser.add_argument(
        "--queue-dir",
        default="review_queue/",
        help="Where to write <table>.plan.md files (default: review_queue/)",
    )
    parser.add_argument(
        "--single-table",
        default=None,
        help="Restrict planning to one table name (still parses all SQLs)",
    )
    parser.add_argument(
        "--mdm-cache",
        default=None,
        help="MDM cache dir override (default: from LumiConfig)",
    )
    parser.add_argument(
        "--baseline",
        default=None,
        help="Baseline LookML dir override (default: from LumiConfig)",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Skip per-table summary block",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    _refuse_in_repo_sa_key()

    cfg = LumiConfig()
    queries_dir = Path(args.input) if args.input else Path(cfg.gold_queries_dir)
    baseline_dir = Path(args.baseline) if args.baseline else Path(cfg.baseline_views_dir)
    mdm_cache_dir = Path(args.mdm_cache) if args.mdm_cache else Path(cfg.mdm_cache_dir)
    queue_dir = Path(args.queue_dir)

    if not queries_dir.exists() or not list(queries_dir.glob("*.sql")):
        print(
            f"ERROR: no .sql files in {queries_dir}\n"
            f"Run:  python scripts/excel_to_queries.py /path/to/your.xlsx",
            file=sys.stderr,
        )
        return 2

    sqls = [f.read_text(encoding="utf-8") for f in sorted(queries_dir.glob("*.sql"))]
    _print_progress("Input", f"{len(sqls)} SQL file(s) in {queries_dir}")

    # Stage 1: Parse
    fps = parse_sqls(sqls)
    parse_errors = sum(1 for fp in fps if fp.parse_error and fp.parse_error != "empty_input")
    parse_empty = sum(1 for fp in fps if fp.parse_error == "empty_input")
    _print_progress(
        "Parse",
        f"{len(fps) - parse_errors - parse_empty}/{len(fps)} parsed "
        f"({parse_empty} empty, {parse_errors} errors)",
    )

    # Stage 2: Discover (MDM + baseline hydration)
    mdm = CachedMDMClient(mdm_cache_dir)
    contexts = prepare_enrichment_context(sqls, mdm, str(baseline_dir))
    _print_progress(
        "Discover",
        f"{len(contexts)} unique tables; "
        f"{len(mdm.cache_misses) if hasattr(mdm, 'cache_misses') else 0} MDM cache miss(es)",
    )

    # Parse/discover guardrail
    fp_dicts = [
        {"tables": fp.tables, "ctes": fp.ctes, "joins": fp.joins, "_parse_error": fp.parse_error}
        for fp in fps
    ]
    gate = guardrails.check_parse_and_discover(sqls, fp_dicts, contexts)
    _print_guardrail(gate)

    # Stage 3: Stage (priority ordering — by deterministic priority score desc)
    if args.single_table:
        if args.single_table not in contexts:
            print(
                f"ERROR: --single-table {args.single_table!r} not found in "
                f"discovered tables: {sorted(contexts)[:10]}",
                file=sys.stderr,
            )
            return 2
        contexts = {args.single_table: contexts[args.single_table]}

    ranked = sorted(
        contexts.values(),
        key=lambda c: (-compute_priority(c), c.table_name),
    )
    _print_progress("Stage", f"ranked {len(ranked)} table(s) by priority")

    # Stage 4: Plan — write one .plan.md per table
    queue_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    auto_count = 0
    for rank, ctx in enumerate(ranked, start=1):
        plan = _build_plan(ctx, rank=rank, total_ranks=len(ranked))
        if plan.auto_approved:
            auto_count += 1
        body = format_plan_markdown(plan) + _approval_template(plan)
        out = queue_dir / f"{ctx.table_name}.plan.md"
        out.write_text(body, encoding="utf-8")
        written.append(out)
    _print_progress(
        "Plan",
        f"wrote {len(written)} plan(s) to {queue_dir} ({auto_count} auto-approved)",
    )

    if not args.quiet:
        print("\nPlans written:")
        for p in written:
            print(f"  - {p}")

    if gate.status == "fail":
        return 1
    return 0


# Suppress unused-import warning in editors when PlannedChange/TablePlan are
# only used through type-only paths in this module.
_ = (PlannedChange, TablePlan)


if __name__ == "__main__":
    raise SystemExit(main())
