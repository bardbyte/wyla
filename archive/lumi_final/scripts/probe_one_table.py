#!/usr/bin/env python3
"""End-to-end probe for ONE table — the iteration loop.

Runs Phase 1 → auto-tick approval → Phase 2 (dry_run by default) →
prints diff against baseline, all in one command. Use this to iterate
on prompts / grounding signals against a single table without touching
the other 28.

Usage:
    # Default: pick the highest-priority table from session1_output.json
    python scripts/probe_one_table.py --dry-run

    # Or specify the table
    python scripts/probe_one_table.py --table cornerstone_metrics --dry-run

    # Real Gemini, single table (the iteration mode for prompt tuning)
    python scripts/probe_one_table.py --table cornerstone_metrics

    # Print the full prompt that would be sent to Gemini (debug)
    python scripts/probe_one_table.py --table cornerstone_metrics \\
        --print-prompt --dry-run

Exit codes:
    0  pipeline ran end-to-end, output present
    1  per-table failure or coverage below target
    2  setup error (missing inputs, unparseable session1_output, etc.)
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumi.config import LumiConfig  # noqa: E402
from lumi.pipeline import (  # noqa: E402
    PipelineHaltError,
    run_execute_phase,
    run_plan_phase,
)


def _pick_top_priority_table() -> str | None:
    p = Path("data/session1_output.json")
    if not p.exists():
        return None
    data = json.loads(p.read_text(encoding="utf-8"))
    if not data:
        return None
    ranked = sorted(
        data.items(),
        key=lambda kv: -len((kv[1] or {}).get("queries_using_this") or []),
    )
    return ranked[0][0] if ranked else None


def _tick_approved(plan_md: Path) -> None:
    """Auto-tick the [ ] APPROVED checkbox in the plan markdown."""
    if not plan_md.exists():
        return
    body = plan_md.read_text(encoding="utf-8")
    # Match the unticked APPROVED line (with or without spaces / unicode).
    body = re.sub(
        r"^- \[ \] ✅ APPROVED",
        "- [x] ✅ APPROVED   (auto-ticked by probe_one_table.py)",
        body, count=1, flags=re.MULTILINE,
    )
    plan_md.write_text(body, encoding="utf-8")


def _diff_baseline_vs_output(table: str, cfg: LumiConfig) -> str:
    """Return a unified diff between baseline view and the published one.

    Empty string if either side is missing. Truncated to first 300 lines
    so console output stays scannable.
    """
    baseline_dir = Path(cfg.baseline_views_dir)
    output_view = Path(cfg.output_dir) / "views" / f"{table}.view.lkml"
    if not output_view.exists():
        return ""

    baseline_path: Path | None = None
    direct = baseline_dir / f"{table}.view.lkml"
    if direct.is_file():
        baseline_path = direct
    else:
        for p in baseline_dir.rglob(f"{table}.view.lkml"):
            baseline_path = p
            break

    if baseline_path is None:
        return f"(no baseline for {table} — published view is brand-new)"

    base_lines = baseline_path.read_text(encoding="utf-8").splitlines(keepends=True)
    out_lines = output_view.read_text(encoding="utf-8").splitlines(keepends=True)
    diff = list(difflib.unified_diff(
        base_lines, out_lines,
        fromfile=str(baseline_path),
        tofile=str(output_view),
        n=3,
    ))
    if not diff:
        return "(no diff — output identical to baseline)"
    return "".join(diff[:300])


def _print_prompt_for_table(table: str, cfg: LumiConfig) -> None:
    """Build and print the exact enrichment prompt that would go to Gemini.

    Useful when iterating on the prompt template or grounding signals —
    you see what context the LLM would receive without spending tokens.
    """
    from lumi.enrich import build_enrichment_prompt
    from lumi.grounding import build_grounding_signals
    from lumi.plan_builder import load_plan_json
    from lumi.schemas import TableContext
    from lumi.sql_to_context import parse_sqls

    s1 = json.loads(Path("data/session1_output.json").read_text(encoding="utf-8"))
    if table not in s1:
        print(f"ERROR: {table} not in session1_output.json", file=sys.stderr)
        return
    ctx = TableContext(**s1[table])
    plan = load_plan_json(Path("data/plans"), table)
    if plan is None:
        print(
            f"ERROR: no plan at data/plans/{table}.plan.json — "
            "run Phase 1 first.", file=sys.stderr,
        )
        return

    sqls = [
        f.read_text(encoding="utf-8")
        for f in sorted(Path(cfg.gold_queries_dir).glob("*.sql"))
    ]
    fps = parse_sqls(sqls)
    contexts_by_table = {n: TableContext(**d) for n, d in s1.items()}
    grounding = build_grounding_signals(ctx, fps, contexts_by_table)

    prompt = build_enrichment_prompt(ctx, plan, config=cfg, grounding=grounding)
    print(f"=== ENRICHMENT PROMPT FOR {table} — {len(prompt)} chars ===\n")
    print(prompt)
    print("\n=== END PROMPT ===\n")


def main() -> int:
    p = argparse.ArgumentParser(prog="probe_one_table")
    p.add_argument(
        "--table",
        help="Table to probe (default: highest-priority from session1_output.json)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Use fixture EnrichedOutputs (no Vertex tokens spent)",
    )
    p.add_argument(
        "--print-prompt",
        action="store_true",
        help="Print the full enrichment prompt instead of running enrichment",
    )
    p.add_argument(
        "--no-diff",
        action="store_true",
        help="Skip the baseline diff at the end",
    )
    args = p.parse_args()

    cfg = LumiConfig()

    table = args.table or _pick_top_priority_table()
    if not table:
        print(
            "ERROR: no table specified and data/session1_output.json missing. "
            "Run scripts/run_session1.py first.",
            file=sys.stderr,
        )
        return 2
    print(f"Probing single table: {table}")
    print(f"Mode: {'DRY-RUN' if args.dry_run else 'REAL GEMINI'}")
    print()

    if args.print_prompt:
        _print_prompt_for_table(table, cfg)
        return 0

    # Phase 1 — plan only this table.
    print("─" * 78)
    print("Phase 1 — building plan")
    print("─" * 78)
    try:
        plan_result = run_plan_phase(cfg, only_tables=[table])
    except PipelineHaltError as e:
        print(f"\nHALT (Phase 1): {e}", file=sys.stderr)
        return 2
    print(
        f"  plan files written: {plan_result.tables_succeeded}, "
        f"failures: {plan_result.tables_failed}, "
        f"elapsed: {plan_result.elapsed_s()}s"
    )
    if plan_result.failures:
        for f in plan_result.failures:
            print(f"  - [{f['stage']}] {f['table']}: {f['error']}")
        return 1

    # Auto-tick approval for our one table.
    plan_md = Path("review_queue") / f"{table}.plan.md"
    _tick_approved(plan_md)
    print(f"  ✓ auto-ticked APPROVED on {plan_md}")
    print()

    # Phase 2 — execute just this table.
    print("─" * 78)
    print("Phase 2 — enrich + validate + publish")
    print("─" * 78)
    try:
        exec_result = run_execute_phase(
            cfg,
            only_tables=[table],
            force=True,  # always re-enrich in probe mode
            dry_run=args.dry_run,
        )
    except PipelineHaltError as e:
        print(f"\nHALT (Phase 2): {e}", file=sys.stderr)
        return 2
    print(
        f"  enriched: {exec_result.tables_succeeded}, "
        f"failures: {exec_result.tables_failed}, "
        f"coverage: "
        f"{f'{exec_result.coverage_pct:.1f}%' if exec_result.coverage_pct is not None else 'n/a'}, "
        f"elapsed: {exec_result.elapsed_s()}s"
    )
    if exec_result.failures:
        print()
        for f in exec_result.failures:
            print(f"  ✗ [{f['stage']}] {f['table']}: {f['error']}")

    # Diff vs baseline so the user sees exactly what changed.
    if not args.no_diff:
        print()
        print("─" * 78)
        print(f"Diff: baseline vs output/views/{table}.view.lkml")
        print("─" * 78)
        diff_text = _diff_baseline_vs_output(table, cfg)
        if diff_text.strip():
            print(diff_text)
        else:
            print(f"  (no output view written for {table} — see failures above)")

    print()
    print("─" * 78)
    print("Inspect:")
    print(f"  cat output/views/{table}.view.lkml         # the merged view")
    print(f"  cat data/enriched/{table}.json             # the raw EnrichedOutput")
    print("  cat output/uncertain_fields.md             # what the LLM admitted it guessed")
    print("  cat output/proposed_overwrites.md          # what got auto-replaced from baseline")
    print("─" * 78)

    if exec_result.tables_failed:
        return 1
    if exec_result.coverage_pct is not None and exec_result.coverage_pct < cfg.coverage_target_pct:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
