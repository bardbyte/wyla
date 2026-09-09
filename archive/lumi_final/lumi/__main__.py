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
    """Phase 1: Parse → Discover → Stage → Plan."""
    from lumi.config import LumiConfig
    from lumi.pipeline import PipelineHaltError, run_plan_phase

    cfg = LumiConfig()
    if args.input:
        cfg.gold_queries_dir = args.input

    mode = "Gemini-authored" if args.with_llm else "deterministic"
    print(f"Phase 1 ({mode}): planning from {cfg.gold_queries_dir}")
    only = args.table or None
    try:
        result = run_plan_phase(
            cfg, only_tables=only, with_llm=args.with_llm,
        )
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


def _cmd_ontology(args: argparse.Namespace) -> int:
    """Build (or refresh) the system-level domain ontology.

    Reads every TableContext + every fingerprint and emits
    data/ontology.json. One Gemini call when --refresh is passed or
    the file is missing.
    """
    import json as _json
    from pathlib import Path

    from lumi.config import LumiConfig
    from lumi.mdm import CachedMDMClient
    from lumi.ontology_builder import ensure_ontology
    from lumi.schemas import TableContext
    from lumi.sql_to_context import discover_tables, parse_sqls

    cfg = LumiConfig()
    queries_dir = Path(cfg.gold_queries_dir)
    if not queries_dir.exists():
        print(f"ERROR: {queries_dir} missing — run probe_mdm first", file=sys.stderr)
        return 2

    # Try to use cached session1_output.json; otherwise rebuild.
    s1_path = Path("data/session1_output.json")
    if s1_path.exists():
        raw = _json.loads(s1_path.read_text(encoding="utf-8"))
        contexts = {n: TableContext(**d) for n, d in raw.items()}
        sqls = [
            f.read_text(encoding="utf-8")
            for f in sorted(queries_dir.glob("*.sql"))
        ]
        fps = parse_sqls(sqls)
    else:
        sqls = [
            f.read_text(encoding="utf-8")
            for f in sorted(queries_dir.glob("*.sql"))
        ]
        fps = parse_sqls(sqls)
        mdm = CachedMDMClient(Path(cfg.mdm_cache_dir))
        contexts = discover_tables(fps, mdm, cfg.baseline_views_dir)

    print(
        f"Building domain ontology from {len(contexts)} tables × "
        f"{len(fps)} fingerprints — one Gemini call"
        f"{' (forced refresh)' if args.refresh else ''}…",
    )
    ontology = ensure_ontology(
        contexts, fps,
        path=Path(args.path),
        refresh=args.refresh,
        with_llm=not args.deterministic_only,
        config=cfg,
    )
    print()
    print("=" * 78)
    print(f"  Ontology — {len(ontology.entities)} entities, "
          f"{len(ontology.relationships)} relationships")
    print(f"  Authoring: {ontology.authoring.get('mode', '?')}")
    print("=" * 78)
    for ent in ontology.entities:
        syn = f" (a.k.a. {', '.join(ent.synonyms[:5])})" if ent.synonyms else ""
        n_tables = len(ent.grain_columns)
        n_cols = sum(len(v) for v in ent.grain_columns.values())
        print(f"  - {ent.name}{syn}: {n_tables} tables, {n_cols} columns")
    if ontology.relationships:
        print()
        for rel in ontology.relationships[:10]:
            print(
                f"  - {rel.from_entity} → {rel.to_entity} ({rel.cardinality})"
            )
    print()
    print(f"Saved to {args.path}")
    return 0


def _cmd_review(args: argparse.Namespace) -> int:
    """Walk plans interactively in the terminal — no editor / web UI needed.

    For each plan in priority order, shows the markdown + critique, then
    prompts for one keystroke:
      a — approve
      r — regenerate (re-runs plan stage with --with-llm for this table)
      e — open in $EDITOR (vi by default), then re-prompt
      f — feedback (capture rejection reason; marks REJECTED)
      s — skip
      q — quit

    Approve/reject decisions are written into the plan markdown (ticking
    the `[ ] APPROVED` / `[ ] REJECTED` checkboxes) so collect_approvals
    picks them up for `python -m lumi execute`.
    """
    import os
    import re
    from pathlib import Path

    queue_dir = Path(args.queue)
    plans = sorted(queue_dir.glob("*.plan.md"))
    if not plans:
        print(f"No plans in {queue_dir}/. Run `python -m lumi plan` first.")
        return 1

    print(f"Reviewing {len(plans)} plan(s) in {queue_dir}/")
    print("Keys: [a]pprove · [r]egenerate · [e]dit · [f]eedback · [s]kip · [q]uit\n")

    approve_count = reject_count = skip_count = 0

    for i, plan_path in enumerate(plans, start=1):
        table_name = plan_path.stem.replace(".plan", "")
        if args.table and table_name not in args.table:
            continue
        text = plan_path.read_text(encoding="utf-8")
        print("=" * 80)
        print(f"[{i}/{len(plans)}] {table_name}")
        print("=" * 80)
        print(text)
        print()
        while True:
            try:
                choice = input(
                    f"{table_name} → [a/r/e/f/s/q]: "
                ).strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("\nQuitting.")
                return 0
            if choice in {"a", "approve"}:
                new_text = _tick_checkbox(text, approved=True)
                plan_path.write_text(new_text, encoding="utf-8")
                approve_count += 1
                break
            if choice in {"r", "regen", "regenerate"}:
                print(
                    f"Re-running plan stage for {table_name} with --with-llm…"
                )
                from lumi.config import LumiConfig
                from lumi.pipeline import run_plan_phase
                run_plan_phase(
                    LumiConfig(), only_tables=[table_name], with_llm=True,
                )
                text = plan_path.read_text(encoding="utf-8")
                print("\n--- regenerated ---\n")
                print(text)
                continue
            if choice in {"e", "edit"}:
                editor = os.environ.get("EDITOR", "vi")
                os.system(f"{editor} {plan_path}")
                text = plan_path.read_text(encoding="utf-8")
                continue
            if choice in {"f", "feedback"}:
                fb = input("Feedback (one line, will be saved as REJECTED): ").strip()
                new_text = _tick_checkbox(text, approved=False, feedback=fb)
                plan_path.write_text(new_text, encoding="utf-8")
                reject_count += 1
                break
            if choice in {"s", "skip"}:
                skip_count += 1
                break
            if choice in {"q", "quit"}:
                print("\nQuitting early.")
                _ = re  # used below
                _print_review_summary(approve_count, reject_count, skip_count)
                return 0
            print("Invalid key. Use a/r/e/f/s/q.")

    _print_review_summary(approve_count, reject_count, skip_count)
    return 0


def _tick_checkbox(text: str, *, approved: bool, feedback: str = "") -> str:
    """Tick the APPROVED or REJECTED checkbox in a plan markdown."""
    if approved:
        text = text.replace("- [ ] ✅ APPROVED", "- [x] ✅ APPROVED")
        text = text.replace("- [x] ❌ REJECTED", "- [ ] ❌ REJECTED")
    else:
        text = text.replace("- [ ] ❌ REJECTED", "- [x] ❌ REJECTED")
        text = text.replace("- [x] ✅ APPROVED", "- [ ] ✅ APPROVED")
        if feedback:
            if "Feedback (required if rejected):" in text:
                text = text.replace(
                    "Feedback (required if rejected):",
                    f"Feedback (required if rejected):\n\n{feedback}",
                )
            else:
                text = text + f"\n\nFeedback: {feedback}\n"
    return text


def _print_review_summary(approved: int, rejected: int, skipped: int) -> None:
    print()
    print("=" * 80)
    print(
        f"Review summary: {approved} approved, {rejected} rejected, "
        f"{skipped} skipped"
    )
    if approved:
        print("\nNext: `python -m lumi execute`")


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
    p_plan.add_argument(
        "--with-llm", action="store_true",
        help=(
            "Author each plan with Gemini using full TableContext + "
            "grounding + narrative. When omitted, plans are deterministic "
            "skeletons. Either way, plans always succeed — Gemini errors "
            "fall back gracefully to skeletons per table."
        ),
    )
    p_plan.set_defaults(func=_cmd_plan)

    p_status = sub.add_parser("status", help="Show 7-stage progress")
    p_status.set_defaults(func=_cmd_status)

    p_ont = sub.add_parser(
        "ontology",
        help="Build (or refresh) the domain ontology — one Gemini call",
    )
    p_ont.add_argument(
        "--path", default="data/ontology.json",
        help="Where to save the ontology JSON (default: data/ontology.json)",
    )
    p_ont.add_argument(
        "--refresh", action="store_true",
        help="Force rebuild even if data/ontology.json exists",
    )
    p_ont.add_argument(
        "--deterministic-only", action="store_true",
        help="Skip the LLM call; output a deterministic-only ontology",
    )
    p_ont.set_defaults(func=_cmd_ontology)

    p_approve = sub.add_parser(
        "approve",
        help="Show approval state for the queue (no auto-mutation; tick "
             "checkboxes manually in your editor)",
    )
    p_approve.add_argument("--queue", default="review_queue", help="Plan queue dir")
    p_approve.set_defaults(func=_cmd_approve)

    p_review = sub.add_parser(
        "review",
        help="Walk plans interactively in the terminal — approve/regen/edit/skip",
    )
    p_review.add_argument("--queue", default="review_queue", help="Plan queue dir")
    p_review.add_argument(
        "--table", action="append",
        help="Review one table only; repeat for multiple",
    )
    p_review.set_defaults(func=_cmd_review)

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
