#!/usr/bin/env python3
"""Run Session 1 (Parse + Discover) end-to-end on real disk inputs.

Reads:
  data/gold_queries/Q*.sql          (from excel_to_queries.py)
  data/looker_master/**/*.view.lkml (from import_lookml_local.py)
  data/mdm_cache/*.json             (from probe_mdm.py)

Produces:
  data/session1_output.json         consolidated TableContext per table
  stdout summary                    per-table: queries, MDM coverage, baseline?

Pipeline-level guardrails are also reported (parse success, MDM coverage
distribution, CTE completeness).

Usage:
    python scripts/run_session1.py                # default paths from LumiConfig
    python scripts/run_session1.py --queries /path/to/sqls/
    python scripts/run_session1.py --json-out /tmp/s1.json
    python scripts/run_session1.py --quiet        # only print the summary table

Exit codes:
    0  success — all inputs present, pipeline ran
    1  pipeline ran but a guardrail flagged failures
    2  inputs missing (run probe_mdm / import_lookml_local first)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

# Make `lumi` importable when running from the repo root or scripts/.
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumi import guardrails  # noqa: E402
from lumi.config import LumiConfig  # noqa: E402
from lumi.mdm import CachedMDMClient  # noqa: E402
from lumi.sql_to_context import parse_sqls, prepare_enrichment_context  # noqa: E402


def _check_inputs(cfg: LumiConfig, queries_dir: Path) -> int:
    """Return 0 if all inputs are present; 2 + diagnostic message otherwise."""
    missing = []
    if not queries_dir.exists() or not list(queries_dir.glob("*.sql")):
        missing.append(
            f"  - {queries_dir}/Q*.sql\n"
            f"    Run:  python scripts/excel_to_queries.py /path/to/your.xlsx"
        )
    if not Path(cfg.baseline_views_dir).exists():
        missing.append(
            f"  - {cfg.baseline_views_dir}/\n"
            f"    Run:  python scripts/import_lookml_local.py /path/to/looker_repo"
        )
    if (
        not Path(cfg.mdm_cache_dir).exists()
        or not list(Path(cfg.mdm_cache_dir).glob("*.json"))
    ):
        missing.append(
            f"  - {cfg.mdm_cache_dir}/*.json\n"
            f"    Run:  python scripts/probe_mdm.py --save {cfg.mdm_cache_dir}/"
        )
    if missing:
        print("ERROR: missing inputs:", file=sys.stderr)
        for m in missing:
            print(m, file=sys.stderr)
        return 2
    return 0


def _print_summary(contexts: dict, mdm: CachedMDMClient, sqls: list[str]) -> None:
    """Pretty per-table summary the user can scan at a glance."""
    print()
    print("=" * 78)
    print(f"Session 1 — {len(sqls)} queries → {len(contexts)} unique tables")
    print("=" * 78)
    print()
    print(
        f"{'TABLE':<45} {'QUERIES':>8} {'MDM%':>6} {'BASE':>5} {'CTE':>4} {'JOIN':>5}"
    )
    print("-" * 78)
    by_query_count = sorted(
        contexts.items(), key=lambda x: (-len(x[1].queries_using_this), x[0])
    )
    for table_name, ctx in by_query_count:
        baseline = "✓" if ctx.existing_view_lkml else "—"
        print(
            f"{table_name[:44]:<45} "
            f"{len(ctx.queries_using_this):>8} "
            f"{ctx.mdm_coverage_pct * 100:>5.0f}% "
            f"{baseline:>5} "
            f"{len(ctx.ctes_referencing_this):>4} "
            f"{len(ctx.joins_involving_this):>5}"
        )
    print()
    if mdm.cache_misses:
        print(
            f"⚠  MDM cache miss for {len(mdm.cache_misses)} table(s): "
            f"{sorted(mdm.cache_misses)[:5]}"
            f"{' …' if len(mdm.cache_misses) > 5 else ''}"
        )
        print(
            "   Run:  python scripts/probe_mdm.py --save data/mdm_cache/  "
            "(or pass --table for individuals)"
        )
        print()


def _print_guardrail(gate) -> None:
    icon = {"pass": "✓", "warn": "⚠", "fail": "✗"}[gate.status]
    print(f"{icon} guardrail [{gate.stage}] — {gate.status.upper()}")
    for c in gate.checks:
        ci = "✓" if c["passed"] else "✗"
        print(f"   {ci} {c['name']:<28} {c.get('message', '')}")
    for w in gate.warnings:
        print(f"   ⚠  {w}")
    for b in gate.blocking_failures:
        print(f"   ✗ BLOCKING: {b}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_session1")
    parser.add_argument(
        "--queries",
        help="Directory of .sql files. Default: from LumiConfig (data/gold_queries)",
    )
    parser.add_argument(
        "--baseline",
        help="Looker baseline dir. Default: from LumiConfig (data/looker_master)",
    )
    parser.add_argument(
        "--mdm-cache",
        help="MDM cache dir. Default: from LumiConfig (data/mdm_cache)",
    )
    parser.add_argument(
        "--json-out",
        default="data/session1_output.json",
        help="Where to write the consolidated TableContext dict as JSON",
    )
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress per-table prints")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    cfg = LumiConfig()
    queries_dir = Path(args.queries) if args.queries else Path(cfg.gold_queries_dir)
    baseline_dir = Path(args.baseline) if args.baseline else Path(cfg.baseline_views_dir)
    mdm_cache_dir = (
        Path(args.mdm_cache) if args.mdm_cache else Path(cfg.mdm_cache_dir)
    )

    rc = _check_inputs(
        LumiConfig(
            gold_queries_dir=str(queries_dir),
            baseline_views_dir=str(baseline_dir),
            mdm_cache_dir=str(mdm_cache_dir),
        ),
        queries_dir,
    )
    if rc != 0:
        return rc

    sqls = [f.read_text(encoding="utf-8") for f in sorted(queries_dir.glob("*.sql"))]
    mdm = CachedMDMClient(mdm_cache_dir)

    # Stage 1: parse — kept separate so we can run the parse-success guardrail
    fps = parse_sqls(sqls)

    # Parse-stage breakdown (empty cells vs real errors vs success).
    empty_idx = [i + 1 for i, fp in enumerate(fps) if fp.parse_error == "empty_input"]
    real_err_idx = [
        i + 1 for i, fp in enumerate(fps)
        if fp.parse_error and fp.parse_error != "empty_input"
    ]
    parsed_n = len(fps) - len(empty_idx) - len(real_err_idx)
    print(
        f"Parse: {parsed_n}/{len(fps)} parsed  "
        f"({len(empty_idx)} empty cells, {len(real_err_idx)} real errors)"
    )
    if empty_idx:
        print(f"  empty cells:  Q{', Q'.join(f'{i:02d}' for i in empty_idx[:10])}"
              f"{' …' if len(empty_idx) > 10 else ''}")
    if real_err_idx:
        print(f"  real errors:  Q{', Q'.join(f'{i:02d}' for i in real_err_idx[:10])}"
              f"{' …' if len(real_err_idx) > 10 else ''}")

    # Stage 2: discover — full hydration with MDM + baselines
    contexts = prepare_enrichment_context(sqls, mdm, str(baseline_dir))

    if not args.quiet:
        _print_summary(contexts, mdm, sqls)

    # Guardrail report
    fp_dicts = [
        {
            "tables": fp.tables,
            "ctes": fp.ctes,
            "joins": fp.joins,
            "_parse_error": fp.parse_error,
        }
        for fp in fps
    ]
    gate = guardrails.check_parse_and_discover(sqls, fp_dicts, contexts)
    _print_guardrail(gate)

    # Dump the full output for downstream inspection / Session 2 consumption
    out_path = Path(args.json_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    serialised = {
        name: ctx.model_dump() if hasattr(ctx, "model_dump") else asdict(ctx)
        for name, ctx in contexts.items()
    }
    out_path.write_text(json.dumps(serialised, indent=2), encoding="utf-8")
    print(f"Wrote consolidated TableContexts → {out_path}")

    return 0 if gate.status != "fail" else 1


if __name__ == "__main__":
    raise SystemExit(main())
