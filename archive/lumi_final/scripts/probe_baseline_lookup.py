#!/usr/bin/env python3
"""Verify which baseline .view.lkml file resolves to each discovered table.

Run when the baseline match rate is suspect — e.g. after seeing
``existing_view_lkml=None`` on tables you know exist in
``data/looker_master/``. The probe shows you, table by table, which file
got picked AND why (canonical filename vs prefix variant vs view-name
fuzzy match), so you can spot a naming-convention mismatch without
reading 30 files.

Usage:
    python scripts/probe_baseline_lookup.py
    python scripts/probe_baseline_lookup.py --baseline data/looker_master/
    python scripts/probe_baseline_lookup.py --queries data/gold_queries/
    python scripts/probe_baseline_lookup.py --table cornerstone_metrics

Exit codes:
    0  every discovered table resolved to a baseline file
    1  one or more tables had no baseline match (printed at the bottom)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumi.config import LumiConfig  # noqa: E402
from lumi.sql_to_context import (  # noqa: E402
    _find_baseline_view,
    parse_sqls,
)


def _trace_match(baseline_dir: Path, table_name: str) -> tuple[str | None, str]:
    """Return (matched_path_or_None, reason)."""
    if not baseline_dir.exists():
        return None, f"baseline_dir does not exist: {baseline_dir}"

    canonical = baseline_dir / f"{table_name}.view.lkml"
    if canonical.is_file():
        return str(canonical), "canonical filename at baseline_dir root"

    for path in baseline_dir.rglob(f"{table_name}.view.lkml"):
        return str(path), f"canonical filename in subdir {path.parent}"

    for prefix in ("bq_", "dw_", "edw_", "fact_", "dim_"):
        candidate = f"{prefix}{table_name}.view.lkml"
        direct = baseline_dir / candidate
        if direct.is_file():
            return str(direct), f"prefix-variant filename: {candidate}"
        for path in baseline_dir.rglob(candidate):
            return str(path), f"prefix-variant in subdir: {path}"

    # Fuzzy view-name scan (cheaper version of what _fuzzy_match_by_view_name does)
    needle = f"view: {table_name} ".encode()
    needle_brace = f"view: {table_name}{{".encode()
    for path in baseline_dir.rglob("*.view.lkml"):
        try:
            with path.open("rb") as f:
                head = f.read(256)
        except OSError:
            continue
        if needle in head or needle_brace in head:
            return str(path), f"view-name match in unrelated filename {path.name}"

    return None, "NO MATCH — no canonical, no prefix, no view-name match"


def main() -> int:
    p = argparse.ArgumentParser(prog="probe_baseline_lookup")
    p.add_argument("--baseline", help="Override LumiConfig.baseline_views_dir")
    p.add_argument("--queries", help="Override LumiConfig.gold_queries_dir")
    p.add_argument("--table", action="append", help="Probe one specific table; repeat to add more")
    args = p.parse_args()

    cfg = LumiConfig()
    baseline_dir = Path(args.baseline) if args.baseline else Path(cfg.baseline_views_dir)
    queries_dir = Path(args.queries) if args.queries else Path(cfg.gold_queries_dir)

    print(f"Baseline dir: {baseline_dir}  (exists={baseline_dir.exists()})")
    print(f"Queries dir:  {queries_dir}  (exists={queries_dir.exists()})")
    print()

    if args.table:
        targets = list(args.table)
        print(f"Probing {len(targets)} explicitly-requested table(s)\n")
    else:
        # Discover tables from the gold queries.
        sql_files = sorted(queries_dir.glob("*.sql")) if queries_dir.exists() else []
        if not sql_files:
            print(
                f"ERROR: no .sql files in {queries_dir}. "
                "Pass --queries or --table.",
                file=sys.stderr,
            )
            return 2
        sqls = [f.read_text(encoding="utf-8") for f in sql_files]
        fps = parse_sqls(sqls)
        seen: set[str] = set()
        targets = []
        for fp in fps:
            if fp.parse_error:
                continue
            for t in fp.tables:
                if t not in seen:
                    seen.add(t)
                    targets.append(t)
            for cte in fp.ctes:
                for src in cte.get("source_tables") or []:
                    if src not in seen:
                        seen.add(src)
                        targets.append(src)
            for tt in fp.temp_tables:
                for src in tt.get("source_tables") or []:
                    if src not in seen:
                        seen.add(src)
                        targets.append(src)
        targets.sort()
        print(f"Discovered {len(targets)} unique tables across {len(sql_files)} SQL files\n")

    # Probe each table.
    misses: list[str] = []
    print(f"{'TABLE':<48} {'MATCH?':<7} REASON")
    print("-" * 100)
    for table in targets:
        matched_path, reason = _trace_match(baseline_dir, table)
        # Sanity check: also call the production lookup so any divergence shows up.
        actual_text = _find_baseline_view(baseline_dir, table)
        flag = "✓" if matched_path else "✗"
        if matched_path is None:
            misses.append(table)
        print(f"{table[:47]:<48} {flag:<7} {reason}")
        if matched_path and actual_text is None:
            print("  ⚠  trace says match but production lookup returned None — bug!")

    print()
    if misses:
        print(f"✗ {len(misses)}/{len(targets)} tables had no baseline match:")
        for t in misses:
            print(f"    {t}")
        print()
        print("Next steps:")
        print(
            "  1. Confirm the .view.lkml files exist for those tables somewhere "
            f"under {baseline_dir}"
        )
        print(
            "  2. If they exist with a non-standard name, add the prefix to "
            "_find_baseline_view's prefix_variants tuple in lumi/sql_to_context.py"
        )
        print(
            "  3. If they don't exist, the table wasn't in the Looker repo "
            "you imported — pipeline will generate a fresh view from scratch."
        )
        return 1

    print(f"✓ All {len(targets)} tables resolved to a baseline file.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
