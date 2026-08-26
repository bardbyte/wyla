#!/usr/bin/env python3
"""Repair run-blocking defects in the semantic-source files — the human
stays in the loop wherever semantics could change.

GOLD  (extracted_gold_queries.json): markdown code fences are transport
artifacts of the chat-log extraction. Stripped automatically — edges
only, and the tail rule needs 2+ backticks so the single closing
backtick of a quoted identifier is never touched. Every row is
re-parsed afterward; a backup lands beside the file.

DMP   (metrics_dmp.json): a certified metric whose expression does not
parse is shown line-numbered together with parse-verified MECHANICAL
repair candidates (a stray ``)`` / a ``) AS name`` tail). Nothing is
written without an explicit choice: if the numerator was lost upstream,
a candidate that parses is still semantically wrong — a bare
denominator masquerading as an average. Hand-edit the real formula
(e.g. ``SAFE_DIVIDE(<numerator>, COUNT(DISTINCT ...))``) and flag the
defect to the catalog owner.

Usage:
    python scripts/repair_sources.py <sources-dir> [--dmp-metric 6a4d2fbb]

Exit 0 = everything parses · 1 = something still needs a human."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

import sqlglot


def _parses(sql: str) -> bool:
    try:
        sqlglot.parse_one(sql, read="bigquery")
        return True
    except Exception:               # every flavor of "won't parse"
        return False


def _defence(sql: str) -> str:
    """Strip markdown fences at the EDGES only. Trailing rule needs 2+
    backticks: one trailing backtick may close a quoted identifier."""
    fixed = re.sub(r"^\s*```[a-zA-Z]*\s*", "", sql)
    return re.sub(r"\s*`{2,}\s*$", "", fixed)


def _dmp_candidates(expr: str) -> list[str]:
    """Parse-verified mechanical repairs, most-content-preserving first:
    drop the stray ``)`` keeping the alias, then drop the whole
    ``) AS name`` tail. An expression that already parses (or matches
    neither shape) yields no candidates."""
    cands: list[str] = []
    v = re.sub(r"\)\s*(AS\s+[A-Za-z_]\w*)\s*$", r" \1", expr)
    if v != expr and _parses(v):
        cands.append(v)
    v = re.sub(r"\)\s*AS\s+[A-Za-z_]\w*\s*$", "", expr).strip()
    if v and v != expr and v not in cands and _parses(v):
        cands.append(v)
    return cands


def _repair_gold(src: Path) -> bool:
    path = src / "extracted_gold_queries.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload if isinstance(payload, list) else \
        payload.get("queries", [])
    changed = []
    for row in rows:
        sql = row.get("sql") or ""
        fixed = _defence(sql)
        if fixed != sql:
            row["sql"] = fixed
            changed.append(row.get("id"))
    if changed:
        shutil.copy2(path, path.with_suffix(".json.bak"))
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8")
    print("GOLD de-fenced rows:", changed or "none (already clean)")
    bad = [str(r.get("id")) for r in rows
           if (r.get("sql") or "").strip()
           and not _parses(r["sql"].strip())]
    print("GOLD parse sweep:",
          "all clean" if not bad else f"STILL failing: {bad}")
    return not bad


def _repair_dmp(src: Path, metric_prefix: str) -> bool:
    path = src / "metrics_dmp.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = (payload.get("metric_catalog", [])
            if isinstance(payload, dict) else payload)
    row = next((r for r in rows
                if str(r.get("metricCatalogId", ""))
                .startswith(metric_prefix)), None)
    if row is None:
        print(f"DMP: no metric id starting {metric_prefix!r}")
        return False
    field = ("sqlExpression"
             if str(row.get("sqlExpression") or "").strip()
             else "referencedSqlQuery")
    stored = str(row.get(field) or "").strip()
    print(f"\nDMP {row.get('metricCatalogId')} · {field} as stored:")
    for i, line in enumerate(stored.splitlines(), 1):
        print(f"  {i}: {line}")
    if _parses(stored):
        print("parses OK — nothing to repair")
        return True
    cands = _dmp_candidates(stored)
    if cands:
        print("\nmechanical repair candidates (parse-verified):")
        for i, c in enumerate(cands, 1):
            print(f"  [{i}] {c!r}")
        print("  [0] skip — I'll hand-edit")
    else:
        print("no mechanical repair parses — hand-edit needed")
    print("CAREFUL: if the numerator is missing from the stored text, a\n"
          "candidate that parses is still semantically wrong — hand-edit\n"
          "the real formula instead, e.g.\n"
          "  SAFE_DIVIDE(<numerator>, COUNT(DISTINCT enrolling_card))\n"
          "and flag the defect to the catalog owner.")
    if not cands:
        return False
    if not sys.stdin.isatty():
        print("non-interactive run: not writing — re-run in a terminal "
              "or hand-edit")
        return False
    pick = input("apply which? [0/1/..]: ").strip()
    if pick.isdigit() and 0 < int(pick) <= len(cands):
        shutil.copy2(path, path.with_suffix(".json.bak"))
        row[field] = cands[int(pick) - 1]
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8")
        print("written — backup:", path.with_suffix(".json.bak").name)
        return True
    print("skipped — edit", path.name, "by hand, then re-run census")
    return False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("sources", type=Path,
                    help="the semantic-sources directory")
    ap.add_argument("--dmp-metric", default="6a4d2fbb",
                    help="metricCatalogId prefix to inspect/repair")
    args = ap.parse_args(argv)
    ok = _repair_gold(args.sources)
    ok = _repair_dmp(args.sources, args.dmp_metric) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
