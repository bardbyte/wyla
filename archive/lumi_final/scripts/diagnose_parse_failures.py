#!/usr/bin/env python3
"""Show the actual SQL + sqlglot diagnosis for every Q*.sql that fails to parse.

Run after `python scripts/run_session1.py` reports parse failures. For each
failing file:
  - Print first/last 200 chars of the SQL (so you can see what's there)
  - Show sqlglot's error
  - Apply common-fix heuristics and report which would help
  - Optionally write a CLEANED candidate to data/gold_queries_cleaned/

Usage:
    python scripts/diagnose_parse_failures.py                    # diagnose all
    python scripts/diagnose_parse_failures.py --files Q22 Q63    # specific ones
    python scripts/diagnose_parse_failures.py --write-cleaned    # save fixes

Common patterns this finds:
  1. Trailing junk (text after the SQL)            → trim to last `;`
  2. Multiple statements joined by `;`             → keep first SELECT/WITH
  3. Stray quote (Q63's TokenError)                → flag the position
  4. SET / DECLARE / BEGIN scripts                 → BigQuery scripting (not
                                                     supported by sqlglot)
  5. Smart quotes / non-breaking spaces            → normalise
  6. Concatenated cells (no separator)             → flag, can't auto-fix
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import sqlglot  # noqa: E402

DEFAULT_QUERIES_DIR = REPO_ROOT / "data" / "gold_queries"
BQ_DIALECT = "bigquery"


# ─── Heuristic cleaners (each returns (cleaned_sql, applied: bool)) ─────────


def _try_trim_semicolons(sql: str) -> tuple[str, bool]:
    s = sql.strip()
    orig = s
    while s.endswith(";"):
        s = s[:-1].rstrip()
    return s, s != orig


def _try_first_statement(sql: str) -> tuple[str, bool]:
    """Pick the first SELECT/WITH from a multi-statement string."""
    try:
        statements = sqlglot.parse(sql, dialect=BQ_DIALECT)
    except Exception:
        return sql, False
    for stmt in statements:
        if stmt is None:
            continue
        if isinstance(stmt, sqlglot.exp.Select | sqlglot.exp.With | sqlglot.exp.Subquery):
            new_sql = stmt.sql(dialect=BQ_DIALECT)
            if new_sql.strip() != sql.strip():
                return new_sql, True
    return sql, False


def _try_normalize_smart_quotes(sql: str) -> tuple[str, bool]:
    smart = {"‘": "'", "’": "'", "“": '"', "”": '"', "\xa0": " "}
    cleaned = sql
    for bad, good in smart.items():
        cleaned = cleaned.replace(bad, good)
    return cleaned, cleaned != sql


def _try_strip_bq_scripting(sql: str) -> tuple[str, bool]:
    """BigQuery DECLARE/SET/BEGIN scripting blocks aren't queries — drop them."""
    pattern = re.compile(
        r"^\s*(DECLARE|SET|BEGIN|END|CALL)\s+", re.IGNORECASE | re.MULTILINE
    )
    if not pattern.search(sql):
        return sql, False
    # Take everything from the first SELECT/WITH onward.
    m = re.search(r"\b(SELECT|WITH)\b", sql, re.IGNORECASE)
    if m:
        return sql[m.start():], True
    return sql, False


# Order matters: cheapest, safest first. Stop at first heuristic that makes
# the SQL parse.
HEURISTICS = [
    ("trim_trailing_semicolons", _try_trim_semicolons),
    ("normalize_smart_quotes", _try_normalize_smart_quotes),
    ("first_statement", _try_first_statement),
    ("strip_bq_scripting", _try_strip_bq_scripting),
]


def _parses(sql: str) -> tuple[bool, str | None]:
    try:
        sqlglot.parse_one(sql, dialect=BQ_DIALECT)
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def diagnose_one(path: Path, write_cleaned: bool, cleaned_dir: Path) -> bool:
    """Print diagnosis. Return True if we found a fix that parses."""
    raw = path.read_text(encoding="utf-8")
    print(f"\n{'═' * 78}\n{path.name} — {len(raw)} chars")
    print("═" * 78)

    parses, err = _parses(raw)
    if parses:
        print("(actually parses cleanly now — re-run run_session1.py)")
        return True

    print(f"original error: {err}")
    print()
    print(f"first 200 chars:\n{raw[:200]!r}")
    print()
    print(f"last  200 chars:\n{raw[-200:]!r}")
    print()

    # Try heuristics one at a time.
    cleaned = raw
    fixed_by: list[str] = []
    for name, fn in HEURISTICS:
        candidate, applied = fn(cleaned)
        if not applied:
            continue
        ok, _ = _parses(candidate)
        if ok:
            cleaned = candidate
            fixed_by.append(name)
            break
        # heuristic ran but didn't fix on its own; keep applying in chain.
        cleaned = candidate
        fixed_by.append(name)

    final_ok, final_err = _parses(cleaned)
    print(f"applied heuristics: {fixed_by or '(none useful)'}")
    if final_ok:
        print("RESULT: cleaned SQL parses cleanly")
        if write_cleaned:
            cleaned_dir.mkdir(parents=True, exist_ok=True)
            target = cleaned_dir / path.name
            target.write_text(cleaned + "\n", encoding="utf-8")
            print(f"  wrote → {target}")
    else:
        print(f"RESULT: still fails — {final_err}")
        print("  → human inspection needed; common causes:")
        print("    - multiple SQL queries concatenated with no separator")
        print("    - non-SQL prose mixed into the cell")
        print("    - sqlglot doesn't yet understand a BigQuery feature in this query")
    return final_ok


def main() -> int:
    parser = argparse.ArgumentParser(prog="diagnose_parse_failures")
    parser.add_argument(
        "--queries",
        default=str(DEFAULT_QUERIES_DIR),
        help=f"Directory of Q*.sql files. Default: {DEFAULT_QUERIES_DIR}",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        help="Specific basenames to diagnose, e.g. Q22 Q63 (with or without .sql)",
    )
    parser.add_argument(
        "--write-cleaned",
        action="store_true",
        help="Write cleaned candidates to data/gold_queries_cleaned/",
    )
    args = parser.parse_args()

    queries_dir = Path(args.queries)
    if not queries_dir.exists():
        print(f"ERROR: {queries_dir} doesn't exist", file=sys.stderr)
        return 2

    if args.files:
        targets = []
        for name in args.files:
            if not name.endswith(".sql"):
                name = f"{name}.sql"
            p = queries_dir / name
            if not p.exists():
                print(f"WARN: not found: {p}", file=sys.stderr)
                continue
            targets.append(p)
    else:
        # Run all Q*.sql, only showing diagnosis for the failures.
        targets = sorted(queries_dir.glob("Q*.sql"))

    if not targets:
        print("No SQL files to diagnose.", file=sys.stderr)
        return 1

    cleaned_dir = REPO_ROOT / "data" / "gold_queries_cleaned"

    fixed_count = 0
    failure_count = 0
    inspected_count = 0
    for path in targets:
        # Only print diagnosis for files that actually fail.
        raw = path.read_text(encoding="utf-8")
        ok, _ = _parses(raw)
        if ok and not args.files:
            continue
        inspected_count += 1
        was_fixed = diagnose_one(path, args.write_cleaned, cleaned_dir)
        if was_fixed:
            fixed_count += 1
        else:
            failure_count += 1

    print(
        f"\n{'═' * 78}\nSummary: inspected {inspected_count}, "
        f"auto-fixable {fixed_count}, still-failing {failure_count}"
    )
    if args.write_cleaned and fixed_count:
        print(
            f"\nCleaned candidates in: {cleaned_dir}/\n"
            f"Review them, then move them over the originals:\n"
            f"  cp {cleaned_dir}/*.sql {queries_dir}/"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
