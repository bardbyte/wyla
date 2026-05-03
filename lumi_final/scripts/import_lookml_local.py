#!/usr/bin/env python3
"""Import LookML from a locally-downloaded repo into data/looker_master/.

Same shape as fetch_lookml_master.py, but reads from the local filesystem
instead of the GHE API. No token, no branch, no rate limits — just a
recursive walk + classify + copy.

Use this when you already have the Looker repo on disk (e.g., you cloned
it with `git clone`, or downloaded a release zip and extracted it).

Usage:

    # Default: copies ALL LookML (view, model, explore, dashboard, manifest)
    python scripts/import_lookml_local.py /path/to/your/looker_repo

    # Just views + models (skip dashboards)
    python scripts/import_lookml_local.py /path/to/your/looker_repo --types view,model

    # Scoped subset
    python scripts/import_lookml_local.py /path/to/your/looker_repo --include 'risk_*'

    # Dry-run — show what would be copied
    python scripts/import_lookml_local.py /path/to/your/looker_repo --list

    # Different destination
    python scripts/import_lookml_local.py /path/to/your/looker_repo --out data/baseline_views/

Reuses classify_type() from fetch_lookml_master.py — same conventions
(*.view.lkml, *.model.lkml, *.explore.lkml, *.dashboard.{lookml,lkml},
manifest.lkml, generic .lkml).
"""

from __future__ import annotations

import argparse
import fnmatch
import shutil
import sys
from collections import Counter
from pathlib import Path

# Reuse the classifier from the GHE-fetcher.
HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from fetch_lookml_master import LOOKML_TYPES, classify_type  # noqa: E402

DEFAULT_OUT_DIR = "data/looker_master"


def walk_local_repo(src: Path) -> list[tuple[Path, str]]:
    """Walk `src`, return [(absolute_path, lookml_type_name)] for every match.

    Skips common ignore-dirs (.git, node_modules, __pycache__, .vscode, etc.)
    """
    SKIP_DIR_NAMES = {
        ".git", ".github", "node_modules", "__pycache__", ".venv", "venv",
        ".idea", ".vscode", "build", "dist", ".mypy_cache", ".pytest_cache",
        ".ruff_cache",
    }
    out: list[tuple[Path, str]] = []
    for p in src.rglob("*"):
        if not p.is_file():
            continue
        # Skip if any path component is in the ignore list
        if any(part in SKIP_DIR_NAMES for part in p.relative_to(src).parts[:-1]):
            continue
        rel = str(p.relative_to(src)).replace("\\", "/")
        type_name = classify_type(rel)
        if type_name is not None:
            out.append((p, type_name))
    return out


def filter_matches(
    matches: list[tuple[Path, str]],
    src: Path,
    wanted_types: set[str] | None,
    include_glob: str | None,
) -> list[tuple[Path, str]]:
    out = []
    for path, type_name in matches:
        if wanted_types and type_name not in wanted_types:
            continue
        if include_glob:
            basename = path.name
            rel = str(path.relative_to(src)).replace("\\", "/")
            if not (
                fnmatch.fnmatch(basename, include_glob)
                or fnmatch.fnmatch(rel, include_glob)
            ):
                continue
        out.append((path, type_name))
    return out


def _parse_types(types_arg: str | None) -> set[str] | None:
    if not types_arg:
        return None
    types = {t.strip().lower() for t in types_arg.split(",") if t.strip()}
    valid = {t for t, _ in LOOKML_TYPES}
    invalid = types - valid
    if invalid:
        raise SystemExit(
            f"ERROR: unknown --types entries: {sorted(invalid)}. "
            f"Valid: {sorted(valid)}"
        )
    return types


def main() -> int:
    parser = argparse.ArgumentParser(prog="import_lookml_local")
    parser.add_argument(
        "src", help="Path to the locally-downloaded Looker repo (root)"
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_OUT_DIR,
        help=f"Destination dir. Default: {DEFAULT_OUT_DIR}/",
    )
    parser.add_argument(
        "--types",
        help=(
            "Comma-separated list. Default: ALL "
            "(view, model, explore, dashboard, manifest, lookml)"
        ),
    )
    parser.add_argument(
        "--flat",
        action="store_true",
        help="Ignore source subdirs — copy every file directly under --out",
    )
    parser.add_argument(
        "--include",
        help="Glob filter on basename or relative path (e.g. 'risk_*' or '*acct*')",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the matching paths and exit (no copy)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print copy operations but don't actually write files",
    )
    args = parser.parse_args()

    src = Path(args.src).expanduser().resolve()
    if not src.exists():
        print(f"ERROR: source path not found: {src}", file=sys.stderr)
        return 2
    if not src.is_dir():
        print(f"ERROR: source must be a directory: {src}", file=sys.stderr)
        return 2

    wanted_types = _parse_types(args.types)

    print(f"# Walking {src} …", file=sys.stderr)
    all_matches = walk_local_repo(src)
    matches = filter_matches(all_matches, src, wanted_types, args.include)

    if not matches:
        print(
            f"No LookML files matched (types={wanted_types or 'ALL'}, "
            f"include={args.include!r}). Walked {len(all_matches)} candidates.",
            file=sys.stderr,
        )
        return 1

    by_type = Counter(t for _, t in matches)

    if args.list:
        types_label = (
            f"types={sorted(wanted_types)}" if wanted_types else "all LookML types"
        )
        print(f"# {len(matches)} matching files in {src} ({types_label})")
        print(f"# Breakdown: {dict(by_type)}")
        for path, type_name in sorted(matches, key=lambda x: x[0]):
            rel = path.relative_to(src)
            size_kb = round(path.stat().st_size / 1024, 1)
            print(f"{type_name:>9}  {size_kb:>6.1f} KB  {rel}")
        return 0

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    layout = "flat" if args.flat else "preserving subdirs"
    mode = " (DRY RUN)" if args.dry_run else ""
    print(
        f"# Copying {len(matches)} files into {out_dir}/ ({layout}){mode}\n"
        f"# Breakdown: {dict(by_type)}\n",
        file=sys.stderr,
    )

    failures: list[str] = []
    total_bytes = 0
    copied_by_type: Counter = Counter()

    for path, type_name in matches:
        rel = path.relative_to(src)
        target = out_dir / path.name if args.flat else out_dir / rel
        try:
            if not args.dry_run:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)
            size_kb = round(path.stat().st_size / 1024, 1)
            copied_by_type[type_name] += 1
            total_bytes += path.stat().st_size
            print(f"[OK]   {type_name:>9}  {target}  ({size_kb} KB)")
        except OSError as e:
            failures.append(f"{rel}: {e}")
            print(f"[FAIL] {type_name:>9}  {rel}: {e}", file=sys.stderr)

    total_mb = round(total_bytes / (1024 * 1024), 2)
    breakdown = ", ".join(
        f"{n} {t}{'s' if n != 1 else ''}"
        for t, n in sorted(copied_by_type.items())
    )
    print(
        f"\nDone — {len(matches) - len(failures)}/{len(matches)} files "
        f"copied ({total_mb} MB total: {breakdown}) → {out_dir}/"
    )
    if failures:
        print("\nFailures:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
