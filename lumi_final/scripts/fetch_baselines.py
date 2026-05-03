#!/usr/bin/env python3
"""Pull baseline .view.lkml files from the Looker Enterprise GitHub repo.

The set of tables to fetch is DERIVED from the SQL files in
data/gold_queries/ (or whatever you point --from-sqls at). Same script
works for the 10 fixtures or 129 production queries.

Usage (on Saheb's work laptop, on VPN):

    export GHE_TOKEN='ghp_xxxxx...'        # SSO-authorized for amex-eng org

    python scripts/fetch_baselines.py                              # all tables in data/gold_queries/
    python scripts/fetch_baselines.py --from-sqls data/extra_sqls/ # different dir
    python scripts/fetch_baselines.py --table cornerstone_metrics  # single table
    python scripts/fetch_baselines.py --views-path lookml/{name}.view.lkml

Saves files into data/baseline_views/<table_name>.view.lkml.

Pure-stdlib urllib for HTTP. Uses lumi.sql_to_context.parse_sqls for table
discovery (one source of truth — same parser the pipeline uses).
Per parent CLAUDE.md: PAT must be SSO-authorized against amex-eng org.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_API_BASE = "https://github.aexp.com/api/v3"
DEFAULT_REPO = "amex-eng/prj-d-lumi-gpt-semantic"
DEFAULT_GOLD_QUERIES_DIR = "data/gold_queries"
DEFAULT_TIMEOUT_SECS = 30

# Common locations for view files inside Looker projects.
DEFAULT_PATH_CANDIDATES = (
    "views/{name}.view.lkml",
    "{name}.view.lkml",
    "looker/views/{name}.view.lkml",
    "lookml/views/{name}.view.lkml",
)


# ─── Discover tables from the SQL corpus ─────────────────────


def discover_tables_from_sqls(sql_dir: Path) -> list[str]:
    """Same logic as probe_mdm — single source of truth via lumi.sql_to_context.

    Includes both top-level FROM tables and CTE source tables.
    """
    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        return []

    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from lumi.sql_to_context import parse_sqls  # noqa: E402

    sqls = [f.read_text(encoding="utf-8") for f in sql_files]
    fps = parse_sqls(sqls)

    tables: set[str] = set()
    parse_failures = 0
    for fp in fps:
        if fp.parse_error:
            parse_failures += 1
            continue
        tables.update(fp.tables)
        for cte in fp.ctes:
            tables.update(cte.get("source_tables") or [])

    if parse_failures:
        print(
            f"WARN: {parse_failures}/{len(sql_files)} SQL files failed to parse",
            file=sys.stderr,
        )
    return sorted(tables)


# ─── HTTP fetch ──────────────────────────────────────────────


def fetch_file(
    repo: str,
    path: str,
    token: str,
    api_base: str = DEFAULT_API_BASE,
) -> str | None:
    """GET /repos/{repo}/contents/{path}. Returns decoded text, or None on 404."""
    url = f"{api_base}/repos/{repo}/contents/{urllib.parse.quote(path)}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "lumi-fetch-baselines/0.1",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECS) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise

    encoding = data.get("encoding", "")
    if encoding != "base64":
        return data.get("content", "")
    import base64
    return base64.b64decode(data["content"]).decode("utf-8", errors="replace")


def find_view(
    repo: str,
    table_name: str,
    token: str,
    api_base: str,
    candidates: tuple[str, ...] = DEFAULT_PATH_CANDIDATES,
) -> tuple[str | None, str | None]:
    """Try each candidate path. Returns (resolved_path, file_contents) or (None, None)."""
    for tmpl in candidates:
        path = tmpl.format(name=table_name)
        contents = fetch_file(repo, path, token, api_base)
        if contents is not None:
            return path, contents
    return None, None


# ─── CLI ─────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(prog="fetch_baselines")
    parser.add_argument(
        "--from-sqls",
        default=DEFAULT_GOLD_QUERIES_DIR,
        help=f"Directory of .sql files. Default: {DEFAULT_GOLD_QUERIES_DIR}/",
    )
    parser.add_argument("--table", help="Single table — overrides --from-sqls discovery")
    parser.add_argument(
        "--list",
        action="store_true",
        help="Just list the tables that would be fetched and exit",
    )
    parser.add_argument("--repo", default=DEFAULT_REPO, help="owner/name on GHE")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument(
        "--out", default="data/baseline_views/", help="Where to save .view.lkml files"
    )
    parser.add_argument(
        "--views-path",
        help="Override view path template, e.g. 'lookml/{name}.view.lkml'",
    )
    parser.add_argument(
        "--token-env",
        default="GHE_TOKEN",
        help="Env var holding the PAT (fallback: GITHUB_TOKEN)",
    )
    args = parser.parse_args()

    # Resolve table set
    if args.table:
        tables = [args.table]
    else:
        sql_dir = Path(args.from_sqls)
        if not sql_dir.exists():
            print(f"ERROR: {sql_dir} does not exist", file=sys.stderr)
            return 2
        tables = discover_tables_from_sqls(sql_dir)
        if not tables:
            print(
                f"ERROR: no .sql files under {sql_dir} (or all failed to parse)",
                file=sys.stderr,
            )
            return 2

    if args.list:
        print(f"# Discovered {len(tables)} unique tables across {args.from_sqls}/")
        for t in tables:
            print(t)
        return 0

    token = os.environ.get(args.token_env, "").strip() or os.environ.get(
        "GITHUB_TOKEN", ""
    ).strip()
    if not token:
        print(
            f"ERROR: no token in ${args.token_env} or $GITHUB_TOKEN. "
            "PAT must be SSO-authorized against the amex-eng org.",
            file=sys.stderr,
        )
        return 2

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    candidates = (args.views_path,) if args.views_path else DEFAULT_PATH_CANDIDATES

    print(f"# Fetching {len(tables)} baseline views (from {args.from_sqls}/)\n")

    failures: list[str] = []
    for t in tables:
        try:
            path, contents = find_view(args.repo, t, token, args.api_base, candidates)
        except urllib.error.HTTPError as e:
            failures.append(f"{t}: HTTP {e.code} {e.reason}")
            print(f"[{t}] FAIL — HTTP {e.code} {e.reason}", file=sys.stderr)
            continue
        except urllib.error.URLError as e:
            failures.append(f"{t}: connection {e.reason}")
            print(f"[{t}] FAIL — connection {e.reason}", file=sys.stderr)
            continue

        if contents is None:
            failures.append(f"{t}: not found at any candidate path")
            print(
                f"[{t}] NOT FOUND — tried: "
                f"{[c.format(name=t) for c in candidates]}",
                file=sys.stderr,
            )
            continue

        target = out_dir / f"{t}.view.lkml"
        target.write_text(contents, encoding="utf-8")
        size_kb = round(len(contents) / 1024, 1)
        print(f"[{t}] OK — {path} ({size_kb} KB) → {target}")

    print(f"\nDone — {len(tables) - len(failures)}/{len(tables)} baselines fetched.")
    if failures:
        print("\nFailures:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
