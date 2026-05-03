#!/usr/bin/env python3
"""Pull baseline .view.lkml files from the Looker Enterprise GitHub repo.

Usage (on Saheb's work laptop, on VPN):

    export GHE_TOKEN='ghp_xxxxx...'        # SSO-authorized for amex-eng org
    python scripts/fetch_baselines.py                       # all 6 baseline tables
    python scripts/fetch_baselines.py --table cornerstone_metrics
    python scripts/fetch_baselines.py --views-path views/   # subdirectory in the repo

Saves files into data/baseline_views/<table_name>.view.lkml.

Pure-stdlib urllib — runs from a fresh laptop with no pip installs.
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
DEFAULT_TIMEOUT_SECS = 30

# Tables Q1-Q10 reference. Update as the gold queries grow.
DEFAULT_TABLES = [
    "cornerstone_metrics",
    "risk_pers_acct_history",
    "risk_indv_cust_hist",
    "drm_product_member",
    "drm_product_hier",
    "acquisitions",
]

# Common locations for view files inside Looker projects.
DEFAULT_PATH_CANDIDATES = (
    "views/{name}.view.lkml",
    "{name}.view.lkml",
    "looker/views/{name}.view.lkml",
    "lookml/views/{name}.view.lkml",
)


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

    # GitHub returns {content: base64, encoding: 'base64'}
    import base64

    encoding = data.get("encoding", "")
    if encoding != "base64":
        return data.get("content", "")
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


def main() -> int:
    parser = argparse.ArgumentParser(prog="fetch_baselines")
    parser.add_argument("--table", help="Single table; default: all 6 Q1-Q10 tables")
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

    token = os.environ.get(args.token_env, "").strip() or os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        print(
            f"ERROR: no token in ${args.token_env} or $GITHUB_TOKEN. "
            "PAT must be SSO-authorized against the amex-eng org "
            "(see parent CLAUDE.md sharp-edge #7).",
            file=sys.stderr,
        )
        return 2

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    tables = [args.table] if args.table else DEFAULT_TABLES
    candidates = (args.views_path,) if args.views_path else DEFAULT_PATH_CANDIDATES

    failures: list[str] = []
    for t in tables:
        try:
            path, contents = find_view(t, t, token, args.api_base, candidates) \
                if False else find_view(args.repo, t, token, args.api_base, candidates)
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
