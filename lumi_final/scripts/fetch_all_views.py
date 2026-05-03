#!/usr/bin/env python3
"""Mirror EVERY .view.lkml from the Looker Enterprise GitHub repo to disk.

Why this exists alongside fetch_baselines.py:
  fetch_baselines.py — pulls only views for tables our gold queries reference.
                       Fast, targeted, ~30 files.
  fetch_all_views.py — pulls THE WHOLE LOOKER MASTER. ~hundreds of files.
                       Use when you want the complete picture (e.g., the
                       enricher needs to look at related views for context,
                       or the gold queries are about to expand and you want
                       coverage to grow without re-fetching).

Strategy:
  1. ONE call to GitHub Trees API with ?recursive=1 to list every file in
     the repo at HEAD of the branch — no per-directory paging.
  2. Filter to *.view.lkml.
  3. For each match: fetch the blob via /git/blobs/{sha} (one API call per
     view, but each is tiny and parallelizable; we keep it serial for
     simplicity since 200 files × ~200ms = ~40s, fine).
  4. Save under data/looker_master/, preserving subdirectory structure
     (or flat, with --flat).

Usage (on Saheb's work laptop, on VPN):

    export GHE_TOKEN='ghp_…'           # SSO-authorized for amex-eng
    python scripts/fetch_all_views.py                      # default: master mirror
    python scripts/fetch_all_views.py --branch develop
    python scripts/fetch_all_views.py --out data/baseline_views/  # overwrite that dir
    python scripts/fetch_all_views.py --flat              # ignore subdirs
    python scripts/fetch_all_views.py --list              # don't download, just list
    python scripts/fetch_all_views.py --include 'risk_*'  # glob filter

Per parent CLAUDE.md sharp-edge #7: PAT must be SSO-authorized for amex-eng.
"""

from __future__ import annotations

import argparse
import base64
import fnmatch
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_API_BASE = "https://github.aexp.com/api/v3"
DEFAULT_REPO = "amex-eng/prj-d-lumi-gpt-semantic"
DEFAULT_BRANCH = "main"
DEFAULT_OUT_DIR = "data/looker_master"
DEFAULT_TIMEOUT_SECS = 30


# ─── HTTP helpers ────────────────────────────────────────────


def _gh_request(url: str, token: str) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "lumi-fetch-all-views/0.1",
        },
    )
    with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECS) as resp:
        return json.loads(resp.read())


def list_all_files(
    repo: str, branch: str, token: str, api_base: str
) -> list[dict[str, Any]]:
    """Use the Trees API to list every file in the repo at branch HEAD.

    Returns the `tree` field — list of {path, sha, size, type, url}.
    Bails clearly if the response is truncated (huge repo edge case).
    """
    branch_url = f"{api_base}/repos/{repo}/branches/{urllib.parse.quote(branch)}"
    branch_data = _gh_request(branch_url, token)
    head_sha = branch_data["commit"]["sha"]

    tree_url = (
        f"{api_base}/repos/{repo}/git/trees/{head_sha}?recursive=1"
    )
    tree_data = _gh_request(tree_url, token)

    if tree_data.get("truncated"):
        print(
            "WARN: tree response is truncated — repo is huge. Falling back "
            "would require directory paging. Add --include '<glob>' to scope.",
            file=sys.stderr,
        )

    return tree_data.get("tree") or []


def fetch_blob(repo: str, sha: str, token: str, api_base: str) -> str:
    """GET /git/blobs/{sha}. Returns decoded text."""
    url = f"{api_base}/repos/{repo}/git/blobs/{sha}"
    data = _gh_request(url, token)
    if data.get("encoding") == "base64":
        return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
    return data.get("content") or ""


# ─── Filter + write ──────────────────────────────────────────


def filter_view_files(
    tree: list[dict[str, Any]],
    include_glob: str | None,
) -> list[dict[str, Any]]:
    """Keep only blobs ending in .view.lkml. Optional glob applied to the
    *file basename*, not the full path.
    """
    out = []
    for entry in tree:
        if entry.get("type") != "blob":
            continue
        path = entry.get("path") or ""
        if not path.endswith(".view.lkml"):
            continue
        basename = path.rsplit("/", 1)[-1]
        # The basename WITHOUT .view.lkml is the "table name" — match against that.
        bare = basename[: -len(".view.lkml")]
        if include_glob and not (
            fnmatch.fnmatch(basename, include_glob)
            or fnmatch.fnmatch(bare, include_glob)
        ):
            continue
        out.append(entry)
    return out


def save_view(
    entry: dict[str, Any],
    contents: str,
    out_dir: Path,
    flat: bool,
) -> Path:
    """Write the view file under out_dir, optionally preserving subdirs."""
    path = entry["path"]
    if flat:
        target = out_dir / path.rsplit("/", 1)[-1]
    else:
        target = out_dir / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(contents, encoding="utf-8")
    return target


# ─── CLI ─────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(prog="fetch_all_views")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="owner/name on GHE")
    parser.add_argument("--branch", default=DEFAULT_BRANCH)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument(
        "--out", default=DEFAULT_OUT_DIR, help=f"Default: {DEFAULT_OUT_DIR}/"
    )
    parser.add_argument(
        "--flat",
        action="store_true",
        help="Ignore source subdirs — save every view file directly in --out",
    )
    parser.add_argument(
        "--include",
        help="Glob filter on the view's basename (e.g. 'risk_*' or '*acct*')",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the matching view paths and exit (no download)",
    )
    parser.add_argument(
        "--token-env",
        default="GHE_TOKEN",
        help="Env var holding the PAT (fallback: GITHUB_TOKEN)",
    )
    args = parser.parse_args()

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

    print(f"# Listing tree for {args.repo}@{args.branch} …", file=sys.stderr)
    try:
        tree = list_all_files(args.repo, args.branch, token, args.api_base)
    except urllib.error.HTTPError as e:
        print(f"ERROR: HTTP {e.code} {e.reason} on tree listing", file=sys.stderr)
        return 1

    matches = filter_view_files(tree, args.include)
    if not matches:
        print(
            f"No .view.lkml files matched (include={args.include!r}, "
            f"tree size={len(tree)}). Try a broader --include or check --branch.",
            file=sys.stderr,
        )
        return 1

    if args.list:
        print(f"# {len(matches)} matching .view.lkml files in {args.repo}@{args.branch}")
        for m in sorted(matches, key=lambda e: e["path"]):
            size_kb = round((m.get("size") or 0) / 1024, 1)
            print(f"{size_kb:>6.1f} KB  {m['path']}")
        return 0

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"# Fetching {len(matches)} .view.lkml files into {out_dir}/ "
        f"({'flat' if args.flat else 'preserving subdirs'})\n",
        file=sys.stderr,
    )

    failures: list[str] = []
    total_bytes = 0
    for entry in matches:
        path = entry["path"]
        sha = entry["sha"]
        try:
            contents = fetch_blob(args.repo, sha, token, args.api_base)
        except urllib.error.HTTPError as e:
            failures.append(f"{path}: HTTP {e.code} {e.reason}")
            print(f"[FAIL] {path}: HTTP {e.code}", file=sys.stderr)
            continue
        except urllib.error.URLError as e:
            failures.append(f"{path}: connection {e.reason}")
            print(f"[FAIL] {path}: {e.reason}", file=sys.stderr)
            continue

        target = save_view(entry, contents, out_dir, args.flat)
        total_bytes += len(contents)
        size_kb = round(len(contents) / 1024, 1)
        print(f"[OK]  {target}  ({size_kb} KB)")

    total_mb = round(total_bytes / (1024 * 1024), 2)
    print(
        f"\nDone — {len(matches) - len(failures)}/{len(matches)} views "
        f"fetched ({total_mb} MB total) → {out_dir}/"
    )
    if failures:
        print("\nFailures:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
