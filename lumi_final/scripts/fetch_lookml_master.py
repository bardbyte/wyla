#!/usr/bin/env python3
"""Mirror the entire LookML project from GitHub to disk.

By default pulls EVERY LookML artifact: views, models, explores, dashboards,
manifest. Preserves the source repo's directory structure under
data/looker_master/ so the merge stage (Stage 7 Publish) can write back
into matching paths.

Why we need more than .view.lkml:
  *.view.lkml      → field definitions (we enrich these)
  *.model.lkml     → connection name + include directives + explores
                     The pipeline must add new explores here when it
                     creates derived views — can't fly blind.
  *.explore.lkml   → some projects split explores into their own files
  manifest.lkml    → project manifest (local_dependencies, includes)
                     Tells us what's loaded by Looker.
  *.dashboard.lookml  Optional — useful for understanding existing
                     usage patterns when we tune enrichment.

Usage (on Saheb's work laptop, on VPN):

    export GHE_TOKEN='ghp_…'           # SSO-authorized for amex-eng

    python scripts/fetch_lookml_master.py                     # default: everything
    python scripts/fetch_lookml_master.py --types view,model  # just views + models
    python scripts/fetch_lookml_master.py --types view        # views only
    python scripts/fetch_lookml_master.py --branch develop
    python scripts/fetch_lookml_master.py --include 'risk_*'  # glob filter on basename
    python scripts/fetch_lookml_master.py --list              # dry-run, list paths only
    python scripts/fetch_lookml_master.py --out data/baseline_views/  # different dir

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
from collections import Counter
from pathlib import Path
from typing import Any

DEFAULT_API_BASE = "https://github.aexp.com/api/v3"
DEFAULT_REPO = "amex-eng/prj-d-lumi-gpt-semantic"
DEFAULT_BRANCH = "auto"  # special — auto-detect from /repos/{repo}.default_branch
DEFAULT_OUT_DIR = "data/looker_master"
DEFAULT_TIMEOUT_SECS = 30

# (type_name, suffix-or-basename matcher), most-specific first so
# manifest.lkml beats the generic .lkml catch-all.
LOOKML_TYPES: list[tuple[str, str]] = [
    ("view", ".view.lkml"),
    ("model", ".model.lkml"),
    ("explore", ".explore.lkml"),
    ("dashboard", ".dashboard.lookml"),
    ("dashboard", ".dashboard.lkml"),
    ("manifest", "manifest.lkml"),  # exact basename match
    ("lookml", ".lkml"),  # catch-all for other .lkml (rare)
]


def classify_type(path: str) -> str | None:
    """Return the best-matching LookML type for a file path, or None."""
    basename = path.rsplit("/", 1)[-1]
    for type_name, matcher in LOOKML_TYPES:
        if matcher.startswith("."):
            if path.endswith(matcher):
                return type_name
        else:
            if basename == matcher:
                return type_name
    return None


# ─── HTTP helpers ────────────────────────────────────────────


def _gh_request(url: str, token: str) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "lumi-fetch-lookml-master/0.1",
        },
    )
    with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECS) as resp:
        return json.loads(resp.read())


def get_repo_metadata(
    repo: str, token: str, api_base: str
) -> dict[str, Any]:
    """GET /repos/{repo}. Returns full repo metadata (default_branch, etc.).
    Raises HTTPError if the repo doesn't exist or PAT can't see it.
    """
    return _gh_request(f"{api_base}/repos/{repo}", token)


def list_all_files(
    repo: str, branch: str, token: str, api_base: str
) -> tuple[list[dict[str, Any]], str]:
    """Trees API ?recursive=1 — every file at branch HEAD in one call.

    Returns (tree_entries, resolved_branch_name). If `branch == "auto"` we
    fetch /repos/{repo} first to discover the default branch. If an explicit
    branch 404s, we ALSO try the default branch and report which one worked.
    """
    # Step 1: figure out the branch.
    if branch == "auto":
        try:
            meta = get_repo_metadata(repo, token, api_base)
        except urllib.error.HTTPError as e:
            raise RuntimeError(
                f"could not read /repos/{repo} (HTTP {e.code} {e.reason}). "
                "Verify: (a) repo name is exact, (b) PAT is SSO-authorized "
                "for the org. Run scripts/check_github_access.py from the "
                "main repo if you have it."
            ) from e
        branch = meta.get("default_branch") or "main"
        print(f"# auto-detected default branch: {branch}", file=sys.stderr)

    # Step 2: fetch the branch's HEAD SHA.
    branch_url = f"{api_base}/repos/{repo}/branches/{urllib.parse.quote(branch)}"
    try:
        branch_data = _gh_request(branch_url, token)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        # Try default branch as a fallback before giving up.
        try:
            meta = get_repo_metadata(repo, token, api_base)
            default_branch = meta.get("default_branch")
        except Exception:
            default_branch = None
        if default_branch and default_branch != branch:
            print(
                f"WARN: branch {branch!r} not found; falling back to "
                f"default branch {default_branch!r}.",
                file=sys.stderr,
            )
            branch = default_branch
            branch_data = _gh_request(
                f"{api_base}/repos/{repo}/branches/{urllib.parse.quote(branch)}",
                token,
            )
        else:
            raise RuntimeError(
                f"branch {branch!r} not found on {repo} (HTTP 404). "
                f"Default branch is {default_branch or 'unknown'}. "
                f"Pass --branch <name> with the correct branch."
            ) from e

    head_sha = branch_data["commit"]["sha"]

    # Step 3: list the tree at that SHA.
    tree_url = f"{api_base}/repos/{repo}/git/trees/{head_sha}?recursive=1"
    tree_data = _gh_request(tree_url, token)

    if tree_data.get("truncated"):
        print(
            "WARN: tree response is truncated — repo is huge. Add --include to scope.",
            file=sys.stderr,
        )

    return tree_data.get("tree") or [], branch


def fetch_blob(repo: str, sha: str, token: str, api_base: str) -> str:
    """GET /git/blobs/{sha}. Returns decoded text."""
    url = f"{api_base}/repos/{repo}/git/blobs/{sha}"
    data = _gh_request(url, token)
    if data.get("encoding") == "base64":
        return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
    return data.get("content") or ""


# ─── Filter ─────────────────────────────────────────────────


def filter_lookml_files(
    tree: list[dict[str, Any]],
    wanted_types: set[str] | None,
    include_glob: str | None,
) -> list[dict[str, Any]]:
    """Keep only LookML blobs of the requested types matching the optional glob."""
    out = []
    for entry in tree:
        if entry.get("type") != "blob":
            continue
        path = entry.get("path") or ""
        type_name = classify_type(path)
        if type_name is None:
            continue
        if wanted_types and type_name not in wanted_types:
            continue
        if include_glob:
            basename = path.rsplit("/", 1)[-1]
            if not (
                fnmatch.fnmatch(basename, include_glob)
                or fnmatch.fnmatch(path, include_glob)
            ):
                continue
        # Annotate with type so downstream doesn't re-classify.
        entry["_lookml_type"] = type_name
        out.append(entry)
    return out


def save_file(
    entry: dict[str, Any],
    contents: str,
    out_dir: Path,
    flat: bool,
) -> Path:
    """Write the file under out_dir, optionally preserving subdirs."""
    path = entry["path"]
    target = out_dir / path.rsplit("/", 1)[-1] if flat else out_dir / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(contents, encoding="utf-8")
    return target


# ─── CLI ─────────────────────────────────────────────────────


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
    parser = argparse.ArgumentParser(prog="fetch_lookml_master")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="owner/name on GHE")
    parser.add_argument(
        "--branch",
        default=DEFAULT_BRANCH,
        help=(
            "Branch to fetch from. Default: 'auto' (reads default_branch from "
            "/repos/{repo}). Pass an explicit name like 'main', 'master', "
            "'develop' to override."
        ),
    )
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument(
        "--out", default=DEFAULT_OUT_DIR, help=f"Default: {DEFAULT_OUT_DIR}/"
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
        help="Ignore source subdirs — save every file directly under --out",
    )
    parser.add_argument(
        "--include",
        help="Glob filter on basename or full path (e.g. 'risk_*' or '*acct*')",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the matching paths and exit (no download)",
    )
    parser.add_argument(
        "--token-env",
        default="GHE_TOKEN",
        help="Env var holding the PAT (fallback: GITHUB_TOKEN)",
    )
    args = parser.parse_args()

    wanted_types = _parse_types(args.types)

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
        tree, resolved_branch = list_all_files(
            args.repo, args.branch, token, args.api_base
        )
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except urllib.error.HTTPError as e:
        print(
            f"ERROR: HTTP {e.code} {e.reason} listing tree for "
            f"{args.repo}@{args.branch}",
            file=sys.stderr,
        )
        return 1

    matches = filter_lookml_files(tree, wanted_types, args.include)
    if not matches:
        print(
            f"No LookML files matched (types={wanted_types or 'ALL'}, "
            f"include={args.include!r}, tree size={len(tree)}).",
            file=sys.stderr,
        )
        return 1

    by_type = Counter(m["_lookml_type"] for m in matches)

    if args.list:
        types_label = (
            f"types={sorted(wanted_types)}" if wanted_types else "all LookML types"
        )
        print(
            f"# {len(matches)} matching files in {args.repo}@{resolved_branch} "
            f"({types_label})"
        )
        print(f"# Breakdown: {dict(by_type)}")
        for m in sorted(matches, key=lambda e: e["path"]):
            size_kb = round((m.get("size") or 0) / 1024, 1)
            type_label = m["_lookml_type"]
            print(f"{type_label:>9}  {size_kb:>6.1f} KB  {m['path']}")
        return 0

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"# Fetching {len(matches)} files into {out_dir}/ "
        f"({'flat' if args.flat else 'preserving subdirs'})\n"
        f"# Breakdown: {dict(by_type)}\n",
        file=sys.stderr,
    )

    failures: list[str] = []
    total_bytes = 0
    fetched_by_type: Counter = Counter()

    for entry in matches:
        path = entry["path"]
        sha = entry["sha"]
        type_name = entry["_lookml_type"]
        try:
            contents = fetch_blob(args.repo, sha, token, args.api_base)
        except urllib.error.HTTPError as e:
            failures.append(f"{path}: HTTP {e.code} {e.reason}")
            print(f"[FAIL] {type_name:>9}  {path}: HTTP {e.code}", file=sys.stderr)
            continue
        except urllib.error.URLError as e:
            failures.append(f"{path}: connection {e.reason}")
            print(f"[FAIL] {type_name:>9}  {path}: {e.reason}", file=sys.stderr)
            continue

        target = save_file(entry, contents, out_dir, args.flat)
        total_bytes += len(contents)
        fetched_by_type[type_name] += 1
        size_kb = round(len(contents) / 1024, 1)
        print(f"[OK]   {type_name:>9}  {target}  ({size_kb} KB)")

    total_mb = round(total_bytes / (1024 * 1024), 2)
    breakdown = ", ".join(
        f"{n} {t}{'s' if n != 1 else ''}"
        for t, n in sorted(fetched_by_type.items())
    )
    print(
        f"\nDone — {len(matches) - len(failures)}/{len(matches)} files "
        f"fetched ({total_mb} MB total: {breakdown}) → {out_dir}/"
    )
    if failures:
        print("\nFailures:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
