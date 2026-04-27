#!/usr/bin/env python3
"""Preflight: verify a Personal Access Token can access a private GitHub
Enterprise repo (default host: github.aexp.com).

Standalone usage (no pip install needed — uses only stdlib):

    export GITHUB_AEXP_TOKEN='ghp_xxx...'
    python scripts/check_github_access.py owner/repo
    python scripts/check_github_access.py owner/repo --path views
    python scripts/check_github_access.py owner/repo --path views/foo.view.lkml

The function `check_github_access(...)` returns the standard
{status, ..., error} dict and is tool-ready: when we wrap this for the agent,
the CLI wrapper goes away and the function gets registered as an ADK tool.

Exit codes:
    0  all checks passed
    1  one or more checks failed (token, repo, or path)
    2  invalid usage / missing env var

Checks performed (in order; bails on first failure):
    1. GET /user                       — token authenticates
    2. GET /repos/{owner}/{name}       — repo is reachable, see its permissions
    3. GET /repos/.../contents/{path}  — optional, only if --path is given
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_BASE_URL = "https://github.aexp.com/api/v3"
DEFAULT_TIMEOUT_SECS = 15
DEFAULT_TOKEN_ENV = "GITHUB_AEXP_TOKEN"


def normalize_repo(repo_arg: str) -> tuple[str, str | None]:
    """Coerce any common repo-reference form into ('owner/name', api_base_url?).

    Accepted inputs:
      - owner/name
      - https://github.aexp.com/owner/name
      - https://github.aexp.com/owner/name.git
      - https://github.aexp.com/owner/name/tree/branch/some/path
      - git@github.aexp.com:owner/name.git
      - github.aexp.com/owner/name

    Returns:
        (owner_name, derived_api_base_url_or_None)
        derived_api_base_url is set when a host is detectable; the CLI uses it
        only if --base-url wasn't explicitly overridden.
    """
    s = repo_arg.strip()
    derived_base: str | None = None

    # SSH form: git@host:owner/name(.git)
    if s.startswith("git@"):
        try:
            host, rest = s[len("git@"):].split(":", 1)
            derived_base = f"https://{host}/api/v3"
            s = rest
        except ValueError:
            pass
    # HTTPS or scheme-less host forms
    elif "://" in s or s.startswith("github."):
        if "://" not in s:
            s = "https://" + s
        parsed = urllib.parse.urlparse(s)
        if parsed.netloc:
            derived_base = f"{parsed.scheme}://{parsed.netloc}/api/v3"
        s = parsed.path.lstrip("/")

    # Trim .git suffix
    if s.endswith(".git"):
        s = s[: -len(".git")]
    # Trim "/tree/<branch>/..." or "/blob/..." trailing paths
    parts = s.split("/")
    if len(parts) >= 2:
        s = "/".join(parts[:2])

    return s, derived_base


# --------------------------------------------------------------------------- #
# Tool-shaped core function — this is what we'll lift into lumi/tools/ later. #
# --------------------------------------------------------------------------- #

def check_github_access(
    repo: str,
    token: str,
    base_url: str = DEFAULT_BASE_URL,
    path: str | None = None,
    ref: str | None = None,
    timeout_secs: int = DEFAULT_TIMEOUT_SECS,
) -> dict[str, Any]:
    """Verify a PAT can access a private GitHub Enterprise repo.

    Args:
        repo: "owner/name", e.g. "amex-eng/looker-project".
        token: Personal Access Token. For private repos, classic PATs need
               the `repo` scope; fine-grained PATs need Contents:Read on the
               target repo.
        base_url: GHE API root. Default https://github.aexp.com/api/v3.
        path: Optional path within the repo to list/fetch as a probe.
              If a directory: returns the listing.
              If a file: returns size + (decoded) content if small.
        ref: Branch / tag / commit SHA to query for the path probe. Defaults
             to the repo's default branch when None.
        timeout_secs: HTTP timeout per request.

    Returns:
        {
          "status": "success" | "error",
          "checks": [{name, status, ...}, ...],
          "user": login string or None,
          "repo": "owner/name",
          "default_branch": str or None,
          "private": bool or None,
          "permissions": {admin, push, pull} or None,
          "path": str or None,
          "path_kind": "directory" | "file" | None,
          "path_listing": list of {name, type, path, size?} or None,
          "error": str or None,
        }
    """
    repo, _derived = normalize_repo(repo)
    if "/" not in repo or repo.count("/") != 1 or not all(repo.split("/")):
        return _err(
            repo,
            "could not parse repo into 'owner/name' (accepts URL, SSH, or short form)",
        )

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"token {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "lumi-preflight/0.1",
    }
    base = base_url.rstrip("/")
    checks: list[dict[str, Any]] = []

    # --- check 1: token authenticates --------------------------------------
    user_resp = _http_get_json(f"{base}/user", headers, timeout_secs)
    if not user_resp["ok"]:
        checks.append({"name": "token", "status": "fail", "detail": user_resp["detail"]})
        return _err(repo, f"token check failed: {user_resp['detail']}", checks=checks)

    user_login = (user_resp["json"] or {}).get("login")
    checks.append({"name": "token", "status": "ok", "user": user_login})

    # --- check 2: repo is reachable + introspect permissions ---------------
    repo_resp = _http_get_json(f"{base}/repos/{repo}", headers, timeout_secs)
    if not repo_resp["ok"]:
        checks.append({"name": "repo", "status": "fail", "detail": repo_resp["detail"]})
        return _err(
            repo,
            f"repo '{repo}' not reachable: {repo_resp['detail']}",
            checks=checks,
            user=user_login,
        )

    repo_meta = repo_resp["json"] or {}
    default_branch = repo_meta.get("default_branch")
    private = repo_meta.get("private")
    permissions = repo_meta.get("permissions")
    checks.append(
        {
            "name": "repo",
            "status": "ok",
            "private": private,
            "default_branch": default_branch,
            "permissions": permissions,
        }
    )

    # --- check 3 (optional): probe a path ----------------------------------
    path_kind: str | None = None
    path_listing: list[dict[str, Any]] | None = None
    if path:
        target_ref = ref or default_branch
        clean_path = path.lstrip("/")
        contents_url = f"{base}/repos/{repo}/contents/{urllib.parse.quote(clean_path)}"
        if target_ref:
            contents_url += f"?ref={urllib.parse.quote(target_ref)}"

        contents_resp = _http_get_json(contents_url, headers, timeout_secs)
        if not contents_resp["ok"]:
            checks.append(
                {
                    "name": "path",
                    "status": "fail",
                    "path": path,
                    "ref": target_ref,
                    "detail": contents_resp["detail"],
                }
            )
            return _err(
                repo,
                f"path '{path}' not fetchable on ref '{target_ref}': {contents_resp['detail']}",
                checks=checks,
                user=user_login,
            )

        body = contents_resp["json"]
        if isinstance(body, list):
            path_kind = "directory"
            path_listing = [
                {
                    "name": e.get("name"),
                    "type": e.get("type"),
                    "path": e.get("path"),
                    "size": e.get("size"),
                }
                for e in body
                if isinstance(e, dict)
            ]
            checks.append(
                {
                    "name": "path",
                    "status": "ok",
                    "kind": "directory",
                    "entries": len(path_listing),
                }
            )
        elif isinstance(body, dict) and body.get("type") == "file":
            path_kind = "file"
            path_listing = [
                {
                    "name": body.get("name"),
                    "type": "file",
                    "path": body.get("path"),
                    "size": body.get("size"),
                }
            ]
            checks.append(
                {
                    "name": "path",
                    "status": "ok",
                    "kind": "file",
                    "size": body.get("size"),
                }
            )
        else:
            checks.append(
                {
                    "name": "path",
                    "status": "warn",
                    "detail": "unexpected response shape (not a list and not a file)",
                }
            )

    return {
        "status": "success",
        "checks": checks,
        "user": user_login,
        "repo": repo,
        "default_branch": default_branch,
        "private": private,
        "permissions": permissions,
        "path": path,
        "path_kind": path_kind,
        "path_listing": path_listing,
        "error": None,
    }


# --------------------------------------------------------------------------- #
# stdlib HTTP helpers — keeps this script zero-dependency.                    #
# --------------------------------------------------------------------------- #

def _http_get_json(
    url: str, headers: dict[str, str], timeout_secs: int
) -> dict[str, Any]:
    """GET url with headers, return {ok, status_code, json, detail}."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
            raw = resp.read()
            try:
                body = json.loads(raw)
            except json.JSONDecodeError as e:
                return {
                    "ok": False,
                    "status_code": resp.status,
                    "json": None,
                    "detail": f"non-JSON response: {e}",
                }
            return {
                "ok": True,
                "status_code": resp.status,
                "json": body,
                "detail": None,
            }
    except urllib.error.HTTPError as e:
        msg = _extract_error_message(e)
        return {
            "ok": False,
            "status_code": e.code,
            "json": None,
            "detail": f"HTTP {e.code}: {msg}",
        }
    except urllib.error.URLError as e:
        return {
            "ok": False,
            "status_code": None,
            "json": None,
            "detail": f"connection failed: {e.reason}",
        }
    except (TimeoutError, OSError) as e:
        return {
            "ok": False,
            "status_code": None,
            "json": None,
            "detail": f"{type(e).__name__}: {e}",
        }


def _extract_error_message(err: urllib.error.HTTPError) -> str:
    try:
        body = json.loads(err.read())
        if isinstance(body, dict):
            return str(body.get("message") or body)[:300]
    except (json.JSONDecodeError, ValueError, OSError):
        pass
    return err.reason or "(no body)"


def _err(
    repo: str,
    error: str,
    checks: list[dict[str, Any]] | None = None,
    user: str | None = None,
) -> dict[str, Any]:
    return {
        "status": "error",
        "checks": checks or [],
        "user": user,
        "repo": repo,
        "default_branch": None,
        "private": None,
        "permissions": None,
        "path": None,
        "path_kind": None,
        "path_listing": None,
        "error": error,
    }


# --------------------------------------------------------------------------- #
# CLI — gone when we wrap the function as an ADK tool.                        #
# --------------------------------------------------------------------------- #

def _format_summary(result: dict[str, Any], repo: str, base_url: str) -> str:
    lines: list[str] = []
    lines.append(f"Base URL:       {base_url}")
    lines.append(f"Repo:           {repo}")
    lines.append(f"Authenticated:  {result.get('user') or '(unknown)'}")
    if result.get("default_branch"):
        lines.append(f"Default branch: {result['default_branch']}")
    if result.get("permissions"):
        perms = result["permissions"]
        roles = ", ".join(k for k, v in perms.items() if v)
        lines.append(f"Permissions:    {roles or '(none)'}")
    lines.append("")
    lines.append("Checks:")
    for c in result.get("checks", []):
        status = c.get("status", "?").upper()
        marker = "[OK]  " if status == "OK" else ("[WARN]" if status == "WARN" else "[FAIL]")
        name = c.get("name", "?")
        extra = ", ".join(
            f"{k}={v}" for k, v in c.items() if k not in {"name", "status"} and v is not None
        )
        lines.append(f"  {marker} {name:<6} {extra}")

    if result["status"] == "success":
        if result.get("path_kind") == "directory":
            entries = result.get("path_listing") or []
            preview = ", ".join(e["name"] for e in entries[:8]) or "(empty)"
            more = "" if len(entries) <= 8 else f" (+{len(entries) - 8} more)"
            lines.append("")
            lines.append(f"Path '{result['path']}' contains {len(entries)} entries: {preview}{more}")
        elif result.get("path_kind") == "file":
            entry = (result.get("path_listing") or [{}])[0]
            lines.append("")
            lines.append(f"Path '{result['path']}' is a file ({entry.get('size')} bytes).")
        lines.append("")
        lines.append("ALL CHECKS PASSED — GitHub Enterprise access is working.")
    else:
        lines.append("")
        lines.append(f"FAILED: {result['error']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_github_access",
        description="Verify PAT access to a private GitHub Enterprise repo.",
    )
    parser.add_argument("repo", help="owner/name, e.g. amex-eng/looker-project")
    parser.add_argument(
        "--path",
        help="Optional path inside the repo to probe (file or directory).",
    )
    parser.add_argument(
        "--ref",
        default=None,
        help="Branch / tag / SHA for the --path probe. Defaults to repo's default branch.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"GHE API root. Default: {DEFAULT_BASE_URL}",
    )
    parser.add_argument(
        "--token-env",
        default=DEFAULT_TOKEN_ENV,
        help=f"Env var holding the PAT. Default: {DEFAULT_TOKEN_ENV} (falls back to GITHUB_TOKEN).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full result as JSON (machine-readable).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECS,
        help=f"HTTP timeout per request (seconds). Default: {DEFAULT_TIMEOUT_SECS}",
    )
    args = parser.parse_args(argv)

    token = os.environ.get(args.token_env, "").strip()
    if not token:
        token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        print(
            f"ERROR: no token in ${args.token_env} or $GITHUB_TOKEN.\n"
            f"Set one with:\n"
            f"  export {args.token_env}='ghp_xxx...'",
            file=sys.stderr,
        )
        return 2

    # Accept URL / SSH / short forms. If the user pasted a URL, we derive the
    # API base from it — but only override the default if --base-url wasn't
    # explicitly set on the command line.
    repo_normalized, derived_base = normalize_repo(args.repo)
    base_url = args.base_url
    user_passed_base = "--base-url" in (argv if argv is not None else sys.argv[1:])
    if derived_base and not user_passed_base:
        base_url = derived_base

    result = check_github_access(
        repo=repo_normalized,
        token=token,
        base_url=base_url,
        path=args.path,
        ref=args.ref,
        timeout_secs=args.timeout,
    )

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(_format_summary(result, repo=repo_normalized, base_url=base_url))

    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
