"""Clone the LookML repo and parse view files via lkml. No regex, no LLM."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any

from lumi.tools.lookml_tools import parse_lookml_file

logger = logging.getLogger(__name__)


class GitError(Exception):
    """Subprocess git invocation failed."""


def clone_and_parse_views(
    repo: str,
    branch: str,
    model_file: str,
    view_files: list[str],
    clone_dir: str | Path = ".git_cache",
) -> dict[str, Any]:
    """Clone (or pull) the repo, then parse every specified view file.

    Args:
        repo: Git URL.
        branch: Branch to check out.
        model_file: Path within repo to .model.lkml (returned under "model_file_text").
        view_files: Paths within repo to each .view.lkml.
        clone_dir: Local destination.

    Returns:
        dict with keys:
          status: "success" | "error"
          parsed_views: dict[view_name, ParsedView]
          model_file_text: raw text of the model file (for context, not parsed)
          clone_dir: absolute path to the checkout
          error: str | None
    """
    clone_path = Path(clone_dir).resolve()
    try:
        _ensure_repo(repo, branch, clone_path)
    except GitError as e:
        return _err(str(e))

    parsed_views: dict[str, Any] = {}
    for rel in view_files:
        view_path = clone_path / rel
        if not view_path.exists():
            return _err(f"View file missing in repo: {rel}")
        result = parse_lookml_file(view_path)
        if result["status"] != "success":
            return _err(f"Parse failed for {rel}: {result['error']}")
        parsed_view = result["parsed_view"]
        parsed_views[parsed_view.view_name] = parsed_view

    model_path = clone_path / model_file
    model_text = model_path.read_text(encoding="utf-8") if model_path.exists() else ""

    logger.info("Parsed %d views from %s@%s", len(parsed_views), repo, branch)
    return {
        "status": "success",
        "parsed_views": parsed_views,
        "model_file_text": model_text,
        "clone_dir": str(clone_path),
        "error": None,
    }


def _ensure_repo(repo: str, branch: str, dest: Path) -> None:
    if (dest / ".git").exists():
        # Guard against a stale cache pointing at a different repo.
        existing_remote = _run_git(
            ["config", "--get", "remote.origin.url"], cwd=dest
        ).strip()
        if existing_remote != repo:
            raise GitError(
                f"{dest} has remote.origin.url='{existing_remote}', but config "
                f"wants '{repo}'. Delete the directory or change your config."
            )
        _run_git(["fetch", "--depth=1", "origin", branch], cwd=dest)
        _run_git(["checkout", branch], cwd=dest)
        _run_git(["reset", "--hard", f"origin/{branch}"], cwd=dest)
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    _run_git(
        ["clone", "--depth=1", "--branch", branch, repo, str(dest)],
        cwd=dest.parent,
    )


def _run_git(args: list[str], cwd: Path) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=True,
            timeout=300,
        )
    except FileNotFoundError as e:
        raise GitError("git executable not found on PATH") from e
    except subprocess.CalledProcessError as e:
        raise GitError(f"git {args[0]} failed: {e.stderr.strip() or e.stdout.strip()}") from e
    except subprocess.TimeoutExpired as e:
        raise GitError(f"git {args[0]} timed out") from e
    return result.stdout


def _err(msg: str) -> dict[str, Any]:
    logger.error(msg)
    return {
        "status": "error",
        "parsed_views": {},
        "model_file_text": "",
        "clone_dir": "",
        "error": msg,
    }
