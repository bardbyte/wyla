"""Every laptop.py path flag resolves from the environment/.env
(paste once, run anywhere), flags still win, and a missing required
path names BOTH spellings in the error."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"


def _run(args, env):
    return subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"), *args],
        env=env, capture_output=True, text=True, cwd=SILO)


def test_census_paths_resolve_from_env(tmp_path):
    env = dict(os.environ)
    env["MERIDIAN_SOURCES_DIR"] = str(FX / "sources")
    env["MERIDIAN_REGISTRY"] = str(FX / "sources"
                                   / "tables_registry.txt")
    result = _run(["census", "--out", str(tmp_path / "run"),
                   "--plain"], env)
    # census may exit 1 on fixture data gates; env resolution is
    # proven by getting PAST argparse and producing the run outputs
    assert result.returncode != 2, result.stderr[-500:]
    assert (tmp_path / "run" / "events.jsonl").exists()


def test_missing_path_names_the_env_var(tmp_path):
    """No flag, no env, an EMPTY .env pinned so a real laptop .env
    cannot leak in: the error teaches both spellings."""
    empty = tmp_path / "empty.env"
    empty.write_text("", encoding="utf-8")
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("MERIDIAN_")}
    env["SAHS_ENV_FILE"] = str(empty)
    result = _run(["census", "--out", str(tmp_path / "run"),
                   "--plain"], env)
    assert result.returncode != 0
    assert "MERIDIAN_SOURCES_DIR" in result.stderr
    assert "--sources-dir" in result.stderr


def test_flag_beats_env(tmp_path):
    """A flag always wins over the environment value."""
    env = dict(os.environ)
    env["MERIDIAN_SOURCES_DIR"] = str(tmp_path / "wrong")
    env["MERIDIAN_REGISTRY"] = str(FX / "sources"
                                   / "tables_registry.txt")
    result = _run(["census", "--sources-dir", str(FX / "sources"),
                   "--out", str(tmp_path / "run"), "--plain"], env)
    # the wrong env dir would die on a missing path; the flag won
    assert result.returncode != 2, result.stderr[-500:]
    assert (tmp_path / "run" / "events.jsonl").exists()
    assert "wrong" not in result.stdout + result.stderr
