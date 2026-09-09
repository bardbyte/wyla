"""Minimal .env loader — KEY=VALUE per line, `#` comments tolerated.

Why minimal:
- We don't want a hard dependency on python-dotenv for one feature.
- Env vars already set in the shell ALWAYS win over .env values, so
  the file is a default-set, not an override.
- Single quotes / double quotes around the value get stripped so users
  can write either ``KEY=value`` or ``KEY="value"`` interchangeably.

Search order (first hit wins per key):
    1. synapse/.env          (project-scoped overrides)
    2. <repo_root>/.env      (whole-repo defaults)

Both files are gitignored. There is no encryption layer here — secrets
go in your local .env which never leaves your machine.
"""

from __future__ import annotations

import os
from pathlib import Path


def _parse_line(line: str) -> tuple[str, str] | None:
    """Parse one KEY=VALUE line. Returns None for comments / blanks."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    # Allow `export KEY=VALUE` shell-style lines
    if line.startswith("export "):
        line = line[len("export "):].lstrip()
    if "=" not in line:
        return None
    k, v = line.split("=", 1)
    k = k.strip()
    if not k:
        return None
    v = v.strip()
    # Strip surrounding quotes (single or double); leave inner alone
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        v = v[1:-1]
    return k, v


def load_dotenv_file(path: Path, *, override: bool = False) -> dict[str, str]:
    """Parse one .env file and load into os.environ.

    Returns the dict of {key: value} actually applied. By default does
    NOT overwrite existing env vars (env-set wins). Pass override=True
    to flip that.
    """
    applied: dict[str, str] = {}
    if not path.exists():
        return applied
    text = path.read_text(encoding="utf-8")
    for raw_line in text.splitlines():
        parsed = _parse_line(raw_line)
        if parsed is None:
            continue
        k, v = parsed
        if not override and k in os.environ:
            continue
        os.environ[k] = v
        applied[k] = v
    return applied


def load_dotenv_chain(*, override: bool = False) -> dict[str, Path]:
    """Walk the standard .env search order, loading each that exists.

    Returns {applied_key: path_loaded_from} for diagnostics.
    """
    # synapse/ is two levels above this file: utils/ → synapse/ → repo root
    here = Path(__file__).resolve()
    synapse_root = here.parents[2]
    repo_root = synapse_root.parent

    search = [synapse_root / ".env", repo_root / ".env"]
    seen: dict[str, Path] = {}
    for p in search:
        applied = load_dotenv_file(p, override=override)
        for k in applied:
            seen.setdefault(k, p)
    return seen
