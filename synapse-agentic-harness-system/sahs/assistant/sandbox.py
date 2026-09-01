"""The ``python`` tool (Synapse v2 §4): a budgeted sandbox where the
actual analysis happens — decomposition, variance, cohorts, checks.

What "sandbox" honestly means here: a subprocess with isolated mode
(-I: no site, no user packages, no cwd on path), a scrubbed
environment (no credentials, no proxy config — the child gets PATH
and locale, nothing else), a private per-session workspace directory
as its only writable ground, a hard wall-clock timeout, and truncated
output. It is a working room, not a security boundary against a
hostile analyst — the analyst already has the laptop.

The ``meridian`` module is written into the workspace: read-only
access to the promoted build's indexes (metrics, bindings, tables,
schema, joins) plus ``rows(name)`` for query results the loop saved
as ``q<N>.json`` after each run_sql. numpy is available when the host
has it; the sandbox REPORTS what is importable rather than promising
pandas it does not have.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

TIMEOUT_SECONDS = 30.0
MAX_OUTPUT = 8000
MAX_CODE = 20000

_MERIDIAN_PY = '''"""meridian — read-only access to the promoted build.

metrics() / bindings() / tables_index() / joins() -> list[dict]
schema() -> {table: {column: type}}
rows(name) -> list[dict]   # a saved query result, e.g. rows("q1")
available() -> what you can import here
"""
import json, os
from pathlib import Path

_BUILD = Path(os.environ["MERIDIAN_BUILD_DIR"])
_WS = Path(__file__).resolve().parent

def _jsonl(p):
    if not p.exists(): return []
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]

def metrics(): return _jsonl(_BUILD / "indexes" / "metrics.jsonl")
def bindings(): return _jsonl(_BUILD / "indexes" / "bindings.jsonl")
def tables_index(): return _jsonl(_BUILD / "indexes" / "tables.jsonl")
def joins(): return _jsonl(_BUILD / "indexes" / "joins.jsonl")
def schema():
    p = _BUILD / "schema.json"
    return json.loads(p.read_text()) if p.exists() else {}

def rows(name):
    p = _WS / (name if name.endswith(".json") else name + ".json")
    if not p.exists():
        saved = sorted(q.stem for q in _WS.glob("q*.json"))
        raise FileNotFoundError(
            f"no saved result {name!r}; saved results here: "
            + (", ".join(saved) or "none yet - run_sql saves rows as q<N>"))
    return json.loads(p.read_text())

def available():
    out = {}
    for mod in ("numpy", "pandas", "matplotlib", "statistics", "math"):
        try:
            __import__(mod); out[mod] = True
        except ImportError:
            out[mod] = False
    return out
'''


def prepare_workspace(workspace: Path, build_root: Path) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    shim = workspace / "meridian.py"
    if not shim.exists():
        shim.write_text(_MERIDIAN_PY, encoding="utf-8")
    (workspace / ".build_dir").write_text(str(build_root),
                                          encoding="utf-8")


def save_rows(workspace: Path, name: str,
              rows: list[dict[str, Any]]) -> str:
    """The loop calls this after a row-returning run_sql, so the next
    ``python`` step can ``meridian.rows("q1")``."""
    workspace.mkdir(parents=True, exist_ok=True)
    path = workspace / f"{name}.json"
    path.write_text(json.dumps(rows, default=str), encoding="utf-8")
    return path.name


def run_python(code: str, workspace: Path, *,
               timeout: float = TIMEOUT_SECONDS) -> dict[str, Any]:
    if not (code or "").strip():
        return {"error": "no code",
                "hint": "write the analysis; import meridian for the "
                        "build's indexes and saved query rows"}
    if len(code) > MAX_CODE:
        return {"error": f"code over {MAX_CODE} chars",
                "hint": "split the analysis into steps; files persist "
                        "in the workspace between calls"}
    build_dir = (workspace / ".build_dir")
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONIOENCODING": "utf-8",
        "MERIDIAN_BUILD_DIR": build_dir.read_text(encoding="utf-8")
        if build_dir.exists() else "",
        "HOME": str(workspace),          # nothing reads ~ for creds
    }
    before = {p.name: p.stat().st_mtime_ns
              for p in workspace.iterdir() if p.is_file()}
    cell = workspace / "_cell.py"
    cell.write_text(code, encoding="utf-8")
    started = time.perf_counter()
    try:
        # -s (no user site) rather than -I: isolated mode would drop
        # the workspace from sys.path and take meridian.py with it;
        # the env is already scrubbed by hand above
        proc = subprocess.run(
            [sys.executable, "-s", str(cell)], cwd=workspace, env=env,
            capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"error": f"timed out after {timeout:.0f}s",
                "hint": "smaller step: the workspace keeps files "
                        "between calls, so compute in stages"}
    elapsed = round((time.perf_counter() - started) * 1000, 1)
    stdout = proc.stdout[-MAX_OUTPUT:]
    stderr = proc.stderr[-MAX_OUTPUT:]
    files = sorted(
        p.name for p in workspace.iterdir()
        if p.is_file() and not p.name.startswith((".", "_"))
        and p.name != "meridian.py"
        and p.stat().st_mtime_ns > before.get(p.name, -1))
    out: dict[str, Any] = {"ok": proc.returncode == 0,
                           "stdout": stdout, "elapsed_ms": elapsed}
    if stderr:
        out["stderr"] = stderr
    if proc.returncode != 0:
        out["hint"] = ("read the traceback; meridian.available() "
                       "lists what imports here (no pandas is "
                       "normal), meridian.rows lists saved results")
    if files:
        out["files"] = files
    return out
