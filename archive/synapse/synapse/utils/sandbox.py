"""Python sandbox for agent-driven analysis — PoC isolation profile.

Runs model-authored analysis code in a separate interpreter with:
  * a scrubbed environment (no inherited credentials, tokens, or proxies)
  * an empty temp working directory (plus explicitly mounted input files)
  * python -I (isolated: no site-packages injection via env, no cwd import)
  * CPU / memory / output limits and a wall-clock timeout
  * no arguments passed through a shell

HONEST SCOPE: this is *containment against accidents*, not a security
boundary against a determined adversary — the child can still open
sockets and read world-readable paths. For production, swap `run_python`
for a Vertex AI Code Interpreter extension call or a gVisor/Firecracker
container with no network; the tool contract below stays identical.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

_MAX_OUTPUT_CHARS = 20_000
_DEFAULT_TIMEOUT_S = 30
_MEMORY_LIMIT_BYTES = 1_024 * 1_024 * 1_024  # 1 GiB
_CPU_LIMIT_SECONDS = 60

_SANDBOX_ENV = {
    "PATH": "/usr/local/bin:/usr/bin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONUNBUFFERED": "1",
    # deliberately ABSENT: GOOGLE_APPLICATION_CREDENTIALS, *_TOKEN, *_KEY,
    # HTTPS_PROXY, HOME, and everything else from the parent environment
}


def _rlimits() -> None:  # pragma: no cover — runs in the child process
    import resource

    resource.setrlimit(resource.RLIMIT_CPU,
                       (_CPU_LIMIT_SECONDS, _CPU_LIMIT_SECONDS))
    try:
        resource.setrlimit(resource.RLIMIT_AS,
                           (_MEMORY_LIMIT_BYTES, _MEMORY_LIMIT_BYTES))
    except (ValueError, OSError):
        pass  # some kernels disallow lowering AS; CPU+timeout still hold
    resource.setrlimit(resource.RLIMIT_NPROC, (64, 64))


def run_python(
    code: str,
    *,
    timeout_seconds: int = _DEFAULT_TIMEOUT_S,
    input_files: dict[str, str | Path] | None = None,
) -> dict[str, Any]:
    """Execute analysis code; return {status, stdout, stderr, artifacts}.

    input_files maps sandbox-relative names to host paths — the ONLY data
    the code can see besides what's inlined in `code`. Files the code
    writes into its working dir are returned (small text ones inline).
    """
    if not code or not code.strip():
        return {"status": "error", "error": "empty code", "stdout": "",
                "stderr": ""}
    workdir = Path(tempfile.mkdtemp(prefix="synapse_sbx_"))
    try:
        for name, host_path in (input_files or {}).items():
            safe_name = Path(name).name  # no path traversal out of workdir
            shutil.copyfile(Path(host_path), workdir / safe_name)
        script = workdir / "__analysis__.py"
        script.write_text(code, encoding="utf-8")
        try:
            proc = subprocess.run(
                [sys.executable, "-I", script.name],
                cwd=workdir,
                env=dict(_SANDBOX_ENV),
                capture_output=True,
                text=True,
                timeout=max(1, timeout_seconds),
                preexec_fn=_rlimits if sys.platform != "win32" else None,
            )
        except subprocess.TimeoutExpired:
            return {"status": "timeout", "stdout": "", "stderr":
                    f"exceeded {timeout_seconds}s wall clock", "artifacts": {}}
        artifacts: dict[str, str] = {}
        for produced in sorted(workdir.iterdir()):
            if produced.name in {script.name} or not produced.is_file():
                continue
            if (input_files or {}) and produced.name in {
                    Path(n).name for n in input_files}:
                continue
            try:
                text = produced.read_text(encoding="utf-8", errors="replace")
                artifacts[produced.name] = text[:_MAX_OUTPUT_CHARS]
            except (OSError, UnicodeError):
                artifacts[produced.name] = f"<binary, {produced.stat().st_size} bytes>"
        return {
            "status": "ok" if proc.returncode == 0 else "error",
            "returncode": proc.returncode,
            "stdout": proc.stdout[:_MAX_OUTPUT_CHARS],
            "stderr": proc.stderr[:_MAX_OUTPUT_CHARS],
            "artifacts": artifacts,
        }
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
