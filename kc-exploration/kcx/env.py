"""Configuration: the ``.env`` files, the variables, the small helpers.

Nothing here touches the network. Precedence everywhere is a shell-exported
variable > this folder's ``.env`` > ``KC_EXTRA_ENV_FILE`` > the default in
code: a file never overrides a variable already in the process, and a
missing file loads nothing rather than falling back to another file, so
what this folder reads is always the two paths it was told about.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

PACKAGE_DIR = Path(__file__).resolve().parent
ROOT_DIR = PACKAGE_DIR.parent

# the exit codes the scripts share: runbooks branch on codes, not on grep
EXIT_OK = 0          # connected, answered
EXIT_REFUSED = 1     # the service answered and said no
EXIT_CONFIG = 3      # an environment or auth problem: fix the .env, not the code


class ConfigError(RuntimeError):
    """Missing or invalid configuration; maps to EXIT_CONFIG."""


def dotenv_value(raw: str) -> str:
    """One value as the file means it: quotes removed when the value is
    quoted, otherwise an inline comment (" #…") dropped and the ends
    trimmed, so a path followed by a note stays a path."""
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    cut = value.find(" #")
    if cut >= 0:
        value = value[:cut]
    return value.strip()


def load_dotenv(path: Path | str) -> list[str]:
    """Read ONE ``.env`` file into ``os.environ``. Never overrides a
    variable already set: the shell wins, and so does a file read
    earlier. A missing file loads nothing. Returns the names loaded."""
    path = Path(path).expanduser()
    if not path.is_file():
        return []
    loaded: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().removeprefix("export ").strip()
        value = dotenv_value(value)
        if key and key not in os.environ:
            os.environ[key] = value
            loaded.append(key)
    return loaded


def _resolve(value: str, root: Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (root / path).resolve()


def load_env(root: Path | None = None) -> dict[str, Any]:
    """This folder's ``.env`` first (``KC_ENV_FILE`` names another), then
    ``KC_EXTRA_ENV_FILE`` if set, even when the first file is what set
    it. Relative paths resolve against the folder, so a value like
    ``../other-project/.env`` works from any working directory. Returns
    what was read from where, for the check scripts to print."""
    root = Path(root) if root else ROOT_DIR
    kc_path = _resolve(os.environ.get("KC_ENV_FILE") or ".env", root)
    report: dict[str, Any] = {
        "kc": {"path": str(kc_path), "found": kc_path.is_file(),
               "loaded": load_dotenv(kc_path)},
        "extra": None,
    }
    extra = first_env("KC_EXTRA_ENV_FILE")
    if extra:
        extra_path = _resolve(extra, root)
        report["extra"] = {"path": str(extra_path),
                           "found": extra_path.is_file(),
                           "loaded": load_dotenv(extra_path)}
    return report


def first_env(*names: str) -> str | None:
    """The first of the named variables that is set and not blank."""
    for name in names:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


def env_flag(name: str, default: bool = False) -> bool:
    value = (os.environ.get(name) or "").strip().lower()
    if not value:
        return default
    return value in ("1", "true", "yes", "on")


def env_int(name: str, default: int) -> int:
    value = (os.environ.get(name) or "").strip()
    try:
        return int(value) if value else default
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    value = (os.environ.get(name) or "").strip()
    try:
        return float(value) if value else default
    except ValueError:
        return default


def env_proxies() -> dict[str, str]:
    """The proxy as the environment declares it (HTTPS_PROXY / HTTP_PROXY),
    for a connection that rides it. Read, never written."""
    out: dict[str, str] = {}
    https = first_env("HTTPS_PROXY", "https_proxy")
    http = first_env("HTTP_PROXY", "http_proxy")
    if https:
        out["https"] = https
    if http:
        out["http"] = http
    return out


def google_proxies(disable_var: str) -> dict[str, str]:
    """A Google host's route: via the proxy the environment declares,
    unless ``<disable_var>=1`` says direct. NO_PROXY is never consulted:
    every host on these planes is a Google host, so "direct for Google"
    is direct, and a bypass list can never reroute another plane."""
    if env_flag(disable_var):
        return {}
    return env_proxies()


def redact_url(url: str) -> str:
    """Strip embedded credentials from a URL for display: ``user:pass@host``
    is common in proxy values and must never reach a log or a screen."""
    url = (url or "").strip()
    if "@" not in url:
        return url
    scheme, sep, rest = url.partition("://")
    host = rest.rsplit("@", 1)[-1] if sep else url.rsplit("@", 1)[-1]
    return f"{scheme}://{host}" if sep else host


def describe_route(proxies: dict[str, str]) -> str:
    """'direct' or 'via <proxy>' (credentials redacted), for display."""
    proxy = proxies.get("https") or proxies.get("http") or ""
    return f"via {redact_url(proxy)}" if proxy else "direct"
