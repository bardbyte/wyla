"""Settings for the identity service and the Google connection.

The enterprise branch's module, ``sahs/util/spanner/settings.py``, is
the base and lands in this repository as written. This facade is what
the app imports (``from sahs.spanner import …``): the same names, their
validation, plus what this repository runs that the branch does not
yet know:

    SAHS_STORE=sqlite       a local file with the same tables, for a
                            laptop and the tests (their switch knows
                            ``local`` and ``spanner``)
    AUTH_LOCK_AFTER,        the lockout dials the identity store reads
    AUTH_LOCK_MINUTES
    AUTH_LOCAL_LOGIN        the email-and-password path, off by default;
                            the front door is Okta
    defaults on every field, so a test names only what it cares about

Their rules hold where a store runs: a pepper of eight characters or
more, the cookie flag one of auto | true | false, the direct-reset and
localhost-callback refusals in a deployed environment. Under
``SAHS_STORE=local`` no store runs and nothing here guards anything,
so ``AuthSettings.from_env`` reads leniently and the routes that only
ask "is the local form open here" can answer on a laptop with nothing
configured.

One deviation, deliberate: their loader re-reads the silo ``.env`` with
``override=True`` (the file beats the shell). The harness convention is
the opposite, so this facade resolves the environment once, shell
first, and hands their readers the resolved mapping.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from sahs.util.spanner import settings as _theirs
from sahs.util.spanner.settings import (GoogleOAuthConfigurationError,
                                        SpannerConfigurationError)

DEFAULT_SPANNER_ENDPOINT = "https://spanner.googleapis.com"
# fixed in their reader: the consent flow needs exactly these
DEFAULT_GOOGLE_SCOPES = (
    "openid",
    "email",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/bigquery",
)
STORE_MODES = ("local", "spanner", "sqlite")


def _values(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    """The environment as one mapping: the silo ``.env`` (shell wins),
    then their merge with ``config.settings`` when that package exists."""
    if environ is None:
        try:
            from sahs.util.auth import load_dotenv
            load_dotenv()
        except Exception:  # noqa: BLE001 - the .env is a convenience, never a requirement
            pass
    return _theirs._settings_values(environ)


def _value(env: Mapping[str, str], *names: str, default: str = "") -> str:
    for name in names:
        raw = env.get(name)
        if raw is not None and str(raw).strip():
            return str(raw).strip().strip("'\"")
    return default


def _flag(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = _value(env, name)
    if not raw:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = _value(env, name)
    try:
        return int(raw) if raw else default
    except ValueError as exc:
        raise SpannerConfigurationError(f"{name} must be an integer, not {raw!r}") from exc


def _copy(base: Any) -> dict[str, Any]:
    return {f.name: getattr(base, f.name) for f in fields(base)}


def store_mode(environ: Mapping[str, str] | None = None) -> str:
    """``local`` | ``spanner`` | ``sqlite``, from SAHS_STORE."""
    mode = _value(_values(environ), "SAHS_STORE", default="local").lower()
    if mode not in STORE_MODES:
        raise SpannerConfigurationError(
            f"SAHS_STORE is local, spanner or sqlite, not {mode!r}")
    return mode


def spanner_is_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Whether an identity store is configured at all (Spanner or the
    local sqlite stand-in). False means the single-developer mode."""
    return store_mode(environ) != "local"


def grpc_endpoint(endpoint: str) -> str:
    """``https://host[:port]/…`` → ``host:port`` for the SDK's api_endpoint;
    a bare ``host:port`` (the emulator) passes through unchanged. Theirs
    returns the host alone; the SDK accepts both, the tests pin this one."""
    value = (endpoint or "").strip()
    if not value:
        return "spanner.googleapis.com:443"
    if "://" not in value:
        return value
    parts = urlsplit(value)
    host = parts.hostname or ""
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return f"{host}:{port}"


@dataclass(frozen=True)
class SpannerSettings:
    """Their fields, their reader; plus the sqlite stand-in."""
    project_id: str = "local"
    instance_id: str = "local"
    database_id: str = "local"
    credentials_path: Path | None = None
    emulator_host: str | None = None
    endpoint: str = DEFAULT_SPANNER_ENDPOINT
    proxies: dict[str, str] = field(default_factory=dict)
    owner_user_id: str = ""
    credentials_data: dict[str, Any] | None = None
    # sqlite stand-in (SAHS_STORE=sqlite): the file that plays the database
    sqlite_path: str = ""
    mode: str = "spanner"

    @property
    def database_path(self) -> str:
        return (f"projects/{self.project_id}/instances/{self.instance_id}"
                f"/databases/{self.database_id}")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SpannerSettings":
        env = _values(environ)
        mode = store_mode(env)
        if mode == "local":
            raise SpannerConfigurationError(
                "the identity store is off (SAHS_STORE=local); set "
                "SAHS_STORE=spanner or SAHS_STORE=sqlite")
        if mode == "sqlite":
            path = _value(env, "SAHS_IDENTITY_SQLITE",
                          default=str(Path("graph") / "runs" / "identity.sqlite3"))
            return cls(sqlite_path=path, mode="sqlite", endpoint="")
        base = _theirs.SpannerSettings.from_env(env)
        return cls(**_copy(base), mode="spanner")


@dataclass(frozen=True)
class AuthSettings:
    """Their fields, their reader where a store runs; plus the lockout
    dials and the local-login flag."""
    pepper: str = ""
    session_hours: int = 12
    idle_minutes: int = 30
    cookie_secure: str = "auto"              # auto | true | false
    bootstrap_admin_email: str = ""
    open_signup: bool = True
    allowed_email_domains: tuple[str, ...] = ()
    default_role: str = "analyst"
    allow_insecure_direct_reset: bool = False
    lock_after: int = 5
    lock_minutes: int = 15
    # the email-and-password path (sign-up, login, reset): the enterprise
    # front door is Okta, so it is off unless a deployment turns it on
    local_login_enabled: bool = False

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "AuthSettings":
        env = _values(environ)
        ours = dict(lock_after=_int(env, "AUTH_LOCK_AFTER", 5),
                    lock_minutes=_int(env, "AUTH_LOCK_MINUTES", 15),
                    local_login_enabled=_flag(env, "AUTH_LOCAL_LOGIN", False))
        if store_mode(env) != "local":
            base = _theirs.AuthSettings.from_env(env)
            data = _copy(base)
            data["default_role"] = data["default_role"].lower()
            return cls(**data, **ours)
        # no store runs: read what is there, demand nothing
        secure = _value(env, "AUTH_COOKIE_SECURE", default="auto").lower()
        if secure not in ("auto", "true", "false"):
            raise SpannerConfigurationError(
                f"AUTH_COOKIE_SECURE is auto, true or false, not {secure!r}")
        domains = tuple(d.strip().lower().lstrip("@")
                        for d in _value(env, "AUTH_ALLOWED_EMAIL_DOMAINS").split(",")
                        if d.strip())
        return cls(
            pepper=_value(env, "AUTH_PEPPER"),
            session_hours=_int(env, "AUTH_SESSION_HOURS", 12),
            idle_minutes=_int(env, "AUTH_IDLE_MINUTES", 30),
            cookie_secure=secure,
            bootstrap_admin_email=_value(env, "AUTH_BOOTSTRAP_ADMIN_EMAIL").lower(),
            open_signup=_flag(env, "AUTH_OPEN_SIGNUP", True),
            allowed_email_domains=domains,
            default_role=_value(env, "AUTH_DEFAULT_ROLE", default="analyst").lower(),
            allow_insecure_direct_reset=_flag(env, "AUTH_ALLOW_INSECURE_DIRECT_RESET", False),
            **ours)


@dataclass(frozen=True)
class GoogleOAuthSettings:
    """Their fields, their reader (the scopes are fixed there)."""
    client_id: str = ""
    client_secret: str = ""
    redirect_uri: str = ""
    scopes: tuple[str, ...] = DEFAULT_GOOGLE_SCOPES
    token_encryption_key: str = ""
    post_connect_uri: str = "/synapse-admin/#/account?google=connected"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "GoogleOAuthSettings":
        return cls(**_copy(_theirs.GoogleOAuthSettings.from_env(_values(environ))))


__all__ = ["AuthSettings", "DEFAULT_GOOGLE_SCOPES", "DEFAULT_SPANNER_ENDPOINT",
           "GoogleOAuthConfigurationError", "GoogleOAuthSettings",
           "STORE_MODES", "SpannerConfigurationError", "SpannerSettings",
           "grpc_endpoint", "spanner_is_enabled", "store_mode"]
