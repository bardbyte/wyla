"""Settings for the identity service and the Google OAuth connection.

Three settings objects, each read once from the environment (the
``.env`` the harness already loads) and each refusing to start with a
clear message when a required value is missing:

    SpannerSettings       the Cloud Spanner database that holds people,
                          sessions, roles and the audit
    AuthSettings          how sign-in behaves: session life, lockout,
                          cookie flags, who may sign up
    GoogleOAuthSettings   the OAuth client a person connects for
                          user-delegated BigQuery, and the key that
                          protects their refresh token at rest

``spanner_is_enabled()`` is the one switch the app consults before it
looks for a signed-in person: ``SAHS_STORE=spanner`` (the database) or
``SAHS_STORE=sqlite`` (a local file, for development and tests). Under
``local`` the app runs as it always did, for one developer, no sign-in.

The variable names follow the ``.env.example`` block the schema shipped
with (``SPANNER_*``, ``AUTH_*``); the Google OAuth ones are new.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

DEFAULT_SPANNER_ENDPOINT = "https://spanner.googleapis.com"
DEFAULT_GOOGLE_SCOPES = (
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/bigquery",
)
STORE_MODES = ("local", "spanner", "sqlite")


class SpannerConfigurationError(RuntimeError):
    """The identity store cannot start: a required setting is missing."""


class GoogleOAuthConfigurationError(RuntimeError):
    """The Google OAuth connection cannot start: a required setting is missing."""


def _environ(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    if environ is not None:
        return environ
    try:
        from sahs.util.auth import load_dotenv
        load_dotenv()
    except Exception:  # noqa: BLE001 - the .env is a convenience, never a requirement
        pass
    return os.environ


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


def store_mode(environ: Mapping[str, str] | None = None) -> str:
    """``local`` | ``spanner`` | ``sqlite``, from SAHS_STORE."""
    mode = _value(_environ(environ), "SAHS_STORE", default="local").lower()
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
    a bare ``host:port`` (the emulator) passes through unchanged."""
    value = (endpoint or "").strip()
    if not value:
        return "spanner.googleapis.com:443"
    if "://" not in value:
        return value
    parts = urlsplit(value)
    host = parts.hostname or ""
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return f"{host}:{port}"


def _credentials(raw: str) -> tuple[str | None, dict[str, Any] | None]:
    """A service-account setting is a file path, inline JSON, or base64
    JSON (a data: URI or bare). → (path, parsed document)."""
    value = (raw or "").strip()
    if not value:
        return None, None
    document: bytes | None = None
    if value.startswith("data:application/json;base64,"):
        try:
            document = base64.b64decode(
                value.removeprefix("data:application/json;base64,"), validate=True)
        except binascii.Error as exc:
            raise SpannerConfigurationError(
                "SYNAPSE_SPANNER_SA_KEY is not valid base64 JSON") from exc
    elif value.startswith("{"):
        document = value.encode("utf-8")
    else:
        try:
            decoded = base64.b64decode(value, validate=True)
            json.loads(decoded)
            document = decoded
        except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
            document = None
    if document is None:
        return value, None
    try:
        parsed = json.loads(document)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SpannerConfigurationError(
            "SYNAPSE_SPANNER_SA_KEY holds invalid service-account JSON") from exc
    if parsed.get("type") != "service_account":
        raise SpannerConfigurationError(
            "SYNAPSE_SPANNER_SA_KEY JSON is not a service account")
    return None, parsed


@dataclass(frozen=True)
class SpannerSettings:
    project_id: str
    instance_id: str
    database_id: str
    endpoint: str = DEFAULT_SPANNER_ENDPOINT
    credentials_path: str | None = None
    credentials_data: dict[str, Any] | None = None
    emulator_host: str = ""
    proxies: dict[str, str] = field(default_factory=dict)
    # sqlite stand-in (SAHS_STORE=sqlite): the file that plays the database
    sqlite_path: str = ""
    mode: str = "spanner"

    @property
    def database_path(self) -> str:
        return (f"projects/{self.project_id}/instances/{self.instance_id}"
                f"/databases/{self.database_id}")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SpannerSettings":
        env = _environ(environ)
        mode = store_mode(env)
        if mode == "local":
            raise SpannerConfigurationError(
                "the identity store is off (SAHS_STORE=local); set "
                "SAHS_STORE=spanner or SAHS_STORE=sqlite")
        if mode == "sqlite":
            path = _value(env, "SAHS_IDENTITY_SQLITE",
                          default=str(Path("graph") / "runs" / "identity.sqlite3"))
            return cls(project_id="local", instance_id="local", database_id="local",
                       sqlite_path=path, mode="sqlite")
        project = _value(env, "SPANNER_PROJECT_ID")
        instance = _value(env, "SPANNER_INSTANCE_ID")
        database = _value(env, "SPANNER_DATABASE_ID")
        missing = [name for name, value in (("SPANNER_PROJECT_ID", project),
                                            ("SPANNER_INSTANCE_ID", instance),
                                            ("SPANNER_DATABASE_ID", database)) if not value]
        if missing:
            raise SpannerConfigurationError(
                "SAHS_STORE=spanner needs " + ", ".join(missing) + " in the environment")
        emulator = _value(env, "SPANNER_EMULATOR_HOST")
        endpoint = _value(env, "SPANNER_URL")
        if not endpoint:
            try:
                from sahs.util.network import spanner_endpoint_for_env
                endpoint = spanner_endpoint_for_env(env)
            except Exception:  # noqa: BLE001
                endpoint = ""
        path, data = _credentials(_value(env, "SYNAPSE_SPANNER_SA_KEY",
                                         "SPANNER_CREDENTIALS"))
        if path is not None and not Path(path).expanduser().is_file():
            raise SpannerConfigurationError(
                f"SYNAPSE_SPANNER_SA_KEY names a file that does not exist: {path}")
        try:
            from sahs.util.network import spanner_proxies
            proxies = spanner_proxies(env)
        except Exception:  # noqa: BLE001
            proxies = {}
        return cls(project_id=project, instance_id=instance, database_id=database,
                   endpoint=emulator or endpoint or DEFAULT_SPANNER_ENDPOINT,
                   credentials_path=str(Path(path).expanduser()) if path else None,
                   credentials_data=data, emulator_host=emulator, proxies=proxies,
                   mode="spanner")


@dataclass(frozen=True)
class AuthSettings:
    pepper: str = ""
    session_hours: int = 12
    idle_minutes: int = 30
    cookie_secure: str = "auto"              # auto | true | false
    bootstrap_admin_email: str = ""
    open_signup: bool = True
    allowed_email_domains: tuple[str, ...] = ()
    default_role: str = "analyst"
    lock_after: int = 5
    lock_minutes: int = 15
    allow_insecure_direct_reset: bool = False
    # the email-and-password path (sign-up, login, reset): the enterprise
    # front door is Okta, so it is off unless a deployment turns it on
    local_login_enabled: bool = False

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "AuthSettings":
        env = _environ(environ)
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
            lock_after=_int(env, "AUTH_LOCK_AFTER", 5),
            lock_minutes=_int(env, "AUTH_LOCK_MINUTES", 15),
            allow_insecure_direct_reset=_flag(env, "AUTH_ALLOW_INSECURE_DIRECT_RESET", False),
            local_login_enabled=_flag(env, "AUTH_LOCAL_LOGIN", False),
        )


@dataclass(frozen=True)
class GoogleOAuthSettings:
    client_id: str
    client_secret: str
    redirect_uri: str
    token_encryption_key: str
    scopes: tuple[str, ...] = DEFAULT_GOOGLE_SCOPES
    post_connect_uri: str = "/#/account"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "GoogleOAuthSettings":
        env = _environ(environ)
        client_id = _value(env, "GOOGLE_OAUTH_CLIENT_ID")
        client_secret = _value(env, "GOOGLE_OAUTH_CLIENT_SECRET")
        redirect_uri = _value(env, "GOOGLE_OAUTH_REDIRECT_URI")
        key = _value(env, "GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY")
        missing = [name for name, value in (
            ("GOOGLE_OAUTH_CLIENT_ID", client_id),
            ("GOOGLE_OAUTH_CLIENT_SECRET", client_secret),
            ("GOOGLE_OAUTH_REDIRECT_URI", redirect_uri),
            ("GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY", key)) if not value]
        if missing:
            raise GoogleOAuthConfigurationError(
                "Google OAuth is not configured: set " + ", ".join(missing))
        scopes = tuple(s for s in _value(env, "GOOGLE_OAUTH_SCOPES").split() if s) \
            or DEFAULT_GOOGLE_SCOPES
        return cls(client_id=client_id, client_secret=client_secret,
                   redirect_uri=redirect_uri, token_encryption_key=key,
                   scopes=scopes,
                   post_connect_uri=_value(env, "GOOGLE_OAUTH_POST_CONNECT_URI",
                                           default="/#/account"))


__all__ = ["AuthSettings", "DEFAULT_GOOGLE_SCOPES", "GoogleOAuthConfigurationError",
           "GoogleOAuthSettings", "SpannerConfigurationError", "SpannerSettings",
           "grpc_endpoint", "spanner_is_enabled", "store_mode"]
