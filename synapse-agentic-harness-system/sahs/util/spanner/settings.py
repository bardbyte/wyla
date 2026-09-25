"""Validated settings for Spanner persistence and authentication."""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlparse

from sahs.util.network import spanner_endpoint_for_env, spanner_proxies


class SpannerConfigurationError(ValueError):
    """Raised when Spanner persistence is selected without its settings."""


class GoogleOAuthConfigurationError(ValueError):
    """Raised when user-delegated Google OAuth is incompletely configured."""


def _settings_values(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    if environ is not None:
        return environ
    values = dict(os.environ)
    try:
        from config.settings import Settings
        configured = Settings.load().as_environment()
    except ImportError:
        return values
    return {**configured, **{
        name: value for name, value in values.items() if value.strip()}}


@dataclass(frozen=True, slots=True)
class AuthSettings:
    """Authentication policy for the Spanner-backed application."""

    pepper: str
    session_hours: int
    idle_minutes: int
    cookie_secure: str
    bootstrap_admin_email: str
    open_signup: bool
    allowed_email_domains: tuple[str, ...]
    default_role: str
    allow_insecure_direct_reset: bool

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> AuthSettings:
        values = _settings_values(environ)
        pepper = values.get("AUTH_PEPPER", "").strip()
        if len(pepper) < 8:
            raise SpannerConfigurationError(
                "SAHS_STORE=spanner requires AUTH_PEPPER with at least 8 characters")
        cookie_secure = values.get("AUTH_COOKIE_SECURE", "auto").strip().lower()
        if cookie_secure not in ("auto", "true", "false"):
            raise SpannerConfigurationError(
                "AUTH_COOKIE_SECURE is auto, true, or false")
        try:
            session_hours = int(values.get("AUTH_SESSION_HOURS", "12"))
            idle_minutes = int(values.get("AUTH_IDLE_MINUTES", "30"))
        except ValueError as exc:
            raise SpannerConfigurationError(
                "AUTH_SESSION_HOURS and AUTH_IDLE_MINUTES must be integers") from exc
        if session_hours < 1 or idle_minutes < 1:
            raise SpannerConfigurationError(
                "AUTH_SESSION_HOURS and AUTH_IDLE_MINUTES must be positive")
        direct_reset = values.get(
            "AUTH_ALLOW_INSECURE_DIRECT_RESET", "0").strip() == "1"
        from sahs.util.network import epaas_env
        if direct_reset and epaas_env(dict(values)) in {"e1", "e2", "e3"}:
            raise SpannerConfigurationError(
                "AUTH_ALLOW_INSECURE_DIRECT_RESET is forbidden in deployed environments")
        domains = tuple(
            domain.strip().lower().lstrip("@")
            for domain in values.get("AUTH_ALLOWED_EMAIL_DOMAINS", "").split(",")
            if domain.strip())
        return cls(
            pepper=pepper,
            session_hours=session_hours,
            idle_minutes=idle_minutes,
            cookie_secure=cookie_secure,
            bootstrap_admin_email=values.get(
                "AUTH_BOOTSTRAP_ADMIN_EMAIL", "").strip().lower(),
            open_signup=values.get("AUTH_OPEN_SIGNUP", "1").strip() == "1",
            allowed_email_domains=domains,
            default_role=values.get("AUTH_DEFAULT_ROLE", "analyst").strip(),
            allow_insecure_direct_reset=direct_reset,
        )


@dataclass(frozen=True, slots=True)
class GoogleOAuthSettings:
    """Configuration for the ESL-to-Google user consent flow."""

    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: tuple[str, ...]
    token_encryption_key: str = ""
    post_connect_uri: str = "/synapse-admin/#/account?google=connected"

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None,
    ) -> GoogleOAuthSettings:
        values = _settings_values(environ)
        required = {
            "GOOGLE_OAUTH_CLIENT_ID": values.get(
                "GOOGLE_OAUTH_CLIENT_ID", "").strip(),
            "GOOGLE_OAUTH_CLIENT_SECRET": values.get(
                "GOOGLE_OAUTH_CLIENT_SECRET", "").strip(),
            "GOOGLE_OAUTH_REDIRECT_URI": values.get(
                "GOOGLE_OAUTH_REDIRECT_URI", "").strip(),
            "GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY": values.get(
                "GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY", "").strip(),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise GoogleOAuthConfigurationError(
                "Google OAuth requires " + ", ".join(missing))
        redirect_uri = urlparse(required["GOOGLE_OAUTH_REDIRECT_URI"])
        from sahs.util.network import epaas_env
        if (epaas_env(dict(values)) in {"e1", "e2", "e3"}
                and redirect_uri.hostname in {"localhost", "127.0.0.1", "::1"}):
            raise GoogleOAuthConfigurationError(
                "GOOGLE_OAUTH_REDIRECT_URI must use the deployed callback host "
                "outside local development")
        return cls(
            client_id=required["GOOGLE_OAUTH_CLIENT_ID"],
            client_secret=required["GOOGLE_OAUTH_CLIENT_SECRET"],
            redirect_uri=redirect_uri.geturl(),
            scopes=(
                "openid",
                "email",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/bigquery",
            ),
            token_encryption_key=required["GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY"],
            post_connect_uri=values.get(
                "GOOGLE_OAUTH_POST_CONNECT_URI",
                "/synapse-admin/#/account?google=connected").strip()
            or "/synapse-admin/#/account?google=connected",
        )


@dataclass(frozen=True, slots=True)
class SpannerSettings:
    """Connection settings loaded when ``SAHS_STORE=spanner``."""

    project_id: str
    instance_id: str
    database_id: str
    credentials_path: Path | None
    emulator_host: str | None
    endpoint: str
    proxies: dict[str, str]
    owner_user_id: str = ""
    credentials_data: dict[str, object] | None = None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> SpannerSettings:
        if environ is None:
            from sahs.util.auth import load_dotenv
            load_dotenv(override=True)
        values = _settings_values(environ)
        required = {
            "SPANNER_PROJECT_ID": values.get("SPANNER_PROJECT_ID", "").strip(),
            "SPANNER_INSTANCE_ID": values.get("SPANNER_INSTANCE_ID", "").strip(),
            "SPANNER_DATABASE_ID": values.get("SPANNER_DATABASE_ID", "").strip(),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise SpannerConfigurationError(
                "SAHS_STORE=spanner requires " + ", ".join(missing))

        emulator_host = values.get("SPANNER_EMULATOR_HOST", "").strip() or None
        credential_values = [
            values.get("SYNAPSE_SPANNER_SA_KEY", "").strip(),
            values.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip(),
        ]
        endpoint = (values.get("SPANNER_API_BASE_URL", "").strip()
                    or values.get("SPANNER_URL", "").strip())
        if not endpoint and not emulator_host:
            endpoint = spanner_endpoint_for_env(dict(values))
        if not endpoint and not emulator_host:
            endpoint = "https://spanner.googleapis.com"

        credentials_data = None
        credentials_path = None
        first_path = None
        for credential_value in credential_values:
            if not credential_value:
                continue
            if credential_value.startswith("data:application/json;base64,"):
                credentials_data = json.loads(base64.b64decode(
                    credential_value.split(",", 1)[1]))
                break
            if credential_value.startswith("{"):
                credentials_data = json.loads(credential_value)
                break
            try:
                document = json.loads(base64.b64decode(
                    credential_value, validate=True))
            except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
                document = None
            if isinstance(document, dict):
                credentials_data = document
                break
            path = Path(credential_value).expanduser()
            first_path = first_path or path
            if path.is_file():
                credentials_path = path
                break
        else:
            if first_path is not None:
                raise SpannerConfigurationError(
                    f"Spanner service-account key not found: {first_path}")

        return cls(
            project_id=required["SPANNER_PROJECT_ID"],
            instance_id=required["SPANNER_INSTANCE_ID"],
            database_id=required["SPANNER_DATABASE_ID"],
            credentials_path=credentials_path,
            emulator_host=emulator_host,
            endpoint=endpoint.rstrip("/"),
            proxies=spanner_proxies(dict(values)),
            owner_user_id=values.get("SPANNER_OWNER_USER_ID", "").strip(),
            credentials_data=credentials_data,
        )


def spanner_is_enabled(environ: Mapping[str, str] | None = None) -> bool:
    if environ is None:
        from sahs.util.auth import load_dotenv
        load_dotenv()
    values = _settings_values(environ)
    return values.get("SAHS_STORE", "local").strip().lower() == "spanner"


def grpc_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    return parsed.netloc or parsed.path


__all__ = [
    "AuthSettings",
    "GoogleOAuthConfigurationError",
    "GoogleOAuthSettings",
    "SpannerConfigurationError",
    "SpannerSettings",
    "grpc_endpoint",
    "spanner_is_enabled",
]
