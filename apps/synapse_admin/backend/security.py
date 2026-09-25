"""HTTP security policy for the Synapse admin service."""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass

from fastapi import Request, Response

CSRF_COOKIE = "synapse_csrf"
CSRF_HEADER = "x-csrf-token"
SESSION_COOKIE = "synapse_session"
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})
_UNAUTHENTICATED_PATHS = frozenset({
    "/api/auth/login",
    "/api/auth/signup",
    "/api/auth/forgot-password",
    "/api/auth/reset-password",
})


@dataclass(frozen=True, slots=True)
class AuditRequest:
    ip: str = ""
    user_agent: str = ""
    request_id: str = ""

    def as_kwargs(self) -> dict[str, str]:
        return {
            "ip": self.ip,
            "user_agent": self.user_agent,
            "request_id": self.request_id,
        }


def audit_request(request: Request | None) -> AuditRequest:
    if request is None:
        return AuditRequest()
    forwarded = request.headers.get("x-forwarded-for", "").partition(",")[0].strip()
    client_ip = request.client.host if request.client else ""
    return AuditRequest(
        ip=(forwarded or client_ip)[:45],
        user_agent=request.headers.get("user-agent", "")[:512],
        request_id=request.headers.get("x-request-id", "")[:64],
    )


def allowed_origins() -> list[str]:
    """Return explicitly configured cross-origin frontend origins."""
    return [
        origin.strip().rstrip("/")
        for origin in os.environ.get("SYNAPSE_ALLOWED_ORIGINS", "").split(",")
        if origin.strip()
    ]


def csrf_token() -> str:
    return secrets.token_urlsafe(32)


def csrf_is_required(request: Request) -> bool:
    return (
        request.method.upper() not in _SAFE_METHODS
        and request.url.path.startswith("/api/")
        and request.url.path not in _UNAUTHENTICATED_PATHS
        and bool(request.cookies.get(SESSION_COOKIE))
    )


def csrf_is_valid(request: Request) -> bool:
    cookie = request.cookies.get(CSRF_COOKIE, "")
    header = request.headers.get(CSRF_HEADER, "")
    return bool(cookie and header and secrets.compare_digest(cookie, header))


def set_csrf_cookie(response: Response, *, secure: bool) -> None:
    response.set_cookie(
        CSRF_COOKIE,
        csrf_token(),
        httponly=False,
        secure=secure,
        samesite="strict",
        path="/",
    )


def clear_csrf_cookie(response: Response) -> None:
    response.delete_cookie(CSRF_COOKIE, path="/")
