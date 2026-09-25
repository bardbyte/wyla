"""Google access-token validation for user-scoped BigQuery requests."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True, slots=True)
class GoogleTokenInfo:
    email: str
    email_verified: bool
    scopes: tuple[str, ...] = field(default_factory=tuple)


DEFAULT_GOOGLE_REQUIRED_SCOPES = (
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/userinfo.email",
)


def validate_access_token(
    token: str,
    *,
    required_scopes: tuple[str, ...] = DEFAULT_GOOGLE_REQUIRED_SCOPES,
) -> GoogleTokenInfo:
    if not token:
        raise ValueError("Google access token is empty")

    body = urlencode({"access_token": token}).encode("utf-8")
    request = Request(
        "https://oauth2.googleapis.com/tokeninfo",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # pragma: no cover - network guard
        raise ValueError("Google token validation failed") from exc

    if not isinstance(payload, dict):
        raise ValueError("Google token validation returned an invalid payload")

    email = str(payload.get("email") or "").strip()
    if not email:
        raise ValueError("Google token did not include an email")
    if payload.get("email_verified") is not True:
        raise ValueError("Google email is not verified")

    scope_text = str(payload.get("scope") or "")
    scopes = tuple(part for part in scope_text.split() if part)
    missing = [scope for scope in required_scopes if scope not in scopes]
    if missing:
        raise ValueError(
            f"Google token is missing required scopes: {', '.join(missing)}")
    return GoogleTokenInfo(email=email, email_verified=True, scopes=scopes)
