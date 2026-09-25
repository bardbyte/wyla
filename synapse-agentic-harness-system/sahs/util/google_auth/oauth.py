"""Google OAuth token protection and request-scoped BigQuery credentials."""
from __future__ import annotations

import base64
import binascii
import json
import threading
import time
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sahs.spanner import GoogleOAuthSettings


class GoogleOAuthTokenError(RuntimeError):
    """Raised when a stored Google credential cannot be used."""


def _fernet(settings: GoogleOAuthSettings):
    try:
        from cryptography.fernet import Fernet, InvalidToken
    except ImportError as exc:
        raise GoogleOAuthTokenError(
            "cryptography is required for Google OAuth token storage") from exc
    try:
        return Fernet(settings.token_encryption_key), InvalidToken
    except (TypeError, ValueError) as exc:
        raise GoogleOAuthTokenError(
            "GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY must be a Fernet key") from exc


def encrypt_refresh_token(token: str, settings: GoogleOAuthSettings) -> bytes:
    if not token:
        raise GoogleOAuthTokenError("Google refresh token is empty")
    fernet, _ = _fernet(settings)
    return fernet.encrypt(token.encode("utf-8"))


def decrypt_refresh_token(ciphertext: bytes, settings: GoogleOAuthSettings) -> str:
    fernet, invalid_token = _fernet(settings)
    try:
        return fernet.decrypt(bytes(ciphertext)).decode("utf-8")
    except (invalid_token, UnicodeDecodeError) as exc:
        try:
            legacy_ciphertext = base64.b64decode(ciphertext, validate=True)
            return fernet.decrypt(legacy_ciphertext).decode("utf-8")
        except (binascii.Error, invalid_token, UnicodeDecodeError, ValueError):
            raise GoogleOAuthTokenError(
                "stored Google OAuth token is invalid") from exc


class GoogleOAuthCredentialProvider:
    """Callable access-token provider backed by one user's stored refresh token."""

    def __init__(self, connection_loader: Callable[[], dict[str, Any] | None],
                 settings: GoogleOAuthSettings) -> None:
        self._connection_loader = connection_loader
        self._settings = settings
        self._access_token = ""
        self._expires_at = 0.0
        self._lock = threading.Lock()

    def __call__(self) -> str:
        with self._lock:
            if self._access_token and self._expires_at > time.time() + 60:
                return self._access_token
            connection = self._connection_loader()
            if not connection:
                raise GoogleOAuthTokenError(
                    "connect Google BigQuery before running live queries")
            refresh_token = decrypt_refresh_token(
                connection["RefreshTokenCiphertext"], self._settings)
            payload = self._refresh(refresh_token)
            token = payload.get("access_token")
            if not isinstance(token, str) or not token:
                raise GoogleOAuthTokenError(
                    "Google did not return an access token")
            self._access_token = token
            self._expires_at = time.time() + int(payload.get("expires_in", 3600))
            return token

    def invalidate(self) -> None:
        """Drop the cached access token after disconnect or revocation."""
        with self._lock:
            self._access_token = ""
            self._expires_at = 0.0

    def _refresh(self, refresh_token: str) -> dict[str, Any]:
        body = urlencode({
            "client_id": self._settings.client_id,
            "client_secret": self._settings.client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }).encode("utf-8")
        request = Request(
            "https://oauth2.googleapis.com/token", data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST")
        try:
            with urlopen(request, timeout=10) as response:
                payload = json.loads(response.read())
        except Exception as exc:
            raise GoogleOAuthTokenError(
                "Google token refresh failed; reconnect Google BigQuery") from exc
        if not isinstance(payload, dict) or payload.get("error"):
            raise GoogleOAuthTokenError(
                "Google token refresh failed; reconnect Google BigQuery")
        return payload
