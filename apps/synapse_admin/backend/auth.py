"""Cookie-authenticated registration and session endpoints for Synapse."""
from __future__ import annotations

import logging
import os
import base64
import hashlib
import json
import secrets
import time
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache
from threading import Lock
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen

from fastapi import APIRouter, Cookie, Depends, Header, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/auth")
callback_router = APIRouter()
COOKIE = "synapse_session"
request_user: ContextVar[dict | None] = ContextVar("request_user", default=None)
logger = logging.getLogger(__name__)

# Every protected request validates the session cookie against Spanner
# (chat polling included), which can turn ordinary traffic into enough
# read QPS to trip transient 503s under load. A few seconds of positive
# caching removes most of that volume; revocation (logout) evicts the
# entry immediately so it never outlives the session it was cached for.
_SESSION_CACHE_SECONDS = 5.0
_session_cache: dict[str, tuple[float, dict]] = {}
# the Google consent hop parks its state in the store's AuthStates table
# (the same table the Okta hop uses), so the callback may land on any pod
_GOOGLE_STATE_KIND = "google_connect"
# one token provider per person, shared by every runner built for them
# (the ask lane builds one per runtime and one per message, the chat one
# per runtime): one refresh trip per token lifetime, and disconnect
# invalidates the one cache, so no access token outlives the connection
_google_providers: dict[str, object] = {}
_google_providers_lock = Lock()
_GOOGLE_AUTHORIZATION_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_ENDPOINT = "https://openidconnect.googleapis.com/v1/userinfo"
_GOOGLE_OAUTH_STATE_TTL_SECONDS = 600


def _cached_session_user(token: str) -> dict | None:
    cached = _session_cache.get(token)
    if cached is not None and cached[0] > time.monotonic():
        return cached[1]
    user = _identity().session_user(token)
    if user is not None:
        _session_cache[token] = (time.monotonic() + _SESSION_CACHE_SECONDS, user)
    else:
        _session_cache.pop(token, None)
    return user


def invalidate_session_cache() -> None:
    _session_cache.clear()


def _google_api_error():
    """Delay importing the optional Spanner client dependency until needed."""
    from google.api_core.exceptions import GoogleAPICallError
    return GoogleAPICallError


def _retryable_error() -> tuple[type[Exception], ...]:
    """Transient Spanner failures worth a 503 (client can safely retry)."""
    from google.api_core.exceptions import (Aborted, DeadlineExceeded,
                                            InternalServerError,
                                            ResourceExhausted,
                                            ServiceUnavailable)
    return (Aborted, DeadlineExceeded, InternalServerError,
            ResourceExhausted, ServiceUnavailable)


class Credentials(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=200)
    first_name: str = Field(default="", max_length=100)
    last_name: str = Field(default="", max_length=100)
    name: str = Field(default="", max_length=200)


class PasswordReset(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=200)


def _bearer_token(authorization: str | None) -> str | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization[7:].strip()
    return token or None


def _google_oauth_settings():
    from sahs.spanner import GoogleOAuthConfigurationError, GoogleOAuthSettings
    try:
        return GoogleOAuthSettings.from_env()
    except GoogleOAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _requires_google_user_oauth() -> bool:
    from sahs.util.auth import resolve_bq_auth_mode
    return resolve_bq_auth_mode() == "user"


def _google_runner(user_id: str):
    from apps.synapse_admin.backend.meridian import _silo_import
    _silo_import()
    from sahs.tools.sandbox import BQJobRunner
    from sahs.util.google_auth.oauth import GoogleOAuthCredentialProvider
    if not _requires_google_user_oauth():
        from sahs.util.auth import AuthError
        try:
            return BQJobRunner()
        except AuthError as exc:
            # no BigQuery configured on this machine (a laptop, a test):
            # live execution stays denied by the sandbox; nothing else breaks
            logger.info("no service-account BigQuery runner: %s", exc)
            return None
    try:
        settings = _google_oauth_settings()
    except HTTPException as exc:
        if exc.status_code == 503:
            return None
        raise
    with _google_providers_lock:
        provider = _google_providers.get(user_id)
        if provider is None:
            provider = GoogleOAuthCredentialProvider(
                lambda: _identity().google_connection(user_id), settings)
            _google_providers[user_id] = provider
    from sahs.util.auth import AuthError
    try:
        return BQJobRunner(token_provider=provider)
    except AuthError as exc:
        # the person's token can be minted but there is no BigQuery
        # project to run in: live execution stays denied by the sandbox;
        # opening a session must not fail on it
        logger.warning("no user-delegated BigQuery runner for %s: %s", user_id, exc)
        return None


def _google_request_runner(access_token: str):
    from apps.synapse_admin.backend.meridian import _silo_import
    _silo_import()
    from sahs.tools.sandbox import BQJobRunner
    return BQJobRunner(token_provider=lambda: access_token)


def _pkce_verifier() -> str:
    return secrets.token_urlsafe(64)


def _pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _google_json_request(url: str, *, data: dict[str, str], headers: dict[str, str] | None = None) -> dict:
    body = urlencode(data).encode("utf-8")
    request = UrlRequest(url, data=body, headers={
        "Content-Type": "application/x-www-form-urlencoded",
        **(headers or {}),
    }, method="POST")
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read())
    except Exception as exc:
        logger.warning("Google OAuth request failed: %s", exc)
        raise HTTPException(status_code=502,
                            detail="Google OAuth service is unavailable") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="invalid Google OAuth response")
    return payload


def _google_userinfo(access_token: str) -> dict:
    request = UrlRequest(_GOOGLE_USERINFO_ENDPOINT, headers={
        "Authorization": f"Bearer {access_token}",
    })
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read())
    except Exception as exc:
        logger.warning("Google identity lookup failed: %s", exc)
        raise HTTPException(status_code=502,
                            detail="Google identity service is unavailable") from exc
    if not isinstance(payload, dict) or not payload.get("sub") or not payload.get("email"):
        raise HTTPException(status_code=401, detail="Google identity could not be verified")
    if payload.get("email_verified") is not True:
        raise HTTPException(status_code=401, detail="Google email is not verified")
    return payload


@lru_cache(maxsize=1)
def _identity():
    from apps.synapse_admin.backend.meridian import _silo_import
    _silo_import()
    from sahs.identity_store import IdentityStore
    from sahs.spanner import (AuthSettings, SpannerConfigurationError,
                              SpannerSettings)
    try:
        return IdentityStore(SpannerSettings.from_env(), AuthSettings.from_env())
    except SpannerConfigurationError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"identity service is unavailable: {exc}") from exc
    except OSError as exc:
        raise _identity_unavailable(exc) from exc
    except _google_api_error() as exc:
        # a failed construction must not be memoized: lru_cache never
        # caches raised exceptions, so the next request retries cleanly
        raise _identity_unavailable(exc) from exc


def _secure_cookie(request: Request | None = None) -> bool:
    """true and false say so; auto is Secure unless the request that
    earned the cookie plainly arrived over http (a laptop at localhost),
    and Secure when no request is known."""
    from sahs.spanner import AuthSettings
    value = AuthSettings.from_env().cookie_secure
    if value == "auto" and request is not None:
        forwarded = request.headers.get("x-forwarded-proto", "").split(",")[0].strip()
        return (forwarded or request.url.scheme) == "https"
    return value == "true" or value == "auto"


def _set_session(response: Response, token: str,
                 request: Request | None = None) -> None:
    from apps.synapse_admin.backend.security import set_csrf_cookie
    secure = _secure_cookie(request)
    response.set_cookie(COOKIE, token, httponly=True, secure=secure,
                        samesite="strict", path="/")
    set_csrf_cookie(response, secure=secure)


def _identity_unavailable(exc: Exception) -> HTTPException:
    """Keep Cloud-client failures out of browser error pages, but log the
    real cause: 'unavailable' hides a mix of transient outages, missing
    schema, and bad credentials unless the detail is captured server-side."""
    logger.error("identity service call failed: %s", exc, exc_info=exc)
    if isinstance(exc, _retryable_error()):
        return HTTPException(
            status_code=503,
            detail="identity service is temporarily unavailable; try again")
    return HTTPException(
        status_code=503,
        detail="identity service is unavailable; verify Cloud Spanner API "
               "access and try again")


def _require_local_login() -> None:
    """The email-and-password routes exist for a laptop and as a
    break-glass path; the enterprise front door is Okta, so they refuse
    unless AUTH_LOCAL_LOGIN=1."""
    from apps.synapse_admin.backend.meridian import _silo_import
    _silo_import()
    from sahs.spanner import AuthSettings, SpannerConfigurationError
    try:
        enabled = AuthSettings.from_env().local_login_enabled
    except SpannerConfigurationError as exc:
        raise HTTPException(status_code=503,
                            detail=f"identity service is unavailable: {exc}") from exc
    if not enabled:
        raise HTTPException(status_code=403,
                            detail="email-and-password sign-in is off here; sign in with Okta")


def _loopback_request(request: Request) -> bool:
    host = (request.client.host if request.client else "") or request.url.hostname or ""
    return host.lower() in {"127.0.0.1", "localhost", "::1"}


def _direct_reset_allowed(request: Request) -> bool:
    from apps.synapse_admin.backend.meridian import _silo_import
    _silo_import()
    from sahs.spanner import AuthSettings, SpannerConfigurationError
    from sahs.util.network import epaas_env
    try:
        settings = AuthSettings.from_env()
    except SpannerConfigurationError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"identity service is unavailable: {exc}") from exc
    if not settings.allow_insecure_direct_reset:
        return False
    if epaas_env() in {"e1", "e2", "e3"} and not _loopback_request(request):
        return False
    return True


def current_user(
    synapse_session: str | None = Cookie(default=None),
    authorization: str | None = Header(default=None),
) -> dict:
    """Resolve Bearer authentication first, then the opaque session cookie."""
    from apps.synapse_admin.backend.meridian import _silo_import
    _silo_import()
    from sahs.spanner import spanner_is_enabled
    if not spanner_is_enabled():
        from sahs.identity.authorization import ROLE_PERMISSIONS
        return {"user_id": "local", "name": "Local developer",
                "roles": ["admin"], "surfaces": ["admin", "synapse"],
                "permissions": sorted(ROLE_PERMISSIONS["admin"])}
    try:
        bearer = _bearer_token(authorization)
        user = _cached_session_user(bearer) if bearer else None
        if user is None and synapse_session:
            # a Bearer that is not a session token (a Google access
            # token riding a cookie session, google_bigquery_user)
            # never unseats the cookie
            user = _cached_session_user(synapse_session)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    if user is None:
        raise HTTPException(status_code=401, detail="sign in required")
    request_user.set(user)
    return user


@dataclass(frozen=True, slots=True)
class GoogleBigQueryAuthorization:
    user: dict
    access_token: str


def google_bigquery_user_if_live(
    user: dict = Depends(current_user),
    authorization: str | None = Header(default=None),
) -> GoogleBigQueryAuthorization | None:
    """Require Google authorization only when Ask is configured for live runs."""
    from sahs.tools.sandbox import live_enabled

    if (live_enabled()
            and os.environ.get("ASK_EXECUTE", "live").strip().lower() == "live"):
        return google_bigquery_user(user, authorization)
    return None


def google_bigquery_user(
    user: dict = Depends(current_user),
    authorization: str | None = Header(default=None),
) -> GoogleBigQueryAuthorization:
    """Require a valid Google BigQuery token for user-scoped live execution."""
    from sahs.util.google_auth.token import validate_access_token

    google_token = _bearer_token(authorization)
    if not google_token:
        raise HTTPException(
            status_code=401,
            detail="Google BigQuery authorization required: connect Google and retry",
        )
    try:
        token_info = validate_access_token(google_token)
    except ValueError as exc:
        raise HTTPException(
            status_code=401,
            detail=f"Google BigQuery authorization failed: {exc}",
        ) from exc
    if token_info.email.strip().lower() != user["email"].strip().lower():
        raise HTTPException(
            status_code=403,
            detail="Google identity does not match the signed-in ESL account",
        )
    return GoogleBigQueryAuthorization(user=user, access_token=google_token)


def require_admin(user: dict = Depends(current_user)) -> dict:
    """Require the admin surface, enforced independently of frontend navigation."""
    if "admin" not in user.get("surfaces", []):
        raise HTTPException(status_code=403, detail="admin access required")
    return user


def require_users_manage(user: dict = Depends(require_admin)) -> dict:
    return require_permission("users.manage")(user)


def require_permission(permission: str):
    """Build a FastAPI dependency that requires one resolved permission."""
    def dependency(user: dict = Depends(current_user)) -> dict:
        permissions = user.get("permissions")
        if permissions is None:
            from sahs.identity.authorization import permissions_for_roles
            permissions = permissions_for_roles(user.get("roles", []))
        if permission not in permissions:
            raise HTTPException(status_code=403,
                                detail=f"{permission} required")
        return user
    return dependency


@router.get("/google/start")
def google_start(popup: bool = False,
                 user: dict = Depends(current_user)) -> RedirectResponse:
    """Start Google consent for the already authenticated ESL user."""
    settings = _google_oauth_settings()
    state = secrets.token_urlsafe(32)
    verifier = _pkce_verifier()
    try:
        _identity().put_state(
            state, _GOOGLE_STATE_KIND,
            {"email": user["email"], "verifier": verifier, "popup": bool(popup)},
            user_id=user["user_id"], ttl_seconds=_GOOGLE_OAUTH_STATE_TTL_SECONDS)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    query = urlencode({
        "client_id": settings.client_id,
        "redirect_uri": settings.redirect_uri,
        "response_type": "code",
        "scope": " ".join(settings.scopes),
        "state": state,
        "code_challenge": _pkce_challenge(verifier),
        "code_challenge_method": "S256",
        "access_type": "offline",
        "prompt": "consent",
    })
    return RedirectResponse(f"{_GOOGLE_AUTHORIZATION_ENDPOINT}?{query}", status_code=307)


@router.get("/google/callback")
def google_callback(code: str | None = None, state: str | None = None,
                    error: str | None = None,
                    synapse_session: str | None = Cookie(default=None)) -> dict:
    """Exchange Google's one-time code and bind its identity to the ESL user.

    Token persistence and BigQuery credential use are intentionally separate
    from the callback so refresh tokens are never returned to the browser.
    """
    if error:
        raise HTTPException(status_code=400, detail=f"Google authorization failed: {error}")
    if not code or not state:
        raise HTTPException(status_code=400, detail="Google callback is missing code or state")
    try:
        pending = _identity().pop_state(state, _GOOGLE_STATE_KIND)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    if pending is None:
        raise HTTPException(status_code=400, detail="Google OAuth state is invalid or expired")
    if not synapse_session:
        raise HTTPException(status_code=401, detail="sign in required")
    try:
        user = _cached_session_user(synapse_session)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    if user is None or user["user_id"] != pending.get("user_id"):
        raise HTTPException(status_code=401, detail="ESL session does not match OAuth request")

    settings = _google_oauth_settings()
    token_response = _google_json_request(_GOOGLE_TOKEN_ENDPOINT, data={
        "client_id": settings.client_id,
        "client_secret": settings.client_secret,
        "code": code,
        "code_verifier": str(pending.get("verifier", "")),
        "grant_type": "authorization_code",
        "redirect_uri": settings.redirect_uri,
    })
    access_token = token_response.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise HTTPException(status_code=401, detail="Google did not issue an access token")
    google_user = _google_userinfo(access_token)
    if google_user["email"].strip().lower() != user["email"].strip().lower():
        raise HTTPException(status_code=403,
                            detail="Google email does not match ESL account")
    granted_scopes = set(str(token_response.get("scope") or "").split())
    required_scopes = {
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/userinfo.email",
    }
    missing_scopes = sorted(required_scopes - granted_scopes)
    if missing_scopes:
        raise HTTPException(
            status_code=400,
            detail="Google did not grant required BigQuery scopes: "
            + ", ".join(missing_scopes),
        )
    if not token_response.get("refresh_token"):
        raise HTTPException(status_code=400,
                            detail="Google did not issue a refresh token; reconnect with consent")
    try:
        _identity().save_google_connection(
            user["user_id"], subject=google_user["sub"],
            email=google_user["email"], refresh_token=token_response["refresh_token"],
            scopes=token_response.get("scope", "").split(), settings=settings)
    except Exception as exc:
        logger.error("Google OAuth connection persistence failed: %s", exc,
                     exc_info=exc)
        raise HTTPException(status_code=503,
                            detail="Google connection could not be saved") from exc
    logger.info("Google OAuth identity connected for ESL user %s", user["user_id"])
    if pending.get("popup"):
        return HTMLResponse(
            "<!doctype html><meta charset=\"utf-8\"><title>Google connected</title>"
            "<script>if(window.opener){window.opener.postMessage({type:'google-connected'},"
            "window.location.origin);}window.close();</script>"
            "<p>Google connected. You can close this window.</p>")
    if "admin" in user.get("surfaces", []):
        destination = settings.post_connect_uri
    else:
        destination = "/synapse/#/chat/new?google=connected"
    return RedirectResponse(destination, status_code=303)


@router.get("/google/connection")
def google_connection(user: dict = Depends(current_user)) -> dict:
    if not _requires_google_user_oauth():
        return {"available": True, "connected": False,
                "requires_user_oauth": False,
                "provider": "service_account", "email": ""}
    try:
        connection = _identity().google_connection(user["user_id"])
    except _google_api_error() as exc:
        if "GoogleOAuthConnections" in str(exc):
            raise HTTPException(
                status_code=503,
                detail="Google OAuth storage is not initialized. Apply "
                       "004_google_oauth.sql to the configured Spanner "
                       "database, then retry.") from exc
        raise _identity_unavailable(exc) from exc
    return {"available": True, "connected": connection is not None,
            "requires_user_oauth": True,
            "provider": "google",
            "email": connection.get("GoogleEmail") if connection else ""}


@router.delete("/google/connection")
def disconnect_google(user: dict = Depends(current_user)) -> dict:
    try:
        _identity().revoke_google_connection(user["user_id"])
    except _google_api_error() as exc:
        if "GoogleOAuthConnections" in str(exc):
            raise HTTPException(
                status_code=503,
                detail="Google OAuth storage is not initialized. Apply "
                       "004_google_oauth.sql to the configured Spanner "
                       "database, then retry.") from exc
        raise _identity_unavailable(exc) from exc
    with _google_providers_lock:
        provider = _google_providers.pop(user["user_id"], None)
    if provider is not None:
        provider.invalidate()
    return {"available": True, "connected": False, "provider": "google"}


@router.post("/signup", status_code=201)
def signup(body: Credentials, response: Response, request: Request = None) -> dict:
    _require_local_login()
    from apps.synapse_admin.backend.security import audit_request
    store = _identity()
    try:
        if body.first_name or body.last_name:
            user, token = store.signup(
                body.email, body.first_name, body.last_name, body.password)
        else:
            user, token = store.signup(body.email, body.name, body.password)
    except HTTPException:
        raise
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    store.record_audit(
        "signup.ok", "success", actor_user_id=user["user_id"],
        subject_user_id=user["user_id"], **audit_request(request).as_kwargs())
    _set_session(response, token, request)
    return {"available": True, "token": token, "user": user}


@router.post("/login")
def login(body: Credentials, response: Response, request: Request = None) -> dict:
    _require_local_login()
    from apps.synapse_admin.backend.security import audit_request
    store = _identity()
    try:
        user, token = store.login(body.email, body.password)
    except HTTPException:
        raise
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    except ValueError as exc:
        store.record_audit(
            "login.failed", "denied", **audit_request(request).as_kwargs())
        raise HTTPException(status_code=401,
                            detail="invalid email or password") from exc
    store.record_audit(
        "login.ok", "success", actor_user_id=user["user_id"],
        subject_user_id=user["user_id"], **audit_request(request).as_kwargs())
    _set_session(response, token, request)
    return {"available": True, "token": token, "user": user}


@router.post("/forgot-password")
@router.post("/reset-password")
def reset_password(body: PasswordReset, request: Request) -> dict:
    """Guarded direct reset; this is not an email-verified recovery flow."""
    _require_local_login()
    if not _direct_reset_allowed(request):
        raise HTTPException(status_code=403,
                            detail="contact an administrator to reset your password")
    try:
        store = _identity()
        user_id = store.direct_reset_password(body.email, body.password)
    except HTTPException:
        raise
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    from apps.synapse_admin.backend.security import audit_request
    store.record_audit(
        "password.reset", "success", subject_user_id=user_id,
        details={"method": "direct_local"},
        **audit_request(request).as_kwargs())
    return {"available": True}


@router.get("/me")
@callback_router.get("/api/whoami")
def me(synapse_session: str | None = Cookie(default=None),
       authorization: str | None = Header(default=None)) -> dict:
    """Who the cookie says you are: ``/api/auth/me`` (the shells boot
    from it) and ``/api/whoami``, the same answer under the name a
    curl reaches for."""
    user = current_user(synapse_session, authorization)
    return {"available": True, "user": user}


@router.post("/logout")
def logout(response: Response,
           synapse_session: str | None = Cookie(default=None),
           authorization: str | None = Header(default=None),
           request: Request = None) -> dict:
    token = _bearer_token(authorization) or synapse_session
    if token:
        try:
            store = _identity()
            user = _cached_session_user(token)
            store.logout(token)
            from apps.synapse_admin.backend.security import audit_request
            store.record_audit(
                "logout", "success",
                actor_user_id=user["user_id"] if user else None,
                subject_user_id=user["user_id"] if user else None,
                **audit_request(request).as_kwargs())
        except _google_api_error() as exc:
            raise _identity_unavailable(exc) from exc
        finally:
            _session_cache.pop(token, None)
    from apps.synapse_admin.backend.security import clear_csrf_cookie
    response.delete_cookie(COOKIE, path="/")
    clear_csrf_cookie(response)
    return {"available": True}


@router.post("/logout-all")
def logout_all(response: Response,
               user: dict = Depends(current_user),
               request: Request = None) -> dict:
    try:
        store = _identity()
        store.logout_all(user["user_id"])
        from apps.synapse_admin.backend.security import audit_request
        store.record_audit(
            "session.revoked_all", "success", actor_user_id=user["user_id"],
            subject_user_id=user["user_id"],
            **audit_request(request).as_kwargs())
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    _session_cache.clear()
    from apps.synapse_admin.backend.security import clear_csrf_cookie
    response.delete_cookie(COOKIE, path="/")
    clear_csrf_cookie(response)
    return {"available": True}
