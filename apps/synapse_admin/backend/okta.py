"""Okta sign-in: the front door of the enterprise deployment.

    GET /api/auth/okta             is Okta configured here, and is the local
                                   email-and-password path on at all
    GET /api/auth/okta/start?next= a state, a nonce and a PKCE verifier are
                                   parked in the identity store; the browser
                                   goes to Okta
    GET /callback?code&state       Okta comes back; the code is exchanged, the
                                   ID token verified, the person found or
                                   created, their roles set from their groups,
                                   a session cookie issued, then a redirect to
                                   where they were going
    GET /api/auth/okta/callback    the same handler under the API prefix

The registered callback (``OKTA_REDIRECT_URI``) is the root ``/callback``,
which the Google connect flow also uses. One route serves both: a state
that starts with ``okta.`` is a sign-in, anything else is handed to the
Google callback unchanged.

State lives in the identity store (``AuthStates``), not in process memory,
so the callback may land on any pod. Everything Okta-specific is in
``sahs.identity.oidc``; this module only speaks HTTP and the store.
"""

from __future__ import annotations

import logging
import secrets
from functools import lru_cache
from urllib.parse import urlsplit

from fastapi import APIRouter, Cookie, HTTPException, Request
from fastapi.responses import RedirectResponse

from apps.synapse_admin.backend.auth import (_google_api_error, _identity,
                                             _identity_unavailable, _set_session,
                                             google_callback)

router = APIRouter(prefix="/api/auth/okta")
callback_router = APIRouter()
logger = logging.getLogger(__name__)

STATE_PREFIX = "okta."
STATE_KIND = "okta_signin"
STATE_TTL_SECONDS = 600


def _settings():
    from apps.synapse_admin.backend.meridian import _silo_import
    _silo_import()
    from sahs.identity.oidc import OidcSettings
    return OidcSettings.from_env()


@lru_cache(maxsize=1)
def _client():
    from sahs.identity.oidc import OidcClient, OidcConfigurationError
    try:
        return OidcClient(_settings())
    except OidcConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def reset_client() -> None:
    """Forget the cached client (tests, or a configuration change)."""
    _client.cache_clear()


def _store():
    from sahs.spanner import spanner_is_enabled
    if not spanner_is_enabled():
        raise HTTPException(
            status_code=503,
            detail="sign-in needs an identity store: set SAHS_STORE=spanner or SAHS_STORE=sqlite")
    return _identity()


def _safe_next(value: str | None, fallback: str) -> str:
    """Only a path on this site; never another host, never a scheme."""
    candidate = (value or "").strip()
    if candidate.startswith("/") and not candidate.startswith("//") and "\\" not in candidate:
        return candidate
    return fallback


def _audit(request: Request):
    from apps.synapse_admin.backend.security import audit_request
    return audit_request(request)


@router.get("")
def okta_status() -> dict:
    """What the sign-in page needs to draw itself."""
    from sahs.spanner import AuthSettings, spanner_is_enabled
    settings = _settings()
    auth = AuthSettings.from_env()
    return {
        "available": True,
        "configured": bool(settings.configured and spanner_is_enabled()),
        "provider": settings.provider,
        "issuer_host": urlsplit(settings.issuer).hostname or "",
        "start": f"{router.prefix}/start",
        "local_login": bool(auth.local_login_enabled and spanner_is_enabled()),
    }


@router.get("/start")
def okta_start(request: Request, next: str = "") -> RedirectResponse:
    from sahs.identity.oidc import OidcError, pkce_pair
    store = _store()
    client = _client()
    verifier, challenge = pkce_pair()
    nonce = secrets.token_urlsafe(24)
    state = STATE_PREFIX + secrets.token_urlsafe(32)
    try:
        store.put_state(state, STATE_KIND,
                        {"verifier": verifier, "nonce": nonce, "next": _safe_next(next, "")},
                        ttl_seconds=STATE_TTL_SECONDS)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    try:
        url = client.authorization_url(state=state, nonce=nonce, code_challenge=challenge)
    except OidcError as exc:
        raise HTTPException(status_code=502, detail=f"Okta is unavailable: {exc}") from exc
    return RedirectResponse(url, status_code=307)


def okta_callback(request: Request, code: str | None, state: str | None,
                  error: str | None, error_description: str | None) -> RedirectResponse:
    from sahs.identity.oidc import OidcError
    store = _store()
    try:
        pending = store.pop_state(state or "", STATE_KIND)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    if pending is None:
        raise HTTPException(status_code=400,
                            detail="this sign-in has expired or was already used; start again")
    if error:
        store.record_audit("login.failed", "denied",
                           details={"via": "okta", "error": error,
                                    "description": error_description or ""},
                           **_audit(request).as_kwargs())
        raise HTTPException(status_code=400,
                            detail=f"Okta refused the sign-in: {error} {error_description or ''}".strip())
    if not code:
        raise HTTPException(status_code=400, detail="Okta returned no code")

    client = _client()
    try:
        tokens = client.exchange_code(code, str(pending.get("verifier", "")))
        claims = client.verify_id_token(str(tokens.get("id_token", "")),
                                        nonce=str(pending.get("nonce", "")))
    except OidcError as exc:
        store.record_audit("login.failed", "denied",
                           details={"via": "okta", "reason": str(exc)},
                           **_audit(request).as_kwargs())
        raise HTTPException(status_code=401, detail=f"Okta sign-in failed: {exc}") from exc

    person = client.person_from(claims)
    if not person["email"]:
        raise HTTPException(
            status_code=401,
            detail="the ID token carries no email: grant the email scope on the Okta client, "
                   "or name the claim that holds it in AUTH_EMAIL_CLAIMS")
    settings = client.settings
    kept = {k: claims[k] for k in ("sub", "iss", "aud", "exp", "iat", "name", "given_name",
                                   "family_name", settings.group_claim, *settings.email_claims)
            if k in claims}
    audit = _audit(request)
    try:
        user = store.find_or_create_external_user(
            provider=settings.provider, issuer=settings.issuer, subject=person["subject"],
            email=person["email"], name=person["name"], first_name=person["first_name"],
            last_name=person["last_name"], claims=kept, default_roles=person["roles"])
        if settings.group_role_map:
            # the provider's groups are the source of truth for roles only
            # when a mapping says so; otherwise roles stay as granted here
            user = store.set_roles(user["user_id"], person["roles"], granted_by="okta") or user
        if user["status"] != "active":
            store.record_audit("login.failed", "denied", subject_user_id=user["user_id"],
                               details={"via": "okta", "status": user["status"]},
                               **audit.as_kwargs())
            raise HTTPException(status_code=403, detail=f"this account is {user['status']}")
        token = store.issue_session(user["user_id"], ip=audit.ip, user_agent=audit.user_agent)
        store.record_audit("login.ok", "success", actor_user_id=user["user_id"],
                           subject_user_id=user["user_id"],
                           details={"via": "okta", "groups": person["groups"],
                                    "roles": user["roles"], "email_claim": person["email_claim"]},
                           **audit.as_kwargs())
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    logger.info("okta sign-in for %s (roles %s)", user["user_id"], ",".join(user["roles"]))
    home = "/" if "admin" in user.get("surfaces", []) else "/synapse/"
    response = RedirectResponse(_safe_next(pending.get("next"), home), status_code=303)
    _set_session(response, token, request)
    return response


@callback_router.get("/callback")
@router.get("/callback")
def callback(request: Request, code: str | None = None, state: str | None = None,
             error: str | None = None, error_description: str | None = None,
             synapse_session: str | None = Cookie(default=None)):
    """The one registered callback: an Okta sign-in when the state says so,
    otherwise the Google connect flow, unchanged."""
    if state and state.startswith(STATE_PREFIX):
        return okta_callback(request, code, state, error, error_description)
    return google_callback(code=code, state=state, error=error,
                           synapse_session=synapse_session)


__all__ = ["callback_router", "okta_callback", "reset_client", "router"]
