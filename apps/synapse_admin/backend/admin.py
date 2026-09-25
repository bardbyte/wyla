"""Administrator APIs for user-account visibility and management."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from apps.synapse_admin.backend.auth import (_google_api_error, _identity,
                                             _identity_unavailable,
                                             invalidate_session_cache, require_admin,
                                             require_users_manage)

router = APIRouter(prefix="/api/admin",
                   dependencies=[Depends(require_users_manage)])


class UpdateUser(BaseModel):
    first_name: str | None = Field(default=None, max_length=100)
    last_name: str | None = Field(default=None, max_length=100)
    name: str | None = Field(default=None, max_length=200)
    status: str | None = Field(default=None, max_length=16)


class RoleChange(BaseModel):
    role: str = Field(pattern="^(analyst|steward|admin)$")


# a person with no chat yet: every counter at zero, the shape the page
# reads (tokens_in, tokens_out, tokens, model_calls, elapsed_ms, turns,
# chats), never a missing key
EMPTY_USAGE = {"tokens_in": 0, "tokens_out": 0, "tokens": 0, "model_calls": 0,
               "elapsed_ms": 0, "turns": 0, "chats": 0}


def _usage_by_person(store) -> tuple[dict[str, dict], str]:
    """Every person's chat usage from the chat tables, one SUM grouped by
    owner (009_usage.sql); a database from before that migration still
    lists its people, with the reason beside the empty column."""
    from sahs.assistant.spanner_store import usage_by_owner
    try:
        return usage_by_owner(store.db), ""
    except Exception as exc:                          # noqa: BLE001
        import logging
        logging.getLogger("synapse_admin.admin").warning(
            "usage totals unavailable (%s: %s)", type(exc).__name__, exc)
        return {}, f"usage unavailable: {type(exc).__name__}: {exc}"


@router.get("/users")
def users(limit: int = 100, admin: dict = Depends(require_users_manage)) -> dict:
    """List account metadata and roles, excluding credentials and tokens,
    each with what their chats cost (tokens in and out, model calls,
    turns, wall time, across every chat of theirs)."""
    limit = min(max(limit, 1), 500)
    from sahs.spanner import spanner_is_enabled
    if not spanner_is_enabled():
        # SAHS_STORE=local: one person, the local developer, whose chats
        # live in the local sqlite store — their totals from it
        from apps.synapse_admin.backend.chat import _chat
        runtime, _ = _chat()
        usage = dict(EMPTY_USAGE)
        totals = getattr(runtime.store, "usage_totals", None)
        if callable(totals):
            usage.update(totals())
        return {"available": True, "users": [{
            "user_id": str(admin.get("user_id") or "local"), "email": "",
            "username": "local", "first_name": "", "last_name": "",
            "name": str(admin.get("name") or "Local developer"),
            "status": "active", "roles": list(admin.get("roles") or ["admin"]),
            "last_login_at": "", "usage": usage}],
            "note": "SAHS_STORE=local: the one person on this machine"}
    try:
        store = _identity()
        # the store's own listing: the same shape every route returns for a
        # person, on Spanner and on the sqlite stand-in alike
        users = store.list_users(limit)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    by_person, usage_note = _usage_by_person(store)
    return {"available": True, "users": [{
        "user_id": user["user_id"], "email": user["email"],
        "username": user["username"], "first_name": user["first_name"],
        "last_name": user["last_name"], "name": user["name"],
        "status": user["status"], "roles": list(user["roles"]),
        "last_login_at": user.get("last_login_at", ""),
        "usage": {**EMPTY_USAGE, **by_person.get(user["user_id"], {})}}
        for user in users], **({"usage_note": usage_note} if usage_note else {})}


@router.patch("/users/{user_id}")
def update_user(user_id: str, body: UpdateUser,
                admin: dict = Depends(require_users_manage),
                request: Request = None) -> dict:
    if user_id == admin["user_id"] and body.status not in (None, "active"):
        raise HTTPException(status_code=400,
                            detail="use your own account settings to change your status")
    try:
        store = _identity()
        user = store.update_user(user_id, first_name=body.first_name,
                                 last_name=body.last_name, name=body.name,
                                 status=body.status)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    from apps.synapse_admin.backend.security import audit_request
    store.record_audit(
        "user.updated", "success", actor_user_id=admin["user_id"],
        subject_user_id=user_id,
        details={"fields": sorted(body.model_dump(exclude_none=True))},
        **audit_request(request).as_kwargs())
    if body.status is not None:
        invalidate_session_cache()
    return {"available": True, "user": user}


@router.delete("/users/{user_id}")
def delete_user(user_id: str,
                admin: dict = Depends(require_users_manage),
                request: Request = None) -> dict:
    if user_id == admin["user_id"]:
        raise HTTPException(status_code=400,
                            detail="an administrator cannot delete their own account")
    try:
        store = _identity()
        deleted = store.delete_user(user_id)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="user not found")
    from apps.synapse_admin.backend.security import audit_request
    store.record_audit(
        "user.deleted", "success", actor_user_id=admin["user_id"],
        subject_user_id=user_id, **audit_request(request).as_kwargs())
    invalidate_session_cache()
    return {"available": True, "deleted": True}


@router.post("/users/{user_id}/roles")
def grant_user_role(user_id: str, body: RoleChange,
                    admin: dict = Depends(require_users_manage),
                    request: Request = None) -> dict:
    try:
        store = _identity()
        user = store.grant_role(user_id, body.role,
                                granted_by=admin["user_id"])
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    from apps.synapse_admin.backend.security import audit_request
    store.record_audit(
        "role.granted", "success", actor_user_id=admin["user_id"],
        subject_user_id=user_id, details={"role": body.role},
        **audit_request(request).as_kwargs())
    invalidate_session_cache()
    return {"available": True, "user": user}


@router.delete("/users/{user_id}/roles/{role}")
def revoke_user_role(user_id: str, role: str,
                     admin: dict = Depends(require_users_manage),
                     request: Request = None) -> dict:
    if role not in {"analyst", "steward", "admin"}:
        raise HTTPException(status_code=400, detail="unknown role")
    if user_id == admin["user_id"] and role == "admin":
        raise HTTPException(status_code=400,
                            detail="an administrator cannot revoke their own admin role")
    try:
        store = _identity()
        user = store.revoke_role(user_id, role)
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if user is None:
        raise HTTPException(status_code=404, detail="active role not found")
    from apps.synapse_admin.backend.security import audit_request
    store.record_audit(
        "role.revoked", "success", actor_user_id=admin["user_id"],
        subject_user_id=user_id, details={"role": role},
        **audit_request(request).as_kwargs())
    invalidate_session_cache()
    return {"available": True, "user": user}
