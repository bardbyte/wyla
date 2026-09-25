"""Administrator access-context visibility from identity audit events."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends

from apps.synapse_admin.backend.auth import (
    _google_api_error,
    _identity,
    _identity_unavailable,
    require_permission,
)

router = APIRouter(
    prefix="/api/admin/access",
    dependencies=[Depends(require_permission("audit.read"))],
)


def _browser_family(user_agent: str) -> str:
    value = (user_agent or "").lower()
    if "edg/" in value:
        return "Edge"
    if "chrome/" in value:
        return "Chrome"
    if "firefox/" in value:
        return "Firefox"
    if "safari/" in value and "chrome/" not in value:
        return "Safari"
    return "Other"


def _os_family(user_agent: str) -> str:
    value = (user_agent or "").lower()
    if "windows" in value:
        return "Windows"
    if "android" in value:
        return "Android"
    if "iphone" in value or "ipad" in value:
        return "iOS"
    if "mac os" in value or "macintosh" in value:
        return "macOS"
    if "linux" in value:
        return "Linux"
    return "Other"


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "")


def _summarize(users: list[dict[str, Any]],
               events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_user[str(event.get("ActorUserId") or "")].append(event)

    result = []
    for user in users:
        user_id = str(user["UserId"])
        rows = by_user.get(user_id, [])
        contexts: dict[tuple[str, str, str], dict[str, Any]] = {}
        login_events = []
        for row in rows:
            ip = str(row.get("Ip") or "")
            user_agent = str(row.get("UserAgent") or "")
            browser = _browser_family(user_agent)
            operating_system = _os_family(user_agent)
            occurred_at = _timestamp(row.get("OccurredAt"))
            key = (ip, browser, operating_system)
            context = contexts.setdefault(key, {
                "ip": ip,
                "browser": browser,
                "operating_system": operating_system,
                "login_count": 0,
                "first_seen_at": occurred_at,
                "last_seen_at": occurred_at,
                "user_agents": set(),
            })
            context["login_count"] += 1
            context["first_seen_at"] = min(
                context["first_seen_at"] or occurred_at, occurred_at)
            context["last_seen_at"] = max(
                context["last_seen_at"] or occurred_at, occurred_at)
            if user_agent:
                context["user_agents"].add(user_agent)
            login_events.append({
                "occurred_at": occurred_at,
                "ip": ip,
                "browser": browser,
                "operating_system": operating_system,
                "user_agent": user_agent,
                "request_id": str(row.get("RequestId") or ""),
            })
        context_rows = []
        for context in contexts.values():
            context["user_agents"] = sorted(context["user_agents"])
            context_rows.append(context)
        context_rows.sort(key=lambda item: item["last_seen_at"], reverse=True)
        login_events.sort(key=lambda item: item["occurred_at"], reverse=True)
        result.append({
            "user_id": user_id,
            "email": user["Email"],
            "first_name": user.get("FirstName") or "",
            "last_name": user.get("LastName") or "",
            "name": user["DisplayName"],
            "status": user["Status"],
            "successful_login_count": len(login_events),
            "source_ip_count": len({row["ip"] for row in login_events
                                    if row["ip"]}),
            "browser_profile_count": len({
                (row["browser"], row["operating_system"])
                for row in login_events}),
            "access_context_count": len(context_rows),
            "last_login_at": login_events[0]["occurred_at"]
            if login_events else "",
            "contexts": context_rows,
            "events": login_events,
        })
    return sorted(result, key=lambda item: item["email"].lower())


@router.get("")
def access_activity(limit: int = 5000) -> dict[str, Any]:
    """Return registered users and successful-login access contexts."""
    limit = min(max(limit, 1), 20_000)
    try:
        store = _identity()
        users = store._query(
            "SELECT UserId, Email, FirstName, LastName, DisplayName, Status "
            "FROM Users ORDER BY Email", {})
        events = store._query(
            "SELECT ActorUserId, OccurredAt, Ip, UserAgent, RequestId "
            "FROM AuditEvents WHERE Action='login.ok' "
            "ORDER BY OccurredAt DESC LIMIT @limit", {"limit": limit})
    except _google_api_error() as exc:
        raise _identity_unavailable(exc) from exc
    return {
        "available": True,
        "users": _summarize(users, events),
        "event_limit": limit,
        "note": (
            "Access contexts are IP/browser/OS combinations. They are risk "
            "signals, not a count of physical people or devices."),
    }


__all__ = ["_browser_family", "_os_family", "_summarize", "router"]
