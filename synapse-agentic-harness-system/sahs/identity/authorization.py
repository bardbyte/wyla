"""Canonical role and permission policy for Synapse identities."""

from __future__ import annotations

ADMIN_ROLE = "admin"
ANALYST_ROLE = "analyst"
STEWARD_ROLE = "steward"

PERMISSIONS: dict[str, str] = {
    "chat.use": "Open a chat and ask",
    "chat.autopilot": "Run queries under the limits without handing over",
    "skills.own": "Save skills that load for oneself",
    "skills.share": "Promote an own skill to the shared shelf",
    "knowledge.stage": "Stage a knowledge file for a business unit",
    "metrics.certify": "Move a metric to certified or back",
    "graph.build": "Run build-graph and compile",
    "sources.manage": "Add, patch, retire sources",
    "users.manage": "Invite, grant roles, lock, disable",
    "audit.read": "Read the audit",
}

_ANALYST_PERMISSIONS = frozenset({
    "chat.use",
    "chat.autopilot",
    "skills.own",
    "knowledge.stage",
})

ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    ADMIN_ROLE: frozenset(PERMISSIONS),
    ANALYST_ROLE: _ANALYST_PERMISSIONS,
    STEWARD_ROLE: _ANALYST_PERMISSIONS | {"metrics.certify", "skills.share"},
}

ROLE_SURFACES: dict[str, tuple[str, ...]] = {
    ADMIN_ROLE: ("admin", "synapse"),
    ANALYST_ROLE: ("synapse",),
    STEWARD_ROLE: ("synapse",),
}

ROLE_DESCRIPTIONS: dict[str, str] = {
    ADMIN_ROLE: "Runs the graph: builds, sources, reviews, users",
    ANALYST_ROLE: "Asks: the Synapse surface, chats, artifacts, own skills",
    STEWARD_ROLE: "Decides: certifies and deprecates metrics",
}


def permissions_for_roles(roles: list[str] | tuple[str, ...]) -> list[str]:
    """Return the stable union of permissions granted by active roles."""
    granted: set[str] = set()
    for role in roles:
        granted.update(ROLE_PERMISSIONS.get(role, ()))
    return sorted(granted)


# ── added here, for the identity store this repository runs ──────────
# (the block above is the enterprise branch's file as written; the
# store needs the surfaces a set of roles opens and a way to tell a
# known role from a typo, so these live under the same names)

ROLES: tuple[str, ...] = tuple(ROLE_PERMISSIONS)


def surfaces_for_roles(roles: list[str] | tuple[str, ...]) -> list[str]:
    """Which apps the roles may open, in a stable order."""
    out: set[str] = set()
    for role in roles or ():
        out |= set(ROLE_SURFACES.get(str(role).lower(), ()))
    return [s for s in ("admin", "synapse") if s in out]


def is_known_role(role: str) -> bool:
    return str(role).lower() in ROLE_PERMISSIONS


__all__ = [
    "ADMIN_ROLE",
    "ANALYST_ROLE",
    "PERMISSIONS",
    "ROLES",
    "ROLE_DESCRIPTIONS",
    "ROLE_PERMISSIONS",
    "ROLE_SURFACES",
    "STEWARD_ROLE",
    "is_known_role",
    "permissions_for_roles",
    "surfaces_for_roles",
]
