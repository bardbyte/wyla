"""Roles, the permissions each holds, and the surfaces each may open.

The schema (``db/spanner/001_identity.sql``) keeps roles and permissions
as rows so they can change without a deploy; this module is the code
the app enforces with, and the seed the store writes when a role row is
missing. The two are kept in step by hand: the names here are the names
in the schema's seed comment.

    admin     every permission; opens the admin console and Synapse
    analyst   asks: chats, autopilot under the limits, own skills,
              staging a knowledge file; opens Synapse
    steward   the analyst's set plus certifying metrics; opens Synapse
"""

from __future__ import annotations

from typing import Iterable

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

_ANALYST = frozenset({"chat.use", "chat.autopilot", "skills.own", "knowledge.stage"})

ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    "admin": frozenset(PERMISSIONS),
    "analyst": _ANALYST,
    "steward": _ANALYST | {"metrics.certify"},
}

ROLE_SURFACES: dict[str, tuple[str, ...]] = {
    "admin": ("admin", "synapse"),
    "analyst": ("synapse",),
    "steward": ("synapse",),
}

ROLE_DESCRIPTIONS: dict[str, str] = {
    "admin": "Runs the graph: builds, sources, reviews, users",
    "analyst": "Asks: the Synapse surface, chats, artifacts, own skills",
    "steward": "Decides: certifies and deprecates metrics",
}

ROLES: tuple[str, ...] = tuple(ROLE_PERMISSIONS)


def permissions_for_roles(roles: Iterable[str]) -> list[str]:
    """The union of the roles' permissions, sorted; unknown roles add nothing."""
    out: set[str] = set()
    for role in roles or ():
        out |= ROLE_PERMISSIONS.get(str(role).lower(), frozenset())
    return sorted(out)


def surfaces_for_roles(roles: Iterable[str]) -> list[str]:
    """Which apps the roles may open, in a stable order."""
    out: set[str] = set()
    for role in roles or ():
        out |= set(ROLE_SURFACES.get(str(role).lower(), ()))
    return [s for s in ("admin", "synapse") if s in out]


def is_known_role(role: str) -> bool:
    return str(role).lower() in ROLE_PERMISSIONS


__all__ = ["PERMISSIONS", "ROLES", "ROLE_DESCRIPTIONS", "ROLE_PERMISSIONS",
           "ROLE_SURFACES", "is_known_role", "permissions_for_roles",
           "surfaces_for_roles"]
