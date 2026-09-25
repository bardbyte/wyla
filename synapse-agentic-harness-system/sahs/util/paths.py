"""Where one person's runtime files live.

The single-developer mode keeps one store and one event log per lane
(``runs/chat/sessions.sqlite3``, ``runs/chat/events``). With an identity
service, each signed-in person gets their own under ``users/<id>/``
beside the shared paths, so two people never share a sqlite file or an
event log. Both the assistant and the Ask runtime use this one rule.
"""

from __future__ import annotations

from pathlib import Path


def owner_slug(owner_user_id: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_"
                   for c in (owner_user_id or "").strip())[:80]


def owner_paths(owner_user_id: str, store_path: Path,
                events_dir: Path | None) -> tuple[Path, Path | None]:
    """→ (store path, events dir) for the owner; the shared paths when
    no owner is named."""
    store_path = Path(store_path)
    if not (owner_user_id or "").strip():
        return store_path, Path(events_dir) if events_dir else None
    base = store_path.parent / "users" / owner_slug(owner_user_id)
    return base / store_path.name, (base / "events" if events_dir else None)


__all__ = ["owner_paths", "owner_slug"]
