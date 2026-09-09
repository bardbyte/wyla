"""Small TTL cache for tool responses. Keys are hashable tuples."""

from __future__ import annotations

import time
from typing import Any, Hashable


class TTLCache:
    def __init__(self, default_ttl_seconds: float = 600.0, max_entries: int = 2048):
        self._default_ttl = default_ttl_seconds
        self._max_entries = max_entries
        self._store: dict[Hashable, tuple[float, Any]] = {}

    def get(self, key: Hashable) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() >= expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: Hashable, value: Any, ttl_seconds: float | None = None) -> None:
        if len(self._store) >= self._max_entries:
            # Drop the soonest-to-expire entry — cheap, good-enough eviction
            oldest = min(self._store, key=lambda k: self._store[k][0])
            self._store.pop(oldest, None)
        ttl = self._default_ttl if ttl_seconds is None else ttl_seconds
        self._store[key] = (time.monotonic() + ttl, value)

    def clear(self) -> None:
        self._store.clear()
