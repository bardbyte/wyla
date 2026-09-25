"""A stand-in for the Cloud Spanner SDK's ``Database`` object, for tests
that must prove the Spanner code path without a Spanner.

``SpannerDatabase`` (sahs/identity/database.py) speaks to the SDK
through four doors: ``database.snapshot()`` → ``execute_sql``, and
``database.run_in_transaction(fn)`` → a transaction with ``execute_sql``,
``insert``, ``insert_or_update``, ``update`` and ``execute_update``.
This double serves the same doors over a sqlite file that carries the
identity and chat tables, so the app's every write under
``SAHS_STORE=spanner`` — the typed parameters, the ``JsonObject`` cells,
``spanner.COMMIT_TIMESTAMP`` — runs through ``SpannerDatabase`` exactly
as it would against the real thing, and lands where a test can read it
back (``FakeSpannerDatabase.rows``).

Think of a flight simulator: the cockpit (``SpannerDatabase``) is the
real one, only the sky outside is painted. What is painted here, and
stays painted: the SDK's result types. Spanner returns TIMESTAMP as
``datetime``, BOOL as ``bool``, ARRAY as ``list`` and JSON as
``JsonObject``; this double converts the sqlite text back by column
name (``_TYPED`` below) so the readers see Spanner's shapes, but a
column it does not know comes back as sqlite stored it. Interleaving,
foreign keys and CHECK constraints are not enforced.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from sahs.assistant.spanner_store import CHAT_SQLITE_SCHEMA
from sahs.identity.database import COMMIT_TS, SqliteDatabase

_COMMIT_TIMESTAMP = "spanner.commit_timestamp()"

# how Spanner would type the columns the stores read back
_TYPED: dict[str, str] = {
    **{name: "timestamp" for name in (
        "CreatedAt", "UpdatedAt", "ExpiresAt", "AbsoluteExpiresAt", "LastSeenAt",
        "RevokedAt", "LockedUntil", "LastLoginAt", "PasswordChangedAt",
        "EmailVerifiedAt", "OccurredAt", "GrantedAt", "RetiredAt", "LinkedAt",
        "DeletedAt", "MfaPassedAt", "StagedAt", "Ts")},
    **{name: "bool" for name in (
        "Starred", "Archived", "Succeeded", "MustChangePassword", "MfaRequired",
        "IsSystem", "Shared")},
    **{name: "json" for name in (
        "Payload", "Spec", "Plan", "Handoff", "Notes", "Details", "Claims", "Params")},
    **{name: "array" for name in ("Skills", "Surfaces", "Scopes")},
}


def _typed(name: str, value: Any) -> Any:
    kind = _TYPED.get(name)
    if value is None or kind is None:
        return value
    if kind == "timestamp":
        text = str(value).replace("Z", "+00:00")
        try:
            stamp = datetime.fromisoformat(text)
        except ValueError:
            return value
        return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
    if kind == "bool":
        return bool(value)
    if kind in ("json", "array"):
        from google.cloud.spanner_v1 import JsonObject
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except ValueError:
                return value
        else:
            parsed = value
        if kind == "array":
            return list(parsed) if isinstance(parsed, list) else parsed
        return JsonObject(parsed)
    return value


def _cell(value: Any) -> Any:
    """An SDK cell back to what the sqlite file stores."""
    if isinstance(value, str) and value == _COMMIT_TIMESTAMP:
        return COMMIT_TS
    if hasattr(value, "serialize"):                # JsonObject
        return value.serialize()
    return value


class _Field:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


class _Result:
    """What ``execute_sql`` returns: iterable rows plus ``fields``."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        names = list(rows[0].keys()) if rows else []
        self.fields = [_Field(n) for n in names]
        self._rows = [tuple(_typed(n, row[n]) for n in names) for row in rows]

    def __iter__(self):
        return iter(self._rows)


class _Snapshot:
    def __init__(self, db: SqliteDatabase) -> None:
        self._db = db

    def __enter__(self) -> "_Snapshot":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute_sql(self, sql: str, params: dict[str, Any] | None = None,
                    param_types: dict[str, Any] | None = None) -> _Result:
        return _Result(self._db.query(sql, params))


class _Transaction:
    def __init__(self, tx: Any) -> None:
        self._tx = tx

    def execute_sql(self, sql: str, params: dict[str, Any] | None = None,
                    param_types: dict[str, Any] | None = None) -> _Result:
        return _Result(self._tx.query(sql, params))

    def execute_update(self, sql: str, params: dict[str, Any] | None = None,
                       param_types: dict[str, Any] | None = None) -> int:
        return self._tx.execute_update(sql, params)

    @staticmethod
    def _rows(rows: Sequence[Sequence[Any]]) -> list[tuple[Any, ...]]:
        return [tuple(_cell(v) for v in row) for row in rows]

    def insert(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        self._tx.insert(table, tuple(columns), self._rows(values))

    def insert_or_update(self, table: str, columns: Sequence[str],
                         values: Sequence[Sequence[Any]]) -> None:
        self._tx.insert_or_update(table, tuple(columns), self._rows(values))

    def update(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        self._tx.update(table, tuple(columns), self._rows(values))


class FakeSpannerDatabase:
    """The SDK ``Database`` the store's ``SpannerDatabase`` wraps."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.sqlite = SqliteDatabase(path)
        self.sqlite.ensure(CHAT_SQLITE_SCHEMA)
        self.transactions = 0

    def snapshot(self, **kwargs: Any) -> _Snapshot:
        return _Snapshot(self.sqlite)

    def run_in_transaction(self, fn: Callable[[Any], Any], *args: Any, **kwargs: Any) -> Any:
        self.transactions += 1
        return self.sqlite.run(lambda tx: fn(_Transaction(tx), *args, **kwargs))

    # ── what a test reads back ──
    def rows(self, table: str, where: str = "", params: dict[str, Any] | None = None
             ) -> list[dict[str, Any]]:
        if not re.fullmatch(r"[A-Za-z_]+", table):
            raise ValueError(table)
        return self.sqlite.query(
            f"SELECT * FROM {table}" + (f" WHERE {where}" if where else ""), params)

    def count(self, table: str, where: str = "", params: dict[str, Any] | None = None) -> int:
        return len(self.rows(table, where, params))


__all__ = ["FakeSpannerDatabase"]
