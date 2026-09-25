"""One small database interface, two implementations.

The identity store speaks a handful of verbs: query rows, and run a
transaction that inserts, updates, or executes DML. ``SpannerDatabase``
maps them onto the Cloud Spanner SDK; ``SqliteDatabase`` maps them onto
a local sqlite file with the same tables, so the sign-in flow runs on a
laptop, in a test, or on a machine with no Spanner at all.

The store writes portable SQL: ``@name`` parameters, no Spanner-only
functions, timestamps passed as values rather than computed in SQL, ids
generated in Python. Commit timestamps are the one exception: the
``COMMIT_TS`` sentinel becomes Spanner's commit timestamp there and
"now" here.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence


class _CommitTimestamp:
    def __repr__(self) -> str:
        return "COMMIT_TS"


COMMIT_TS = _CommitTimestamp()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    """A fixed-width UTC timestamp, so text comparison orders like time."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


class Transaction(Protocol):
    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...
    def insert(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None: ...
    def insert_or_update(self, table: str, columns: Sequence[str],
                         rows: Sequence[Sequence[Any]]) -> None: ...
    def update(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None: ...
    def execute_update(self, sql: str, params: dict[str, Any] | None = None) -> int: ...


class Database(Protocol):
    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...
    def run(self, work: Callable[[Transaction], Any]) -> Any: ...


# ─────────────────────────────────────────────────────── Spanner ──
class SpannerDatabase:
    """The Cloud Spanner SDK behind the store's verbs."""

    def __init__(self, database: Any) -> None:
        self._database = database

    @classmethod
    def from_settings(cls, settings: Any) -> "SpannerDatabase":
        from sahs.builds.spanner_store import _database
        return cls(_database(settings))

    @staticmethod
    def _types(params: dict[str, Any]) -> dict[str, Any]:
        from google.cloud.spanner import param_types
        out: dict[str, Any] = {}
        for name, value in params.items():
            if isinstance(value, bool):
                out[name] = param_types.BOOL
            elif isinstance(value, int):
                out[name] = param_types.INT64
            elif isinstance(value, bytes):
                out[name] = param_types.BYTES
            elif isinstance(value, datetime):
                out[name] = param_types.TIMESTAMP
            elif isinstance(value, (list, tuple)):
                out[name] = param_types.Array(param_types.STRING)
            else:
                out[name] = param_types.STRING
        return out

    @staticmethod
    def _cell(value: Any) -> Any:
        from google.cloud import spanner
        if value is COMMIT_TS:
            return spanner.COMMIT_TIMESTAMP
        if isinstance(value, dict):
            return spanner.JsonObject(value)
        return value

    def _rows(self, result: Any) -> list[dict[str, Any]]:
        values = list(result)
        names = [field.name for field in result.fields]
        return [dict(zip(names, tuple(row), strict=True)) for row in values]

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        with self._database.snapshot() as snapshot:
            return self._rows(snapshot.execute_sql(
                sql, params=params, param_types=self._types(params)))

    def run(self, work: Callable[[Transaction], Any]) -> Any:
        outer = self

        class _Tx:
            def __init__(self, transaction: Any) -> None:
                self._t = transaction

            def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
                params = params or {}
                return outer._rows(self._t.execute_sql(
                    sql, params=params, param_types=outer._types(params)))

            def insert(self, table, columns, rows) -> None:
                self._t.insert(table, tuple(columns),
                               [tuple(outer._cell(v) for v in row) for row in rows])

            def insert_or_update(self, table, columns, rows) -> None:
                self._t.insert_or_update(table, tuple(columns),
                                         [tuple(outer._cell(v) for v in row) for row in rows])

            def update(self, table, columns, rows) -> None:
                self._t.update(table, tuple(columns),
                               [tuple(outer._cell(v) for v in row) for row in rows])

            def execute_update(self, sql: str, params: dict[str, Any] | None = None) -> int:
                params = params or {}
                return self._t.execute_update(
                    sql, params=params, param_types=outer._types(params))

        return self._database.run_in_transaction(lambda t: work(_Tx(t)))


# ──────────────────────────────────────────────────────── sqlite ──
SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS Users (
  UserId TEXT PRIMARY KEY, Email TEXT NOT NULL,
  EmailNormalized TEXT GENERATED ALWAYS AS (lower(trim(Email))) STORED,
  Username TEXT NOT NULL,
  UsernameNormalized TEXT GENERATED ALWAYS AS (lower(trim(Username))) STORED,
  DisplayName TEXT NOT NULL, FirstName TEXT, LastName TEXT,
  Status TEXT NOT NULL DEFAULT 'pending_verification', EmailVerifiedAt TEXT,
  FailedLoginCount INTEGER NOT NULL DEFAULT 0, LockedUntil TEXT, LastLoginAt TEXT,
  PasswordChangedAt TEXT, MustChangePassword INTEGER NOT NULL DEFAULT 0,
  MfaRequired INTEGER NOT NULL DEFAULT 0, Timezone TEXT, Locale TEXT,
  CreatedAt TEXT NOT NULL, UpdatedAt TEXT NOT NULL, DeletedAt TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS UsersByEmail ON Users (EmailNormalized);
CREATE UNIQUE INDEX IF NOT EXISTS UsersByUsername ON Users (UsernameNormalized);
CREATE TABLE IF NOT EXISTS UserCredentials (
  UserId TEXT NOT NULL, CredentialId TEXT NOT NULL, Kind TEXT NOT NULL DEFAULT 'password',
  PasswordHash TEXT NOT NULL, Algorithm TEXT NOT NULL DEFAULT 'argon2id', Params TEXT,
  PepperVersion INTEGER NOT NULL DEFAULT 1, CreatedAt TEXT NOT NULL, RetiredAt TEXT,
  PRIMARY KEY (UserId, CredentialId));
CREATE TABLE IF NOT EXISTS Roles (
  RoleId TEXT PRIMARY KEY, Name TEXT NOT NULL UNIQUE, Description TEXT,
  Surfaces TEXT NOT NULL DEFAULT '[]', IsSystem INTEGER NOT NULL DEFAULT 1, CreatedAt TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS Permissions (
  PermissionId TEXT PRIMARY KEY, Name TEXT NOT NULL UNIQUE, Description TEXT);
CREATE TABLE IF NOT EXISTS RolePermissions (
  RoleId TEXT NOT NULL, PermissionId TEXT NOT NULL, PRIMARY KEY (RoleId, PermissionId));
CREATE TABLE IF NOT EXISTS UserRoles (
  UserId TEXT NOT NULL, RoleId TEXT NOT NULL, Scope TEXT NOT NULL DEFAULT '',
  GrantedBy TEXT, GrantedAt TEXT NOT NULL, ExpiresAt TEXT, RevokedAt TEXT,
  PRIMARY KEY (UserId, RoleId, Scope));
CREATE TABLE IF NOT EXISTS AuthSessions (
  SessionId TEXT PRIMARY KEY, UserId TEXT NOT NULL, TokenHash BLOB NOT NULL UNIQUE,
  CreatedAt TEXT NOT NULL, LastSeenAt TEXT NOT NULL, ExpiresAt TEXT NOT NULL,
  AbsoluteExpiresAt TEXT NOT NULL, Ip TEXT, UserAgent TEXT, DeviceLabel TEXT,
  MfaPassedAt TEXT, RevokedAt TEXT, RevokedReason TEXT);
CREATE TABLE IF NOT EXISTS LoginAttempts (
  AttemptId TEXT PRIMARY KEY, EmailNormalized TEXT NOT NULL, UserId TEXT, Ip TEXT NOT NULL,
  UserAgent TEXT, Succeeded INTEGER NOT NULL, Reason TEXT NOT NULL, OccurredAt TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS AuditEvents (
  EventId TEXT PRIMARY KEY, OccurredAt TEXT NOT NULL, ActorUserId TEXT, SubjectUserId TEXT,
  Action TEXT NOT NULL, Outcome TEXT NOT NULL, Ip TEXT, UserAgent TEXT, RequestId TEXT, Details TEXT);
CREATE TABLE IF NOT EXISTS GoogleOAuthConnections (
  UserId TEXT NOT NULL, Provider TEXT NOT NULL DEFAULT 'google', GoogleSubject TEXT NOT NULL,
  GoogleEmail TEXT NOT NULL, RefreshTokenCiphertext BLOB NOT NULL, Scopes TEXT NOT NULL,
  CreatedAt TEXT NOT NULL, UpdatedAt TEXT NOT NULL, RevokedAt TEXT, PRIMARY KEY (UserId, Provider));
CREATE TABLE IF NOT EXISTS ExternalIdentities (
  Provider TEXT NOT NULL, Issuer TEXT NOT NULL, Subject TEXT NOT NULL, UserId TEXT NOT NULL,
  Email TEXT NOT NULL, LinkedAt TEXT NOT NULL, LastLoginAt TEXT, Claims TEXT,
  PRIMARY KEY (Provider, Issuer, Subject));
CREATE INDEX IF NOT EXISTS ExternalIdentitiesByUser ON ExternalIdentities (UserId);
CREATE TABLE IF NOT EXISTS AuthStates (
  State TEXT PRIMARY KEY, Kind TEXT NOT NULL, UserId TEXT, Payload TEXT NOT NULL,
  CreatedAt TEXT NOT NULL, ExpiresAt TEXT NOT NULL);
"""

_PARAM = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")
# how many leading columns form the primary key, for the update verb
_KEY_WIDTH = {"UserRoles": 3, "GoogleOAuthConnections": 2, "UserCredentials": 2,
              "RolePermissions": 2, "ExternalIdentities": 3}


class SqliteDatabase:
    """A local file that plays the identity database, for development and tests."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SQLITE_SCHEMA)

    # values: datetimes and JSON travel as text, lists as JSON arrays
    @staticmethod
    def _cell(value: Any) -> Any:
        if value is COMMIT_TS:
            return iso(utcnow())
        if isinstance(value, datetime):
            return iso(value)
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(list(value) if isinstance(value, tuple) else value)
        return value

    @classmethod
    def _params(cls, params: dict[str, Any] | None) -> dict[str, Any]:
        return {k: cls._cell(v) for k, v in (params or {}).items()}

    @staticmethod
    def _sql(sql: str) -> str:
        return _PARAM.sub(r":\1", sql)

    @staticmethod
    def _rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
        return [dict(row) for row in cursor.fetchall()]

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        with self._lock:
            return self._rows(self._conn.execute(self._sql(sql), self._params(params)))

    def run(self, work: Callable[[Transaction], Any]) -> Any:
        outer = self

        class _Tx:
            def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
                return outer._rows(outer._conn.execute(outer._sql(sql), outer._params(params)))

            def _write(self, verb: str, table: str, columns, rows) -> None:
                cols = ", ".join(columns)
                marks = ", ".join("?" for _ in columns)
                outer._conn.executemany(
                    f"{verb} INTO {table} ({cols}) VALUES ({marks})",
                    [tuple(outer._cell(v) for v in row) for row in rows])

            def insert(self, table, columns, rows) -> None:
                self._write("INSERT", table, columns, rows)

            def insert_or_update(self, table, columns, rows) -> None:
                self._write("INSERT OR REPLACE", table, columns, rows)

            def update(self, table, columns, rows) -> None:
                width = _KEY_WIDTH.get(table, 1)
                key_cols, rest = list(columns[:width]), list(columns[width:])
                sets = ", ".join(f"{c} = ?" for c in rest)
                where = " AND ".join(f"{c} = ?" for c in key_cols)
                for row in rows:
                    cells = [outer._cell(v) for v in row]
                    outer._conn.execute(f"UPDATE {table} SET {sets} WHERE {where}",
                                        tuple(cells[width:]) + tuple(cells[:width]))

            def execute_update(self, sql: str, params: dict[str, Any] | None = None) -> int:
                return outer._conn.execute(outer._sql(sql), outer._params(params)).rowcount

        with self._lock:
            self._conn.execute("BEGIN")
            try:
                result = work(_Tx())
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")
            return result


def open_database(settings: Any) -> Database:
    """The database the settings describe: sqlite for the stand-in, else Spanner."""
    if getattr(settings, "mode", "spanner") == "sqlite":
        return SqliteDatabase(settings.sqlite_path)
    return SpannerDatabase.from_settings(settings)


__all__ = ["COMMIT_TS", "Database", "SpannerDatabase", "SqliteDatabase",
           "Transaction", "iso", "open_database", "utcnow"]
