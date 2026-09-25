"""People, sessions, roles and the audit, on Spanner or the sqlite stand-in.

The store is the one place that reads and writes the identity tables
(``db/spanner/001_identity.sql``, ``004_google_oauth.sql``,
``006_external_identities.sql``). Everything the app needs of a person
comes back as one dict::

    {"user_id", "email", "username", "name", "first_name", "last_name",
     "status", "roles": [...], "surfaces": [...], "permissions": [...]}

Sessions are opaque tokens: the cookie carries 32 random bytes, the
table holds their SHA-256. Passwords, for the email-and-password path,
are Argon2id over an HMAC of the password with the deployment's pepper.
Roles are rows, seeded from ``sahs.identity.authorization`` the first
time one is missing, so a fresh database needs no bootstrap job.

Two sign-in paths meet here. The email-and-password path (``signup``,
``login``, ``direct_reset_password``) and the identity-provider path
(``find_or_create_external_user``, ``set_roles``) both end in
``issue_session``; the app never learns which one a person used.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from sahs.identity import authorization as authz
from sahs.identity.database import COMMIT_TS, Database, open_database, utcnow
from sahs.spanner import AuthSettings, SpannerSettings

logger = logging.getLogger(__name__)

STATUSES = ("pending_verification", "active", "locked", "disabled", "deleted")
_USERNAME_OK = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,63}$")
_USER_COLUMNS = ("UserId, Email, Username, DisplayName, FirstName, LastName, Status, "
                 "FailedLoginCount, LockedUntil, LastLoginAt, MustChangePassword")


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def hash_token(token: str) -> bytes:
    return hashlib.sha256((token or "").encode("utf-8")).digest()


def _ts(value: Any) -> datetime | None:
    """A timestamp as the database returned it → aware UTC datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    text = str(value)
    if text.startswith("["):
        try:
            return [str(v) for v in json.loads(text)]
        except ValueError:
            return []
    return [text] if text else []


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


class IdentityStore:
    """The identity tables, behind the verbs the app uses."""

    def __init__(self, settings: SpannerSettings, auth: AuthSettings | None = None, *,
                 database: Database | None = None) -> None:
        self.settings = settings
        self.auth = auth or AuthSettings()
        self.db: Database = database if database is not None else open_database(settings)
        self._hasher: Any = None
        self._roles_ready = False

    # ── the raw read the admin routes use ─────────────────────
    def _query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return self.db.query(sql, params or {})

    # ── people ────────────────────────────────────────────────
    def _user_row(self, *, user_id: str | None = None,
                  email: str | None = None) -> dict[str, Any] | None:
        if user_id:
            rows = self._query(
                f"SELECT {_USER_COLUMNS} FROM Users WHERE UserId = @id "
                "AND Status != 'deleted'", {"id": user_id})
        elif email:
            rows = self._query(
                f"SELECT {_USER_COLUMNS} FROM Users WHERE EmailNormalized = @email "
                "AND Status != 'deleted'", {"email": normalize_email(email)})
        else:
            return None
        return rows[0] if rows else None

    def _roles_of(self, user_id: str) -> list[str]:
        rows = self._query(
            "SELECT r.Name FROM UserRoles ur JOIN Roles r ON r.RoleId = ur.RoleId "
            "WHERE ur.UserId = @id AND ur.RevokedAt IS NULL "
            "AND (ur.ExpiresAt IS NULL OR ur.ExpiresAt > @now) ORDER BY r.Name",
            {"id": user_id, "now": utcnow()})
        return [str(row["Name"]) for row in rows]

    def _shape(self, row: dict[str, Any], roles: list[str]) -> dict[str, Any]:
        last_login = _ts(row.get("LastLoginAt"))
        return {
            "user_id": str(row["UserId"]),
            "email": str(row["Email"]),
            "username": str(row.get("Username") or ""),
            "name": str(row.get("DisplayName") or ""),
            "first_name": str(row.get("FirstName") or ""),
            "last_name": str(row.get("LastName") or ""),
            "status": str(row.get("Status") or ""),
            "roles": roles,
            "surfaces": authz.surfaces_for_roles(roles),
            "permissions": authz.permissions_for_roles(roles),
            "must_change_password": bool(row.get("MustChangePassword")),
            "last_login_at": last_login.isoformat() if last_login else "",
        }

    def user(self, user_id: str) -> dict[str, Any] | None:
        row = self._user_row(user_id=user_id)
        return self._shape(row, self._roles_of(user_id)) if row else None

    def user_by_email(self, email: str) -> dict[str, Any] | None:
        row = self._user_row(email=email)
        return self._shape(row, self._roles_of(str(row["UserId"]))) if row else None

    def list_users(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._query(
            f"SELECT {_USER_COLUMNS} FROM Users WHERE Status != 'deleted' "
            "ORDER BY Email LIMIT @limit", {"limit": int(limit)})
        return [self._shape(row, self._roles_of(str(row["UserId"]))) for row in rows]

    def _username_for(self, email: str) -> str:
        local = normalize_email(email).split("@", 1)[0]
        base = re.sub(r"[^a-z0-9._-]", "_", local).strip("._-") or "user"
        if not base[0].isalnum():
            base = "u" + base
        while len(base) < 3:
            base += "0"
        base = base[:56]
        candidate = base
        while self._query("SELECT UserId FROM Users WHERE UsernameNormalized = @u",
                          {"u": candidate}):
            candidate = f"{base}-{secrets.token_hex(2)}"
        return candidate

    def _create_user(self, *, email: str, display_name: str, first_name: str = "",
                     last_name: str = "", status: str = "active") -> str:
        user_id = str(uuid.uuid4())
        username = self._username_for(email)
        display = (display_name or f"{first_name} {last_name}".strip()
                   or email.split("@", 1)[0])

        def work(tx: Any) -> None:
            tx.insert("Users",
                      ("UserId", "Email", "Username", "DisplayName", "FirstName",
                       "LastName", "Status", "FailedLoginCount", "MustChangePassword",
                       "MfaRequired", "CreatedAt", "UpdatedAt"),
                      [(user_id, email.strip(), username, display[:200],
                        (first_name or None), (last_name or None), status, 0, False,
                        False, COMMIT_TS, COMMIT_TS)])

        self.db.run(work)
        return user_id

    # ── sessions ──────────────────────────────────────────────
    def issue_session(self, user_id: str, *, ip: str = "", user_agent: str = "") -> str:
        token = secrets.token_urlsafe(32)
        now = utcnow()

        def work(tx: Any) -> None:
            tx.insert("AuthSessions",
                      ("SessionId", "UserId", "TokenHash", "CreatedAt", "LastSeenAt",
                       "ExpiresAt", "AbsoluteExpiresAt", "Ip", "UserAgent"),
                      [(str(uuid.uuid4()), user_id, hash_token(token), COMMIT_TS, now,
                        now + timedelta(minutes=self.auth.idle_minutes),
                        now + timedelta(hours=self.auth.session_hours),
                        (ip or None), (user_agent or None))])

        self.db.run(work)
        return token

    def session_user(self, token: str) -> dict[str, Any] | None:
        if not token:
            return None
        rows = self._query(
            "SELECT SessionId, UserId, ExpiresAt, AbsoluteExpiresAt FROM AuthSessions "
            "WHERE TokenHash = @h AND RevokedAt IS NULL", {"h": hash_token(token)})
        if not rows:
            return None
        row = rows[0]
        now = utcnow()
        expires = _ts(row.get("ExpiresAt"))
        absolute = _ts(row.get("AbsoluteExpiresAt"))
        if (expires and expires <= now) or (absolute and absolute <= now):
            return None
        user = self.user(str(row["UserId"]))
        if user is None or user["status"] != "active":
            return None
        self._touch_session(str(row["SessionId"]), now, absolute)
        return user

    def _touch_session(self, session_id: str, now: datetime,
                       absolute: datetime | None) -> None:
        expires = now + timedelta(minutes=self.auth.idle_minutes)
        if absolute and expires > absolute:
            expires = absolute
        try:
            self.db.run(lambda tx: tx.update(
                "AuthSessions", ("SessionId", "LastSeenAt", "ExpiresAt"),
                [(session_id, now, expires)]))
        except Exception as exc:  # noqa: BLE001 - a read must not fail on a touch
            logger.warning("session touch failed for %s: %s", session_id, exc)

    def logout(self, token: str) -> None:
        if not token:
            return
        self.db.run(lambda tx: tx.execute_update(
            "UPDATE AuthSessions SET RevokedAt = @now, RevokedReason = 'logout' "
            "WHERE TokenHash = @h AND RevokedAt IS NULL",
            {"now": utcnow(), "h": hash_token(token)}))

    def logout_all(self, user_id: str) -> None:
        self.db.run(lambda tx: tx.execute_update(
            "UPDATE AuthSessions SET RevokedAt = @now, RevokedReason = 'logout_all' "
            "WHERE UserId = @u AND RevokedAt IS NULL", {"now": utcnow(), "u": user_id}))

    # ── passwords: the email-and-password path ────────────────
    def _argon(self) -> Any:
        if self._hasher is None:
            from argon2 import PasswordHasher
            self._hasher = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=4)
        return self._hasher

    def _prehash(self, password: str) -> str:
        if self.auth.pepper:
            return hmac.new(self.auth.pepper.encode("utf-8"), password.encode("utf-8"),
                            hashlib.sha256).hexdigest()
        return password

    def _password_record(self, password: str) -> tuple[str, dict[str, Any], int]:
        encoded = self._argon().hash(self._prehash(password))
        return encoded, {"time_cost": 3, "memory_cost": 65536, "parallelism": 4}, \
            (1 if self.auth.pepper else 0)

    def _password_ok(self, stored: str, password: str) -> bool:
        try:
            return bool(self._argon().verify(stored, self._prehash(password)))
        except Exception:  # noqa: BLE001 - argon2 raises on mismatch and malformed hashes
            return False

    def _current_credential(self, user_id: str) -> dict[str, Any] | None:
        rows = self._query(
            "SELECT CredentialId, PasswordHash, PepperVersion FROM UserCredentials "
            "WHERE UserId = @u AND Kind = 'password' AND RetiredAt IS NULL "
            "ORDER BY CreatedAt DESC LIMIT 1", {"u": user_id})
        return rows[0] if rows else None

    def _store_password(self, tx: Any, user_id: str, password: str) -> None:
        encoded, params, pepper_version = self._password_record(password)
        tx.execute_update(
            "UPDATE UserCredentials SET RetiredAt = @now WHERE UserId = @u "
            "AND RetiredAt IS NULL", {"now": utcnow(), "u": user_id})
        tx.insert("UserCredentials",
                  ("UserId", "CredentialId", "Kind", "PasswordHash", "Algorithm",
                   "Params", "PepperVersion", "CreatedAt"),
                  [(user_id, str(uuid.uuid4()), "password", encoded, "argon2id",
                    params, pepper_version, COMMIT_TS)])

    def _check_signup_allowed(self, email: str) -> None:
        normalized = normalize_email(email)
        if "@" not in normalized or normalized.startswith("@"):
            raise ValueError("enter a valid email address")
        if self.auth.allowed_email_domains:
            domain = normalized.rsplit("@", 1)[-1]
            if domain not in self.auth.allowed_email_domains:
                raise ValueError("sign-up is limited to the organisation's email domains")
        if not self.auth.open_signup and normalized != self.auth.bootstrap_admin_email:
            raise ValueError("sign-up is by invitation")
        if self._user_row(email=normalized) is not None:
            raise ValueError("an account with this email already exists")

    def signup(self, email: str, *parts: str) -> tuple[dict[str, Any], str]:
        """``signup(email, first_name, last_name, password)`` or
        ``signup(email, display_name, password)`` → (user, session token)."""
        if len(parts) == 3:
            first_name, last_name, password = parts
            display = f"{first_name} {last_name}".strip()
        elif len(parts) == 2:
            display, password = parts
            first_name, last_name = "", ""
        else:
            raise TypeError("signup(email, first, last, password) or signup(email, name, password)")
        if len(password or "") < 8:
            raise ValueError("a password needs at least 8 characters")
        self._check_signup_allowed(email)
        user_id = self._create_user(email=email, display_name=display,
                                    first_name=first_name, last_name=last_name)
        self.db.run(lambda tx: self._store_password(tx, user_id, password))
        role = ("admin" if normalize_email(email) == self.auth.bootstrap_admin_email
                else self.auth.default_role)
        self.grant_role(user_id, role, granted_by=None)
        user = self.user(user_id)
        assert user is not None
        return user, self.issue_session(user_id)

    def login(self, email: str, password: str, *, ip: str = "",
              user_agent: str = "") -> tuple[dict[str, Any], str]:
        """→ (user, session token); ValueError names the refusal."""
        normalized = normalize_email(email)
        row = self._user_row(email=normalized)
        if row is None:
            self._login_attempt(normalized, None, ip, user_agent, False, "unknown_user")
            raise ValueError("invalid email or password")
        user_id = str(row["UserId"])
        now = utcnow()
        locked_until = _ts(row.get("LockedUntil"))
        if row.get("Status") == "locked" and locked_until and locked_until > now:
            self._login_attempt(normalized, user_id, ip, user_agent, False, "locked")
            raise ValueError("this account is locked; try again later")
        if row.get("Status") in ("disabled", "deleted"):
            self._login_attempt(normalized, user_id, ip, user_agent, False, "disabled")
            raise ValueError("this account is disabled")
        credential = self._current_credential(user_id)
        if credential is None or not self._password_ok(str(credential["PasswordHash"]),
                                                       password):
            failures = int(row.get("FailedLoginCount") or 0) + 1
            lock = failures >= self.auth.lock_after

            def fail(tx: Any) -> None:
                if lock:
                    tx.execute_update(
                        "UPDATE Users SET FailedLoginCount = @n, Status = 'locked', "
                        "LockedUntil = @until, UpdatedAt = @now WHERE UserId = @u",
                        {"n": failures, "u": user_id, "now": now,
                         "until": now + timedelta(minutes=self.auth.lock_minutes)})
                else:
                    tx.execute_update(
                        "UPDATE Users SET FailedLoginCount = @n, UpdatedAt = @now "
                        "WHERE UserId = @u", {"n": failures, "u": user_id, "now": now})

            self.db.run(fail)
            self._login_attempt(normalized, user_id, ip, user_agent, False, "bad_password")
            raise ValueError("invalid email or password")
        self.db.run(lambda tx: tx.execute_update(
            "UPDATE Users SET FailedLoginCount = 0, Status = 'active', LockedUntil = NULL, "
            "LastLoginAt = @now, UpdatedAt = @now WHERE UserId = @u",
            {"now": now, "u": user_id}))
        self._login_attempt(normalized, user_id, ip, user_agent, True, "ok")
        user = self.user(user_id)
        assert user is not None
        return user, self.issue_session(user_id, ip=ip, user_agent=user_agent)

    def direct_reset_password(self, email: str, password: str) -> str:
        row = self._user_row(email=email)
        if row is None:
            raise ValueError("no account with this email")
        if len(password or "") < 8:
            raise ValueError("a password needs at least 8 characters")
        user_id = str(row["UserId"])

        def work(tx: Any) -> None:
            self._store_password(tx, user_id, password)
            tx.execute_update(
                "UPDATE Users SET FailedLoginCount = 0, Status = 'active', "
                "LockedUntil = NULL, PasswordChangedAt = @now, UpdatedAt = @now "
                "WHERE UserId = @u", {"now": utcnow(), "u": user_id})

        self.db.run(work)
        self.logout_all(user_id)
        return user_id

    def _login_attempt(self, email_norm: str, user_id: str | None, ip: str,
                       user_agent: str, succeeded: bool, reason: str) -> None:
        try:
            self.db.run(lambda tx: tx.insert(
                "LoginAttempts",
                ("AttemptId", "EmailNormalized", "UserId", "Ip", "UserAgent",
                 "Succeeded", "Reason", "OccurredAt"),
                [(str(uuid.uuid4()), email_norm, user_id, ip or "", user_agent or None,
                  bool(succeeded), reason, COMMIT_TS)]))
        except Exception as exc:  # noqa: BLE001 - the attempt log never blocks a login
            logger.warning("login attempt not recorded: %s", exc)

    # ── roles ─────────────────────────────────────────────────
    def _ensure_roles(self) -> None:
        if self._roles_ready:
            return
        have_roles = {str(r["Name"]): str(r["RoleId"])
                      for r in self._query("SELECT RoleId, Name FROM Roles")}
        have_perms = {str(r["Name"]): str(r["PermissionId"])
                      for r in self._query("SELECT PermissionId, Name FROM Permissions")}

        def work(tx: Any) -> None:
            for name, description in authz.PERMISSIONS.items():
                if name not in have_perms:
                    have_perms[name] = str(uuid.uuid4())
                    tx.insert("Permissions", ("PermissionId", "Name", "Description"),
                              [(have_perms[name], name, description)])
            for role in authz.ROLES:
                if role not in have_roles:
                    have_roles[role] = str(uuid.uuid4())
                    tx.insert("Roles", ("RoleId", "Name", "Description", "Surfaces",
                                        "IsSystem", "CreatedAt"),
                              [(have_roles[role], role, authz.ROLE_DESCRIPTIONS.get(role, ""),
                                list(authz.ROLE_SURFACES.get(role, ())), True, COMMIT_TS)])
                    tx.insert("RolePermissions", ("RoleId", "PermissionId"),
                              [(have_roles[role], have_perms[p])
                               for p in sorted(authz.ROLE_PERMISSIONS[role])])

        self.db.run(work)
        self._roles_ready = True

    def _role_id(self, role: str) -> str:
        name = (role or "").strip().lower()
        if not authz.is_known_role(name):
            raise ValueError(f"unknown role: {role!r}")
        self._ensure_roles()
        rows = self._query("SELECT RoleId FROM Roles WHERE Name = @n", {"n": name})
        if not rows:
            self._roles_ready = False
            self._ensure_roles()
            rows = self._query("SELECT RoleId FROM Roles WHERE Name = @n", {"n": name})
        return str(rows[0]["RoleId"])

    def grant_role(self, user_id: str, role: str, *,
                   granted_by: str | None = None) -> dict[str, Any] | None:
        if self._user_row(user_id=user_id) is None:
            return None
        role_id = self._role_id(role)
        self.db.run(lambda tx: tx.insert_or_update(
            "UserRoles", ("UserId", "RoleId", "Scope", "GrantedBy", "GrantedAt",
                          "ExpiresAt", "RevokedAt"),
            [(user_id, role_id, "", granted_by, COMMIT_TS, None, None)]))
        return self.user(user_id)

    def revoke_role(self, user_id: str, role: str) -> dict[str, Any] | None:
        role_id = self._role_id(role)
        changed = self.db.run(lambda tx: tx.execute_update(
            "UPDATE UserRoles SET RevokedAt = @now WHERE UserId = @u AND RoleId = @r "
            "AND RevokedAt IS NULL", {"now": utcnow(), "u": user_id, "r": role_id}))
        if not changed:
            return None
        return self.user(user_id)

    def set_roles(self, user_id: str, roles: Iterable[str], *,
                  granted_by: str | None = "idp") -> dict[str, Any] | None:
        """Make the person's active roles exactly ``roles`` (the known
        ones): grant the missing, revoke the extra. The identity
        provider's groups drive this on every sign-in."""
        wanted = {str(r).lower() for r in roles if authz.is_known_role(str(r))}
        current = set(self._roles_of(user_id))
        for role in sorted(wanted - current):
            self.grant_role(user_id, role, granted_by=granted_by)
        for role in sorted(current - wanted):
            self.revoke_role(user_id, role)
        return self.user(user_id)

    # ── administration ────────────────────────────────────────
    def update_user(self, user_id: str, *, first_name: str | None = None,
                    last_name: str | None = None, name: str | None = None,
                    status: str | None = None) -> dict[str, Any] | None:
        if status is not None and status not in STATUSES:
            raise ValueError(f"status is one of {', '.join(STATUSES)}")
        if status == "deleted":
            raise ValueError("delete the account instead of setting its status")
        row = self._user_row(user_id=user_id)
        if row is None:
            return None
        sets, params = [], {"u": user_id, "now": utcnow()}
        for column, value in (("FirstName", first_name), ("LastName", last_name),
                              ("DisplayName", name), ("Status", status)):
            if value is not None:
                key = column.lower()
                sets.append(f"{column} = @{key}")
                params[key] = value.strip()[:200] if column != "Status" else value
        if not sets:
            return self.user(user_id)
        self.db.run(lambda tx: tx.execute_update(
            f"UPDATE Users SET {', '.join(sets)}, UpdatedAt = @now WHERE UserId = @u",
            params))
        if status in ("disabled", "locked"):
            self.logout_all(user_id)
        return self.user(user_id)

    def delete_user(self, user_id: str) -> bool:
        if self._user_row(user_id=user_id) is None:
            return False
        now = utcnow()
        self.db.run(lambda tx: tx.execute_update(
            "UPDATE Users SET Status = 'deleted', DeletedAt = @now, UpdatedAt = @now "
            "WHERE UserId = @u", {"now": now, "u": user_id}))
        self.logout_all(user_id)
        return True

    # ── the audit ─────────────────────────────────────────────
    def record_audit(self, action: str, outcome: str, *,
                     actor_user_id: str | None = None,
                     subject_user_id: str | None = None,
                     details: dict[str, Any] | None = None, ip: str = "",
                     user_agent: str = "", request_id: str = "") -> None:
        """Append one audit row. Never raises: a failed audit write is
        logged, because the action it describes has already happened."""
        try:
            self.db.run(lambda tx: tx.insert(
                "AuditEvents",
                ("EventId", "OccurredAt", "ActorUserId", "SubjectUserId", "Action",
                 "Outcome", "Ip", "UserAgent", "RequestId", "Details"),
                [(str(uuid.uuid4()), COMMIT_TS, actor_user_id, subject_user_id,
                  action[:64], outcome[:16], (ip or None), (user_agent or None),
                  (request_id or None), (details or {}))]))
        except Exception as exc:  # noqa: BLE001
            logger.error("audit row not written (%s %s): %s", action, outcome, exc)

    # ── the Google connection for user-delegated BigQuery ─────
    def google_connection(self, user_id: str) -> dict[str, Any] | None:
        rows = self._query(
            "SELECT UserId, Provider, GoogleSubject, GoogleEmail, RefreshTokenCiphertext, "
            "Scopes, CreatedAt, UpdatedAt FROM GoogleOAuthConnections "
            "WHERE UserId = @u AND Provider = 'google' AND RevokedAt IS NULL",
            {"u": user_id})
        if not rows:
            return None
        row = dict(rows[0])
        row["Scopes"] = _list(row.get("Scopes"))
        cipher = row.get("RefreshTokenCiphertext")
        if isinstance(cipher, str):
            row["RefreshTokenCiphertext"] = cipher.encode("utf-8")
        return row

    def save_google_connection(self, user_id: str, *, subject: str, email: str,
                               refresh_token: str, scopes: Iterable[str],
                               settings: Any) -> None:
        from sahs.util.google_auth.oauth import encrypt_refresh_token
        ciphertext = encrypt_refresh_token(refresh_token, settings)
        self.db.run(lambda tx: tx.insert_or_update(
            "GoogleOAuthConnections",
            ("UserId", "Provider", "GoogleSubject", "GoogleEmail", "RefreshTokenCiphertext",
             "Scopes", "CreatedAt", "UpdatedAt", "RevokedAt"),
            [(user_id, "google", subject, email, ciphertext, [str(s) for s in scopes],
              COMMIT_TS, COMMIT_TS, None)]))

    def revoke_google_connection(self, user_id: str) -> None:
        self.db.run(lambda tx: tx.execute_update(
            "UPDATE GoogleOAuthConnections SET RevokedAt = @now, UpdatedAt = @now "
            "WHERE UserId = @u AND Provider = 'google' AND RevokedAt IS NULL",
            {"now": utcnow(), "u": user_id}))

    # ── the identity-provider path (Okta) ─────────────────────
    def find_or_create_external_user(self, *, provider: str, issuer: str, subject: str,
                                     email: str, name: str = "", first_name: str = "",
                                     last_name: str = "",
                                     claims: dict[str, Any] | None = None,
                                     default_roles: Iterable[str] = ()) -> dict[str, Any]:
        """The person behind a provider's subject: the one already linked,
        else the account with the same email (linked now), else a new
        account. Returns the user dict."""
        normalized = normalize_email(email)
        if not normalized:
            raise ValueError("the identity provider did not supply an email")
        now = utcnow()
        linked = self._query(
            "SELECT UserId FROM ExternalIdentities WHERE Provider = @p AND Issuer = @i "
            "AND Subject = @s", {"p": provider, "i": issuer, "s": subject})
        if linked:
            user_id = str(linked[0]["UserId"])
            self.db.run(lambda tx: tx.update(
                "ExternalIdentities",
                ("Provider", "Issuer", "Subject", "Email", "LastLoginAt", "Claims"),
                [(provider, issuer, subject, email.strip(), now, claims or {})]))
            if self._user_row(user_id=user_id) is None:
                raise ValueError("the linked account no longer exists")
        else:
            existing = self._user_row(email=normalized)
            if existing is not None:
                user_id = str(existing["UserId"])
            else:
                user_id = self._create_user(email=email, display_name=name,
                                            first_name=first_name, last_name=last_name)
                for role in default_roles:
                    self.grant_role(user_id, role, granted_by=provider)
            self.db.run(lambda tx: tx.insert(
                "ExternalIdentities",
                ("Provider", "Issuer", "Subject", "UserId", "Email", "LinkedAt",
                 "LastLoginAt", "Claims"),
                [(provider, issuer, subject, user_id, email.strip(), COMMIT_TS, now,
                  claims or {})]))
        if name or first_name or last_name:
            self.db.run(lambda tx: tx.execute_update(
                "UPDATE Users SET DisplayName = @d, FirstName = @f, LastName = @l, "
                "LastLoginAt = @now, UpdatedAt = @now WHERE UserId = @u",
                {"d": (name or f"{first_name} {last_name}".strip() or normalized)[:200],
                 "f": first_name or "", "l": last_name or "", "now": now, "u": user_id}))
        else:
            self.db.run(lambda tx: tx.execute_update(
                "UPDATE Users SET LastLoginAt = @now, UpdatedAt = @now WHERE UserId = @u",
                {"now": now, "u": user_id}))
        user = self.user(user_id)
        assert user is not None
        return user

    # ── one-time authorization states, so a callback may land on any pod ──
    def put_state(self, state: str, kind: str, payload: dict[str, Any], *,
                  user_id: str | None = None, ttl_seconds: int = 600) -> None:
        now = utcnow()
        self.db.run(lambda tx: tx.insert_or_update(
            "AuthStates", ("State", "Kind", "UserId", "Payload", "CreatedAt", "ExpiresAt"),
            [(state, kind, user_id, payload, COMMIT_TS,
              now + timedelta(seconds=int(ttl_seconds)))]))

    def pop_state(self, state: str, kind: str) -> dict[str, Any] | None:
        """The payload stored under ``state``, once; None when unknown,
        of another kind, or expired."""
        if not state:
            return None

        def work(tx: Any) -> dict[str, Any] | None:
            rows = tx.query(
                "SELECT Kind, UserId, Payload, ExpiresAt FROM AuthStates WHERE State = @s",
                {"s": state})
            if not rows:
                return None
            tx.execute_update("DELETE FROM AuthStates WHERE State = @s", {"s": state})
            row = rows[0]
            expires = _ts(row.get("ExpiresAt"))
            if str(row.get("Kind")) != kind or (expires and expires <= utcnow()):
                return None
            payload = _json(row.get("Payload"))
            if row.get("UserId"):
                payload.setdefault("user_id", str(row["UserId"]))
            return payload

        return self.db.run(work)


__all__ = ["IdentityStore", "STATUSES", "hash_token", "normalize_email"]
