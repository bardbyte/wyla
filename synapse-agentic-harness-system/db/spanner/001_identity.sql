-- ============================================================
-- Synapse on Spanner · 001 · identity, credentials, roles, sessions
-- GoogleSQL dialect. Apply in order: 001, 002, 003.
--
-- Principles, each a line in docs/spanner_schema.md (the design,
-- table by table; §3.8 names the five tables the first rollout
-- applies but leaves empty):
--   * keys are random UUIDs (GENERATE_UUID), never monotonic: no hot
--     tail on the split;
--   * nothing secret is stored recoverable: passwords are Argon2id
--     strings, session and reset tokens are SHA-256 digests, TOTP
--     secrets are KMS-wrapped bytes;
--   * every timestamp that means "when this row landed" is a commit
--     timestamp; "until when" columns drive row deletion policies;
--   * roles are data (Roles, Permissions), not code: a surface a
--     role may open is a column, so admin→the admin console, analyst→Synapse
--     and the steward's set change without a deploy;
--   * audit is append-only, on its own change stream.
-- ============================================================

CREATE TABLE Users (
  UserId              STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  Email               STRING(320) NOT NULL,
  -- the uniqueness key: case-folded, trimmed by the app before write
  EmailNormalized     STRING(320) NOT NULL AS (LOWER(TRIM(Email))) STORED,
  Username            STRING(64)  NOT NULL,
  UsernameNormalized  STRING(64)  NOT NULL AS (LOWER(TRIM(Username))) STORED,
  DisplayName         STRING(200) NOT NULL,
  -- pending_verification | active | locked | disabled | deleted
  Status              STRING(24)  NOT NULL DEFAULT ('pending_verification'),
  EmailVerifiedAt     TIMESTAMP,
  -- the lockout: failures since the last success, and until when
  FailedLoginCount    INT64       NOT NULL DEFAULT (0),
  LockedUntil         TIMESTAMP,
  LastLoginAt         TIMESTAMP,
  PasswordChangedAt   TIMESTAMP,
  MustChangePassword  BOOL        NOT NULL DEFAULT (false),
  MfaRequired         BOOL        NOT NULL DEFAULT (false),
  Timezone            STRING(64),
  Locale              STRING(16),
  CreatedAt           TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdatedAt           TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  -- soft delete: the row stays for audit joins, the person is
  -- anonymized by the deletion job (email/username replaced)
  DeletedAt           TIMESTAMP,
  CONSTRAINT ck_users_status CHECK (Status IN (
    'pending_verification', 'active', 'locked', 'disabled', 'deleted')),
  CONSTRAINT ck_users_username CHECK (
    REGEXP_CONTAINS(Username, r'^[A-Za-z0-9][A-Za-z0-9._-]{2,63}$')),
) PRIMARY KEY (UserId);

CREATE UNIQUE INDEX UsersByEmail ON Users (EmailNormalized);
CREATE UNIQUE INDEX UsersByUsername ON Users (UsernameNormalized);
CREATE INDEX UsersByStatus ON Users (Status, CreatedAt);

-- A password is one credential of a user; history stays so a
-- password cannot be reused (the app keeps the last 5 unretired
-- or retired rows for the check, then prunes).
CREATE TABLE UserCredentials (
  UserId          STRING(36)  NOT NULL,
  CredentialId    STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  Kind            STRING(16)  NOT NULL DEFAULT ('password'),
  -- the Argon2id encoded string: $argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>
  -- salt and parameters ride inside; the pepper does not (KMS)
  PasswordHash    STRING(512) NOT NULL,
  Algorithm       STRING(32)  NOT NULL DEFAULT ('argon2id'),
  Params          JSON,
  PepperVersion   INT64       NOT NULL DEFAULT (1),
  CreatedAt       TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  RetiredAt       TIMESTAMP,
  CONSTRAINT ck_credentials_kind CHECK (Kind IN ('password')),
) PRIMARY KEY (UserId, CredentialId),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;

-- The current password is the child row with RetiredAt IS NULL, read
-- by the key range (UserId): a person has a handful of rows, no index.

-- Roles are rows. Surfaces says which app a role may open:
-- 'admin' is the admin console at /, 'synapse' the analyst surface
-- at /synapse/. The steward's row exists so its set can be decided
-- without a schema change.
CREATE TABLE Roles (
  RoleId        STRING(36)    NOT NULL DEFAULT (GENERATE_UUID()),
  Name          STRING(32)    NOT NULL,
  Description   STRING(400),
  Surfaces      ARRAY<STRING(16)> NOT NULL DEFAULT (ARRAY<STRING>[]),
  IsSystem      BOOL          NOT NULL DEFAULT (true),
  CreatedAt     TIMESTAMP     NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (RoleId);

CREATE UNIQUE INDEX RolesByName ON Roles (Name);

CREATE TABLE Permissions (
  PermissionId  STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  -- dotted, resource.action: metrics.certify, users.manage, chat.use
  Name          STRING(64)  NOT NULL,
  Description   STRING(400),
) PRIMARY KEY (PermissionId);

CREATE UNIQUE INDEX PermissionsByName ON Permissions (Name);

CREATE TABLE RolePermissions (
  RoleId        STRING(36)  NOT NULL,
  PermissionId  STRING(36)  NOT NULL,
  CONSTRAINT fk_rolepermissions_permission
    FOREIGN KEY (PermissionId) REFERENCES Permissions (PermissionId),
) PRIMARY KEY (RoleId, PermissionId),
  INTERLEAVE IN PARENT Roles ON DELETE CASCADE;

-- Who holds which role, granted by whom, for how long. Scope narrows
-- a role to a business unit when the day comes ('' = everywhere).
CREATE TABLE UserRoles (
  UserId        STRING(36)  NOT NULL,
  RoleId        STRING(36)  NOT NULL,
  Scope         STRING(40)  NOT NULL DEFAULT (''),
  GrantedBy     STRING(36),
  GrantedAt     TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  ExpiresAt     TIMESTAMP,
  RevokedAt     TIMESTAMP,
  CONSTRAINT fk_userroles_role FOREIGN KEY (RoleId) REFERENCES Roles (RoleId),
) PRIMARY KEY (UserId, RoleId, Scope),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;

CREATE INDEX UserRolesByRole ON UserRoles (RoleId, UserId);

-- Login sessions: the cookie carries 32 random bytes; the table holds
-- their SHA-256. Idle expiry (ExpiresAt, pushed forward on use) and an
-- absolute one (AbsoluteExpiresAt, never pushed). Rows die a week after
-- the absolute expiry — long enough for an investigation to see them.
CREATE TABLE AuthSessions (
  SessionId         STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  UserId            STRING(36)  NOT NULL,
  TokenHash         BYTES(32)   NOT NULL,
  CreatedAt         TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  LastSeenAt        TIMESTAMP   NOT NULL,
  ExpiresAt         TIMESTAMP   NOT NULL,
  AbsoluteExpiresAt TIMESTAMP   NOT NULL,
  Ip                STRING(45),
  UserAgent         STRING(512),
  DeviceLabel       STRING(120),
  MfaPassedAt       TIMESTAMP,
  RevokedAt         TIMESTAMP,
  RevokedReason     STRING(64),
  CONSTRAINT fk_authsessions_user FOREIGN KEY (UserId) REFERENCES Users (UserId),
) PRIMARY KEY (SessionId),
  ROW DELETION POLICY (OLDER_THAN(AbsoluteExpiresAt, INTERVAL 7 DAY));

CREATE UNIQUE INDEX AuthSessionsByToken ON AuthSessions (TokenHash);
CREATE INDEX AuthSessionsByUser ON AuthSessions (UserId, RevokedAt, ExpiresAt);

-- ── phase 2: applied with the file, empty in the first rollout (docs/spanner_schema.md §3.8) ──
-- Refresh tokens rotate in families: each use issues a child and
-- marks the parent used; a used token presented again is reuse, and
-- the whole family is revoked (ReuseDetectedAt).
CREATE TABLE RefreshTokens (
  FamilyId        STRING(36)  NOT NULL,
  TokenId         STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  UserId          STRING(36)  NOT NULL,
  SessionId       STRING(36)  NOT NULL,
  TokenHash       BYTES(32)   NOT NULL,
  ParentTokenId   STRING(36),
  IssuedAt        TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  ExpiresAt       TIMESTAMP   NOT NULL,
  UsedAt          TIMESTAMP,
  RevokedAt       TIMESTAMP,
  ReuseDetectedAt TIMESTAMP,
  CONSTRAINT fk_refresh_user FOREIGN KEY (UserId) REFERENCES Users (UserId),
) PRIMARY KEY (FamilyId, TokenId),
  ROW DELETION POLICY (OLDER_THAN(ExpiresAt, INTERVAL 30 DAY));

CREATE UNIQUE INDEX RefreshTokensByHash ON RefreshTokens (TokenHash);

-- Every login attempt, success or not, for rate limiting by account
-- and by address and for the audit; a month is enough.
CREATE TABLE LoginAttempts (
  AttemptId       STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  EmailNormalized STRING(320) NOT NULL,
  UserId          STRING(36),
  Ip              STRING(45)  NOT NULL,
  UserAgent       STRING(512),
  Succeeded       BOOL        NOT NULL,
  -- bad_password | unknown_user | locked | mfa_failed | disabled | ok
  Reason          STRING(32)  NOT NULL,
  OccurredAt      TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (AttemptId),
  ROW DELETION POLICY (OLDER_THAN(OccurredAt, INTERVAL 30 DAY));

CREATE INDEX LoginAttemptsByEmail ON LoginAttempts (EmailNormalized, OccurredAt DESC);
CREATE INDEX LoginAttemptsByIp ON LoginAttempts (Ip, OccurredAt DESC);

-- ── phase 2: applied with the file, empty in the first rollout (docs/spanner_schema.md §3.8) ──
-- Second factors. A TOTP secret is wrapped by Cloud KMS (envelope
-- encryption): the ciphertext and the key version land here, the key
-- never does. Recovery codes are hashed, single-use.
CREATE TABLE MfaFactors (
  UserId          STRING(36)  NOT NULL,
  FactorId        STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  Kind            STRING(16)  NOT NULL,
  Label           STRING(120),
  SecretCiphertext BYTES(1024),
  KmsKeyVersion   STRING(256),
  CredentialPublicKey BYTES(2048),
  CredentialId    BYTES(1024),
  SignCount       INT64,
  CreatedAt       TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  VerifiedAt      TIMESTAMP,
  LastUsedAt      TIMESTAMP,
  RevokedAt       TIMESTAMP,
  CONSTRAINT ck_mfa_kind CHECK (Kind IN ('totp', 'webauthn')),
) PRIMARY KEY (UserId, FactorId),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;

CREATE TABLE MfaRecoveryCodes (
  UserId      STRING(36)  NOT NULL,
  CodeHash    BYTES(32)   NOT NULL,
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  UsedAt      TIMESTAMP,
) PRIMARY KEY (UserId, CodeHash),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;

-- ── phase 2: applied with the file, empty in the first rollout (docs/spanner_schema.md §3.8) ──
-- One table for every one-shot token a person receives by mail:
-- verify the address, reset the password, accept an invitation.
-- Hashed, single-use, short-lived; gone a day after expiry.
CREATE TABLE ActionTokens (
  TokenId     STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  UserId      STRING(36),
  Purpose     STRING(24)  NOT NULL,
  TokenHash   BYTES(32)   NOT NULL,
  Meta        JSON,
  CreatedBy   STRING(36),
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  ExpiresAt   TIMESTAMP   NOT NULL,
  UsedAt      TIMESTAMP,
  CONSTRAINT ck_actiontokens_purpose CHECK (Purpose IN (
    'verify_email', 'reset_password', 'invite')),
) PRIMARY KEY (TokenId),
  ROW DELETION POLICY (OLDER_THAN(ExpiresAt, INTERVAL 1 DAY));

CREATE UNIQUE INDEX ActionTokensByHash ON ActionTokens (TokenHash);

-- ── phase 2: applied with the file, empty in the first rollout (docs/spanner_schema.md §3.8) ──
-- An invitation names the address and the role it will hold; the
-- token that redeems it lives in ActionTokens (Purpose = 'invite').
CREATE TABLE Invitations (
  InviteId        STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  EmailNormalized STRING(320) NOT NULL,
  RoleId          STRING(36)  NOT NULL,
  InvitedBy       STRING(36)  NOT NULL,
  Message         STRING(1000),
  CreatedAt       TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  ExpiresAt       TIMESTAMP   NOT NULL,
  AcceptedAt      TIMESTAMP,
  AcceptedUserId  STRING(36),
  RevokedAt       TIMESTAMP,
  CONSTRAINT fk_invitations_role FOREIGN KEY (RoleId) REFERENCES Roles (RoleId),
) PRIMARY KEY (InviteId);

CREATE INDEX InvitationsByEmail ON Invitations (EmailNormalized, CreatedAt DESC);

-- What a person set for themselves: the default model plane, the
-- theme, the depth they prefer. Small JSON, keyed by name.
CREATE TABLE UserPreferences (
  UserId      STRING(36)  NOT NULL,
  Name        STRING(64)  NOT NULL,
  Value       JSON        NOT NULL,
  UpdatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (UserId, Name),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;

-- The audit: who did what to whom, from where, with what outcome.
-- Append-only by convention (the app has no UPDATE path), read
-- through its change stream by the security tooling, kept 400 days.
CREATE TABLE AuditEvents (
  EventId       STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  OccurredAt    TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  ActorUserId   STRING(36),
  SubjectUserId STRING(36),
  -- login.ok, login.failed, password.changed, role.granted, session.revoked …
  Action        STRING(64)  NOT NULL,
  Outcome       STRING(16)  NOT NULL,
  Ip            STRING(45),
  UserAgent     STRING(512),
  RequestId     STRING(64),
  Details       JSON,
) PRIMARY KEY (EventId),
  ROW DELETION POLICY (OLDER_THAN(OccurredAt, INTERVAL 400 DAY));

CREATE INDEX AuditEventsBySubject ON AuditEvents (SubjectUserId, OccurredAt DESC);
CREATE INDEX AuditEventsByActor ON AuditEvents (ActorUserId, OccurredAt DESC);
CREATE INDEX AuditEventsByAction ON AuditEvents (Action, OccurredAt DESC);

CREATE CHANGE STREAM AuditStream FOR AuditEvents
  OPTIONS (retention_period = '7d', value_capture_type = 'NEW_ROW');

-- The seed rows the app expects (applied once by the bootstrap job,
-- idempotent on Name). Kept here so the roles and their surfaces are
-- reviewed with the schema.
--
-- INSERT INTO Roles (Name, Description, Surfaces, IsSystem, CreatedAt) VALUES
--   ('admin',   'Runs the graph: builds, sources, reviews, users',
--               ['admin', 'synapse'], true, PENDING_COMMIT_TIMESTAMP()),
--   ('analyst', 'Asks: the Synapse surface, chats, artifacts, own skills',
--               ['synapse'], true, PENDING_COMMIT_TIMESTAMP()),
--   ('steward', 'Decides: certifies and deprecates metrics (set to be decided)',
--               ['synapse'], true, PENDING_COMMIT_TIMESTAMP());
-- INSERT INTO Permissions (Name, Description) VALUES
--   ('chat.use', 'Open a chat and ask'),
--   ('chat.autopilot', 'Run queries under the limits without handing over'),
--   ('skills.own', 'Save skills that load for oneself'),
--   ('skills.share', 'Promote an own skill to the shared shelf'),
--   ('knowledge.stage', 'Stage a knowledge file for a business unit'),
--   ('metrics.certify', 'Move a metric to certified or back'),
--   ('graph.build', 'Run build-graph and compile'),
--   ('sources.manage', 'Add, patch, retire sources'),
--   ('users.manage', 'Invite, grant roles, lock, disable'),
--   ('audit.read', 'Read the audit');
-- INSERT INTO RolePermissions (RoleId, PermissionId)
--   SELECT r.RoleId, p.PermissionId FROM Roles r CROSS JOIN Permissions p
--   WHERE r.Name = 'admin';                       -- admin holds every permission
-- INSERT INTO RolePermissions (RoleId, PermissionId)
--   SELECT r.RoleId, p.PermissionId FROM Roles r CROSS JOIN Permissions p
--   WHERE r.Name IN ('analyst', 'steward')        -- the steward's own set: to be decided
--     AND p.Name IN ('chat.use', 'chat.autopilot', 'skills.own', 'knowledge.stage');
