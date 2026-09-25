-- ============================================================
-- Synapse on Spanner · 006 · sign-in through an identity provider
-- GoogleSQL dialect. Apply after 001 (Users).
--
-- Two small tables the Okta sign-in needs and the email-and-password
-- path never did:
--   * ExternalIdentities: which provider subject is which person. The
--     subject is the stable key; the email is what people recognise and
--     what links a first sign-in to an account that already exists.
--   * AuthStates: the one-time state of an authorization request in
--     flight (PKCE verifier, nonce, where to return), so the callback may
--     land on any pod. Rows die an hour after they expire.
-- ============================================================

CREATE TABLE ExternalIdentities (
  Provider      STRING(32)  NOT NULL,          -- okta | google
  Issuer        STRING(512) NOT NULL,          -- the OIDC issuer URL
  Subject       STRING(256) NOT NULL,          -- the provider's stable id
  UserId        STRING(36)  NOT NULL,
  Email         STRING(320) NOT NULL,
  LinkedAt      TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  LastLoginAt   TIMESTAMP,
  Claims        JSON,                          -- the last ID token's claims, minus secrets
  CONSTRAINT fk_external_identity_user FOREIGN KEY (UserId) REFERENCES Users (UserId),
) PRIMARY KEY (Provider, Issuer, Subject);

CREATE INDEX ExternalIdentitiesByUser ON ExternalIdentities (UserId);

CREATE TABLE AuthStates (
  State         STRING(128) NOT NULL,
  Kind          STRING(32)  NOT NULL,          -- okta_signin | google_connect
  UserId        STRING(36),
  Payload       JSON        NOT NULL,
  CreatedAt     TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  ExpiresAt     TIMESTAMP   NOT NULL,
) PRIMARY KEY (State),
  ROW DELETION POLICY (OLDER_THAN(ExpiresAt, INTERVAL 1 HOUR));
