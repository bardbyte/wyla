-- Synapse on Spanner - 004 - user-delegated Google OAuth connections
-- Store only encrypted refresh tokens. Access tokens remain request-scoped.

CREATE TABLE GoogleOAuthConnections (
  UserId               STRING(36) NOT NULL,
  Provider             STRING(32) NOT NULL DEFAULT ('google'),
  GoogleSubject        STRING(256) NOT NULL,
  GoogleEmail          STRING(320) NOT NULL,
  RefreshTokenCiphertext BYTES(8192) NOT NULL,
  Scopes               ARRAY<STRING(256)> NOT NULL,
  CreatedAt            TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdatedAt            TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  RevokedAt            TIMESTAMP,
  CONSTRAINT fk_google_oauth_user FOREIGN KEY (UserId) REFERENCES Users (UserId),
) PRIMARY KEY (UserId, Provider),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;

CREATE UNIQUE INDEX GoogleOAuthBySubject
  ON GoogleOAuthConnections (Provider, GoogleSubject);
