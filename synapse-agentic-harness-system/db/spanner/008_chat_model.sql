-- ============================================================
-- Synapse on Spanner · 008 · the chat's model choice, and every
-- artifact type the assistant knows.
--
-- Two CHECK constraints in 002_chat.sql were narrower than what the
-- code writes (docs/spanner-wiring.md, "Schema notes"), and the
-- database — not the app — refused the row:
--
--   * ChatSessions.Model was STRING(16) with CHECK (Model IN ('',
--     'vertex', 'gateway')). The composer's model switch records a
--     catalog choice: a plane, or plane:model, such as
--     gateway:gemini-3.7-flash (24 characters). The column becomes
--     STRING(64) and the plane CHECK goes: the runtime validates the
--     choice against the catalog (sahs/assistant/runtime.py,
--     choice_for) before the store writes it, and the catalog is the
--     .env's, not the schema's.
--   * ChatArtifacts.Type listed chart, table, document, dashboard,
--     diagram and query; sahs/assistant/artifacts.py TYPES also has
--     kpi. The constraint is replaced by the registry's list, and
--     scripts/spanner_ddl_check.py holds the two equal from here on.
--
-- Apply after 002 (and after 007 on a database that has it). A fresh
-- database applied 001…008 and the live E1 database with this file
-- on top end up the same. Each ALTER is its own statement in one
-- batch; Spanner applies the batch atomically.
-- ============================================================

ALTER TABLE ChatSessions DROP CONSTRAINT ck_sessions_model;

ALTER TABLE ChatSessions ALTER COLUMN Model STRING(64) NOT NULL DEFAULT ('');

ALTER TABLE ChatArtifacts DROP CONSTRAINT ck_artifacts_type;

ALTER TABLE ChatArtifacts ADD CONSTRAINT ck_artifacts_type CHECK (Type IN (
  'chart', 'table', 'document', 'kpi', 'dashboard', 'diagram'));
