-- ============================================================
-- Synapse on Spanner · 007 · the content that was still on the
-- filesystem: a chat file's bytes, and the review board.
--
-- ChatFileChunks holds the bytes of an uploaded file, in chunks of at
-- most 8 MiB, interleaved in the file's ChatFiles row (002_chat.sql) —
-- the pattern the build bundle uses (sahs/builds/spanner_store.py),
-- because the deployment has no bucket. The review board is the
-- ledger that sahs/assistant/reviews.py folds today, as tables: one
-- head row per submission, the text of every version, one row per
-- event (the fold runs over them, oldest first), and how far each
-- person has read their notices.
-- ============================================================

-- The bytes of one file on a chat: ChatFiles keeps the manifest and
-- the converted text; the raw bytes sit here, Seq-ordered, at most
-- 8 MiB a row (Spanner caps a cell at 10 MiB), read back in order.
CREATE TABLE ChatFileChunks (
  SessionId   STRING(36)  NOT NULL,
  FileId      STRING(36)  NOT NULL,
  Seq         INT64       NOT NULL,
  Chunk       BYTES(MAX)  NOT NULL,
) PRIMARY KEY (SessionId, FileId, Seq),
  INTERLEAVE IN PARENT ChatFiles ON DELETE CASCADE;

-- One submission on the board: the fields the person gave, who filed
-- it, who the system assigned to approve it (their band decides
-- whether they may), and the current head — Status and Version, kept
-- in step with the events below so the manager's queue is one index.
-- The board is shared across everyone on the deployment by design.
CREATE TABLE ReviewSubmissions (
  SubmissionId    STRING(36)  NOT NULL,
  Kind            STRING(16)  NOT NULL,
  Name            STRING(64)  NOT NULL,
  Title           STRING(200) NOT NULL DEFAULT (''),
  Description     STRING(400) NOT NULL DEFAULT (''),
  Purpose         STRING(600) NOT NULL DEFAULT (''),
  BusinessUnit    STRING(40)  NOT NULL DEFAULT (''),
  Ext             STRING(8)   NOT NULL DEFAULT ('md'),
  SubmitterUserId STRING(36)  NOT NULL,
  SubmitterName   STRING(200) NOT NULL,
  ApproverName    STRING(200) NOT NULL,
  ApproverBand    INT64       NOT NULL,
  Status          STRING(16)  NOT NULL DEFAULT ('pending'),
  Version         INT64       NOT NULL DEFAULT (1),
  CreatedAt       TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdatedAt       TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_reviews_kind CHECK (Kind IN ('skill', 'knowledge')),
  CONSTRAINT ck_reviews_status CHECK (Status IN ('pending', 'published', 'rejected', 'withdrawn')),
  CONSTRAINT ck_reviews_ext CHECK (Ext IN ('md', 'txt', 'csv', 'json', 'yaml', 'yml', 'sql')),
  CONSTRAINT ck_reviews_name CHECK (REGEXP_CONTAINS(Name, r'^[a-z0-9][a-z0-9-]{0,39}$')),
  CONSTRAINT fk_reviews_submitter FOREIGN KEY (SubmitterUserId) REFERENCES Users (UserId),
) PRIMARY KEY (SubmissionId);

-- the manager's queue, and a person's own submissions
CREATE INDEX ReviewSubmissionsByStatus ON ReviewSubmissions (Status, UpdatedAt DESC);
CREATE INDEX ReviewSubmissionsBySubmitter ON ReviewSubmissions (SubmitterUserId, UpdatedAt DESC);

-- The text of every version (files/<id>/v<n>.md today), with what the
-- person said about that version and who filed it (Actor: By and At
-- are reserved words in GoogleSQL, so the ledger's by/at are Actor and
-- OccurredAt here).
CREATE TABLE ReviewVersions (
  SubmissionId  STRING(36)  NOT NULL,
  Version       INT64       NOT NULL,
  Text          STRING(MAX) NOT NULL,
  Description   STRING(400) NOT NULL DEFAULT (''),
  Purpose       STRING(600) NOT NULL DEFAULT (''),
  Actor         STRING(200) NOT NULL DEFAULT (''),
  CreatedAt     TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (SubmissionId, Version),
  INTERLEAVE IN PARENT ReviewSubmissions ON DELETE CASCADE;

-- The ledger, one row per event, append-only: the state of a
-- submission is the fold of its rows in Seq order, exactly as the
-- JSONL was folded. Payload carries what the record carried beyond
-- the columns: the version an event is about, the model's read (an
-- ai_review), the published path (an approved), the submitter's slug
-- and the approver record (a submitted). The notices are these rows
-- read for a person, newest first.
CREATE TABLE ReviewEvents (
  SubmissionId  STRING(36)  NOT NULL,
  Seq           INT64       NOT NULL,
  Event         STRING(16)  NOT NULL,
  Actor         STRING(200) NOT NULL DEFAULT (''),
  Comment       STRING(4000) NOT NULL DEFAULT (''),
  Payload       JSON,
  OccurredAt    TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_review_events_event CHECK (Event IN (
    'submitted', 'resubmitted', 'ai_review', 'approved', 'rejected', 'withdrawn')),
) PRIMARY KEY (SubmissionId, Seq),
  INTERLEAVE IN PARENT ReviewSubmissions ON DELETE CASCADE;

-- the board's notices, newest first, across submissions
CREATE INDEX ReviewEventsByTime ON ReviewEvents (OccurredAt DESC);

-- How far each person has read their notices (seen.json today): a
-- notice is unread when its event is later than SeenAt.
CREATE TABLE ReviewSeen (
  UserId    STRING(36)  NOT NULL,
  SeenAt    TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (UserId),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;
