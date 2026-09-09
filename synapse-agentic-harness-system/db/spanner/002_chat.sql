-- ============================================================
-- Synapse on Spanner · 002 · the assistant store: chats, messages,
-- artifacts, projects, memory, files, events, own skills, knowledge
-- Mapped one-to-one from sahs/ask/store.py + sahs/assistant/store.py
-- (the SQLite store the laptop runs today), with the person as the
-- owner everywhere the laptop assumed one configured user. The
-- design, table by table: docs/spanner_schema.md §4.
-- ============================================================

-- A project: a folder with its own instructions and pinned skills.
CREATE TABLE ChatProjects (
  ProjectId     STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  OwnerUserId   STRING(36)  NOT NULL,
  Name          STRING(200) NOT NULL,
  Instructions  STRING(MAX) NOT NULL DEFAULT (''),
  Skills        ARRAY<STRING(64)> NOT NULL DEFAULT (ARRAY<STRING>[]),
  Archived      BOOL        NOT NULL DEFAULT (false),
  CreatedAt     TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdatedAt     TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT fk_projects_owner FOREIGN KEY (OwnerUserId) REFERENCES Users (UserId),
) PRIMARY KEY (ProjectId);

CREATE INDEX ChatProjectsByOwner ON ChatProjects (OwnerUserId, Archived, UpdatedAt DESC);

-- A chat. Kind keeps the two hats and the v2 chat apart; Model is the
-- plane the chat rides ('' = the deployment default); Skills the packs
-- pinned on it; Handoff and Notes the JSON the store kept.
CREATE TABLE ChatSessions (
  SessionId     STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  OwnerUserId   STRING(36)  NOT NULL,
  Kind          STRING(16)  NOT NULL DEFAULT ('assistant'),
  Title         STRING(300) NOT NULL DEFAULT (''),
  BuildId       STRING(64)  NOT NULL DEFAULT (''),
  ProjectId     STRING(36),
  Model         STRING(16)  NOT NULL DEFAULT (''),
  Skills        ARRAY<STRING(64)> NOT NULL DEFAULT (ARRAY<STRING>[]),
  Starred       BOOL        NOT NULL DEFAULT (false),
  Archived      BOOL        NOT NULL DEFAULT (false),
  Handoff       JSON,
  Notes         JSON,
  MessageCount  INT64       NOT NULL DEFAULT (0),
  CreatedAt     TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdatedAt     TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_sessions_kind CHECK (Kind IN ('analyst', 'steward', 'assistant')),
  CONSTRAINT ck_sessions_model CHECK (Model IN ('', 'vertex', 'gateway')),
  CONSTRAINT fk_sessions_owner FOREIGN KEY (OwnerUserId) REFERENCES Users (UserId),
  CONSTRAINT fk_sessions_project FOREIGN KEY (ProjectId) REFERENCES ChatProjects (ProjectId),
) PRIMARY KEY (SessionId);

-- the shelf: a person's chats, newest first, starred and archived apart
CREATE INDEX ChatSessionsByOwner ON ChatSessions (OwnerUserId, Archived, UpdatedAt DESC);
CREATE INDEX ChatSessionsByProject ON ChatSessions (ProjectId, UpdatedAt DESC);

-- Messages live inside their chat (interleaved): one split, one read.
-- Seq orders them; Payload keeps what the store kept (answer payload,
-- chips, the trace, the files a user message carried).
CREATE TABLE ChatMessages (
  SessionId   STRING(36)  NOT NULL,
  MessageId   STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  -- the chat's owner, repeated here so the search index can partition
  -- by person (a search spans a person's chats, never one chat)
  OwnerUserId STRING(36)  NOT NULL,
  Seq         INT64       NOT NULL,
  TurnId      STRING(24)  NOT NULL DEFAULT (''),
  Role        STRING(16)  NOT NULL,
  Text        STRING(MAX) NOT NULL DEFAULT (''),
  Text_Tokens TOKENLIST   AS (TOKENIZE_FULLTEXT(Text)) HIDDEN,
  Payload     JSON,
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_messages_role CHECK (Role IN ('user', 'assistant', 'choice')),
) PRIMARY KEY (SessionId, MessageId),
  INTERLEAVE IN PARENT ChatSessions ON DELETE CASCADE;

CREATE INDEX ChatMessagesBySeq ON ChatMessages (SessionId, Seq), INTERLEAVE IN ChatSessions;
CREATE INDEX ChatMessagesByTurn ON ChatMessages (SessionId, TurnId), INTERLEAVE IN ChatSessions;

-- Search chats (the fuzzy finder) reads titles and message text; a
-- search index, partitioned by the person, keeps it fast past the
-- first thousand chats: SEARCH(Text_Tokens, @q) WHERE OwnerUserId = @me.
CREATE SEARCH INDEX ChatMessagesText ON ChatMessages (Text_Tokens)
  PARTITION BY OwnerUserId;

-- Artifacts are versioned: an edit is a new version, never an
-- overwrite, so "what did the dashboard say on Tuesday" stays answerable.
CREATE TABLE ChatArtifacts (
  SessionId   STRING(36)  NOT NULL,
  ArtifactId  STRING(36)  NOT NULL,
  Version     INT64       NOT NULL,
  TurnId      STRING(24)  NOT NULL DEFAULT (''),
  Type        STRING(24)  NOT NULL,
  Title       STRING(300) NOT NULL DEFAULT (''),
  Spec        JSON        NOT NULL,
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_artifacts_type CHECK (Type IN (
    'chart', 'table', 'document', 'dashboard', 'diagram', 'query')),
) PRIMARY KEY (SessionId, ArtifactId, Version),
  INTERLEAVE IN PARENT ChatSessions ON DELETE CASCADE;

-- Plan versions and feedback, as the ask store keeps them.
CREATE TABLE ChatPlanVersions (
  SessionId   STRING(36)  NOT NULL,
  Version     INT64       NOT NULL,
  Parent      INT64,
  TurnId      STRING(24)  NOT NULL DEFAULT (''),
  Plan        JSON        NOT NULL,
  Summary     STRING(MAX) NOT NULL DEFAULT (''),
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (SessionId, Version),
  INTERLEAVE IN PARENT ChatSessions ON DELETE CASCADE;

CREATE TABLE ChatFeedback (
  SessionId   STRING(36)  NOT NULL,
  FeedbackId  STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  UserId      STRING(36)  NOT NULL,
  TurnId      STRING(24)  NOT NULL DEFAULT (''),
  Subject     STRING(24)  NOT NULL,
  Vote        STRING(8)   NOT NULL,
  Note        STRING(2000),
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_feedback_vote CHECK (Vote IN ('up', 'down')),
) PRIMARY KEY (SessionId, FeedbackId),
  INTERLEAVE IN PARENT ChatSessions ON DELETE CASCADE;

-- Files on a chat: the manifest the workspace keeps today. The bytes
-- go to Cloud Storage (ObjectPath), never into Spanner; the converted
-- text stays here because the model reads it as text anyway.
CREATE TABLE ChatFiles (
  SessionId   STRING(36)  NOT NULL,
  FileId      STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  Name        STRING(200) NOT NULL,
  Suffix      STRING(8)   NOT NULL,
  Mime        STRING(120) NOT NULL,
  Family      STRING(16)  NOT NULL,
  Rides       STRING(8)   NOT NULL,
  SizeBytes   INT64       NOT NULL,
  ObjectPath  STRING(1024),
  Text        STRING(MAX),
  TextChars   INT64       NOT NULL DEFAULT (0),
  Note        STRING(400),
  SentTurn    STRING(24),
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_files_rides CHECK (Rides IN ('inline', 'text', 'convert')),
) PRIMARY KEY (SessionId, FileId),
  INTERLEAVE IN PARENT ChatSessions ON DELETE CASCADE;

-- The event log the surface replays (the JSONL per session today):
-- one row per event, ordered by Seq, ninety days, tailed live through
-- a change stream instead of a poll when the app moves off the laptop.
CREATE TABLE ChatEvents (
  SessionId   STRING(36)  NOT NULL,
  Seq         INT64       NOT NULL,
  TurnId      STRING(24)  NOT NULL DEFAULT (''),
  Ev          STRING(32)  NOT NULL,
  Ts          TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  Payload     JSON        NOT NULL,
) PRIMARY KEY (SessionId, Seq),
  INTERLEAVE IN PARENT ChatSessions ON DELETE CASCADE,
  ROW DELETION POLICY (OLDER_THAN(Ts, INTERVAL 90 DAY));

CREATE CHANGE STREAM ChatEventsStream FOR ChatEvents
  OPTIONS (retention_period = '1d', value_capture_type = 'NEW_ROW');

-- Memory is bound to the person, scoped (global | project:<id>),
-- statused (active | retired — never deleted), disclosed in the prompt.
CREATE TABLE ChatMemories (
  UserId      STRING(36)  NOT NULL,
  MemoryId    STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  Text        STRING(2000) NOT NULL,
  Scope       STRING(48)  NOT NULL DEFAULT ('global'),
  Status      STRING(16)  NOT NULL DEFAULT ('active'),
  Source      STRING(24)  NOT NULL DEFAULT ('assistant'),
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  RetiredAt   TIMESTAMP,
  CONSTRAINT ck_memories_status CHECK (Status IN ('active', 'retired')),
) PRIMARY KEY (UserId, MemoryId),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;

CREATE INDEX ChatMemoriesActive ON ChatMemories (UserId, Status, Scope), INTERLEAVE IN Users;

-- A person's own skills (graph/skills/users/<owner>/ today): the
-- text is the pack; Shared marks one a steward promoted to the shelf.
CREATE TABLE UserSkills (
  UserId      STRING(36)  NOT NULL,
  Name        STRING(64)  NOT NULL,
  Title       STRING(200) NOT NULL,
  Description STRING(400) NOT NULL DEFAULT (''),
  Text        STRING(MAX) NOT NULL,
  Origin      STRING(16)  NOT NULL DEFAULT ('unreviewed'),
  Shared      BOOL        NOT NULL DEFAULT (false),
  SharedBy    STRING(36),
  CreatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_userskills_name CHECK (REGEXP_CONTAINS(Name, r'^[a-z0-9][a-z0-9-]{0,39}$')),
) PRIMARY KEY (UserId, Name),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE;

CREATE INDEX UserSkillsShared ON UserSkills (Shared, Name);

-- Knowledge files staged for a business unit (sources/artifacts/
-- today): the content, who staged it, and which build-graph run
-- ingested it (empty until one does — never silently pretended in).
CREATE TABLE KnowledgeFiles (
  FileId        STRING(36)  NOT NULL DEFAULT (GENERATE_UUID()),
  BusinessUnit  STRING(40)  NOT NULL,
  Name          STRING(120) NOT NULL,
  Ext           STRING(8)   NOT NULL DEFAULT ('md'),
  Content       STRING(MAX) NOT NULL,
  StagedBy      STRING(36)  NOT NULL,
  StagedAt      TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  IngestedRun   STRING(64),
  RetiredAt     TIMESTAMP,
  CONSTRAINT ck_knowledge_ext CHECK (Ext IN ('md', 'txt', 'csv', 'json', 'yaml', 'yml', 'sql')),
  CONSTRAINT fk_knowledge_stager FOREIGN KEY (StagedBy) REFERENCES Users (UserId),
) PRIMARY KEY (FileId);

CREATE UNIQUE INDEX KnowledgeFilesByName ON KnowledgeFiles (BusinessUnit, Name, Ext);
