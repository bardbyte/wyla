-- ============================================================
-- Synapse on Spanner · 005 · the promoted build's bytes
--
-- 003_graph.sql keeps a build's manifest (Builds). This file keeps
-- the build itself: sahs/builds/spanner_store.py packs a compiled
-- build directory into one bundle, splits it into chunks under
-- Spanner's per-value ceiling, and writes them here so any pod can
-- materialise the promoted build without a shared disk
-- (MERIDIAN_BUILDS_SOURCE=spanner).
--
-- The write order is the reader's guarantee: the Builds row and the
-- BuildBundles row land in one transaction with Complete = false,
-- the chunks follow one commit each, and Complete flips to true only
-- after the last chunk. A reader joins on Complete = TRUE, so a
-- publish cut off halfway is invisible until it is re-run, and
-- re-publishing the same build id deletes the Builds row, which
-- cascades through both tables below.
-- ============================================================

-- One bundle per build: how it was packed and how to verify it.
CREATE TABLE BuildBundles (
  BuildId      STRING(64)  NOT NULL,
  Format       STRING(32)  NOT NULL,
  SizeBytes    INT64       NOT NULL,
  Sha256       STRING(64)  NOT NULL,
  FileCount    INT64       NOT NULL,
  ChunkBytes   INT64       NOT NULL,
  ChunkCount   INT64       NOT NULL,
  Complete     BOOL        NOT NULL DEFAULT (false),
  PublishedBy  STRING(36),
  PublishedAt  TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (BuildId),
  INTERLEAVE IN PARENT Builds ON DELETE CASCADE;

-- The bytes, in order. Chunk is at most ChunkBytes long (4 MiB in the
-- writer; Spanner's ceiling per value is 10 MiB).
CREATE TABLE BuildBundleChunks (
  BuildId  STRING(64)  NOT NULL,
  Seq      INT64       NOT NULL,
  Chunk    BYTES(MAX)  NOT NULL,
) PRIMARY KEY (BuildId, Seq),
  INTERLEAVE IN PARENT BuildBundles ON DELETE CASCADE;
