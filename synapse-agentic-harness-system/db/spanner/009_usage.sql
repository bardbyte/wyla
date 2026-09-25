-- ============================================================
-- Synapse on Spanner · 009 · what a chat cost: the usage totals on
-- ChatSessions.
--
-- The runtime adds a finished turn's usage to its chat's row
-- (sahs/assistant/runtime.py, _settle_usage → SpannerAssistantStore.
-- add_usage): tokens in and out, model calls, wall time, and one more
-- turn. Sub-turns of a multi-task turn count once, through the
-- parent's turn_done. The sidebar and Search chats read them per chat
-- ("8.2K tokens · 3 turns"); the People page reads them per person
-- (one SUM grouped by OwnerUserId, usage_by_owner). The same turn's
-- usage also rides the final assistant message's Payload as "usage",
-- for the footer under the answer.
--
-- Apply after 008. Each ALTER is its own statement in one batch;
-- Spanner applies the batch atomically. The sqlite stand-in adds the
-- same columns to a file from before this migration on first use.
-- ============================================================

ALTER TABLE ChatSessions ADD COLUMN TokensIn INT64 NOT NULL DEFAULT (0);

ALTER TABLE ChatSessions ADD COLUMN TokensOut INT64 NOT NULL DEFAULT (0);

ALTER TABLE ChatSessions ADD COLUMN ModelCalls INT64 NOT NULL DEFAULT (0);

ALTER TABLE ChatSessions ADD COLUMN ElapsedMs INT64 NOT NULL DEFAULT (0);

ALTER TABLE ChatSessions ADD COLUMN Turns INT64 NOT NULL DEFAULT (0);
