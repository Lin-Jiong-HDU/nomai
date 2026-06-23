-- V7__drop_entries_body.sql
-- Spec 6 Plan 3 Task 3: contract phase. entries.body is no longer used.
-- EntryService derives "body" from blocks for FTS5/chunks/embedding and
-- writes fts_entries directly (the V1 triggers that referenced new.body /
-- old.body are obsolete after this migration).
--
-- Plan 2's V6 was the expand phase (added blocks/fs_path/fs_mtime/block_id).
-- This is the matching contract: drop the legacy column + triggers.
--
-- See docs/superpowers/specs/2026-06-22-content-storage-design.md §5.1.

-- V1 created these triggers to keep fts_entries in sync with entries.body.
-- Drop them BEFORE the column drop because they reference new.body/old.body.
-- EntryService now writes fts_entries directly (INSERT/DELETE), so removing
-- the triggers does not break FTS5 maintenance.
DROP TRIGGER IF EXISTS entries_ai;
DROP TRIGGER IF EXISTS entries_ad;
DROP TRIGGER IF EXISTS entries_au;

-- SQLite supports ALTER TABLE ... DROP COLUMN as of 3.35.0 (2021). The
-- bundled rusqlite (0.32) ships a recent SQLite, so this is supported.
ALTER TABLE entries DROP COLUMN body;
