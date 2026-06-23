-- V8__per_block_fts_and_chunks.sql
-- Spec 6 Plan 4 Task 2: per-block FTS5 + chunks schema contract.
--
-- Drops:
--   - vec_embeddings (entry-level embeddings retired; chunks own embeddings now)
--   - fts_entries (per-entry FTS; replaced by per-block fts_blocks)
--   - chunks.entry_id (chunks now block-addressed via block_id; entry reachable via JOIN)
--
-- Adds:
--   - fts_blocks virtual table (block_id, entry_id, type, text)
--   - blocks_ai / blocks_ad triggers to keep fts_blocks in sync
--
-- See docs/superpowers/specs/2026-06-22-content-storage-design.md §5.3, §10.

-- Drop legacy entry-level storage. vec_embeddings is a vec0 virtual table;
-- DROP IF EXISTS is safe even if already absent.
DROP TABLE IF EXISTS vec_embeddings;

-- Drop per-entry FTS (Plan 3's direct INSERT path becomes obsolete once
-- fts_blocks triggers fire on blocks INSERT).
DROP TABLE IF EXISTS fts_entries;

-- Chunks: drop entry_id. SQLite's ALTER TABLE DROP COLUMN refuses to drop a
-- column referenced in a FOREIGN KEY clause (error: "unknown column entry_id
-- in foreign key definition"), so we rebuild the table by hand. block_id
-- (added in V6) becomes the sole FK; entry is reachable via JOIN chunks →
-- blocks → entries.
DROP INDEX IF EXISTS idx_chunks_entry;
DROP TABLE chunks;
CREATE TABLE chunks (
  id          TEXT PRIMARY KEY,             -- ULID string
  block_id    TEXT NOT NULL,
  ordinal     INTEGER NOT NULL,             -- 0-based, ordering within block
  text        TEXT NOT NULL,                -- chunk content
  attrs       TEXT NOT NULL,                -- JSON object (includes parent_block_type)
  created_at  TEXT NOT NULL,                -- RFC3339
  updated_at  TEXT NOT NULL,                -- RFC3339
  FOREIGN KEY (block_id) REFERENCES blocks(id) ON DELETE CASCADE,
  UNIQUE(block_id, ordinal)
);

CREATE INDEX idx_chunks_block ON chunks(block_id, ordinal);

-- Per-block FTS5. block_id + entry_id + type are UNINDEXED (returned on
-- match, not tokenized). text is the searchable column.
CREATE VIRTUAL TABLE fts_blocks USING fts5(
    block_id UNINDEXED,
    entry_id UNINDEXED,
    type UNINDEXED,
    text
);

-- Triggers: blocks ↔ fts_blocks sync. entry_id is denormalized into
-- fts_blocks so search.fulltext can filter to a specific entry without JOIN.
CREATE TRIGGER blocks_ai AFTER INSERT ON blocks BEGIN
    INSERT INTO fts_blocks (block_id, entry_id, type, text)
    VALUES (new.id, new.entry_id, new.type, new.text);
END;

CREATE TRIGGER blocks_ad AFTER DELETE ON blocks BEGIN
    DELETE FROM fts_blocks WHERE block_id = old.id;
END;
