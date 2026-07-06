-- V10__fts_blocks_trigram.sql
-- 0.2.2: switch fts_blocks tokenizer from default unicode61 to trigram
-- so CJK (Chinese/Japanese/Korean) text is searchable via substring match.
-- fts_blocks is a derived index; this migration backfills it from existing
-- blocks rows (step 3), so no manual reindex or index.sync is required.
-- (index.sync does NOT help here — it diffs block mtimes vs the FS and
-- skips unchanged blocks, so the trigger never fires to repopulate.)
--
-- Trade-off: trigram requires queries of >=3 characters. search.fulltext
-- falls back to a LIKE scan for shorter queries (see service.rs
-- fulltext_search). See docs/reference.md "Retrieval modes".

-- 1. Drop triggers defined ON the `blocks` table that reference fts_blocks.
--    DROP TABLE fts_blocks does NOT remove triggers on other tables, so we
--    drop them explicitly to keep the schema unambiguous.
DROP TRIGGER IF EXISTS blocks_ai;
DROP TRIGGER IF EXISTS blocks_ad;
DROP TRIGGER IF EXISTS blocks_au;

-- 2. Recreate fts_blocks with the trigram tokenizer.
DROP TABLE IF EXISTS fts_blocks;
CREATE VIRTUAL TABLE fts_blocks USING fts5(
    block_id UNINDEXED,
    entry_id UNINDEXED,
    type UNINDEXED,
    text,
    tokenize='trigram'
);

-- 3. Backfill fts_blocks from existing blocks rows. The blocks_ai trigger
--    only fires on future writes; without this backfill, fts_blocks would
--    stay empty (fulltext returns nothing) until the user manually runs
--    index.rebuild. index.sync does NOT help here — it diffs entries/blocks
--    mtimes against the FS, and unchanged blocks are not rewritten, so the
--    trigger never fires. Self-contained migration = consistent post-state.
INSERT INTO fts_blocks (block_id, entry_id, type, text)
SELECT id, entry_id, type, text FROM blocks;

-- 4. Rebuild triggers (definitions identical to V8 blocks_ai/blocks_ad and
--    V9 blocks_au — only the fts_blocks tokenizer changed).
CREATE TRIGGER blocks_ai AFTER INSERT ON blocks BEGIN
    INSERT INTO fts_blocks (block_id, entry_id, type, text)
    VALUES (new.id, new.entry_id, new.type, new.text);
END;

CREATE TRIGGER blocks_ad AFTER DELETE ON blocks BEGIN
    DELETE FROM fts_blocks WHERE block_id = old.id;
END;

CREATE TRIGGER blocks_au AFTER UPDATE ON blocks BEGIN
    DELETE FROM fts_blocks WHERE block_id = old.id;
    INSERT INTO fts_blocks (block_id, entry_id, type, text)
    VALUES (new.id, new.entry_id, new.type, new.text);
END;
