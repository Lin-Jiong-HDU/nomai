-- V9__blocks_au_and_chunks_ad_triggers.sql
-- Spec 6 Plan 5 Task 1: triggers for block UPDATE + chunk DELETE cleanup.
--
-- blocks_au: AFTER UPDATE ON blocks, refresh fts_blocks. Without this,
-- block.update RPC would leave stale text in fts_blocks. V8 only had
-- INSERT/DELETE triggers; this adds the UPDATE path.
--
-- chunks_ad: AFTER DELETE ON chunks, remove the corresponding row from
-- vec_chunk_embeddings. CASCADE doesn't work across virtual tables, and
-- vec_chunk_embeddings has no FK to chunks. This trigger replaces the
-- N+1 cleanup loop previously in the entry.delete daemon handler
-- (Plan 4 Minor #3, Plan 5 design note).

-- vec_chunk_embeddings: previously created lazily by ChunkService. Plan 5
-- creates it here so the chunks_ad trigger can reference it unconditionally.
-- Dimension 2048 matches daemon default (config.embedding.dim, GLM embedding-3).
-- If config uses a different dim, the daemon's ensure_vec_chunk_embeddings call
-- will be a no-op (IF NOT EXISTS) — but dim mismatch means re-creation requires
-- manual table drop + ChunkService::ensure call with correct dim.
CREATE VIRTUAL TABLE IF NOT EXISTS vec_chunk_embeddings USING vec0(
    chunk_id TEXT PRIMARY KEY,
    embedding FLOAT[2048] distance_metric=cosine
);

-- blocks_au: fts_blocks is a self-stored FTS5 table (no external content),
-- so on UPDATE we DELETE the old row + INSERT the new one.
CREATE TRIGGER IF NOT EXISTS blocks_au AFTER UPDATE ON blocks BEGIN
    DELETE FROM fts_blocks WHERE block_id = old.id;
    INSERT INTO fts_blocks (block_id, entry_id, type, text)
    VALUES (new.id, new.entry_id, new.type, new.text);
END;

-- chunks_ad: vec_chunk_embeddings is a vec0 virtual table; no FK CASCADE.
-- This trigger cleans up embeddings when chunks are deleted (which happens
-- via CASCADE when a block is deleted, or explicitly during re-chunking).
CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
    DELETE FROM vec_chunk_embeddings WHERE chunk_id = old.id;
END;
