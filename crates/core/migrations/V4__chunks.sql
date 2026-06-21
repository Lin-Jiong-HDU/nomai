-- V4__chunks: chunk table for the Chunks primitive (spec §4).
-- See docs/superpowers/specs/2026-06-21-chunks-design.md.
--
-- vec_chunk_embeddings is created at runtime by ChunkService::ensure_vec_chunk_embeddings
-- (same pattern as EntryService::ensure_vec_embeddings) because dim comes from config.

CREATE TABLE chunks (
  id          TEXT PRIMARY KEY,             -- ULID string
  entry_id    TEXT NOT NULL,
  ordinal     INTEGER NOT NULL,             -- 0-based, ordering within entry
  text        TEXT NOT NULL,                -- chunk content
  attrs       TEXT NOT NULL,                -- JSON object
  created_at  TEXT NOT NULL,                -- RFC3339
  updated_at  TEXT NOT NULL,                -- RFC3339 (preserved for symmetry; chunks are immutable)
  FOREIGN KEY (entry_id) REFERENCES entries(id) ON DELETE CASCADE,
  UNIQUE(entry_id, ordinal)
);

CREATE INDEX idx_chunks_entry ON chunks(entry_id, ordinal);
