-- V5__emb_cache: persistent embedding cache (spec §4).
-- See docs/superpowers/specs/2026-06-22-embedding-cache-design.md.
--
-- Stores (model, body_hash) → embedding so identical bodies never trigger
-- duplicate embedding API calls. Embeddings are deterministic functions of
-- (model, body), so entries never expire. WITHOUT ROWID uses the composite
-- primary key as the cluster index, saving 8 bytes/row and speeding lookups.

CREATE TABLE emb_cache (
  model       TEXT    NOT NULL,
  body_hash   BLOB    NOT NULL,             -- BLAKE3-256 of body UTF-8 bytes (32 bytes)
  dim         INTEGER NOT NULL,             -- embedding dimension (e.g. 2048)
  embedding   BLOB    NOT NULL,             -- little-endian f32 array
  created_at  TEXT    NOT NULL,             -- RFC3339 (debug/LRU reference only)
  PRIMARY KEY (model, body_hash)
) WITHOUT ROWID;
