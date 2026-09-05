-- V12: local adaptive-memory search signals.
--
-- These tables are authoritative local state, not projections of .nomai.
-- Deliberately do not foreign-key derived entries, blocks, or chunks: index
-- operations temporarily rebuild those rows. Only session results follow
-- their owning search session's lifecycle.

CREATE TABLE search_sessions (
    id                   TEXT PRIMARY KEY,
    raw_query_text       TEXT NOT NULL,
    effective_query_text TEXT NOT NULL,
    query_embedding      BLOB NOT NULL,
    embedding_model      TEXT NOT NULL,
    embedding_dim        INTEGER NOT NULL,
    created_at           TEXT NOT NULL,
    expires_at           TEXT NOT NULL
);

CREATE INDEX idx_search_sessions_expiry
ON search_sessions(expires_at, created_at, id);

CREATE TABLE search_session_results (
    search_id         TEXT NOT NULL,
    entry_id          TEXT NOT NULL,
    matched_block_id  TEXT NULL,
    matched_chunk_id  TEXT NULL,
    result_rank       INTEGER NOT NULL,
    PRIMARY KEY (search_id, entry_id),
    FOREIGN KEY (search_id) REFERENCES search_sessions(id) ON DELETE CASCADE
);

CREATE INDEX idx_search_session_results_entry
ON search_session_results(entry_id);

CREATE INDEX idx_search_session_results_block
ON search_session_results(matched_block_id);

CREATE INDEX idx_search_session_results_chunk
ON search_session_results(matched_chunk_id);

CREATE TABLE search_feedback (
    id          TEXT PRIMARY KEY,
    search_id   TEXT NOT NULL,
    entry_id    TEXT NOT NULL,
    block_id    TEXT NULL,
    chunk_id    TEXT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE (search_id, entry_id)
);

CREATE INDEX idx_search_feedback_entry
ON search_feedback(entry_id);

CREATE TABLE entry_memory_stats (
    entry_id                TEXT PRIMARY KEY,
    reinforcement_count     INTEGER NOT NULL CHECK (reinforcement_count BETWEEN 0 AND 3),
    last_reinforced_at      TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);

CREATE TABLE query_affinities (
    id                    TEXT PRIMARY KEY,
    normalized_query_hash TEXT NOT NULL,
    raw_query_text        TEXT NOT NULL,
    effective_query_text  TEXT NOT NULL,
    embedding_model       TEXT NOT NULL,
    embedding_dim         INTEGER NOT NULL,
    entry_id              TEXT NOT NULL,
    block_id              TEXT NULL,
    chunk_id              TEXT NULL,
    reinforcement_count   INTEGER NOT NULL CHECK (reinforcement_count BETWEEN 1 AND 3),
    last_reinforced_at    TEXT NOT NULL,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);

CREATE UNIQUE INDEX query_affinities_exact_target
ON query_affinities(
    normalized_query_hash,
    embedding_model,
    entry_id,
    COALESCE(block_id, ''),
    COALESCE(chunk_id, '')
);

CREATE INDEX idx_query_affinities_entry
ON query_affinities(entry_id);

CREATE INDEX idx_query_affinities_block
ON query_affinities(block_id);

CREATE INDEX idx_query_affinities_chunk
ON query_affinities(chunk_id);
