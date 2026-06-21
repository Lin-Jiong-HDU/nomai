-- V1__initial: entries + FTS5. vec_embeddings is added
-- in a later plan when the embedding dimension is known at runtime.

CREATE TABLE entries (
  id          TEXT PRIMARY KEY,           -- ULID string
  title       TEXT NOT NULL,
  body        TEXT NOT NULL,
  tags        TEXT NOT NULL,              -- JSON array of strings
  attrs       TEXT NOT NULL,              -- JSON object
  source      TEXT,
  created_at  TEXT NOT NULL,              -- RFC3339
  updated_at  TEXT NOT NULL
);

-- FTS5 self-stored table. We keep a duplicate copy of (title, body) inside
-- the FTS index because external-content mode (content='') does not allow
-- retrieving column values back, which would make the JOIN to `entries`
-- (on fts_entries.entry_id) impossible. entry_id is UNINDEXED so it is not
-- tokenized but is still retrievable on read.
CREATE VIRTUAL TABLE fts_entries USING fts5(
  entry_id UNINDEXED,
  title,
  body
);

-- Keep fts_entries synchronized with entries. For this self-stored FTS5
-- table, deletes use a normal DELETE (not the 'delete' special-rowid form,
-- which is reserved for external-content tables).
CREATE TRIGGER entries_ai AFTER INSERT ON entries BEGIN
  INSERT INTO fts_entries (entry_id, title, body)
  VALUES (new.id, new.title, new.body);
END;

CREATE TRIGGER entries_ad AFTER DELETE ON entries BEGIN
  DELETE FROM fts_entries WHERE entry_id = old.id;
END;

CREATE TRIGGER entries_au AFTER UPDATE ON entries BEGIN
  DELETE FROM fts_entries WHERE entry_id = old.id;
  INSERT INTO fts_entries (entry_id, title, body)
  VALUES (new.id, new.title, new.body);
END;
