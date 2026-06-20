-- V1__initial: entries + FTS5 (external content). vec_embeddings is added
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

-- FTS5 external-content table. content='' means FTS does not store its own
-- copy; we sync via triggers and read columns back from `entries` on query.
CREATE VIRTUAL TABLE fts_entries USING fts5(
  entry_id UNINDEXED,
  title,
  body,
  content=''
);

-- Keep fts_entries synchronized with entries. The 'delete' special rowid
-- tells FTS5 to remove the row in external-content mode.
CREATE TRIGGER entries_ai AFTER INSERT ON entries BEGIN
  INSERT INTO fts_entries (entry_id, title, body)
  VALUES (new.id, new.title, new.body);
END;

CREATE TRIGGER entries_ad AFTER DELETE ON entries BEGIN
  INSERT INTO fts_entries (fts_entries, entry_id, title, body)
  VALUES ('delete', old.id, old.title, old.body);
END;

CREATE TRIGGER entries_au AFTER UPDATE ON entries BEGIN
  INSERT INTO fts_entries (fts_entries, entry_id, title, body)
  VALUES ('delete', old.id, old.title, old.body);
  INSERT INTO fts_entries (entry_id, title, body)
  VALUES (new.id, new.title, new.body);
END;
