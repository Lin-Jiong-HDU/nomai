-- V10: Conversation storage primitives.
-- conversations: session metadata (one row per conversation).
-- turns: individual messages within a conversation.
-- fts_turns: fulltext index over turn content (trigram tokenizer, same as fts_blocks).

CREATE TABLE conversations (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL DEFAULT '',
    tags        TEXT NOT NULL DEFAULT '[]',
    attrs       TEXT NOT NULL DEFAULT '{}',
    turn_count  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE turns (
    id              TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    ordinal         INTEGER NOT NULL,
    role            TEXT NOT NULL,
    content         TEXT NOT NULL,
    attrs           TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    UNIQUE(conversation_id, ordinal)
);

CREATE INDEX idx_turns_conversation_ordinal ON turns(conversation_id, ordinal);

CREATE VIRTUAL TABLE fts_turns USING fts5(
    content,
    content=turns,
    content_rowid=rowid,
    tokenize='trigram'
);

-- Trigger: after INSERT on turns, bump turn_count + sync FTS.
CREATE TRIGGER turns_ai AFTER INSERT ON turns BEGIN
    UPDATE conversations
    SET turn_count = turn_count + 1,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE id = NEW.conversation_id;
    INSERT INTO fts_turns(rowid, content) VALUES (NEW.rowid, NEW.content);
END;

-- Trigger: after DELETE on turns, decrement turn_count + remove from FTS.
CREATE TRIGGER turns_ad AFTER DELETE ON turns BEGIN
    UPDATE conversations
    SET turn_count = turn_count - 1,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE id = OLD.conversation_id;
    INSERT INTO fts_turns(fts_turns, rowid, content) VALUES ('delete', OLD.rowid, OLD.content);
END;

-- Trigger: after UPDATE on turns, sync FTS (content change).
CREATE TRIGGER turns_au AFTER UPDATE ON turns BEGIN
    INSERT INTO fts_turns(fts_turns, rowid, content) VALUES ('delete', OLD.rowid, OLD.content);
    INSERT INTO fts_turns(rowid, content) VALUES (NEW.rowid, NEW.content);
END;
