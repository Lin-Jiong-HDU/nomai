-- V13: Agent channel primitives.
-- channel_messages: append-only named message log (one row per message).
-- channel_cursors: server-side per-subscriber read position.
--
-- The cursor is the INTEGER `seq`, not the ULID `id`: Ulid::new() is not
-- monotonic within a single millisecond (80 random tail bits), so a
-- lexicographic ULID cursor can skip messages written in the same ms.
-- AUTOINCREMENT additionally guarantees seq is never reused after a purge
-- (a plain MAX(seq)+1 or bare rowid would reuse the freed maximum).

CREATE TABLE channel_messages (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    id         TEXT NOT NULL UNIQUE,
    channel    TEXT NOT NULL,
    sender     TEXT NOT NULL DEFAULT '',
    text       TEXT NOT NULL,
    attrs      TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX idx_channel_messages_channel_seq
    ON channel_messages(channel, seq);

CREATE TABLE channel_cursors (
    channel    TEXT NOT NULL,
    subscriber TEXT NOT NULL,
    last_seq   INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel, subscriber)
);
