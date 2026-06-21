-- V3__events: append-only event log for the Events primitive (spec §4).
-- See docs/superpowers/specs/2026-06-21-events-design.md.

CREATE TABLE events (
  id           TEXT PRIMARY KEY,             -- ULID (time-ordered, cursor)
  type         TEXT NOT NULL,                -- event type (free-form string)
  target_type  TEXT NOT NULL,                -- "entry" | "link"
  target_id    TEXT NOT NULL,                -- ULID string (no FK; multi-target)
  payload      TEXT NOT NULL,                -- JSON full snapshot
  created_at   TEXT NOT NULL                 -- RFC3339
);

CREATE INDEX idx_events_created ON events(created_at);
CREATE INDEX idx_events_target ON events(target_type, target_id);
