-- V2__links: link table for the Links primitive (spec §4).
-- See docs/superpowers/specs/2026-06-21-primitives-design.md.

CREATE TABLE links (
  id          TEXT PRIMARY KEY,             -- ULID string
  source_id   TEXT NOT NULL,
  target_id   TEXT NOT NULL,
  relation    TEXT NOT NULL,                -- free-form string
  attrs       TEXT NOT NULL,                -- JSON object
  created_at  TEXT NOT NULL,                -- RFC3339
  FOREIGN KEY (source_id) REFERENCES entries(id) ON DELETE CASCADE,
  FOREIGN KEY (target_id) REFERENCES entries(id) ON DELETE CASCADE,
  UNIQUE(source_id, target_id, relation)
);

CREATE INDEX idx_links_source ON links(source_id, relation);
CREATE INDEX idx_links_target ON links(target_id, relation);
