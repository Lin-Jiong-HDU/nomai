-- V6__add_blocks_and_fs_paths.sql
-- Spec 6 Plan 2: ADDITIVE schema expansion. No drops, no type changes.
-- Existing services (EntryService/ChunkService/LinkService) continue working
-- unchanged. Plan 3 will flip the services to the new world and drop the
-- legacy columns.
--
-- See docs/superpowers/specs/2026-06-22-content-storage-design.md §5.1.

-- Typed blocks table. One entry → many blocks (Spec §5.1).
CREATE TABLE blocks (
  id          TEXT PRIMARY KEY,
  entry_id    TEXT NOT NULL,
  ordinal     INTEGER NOT NULL,
  type        TEXT NOT NULL,
  text        TEXT NOT NULL,
  attrs       TEXT NOT NULL,
  created_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL,
  FOREIGN KEY (entry_id) REFERENCES entries(id) ON DELETE CASCADE,
  UNIQUE(entry_id, ordinal)
);

CREATE INDEX idx_blocks_entry ON blocks(entry_id, ordinal);

-- Track FS location of entries whose source of truth is a .nomai file.
-- Nullable: existing entries and entries created via the legacy path have
-- NULL; Plan 3's flip will populate these for all new entries.
ALTER TABLE entries ADD COLUMN fs_path TEXT;
ALTER TABLE entries ADD COLUMN fs_mtime TEXT;

-- Optional FK from legacy chunks to the new blocks table. Nullable: existing
-- chunks (created via ChunkService.create) keep NULL; Plan 3's auto-derived
-- chunks will set this.
ALTER TABLE chunks ADD COLUMN block_id TEXT REFERENCES blocks(id) ON DELETE CASCADE;
