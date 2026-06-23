//! SQLite storage layer: migrations and connection setup.

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

use rusqlite::Connection;

use crate::error::CoreError;

/// Run all pending migrations on the given connection. Idempotent.
///
/// Calls `init_sqlite_extensions()` first so V9's `CREATE VIRTUAL TABLE …
/// USING vec0` (for `vec_chunk_embeddings`) succeeds regardless of whether
/// the caller remembered to initialize extensions. The init is idempotent
/// and safe to call multiple times.
pub fn run_migrations(conn: &mut Connection) -> Result<(), CoreError> {
    init_sqlite_extensions();
    embedded::migrations::runner()
        .run(conn)
        .map(|_| ())
        .map_err(|e| CoreError::Migration(e.to_string()))
}

/// Map SQLite ConstraintViolation (FK or UNIQUE) to CoreError::Validation.
/// Other errors pass through as CoreError::Storage. Used by all services
/// that do INSERT into tables with FK / UNIQUE constraints. Dedup of the
/// former per-service local helpers (Plan 4 P2-3).
pub fn map_constraint_violation(e: rusqlite::Error) -> crate::error::CoreError {
    use rusqlite::ffi::ErrorCode;
    match e {
        rusqlite::Error::SqliteFailure(err, _) if err.code == ErrorCode::ConstraintViolation => {
            crate::error::CoreError::Validation(format!("constraint violation: {e}"))
        }
        other => crate::error::CoreError::Storage(other),
    }
}

/// Register the sqlite-vec extension globally so any subsequently-opened
/// `Connection` supports `vec0` virtual tables. Idempotent and safe to call
/// multiple times.
///
/// # Safety
///
/// `sqlite3_auto_extension` is thread-safe and idempotent per SQLite docs.
/// The `transmute` casts the `sqlite3_vec_init` entry point to the
/// `sqlite3_ext_init` signature that `sqlite3_auto_extension` expects.
pub fn init_sqlite_extensions() {
    unsafe {
        rusqlite::ffi::sqlite3_auto_extension(Some(std::mem::transmute::<
            *const (),
            unsafe extern "C" fn(
                *mut rusqlite::ffi::sqlite3,
                *mut *mut std::os::raw::c_char,
                *const rusqlite::ffi::sqlite3_api_routines,
            ) -> std::os::raw::c_int,
        >(
            sqlite_vec::sqlite3_vec_init as *const ()
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn init_registers_vec0_virtual_table_module() {
        init_sqlite_extensions();
        let conn = Connection::open_in_memory().unwrap();
        // After init, vec0 virtual tables should be creatable.
        conn.execute_batch(
            "CREATE VIRTUAL TABLE test_vec USING vec0(id TEXT PRIMARY KEY, embedding float[4])",
        )
        .expect("vec0 should be available after init_sqlite_extensions");
    }

    #[test]
    fn v5_migration_creates_emb_cache_table() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='emb_cache'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "V5 migration should create emb_cache table");
    }

    #[test]
    fn v6_migration_creates_blocks_table() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='blocks'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "V6 migration should create blocks table");
    }

    #[test]
    fn v6_migration_adds_fs_path_columns_to_entries() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        // PRAGMA table_info lists columns; check fs_path and fs_mtime present.
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(entries)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            cols.contains(&"fs_path".to_string()),
            "entries should have fs_path"
        );
        assert!(
            cols.contains(&"fs_mtime".to_string()),
            "entries should have fs_mtime"
        );
    }

    #[test]
    fn v6_migration_adds_block_id_to_chunks() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(chunks)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            cols.contains(&"block_id".to_string()),
            "chunks should have block_id"
        );
    }

    #[test]
    fn v6_migration_preserves_existing_data() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        // Insert without fs_path/block_id (legacy columns nullable). body is
        // still NOT NULL in V6 but is dropped in V7 — this test runs the full
        // migration chain through V7 so body is gone. Use the V7-era column
        // set (no body).
        conn.execute(
            "INSERT INTO entries (id, title, tags, attrs, created_at, updated_at)
             VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 't', '[]', '{}', '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
            [],
        )
        .unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn v7_migration_drops_entries_body_column() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(entries)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            !cols.contains(&"body".to_string()),
            "V7 should drop entries.body column"
        );
        // fs_path, fs_mtime still present from V6 (V7 only drops body).
        assert!(cols.contains(&"fs_path".to_string()));
        assert!(cols.contains(&"fs_mtime".to_string()));
    }

    #[test]
    fn v7_migration_drops_v1_fts5_triggers() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        // V1 created entries_ai / entries_ad / entries_au. V7 drops them so
        // INSERTs into entries no longer maintain fts_entries automatically
        // (EntryService writes fts_entries directly in the new world).
        for trigger in ["entries_ai", "entries_ad", "entries_au"] {
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type='trigger' AND name=?1",
                    [trigger],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(n, 0, "V7 should drop trigger {trigger}");
        }
    }

    #[test]
    fn v8_migration_drops_entry_level_embeddings_and_fts() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('vec_embeddings', 'fts_entries')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 0, "V8 should drop vec_embeddings + fts_entries");
    }

    #[test]
    fn v8_migration_creates_fts_blocks() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='fts_blocks'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "V8 should create fts_blocks virtual table");
    }

    #[test]
    fn v8_migration_drops_chunks_entry_id() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(chunks)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            !cols.contains(&"entry_id".to_string()),
            "chunks.entry_id should be gone"
        );
        assert!(
            cols.contains(&"block_id".to_string()),
            "chunks.block_id must remain"
        );
    }

    #[test]
    fn v8_migration_creates_blocks_fts_triggers() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name IN ('blocks_ai', 'blocks_ad')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 2, "V8 should create blocks_ai + blocks_ad triggers");
    }

    #[test]
    fn v9_migration_creates_blocks_au_trigger() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='blocks_au'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "V9 should create blocks_au UPDATE trigger");
    }

    #[test]
    fn v9_migration_creates_chunks_ad_trigger() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='chunks_ad'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "V9 should create chunks_ad DELETE trigger");
    }

    #[test]
    fn v9_blocks_au_trigger_keeps_fts_blocks_sync_on_update() {
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        // Insert a block (need entries + blocks row manually since no service in unit test)
        conn.execute(
            "INSERT INTO entries (id, title, tags, attrs, source, fs_path, fs_mtime, created_at, updated_at)
             VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 't', '[]', '{}', NULL, NULL, NULL, '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blocks (id, entry_id, ordinal, type, text, attrs, created_at, updated_at)
             VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAB', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 0, 'note', 'original', '{}', '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
            [],
        )
        .unwrap();
        // Verify fts_blocks populated by blocks_ai (V8)
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_blocks WHERE block_id = '01ARZ3NDEKTSV4RRFFQ69G5FAB'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);

        // UPDATE the block — blocks_au should refresh fts_blocks
        conn.execute(
            "UPDATE blocks SET text = 'updated' WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAB'",
            [],
        )
        .unwrap();
        let text: String = conn
            .query_row(
                "SELECT text FROM fts_blocks WHERE block_id = '01ARZ3NDEKTSV4RRFFQ69G5FAB'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(text, "updated");
    }

    #[test]
    fn v9_chunks_ad_trigger_cleans_vec_chunk_embeddings() {
        // Requires sqlite-vec extension for vec_chunk_embeddings table.
        crate::storage::init_sqlite_extensions();
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        // V9 creates vec_chunk_embeddings with dim=1536 (daemon default).
        // The CREATE here is a no-op (IF NOT EXISTS); embeddings must match 1536.
        // Insert entry + block + chunk
        conn.execute(
            "INSERT INTO entries (id, title, tags, attrs, source, fs_path, fs_mtime, created_at, updated_at)
             VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 't', '[]', '{}', NULL, NULL, NULL, '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blocks (id, entry_id, ordinal, type, text, attrs, created_at, updated_at)
             VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAB', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 0, 'note', 'x', '{}', '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (id, block_id, ordinal, text, attrs, created_at, updated_at)
             VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAC', '01ARZ3NDEKTSV4RRFFQ69G5FAB', 0, 'x', '{}', '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
            [],
        )
        .unwrap();
        // Insert chunk embedding (FLOAT[1536] = 1536 * 4 bytes = 6144 bytes).
        let emb: Vec<u8> = vec![0u8; 6144];
        conn.execute(
            "INSERT INTO vec_chunk_embeddings (chunk_id, embedding) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAC', ?1)",
            [&emb[..]],
        )
        .unwrap();
        // DELETE the chunk — chunks_ad should clean vec_chunk_embeddings
        conn.execute(
            "DELETE FROM chunks WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAC'",
            [],
        )
        .unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM vec_chunk_embeddings WHERE chunk_id = '01ARZ3NDEKTSV4RRFFQ69G5FAC'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 0, "chunks_ad should have removed the embedding");
    }

    #[test]
    fn v9_creates_vec_chunk_embeddings_with_daemon_default_dim() {
        // Plan 5 final review (C1): V9 must create vec_chunk_embeddings with
        // dim=1536 (matching daemon default config.rs:175) — not 2048. A
        // mismatch breaks the first embedding write at the vec0 layer with
        // "Dimension mismatch for inserted vector".
        crate::storage::init_sqlite_extensions();
        let mut conn = Connection::open_in_memory().unwrap();
        run_migrations(&mut conn).unwrap();
        // Inspect the CREATE VIRTUAL TABLE SQL captured in sqlite_master.
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='vec_chunk_embeddings'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            sql.contains("FLOAT[1536]"),
            "V9 should create vec_chunk_embeddings with dim=1536 (daemon default), got: {sql}"
        );
        assert!(
            !sql.contains("FLOAT[2048]"),
            "V9 must not hardcode dim=2048; got: {sql}"
        );
    }
}
