//! SQLite storage layer: migrations and connection setup.

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

use rusqlite::Connection;

use crate::error::CoreError;

/// Run all pending migrations on the given connection. Idempotent.
pub fn run_migrations(conn: &mut Connection) -> Result<(), CoreError> {
    embedded::migrations::runner()
        .run(conn)
        .map(|_| ())
        .map_err(|e| CoreError::Migration(e.to_string()))
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
        // Insert via old schema (no fs_path/block_id) — should succeed.
        conn.execute(
            "INSERT INTO entries (id, title, body, tags, attrs, created_at, updated_at)
             VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 't', 'b', '[]', '{}', '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
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
}
