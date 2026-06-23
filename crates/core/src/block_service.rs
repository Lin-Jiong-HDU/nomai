//! BlockService: storage and retrieval of typed blocks belonging to entries.
//!
//! Plan 2 scope: standalone CRUD on the `blocks` table. Plan 3 will wire
//! this into EntryService so entry.create auto-creates blocks. Events are
//! emitted on create/delete (`block.created` / `block.deleted`).
//!
//! See Spec 6 §5.1, §6.1, §7.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::CoreError;
use crate::storage;

pub struct BlockService {
    // Consumed by CRUD methods (Tasks 9-11).
    #[allow(dead_code)]
    conn: Arc<Mutex<Connection>>,
}

impl BlockService {
    /// Take ownership of a connection and ensure migrations applied.
    /// Idempotent. Does NOT take the connection's lock at construction time
    /// beyond what `run_migrations` requires.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, CoreError> {
        {
            let mut guard = conn.lock().unwrap();
            guard
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(CoreError::Storage)?;
            storage::run_migrations(&mut guard)?;
        }
        Ok(Self { conn })
    }

    /// In-memory DB for unit tests. Runs EntryService migrations so the
    /// `entries` table exists (FK target for `blocks.entry_id`).
    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        crate::EntryService::new(conn.clone())?;
        Self::new(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_test_constructs_service() {
        let svc = BlockService::for_test().unwrap();
        let _guard = svc.conn.lock().unwrap();
        // Service is constructible; conn is usable.
    }

    #[test]
    fn new_enables_foreign_keys_pragma() {
        let svc = BlockService::for_test().unwrap();
        let guard = svc.conn.lock().unwrap();
        let on: i64 = guard
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(on, 1, "PRAGMA foreign_keys must be ON");
    }

    #[test]
    fn new_creates_blocks_table() {
        // Construct via for_test, then verify blocks table exists.
        let svc = BlockService::for_test().unwrap();
        let guard = svc.conn.lock().unwrap();
        let n: i64 = guard
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='blocks'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "blocks table must exist after BlockService::new");
    }
}
