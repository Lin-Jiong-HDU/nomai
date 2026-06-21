//! LinkService: directed edges between entries.
//!
//! See spec §4-§5 for schema and RPC contract.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::CoreError;
use crate::storage;

pub struct LinkService {
    // Used by business methods in Tasks 5-6 (create/get/delete).
    #[allow(dead_code)]
    conn: Arc<Mutex<Connection>>,
}

impl LinkService {
    /// Take a shared connection and assume migrations have already been run
    /// (by `EntryService::new` or `Daemon::new`). Idempotent if migrations
    /// are no-ops.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, CoreError> {
        // Ensure migrations are applied (idempotent). EntryService::new
        // already runs them; this is defensive in case LinkService is
        // constructed directly.
        {
            let mut guard = conn.lock().unwrap();
            guard
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(CoreError::Storage)?;
            storage::run_migrations(&mut guard)?;
        }
        Ok(Self { conn })
    }

    /// Test-only constructor backed by an in-memory SQLite database.
    /// Mirrors `EntryService::for_test`.
    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        // Run migrations via EntryService so both entries and links tables exist.
        crate::EntryService::new(conn.clone())?;
        Self::new(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_test_creates_link_service_with_links_table() {
        let svc = LinkService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM links", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }
}
