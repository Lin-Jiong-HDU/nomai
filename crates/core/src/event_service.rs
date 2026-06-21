//! EventService: query/cleanup for the append-only events log.
//!
//! Emission is NOT here — EntryService and LinkService append events directly
//! via SQL INSERT inside their mutation transactions (spec §5). EventService
//! only reads and purges, to avoid circular dependencies.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::CoreError;
use crate::storage;

pub struct EventService {
    // list/get/purge (Tasks 2-3) consume this; unused until then.
    #[allow(dead_code)]
    conn: Arc<Mutex<Connection>>,
}

impl EventService {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, CoreError> {
        // Defensive: ensure migrations applied (idempotent). EntryService::new
        // and LinkService::new also do this.
        {
            let mut guard = conn.lock().unwrap();
            guard
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(CoreError::Storage)?;
            storage::run_migrations(&mut guard)?;
        }
        Ok(Self { conn })
    }

    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        // Run migrations via EntryService so all tables exist.
        crate::EntryService::new(conn.clone())?;
        Self::new(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_test_creates_event_service_with_events_table() {
        let svc = EventService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }
}
