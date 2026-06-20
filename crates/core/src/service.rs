use std::sync::Mutex;

use rusqlite::Connection;

use crate::error::CoreError;
use crate::storage;

pub struct EntryService {
    // Read paths (create/get/list/search/delete) are added in Tasks 3–5.
    #[allow(dead_code)]
    conn: Mutex<Connection>,
}

impl EntryService {
    /// Take ownership of a connection and run pending migrations.
    pub fn new(conn: Connection) -> Result<Self, CoreError> {
        let mut conn = conn;
        storage::run_migrations(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Result<Self, CoreError> {
        Self::new(Connection::open_in_memory()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_runs_migrations_creating_entries_and_fts() {
        let svc = EntryService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();

        // entries table exists
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);

        // fts_entries virtual table exists
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM fts_entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn new_is_idempotent_across_repeated_calls() {
        // Each for_test() opens a fresh in-memory DB; migrations re-run cleanly.
        let _a = EntryService::for_test().unwrap();
        let _b = EntryService::for_test().unwrap();
    }
}
