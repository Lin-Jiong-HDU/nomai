//! ChunkService: storage and retrieval of entry chunks.
//!
//! Each chunk is an independently embeddable piece of an entry. Chunks are
//! immutable (no update); re-chunking is delete + create. Emission follows
//! the Phase 2 Events pattern (chunk.created / chunk.deleted).

use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::CoreError;
use crate::storage;

pub struct ChunkService {
    // Consumed by CRUD + vec methods + semantic_search (Tasks 2-6).
    #[allow(dead_code)]
    conn: Arc<Mutex<Connection>>,
}

impl ChunkService {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, CoreError> {
        // Defensive: ensure migrations applied (idempotent). EntryService::new
        // also does this.
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
    fn for_test_creates_chunk_service_with_chunks_table() {
        let svc = ChunkService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }
}
