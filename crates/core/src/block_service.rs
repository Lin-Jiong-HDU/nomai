//! BlockService: storage and retrieval of typed blocks belonging to entries.
//!
//! Plan 2 scope: standalone CRUD on the `blocks` table. Plan 3 will wire
//! this into EntryService so entry.create auto-creates blocks. Events are
//! emitted on create/delete (`block.created` / `block.deleted`).
//!
//! See Spec 6 §5.1, §6.1, §7.

use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};
use ulid::Ulid;

use crate::block_model::{Block, CreateBlock};
use crate::error::CoreError;
use crate::storage;

pub struct BlockService {
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

    pub fn create(&self, params: CreateBlock) -> Result<Block, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.create_in_tx(&conn, params);
        match result {
            Ok(block) => {
                conn.execute_batch("COMMIT")?;
                Ok(block)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Execute create within an existing transaction. Caller controls BEGIN/COMMIT.
    /// Does NOT lock self.conn. Does NOT call self methods that lock conn.
    ///
    /// FK + UNIQUE ConstraintViolation → `CoreError::Validation` per spec §6.3.
    pub fn create_in_tx(&self, conn: &Connection, params: CreateBlock) -> Result<Block, CoreError> {
        use chrono::Utc;

        let attrs = params
            .attrs
            .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
        if !attrs.is_object() {
            return Err(CoreError::Validation("attrs must be a JSON object".into()));
        }

        let now = Utc::now();
        let block = Block {
            id: Ulid::new(),
            entry_id: params.entry_id,
            ordinal: params.ordinal,
            r#type: params.r#type,
            text: params.text,
            attrs,
            created_at: now,
            updated_at: now,
        };

        conn.execute(
            "INSERT INTO blocks (id, entry_id, ordinal, type, text, attrs, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                block.id.to_string(),
                block.entry_id.to_string(),
                block.ordinal,
                &block.r#type,
                &block.text,
                block.attrs.to_string(),
                block.created_at.to_rfc3339(),
                block.updated_at.to_rfc3339(),
            ],
        )
        .map_err(map_constraint_violation)?;

        let event_id = Ulid::new();
        let event_payload = serde_json::to_value(&block).expect("block serializes");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event_id.to_string(),
                "block.created",
                "block",
                block.id.to_string(),
                event_payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(map_constraint_violation)?;

        Ok(block)
    }
}

/// Map SQLite ConstraintViolation (FK or UNIQUE) to `CoreError::Validation`.
/// Other storage errors pass through as `CoreError::Storage`.
fn map_constraint_violation(e: rusqlite::Error) -> CoreError {
    use rusqlite::ffi::ErrorCode;
    match e {
        rusqlite::Error::SqliteFailure(err, _) if err.code == ErrorCode::ConstraintViolation => {
            CoreError::Validation(format!("constraint violation: {e}"))
        }
        other => CoreError::Storage(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_model::CreateBlock;
    use crate::{CreateEntry, EntryService};
    use serde_json::json;

    /// Create an entry via EntryService and return its ULID. BlockService
    /// tests need this because `blocks.entry_id` has an FK to `entries.id`.
    fn seed_entry(conn: Arc<Mutex<rusqlite::Connection>>) -> Ulid {
        let entries = EntryService::new(conn).unwrap();
        let entry = entries
            .create(CreateEntry {
                title: "seed".into(),
                body: "seed body".into(),
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        entry.id
    }

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

    #[test]
    fn create_inserts_block_and_returns_it() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());

        let block = svc
            .create(CreateBlock {
                entry_id,
                ordinal: 0,
                r#type: "claim".into(),
                text: "Earth orbits the sun.".into(),
                attrs: Some(json!({"confidence": "high"})),
            })
            .unwrap();

        assert_eq!(block.entry_id, entry_id);
        assert_eq!(block.ordinal, 0);
        assert_eq!(block.r#type, "claim");
        assert_eq!(block.text, "Earth orbits the sun.");
        assert_eq!(block.attrs["confidence"], json!("high"));
    }

    #[test]
    fn create_emits_block_created_event() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());

        let block = svc
            .create(CreateBlock {
                entry_id,
                ordinal: 0,
                r#type: "note".into(),
                text: "x".into(),
                attrs: None,
            })
            .unwrap();

        let guard = svc.conn.lock().unwrap();
        let (event_type, target_type, target_id): (String, String, String) = guard
            .query_row(
                "SELECT type, target_type, target_id FROM events WHERE target_id = ?1",
                params![block.id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(event_type, "block.created");
        assert_eq!(target_type, "block");
        assert_eq!(target_id, block.id.to_string());
    }

    #[test]
    fn create_rejects_unknown_entry_id() {
        let svc = BlockService::for_test().unwrap();
        let fake_id: Ulid = Ulid::new();
        let err = svc
            .create(CreateBlock {
                entry_id: fake_id,
                ordinal: 0,
                r#type: "note".into(),
                text: "x".into(),
                attrs: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_rejects_duplicate_entry_ordinal() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());

        svc.create(CreateBlock {
            entry_id,
            ordinal: 0,
            r#type: "note".into(),
            text: "first".into(),
            attrs: None,
        })
        .unwrap();

        let err = svc
            .create(CreateBlock {
                entry_id,
                ordinal: 0, // duplicate
                r#type: "note".into(),
                text: "second".into(),
                attrs: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_rejects_non_object_attrs() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let err = svc
            .create(CreateBlock {
                entry_id,
                ordinal: 0,
                r#type: "note".into(),
                text: "x".into(),
                attrs: Some(json!([1, 2, 3])),
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_defaults_attrs_to_empty_object() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let block = svc
            .create(CreateBlock {
                entry_id,
                ordinal: 0,
                r#type: "note".into(),
                text: "x".into(),
                attrs: None,
            })
            .unwrap();
        assert_eq!(block.attrs, json!({}));
    }
}
