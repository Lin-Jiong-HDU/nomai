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

use crate::block_model::{Block, BlockListResult, CreateBlock};
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

    /// List all blocks for an entry, ordered by ordinal. Empty result if
    /// the entry has no blocks or doesn't exist (callers can distinguish
    /// via EntryService::get if needed).
    pub fn list(&self, entry_id: Ulid) -> Result<BlockListResult, CoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, entry_id, ordinal, type, text, attrs, created_at, updated_at
             FROM blocks WHERE entry_id = ?1
             ORDER BY ordinal ASC",
        )?;
        let items: Result<Vec<Block>, _> = stmt
            .query_map(params![entry_id.to_string()], |row| row_to_block(row, 0))?
            .collect();
        let items = items?;
        let total = items.len() as u64;
        Ok(BlockListResult { items, total })
    }

    /// Fetch a single block by id. NotFound if missing.
    pub fn get(&self, id: Ulid) -> Result<Block, CoreError> {
        let conn = self.conn.lock().unwrap();
        match conn.query_row(
            "SELECT id, entry_id, ordinal, type, text, attrs, created_at, updated_at
             FROM blocks WHERE id = ?1",
            params![id.to_string()],
            |row| row_to_block(row, 0),
        ) {
            Ok(block) => Ok(block),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(CoreError::NotFound(id)),
            Err(e) => Err(CoreError::Storage(e)),
        }
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

/// Map a SQLite row to a `Block`. Expects columns in the canonical order:
/// id, entry_id, ordinal, type, text, attrs, created_at, updated_at.
///
/// `offset` is kept for signature symmetry with `row_to_entry` (which also
/// takes an offset for the same reason — currently unused, reserved for
/// future prepared statements that prefix the row with other columns).
fn row_to_block(row: &rusqlite::Row<'_>, _offset: usize) -> rusqlite::Result<Block> {
    use chrono::{DateTime, Utc};

    let id_str: String = row.get(0)?;
    let entry_id_str: String = row.get(1)?;
    let ordinal: u32 = row.get(2)?;
    let ty: String = row.get(3)?;
    let text: String = row.get(4)?;
    let attrs_str: String = row.get(5)?;
    let created_str: String = row.get(6)?;
    let updated_str: String = row.get(7)?;

    let attrs: serde_json::Value =
        serde_json::from_str(&attrs_str).unwrap_or(serde_json::Value::Object(Default::default()));

    Ok(Block {
        id: id_str.parse().expect("ULID stored in DB is always valid"),
        entry_id: entry_id_str
            .parse()
            .expect("ULID stored in DB is always valid"),
        ordinal,
        r#type: ty,
        text,
        attrs,
        created_at: DateTime::parse_from_rfc3339(&created_str)
            .expect("RFC3339 stored in DB is always valid")
            .with_timezone(&Utc),
        updated_at: DateTime::parse_from_rfc3339(&updated_str)
            .expect("RFC3339 stored in DB is always valid")
            .with_timezone(&Utc),
    })
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

    fn seed_block(svc: &BlockService, entry_id: Ulid, ordinal: u32, ty: &str) -> Block {
        svc.create(CreateBlock {
            entry_id,
            ordinal,
            r#type: ty.into(),
            text: format!("block {ordinal}"),
            attrs: None,
        })
        .unwrap()
    }

    #[test]
    fn list_returns_blocks_ordered_by_ordinal() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());

        // Insert out of order to verify ORDER BY
        seed_block(&svc, entry_id, 2, "note");
        seed_block(&svc, entry_id, 0, "claim");
        seed_block(&svc, entry_id, 1, "evidence");

        let result = svc.list(entry_id).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.items[0].ordinal, 0);
        assert_eq!(result.items[1].ordinal, 1);
        assert_eq!(result.items[2].ordinal, 2);
        assert_eq!(result.items[0].r#type, "claim");
    }

    #[test]
    fn list_returns_empty_for_entry_with_no_blocks() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let result = svc.list(entry_id).unwrap();
        assert_eq!(result.total, 0);
        assert!(result.items.is_empty());
    }

    #[test]
    fn list_only_returns_blocks_for_target_entry() {
        let svc = BlockService::for_test().unwrap();
        let entry_a = seed_entry(svc.conn.clone());
        // Need a second entry. seed_entry creates via EntryService::create
        // which generates a fresh ULID each call.
        let entry_b = {
            let entries = EntryService::new(svc.conn.clone()).unwrap();
            entries
                .create(CreateEntry {
                    title: "second".into(),
                    body: "b".into(),
                    tags: None,
                    attrs: None,
                    source: None,
                })
                .unwrap()
                .id
        };

        seed_block(&svc, entry_a, 0, "note");
        seed_block(&svc, entry_a, 1, "note");
        seed_block(&svc, entry_b, 0, "note");

        assert_eq!(svc.list(entry_a).unwrap().total, 2);
        assert_eq!(svc.list(entry_b).unwrap().total, 1);
    }

    #[test]
    fn get_returns_block_by_id() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let created = seed_block(&svc, entry_id, 0, "claim");
        let fetched = svc.get(created.id).unwrap();
        assert_eq!(created, fetched);
    }

    #[test]
    fn get_returns_not_found_for_missing_id() {
        let svc = BlockService::for_test().unwrap();
        let fake_id = Ulid::new();
        let err = svc.get(fake_id).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }
}
