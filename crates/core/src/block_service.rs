//! BlockService: storage and retrieval of typed blocks belonging to entries.
//!
//! Plan 2 scope: standalone CRUD on the `blocks` table. Plan 3 will wire
//! this into EntryService so entry.create auto-creates blocks. Events are
//! emitted on create/delete (`block.created` / `block.deleted`).
//!
//! See Spec 6 §5.1, §6.1, §7.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, params};
use ulid::Ulid;

use crate::block_model::{Block, BlockListResult, CreateBlock};
use crate::error::CoreError;
use crate::storage;

pub struct BlockService {
    conn: Arc<Mutex<Connection>>,
    chunk_target_size: usize,
}

impl BlockService {
    /// Take ownership of a connection and ensure migrations applied.
    /// Idempotent. Does NOT take the connection's lock at construction time
    /// beyond what `run_migrations` requires.
    ///
    /// `chunk_target_size` is the character budget passed to
    /// `chunking::chunk_text` when deriving chunks from block text. Defaults
    /// to 1024 in `EntryService::for_test` and daemon examples; production
    /// callers thread `config.chunking.target_size` through.
    pub fn new(conn: Arc<Mutex<Connection>>, chunk_target_size: usize) -> Result<Self, CoreError> {
        {
            let mut guard = conn.lock().unwrap();
            guard
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(CoreError::Storage)?;
            storage::run_migrations(&mut guard)?;
        }
        Ok(Self {
            conn,
            chunk_target_size,
        })
    }

    /// In-memory DB for unit tests. Runs EntryService migrations so the
    /// `entries` table exists (FK target for `blocks.entry_id`).
    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        // V9 migration creates a vec0 virtual table; the extension must be
        // registered before the connection is opened.
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        let tmp = tempfile::tempdir()?;
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        crate::EntryService::new(conn.clone(), content_store, 1024)?;
        Self::new(conn, 1024)
    }

    pub fn create(&self, params: CreateBlock) -> Result<Block, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.create_in_tx(&conn, params, true);
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
    pub fn create_in_tx(
        &self,
        conn: &Connection,
        params: CreateBlock,
        emit_event: bool,
    ) -> Result<Block, CoreError> {
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
        .map_err(crate::storage::map_constraint_violation)?;

        // Derive chunks from block text (Spec §10). One chunk per output of
        // `chunking::chunk_text`. `attrs` inherits `block.attrs` plus the
        // `parent_block_type` marker used by downstream search filters.
        let chunk_texts = crate::chunking::chunk_text(&block.text, self.chunk_target_size);
        let mut chunk_attrs = block.attrs.clone();
        if let Some(obj) = chunk_attrs.as_object_mut() {
            obj.insert(
                "parent_block_type".into(),
                serde_json::Value::String(block.r#type.clone()),
            );
        }
        for (chunk_ordinal, chunk_text) in chunk_texts.into_iter().enumerate() {
            let chunk_id = Ulid::new();
            conn.execute(
                "INSERT INTO chunks (id, block_id, ordinal, text, attrs, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    chunk_id.to_string(),
                    block.id.to_string(),
                    chunk_ordinal as u32,
                    &chunk_text,
                    chunk_attrs.to_string(),
                    block.created_at.to_rfc3339(),
                    block.updated_at.to_rfc3339(),
                ],
            )
            .map_err(crate::storage::map_constraint_violation)?;
        }

        // `block.created` event emission is gated on `emit_event`. Callers
        // that already emit an aggregate `entry.created` payload (which
        // embeds the full blocks vector) pass `false` to avoid N+1 event
        // amplification: one block.created per block + one entry.created
        // = N+1 events for a single entry.create. Direct user actions
        // (BlockService::create, BlockService::append) pass `true`.
        if emit_event {
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
            .map_err(crate::storage::map_constraint_violation)?;
        }

        Ok(block)
    }

    /// Append a block to an entry. Computes `ordinal = max(existing) + 1`
    /// (0 when the entry has no blocks). Auto-chunks via the same path as
    /// `create_in_tx`. Emits `block.created`. Returns NotFound if the entry
    /// does not exist.
    pub fn append(
        &self,
        entry_id: Ulid,
        r#type: String,
        text: String,
        attrs: Option<serde_json::Value>,
    ) -> Result<Block, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.append_in_tx(&conn, entry_id, r#type, text, attrs);
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

    /// Append within an existing transaction. Caller controls BEGIN/COMMIT.
    /// Computes ordinal = MAX(ordinal) + 1 over existing blocks for the entry
    /// (0 if the entry has no blocks yet). Returns NotFound if the entry does
    /// not exist; Validation on unknown block type via create_in_tx.
    pub fn append_in_tx(
        &self,
        conn: &Connection,
        entry_id: Ulid,
        r#type: String,
        text: String,
        attrs: Option<serde_json::Value>,
    ) -> Result<Block, CoreError> {
        // Verify entry exists. The FK on blocks.entry_id would catch this
        // anyway, but a NotFound error is more informative than the mapped
        // constraint violation.
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE id = ?1",
                params![entry_id.to_string()],
                |row| row.get(0),
            )
            .map_err(CoreError::Storage)?;
        if exists == 0 {
            return Err(CoreError::NotFound(entry_id));
        }

        // Compute next ordinal. MAX returns NULL when there are no rows; map
        // that to 0.
        let max_ordinal: Option<i64> = conn
            .query_row(
                "SELECT MAX(ordinal) FROM blocks WHERE entry_id = ?1",
                params![entry_id.to_string()],
                |row| row.get(0),
            )
            .map_err(CoreError::Storage)?;
        let ordinal = max_ordinal.map(|m| (m + 1) as u32).unwrap_or(0);

        self.create_in_tx(
            conn,
            CreateBlock {
                entry_id,
                ordinal,
                r#type,
                text,
                attrs,
            },
            true,
        )
    }

    /// Update a block. Any of `type`/`text`/`attrs` may be `None` (= leave
    /// unchanged). Re-chunks when `text` changes (DELETE existing chunks +
    /// `chunking::chunk_text` on the new text + INSERT). The chunks_ad
    /// trigger removes the prior chunk embeddings automatically. The
    /// blocks_au trigger refreshes `fts_blocks`. Emits `block.updated`.
    pub fn update(
        &self,
        id: Ulid,
        r#type: Option<String>,
        text: Option<String>,
        attrs: Option<serde_json::Value>,
    ) -> Result<Block, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.update_in_tx(&conn, id, r#type, text, attrs);
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

    /// Execute update within an existing transaction. Caller controls
    /// BEGIN/COMMIT. Does NOT lock self.conn. Does NOT call self methods
    /// that lock conn.
    pub fn update_in_tx(
        &self,
        conn: &Connection,
        id: Ulid,
        r#type: Option<String>,
        text: Option<String>,
        attrs: Option<serde_json::Value>,
    ) -> Result<Block, CoreError> {
        use chrono::Utc;

        // Fetch existing snapshot (for diff + return value).
        let existing = match conn.query_row(
            "SELECT id, entry_id, ordinal, type, text, attrs, created_at, updated_at
             FROM blocks WHERE id = ?1",
            params![id.to_string()],
            |row| row_to_block(row, 0),
        ) {
            Ok(b) => b,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Err(CoreError::NotFound(id)),
            Err(e) => return Err(CoreError::Storage(e)),
        };

        let new_type = r#type.unwrap_or_else(|| existing.r#type.clone());
        let new_text = text.unwrap_or_else(|| existing.text.clone());
        let new_attrs = attrs.unwrap_or_else(|| existing.attrs.clone());
        if !new_attrs.is_object() {
            return Err(CoreError::Validation("attrs must be a JSON object".into()));
        }
        let now = Utc::now();

        // UPDATE block (blocks_au trigger refreshes fts_blocks).
        conn.execute(
            "UPDATE blocks SET type = ?1, text = ?2, attrs = ?3, updated_at = ?4
             WHERE id = ?5",
            params![
                &new_type,
                &new_text,
                new_attrs.to_string(),
                now.to_rfc3339(),
                id.to_string(),
            ],
        )?;

        // Re-chunk only when text changed. The chunks_ad trigger cleans
        // vec_chunk_embeddings for each deleted chunk row.
        if new_text != existing.text {
            conn.execute(
                "DELETE FROM chunks WHERE block_id = ?1",
                params![id.to_string()],
            )?;
            let chunk_texts = crate::chunking::chunk_text(&new_text, self.chunk_target_size);
            let mut chunk_attrs = new_attrs.clone();
            if let Some(obj) = chunk_attrs.as_object_mut() {
                obj.insert(
                    "parent_block_type".into(),
                    serde_json::Value::String(new_type.clone()),
                );
            }
            for (chunk_ordinal, chunk_text) in chunk_texts.into_iter().enumerate() {
                let chunk_id = Ulid::new();
                conn.execute(
                    "INSERT INTO chunks (id, block_id, ordinal, text, attrs, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        chunk_id.to_string(),
                        id.to_string(),
                        chunk_ordinal as u32,
                        &chunk_text,
                        chunk_attrs.to_string(),
                        now.to_rfc3339(),
                        now.to_rfc3339(),
                    ],
                )
                .map_err(crate::storage::map_constraint_violation)?;
            }
        }

        let updated = Block {
            id,
            entry_id: existing.entry_id,
            ordinal: existing.ordinal,
            r#type: new_type,
            text: new_text,
            attrs: new_attrs,
            created_at: existing.created_at,
            updated_at: now,
        };

        // Emit block.updated event.
        let event_id = Ulid::new();
        let event_payload = serde_json::to_value(&updated).expect("block serializes");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event_id.to_string(),
                "block.updated",
                "block",
                id.to_string(),
                event_payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(crate::storage::map_constraint_violation)?;

        Ok(updated)
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

    /// Delete a block by id. Returns the pre-deletion snapshot. NotFound if
    /// the block doesn't exist. Emits `block.deleted` event with snapshot.
    pub fn delete(&self, id: Ulid) -> Result<Block, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.delete_in_tx(&conn, id);
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

    /// Execute delete within an existing transaction. Caller controls BEGIN/COMMIT.
    /// Does NOT lock self.conn. Does NOT call self methods that lock conn.
    pub fn delete_in_tx(&self, conn: &Connection, id: Ulid) -> Result<Block, CoreError> {
        use chrono::Utc;

        // Fetch pre-deletion snapshot (for event payload + return value)
        let block = match conn.query_row(
            "SELECT id, entry_id, ordinal, type, text, attrs, created_at, updated_at
             FROM blocks WHERE id = ?1",
            params![id.to_string()],
            |row| row_to_block(row, 0),
        ) {
            Ok(b) => b,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Err(CoreError::NotFound(id)),
            Err(e) => return Err(CoreError::Storage(e)),
        };

        conn.execute("DELETE FROM blocks WHERE id = ?1", params![id.to_string()])?;

        let event_id = Ulid::new();
        let event_payload = serde_json::to_value(&block).expect("block serializes");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event_id.to_string(),
                "block.deleted",
                "block",
                block.id.to_string(),
                event_payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(block)
    }
}

/// Map a SQLite row to a `Block`. Expects columns in the canonical order:
/// id, entry_id, ordinal, type, text, attrs, created_at, updated_at.
///
/// `offset` is kept for signature symmetry with `row_to_entry` (which also
/// takes an offset for the same reason — currently unused, reserved for
/// future prepared statements that prefix the row with other columns).
pub(crate) fn row_to_block(row: &rusqlite::Row<'_>, _offset: usize) -> rusqlite::Result<Block> {
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
        id: crate::storage::from_text(0, &id_str, ulid::Ulid::from_string)?,
        entry_id: crate::storage::from_text(1, &entry_id_str, ulid::Ulid::from_string)?,
        ordinal,
        r#type: ty,
        text,
        attrs,
        created_at: crate::storage::from_text(6, &created_str, DateTime::parse_from_rfc3339)?
            .with_timezone(&Utc),
        updated_at: crate::storage::from_text(7, &updated_str, DateTime::parse_from_rfc3339)?
            .with_timezone(&Utc),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_model::CreateBlock;
    use crate::{CreateEntry, EntryService};
    use serde_json::json;

    /// Create an entry (with no blocks) and return its ULID. BlockService
    /// tests need this because `blocks.entry_id` has an FK to `entries.id`.
    /// Uses direct SQL instead of EntryService::create because the latter
    /// requires at least one block and would collide with the block the test
    /// is about to create at ordinal 0.
    fn seed_entry(conn: Arc<Mutex<rusqlite::Connection>>) -> Ulid {
        let id = Ulid::new();
        let now = chrono::Utc::now();
        let conn = conn.lock().unwrap();
        conn.execute(
            "INSERT INTO entries (id, title, tags, attrs, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                id.to_string(),
                "seed",
                "[]",
                "{}",
                now.to_rfc3339(),
                now.to_rfc3339(),
            ],
        )
        .unwrap();
        id
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
        // Need a second entry. seed_entry uses direct SQL and generates a
        // fresh ULID each call.
        let entry_b = seed_entry(svc.conn.clone());

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

    #[test]
    fn delete_removes_block_and_returns_snapshot() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let created = seed_block(&svc, entry_id, 0, "claim");

        let deleted = svc.delete(created.id).unwrap();
        assert_eq!(deleted.id, created.id);
        assert_eq!(deleted.text, created.text);

        // Verify gone
        let err = svc.get(created.id).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn delete_emits_block_deleted_event() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let created = seed_block(&svc, entry_id, 0, "note");

        svc.delete(created.id).unwrap();

        let guard = svc.conn.lock().unwrap();
        let event_type: String = guard
            .query_row(
                "SELECT type FROM events WHERE type = 'block.deleted' AND target_id = ?1",
                params![created.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_type, "block.deleted");
    }

    #[test]
    fn delete_returns_not_found_for_missing_id() {
        let svc = BlockService::for_test().unwrap();
        let fake_id = Ulid::new();
        let err = svc.delete(fake_id).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn append_adds_block_at_end() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        // Seed one block at ordinal 0
        svc.create(CreateBlock {
            entry_id,
            ordinal: 0,
            r#type: "note".into(),
            text: "first".into(),
            attrs: None,
        })
        .unwrap();

        let appended = svc
            .append(entry_id, "claim".into(), "appended".into(), None)
            .unwrap();
        assert_eq!(appended.entry_id, entry_id);
        assert_eq!(appended.ordinal, 1, "append should use max ordinal + 1");
        assert_eq!(appended.r#type, "claim");

        let list = svc.list(entry_id).unwrap();
        assert_eq!(list.total, 2);
        assert_eq!(list.items[1].id, appended.id);
    }

    #[test]
    fn append_to_empty_entry_uses_ordinal_zero() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let appended = svc
            .append(entry_id, "note".into(), "first".into(), None)
            .unwrap();
        assert_eq!(appended.ordinal, 0);
    }

    #[test]
    fn append_emits_block_created_event() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let block = svc
            .append(entry_id, "note".into(), "x".into(), None)
            .unwrap();

        let guard = svc.conn.lock().unwrap();
        let event_type: String = guard
            .query_row(
                "SELECT type FROM events WHERE target_id = ?1 AND type = 'block.created'",
                params![block.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_type, "block.created");
    }

    // ----- update tests (Plan 5 Task 3) -----

    #[test]
    fn update_changes_block_text() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let block = svc
            .append(entry_id, "note".into(), "original".into(), None)
            .unwrap();

        let updated = svc
            .update(block.id, None, Some("new text".into()), None)
            .unwrap();
        assert_eq!(updated.text, "new text");
        assert_eq!(updated.r#type, "note"); // unchanged
    }

    #[test]
    fn update_re_chunks_when_text_changes() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        // Long text → multiple chunks
        let long_text: String = "para.\n\n".repeat(200); // > 1024 chars
        let block = svc
            .append(entry_id, "note".into(), long_text, None)
            .unwrap();

        let chunks = crate::chunk_service::ChunkService::new(svc.conn.clone()).unwrap();
        let before_count = chunks.list(block.id).unwrap().total;

        // Update to short text
        svc.update(block.id, None, Some("short".into()), None)
            .unwrap();
        let after_count = chunks.list(block.id).unwrap().total;
        assert!(
            after_count < before_count,
            "re-chunking should reduce chunk count for shorter text"
        );
        assert_eq!(after_count, 1);
    }

    #[test]
    fn update_emits_block_updated_event() {
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let block = svc
            .append(entry_id, "note".into(), "x".into(), None)
            .unwrap();

        svc.update(block.id, Some("claim".into()), None, None)
            .unwrap();

        let guard = svc.conn.lock().unwrap();
        let event_type: String = guard
            .query_row(
                "SELECT type FROM events WHERE target_id = ?1 AND type = 'block.updated'",
                params![block.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_type, "block.updated");
    }

    #[test]
    fn update_returns_not_found_for_missing_block() {
        let svc = BlockService::for_test().unwrap();
        let err = svc
            .update(Ulid::new(), None, Some("x".into()), None)
            .unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn delete_cascades_when_entry_deleted() {
        // The FK on blocks.entry_id is ON DELETE CASCADE. Verify EntryService::delete
        // removes the entry's blocks.
        let svc = BlockService::for_test().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        let entries = EntryService::new(svc.conn.clone(), content_store, 1024).unwrap();
        let entry = entries
            .create(CreateEntry {
                title: "x".into(),
                blocks: vec![crate::block_model::BlockInput {
                    r#type: "note".into(),
                    text: "y".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        // CreateEntry above created block at ordinal 0; add two more at 1, 2.
        seed_block(&svc, entry.id, 1, "note");
        seed_block(&svc, entry.id, 2, "note");
        // 3 total blocks: 1 from CreateEntry + 2 from seed_block.
        assert_eq!(svc.list(entry.id).unwrap().total, 3);

        entries.delete(entry.id).unwrap();

        assert_eq!(svc.list(entry.id).unwrap().total, 0);
    }

    #[test]
    fn list_returns_err_not_panic_on_corrupt_ulid() {
        // Plan 6 followup (P2-5): corrupted ULID in DB must surface as
        // Err(CoreError::Storage), not panic the daemon.
        //
        // We INSERT a row with a malformed id directly via SQL rather than
        // UPDATE-ing an existing row: UPDATE would violate the chunks.block_id
        // FK (BlockService::create_in_tx auto-derives a chunk that references
        // the block id). A fresh INSERT with the parent entry satisfies the
        // only relevant FK (blocks.entry_id) and leaves no chunk referencing
        // the corrupt id.
        let svc = BlockService::for_test().unwrap();
        let entry_id = seed_entry(svc.conn.clone());
        let now = chrono::Utc::now().to_rfc3339();
        {
            let conn = svc.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO blocks (id, entry_id, ordinal, type, text, attrs, created_at, updated_at)
                 VALUES ('NOT-A-ULID', ?1, 0, 'note', 'x', '{}', ?2, ?2)",
                params![entry_id.to_string(), now],
            )
            .unwrap();
        }
        let result = svc.list(entry_id);
        assert!(result.is_err(), "expected Err on corrupt ULID, got Ok");
        assert!(
            matches!(result, Err(CoreError::Storage(_))),
            "expected CoreError::Storage, got {result:?}"
        );
    }

    #[test]
    fn chunk_target_size_splits_block_when_set_below_default() {
        // Regression guard: the value threaded through BlockService::new must
        // actually reach chunking::chunk_text. Before config-ification, this
        // was a hardcoded 1024 and a tiny target would have had no effect.
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()));
        let tmp = tempfile::tempdir().unwrap();
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        crate::EntryService::new(conn.clone(), content_store, 50).unwrap();
        let svc = BlockService::new(conn, 50).unwrap();
        let entry_id = seed_entry(svc.conn.clone());

        // 200 chars, no paragraph or sentence boundaries → chunk_text's
        // hard-cut path produces 4 chunks of 50 chars each.
        let long_text = "x".repeat(200);
        let block = svc
            .append(entry_id, "note".into(), long_text, None)
            .unwrap();

        let chunks = crate::chunk_service::ChunkService::new(svc.conn.clone()).unwrap();
        let result = chunks.list(block.id).unwrap();
        assert!(
            result.total > 1,
            "expected multiple chunks with target_size=50, got {}",
            result.total
        );
        for chunk in &result.items {
            assert!(
                chunk.text.chars().count() <= 50,
                "chunk text exceeded target_size: {} chars",
                chunk.text.chars().count()
            );
        }
    }
}
