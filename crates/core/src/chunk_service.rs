//! ChunkService: storage and retrieval of entry chunks.
//!
//! Each chunk is an independently embeddable piece of an entry. Chunks are
//! immutable (no update); re-chunking is delete + create. Emission follows
//! the Phase 2 Events pattern (chunk.created / chunk.deleted).

use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, params};
use ulid::Ulid;

use crate::chunk_model::{Chunk, ChunkListResult, ChunkSearchResult, CreateChunk};
use crate::error::CoreError;
use crate::storage;

pub struct ChunkService {
    // Consumed by CRUD + vec methods + semantic_search (Tasks 2-6).
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

    pub fn create(&self, params: CreateChunk) -> Result<Chunk, CoreError> {
        let attrs = params
            .attrs
            .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
        if !attrs.is_object() {
            return Err(CoreError::Validation("attrs must be a JSON object".into()));
        }

        let now = Utc::now();
        let chunk = Chunk {
            id: Ulid::new(),
            entry_id: params.entry_id,
            ordinal: params.ordinal,
            text: params.text,
            attrs,
            created_at: now,
            updated_at: now,
        };

        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = (|| -> rusqlite::Result<()> {
            match conn.execute(
                "INSERT INTO chunks (id, entry_id, ordinal, text, attrs, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    chunk.id.to_string(),
                    chunk.entry_id.to_string(),
                    chunk.ordinal as i64,
                    &chunk.text,
                    chunk.attrs.to_string(),
                    chunk.created_at.to_rfc3339(),
                    chunk.updated_at.to_rfc3339(),
                ],
            ) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }?;
            // Emit event.
            let event_id = Ulid::new();
            let event_payload = serde_json::to_value(&chunk).expect("chunk serialize");
            conn.execute(
                "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    event_id.to_string(),
                    "chunk.created",
                    "chunk",
                    chunk.id.to_string(),
                    event_payload.to_string(),
                    Utc::now().to_rfc3339(),
                ],
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                conn.execute_batch("COMMIT")?;
                Ok(chunk)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                // Map FK + UNIQUE constraint violations to Validation.
                if let rusqlite::Error::SqliteFailure(ref fe, _) = e {
                    if fe.code == rusqlite::ErrorCode::ConstraintViolation {
                        return Err(CoreError::Validation(format!(
                            "chunk constraint violation: {e}"
                        )));
                    }
                }
                Err(CoreError::Storage(e))
            }
        }
    }

    pub fn get(&self, id: Ulid) -> Result<Chunk, CoreError> {
        let conn = self.conn.lock().unwrap();
        match conn.query_row(
            "SELECT id, entry_id, ordinal, text, attrs, created_at, updated_at
             FROM chunks WHERE id = ?1",
            params![id.to_string()],
            row_to_chunk,
        ) {
            Ok(chunk) => Ok(chunk),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(CoreError::NotFound(id)),
            Err(e) => Err(CoreError::Storage(e)),
        }
    }

    pub fn list(
        &self,
        entry_id: Ulid,
        limit: u32,
        offset: u32,
    ) -> Result<ChunkListResult, CoreError> {
        let conn = self.conn.lock().unwrap();

        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM chunks WHERE entry_id = ?1",
            params![entry_id.to_string()],
            |row| row.get(0),
        )?;

        let mut stmt = conn.prepare(
            "SELECT id, entry_id, ordinal, text, attrs, created_at, updated_at
             FROM chunks WHERE entry_id = ?1
             ORDER BY ordinal ASC
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(
            params![entry_id.to_string(), limit as i64, offset as i64],
            row_to_chunk,
        )?;
        let items: Vec<Chunk> = rows.collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(ChunkListResult {
            items,
            total: total as u64,
        })
    }

    pub fn delete(&self, id: Ulid) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = (|| -> rusqlite::Result<Option<()>> {
            // SELECT before-snapshot first.
            let row_result = conn.query_row(
                "SELECT id, entry_id, ordinal, text, attrs, created_at, updated_at
                 FROM chunks WHERE id = ?1",
                params![id.to_string()],
                row_to_chunk,
            );
            let before_chunk = match row_result {
                Ok(c) => c,
                Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
                Err(e) => return Err(e),
            };

            conn.execute(
                "DELETE FROM chunks WHERE id = ?1",
                params![id.to_string()],
            )?;

            // Emit event with before-snapshot.
            let event_id = Ulid::new();
            let event_payload = serde_json::to_value(&before_chunk).expect("chunk serialize");
            conn.execute(
                "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    event_id.to_string(),
                    "chunk.deleted",
                    "chunk",
                    id.to_string(),
                    event_payload.to_string(),
                    Utc::now().to_rfc3339(),
                ],
            )?;
            Ok(Some(()))
        })();
        match result {
            Ok(Some(())) => {
                conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Ok(None) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(CoreError::NotFound(id))
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(CoreError::Storage(e))
            }
        }
    }

    pub fn ensure_vec_chunk_embeddings(&self, dim: usize) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        let exists: bool = conn.query_row(
            "SELECT EXISTS (
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='vec_chunk_embeddings'
            )",
            [],
            |row| row.get(0),
        )?;
        if exists {
            return Ok(());
        }
        let sql = format!(
            "CREATE VIRTUAL TABLE vec_chunk_embeddings USING vec0(
                chunk_id TEXT PRIMARY KEY,
                embedding float[{dim}] distance_metric=cosine
            )"
        );
        conn.execute_batch(&sql)?;
        Ok(())
    }

    pub fn write_embedding(&self, id: Ulid, embedding: &[f32]) -> Result<(), CoreError> {
        let bytes: Vec<u8> = embedding.iter().flat_map(|f| f.to_le_bytes()).collect();
        let id_str = id.to_string();
        let conn = self.conn.lock().unwrap();
        // sqlite-vec's vec0 virtual table does not support INSERT OR REPLACE.
        // Emulate the upsert with a DELETE-then-INSERT inside a transaction so
        // the operation is atomic and re-inserting the same id replaces the row.
        conn.execute_batch("BEGIN")?;
        let result = (|| -> rusqlite::Result<()> {
            conn.execute(
                "DELETE FROM vec_chunk_embeddings WHERE chunk_id = ?1",
                params![&id_str],
            )?;
            conn.execute(
                "INSERT INTO vec_chunk_embeddings (chunk_id, embedding) VALUES (?1, ?2)",
                params![&id_str, &bytes],
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(CoreError::Storage(e))
            }
        }
    }

    pub fn delete_embedding(&self, id: Ulid) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM vec_chunk_embeddings WHERE chunk_id = ?1",
            params![id.to_string()],
        )?;
        Ok(())
    }

    pub fn semantic_search(
        &self,
        query: &[f32],
        limit: u32,
    ) -> Result<Vec<ChunkSearchResult>, CoreError> {
        let bytes: Vec<u8> = query.iter().flat_map(|f| f.to_le_bytes()).collect();
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT c.id, c.entry_id, c.ordinal, c.text, c.attrs, c.created_at, c.updated_at,
                    v.distance
             FROM vec_chunk_embeddings v
             JOIN chunks c ON c.id = v.chunk_id
             WHERE v.embedding MATCH ?1
               AND k = ?2
             ORDER BY v.distance",
        )?;
        let rows = stmt.query_map(params![bytes, limit], |row| {
            let chunk = row_to_chunk(row)?;
            let distance: f64 = row.get(7)?;
            Ok(ChunkSearchResult {
                chunk,
                score: (1.0 - distance) as f32,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(CoreError::Storage)
    }
}

fn row_to_chunk(row: &rusqlite::Row<'_>) -> rusqlite::Result<Chunk> {
    let id_str: String = row.get(0)?;
    let entry_id_str: String = row.get(1)?;
    let ordinal_i64: i64 = row.get(2)?;
    let text: String = row.get(3)?;
    let attrs_json: String = row.get(4)?;
    let created_at_str: String = row.get(5)?;
    let updated_at_str: String = row.get(6)?;

    let id = from_text(0, &id_str, Ulid::from_string)?;
    let entry_id = from_text(1, &entry_id_str, Ulid::from_string)?;
    let attrs: serde_json::Value = from_text(4, &attrs_json, |s| serde_json::from_str(s))?;
    let created_at = from_text(5, &created_at_str, chrono::DateTime::parse_from_rfc3339)?
        .with_timezone(&Utc);
    let updated_at = from_text(6, &updated_at_str, chrono::DateTime::parse_from_rfc3339)?
        .with_timezone(&Utc);

    Ok(Chunk {
        id,
        entry_id,
        ordinal: ordinal_i64 as u32,
        text,
        attrs,
        created_at,
        updated_at,
    })
}

fn from_text<T, E>(
    idx: usize,
    s: &str,
    f: impl for<'a> FnOnce(&'a str) -> Result<T, E>,
) -> rusqlite::Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    f(s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, Box::new(e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_model::CreateChunk;
    use crate::event_service::EventService;
    use crate::{CreateEntry, EntryService, ListEventsQuery};
    use serde_json::json;
    use ulid::Ulid;

    fn setup_with_entry() -> (EntryService, ChunkService, Ulid) {
        let entries = EntryService::for_test().unwrap();
        let conn = entries.conn_for_test();
        let chunks = ChunkService::new(conn).unwrap();
        let e = entries
            .create(CreateEntry {
                title: "container".into(),
                body: "body".into(),
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        (entries, chunks, e.id)
    }

    #[test]
    fn for_test_creates_chunk_service_with_chunks_table() {
        let svc = ChunkService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn create_returns_chunk_with_generated_id_and_default_attrs() {
        let (_, chunks, entry_id) = setup_with_entry();
        let chunk = chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 0,
                text: "first chunk".into(),
                attrs: None,
            })
            .unwrap();
        assert_eq!(chunk.entry_id, entry_id);
        assert_eq!(chunk.ordinal, 0);
        assert_eq!(chunk.text, "first chunk");
        assert_eq!(chunk.attrs, json!({}));
        assert!(chunk.created_at <= chrono::Utc::now());
    }

    #[test]
    fn create_persists_chunk_retrievable_via_list_later() {
        // list is implemented in Task 3; for now use direct SQL to verify persistence.
        let (_, chunks, entry_id) = setup_with_entry();
        let chunk = chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 5,
                text: "persisted".into(),
                attrs: Some(json!({"section": "intro"})),
            })
            .unwrap();

        let conn = chunks.conn.lock().unwrap();
        let (eid, ord, text, attrs_json): (String, i64, String, String) = conn
            .query_row(
                "SELECT entry_id, ordinal, text, attrs FROM chunks WHERE id = ?1",
                rusqlite::params![chunk.id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(eid, entry_id.to_string());
        assert_eq!(ord, 5);
        assert_eq!(text, "persisted");
        assert_eq!(attrs_json, r#"{"section":"intro"}"#);
    }

    #[test]
    fn create_rejects_non_object_attrs() {
        let (_, chunks, entry_id) = setup_with_entry();
        let err = chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 0,
                text: "x".into(),
                attrs: Some(json!([1, 2, 3])),
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_returns_validation_when_entry_does_not_exist() {
        // FK violation. With PRAGMA foreign_keys = ON, SQLite returns
        // SQLITE_CONSTRAINT ForeignKey.
        let chunks = ChunkService::for_test().unwrap();
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = chunks
            .create(CreateChunk {
                entry_id: phantom,
                ordinal: 0,
                text: "x".into(),
                attrs: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_returns_validation_on_duplicate_entry_ordinal() {
        let (_, chunks, entry_id) = setup_with_entry();
        chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 0,
                text: "first".into(),
                attrs: None,
            })
            .unwrap();
        let err = chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 0, // duplicate
                text: "second".into(),
                attrs: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_allows_same_entry_different_ordinal() {
        let (_, chunks, entry_id) = setup_with_entry();
        chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 0,
                text: "first".into(),
                attrs: None,
            })
            .unwrap();
        // Different ordinal: should succeed.
        chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 1,
                text: "second".into(),
                attrs: None,
            })
            .unwrap();
    }

    #[test]
    fn create_emits_chunk_created_event_with_full_snapshot() {
        let (entries, chunks, entry_id) = setup_with_entry();
        let events = EventService::for_test_shared_with_entries(&entries);

        let chunk = chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 0,
                text: "snippet".into(),
                attrs: None,
            })
            .unwrap();

        let result = events.list(ListEventsQuery::default()).unwrap();
        // 2 events: 1x entry.created (from setup) + 1x chunk.created
        let chunk_events: Vec<_> = result
            .items
            .iter()
            .filter(|e| e.type_ == "chunk.created")
            .collect();
        assert_eq!(chunk_events.len(), 1);
        let event = chunk_events[0];
        assert_eq!(event.target_type, "chunk");
        assert_eq!(event.target_id, chunk.id);
        assert_eq!(event.payload["ordinal"], 0);
        assert_eq!(event.payload["text"], "snippet");
        assert_eq!(event.payload["entry_id"], entry_id.to_string());
    }

    fn seed_chunk(chunks: &ChunkService, entry_id: Ulid, ordinal: u32, text: &str) -> Ulid {
        chunks
            .create(CreateChunk {
                entry_id,
                ordinal,
                text: text.into(),
                attrs: None,
            })
            .unwrap()
            .id
    }

    #[test]
    fn get_returns_chunk_created_by_create() {
        let (_, chunks, entry_id) = setup_with_entry();
        let created = chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 0,
                text: "x".into(),
                attrs: None,
            })
            .unwrap();
        let fetched = chunks.get(created.id).unwrap();
        assert_eq!(created, fetched);
    }

    #[test]
    fn get_returns_not_found_for_unknown_id() {
        let chunks = ChunkService::for_test().unwrap();
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = chunks.get(phantom).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn list_returns_chunks_for_entry_sorted_by_ordinal() {
        let (_, chunks, entry_id) = setup_with_entry();
        // Insert out of order — list should sort by ordinal ascending.
        seed_chunk(&chunks, entry_id, 2, "two");
        seed_chunk(&chunks, entry_id, 0, "zero");
        seed_chunk(&chunks, entry_id, 1, "one");

        let result = chunks.list(entry_id, 100, 0).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.items.len(), 3);
        assert_eq!(result.items[0].ordinal, 0);
        assert_eq!(result.items[0].text, "zero");
        assert_eq!(result.items[1].ordinal, 1);
        assert_eq!(result.items[2].ordinal, 2);
    }

    #[test]
    fn list_paginates_with_limit_and_offset() {
        let (_, chunks, entry_id) = setup_with_entry();
        for i in 0..5 {
            seed_chunk(&chunks, entry_id, i, &format!("c{i}"));
        }
        let page1 = chunks.list(entry_id, 2, 0).unwrap();
        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.total, 5);
        assert_eq!(page1.items[0].ordinal, 0);

        let page2 = chunks.list(entry_id, 2, 2).unwrap();
        assert_eq!(page2.items.len(), 2);
        assert_eq!(page2.items[0].ordinal, 2);
    }

    #[test]
    fn list_returns_empty_for_entry_with_no_chunks() {
        let (_, chunks, entry_id) = setup_with_entry();
        let result = chunks.list(entry_id, 100, 0).unwrap();
        assert_eq!(result.total, 0);
        assert!(result.items.is_empty());
    }

    #[test]
    fn list_only_returns_chunks_for_specified_entry() {
        let entries = EntryService::for_test().unwrap();
        let conn = entries.conn_for_test();
        let chunks = ChunkService::new(conn).unwrap();
        let a = entries
            .create(CreateEntry {
                title: "a".into(),
                body: "x".into(),
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let b = entries
            .create(CreateEntry {
                title: "b".into(),
                body: "y".into(),
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        seed_chunk(&chunks, a.id, 0, "a0");
        seed_chunk(&chunks, b.id, 0, "b0");

        let result_a = chunks.list(a.id, 100, 0).unwrap();
        assert_eq!(result_a.total, 1);
        assert_eq!(result_a.items[0].text, "a0");

        let result_b = chunks.list(b.id, 100, 0).unwrap();
        assert_eq!(result_b.total, 1);
        assert_eq!(result_b.items[0].text, "b0");
    }

    #[test]
    fn delete_removes_chunk() {
        let (_, chunks, entry_id) = setup_with_entry();
        let id = seed_chunk(&chunks, entry_id, 0, "x");
        chunks.delete(id).unwrap();
        let err = chunks.get(id).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn delete_returns_not_found_for_unknown_id() {
        let chunks = ChunkService::for_test().unwrap();
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = chunks.delete(phantom).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn delete_emits_chunk_deleted_event_with_before_snapshot() {
        let (entries, chunks, entry_id) = setup_with_entry();
        let events = EventService::for_test_shared_with_entries(&entries);
        let chunk = chunks
            .create(CreateChunk {
                entry_id,
                ordinal: 0,
                text: "to be deleted".into(),
                attrs: None,
            })
            .unwrap();

        chunks.delete(chunk.id).unwrap();

        let result = events.list(ListEventsQuery::default()).unwrap();
        let delete_events: Vec<_> = result
            .items
            .iter()
            .filter(|e| e.type_ == "chunk.deleted")
            .collect();
        assert_eq!(delete_events.len(), 1);
        let event = delete_events[0];
        assert_eq!(event.target_id, chunk.id);
        // Payload is BEFORE snapshot (chunk no longer in chunks table but event retains it).
        assert_eq!(event.payload["text"], "to be deleted");
        assert_eq!(event.payload["ordinal"], 0);
    }

    #[test]
    fn ensure_vec_chunk_embeddings_is_idempotent() {
        crate::storage::init_sqlite_extensions();
        let chunks = ChunkService::for_test().unwrap();
        chunks.ensure_vec_chunk_embeddings(8).unwrap();
        chunks.ensure_vec_chunk_embeddings(8).unwrap(); // idempotent
    }

    #[test]
    fn write_embedding_persists_row_visible_via_direct_sql() {
        crate::storage::init_sqlite_extensions();
        let (_, chunks, entry_id) = setup_with_entry();
        chunks.ensure_vec_chunk_embeddings(4).unwrap();
        let c = seed_chunk(&chunks, entry_id, 0, "x");

        chunks.write_embedding(c, &[1.0, 0.0, 0.0, 0.0]).unwrap();

        // Verify via direct SQL that the row exists.
        let conn = chunks.conn.lock().unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM vec_chunk_embeddings WHERE chunk_id = ?1",
                rusqlite::params![c.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn delete_embedding_removes_row_visible_via_direct_sql() {
        crate::storage::init_sqlite_extensions();
        let (_, chunks, entry_id) = setup_with_entry();
        chunks.ensure_vec_chunk_embeddings(2).unwrap();
        let c = seed_chunk(&chunks, entry_id, 0, "x");
        chunks.write_embedding(c, &[1.0, 0.0]).unwrap();

        chunks.delete_embedding(c).unwrap();

        let conn = chunks.conn.lock().unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM vec_chunk_embeddings WHERE chunk_id = ?1",
                rusqlite::params![c.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn deleting_entry_cascades_to_chunks() {
        // FK ON DELETE CASCADE: removing entry removes all its chunks.
        let (entries, chunks, entry_id) = setup_with_entry();
        let c1 = seed_chunk(&chunks, entry_id, 0, "first");
        let c2 = seed_chunk(&chunks, entry_id, 1, "second");

        entries.delete(entry_id).unwrap();

        // Both chunks should be gone (CASCADE).
        assert!(matches!(
            chunks.get(c1).unwrap_err(),
            CoreError::NotFound(_)
        ));
        assert!(matches!(
            chunks.get(c2).unwrap_err(),
            CoreError::NotFound(_)
        ));
    }

    #[test]
    fn semantic_search_ranks_chunks_by_cosine_similarity() {
        crate::storage::init_sqlite_extensions();
        let (_, chunks, entry_id) = setup_with_entry();
        chunks.ensure_vec_chunk_embeddings(3).unwrap();

        let near = seed_chunk(&chunks, entry_id, 0, "near");
        let far = seed_chunk(&chunks, entry_id, 1, "far");
        chunks.write_embedding(near, &[1.0, 0.0, 0.0]).unwrap();
        chunks.write_embedding(far, &[0.0, 0.0, 1.0]).unwrap();

        // Query close to near.
        let hits = chunks.semantic_search(&[0.9, 0.1, 0.0], 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk.id, near, "near should rank first");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn semantic_search_returns_empty_when_no_embeddings() {
        crate::storage::init_sqlite_extensions();
        let (_, chunks, entry_id) = setup_with_entry();
        chunks.ensure_vec_chunk_embeddings(2).unwrap();
        // Chunk exists but no embedding.
        seed_chunk(&chunks, entry_id, 0, "x");

        let hits = chunks.semantic_search(&[1.0, 0.0], 10).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn semantic_search_respects_limit() {
        crate::storage::init_sqlite_extensions();
        let (_, chunks, entry_id) = setup_with_entry();
        chunks.ensure_vec_chunk_embeddings(2).unwrap();
        for i in 0..5 {
            let c = seed_chunk(&chunks, entry_id, i, &format!("c{i}"));
            chunks.write_embedding(c, &[1.0, 0.0]).unwrap();
        }

        let hits = chunks.semantic_search(&[1.0, 0.0], 3).unwrap();
        assert_eq!(hits.len(), 3);
    }
}
