//! ChunkService: storage and retrieval of entry chunks.
//!
//! Each chunk is an independently embeddable piece of an entry. Chunks are
//! immutable (no update); re-chunking is delete + create. Emission follows
//! the Phase 2 Events pattern (chunk.created / chunk.deleted).

use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, params};
use ulid::Ulid;

use crate::chunk_model::{Chunk, CreateChunk};
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
}
