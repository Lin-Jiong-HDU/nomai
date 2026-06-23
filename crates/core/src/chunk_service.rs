//! ChunkService: read-only access to chunks (auto-derived from blocks).
//!
//! Plan 4: chunks are no longer user-managed. `BlockService::create_in_tx`
//! runs the chunking algorithm (Spec §10) and INSERTs derived chunks.
//! CASCADE on `chunks.block_id` removes them when the parent block is
//! deleted.
//!
//! This service exposes read-only access (list/get) + embedding helpers
//! (`vec_chunk_embeddings`) used by `semantic_search`.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, params};
use ulid::Ulid;

use crate::chunk_model::{Chunk, ChunkListResult, ChunkSearchResult};
use crate::error::CoreError;
use crate::storage;

pub struct ChunkService {
    conn: Arc<Mutex<Connection>>,
}

impl ChunkService {
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

    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        let tmp_dir = std::env::temp_dir().join(format!("nomai-test-{}", Ulid::new()));
        let content_store = Arc::new(crate::content_store::ContentStore::new(tmp_dir));
        crate::EntryService::new(conn.clone(), content_store)?;
        Self::new(conn)
    }

    /// Fetch a single chunk by id.
    pub fn get(&self, id: Ulid) -> Result<Chunk, CoreError> {
        let conn = self.conn.lock().unwrap();
        match conn.query_row(
            "SELECT id, block_id, ordinal, text, attrs, created_at, updated_at
             FROM chunks WHERE id = ?1",
            params![id.to_string()],
            |row| row_to_chunk(row, 0),
        ) {
            Ok(c) => Ok(c),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(CoreError::NotFound(id)),
            Err(e) => Err(CoreError::Storage(e)),
        }
    }

    /// List chunks for a block, ordered by ordinal.
    pub fn list(&self, block_id: Ulid) -> Result<ChunkListResult, CoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, block_id, ordinal, text, attrs, created_at, updated_at
             FROM chunks WHERE block_id = ?1 ORDER BY ordinal ASC",
        )?;
        let items: Result<Vec<Chunk>, _> = stmt
            .query_map(params![block_id.to_string()], |row| row_to_chunk(row, 0))?
            .collect();
        let items = items?;
        let total = items.len() as u64;
        Ok(ChunkListResult { items, total })
    }

    /// Ensure `vec_chunk_embeddings` virtual table exists. Idempotent.
    pub fn ensure_vec_chunk_embeddings(&self, dim: usize) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE IF NOT EXISTS vec_chunk_embeddings USING vec0(
                chunk_id TEXT PRIMARY KEY,
                embedding FLOAT[{dim}] distance_metric=cosine
            )"
        ))
        .map_err(CoreError::Storage)?;
        Ok(())
    }

    /// Upsert a chunk embedding. vec0 doesn't support `INSERT OR REPLACE`,
    /// so we DELETE-then-INSERT inside a transaction.
    pub fn write_embedding(&self, chunk_id: Ulid, embedding: &[f32]) -> Result<(), CoreError> {
        let bytes: Vec<u8> = embedding.iter().flat_map(|f| f.to_le_bytes()).collect();
        let id_str = chunk_id.to_string();
        let conn = self.conn.lock().unwrap();
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

    pub fn delete_embedding(&self, chunk_id: Ulid) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM vec_chunk_embeddings WHERE chunk_id = ?1",
            params![chunk_id.to_string()],
        )?;
        Ok(())
    }

    /// KNN search over chunk embeddings. Returns top-K chunks.
    /// `block_type` filter (Plan 4): when supplied, JOIN `blocks` to filter.
    pub fn semantic_search(
        &self,
        query_embedding: &[f32],
        limit: usize,
        block_type: Option<&str>,
    ) -> Result<Vec<ChunkSearchResult>, CoreError> {
        let query_bytes: Vec<u8> = query_embedding
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let conn = self.conn.lock().unwrap();
        let sql = match block_type {
            Some(_) => {
                "SELECT c.id, c.block_id, c.ordinal, c.text, c.attrs, c.created_at, c.updated_at,
                        vec.distance
                 FROM vec_chunk_embeddings vec
                 JOIN chunks c ON c.id = vec.chunk_id
                 JOIN blocks b ON b.id = c.block_id
                 WHERE vec.embedding MATCH ?1 AND k = ?2 AND b.type = ?3
                 ORDER BY vec.distance"
            }
            None => {
                "SELECT c.id, c.block_id, c.ordinal, c.text, c.attrs, c.created_at, c.updated_at,
                        vec.distance
                 FROM vec_chunk_embeddings vec
                 JOIN chunks c ON c.id = vec.chunk_id
                 WHERE vec.embedding MATCH ?1 AND k = ?2
                 ORDER BY vec.distance"
            }
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = match block_type {
            Some(t) => stmt
                .query_map(params![&query_bytes, limit as i64, t], |row| {
                    let chunk = row_to_chunk(row, 0)?;
                    let distance: f64 = row.get(7)?;
                    Ok(ChunkSearchResult {
                        chunk,
                        score: (1.0 - distance) as f32,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?,
            None => stmt
                .query_map(params![&query_bytes, limit as i64], |row| {
                    let chunk = row_to_chunk(row, 0)?;
                    let distance: f64 = row.get(7)?;
                    Ok(ChunkSearchResult {
                        chunk,
                        score: (1.0 - distance) as f32,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(rows)
    }
}

fn row_to_chunk(row: &rusqlite::Row<'_>, _offset: usize) -> rusqlite::Result<Chunk> {
    use chrono::{DateTime, Utc};

    let id_str: String = row.get(0)?;
    let block_id_str: String = row.get(1)?;
    let ordinal: u32 = row.get(2)?;
    let text: String = row.get(3)?;
    let attrs_str: String = row.get(4)?;
    let created_str: String = row.get(5)?;
    let updated_str: String = row.get(6)?;

    let attrs: serde_json::Value = serde_json::from_str(&attrs_str)
        .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));

    Ok(Chunk {
        id: id_str.parse().expect("ULID stored in DB is always valid"),
        block_id: block_id_str
            .parse()
            .expect("ULID stored in DB is always valid"),
        ordinal,
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
    use serde_json::json;
    use ulid::Ulid;

    /// Seed an entry with a single note block; return (entry_id, block_id, conn).
    /// BlockService::create_in_tx auto-derives chunks (Plan 4 Task 5), so the
    /// block will have 1 chunk under it after this.
    fn seed_entry_and_block() -> (Ulid, Ulid, Arc<Mutex<Connection>>) {
        let entries = crate::EntryService::for_test().unwrap();
        let conn = entries.conn_for_test();
        let entry = entries
            .create(crate::CreateEntry {
                title: "t".into(),
                blocks: vec![crate::BlockInput {
                    r#type: "note".into(),
                    text: "block body".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let blocks = crate::BlockService::new(conn.clone()).unwrap();
        let block_list = blocks.list(entry.id).unwrap();
        (entry.id, block_list.items[0].id, conn)
    }

    /// Insert a chunk directly via SQL (test helper for cases that need
    /// multiple chunks per block without going through chunking).
    fn insert_chunk_sql(
        conn: &Arc<Mutex<Connection>>,
        block_id: Ulid,
        ordinal: u32,
        text: &str,
        attrs: serde_json::Value,
    ) -> Ulid {
        let id = Ulid::new();
        let now = chrono::Utc::now();
        let c = conn.lock().unwrap();
        c.execute(
            "INSERT INTO chunks (id, block_id, ordinal, text, attrs, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id.to_string(),
                block_id.to_string(),
                ordinal,
                text,
                attrs.to_string(),
                now.to_rfc3339(),
                now.to_rfc3339(),
            ],
        )
        .unwrap();
        id
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
    fn list_returns_empty_for_block_with_no_chunks() {
        // Seed an entry+block; chunking auto-derives a chunk for "block body"
        // (one chunk because text ≤ 1024 chars). For an explicit no-chunks
        // case we insert a block by direct SQL with empty body — but
        // BlockService::create_in_tx is the only entry path and it always
        // chunks. Instead: query a freshly-seeded block whose chunks we then
        // delete directly.
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        // Wipe chunks for this block; then list should be empty.
        {
            let c = conn.lock().unwrap();
            c.execute(
                "DELETE FROM chunks WHERE block_id = ?1",
                params![block_id.to_string()],
            )
            .unwrap();
        }
        let chunks = ChunkService::new(conn).unwrap();
        let result = chunks.list(block_id).unwrap();
        assert_eq!(result.total, 0);
        assert!(result.items.is_empty());
    }

    #[test]
    fn list_returns_chunks_for_block_sorted_by_ordinal() {
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        // Wipe auto-derived chunks; insert 3 out of order via SQL.
        {
            let c = conn.lock().unwrap();
            c.execute(
                "DELETE FROM chunks WHERE block_id = ?1",
                params![block_id.to_string()],
            )
            .unwrap();
        }
        insert_chunk_sql(&conn, block_id, 2, "two", json!({}));
        insert_chunk_sql(&conn, block_id, 0, "zero", json!({}));
        insert_chunk_sql(&conn, block_id, 1, "one", json!({}));

        let chunks = ChunkService::new(conn).unwrap();
        let result = chunks.list(block_id).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.items[0].ordinal, 0);
        assert_eq!(result.items[0].text, "zero");
        assert_eq!(result.items[1].ordinal, 1);
        assert_eq!(result.items[2].ordinal, 2);
    }

    #[test]
    fn list_only_returns_chunks_for_target_block() {
        let entries = crate::EntryService::for_test().unwrap();
        let conn = entries.conn_for_test();
        let a = entries
            .create(crate::CreateEntry {
                title: "a".into(),
                blocks: vec![crate::BlockInput {
                    r#type: "note".into(),
                    text: "block a body".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let b = entries
            .create(crate::CreateEntry {
                title: "b".into(),
                blocks: vec![crate::BlockInput {
                    r#type: "note".into(),
                    text: "block b body".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let blocks = crate::BlockService::new(conn.clone()).unwrap();
        let block_a = blocks.list(a.id).unwrap().items[0].id;
        let block_b = blocks.list(b.id).unwrap().items[0].id;

        let chunks = ChunkService::new(conn).unwrap();
        let result_a = chunks.list(block_a).unwrap();
        assert_eq!(result_a.total, 1);
        assert_eq!(result_a.items[0].text, "block a body");

        let result_b = chunks.list(block_b).unwrap();
        assert_eq!(result_b.total, 1);
        assert_eq!(result_b.items[0].text, "block b body");
    }

    #[test]
    fn get_returns_chunk_by_id() {
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        let listed = chunks.list(block_id).unwrap();
        assert_eq!(listed.total, 1);
        let fetched = chunks.get(listed.items[0].id).unwrap();
        assert_eq!(listed.items[0], fetched);
    }

    #[test]
    fn get_returns_not_found_for_unknown_id() {
        let chunks = ChunkService::for_test().unwrap();
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = chunks.get(phantom).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn auto_derived_chunk_has_parent_block_type_attr() {
        // BlockService::create_in_tx (Plan 4 Task 5) should auto-derive
        // chunks with `parent_block_type` in attrs.
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn).unwrap();
        let result = chunks.list(block_id).unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.items[0].attrs["parent_block_type"], json!("note"));
    }

    #[test]
    fn ensure_vec_chunk_embeddings_is_idempotent() {
        crate::storage::init_sqlite_extensions();
        let chunks = ChunkService::for_test().unwrap();
        chunks.ensure_vec_chunk_embeddings(8).unwrap();
        chunks.ensure_vec_chunk_embeddings(8).unwrap();
    }

    #[test]
    fn write_embedding_persists_row_visible_via_direct_sql() {
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(4).unwrap();
        let chunk_id = chunks.list(block_id).unwrap().items[0].id;

        chunks
            .write_embedding(chunk_id, &[1.0, 0.0, 0.0, 0.0])
            .unwrap();

        let c = conn.lock().unwrap();
        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM vec_chunk_embeddings WHERE chunk_id = ?1",
                params![chunk_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn delete_embedding_removes_row() {
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(2).unwrap();
        let chunk_id = chunks.list(block_id).unwrap().items[0].id;
        chunks.write_embedding(chunk_id, &[1.0, 0.0]).unwrap();

        chunks.delete_embedding(chunk_id).unwrap();

        let c = conn.lock().unwrap();
        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM vec_chunk_embeddings WHERE chunk_id = ?1",
                params![chunk_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn semantic_search_ranks_chunks_by_cosine_similarity() {
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(3).unwrap();

        // Insert two chunks under the same block with known embeddings.
        {
            let c = conn.lock().unwrap();
            c.execute(
                "DELETE FROM chunks WHERE block_id = ?1",
                params![block_id.to_string()],
            )
            .unwrap();
        }
        let near = insert_chunk_sql(&conn, block_id, 0, "near", json!({}));
        let far = insert_chunk_sql(&conn, block_id, 1, "far", json!({}));
        chunks.write_embedding(near, &[1.0, 0.0, 0.0]).unwrap();
        chunks.write_embedding(far, &[0.0, 0.0, 1.0]).unwrap();

        let hits = chunks.semantic_search(&[0.9, 0.1, 0.0], 10, None).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk.id, near, "near should rank first");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn semantic_search_filters_by_block_type() {
        crate::storage::init_sqlite_extensions();
        let entries = crate::EntryService::for_test().unwrap();
        let conn = entries.conn_for_test();
        // Create one entry with a claim block + another with a note block.
        let a = entries
            .create(crate::CreateEntry {
                title: "a".into(),
                blocks: vec![crate::BlockInput {
                    r#type: "claim".into(),
                    text: "claim text".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let b = entries
            .create(crate::CreateEntry {
                title: "b".into(),
                blocks: vec![crate::BlockInput {
                    r#type: "note".into(),
                    text: "note text".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let blocks = crate::BlockService::new(conn.clone()).unwrap();
        let claim_block = blocks.list(a.id).unwrap().items[0].id;
        let note_block = blocks.list(b.id).unwrap().items[0].id;

        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(2).unwrap();
        let claim_chunk = chunks.list(claim_block).unwrap().items[0].id;
        let note_chunk = chunks.list(note_block).unwrap().items[0].id;
        chunks.write_embedding(claim_chunk, &[1.0, 0.0]).unwrap();
        chunks.write_embedding(note_chunk, &[1.0, 0.0]).unwrap();

        // Filter by block_type="claim": only the claim chunk matches.
        let hits = chunks
            .semantic_search(&[0.9, 0.1], 10, Some("claim"))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].chunk.id, claim_chunk);
    }

    #[test]
    fn semantic_search_returns_empty_when_no_embeddings() {
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn).unwrap();
        chunks.ensure_vec_chunk_embeddings(2).unwrap();
        // Auto-derived chunk exists but no embedding written.
        assert_eq!(chunks.list(block_id).unwrap().total, 1);

        let hits = chunks.semantic_search(&[1.0, 0.0], 10, None).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn semantic_search_respects_limit() {
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(2).unwrap();
        // Wipe auto-derived; insert 5 chunks with embeddings.
        {
            let c = conn.lock().unwrap();
            c.execute(
                "DELETE FROM chunks WHERE block_id = ?1",
                params![block_id.to_string()],
            )
            .unwrap();
        }
        for i in 0..5 {
            let cid = insert_chunk_sql(&conn, block_id, i, &format!("c{i}"), json!({}));
            chunks.write_embedding(cid, &[1.0, 0.0]).unwrap();
        }

        let hits = chunks.semantic_search(&[1.0, 0.0], 3, None).unwrap();
        assert_eq!(hits.len(), 3);
    }
}
