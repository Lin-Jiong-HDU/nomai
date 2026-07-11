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

use crate::chunk_model::{Chunk, ChunkListResult, ChunkSearchResult, DimReconciliation};
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
        let tmp = tempfile::tempdir()?;
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        crate::EntryService::new(conn.clone(), content_store, 1024)?;
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

    /// Ensure `vec_chunk_embeddings` virtual table exists with the requested
    /// dim. Returns the reconciliation action taken:
    /// - `Created { dim }` if the table was missing and we created it
    /// - `Consistent { dim }` if the table existed with a matching dim
    /// - `Recreated { from, to }` if the table existed at a different dim —
    ///   we DROP + CREATE
    ///
    /// V9 (Plan 5) creates this table with the daemon default dim (1536) so
    /// the `chunks_ad` DELETE trigger can resolve it at fire time. Users with
    /// a non-default `config.embedding.dim` (e.g. GLM at 2048) would otherwise
    /// hit a vec0 "Dimension mismatch" error on the first embedding write, so
    /// the daemon reconciles at boot.
    ///
    /// The `Recreated` path loses existing `vec_chunk_embeddings` rows but
    /// `emb_cache` (keyed by content hash, FK-free, independent of this table)
    /// preserves them — the next `semantic_search` re-embeds from cache with
    /// zero API calls.
    pub fn ensure_vec_chunk_embeddings(&self, dim: usize) -> Result<DimReconciliation, CoreError> {
        let conn = self.conn.lock().unwrap();

        // Check if table exists; capture its CREATE SQL if so.
        let existing_sql: Option<String> = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='vec_chunk_embeddings'",
                [],
                |row| row.get(0),
            )
            .ok();

        let Some(sql) = existing_sql else {
            // Table doesn't exist — create fresh at the requested dim.
            conn.execute_batch(&format!(
                "CREATE VIRTUAL TABLE vec_chunk_embeddings USING vec0(
                    chunk_id TEXT PRIMARY KEY,
                    embedding FLOAT[{dim}] distance_metric=cosine
                )"
            ))
            .map_err(CoreError::Storage)?;
            return Ok(DimReconciliation::Created { dim });
        };

        // Parse the dim baked into the existing CREATE VIRTUAL TABLE SQL.
        // Format: `... embedding FLOAT[N] distance_metric=cosine ...`
        let actual_dim = parse_vec_dim(&sql).ok_or_else(|| {
            CoreError::Storage(rusqlite::Error::InvalidParameterName(format!(
                "cannot parse dim from vec_chunk_embeddings SQL: {sql}"
            )))
        })?;

        if actual_dim == dim {
            return Ok(DimReconciliation::Consistent { dim });
        }

        // Mismatch — DROP + CREATE at the requested dim.
        conn.execute_batch("DROP TABLE vec_chunk_embeddings")
            .map_err(CoreError::Storage)?;
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE vec_chunk_embeddings USING vec0(
                chunk_id TEXT PRIMARY KEY,
                embedding FLOAT[{dim}] distance_metric=cosine
            )"
        ))
        .map_err(CoreError::Storage)?;

        Ok(DimReconciliation::Recreated {
            from: actual_dim,
            to: dim,
        })
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
        id: crate::storage::from_text(0, &id_str, ulid::Ulid::from_string)?,
        block_id: crate::storage::from_text(1, &block_id_str, ulid::Ulid::from_string)?,
        ordinal,
        text,
        attrs,
        created_at: crate::storage::from_text(5, &created_str, DateTime::parse_from_rfc3339)?
            .with_timezone(&Utc),
        updated_at: crate::storage::from_text(6, &updated_str, DateTime::parse_from_rfc3339)?
            .with_timezone(&Utc),
    })
}

/// Parse the embedding dim from a vec0 CREATE VIRTUAL TABLE SQL.
/// Matches the first `FLOAT[N]` token in the SQL text (the embedding
/// column declaration). Returns `None` if no token is found or the inner
/// text isn't a valid `usize`.
fn parse_vec_dim(sql: &str) -> Option<usize> {
    let marker = "FLOAT[";
    let start = sql.find(marker)? + marker.len();
    let rest = &sql[start..];
    let end = rest.find(']')?;
    rest[..end].parse().ok()
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
                attachments: None,
            })
            .unwrap();
        let blocks = crate::BlockService::new(conn.clone(), 1024).unwrap();
        let block_list = blocks.list(entry.id).unwrap();
        (entry.id, block_list.items[0].id, conn)
    }

    /// Build a 1536-dim embedding from a short prefix (rest zero-padded).
    /// V9 migration creates `vec_chunk_embeddings` with dim=1536 (daemon
    /// default); tests override to dim=1536 via ensure_vec_chunk_embeddings
    /// before writing, so 1536 is the effective test dim.
    fn vec_1536(prefix: &[f32]) -> Vec<f32> {
        let mut v = vec![0.0f32; 1536];
        for (i, x) in prefix.iter().enumerate() {
            v[i] = *x;
        }
        v
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
                attachments: None,
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
                attachments: None,
            })
            .unwrap();
        let blocks = crate::BlockService::new(conn.clone(), 1024).unwrap();
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
        // V9 migration creates vec_chunk_embeddings with dim=1536 (daemon
        // default). First call requests dim=8, which mismatches 1536 → the
        // table is dropped and recreated at dim=8. Second call matches the
        // new dim → Consistent. Both calls must succeed (no error path).
        chunks.ensure_vec_chunk_embeddings(8).unwrap();
        chunks.ensure_vec_chunk_embeddings(8).unwrap();
    }

    #[test]
    fn ensure_vec_chunk_embeddings_creates_when_missing() {
        crate::storage::init_sqlite_extensions();
        let svc = ChunkService::for_test().unwrap();
        // Manually drop the table that V9 created
        {
            let conn = svc.conn.lock().unwrap();
            conn.execute_batch("DROP TABLE vec_chunk_embeddings")
                .unwrap();
        }
        let result = svc.ensure_vec_chunk_embeddings(2048).unwrap();
        match result {
            DimReconciliation::Created { dim } => assert_eq!(dim, 2048),
            other => panic!("expected Created, got {other:?}"),
        }
        // Verify table exists with dim 2048
        let conn = svc.conn.lock().unwrap();
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='vec_chunk_embeddings'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(sql.contains("FLOAT[2048]"), "table should have dim 2048");
    }

    #[test]
    fn ensure_vec_chunk_embeddings_recreates_on_dim_mismatch() {
        crate::storage::init_sqlite_extensions();
        let svc = ChunkService::for_test().unwrap();
        // V9 created with dim=1536 (daemon default). Request dim=2048.
        let result = svc.ensure_vec_chunk_embeddings(2048).unwrap();
        match result {
            DimReconciliation::Recreated { from, to } => {
                assert_eq!(from, 1536);
                assert_eq!(to, 2048);
            }
            other => panic!("expected Recreated, got {other:?}"),
        }
        // Verify table now has dim 2048
        let conn = svc.conn.lock().unwrap();
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='vec_chunk_embeddings'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(sql.contains("FLOAT[2048]"));
    }

    #[test]
    fn ensure_vec_chunk_embeddings_returns_consistent_when_dims_match() {
        crate::storage::init_sqlite_extensions();
        let svc = ChunkService::for_test().unwrap();
        // V9 created with dim=1536. Request dim=1536.
        let result = svc.ensure_vec_chunk_embeddings(1536).unwrap();
        assert!(matches!(
            result,
            DimReconciliation::Consistent { dim: 1536 }
        ));
    }

    #[test]
    fn ensure_vec_chunk_embeddings_recreate_preserves_emb_cache() {
        // emb_cache is independent of vec_chunk_embeddings (keyed by content
        // hash, FK-free). Recreating the vec table doesn't touch emb_cache.
        crate::storage::init_sqlite_extensions();
        let svc = ChunkService::for_test().unwrap();
        // Manually insert into emb_cache
        {
            let conn = svc.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at)
                 VALUES ('test-model', X'00', 1536, ?1, '2026-06-23T10:00:00Z')",
                [&[0u8; 4][..]],
            )
            .unwrap();
        }
        // Recreate vec_chunk_embeddings with new dim
        let _ = svc.ensure_vec_chunk_embeddings(2048).unwrap();
        // emb_cache row still there
        let conn = svc.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM emb_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 1, "emb_cache should be untouched by vec recreate");
    }

    #[test]
    fn write_embedding_persists_row_visible_via_direct_sql() {
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(1536).unwrap();
        let chunk_id = chunks.list(block_id).unwrap().items[0].id;

        let emb = vec_1536(&[1.0]);
        chunks.write_embedding(chunk_id, &emb).unwrap();

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
        chunks.ensure_vec_chunk_embeddings(1536).unwrap();
        let chunk_id = chunks.list(block_id).unwrap().items[0].id;
        let emb = vec_1536(&[1.0]);
        chunks.write_embedding(chunk_id, &emb).unwrap();

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
        chunks.ensure_vec_chunk_embeddings(1536).unwrap();

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
        // near aligned with +X axis, far aligned with +Z axis.
        chunks.write_embedding(near, &vec_1536(&[1.0])).unwrap();
        chunks
            .write_embedding(far, &vec_1536(&[0.0, 0.0, 1.0]))
            .unwrap();

        // Query slightly off +X: near should rank above far.
        let hits = chunks
            .semantic_search(&vec_1536(&[0.9, 0.1, 0.0]), 10, None)
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk.id, near, "near should rank first");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn semantic_search_uses_cosine_not_l2_for_non_unit_vectors() {
        // Regression: V9 must declare vec_chunk_embeddings with
        // distance_metric=cosine. Without it, vec0 defaults to L2, and L2
        // gives different rankings than cosine for non-unit vectors. Existing
        // tests use unit vectors (axis-aligned) which are metric-invariant,
        // so they can't catch the bug.
        //
        // Here A = [1, 0, ...] and B = 2*A = [2, 0, ...]: same direction.
        // Cosine says both should match query [1,0,...] with similarity 1.0
        // (distance 0). L2 says B is at distance 1.0 from the query (A's
        // distance is 0), so under L2 their scores differ.
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(1536).unwrap();

        // Wipe auto-derived chunk; insert two manual chunks for A and B.
        {
            let c = conn.lock().unwrap();
            c.execute(
                "DELETE FROM chunks WHERE block_id = ?1",
                params![block_id.to_string()],
            )
            .unwrap();
        }
        let chunk_a = insert_chunk_sql(&conn, block_id, 0, "a", json!({}));
        let chunk_b = insert_chunk_sql(&conn, block_id, 1, "b", json!({}));

        let mut a = vec![0.0f32; 1536];
        a[0] = 1.0;
        let mut b = vec![0.0f32; 1536];
        b[0] = 2.0;
        chunks.write_embedding(chunk_a, &a).unwrap();
        chunks.write_embedding(chunk_b, &b).unwrap();

        let hits = chunks.semantic_search(&a, 10, None).unwrap();
        assert_eq!(hits.len(), 2, "both chunks should be returned");
        // Both should score ~1.0 (identical direction under cosine). Under L2,
        // B's score would be 1.0 - 1.0 = 0.0 because |B - A| = 1.
        let max_diff = hits
            .iter()
            .map(|h| (h.score - 1.0).abs())
            .fold(0.0f32, f32::max);
        assert!(
            max_diff < 0.01,
            "cosine scores should all be ≈1.0 for same-direction vectors, got: {:?}",
            hits.iter().map(|h| h.score).collect::<Vec<_>>()
        );
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
                attachments: None,
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
                attachments: None,
            })
            .unwrap();
        let blocks = crate::BlockService::new(conn.clone(), 1024).unwrap();
        let claim_block = blocks.list(a.id).unwrap().items[0].id;
        let note_block = blocks.list(b.id).unwrap().items[0].id;

        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(1536).unwrap();
        let claim_chunk = chunks.list(claim_block).unwrap().items[0].id;
        let note_chunk = chunks.list(note_block).unwrap().items[0].id;
        let emb = vec_1536(&[1.0]);
        chunks.write_embedding(claim_chunk, &emb).unwrap();
        chunks.write_embedding(note_chunk, &emb).unwrap();

        // Filter by block_type="claim": only the claim chunk matches.
        let hits = chunks
            .semantic_search(&vec_1536(&[0.9, 0.1]), 10, Some("claim"))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].chunk.id, claim_chunk);
    }

    #[test]
    fn semantic_search_returns_empty_when_no_embeddings() {
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn).unwrap();
        chunks.ensure_vec_chunk_embeddings(1536).unwrap();
        // Auto-derived chunk exists but no embedding written.
        assert_eq!(chunks.list(block_id).unwrap().total, 1);

        let hits = chunks.semantic_search(&vec_1536(&[1.0]), 10, None).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn semantic_search_respects_limit() {
        crate::storage::init_sqlite_extensions();
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        chunks.ensure_vec_chunk_embeddings(1536).unwrap();
        // Wipe auto-derived; insert 5 chunks with embeddings.
        {
            let c = conn.lock().unwrap();
            c.execute(
                "DELETE FROM chunks WHERE block_id = ?1",
                params![block_id.to_string()],
            )
            .unwrap();
        }
        let emb = vec_1536(&[1.0]);
        for i in 0..5 {
            let cid = insert_chunk_sql(&conn, block_id, i, &format!("c{i}"), json!({}));
            chunks.write_embedding(cid, &emb).unwrap();
        }

        let hits = chunks.semantic_search(&vec_1536(&[1.0]), 3, None).unwrap();
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn ensure_vec_chunk_embeddings_errors_on_corrupt_sql_not_panic() {
        // Invariant: if `vec_chunk_embeddings` SQL is malformed (lacks
        // `FLOAT[N]`), `ensure_vec_chunk_embeddings` must return `Err`, never
        // panic. parse_vec_dim returns None on this SQL, and production code
        // must surface that as `CoreError::Storage`.
        crate::storage::init_sqlite_extensions();
        let svc = ChunkService::for_test().unwrap();

        // Strategy 1: DROP well-formed table + CREATE with `FLOAT` (no `[N]`).
        // If vec0 rejects that at CREATE time, fall through to strategy 2.
        {
            let conn = svc.conn.lock().unwrap();
            conn.execute_batch("DROP TABLE vec_chunk_embeddings")
                .unwrap();
            let created = conn.execute_batch(
                "CREATE VIRTUAL TABLE vec_chunk_embeddings USING vec0(
                    chunk_id TEXT PRIMARY KEY,
                    embedding FLOAT distance_metric=cosine
                )",
            );
            if created.is_err() {
                // Strategy 2: replace the well-formed table with a plain
                // (non-virtual) table whose SQL has no FLOAT[N] token.
                conn.execute_batch(
                    "CREATE TABLE vec_chunk_embeddings (
                        chunk_id TEXT PRIMARY KEY,
                        embedding BLOB
                    )",
                )
                .unwrap();
            }
        }

        // Must return Err, not panic.
        let result = svc.ensure_vec_chunk_embeddings(1536);
        assert!(
            result.is_err(),
            "expected Err on corrupt SQL, got {:?}",
            result
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, CoreError::Storage(_)),
            "expected CoreError::Storage, got {:?}",
            err
        );
    }

    #[test]
    fn list_returns_err_not_panic_on_corrupt_ulid() {
        // Plan 6 followup (P2-5): corrupted ULID in DB must surface as
        // Err(CoreError::Storage), not panic the daemon.
        let (_entry_id, block_id, conn) = seed_entry_and_block();
        let chunks = ChunkService::new(conn.clone()).unwrap();
        // seed_entry_and_block auto-derives 1 chunk under the block.
        let chunk_list = chunks.list(block_id).unwrap();
        assert_eq!(chunk_list.total, 1);
        let chunk_id = chunk_list.items[0].id;
        {
            let guard = conn.lock().unwrap();
            guard
                .execute(
                    "UPDATE chunks SET id = 'NOT-A-ULID' WHERE id = ?1",
                    params![chunk_id.to_string()],
                )
                .unwrap();
        }
        let result = chunks.list(block_id);
        assert!(result.is_err(), "expected Err on corrupt ULID, got Ok");
        assert!(
            matches!(result, Err(CoreError::Storage(_))),
            "expected CoreError::Storage, got {result:?}"
        );
    }
}
