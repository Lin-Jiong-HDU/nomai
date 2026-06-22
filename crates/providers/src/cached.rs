//! CachedEmbedder: a transparent wrapper around `EmbeddingProvider` that
//! caches embeddings in SQLite keyed by `(model, blake3(body))`.
//!
//! Embeddings are deterministic functions of `(model, body)`, so cached
//! entries never expire. On `embed(texts)`:
//!   1. Hash all bodies (CPU-bound, sync).
//!   2. Batch lookup in `emb_cache` (sync, short lock).
//!   3. Collect misses, call `inner.embed(misses)` (await — no lock held).
//!   4. Persist new embeddings via `INSERT OR IGNORE` (sync, short lock).
//!   5. Assemble result preserving input order.
//!
//! See `docs/superpowers/specs/2026-06-22-embedding-cache-design.md`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params, types::Value as SqlValue};
use serde::{Deserialize, Serialize};

use crate::error::{ProviderError, ProviderErrorKind};
use crate::traits::EmbeddingProvider;

/// Snapshot of cache statistics returned by `CachedEmbedder::stats`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CacheStats {
    pub model: String,
    pub dim: usize,
    pub rows: u64,
    pub hits: u64,
    pub misses: u64,
    pub warn_rows: u64,
    pub warning: bool,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Options for `CachedEmbedder::clear`. All fields optional; absent filters
/// match every row. `before` and `keep_recent` may be combined with each
/// other and with `model`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClearOptions {
    /// Restrict to a single model namespace. `None` clears every model.
    #[serde(default)]
    pub model: Option<String>,
    /// Delete only rows created strictly before this timestamp (RFC3339).
    #[serde(default)]
    pub before: Option<DateTime<Utc>>,
    /// Keep only the N most-recent rows (by `created_at DESC`) among the
    /// rows matching the other filters; delete the rest. `None` deletes
    /// every matching row.
    #[serde(default)]
    pub keep_recent: Option<u64>,
}

/// Result of `CachedEmbedder::clear`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ClearResult {
    /// Total rows deleted.
    pub cleared: u64,
    /// Rows deleted, grouped by model.
    pub by_model: HashMap<String, u64>,
}

/// Transparent wrapper that caches embedding API results in SQLite.
///
/// Implements `EmbeddingProvider` by delegating cache-miss lookups to an
/// inner provider. All lookups and persists are confined to short critical
/// sections on the shared connection mutex; `inner.embed().await` is called
/// with no lock held so concurrent daemon work is not blocked on the network.
pub struct CachedEmbedder {
    inner: Arc<dyn EmbeddingProvider>,
    conn: Arc<Mutex<Connection>>,
    model: String,
    dim: usize,
    warn_rows: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl CachedEmbedder {
    pub fn new(
        inner: Arc<dyn EmbeddingProvider>,
        conn: Arc<Mutex<Connection>>,
        model: impl Into<String>,
        warn_rows: u64,
    ) -> Self {
        let dim = inner.dim();
        Self {
            inner,
            conn,
            model: model.into(),
            dim,
            warn_rows,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Snapshot of cache statistics: persistent row count plus in-memory
    /// hit/miss counters. `rows` reflects a fresh `COUNT(*)` on `emb_cache`
    /// filtered by `self.model`; counters are read atomically. `warning` is
    /// true when `rows > warn_rows` — the cache is never auto-evicted, this
    /// flag merely signals that the user may want to run `clear`.
    pub fn stats(&self) -> Result<CacheStats, ProviderError> {
        let rows = self.count_rows()?;
        Ok(CacheStats {
            model: self.model.clone(),
            dim: self.dim,
            rows,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            warn_rows: self.warn_rows,
            warning: rows > self.warn_rows,
        })
    }

    /// Delete cached embeddings matching `opts`. See [`ClearOptions`] for
    /// filter semantics. Returns per-model breakdown via `DELETE ... RETURNING`.
    /// Counters (`hits` / `misses`) are not reset — they reflect lifetime
    /// activity, not current cache contents.
    pub fn clear(&self, opts: ClearOptions) -> Result<ClearResult, ProviderError> {
        let conn = self.conn.lock().unwrap();

        // Build WHERE conditions and bound params in lock-step so the ?
        // placeholders line up with params after the IN-subquery OFFSET.
        let mut conditions: Vec<&'static str> = Vec::new();
        let mut params: Vec<SqlValue> = Vec::new();
        if let Some(m) = opts.model.as_ref() {
            conditions.push("model = ?");
            params.push(SqlValue::Text(m.clone()));
        }
        if let Some(before) = opts.before {
            conditions.push("created_at < ?");
            params.push(SqlValue::Text(before.to_rfc3339()));
        }
        let where_sql = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        // keep_recent: delete every row except the N newest (by created_at).
        // SQLite's `LIMIT -1 OFFSET N` selects everything past the first N.
        // The WHERE clause lives inside the subquery so filters apply before
        // the OFFSET picks the survivors to keep.
        let sql = if let Some(keep) = opts.keep_recent {
            params.push(SqlValue::Integer(keep as i64));
            format!(
                "DELETE FROM emb_cache WHERE (model, body_hash) IN (
                    SELECT model, body_hash FROM emb_cache {where_sql}
                    ORDER BY created_at DESC
                    LIMIT -1 OFFSET ?
                ) RETURNING model"
            )
        } else {
            format!("DELETE FROM emb_cache {where_sql} RETURNING model")
        };

        let mut by_model: HashMap<String, u64> = HashMap::new();
        let mut stmt = conn.prepare(&sql).map_err(storage_err)?;
        let param_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(param_refs.iter()), |row| {
                let model: String = row.get(0)?;
                Ok(model)
            })
            .map_err(storage_err)?;
        for row in rows {
            let model = row.map_err(storage_err)?;
            *by_model.entry(model).or_insert(0) += 1;
        }
        let cleared = by_model.values().sum();
        Ok(ClearResult { cleared, by_model })
    }

    fn count_rows(&self) -> Result<u64, ProviderError> {
        let conn = self.conn.lock().unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM emb_cache WHERE model = ?1",
                params![&self.model],
                |row| row.get(0),
            )
            .map_err(storage_err)?;
        Ok(n as u64)
    }

    /// Batch lookup: returns `Some(embedding)` for each hash already cached
    /// under `self.model` with matching `dim`, `None` for misses.
    /// On storage error, treats every input as a miss (safe fallback).
    fn lookup_batch(&self, hashes: &[[u8; 32]]) -> Vec<Option<Vec<f32>>> {
        let Ok(conn) = self.conn.lock() else {
            return (0..hashes.len()).map(|_| None).collect();
        };
        let Ok(mut stmt) = conn.prepare(
            "SELECT embedding FROM emb_cache WHERE model = ?1 AND body_hash = ?2 AND dim = ?3",
        ) else {
            return (0..hashes.len()).map(|_| None).collect();
        };
        let dim_i64 = self.dim as i64;
        hashes
            .iter()
            .map(|h| {
                let result = stmt.query_row(params![&self.model, h.to_vec(), dim_i64], |row| {
                    let blob: Vec<u8> = row.get(0)?;
                    Ok(decode_f32_le(&blob))
                });
                match result {
                    Ok(emb) => Some(emb),
                    Err(rusqlite::Error::QueryReturnedNoRows) => None,
                    Err(_) => None,
                }
            })
            .collect()
    }

    /// Persist a batch of newly computed embeddings. Failures are silently
    /// dropped — cache write failure must not affect the main embed flow.
    fn persist_batch(&self, indices: &[usize], hashes: &[[u8; 32]], embeddings: &[Vec<f32>]) {
        let Ok(conn) = self.conn.lock() else {
            return;
        };
        let Ok(mut stmt) = conn.prepare(
            "INSERT OR IGNORE INTO emb_cache (model, body_hash, dim, embedding, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        ) else {
            return;
        };
        let now = chrono::Utc::now().to_rfc3339();
        // zip-short-circuits when inner returns fewer embeddings than inputs
        // (test mocks do this) — only the returned subset is persisted.
        for (&idx, emb) in indices.iter().zip(embeddings.iter()) {
            let bytes: Vec<u8> = emb.iter().flat_map(|f| f.to_le_bytes()).collect();
            let _ = stmt.execute(params![
                &self.model,
                hashes[idx].to_vec(),
                self.dim as i64,
                &bytes,
                &now,
            ]);
        }
    }
}

#[async_trait]
impl EmbeddingProvider for CachedEmbedder {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        // 1. Hash all bodies (CPU-bound, sync).
        let hashes: Vec<[u8; 32]> = texts
            .iter()
            .map(|t| blake3::hash(t.as_bytes()).into())
            .collect();

        // 2. Batch lookup in SQLite (sync, short lock).
        let cached = self.lookup_batch(&hashes);

        // 3. Collect miss indices.
        let miss_indices: Vec<usize> = cached
            .iter()
            .enumerate()
            .filter_map(|(i, c)| if c.is_none() { Some(i) } else { None })
            .collect();

        // 4. Update counters (hit = cached, miss = uncached).
        let hit_count = texts.len() - miss_indices.len();
        self.hits.fetch_add(hit_count as u64, Ordering::Relaxed);
        self.misses
            .fetch_add(miss_indices.len() as u64, Ordering::Relaxed);

        // 5. Early return if everything hit.
        if miss_indices.is_empty() {
            return Ok(cached.into_iter().map(Option::unwrap).collect());
        }

        // 6. Embed misses via inner provider (await — NO lock held).
        let miss_texts: Vec<&str> = miss_indices.iter().map(|&i| texts[i]).collect();
        let new_embeddings = self.inner.embed(&miss_texts).await?;

        // 7. Persist new embeddings (sync, short lock; failures ignored).
        // zip-short-circuits: if inner returns fewer embeddings than inputs
        // (test mocks do this), only the returned subset is cached — mirrors
        // the inner provider's existing leniency.
        self.persist_batch(&miss_indices, &hashes, &new_embeddings);

        // 8. Assemble result preserving input order. Misses that did not get
        // an embedding from inner are filled with the inner's zero-pad vector
        // if available, otherwise left as the inner's actual output truncated.
        // For simplicity (and matching inner's existing behavior), we use
        // zip-style truncation: callers should ensure inner returns the
        // right count in production.
        let mut result: Vec<Vec<f32>> = Vec::with_capacity(texts.len());
        let mut miss_iter = new_embeddings.into_iter();
        for cached_opt in cached.into_iter() {
            match cached_opt {
                Some(emb) => result.push(emb),
                None => match miss_iter.next() {
                    Some(emb) => result.push(emb),
                    None => result.push(vec![0.0; self.dim]),
                },
            }
        }
        Ok(result)
    }

    fn dim(&self) -> usize {
        // Delegate to inner rather than returning cached self.dim: the inner
        // provider is the source of truth, and the two are kept in sync by
        // the constructor.
        self.inner.dim()
    }

    fn name(&self) -> &str {
        // Transparent wrapper: expose inner's identity (e.g. for provider.list).
        self.inner.name()
    }
}

fn decode_f32_le(blob: &[u8]) -> Vec<f32> {
    blob.chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

fn storage_err(e: rusqlite::Error) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Server,
        format!("emb_cache storage: {e}"),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    /// Mock embedder that returns deterministic vectors: each char's ASCII
    /// code padded to `dim`. Tracks call count so tests can assert cache hits.
    struct MockEmbedder {
        dim: usize,
        call_count: Mutex<usize>,
    }

    impl MockEmbedder {
        fn new(dim: usize) -> Self {
            Self {
                dim,
                call_count: Mutex::new(0),
            }
        }
        fn calls(&self) -> usize {
            *self.call_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl EmbeddingProvider for MockEmbedder {
        async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
            *(self.call_count.lock().unwrap()) += texts.len();
            Ok(texts
                .iter()
                .map(|t| {
                    let mut v = vec![0.0f32; self.dim];
                    for (i, b) in t.bytes().take(self.dim).enumerate() {
                        v[i] = b as f32;
                    }
                    v
                })
                .collect())
        }
        fn dim(&self) -> usize {
            self.dim
        }
        fn name(&self) -> &str {
            "mock"
        }
    }

    fn setup() -> (CachedEmbedder, Arc<MockEmbedder>, Arc<Mutex<Connection>>) {
        setup_with_warn(100)
    }

    fn setup_with_warn(
        warn_rows: u64,
    ) -> (CachedEmbedder, Arc<MockEmbedder>, Arc<Mutex<Connection>>) {
        // Run core migrations (creates emb_cache table).
        // We open an in-memory DB and embed the migration SQL inline because
        // providers cannot depend on core (would create a cycle).
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE emb_cache (
               model       TEXT    NOT NULL,
               body_hash   BLOB    NOT NULL,
               dim         INTEGER NOT NULL,
               embedding   BLOB    NOT NULL,
               created_at  TEXT    NOT NULL,
               PRIMARY KEY (model, body_hash)
             ) WITHOUT ROWID;",
        )
        .unwrap();
        let conn = Arc::new(Mutex::new(conn));
        let inner = Arc::new(MockEmbedder::new(4));
        let cached = CachedEmbedder::new(inner.clone(), conn.clone(), "test-model", warn_rows);
        (cached, inner, conn)
    }

    /// Insert a row directly with a specific model + created_at, bypassing
    /// the wrapper. Used to test `before` and `keep_recent` filters.
    fn insert_row(conn: &Arc<Mutex<Connection>>, model: &str, hash: u8, created_at: &str) {
        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![model, vec![hash; 32], 4, vec![0u8; 16], created_at],
            )
            .unwrap();
    }

    #[tokio::test]
    async fn embed_empty_input_returns_empty_without_db_access() {
        let (cached, inner, _conn) = setup();
        let result = cached.embed(&[]).await.unwrap();
        assert!(result.is_empty());
        assert_eq!(inner.calls(), 0);
    }

    #[tokio::test]
    async fn first_call_misses_second_call_hits() {
        let (cached, inner, _conn) = setup();
        // First call: 2 misses.
        let r1 = cached.embed(&["foo", "bar"]).await.unwrap();
        assert_eq!(r1.len(), 2);
        assert_eq!(inner.calls(), 2);

        let s1 = cached.stats().unwrap();
        assert_eq!(s1.hits, 0);
        assert_eq!(s1.misses, 2);
        assert_eq!(s1.rows, 2);

        // Second call with same texts: 2 hits.
        let r2 = cached.embed(&["foo", "bar"]).await.unwrap();
        assert_eq!(r2.len(), 2);
        assert_eq!(r2[0], r1[0]);
        assert_eq!(r2[1], r1[1]);
        assert_eq!(inner.calls(), 2, "inner not called on cache hit");

        let s2 = cached.stats().unwrap();
        assert_eq!(s2.hits, 2);
        assert_eq!(s2.misses, 2);
        assert_eq!(s2.rows, 2);
        assert!((s2.hit_rate() - 0.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn embed_persists_across_wrapper_instances_sharing_conn() {
        let (cached1, inner1, conn) = setup();
        cached1.embed(&["hello"]).await.unwrap();
        assert_eq!(inner1.calls(), 1);

        // Build a fresh CachedEmbedder on the same connection: cache survives.
        let inner2 = Arc::new(MockEmbedder::new(4));
        let cached2 = CachedEmbedder::new(inner2.clone(), conn, "test-model", 100);
        cached2.embed(&["hello"]).await.unwrap();
        assert_eq!(inner2.calls(), 0, "should hit persistent cache");
    }

    #[tokio::test]
    async fn different_model_names_do_not_share_cache() {
        let (cached_a, inner_a, conn) = setup();
        cached_a.embed(&["shared body"]).await.unwrap();
        assert_eq!(inner_a.calls(), 1);

        // Same conn, different model → fresh cache namespace.
        let inner_b = Arc::new(MockEmbedder::new(4));
        let cached_b = CachedEmbedder::new(inner_b.clone(), conn, "other-model", 100);
        cached_b.embed(&["shared body"]).await.unwrap();
        assert_eq!(inner_b.calls(), 1, "different model should miss");
    }

    #[tokio::test]
    async fn miss_and_hit_interleave_correctly() {
        let (cached, _inner, _conn) = setup();
        cached.embed(&["a", "b", "c"]).await.unwrap(); // 3 misses
        cached.embed(&["a", "x", "c"]).await.unwrap(); // 2 hits, 1 miss

        let s = cached.stats().unwrap();
        assert_eq!(s.hits, 2);
        assert_eq!(s.misses, 4);
        assert_eq!(s.rows, 4);
    }

    #[tokio::test]
    async fn stats_returns_zero_initially() {
        let (cached, _inner, _conn) = setup();
        let s = cached.stats().unwrap();
        assert_eq!(s.rows, 0);
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 0);
        assert_eq!(s.hit_rate(), 0.0);
        assert_eq!(s.model, "test-model");
        assert_eq!(s.dim, 4);
    }

    #[tokio::test]
    async fn clear_removes_all_models_when_no_filter() {
        let (cached, _inner, conn) = setup();
        cached.embed(&["a"]).await.unwrap();
        insert_row(&conn, "other", 1, "2026-01-01T00:00:00Z");

        assert_eq!(cached.stats().unwrap().rows, 1); // only own model counted
        let result = cached.clear(ClearOptions::default()).unwrap();
        assert_eq!(result.cleared, 2);
        assert_eq!(result.by_model.get("test-model"), Some(&1));
        assert_eq!(result.by_model.get("other"), Some(&1));
        assert_eq!(cached.stats().unwrap().rows, 0);
    }

    #[tokio::test]
    async fn clear_with_model_filter_only_clears_matching() {
        let (cached, _inner, conn) = setup();
        cached.embed(&["a"]).await.unwrap(); // 1 row in test-model
        insert_row(&conn, "other", 1, "2026-01-01T00:00:00Z");

        let result = cached
            .clear(ClearOptions {
                model: Some("other".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.cleared, 1);
        assert_eq!(result.by_model.get("other"), Some(&1));
        assert_eq!(cached.stats().unwrap().rows, 1, "own model untouched");
    }

    #[tokio::test]
    async fn clear_by_before_deletes_only_older_rows() {
        let (cached, _inner, conn) = setup();
        // 3 rows in test-model with different created_at.
        insert_row(&conn, "test-model", 1, "2026-01-01T00:00:00Z");
        insert_row(&conn, "test-model", 2, "2026-02-01T00:00:00Z");
        insert_row(&conn, "test-model", 3, "2026-03-01T00:00:00Z");

        let cutoff = DateTime::parse_from_rfc3339("2026-01-15T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = cached
            .clear(ClearOptions {
                before: Some(cutoff),
                ..Default::default()
            })
            .unwrap();
        // Only the 2026-01-01 row is before cutoff.
        assert_eq!(result.cleared, 1);
        assert_eq!(cached.stats().unwrap().rows, 2);
    }

    #[tokio::test]
    async fn clear_keep_recent_preserves_newest_n() {
        let (cached, _inner, conn) = setup();
        insert_row(&conn, "test-model", 1, "2026-01-01T00:00:00Z");
        insert_row(&conn, "test-model", 2, "2026-02-01T00:00:00Z");
        insert_row(&conn, "test-model", 3, "2026-03-01T00:00:00Z");
        insert_row(&conn, "test-model", 4, "2026-04-01T00:00:00Z");
        assert_eq!(cached.stats().unwrap().rows, 4);

        // Keep newest 1: should delete 3 oldest, leave 2026-04-01.
        let result = cached
            .clear(ClearOptions {
                keep_recent: Some(1),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.cleared, 3);
        assert_eq!(cached.stats().unwrap().rows, 1);
        // Verify the remaining row is the newest.
        let remaining_at: String = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT created_at FROM emb_cache WHERE model = 'test-model'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining_at, "2026-04-01T00:00:00Z");
    }

    #[tokio::test]
    async fn clear_combine_model_and_keep_recent() {
        let (cached, _inner, conn) = setup();
        // 2 in test-model, 2 in other-model.
        insert_row(&conn, "test-model", 1, "2026-01-01T00:00:00Z");
        insert_row(&conn, "test-model", 2, "2026-02-01T00:00:00Z");
        insert_row(&conn, "other-model", 3, "2026-01-01T00:00:00Z");
        insert_row(&conn, "other-model", 4, "2026-02-01T00:00:00Z");

        // Keep newest 1 row of test-model only.
        let result = cached
            .clear(ClearOptions {
                model: Some("test-model".into()),
                keep_recent: Some(1),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.cleared, 1);
        assert_eq!(result.by_model.get("test-model"), Some(&1));
        assert_eq!(result.by_model.get("other-model"), None);
        // other-model untouched.
        let n: i64 = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM emb_cache WHERE model = 'other-model'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn clear_does_not_reset_counters() {
        let (cached, _inner, _conn) = setup();
        cached.embed(&["a"]).await.unwrap();
        cached.clear(ClearOptions::default()).unwrap();
        let s = cached.stats().unwrap();
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 1, "counters persist across clear");
    }

    #[tokio::test]
    async fn stats_warning_false_below_threshold() {
        let (cached, _inner, _conn) = setup_with_warn(100);
        cached.embed(&["a", "b"]).await.unwrap(); // 2 rows
        let s = cached.stats().unwrap();
        assert_eq!(s.warn_rows, 100);
        assert!(!s.warning, "rows=2 < warn_rows=100");
    }

    #[tokio::test]
    async fn stats_warning_true_above_threshold() {
        let (cached, _inner, _conn) = setup_with_warn(1);
        cached.embed(&["a", "b"]).await.unwrap(); // 2 rows
        let s = cached.stats().unwrap();
        assert_eq!(s.warn_rows, 1);
        assert!(s.warning, "rows=2 > warn_rows=1");
    }

    #[tokio::test]
    async fn repeated_body_in_one_batch_uses_one_cache_row() {
        let (cached, _inner, conn) = setup();
        cached.embed(&["dup", "dup", "unique"]).await.unwrap();

        let s = cached.stats().unwrap();
        assert_eq!(s.rows, 2, "two distinct bodies → two cache rows");
        assert_eq!(s.misses, 3, "no dedup at cache layer; inner may dedup");

        // Direct SQL: ensure only 2 distinct hashes stored.
        let c = conn.lock().unwrap();
        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM emb_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn inner_failure_does_not_persist_and_propagates_error() {
        struct FailingEmbedder;
        #[async_trait]
        impl EmbeddingProvider for FailingEmbedder {
            async fn embed(&self, _texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
                Err(ProviderError::new(
                    ProviderErrorKind::Server,
                    "boom",
                    Some(500),
                ))
            }
            fn dim(&self) -> usize {
                4
            }
            fn name(&self) -> &str {
                "failing"
            }
        }

        let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        conn.lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE emb_cache (
               model TEXT NOT NULL, body_hash BLOB NOT NULL, dim INTEGER NOT NULL,
               embedding BLOB NOT NULL, created_at TEXT NOT NULL,
               PRIMARY KEY (model, body_hash)
             ) WITHOUT ROWID;",
            )
            .unwrap();
        let cached = CachedEmbedder::new(Arc::new(FailingEmbedder), conn.clone(), "test", 100);

        let err = cached.embed(&["x"]).await.unwrap_err();
        assert!(err.message.contains("boom"));

        // Nothing should have been written.
        let c = conn.lock().unwrap();
        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM emb_cache", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn name_delegates_to_inner() {
        let (cached, _inner, _conn) = setup();
        assert_eq!(cached.name(), "mock");
    }
}
