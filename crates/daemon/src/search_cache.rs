//! SearchCache: in-memory cache for search.semantic / search.fulltext
//! results. Generation-based invalidation; bump on every mutation
//! that affects search results.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use nomai_core::CoreError;
use serde::Serialize;
use serde_json::Value;

/// Which search RPC a cached result came from. Part of the cache key so
/// the same query string hitting both RPCs doesn't collide.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) enum SearchRpc {
    Semantic,
    Fulltext,
    Hybrid,
}

/// Cache key. `generation` is the snapshot of the daemon-wide counter at
/// lookup time; bumping the counter invalidates every prior key without
/// iterating the map.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) struct Key {
    pub(crate) generation: u64,
    pub(crate) rpc: SearchRpc,
    pub(crate) query_hash: [u8; 32],
    pub(crate) limit: u32,
    pub(crate) block_type_hash: Option<u64>,
    pub(crate) tag_hash: Option<u64>,
    pub(crate) weights_hash: Option<u64>,
}

/// Hash an optional filter string into a fixed-size `Option<u64>` for use
/// as part of the cache key. Used for both `block_type` and `tag` so the
/// key stays fixed-size and DRY. `blake3::Hash::as_bytes()` is always 32
/// bytes (a structural invariant of blake3), so `[..8]` + `try_into()` into
/// `[u8; 8]` cannot fail — the `expect` documents the invariant and is
/// never reached on user paths.
fn hash_opt(s: Option<&str>) -> Option<u64> {
    s.map(|s| {
        let h = blake3::hash(s.as_bytes());
        let bytes: [u8; 8] = h.as_bytes()[..8]
            .try_into()
            .expect("blake3 yields 32 bytes");
        u64::from_le_bytes(bytes)
    })
}

/// Hash a pair of f32 weights into a `u64` for use as part of the cache
/// key. Two weight vectors that differ are never confused; identical weight
/// vectors hash identically.
pub(crate) fn hash_weights(fw: f32, sw: f32) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&fw.to_le_bytes());
    hasher.update(&sw.to_le_bytes());
    let h = hasher.finalize();
    let bytes: [u8; 8] = h.as_bytes()[..8]
        .try_into()
        .expect("blake3 yields 32 bytes");
    u64::from_le_bytes(bytes)
}

/// Cached value. `Arc` so a hit path is one atomic increment (no deep
/// clone of the `Vec<Value>`).
pub(crate) type CachedResults = Arc<Vec<Value>>;

/// In-process search-results cache. Lives in the daemon; core is unaware.
pub struct SearchCache {
    map: DashMap<Key, CachedResults>,
    generation: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    semantic_hits: AtomicU64,
    semantic_misses: AtomicU64,
    fulltext_hits: AtomicU64,
    fulltext_misses: AtomicU64,
    hybrid_hits: AtomicU64,
    hybrid_misses: AtomicU64,
}

/// Snapshot of cache statistics returned by `SearchCache::stats`.
#[derive(Debug, Clone, Serialize)]
pub struct SearchCacheStats {
    pub generation: u64,
    pub entries: u64,
    pub hits: u64,
    pub misses: u64,
    pub semantic_hits: u64,
    pub semantic_misses: u64,
    pub fulltext_hits: u64,
    pub fulltext_misses: u64,
    pub hybrid_hits: u64,
    pub hybrid_misses: u64,
}

impl SearchCacheStats {
    /// Hits divided by total lookups; `0.0` when no lookups yet.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

impl SearchCache {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            generation: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            semantic_hits: AtomicU64::new(0),
            semantic_misses: AtomicU64::new(0),
            fulltext_hits: AtomicU64::new(0),
            fulltext_misses: AtomicU64::new(0),
            hybrid_hits: AtomicU64::new(0),
            hybrid_misses: AtomicU64::new(0),
        }
    }

    /// Current generation counter value. Hook points call
    /// `bump_generation` to invalidate the cache.
    // Public API for lib-mode users / future RPCs that want raw generation
    // without the full stats snapshot. `stats()` reads the atomic directly
    // rather than going through this method, so it has no in-tree caller.
    #[allow(dead_code)]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Atomically bump generation. Prior cached keys become unfindable
    /// (their embedded `generation` no longer matches any new lookup).
    pub fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of entries currently held in the map (across all generations;
    /// old-generation entries linger until `clear`).
    // Public API for lib-mode users / future RPCs that want raw len without
    // the full stats snapshot. `stats()` casts `self.map.len()` directly
    // rather than going through this method, so it has no in-tree caller.
    #[allow(dead_code)]
    #[allow(clippy::len_without_is_empty)] // surfaced via cache.stats
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Empty the map. Returns the count removed. Does NOT bump generation:
    /// after clear the map is empty, so the next lookup misses regardless
    /// of generation. Counters are intentionally not reset (matches
    /// emb_cache's `clear` semantics).
    pub fn clear(&self) -> u64 {
        let cleared = self.map.len() as u64;
        self.map.clear();
        cleared
    }

    /// Look up a cached result by `(rpc, query, limit, block_type, tag,
    /// weights_hash)`. On hit, returns the cached `Arc<Vec<Value>>` (one
    /// atomic increment). On miss, invokes `compute()`, persists the result,
    /// and returns it. Failures from `compute()` propagate and are NOT cached.
    ///
    /// `block_type`/`tag: Option<&str>` — `None` means no filter; `Some(s)`
    /// is hashed (via shared `hash_opt`) so the key is fixed-size. None vs
    /// Some("") are distinct.
    ///
    /// `weights_hash: Option<u64>` — `None` for RPCs that don't use weights
    /// (fulltext, semantic); `Some(hash_weights(fw, sw))` for RPCs that do
    /// (hybrid). Required so two Hybrid calls with different weight values
    /// don't share the same cache entry.
    ///
    /// Generic bounds (`F: FnOnce() -> Fut, Fut: Future`) let call sites
    /// pass `|| async move { ... }` async closures; verified by tests.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn lookup_or_compute<F, Fut>(
        &self,
        rpc: SearchRpc,
        query: &str,
        limit: u32,
        block_type: Option<&str>,
        tag: Option<&str>,
        weights_hash: Option<u64>,
        compute: F,
    ) -> Result<CachedResults, CoreError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Vec<Value>, CoreError>>,
    {
        let key = Key {
            generation: self.generation.load(Ordering::Relaxed),
            rpc,
            query_hash: blake3::hash(query.as_bytes()).into(),
            limit,
            block_type_hash: hash_opt(block_type),
            tag_hash: hash_opt(tag),
            weights_hash,
        };

        // 1. Lookup. Ref drops at end of block so compute() doesn't hold
        //    a DashMap shard lock (DashMap Ref isn't Send across .await).
        {
            if let Some(entry) = self.map.get(&key) {
                self.bump_counter(rpc, /*hit=*/ true);
                return Ok(Arc::clone(&entry));
            }
        }

        // 2. Miss → compute (no lock held).
        self.bump_counter(rpc, /*hit=*/ false);
        let items = Arc::new(compute().await?);

        // 3. Store. Concurrent compute on same key: last writer wins. Both
        //    produce identical values for a deterministic function.
        self.map.insert(key, Arc::clone(&items));
        Ok(items)
    }

    /// Snapshot of cache statistics.
    pub fn stats(&self) -> SearchCacheStats {
        SearchCacheStats {
            generation: self.generation.load(Ordering::Relaxed),
            entries: self.map.len() as u64,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            semantic_hits: self.semantic_hits.load(Ordering::Relaxed),
            semantic_misses: self.semantic_misses.load(Ordering::Relaxed),
            fulltext_hits: self.fulltext_hits.load(Ordering::Relaxed),
            fulltext_misses: self.fulltext_misses.load(Ordering::Relaxed),
            hybrid_hits: self.hybrid_hits.load(Ordering::Relaxed),
            hybrid_misses: self.hybrid_misses.load(Ordering::Relaxed),
        }
    }

    fn bump_counter(&self, rpc: SearchRpc, hit: bool) {
        let (total, per_rpc) = match (rpc, hit) {
            (SearchRpc::Semantic, true) => (&self.hits, &self.semantic_hits),
            (SearchRpc::Semantic, false) => (&self.misses, &self.semantic_misses),
            (SearchRpc::Fulltext, true) => (&self.hits, &self.fulltext_hits),
            (SearchRpc::Fulltext, false) => (&self.misses, &self.fulltext_misses),
            (SearchRpc::Hybrid, true) => (&self.hits, &self.hybrid_hits),
            (SearchRpc::Hybrid, false) => (&self.misses, &self.hybrid_misses),
        };
        total.fetch_add(1, Ordering::Relaxed);
        per_rpc.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for SearchCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_empty_at_generation_zero() {
        let cache = SearchCache::new();
        assert_eq!(cache.generation(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn bump_generation_increments_counter() {
        let cache = SearchCache::new();
        cache.bump_generation();
        cache.bump_generation();
        assert_eq!(cache.generation(), 2);
    }

    #[test]
    fn clear_returns_zero_on_empty_cache() {
        let cache = SearchCache::new();
        assert_eq!(cache.clear(), 0);
        assert_eq!(cache.len(), 0);
    }

    use serde_json::json;

    fn mk_cache() -> SearchCache {
        SearchCache::new()
    }

    #[tokio::test]
    async fn lookup_miss_then_hit() {
        let cache = mk_cache();
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();
        let r1 = cache
            .lookup_or_compute(SearchRpc::Fulltext, "rust", 10, None, None, None, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(vec![json!({"score": 0.5})])
                }
            })
            .await
            .unwrap();
        assert_eq!(r1.len(), 1);

        let cc2 = call_count.clone();
        let r2 = cache
            .lookup_or_compute(SearchRpc::Fulltext, "rust", 10, None, None, None, || {
                let cc = cc2.clone();
                async move {
                    cc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(vec![json!({"score": 0.5})])
                }
            })
            .await
            .unwrap();
        assert_eq!(r2.len(), 1);
        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "compute_fn called once total (second was a hit)"
        );
    }

    #[tokio::test]
    async fn bump_generation_invalidates_old_keys() {
        let cache = mk_cache();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let run = |_c: std::sync::Arc<std::sync::atomic::AtomicU32>| {
            let _cache_ref = &cache; // borrow for closure
            // We need owned access; use a helper below instead.
            unreachable!()
        };
        let _ = run; // suppress unused warning; real logic below
        // First call: miss, populates cache.
        let c1 = calls.clone();
        let _ = cache
            .lookup_or_compute(SearchRpc::Fulltext, "q", 5, None, None, None, || {
                let c1 = c1.clone();
                async move {
                    c1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(vec![])
                }
            })
            .await
            .unwrap();
        // Bump.
        cache.bump_generation();
        // Same query, new generation → miss again.
        let c2 = calls.clone();
        let _ = cache
            .lookup_or_compute(SearchRpc::Fulltext, "q", 5, None, None, None, || {
                let c2 = c2.clone();
                async move {
                    c2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(vec![])
                }
            })
            .await
            .unwrap();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "compute_fn should fire twice after bump"
        );
    }

    #[tokio::test]
    async fn compute_failure_not_cached() {
        use nomai_core::CoreError;
        let cache = mk_cache();
        let err: Result<Vec<serde_json::Value>, CoreError> =
            Err(CoreError::Storage(rusqlite::Error::ExecuteReturnedResults));
        let _ = cache
            .lookup_or_compute(SearchRpc::Fulltext, "q", 5, None, None, None, || async {
                err
            })
            .await;
        // Map empty: failure was not cached.
        assert_eq!(cache.len(), 0);
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn key_distinct_for_different_rpc() {
        let cache = mk_cache();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        for rpc in [SearchRpc::Semantic, SearchRpc::Fulltext] {
            let c = calls.clone();
            let _ = cache
                .lookup_or_compute(rpc, "same", 5, None, None, None, || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(vec![])
                    }
                })
                .await
                .unwrap();
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "different rpc → 2 distinct keys → 2 compute calls"
        );
    }

    #[tokio::test]
    async fn key_distinct_for_different_limit() {
        let cache = mk_cache();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        for limit in [5u32, 50u32] {
            let c = calls.clone();
            let _ = cache
                .lookup_or_compute(SearchRpc::Fulltext, "q", limit, None, None, None, || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(vec![])
                    }
                })
                .await
                .unwrap();
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "different limit → 2 distinct keys → 2 compute calls"
        );
    }

    #[tokio::test]
    async fn key_distinct_for_different_block_type() {
        let cache = mk_cache();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        for bt in [None, Some("note"), Some("claim")] {
            let c = calls.clone();
            let _ = cache
                .lookup_or_compute(SearchRpc::Fulltext, "q", 5, bt, None, None, || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(vec![])
                    }
                })
                .await
                .unwrap();
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "None vs Some(note) vs Some(claim) → 3 distinct keys"
        );
    }

    #[tokio::test]
    async fn lookup_or_compute_distinguishes_tag() {
        let cache = mk_cache();
        // Same query/limit/block_type, different tag → two misses (keys differ
        // by tag_hash, so the second lookup does not hit the first's entry).
        let a = cache
            .lookup_or_compute(
                SearchRpc::Fulltext,
                "rust",
                10,
                None,
                Some("alpha"),
                None,
                || async move { Ok(vec![json!({"v": "a"})]) },
            )
            .await
            .unwrap();
        let b = cache
            .lookup_or_compute(
                SearchRpc::Fulltext,
                "rust",
                10,
                None,
                Some("beta"),
                None,
                || async move { Ok(vec![json!({"v": "b"})]) },
            )
            .await
            .unwrap();
        assert_eq!(a[0]["v"], "a");
        assert_eq!(b[0]["v"], "b");
        // Both lookups should be misses (different tags don't cross-hit).
        let stats = cache.stats();
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn clear_empties_map_but_preserves_generation() {
        let cache = mk_cache();
        cache.bump_generation();
        let gen_before = cache.generation();
        // Populate.
        let _ = cache
            .lookup_or_compute(SearchRpc::Fulltext, "q", 5, None, None, None, || async {
                Ok(vec![json!(1)])
            })
            .await
            .unwrap();
        assert_eq!(cache.len(), 1);
        let cleared = cache.clear();
        assert_eq!(cleared, 1);
        assert_eq!(cache.len(), 0);
        assert_eq!(
            cache.generation(),
            gen_before,
            "clear does NOT bump generation"
        );
    }

    #[test]
    fn stats_reflects_hits_misses() {
        let cache = mk_cache();
        let stats = cache.stats();
        assert_eq!(stats.generation, 0);
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.semantic_hits, 0);
        assert_eq!(stats.semantic_misses, 0);
        assert_eq!(stats.fulltext_hits, 0);
        assert_eq!(stats.fulltext_misses, 0);
        assert_eq!(stats.hybrid_hits, 0);
        assert_eq!(stats.hybrid_misses, 0);
    }

    #[tokio::test]
    async fn hybrid_rpc_counts_separately() {
        let cache = mk_cache();
        let _ = cache
            .lookup_or_compute(SearchRpc::Hybrid, "q", 5, None, None, None, || async {
                Ok(vec![json!({"fusion_score": 0.03})])
            })
            .await
            .unwrap();
        let stats = cache.stats();
        assert_eq!(stats.hybrid_misses, 1);
        assert_eq!(stats.hybrid_hits, 0);
        assert_eq!(stats.semantic_misses, 0);
        assert_eq!(stats.fulltext_misses, 0);
    }
}
