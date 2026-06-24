//! SearchCache: in-memory cache for search.semantic / search.fulltext
//! results. Spec 7. Generation-based invalidation; bump on every mutation
//! that affects search results. See `docs/superpowers/specs/2026-06-24-search-cache-design.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use serde_json::Value;

/// Which search RPC a cached result came from. Part of the cache key so
/// the same query string hitting both RPCs doesn't collide.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
#[allow(dead_code)] // constructed in Task 3 (search handler wiring)
pub(crate) enum SearchRpc {
    Semantic,
    Fulltext,
}

/// Cache key. `generation` is the snapshot of the daemon-wide counter at
/// lookup time; bumping the counter invalidates every prior key without
/// iterating the map. See spec §4.1.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
#[allow(dead_code)] // constructed in Task 2 (lookup_or_compute)
pub(crate) struct Key {
    pub(crate) generation: u64,
    pub(crate) rpc: SearchRpc,
    pub(crate) query_hash: [u8; 32],
    pub(crate) limit: u32,
    pub(crate) block_type_hash: Option<u64>,
}

/// Cached value. `Arc` so a hit path is one atomic increment (no deep
/// clone of the `Vec<Value>`).
#[allow(dead_code)] // constructed in Task 2 (lookup_or_compute)
pub(crate) type CachedResults = Arc<Vec<Value>>;

/// In-process search-results cache. Lives in the daemon; core is unaware.
pub struct SearchCache {
    #[allow(dead_code)] // written in Task 2 (lookup_or_compute)
    map: DashMap<Key, CachedResults>,
    #[allow(dead_code)] // read in Task 7 (cache.stats) / Tasks 4-6 (bump sites)
    generation: AtomicU64,
}

impl SearchCache {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            generation: AtomicU64::new(0),
        }
    }

    /// Current generation counter value. Spec §6 hook points call
    /// `bump_generation` to invalidate the cache.
    #[allow(dead_code)] // read in Task 7 (cache.stats) / Tasks 4-6 (bump sites)
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Atomically bump generation. Prior cached keys become unfindable
    /// (their embedded `generation` no longer matches any new lookup).
    #[allow(dead_code)] // called from mutation handlers in Tasks 4-6
    pub fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of entries currently held in the map (across all generations;
    /// old-generation entries linger until `clear`).
    #[allow(clippy::len_without_is_empty, dead_code)] // surfaced via cache.stats in Task 7
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Empty the map. Returns the count removed. Does NOT bump generation:
    /// after clear the map is empty, so the next lookup misses regardless
    /// of generation. Counters are intentionally not reset (matches
    /// emb_cache's `clear` semantics).
    #[allow(dead_code)] // called from cache.clear in Task 7
    pub fn clear(&self) -> u64 {
        let cleared = self.map.len() as u64;
        self.map.clear();
        cleared
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
}
