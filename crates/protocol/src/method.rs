//! JSON-RPC method-name constants, grouped by namespace.
//!
//! Reserved method names (`search.hybrid`, `provider.set`) are present so
//! the daemon can match them and return `METHOD_NOT_FOUND` (`-32601`)
//! cleanly.

pub mod entry {
    pub const CREATE: &str = "entry.create";
    pub const GET: &str = "entry.get";
    pub const UPDATE: &str = "entry.update";
    pub const DELETE: &str = "entry.delete";
    pub const LIST: &str = "entry.list";
}

pub mod entries {
    /// Purge transient (short-term) entries.
    pub const PURGE_TRANSIENT: &str = "entries.purge_transient";
}

pub mod search {
    pub const FULLTEXT: &str = "search.fulltext";
    pub const SEMANTIC: &str = "search.semantic";
    pub const HYBRID: &str = "search.hybrid";
}

pub mod provider {
    pub const LIST: &str = "provider.list";
    /// Reserved: returns `METHOD_NOT_FOUND` (-32601) in MVP.
    pub const SET: &str = "provider.set";
}

pub mod link {
    pub const CREATE: &str = "link.create";
    pub const GET: &str = "link.get";
    pub const DELETE: &str = "link.delete";
    pub const LIST: &str = "link.list";
    pub const NEIGHBORS: &str = "link.neighbors";
    /// Reserved for Phase 2: returns `METHOD_NOT_FOUND` (-32601).
    pub const TRAVERSE: &str = "link.traverse";
}

pub mod events {
    pub const LIST: &str = "events.list";
    pub const GET: &str = "events.get";
    pub const PURGE: &str = "events.purge";
}

pub mod chunk {
    /// Reserved: chunks are auto-derived from blocks.
    /// Returns `METHOD_NOT_FOUND` (-32601) if dispatched. Constant
    /// retained for symmetry with GET/LIST and future re-enable.
    pub const CREATE: &str = "chunk.create";
    pub const GET: &str = "chunk.get";
    /// Reserved: see CREATE. Chunks are immutable; deleted when the
    /// parent block is deleted (CASCADE + V9 trigger).
    pub const DELETE: &str = "chunk.delete";
    pub const LIST: &str = "chunk.list";
}

pub mod block {
    /// Append a block to an entry. Computes ordinal = max(existing)+1
    /// and re-renders the entry's `.nomai` file.
    pub const APPEND: &str = "block.append";
    /// 0.2.2: fetch a single block by ULID. Namespace completeness —
    /// entry/link/chunk/events all have `get`. Returns 1001 if not found.
    pub const GET: &str = "block.get";
    /// Update a block's type/text/attrs. Re-chunks when text changes
    /// (chunks_ad trigger cleans vec_chunk_embeddings) and re-renders the
    /// entry's `.nomai` file.
    pub const UPDATE: &str = "block.update";
    /// Delete a block. The chunks_ad trigger cleans vec_chunk_embeddings
    /// and re-renders the entry's `.nomai` file.
    pub const DELETE: &str = "block.delete";
    /// List blocks for an entry by entry_id.
    /// Fills the namespace gap (entry/link/chunk/events all have list).
    pub const LIST: &str = "block.list";
}

pub mod index {
    /// Reconcile the SQLite index against the filesystem. Adds
    /// new entries discovered on disk, re-indexes those whose `.nomai`
    /// mtime changed, and removes index rows whose `.nomai` is gone.
    /// Returns `{ added, updated, removed, unchanged }` counts.
    pub const SYNC: &str = "index.sync";
    /// Wholesale rebuild. DELETEs every derived table (chunks,
    /// blocks, links, entries, fts_blocks, vec_chunk_embeddings) then
    /// re-indexes every FS entry. Does NOT touch events (daemon history)
    /// or emb_cache (deterministic, reusable). Returns
    /// `{ reindexed, errors }` where `errors` collects per-entry failures.
    pub const REBUILD: &str = "index.rebuild";
    /// Read-only drift report between FS and the SQLite index.
    /// Mirrors `index.sync`'s scan/diff but does NOT mutate. Returns
    /// `{ fs_only, db_only, stale_mtime, consistent }` so callers can
    /// surface drift before deciding whether to run `sync` / `rebuild`.
    pub const VERIFY: &str = "index.verify";
}

pub mod system {
    /// Walk every entry row and render `.nomai` for any that lacks
    /// one. Migration utility — entries created via
    /// `entry.create` already have `.nomai` and are skipped; this is for
    /// rows created via direct DB manipulation. Returns
    /// `{ exported, skipped, errors }`.
    pub const EXPORT_TO_FS: &str = "system.export_to_fs";
    /// Rebuild the resident daemon's internal state (sqlite/embedder/
    /// llm/cache) in-process without dropping client connections. Use when
    /// embedding calls (search.semantic / ingest) start failing due to long-
    /// uptime state decay. Returns `{ ok: true }`. No params, no events.
    pub const RESTART: &str = "system.restart";
}

pub mod benchmark {
    pub const START: &str = "benchmark.start";
    pub const NEXT_CASE: &str = "benchmark.next_case";
    pub const RECORD_ANSWER: &str = "benchmark.record_answer";
    pub const FINISH: &str = "benchmark.finish";
    pub const ABORT: &str = "benchmark.abort";
    pub const STATUS: &str = "benchmark.status";
}

pub mod cache {
    /// emb_cache introspection (model, rows, hits/misses, warning).
    pub const STATS: &str = "cache.stats";
    /// Clear cache by namespace. Default namespace
    /// `"embeddings"` for backward compat; `"searches"` / `"all"` opts
    /// into the search cache.
    pub const CLEAR: &str = "cache.clear";
}

pub mod attachment {
    /// Read a sibling attachment file as base64. Returns
    /// `{filename, mime, base64}`. MIME inferred from extension.
    pub const READ: &str = "attachment.read";
    /// List sibling attachment files for an entry (excludes `entry.nomai`).
    /// Returns `{items: [{filename, size, modified}]}`.
    pub const LIST: &str = "attachment.list";
}

pub mod sync {
    /// Initialize the knowledge_root as a git repository for multi-device
    /// sync: `git init` + remote + LFS install + `.gitignore` / `.gitattributes`
    /// + initial commit. Idempotent-rejects if `.git` already exists.
    pub const INIT: &str = "sync.init";
    /// Pull/push the sync remote, rebasing local entry
    /// mutations on top of incoming commits.
    pub const RUN: &str = "sync.run";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_namespace_methods() {
        assert_eq!(entry::CREATE, "entry.create");
        assert_eq!(entry::GET, "entry.get");
        assert_eq!(entry::UPDATE, "entry.update");
        assert_eq!(entry::DELETE, "entry.delete");
        assert_eq!(entry::LIST, "entry.list");
    }

    #[test]
    fn search_namespace_methods() {
        assert_eq!(search::FULLTEXT, "search.fulltext");
        assert_eq!(search::SEMANTIC, "search.semantic");
        assert_eq!(search::HYBRID, "search.hybrid");
    }

    #[test]
    fn provider_namespace_methods() {
        assert_eq!(provider::LIST, "provider.list");
        assert_eq!(provider::SET, "provider.set");
    }

    #[test]
    fn link_namespace_methods() {
        assert_eq!(link::CREATE, "link.create");
        assert_eq!(link::GET, "link.get");
        assert_eq!(link::DELETE, "link.delete");
        assert_eq!(link::LIST, "link.list");
        assert_eq!(link::NEIGHBORS, "link.neighbors");
    }

    #[test]
    fn events_namespace_methods() {
        assert_eq!(events::LIST, "events.list");
        assert_eq!(events::GET, "events.get");
        assert_eq!(events::PURGE, "events.purge");
    }

    #[test]
    fn chunk_namespace_methods() {
        assert_eq!(chunk::CREATE, "chunk.create");
        assert_eq!(chunk::GET, "chunk.get");
        assert_eq!(chunk::DELETE, "chunk.delete");
        assert_eq!(chunk::LIST, "chunk.list");
    }

    #[test]
    fn block_namespace_methods() {
        assert_eq!(block::APPEND, "block.append");
        assert_eq!(block::GET, "block.get");
        assert_eq!(block::UPDATE, "block.update");
        assert_eq!(block::DELETE, "block.delete");
        assert_eq!(block::LIST, "block.list");
    }

    #[test]
    fn index_namespace_methods() {
        assert_eq!(index::SYNC, "index.sync");
        assert_eq!(index::REBUILD, "index.rebuild");
        assert_eq!(index::VERIFY, "index.verify");
    }

    #[test]
    fn system_namespace_methods() {
        assert_eq!(system::EXPORT_TO_FS, "system.export_to_fs");
        assert_eq!(system::RESTART, "system.restart");
    }

    #[test]
    fn benchmark_namespace_methods() {
        assert_eq!(benchmark::START, "benchmark.start");
        assert_eq!(benchmark::NEXT_CASE, "benchmark.next_case");
        assert_eq!(benchmark::RECORD_ANSWER, "benchmark.record_answer");
        assert_eq!(benchmark::FINISH, "benchmark.finish");
        assert_eq!(benchmark::ABORT, "benchmark.abort");
        assert_eq!(benchmark::STATUS, "benchmark.status");
    }

    #[test]
    fn cache_namespace_methods() {
        assert_eq!(cache::STATS, "cache.stats");
        assert_eq!(cache::CLEAR, "cache.clear");
    }

    #[test]
    fn attachment_namespace_methods() {
        assert_eq!(attachment::READ, "attachment.read");
        assert_eq!(attachment::LIST, "attachment.list");
    }

    #[test]
    fn sync_namespace_methods() {
        assert_eq!(sync::INIT, "sync.init");
        assert_eq!(sync::RUN, "sync.run");
    }
}
