//! index.* handlers. Plan 5 introduces index-reconciliation RPCs that treat
//! the filesystem as source-of-truth (Spec §7.1) and bring the SQLite index
//! into agreement with it.
//!
//! `index.sync` walks the content store, diffs each entry's `.nomai` mtime
//! against the indexed `fs_mtime`, and reconciles via
//! `EntryService::reindex_one` / direct DELETE. Returns per-bucket counts.
//!
//! `index.rebuild` is the nuclear option: wipes every derived table, then
//! re-indexes every FS entry. Used to recover from index corruption.
//!
//! `index.verify` (Plan 6 Task 4) is a read-only drift report: same scan/diff
//! as `index.sync` but never mutates. Useful for surfacing drift to the user
//! before deciding whether to run sync/rebuild.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use nomai_core::{CoreError, EntryService, RebuildResult, SyncResult, VerifyResult};

use crate::daemon::Daemon;
use crate::handlers::embed::embed_entry_chunks;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use nomai_protocol::method::index::{
    REBUILD as INDEX_REBUILD, SYNC as INDEX_SYNC, VERIFY as INDEX_VERIFY,
};

pub struct Sync;

#[async_trait]
impl RpcHandler for Sync {
    fn method(&self) -> &'static str {
        INDEX_SYNC
    }
    fn description(&self) -> &'static str {
        "Reconcile the SQLite index with the filesystem source-of-truth. Walks every entry's .nomai file, diffing mtime; adds/updates/removes rows as needed. Returns per-bucket counts."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Clone the Arc before spawning so the closure is 'static. The sync
        // pass takes per-entry locks internally; we don't hold any lock here.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let result: SyncResult = { blocking(move || entries.sync_from_fs()).await?? };

        // Spec 7: only bump if FS drift was reconciled; no-op syncs are
        // common and bumping them wastes the cache.
        if result.added + result.updated + result.removed > 0 {
            daemon.search_cache.bump_generation();
        }

        // reindex_one (run for added/updated entries) clears their
        // vec_chunk_embeddings via CASCADE but does not re-embed — without
        // this, an entry that was previously searchable goes silent after a
        // sync, and newly-synced entries never become searchable (sibling of
        // issue #1). emb_cache keeps this near-zero API cost for unchanged
        // bodies. Mirrors entry.create: an embed failure surfaces as a
        // provider error without rolling back the already-committed reindex.
        for id in &result.reindexed_ids {
            embed_entry_chunks(daemon, *id, false).await?;
        }

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Rebuild;

#[async_trait]
impl RpcHandler for Rebuild {
    fn method(&self) -> &'static str {
        INDEX_REBUILD
    }
    fn description(&self) -> &'static str {
        "Wipe every derived table and re-index every entry from the filesystem. Use to recover from index corruption; heavier than index.sync."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Clone the Arc before spawning so the closure is 'static. The
        // rebuild takes per-entry locks internally during the reindex phase.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let mut result: RebuildResult = { blocking(move || entries.rebuild_index()).await?? };

        // Spec 7: rebuild always invalidates — even an empty FS rebuild
        // wipes the derived index, so cached results are stale by definition.
        daemon.search_cache.bump_generation();

        // reindex_one (run per entry by rebuild_index) re-derives chunks + FTS
        // but does NOT re-embed — rebuild_index phase 1 wiped
        // vec_chunk_embeddings, so semantic search would return empty without
        // this pass (issue #1). emb_cache (intentionally untouched by rebuild)
        // makes this near-zero API cost for unchanged bodies; only genuinely
        // new chunk texts hit the provider. Embed failures are collected into
        // `errors` (best-effort, mirroring rebuild_index's own handling) so
        // one bad entry doesn't abort the rest of the KB.
        let entry_ids = {
            let entries = daemon.entries.clone();
            blocking(move || entries.list_ids()).await??
        };
        for id in entry_ids {
            if let Err(e) = embed_entry_chunks(daemon, id, false).await {
                result.errors.push(format!("re-embed entry {id}: {e}"));
            }
        }

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Verify;

#[async_trait]
impl RpcHandler for Verify {
    fn method(&self) -> &'static str {
        INDEX_VERIFY
    }
    fn description(&self) -> &'static str {
        "Read-only drift report between the filesystem and the SQLite index. Same scan as index.sync but never mutates; use to preview drift before deciding to sync or rebuild."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Read-only drift report. verify_fs snapshots the index under one
        // short lock and then walks the FS without taking any lock; safe to
        // run on a live daemon.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let result: VerifyResult = { blocking(move || entries.verify_fs()).await?? };
        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params)
            .map_err(|errs| errs.map(|e| format!("{e}")).collect::<Vec<_>>())
    }

    #[test]
    fn sync_schema_accepts_empty_object() {
        let schema = Sync.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({})).is_ok());
    }

    #[test]
    fn sync_schema_rejects_extra_props() {
        let schema = Sync.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({"foo": 1})).is_err());
    }

    #[test]
    fn rebuild_schema_accepts_empty_object() {
        let schema = Rebuild.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({})).is_ok());
    }

    #[test]
    fn rebuild_schema_rejects_extra_props() {
        let schema = Rebuild.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({"foo": 1})).is_err());
    }

    #[test]
    fn verify_schema_accepts_empty_object() {
        let schema = Verify.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({})).is_ok());
    }

    #[test]
    fn verify_schema_rejects_extra_props() {
        let schema = Verify.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({"foo": 1})).is_err());
    }
}
