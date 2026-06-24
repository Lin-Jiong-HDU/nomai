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

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Rebuild;

#[async_trait]
impl RpcHandler for Rebuild {
    fn method(&self) -> &'static str {
        INDEX_REBUILD
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Clone the Arc before spawning so the closure is 'static. The
        // rebuild takes per-entry locks internally during the reindex phase.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let result: RebuildResult = { blocking(move || entries.rebuild_index()).await?? };

        // Spec 7: rebuild always invalidates — even an empty FS rebuild
        // wipes the derived index, so cached results are stale by definition.
        daemon.search_cache.bump_generation();

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Verify;

#[async_trait]
impl RpcHandler for Verify {
    fn method(&self) -> &'static str {
        INDEX_VERIFY
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
