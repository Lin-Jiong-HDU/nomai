//! index.* handlers. Plan 5 introduces index-reconciliation RPCs that treat
//! the filesystem as source-of-truth (Spec §7.1) and bring the SQLite index
//! into agreement with it.
//!
//! `index.sync` walks the content store, diffs each entry's `.nomai` mtime
//! against the indexed `fs_mtime`, and reconciles via
//! `EntryService::reindex_one` / direct DELETE. Returns per-bucket counts.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use nomai_core::{CoreError, EntryService, SyncResult};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use nomai_protocol::method::index::SYNC as INDEX_SYNC;

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
        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}
