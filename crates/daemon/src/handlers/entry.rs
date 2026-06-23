//! entry.* handlers. Embedding orchestration on create/update lives here
//! (not in core) so core remains sync.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, CreateEntry, EntryListQuery, UpdateEntry};

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

/// Wrap a sync closure as a spawn_blocking task, mapping JoinError to CoreError.
///
/// Returns a nested `Result<Result<T, CoreError>, CoreError>` so callers can use
/// `??` to flatten both the join-error layer and the inner core-error layer.
pub(crate) async fn blocking<F, T>(f: F) -> Result<Result<T, CoreError>, CoreError>
where
    F: FnOnce() -> Result<T, CoreError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| CoreError::Config(format!("blocking task join error: {e}")))
}

pub struct Create;
#[async_trait]
impl RpcHandler for Create {
    fn method(&self) -> &'static str {
        "entry.create"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let input: CreateEntry = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let entry = blocking(move || entries.create(input)).await??;

        // Plan 4: entry-level embeddings are retired; chunk-level embeddings
        // are managed via BlockService::create_in_tx + a separate background
        // embedder. For v1, entry.create no longer triggers embedding work.

        serde_json::to_value(&entry).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Get;
#[async_trait]
impl RpcHandler for Get {
    fn method(&self) -> &'static str {
        "entry.get"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let entry = blocking(move || entries.get(p.id)).await??;

        serde_json::to_value(&entry).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Update;
#[async_trait]
impl RpcHandler for Update {
    fn method(&self) -> &'static str {
        "entry.update"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
            #[serde(flatten)]
            fields: UpdateEntry,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let id_for_update = p.id;
        let fields = p.fields;
        let updated = blocking(move || entries.update(id_for_update, fields)).await??;

        // Plan 4: entry.update touches metadata only; FTS is per-block and
        // updated automatically when blocks change. No embedding re-trigger
        // is needed at this layer.

        serde_json::to_value(&updated).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Delete;
#[async_trait]
impl RpcHandler for Delete {
    fn method(&self) -> &'static str {
        "entry.delete"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        // Plan 4: chunks are block-addressed with CASCADE on block_id; deleting
        // the entry CASCADEs blocks → chunks. Chunk embeddings live in
        // vec_chunk_embeddings (vec0 has no FK CASCADE), so we clean them up
        // explicitly here. Plan 5 may move this to a TRIGGER + retry job.
        let entries = daemon.entries.clone();
        let id_for_get = p.id;
        let blocks = blocking(move || {
            let entries = entries;
            let entry = entries.get(id_for_get)?;
            Ok(entry.blocks)
        })
        .await??;

        let chunks = daemon.chunks.clone();
        for block in blocks {
            let chunks = chunks.clone();
            let block_id = block.id;
            let chunk_ids: Vec<ulid::Ulid> = blocking(move || {
                let result = chunks.list(block_id)?;
                Ok(result.items.into_iter().map(|c| c.id).collect::<Vec<_>>())
            })
            .await??;
            for cid in chunk_ids {
                let chunks = daemon.chunks.clone();
                blocking(move || chunks.delete_embedding(cid)).await??;
            }
        }

        // Now delete the entry — FK CASCADE removes blocks + chunks.
        let entries = daemon.entries.clone();
        blocking(move || entries.delete(p.id)).await??;
        Ok(json!({ "deleted": true }))
    }
}

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "entry.list"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: EntryListQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let result = blocking(move || entries.list(query)).await??;

        Ok(json!({
            "items": result.items,
            "total": result.total,
        }))
    }
}
