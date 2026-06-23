//! entry.* handlers. Embedding orchestration on create/update lives here
//! (not in core) so core remains sync.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, CreateEntry, EntryListQuery, UpdateEntry};
use nomai_providers::EmbeddingProvider;

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

/// Compute the derived body of an entry from its blocks. Mirrors the private
/// `derived_body_from_blocks` in `nomai_core::service`: blocks joined with
/// `\n\n` (paragraph break), in ordinal order. Used as the embedding input.
fn derived_body_from_blocks(blocks: &[nomai_core::Block]) -> String {
    blocks
        .iter()
        .map(|b| b.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
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

        // Trigger embedding if derived body is non-empty.
        let body = derived_body_from_blocks(&entry.blocks);
        if !body.is_empty() {
            let embeddings = daemon.cache.embed(&[&body]).await?;
            if let Some(emb) = embeddings.into_iter().next() {
                let entries = daemon.entries.clone();
                let id = entry.id;
                blocking(move || entries.write_embedding(id, &emb)).await??;
            }
        }

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

        // Snapshot current derived body to detect change. UpdateEntry no
        // longer carries a body field (blocks are immutable at this layer),
        // so only title/tags/attrs/source changes can occur — but we still
        // re-embed if the derived body differs for any reason (e.g. caller
        // mutated blocks out-of-band).
        let entries = daemon.entries.clone();
        let id_for_get = p.id;
        let old_body =
            derived_body_from_blocks(&blocking(move || entries.get(id_for_get)).await??.blocks);

        let entries = daemon.entries.clone();
        let id_for_update = p.id;
        let fields = p.fields;
        let updated = blocking(move || entries.update(id_for_update, fields)).await??;

        let new_body = derived_body_from_blocks(&updated.blocks);

        // Re-embed if derived body changed.
        if new_body != old_body {
            if new_body.is_empty() {
                // body cleared — remove stale embedding so searches no longer match.
                let entries = daemon.entries.clone();
                let id = updated.id;
                blocking(move || entries.delete_embedding(id)).await??;
            } else {
                // body changed (non-empty) — re-embed.
                let body = new_body;
                let embeddings = daemon.cache.embed(&[&body]).await?;
                if let Some(emb) = embeddings.into_iter().next() {
                    let entries = daemon.entries.clone();
                    let id = updated.id;
                    blocking(move || entries.write_embedding(id, &emb)).await??;
                }
            }
        }

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

        // Cleanup chunk embeddings before entry deletion (spec §11 方案 D).
        // vec_chunk_embeddings is a vec0 virtual table without FK CASCADE —
        // if we don't clean up here, deleting the entry CASCADE-deletes the
        // chunk rows but leaves orphan vectors in vec_chunk_embeddings.
        //
        // Use a large limit (u32::MAX) as "no effective ceiling" — SQLite
        // handles it fine and real-world single entries don't approach it.
        let chunks = daemon.chunks.clone();
        let id_for_list = p.id;
        let chunk_ids: Vec<ulid::Ulid> = blocking(move || {
            let result = chunks.list(id_for_list, u32::MAX, 0)?;
            Ok(result.items.into_iter().map(|c| c.id).collect::<Vec<_>>())
        })
        .await??;

        for cid in chunk_ids {
            let chunks = daemon.chunks.clone();
            blocking(move || chunks.delete_embedding(cid)).await??;
        }

        // Now delete the entry — FK CASCADE will remove chunk rows automatically.
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
