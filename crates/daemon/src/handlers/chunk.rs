//! chunk.* handlers. chunk.create auto-embeds (same as entry.create).
//! chunk.delete explicitly removes vec_chunk_embeddings before deleting
//! the chunk row (vec0 virtual table doesn't support FK CASCADE).

use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{ChunkService, CoreError, CreateChunk};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;

pub async fn create(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    let input: CreateChunk = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    let chunks = daemon.chunks.clone();
    let chunk = blocking(move || chunks.create(input)).await??;

    // Auto-embed chunk text (same as entry.create pattern).
    let text = chunk.text.clone();
    let embeddings = daemon.embedder.embed(&[&text]).await?;
    if let Some(emb) = embeddings.into_iter().next() {
        let chunks = daemon.chunks.clone();
        let id = chunk.id;
        blocking(move || chunks.write_embedding(id, &emb)).await??;
    }

    serde_json::to_value(&chunk).map_err(|e| CoreError::Config(format!("serialize: {e}")))
}

pub async fn get(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    #[derive(Deserialize)]
    struct Params {
        id: ulid::Ulid,
    }
    let p: Params = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    let chunks = daemon.chunks.clone();
    let chunk = blocking(move || chunks.get(p.id)).await??;

    serde_json::to_value(&chunk).map_err(|e| CoreError::Config(format!("serialize: {e}")))
}

pub async fn delete(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    #[derive(Deserialize)]
    struct Params {
        id: ulid::Ulid,
    }
    let p: Params = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    // First remove vec_chunk_embeddings (vec0 doesn't support FK CASCADE).
    let chunks: std::sync::Arc<ChunkService> = daemon.chunks.clone();
    blocking(move || chunks.delete_embedding(p.id)).await??;
    // Then delete the chunk row (emits chunk.deleted event).
    let chunks = daemon.chunks.clone();
    blocking(move || chunks.delete(p.id)).await??;

    Ok(json!({ "deleted": true }))
}

pub async fn list(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    #[derive(Deserialize)]
    struct Params {
        entry_id: ulid::Ulid,
        #[serde(default = "default_limit")]
        limit: u32,
        #[serde(default)]
        offset: u32,
    }
    let p: Params = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    let chunks = daemon.chunks.clone();
    let result = blocking(move || chunks.list(p.entry_id, p.limit, p.offset)).await??;

    Ok(json!({
        "items": result.items,
        "total": result.total,
    }))
}

fn default_limit() -> u32 {
    100
}
