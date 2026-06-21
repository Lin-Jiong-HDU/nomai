//! entry.* handlers. Embedding orchestration on create/update lives here
//! (not in core) so core remains sync.

use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, CreateEntry, EntryListQuery, UpdateEntry};

use crate::daemon::Daemon;

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

pub async fn create(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    let input: CreateEntry = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    let entries = daemon.entries.clone();
    let entry = blocking(move || entries.create(input)).await??;

    // Trigger embedding if body is non-empty.
    if !entry.body.is_empty() {
        let body = entry.body.clone();
        let embeddings = daemon.embedder.embed(&[&body]).await?;
        if let Some(emb) = embeddings.into_iter().next() {
            let entries = daemon.entries.clone();
            let id = entry.id;
            blocking(move || entries.write_embedding(id, &emb)).await??;
        }
    }

    serde_json::to_value(&entry).map_err(|e| CoreError::Config(format!("serialize: {e}")))
}

pub async fn get(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
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

pub async fn update(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    #[derive(Deserialize)]
    struct Params {
        id: ulid::Ulid,
        #[serde(flatten)]
        fields: UpdateEntry,
    }
    let p: Params = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    // Snapshot current body to detect change.
    let entries = daemon.entries.clone();
    let id_for_get = p.id;
    let old_body = blocking(move || entries.get(id_for_get)).await??.body;

    let entries = daemon.entries.clone();
    let id_for_update = p.id;
    let fields = p.fields;
    let updated = blocking(move || entries.update(id_for_update, fields)).await??;

    // Re-embed if body changed.
    if updated.body != old_body {
        if updated.body.is_empty() {
            // body cleared — remove stale embedding so searches no longer match.
            let entries = daemon.entries.clone();
            let id = updated.id;
            blocking(move || entries.delete_embedding(id)).await??;
        } else {
            // body changed (non-empty) — re-embed.
            let body = updated.body.clone();
            let embeddings = daemon.embedder.embed(&[&body]).await?;
            if let Some(emb) = embeddings.into_iter().next() {
                let entries = daemon.entries.clone();
                let id = updated.id;
                blocking(move || entries.write_embedding(id, &emb)).await??;
            }
        }
    }

    serde_json::to_value(&updated).map_err(|e| CoreError::Config(format!("serialize: {e}")))
}

pub async fn delete(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
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

pub async fn list(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    let query: EntryListQuery = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    let entries = daemon.entries.clone();
    let result = blocking(move || entries.list(query)).await??;

    Ok(json!({
        "items": result.items,
        "total": result.total,
    }))
}
