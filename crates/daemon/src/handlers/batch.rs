//! batch RPC handler: single-request multi-op with atomic transaction.
//!
//! See spec 2026-06-22-batch-rpc-design.md §3-§6.

use std::collections::HashMap;

use async_trait::async_trait;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{
    ChunkService, CoreError, CreateChunk, CreateEntry, CreateLink, EntryService, LinkService,
    UpdateEntry,
};
use nomai_providers::EmbeddingProvider;

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

/// A single operation within a batch request.
#[derive(Debug, Deserialize)]
pub struct BatchOp {
    /// Optional identifier for $ref referencing. If absent, result can't be referenced.
    #[serde(default)]
    pub id: Option<String>,
    /// RPC method name (must be a mutation: entry/chunk/link create/delete/update).
    pub method: String,
    /// Parameters for the method. May contain $ref placeholders.
    pub params: Value,
}

/// Batch request.
#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    pub ops: Vec<BatchOp>,
    /// When true (default), any op failure rolls back the whole transaction.
    /// Task 1 always treats the batch as atomic; non-atomic mode is a future
    /// addition and the field is parsed but not yet branched on.
    #[serde(default = "default_atomic")]
    #[allow(dead_code)]
    pub atomic: bool,
}

fn default_atomic() -> bool {
    true
}

/// Allowed methods in batch (mutation only).
const ALLOWED_METHODS: &[&str] = &[
    "entry.create",
    "entry.update",
    "entry.delete",
    "chunk.create",
    "chunk.delete",
    "link.create",
    "link.delete",
    "events.purge",
];

pub struct Batch;

#[async_trait]
impl RpcHandler for Batch {
    fn method(&self) -> &'static str {
        "batch"
    }

    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let req: BatchRequest = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid batch params: {e}")))?;

        if req.ops.is_empty() {
            return Err(CoreError::Validation(
                "batch requires at least one op".into(),
            ));
        }
        if req.ops.len() > 1000 {
            return Err(CoreError::Validation("batch exceeds 1000 ops limit".into()));
        }

        // Phase 1: transactional dispatch. All connection access is confined to
        // this block so the MutexGuard is dropped before any subsequent await.
        // Returns (results, embed_queue, commit_outcome).
        //
        // commit_outcome: Ok = COMMIT succeeded; OpErr = ROLLBACK with the
        // failing op's (idx, err); CommitErr = COMMIT itself failed (returned
        // verbatim as a Storage error, matching the single-op path).
        enum CommitOutcome {
            Ok,
            OpErr(usize, CoreError),
            CommitErr(CoreError),
        }

        let (results, embed_queue, commit_outcome): (Vec<Value>, Vec<EmbedTask>, CommitOutcome) = {
            let conn_arc = daemon.entries.conn_for_test();
            let conn = conn_arc.lock().unwrap();

            // Build id → result index map for $ref resolution
            let mut id_to_index: HashMap<String, usize> = HashMap::new();

            let mut results: Vec<Value> = Vec::with_capacity(req.ops.len());
            let mut embed_queue: Vec<EmbedTask> = Vec::new();
            let mut failed_at: Option<(usize, CoreError)> = None;

            conn.execute_batch("BEGIN").map_err(CoreError::Storage)?;

            for (i, op) in req.ops.iter().enumerate() {
                if failed_at.is_some() {
                    // atomic=true: subsequent ops are skipped
                    results.push(json!({
                        "ok": false,
                        "error": {
                            "code": -32603,
                            "message": "skipped due to earlier op failure"
                        }
                    }));
                    continue;
                }

                // Check method is allowed
                if !ALLOWED_METHODS.contains(&op.method.as_str()) {
                    let err = CoreError::Validation(format!(
                        "method '{}' not allowed in batch (mutation only)",
                        op.method
                    ));
                    results.push(json!({
                        "ok": false,
                        "error": error_to_rpc(&err)
                    }));
                    failed_at = Some((i, err));
                    continue;
                }

                // Resolve $ref in params
                let resolved_params = match resolve_refs(&op.params, &results, &id_to_index) {
                    Ok(p) => p,
                    Err(e) => {
                        results.push(json!({
                            "ok": false,
                            "error": error_to_rpc(&e)
                        }));
                        failed_at = Some((i, e));
                        continue;
                    }
                };

                // Dispatch to service _in_tx
                let outcome = dispatch_in_tx(
                    &conn,
                    &op.method,
                    resolved_params,
                    &daemon.entries,
                    &daemon.links,
                    &daemon.chunks,
                );

                match outcome {
                    Ok(value) => {
                        // Track embed targets
                        if op.method == "entry.create" || op.method == "entry.update" {
                            if let Some(id_str) = value.get("id").and_then(|v| v.as_str()) {
                                if let Ok(id) = id_str.parse::<ulid::Ulid>() {
                                    // Derived body = blocks' text joined with
                                    // "\n\n" (mirrors core's
                                    // `derived_body_from_blocks`). Empty when
                                    // the entry has no blocks.
                                    let text = derived_body_from_value(&value);
                                    if !text.is_empty() {
                                        embed_queue.push(EmbedTask {
                                            id,
                                            text,
                                            target: EmbedTarget::Entry,
                                        });
                                    }
                                }
                            }
                        } else if op.method == "chunk.create" {
                            if let Some(id_str) = value.get("id").and_then(|v| v.as_str()) {
                                if let Ok(id) = id_str.parse::<ulid::Ulid>() {
                                    if let Some(text) = value.get("text").and_then(|v| v.as_str()) {
                                        embed_queue.push(EmbedTask {
                                            id,
                                            text: text.to_string(),
                                            target: EmbedTarget::Chunk,
                                        });
                                    }
                                }
                            }
                        }

                        results.push(json!({"ok": true, "result": value}));

                        // Register id for $ref
                        if let Some(ref id) = op.id {
                            id_to_index.insert(id.clone(), i);
                        }
                    }
                    Err(e) => {
                        results.push(json!({
                            "ok": false,
                            "error": error_to_rpc(&e)
                        }));
                        failed_at = Some((i, e));
                    }
                }
            }

            // Commit or rollback (synchronous, before guard drops)
            let commit_outcome = match failed_at {
                None => match conn.execute_batch("COMMIT") {
                    Ok(()) => CommitOutcome::Ok,
                    Err(e) => CommitOutcome::CommitErr(CoreError::Storage(e)),
                },
                Some((idx, err)) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    CommitOutcome::OpErr(idx, err)
                }
            };

            (results, embed_queue, commit_outcome)
            // conn + conn_arc drop here, releasing the Mutex.
        };

        match commit_outcome {
            CommitOutcome::Ok => {
                // Phase 2: batch embed (all texts in one API call)
                run_embed_queue(daemon, embed_queue).await?;

                Ok(json!({
                    "results": results,
                    "rolled_back": false
                }))
            }
            CommitOutcome::CommitErr(e) => Err(e),
            CommitOutcome::OpErr(_idx, err) => {
                // Return the underlying CoreError directly so the top-level
                // JSON-RPC error code matches (NotFound→1001, Validation→1003, etc).
                // Per-op context (index, method, message) is in the results array.
                Err(err)
            }
        }
    }
}

/// Compute the derived body from an entry-shaped JSON value (the result of
/// `entry.create` / `entry.update` inside a batch). Mirrors core's private
/// `derived_body_from_blocks`: blocks' text fields joined with `\n\n` in
/// array order. Returns an empty string when there are no blocks or the
/// value isn't entry-shaped.
fn derived_body_from_value(value: &Value) -> String {
    value
        .get("blocks")
        .and_then(|v| v.as_array())
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("\n\n")
        })
        .unwrap_or_default()
}

/// Resolve $ref placeholders in params, using results from previous ops.
///
/// `{"$ref": "op_id"}` → entire result of that op
/// `{"$ref": "op_id.field"}` → nested field access via dot notation
pub fn resolve_refs(
    params: &Value,
    results: &[Value],
    id_to_index: &HashMap<String, usize>,
) -> Result<Value, CoreError> {
    match params {
        Value::Object(map) if map.len() == 1 && map.contains_key("$ref") => {
            let ref_path = map["$ref"]
                .as_str()
                .ok_or_else(|| CoreError::Validation("$ref value must be a string".into()))?;
            resolve_ref_path(ref_path, results, id_to_index)
        }
        Value::Object(map) => {
            let mut resolved_map = serde_json::Map::new();
            for (k, v) in map {
                resolved_map.insert(k.clone(), resolve_refs(v, results, id_to_index)?);
            }
            Ok(Value::Object(resolved_map))
        }
        Value::Array(arr) => {
            let mut resolved_arr = Vec::with_capacity(arr.len());
            for v in arr {
                resolved_arr.push(resolve_refs(v, results, id_to_index)?);
            }
            Ok(Value::Array(resolved_arr))
        }
        _ => Ok(params.clone()),
    }
}

fn resolve_ref_path(
    path: &str,
    results: &[Value],
    id_to_index: &HashMap<String, usize>,
) -> Result<Value, CoreError> {
    let parts: Vec<&str> = path.splitn(2, '.').collect();
    let op_id = parts[0];

    let idx = *id_to_index
        .get(op_id)
        .ok_or_else(|| CoreError::Validation(format!("$ref: unknown op_id '{}'", op_id)))?;

    // results[idx] is {"ok": true, "result": {...}}
    let result_wrapper = &results[idx];
    let result = result_wrapper.get("result").ok_or_else(|| {
        CoreError::Validation(format!("$ref: op '{}' has no result (failed?)", op_id))
    })?;

    if parts.len() == 1 {
        Ok(result.clone())
    } else {
        let field_path = parts[1];
        let mut current = result;
        for field in field_path.split('.') {
            current = current.get(field).ok_or_else(|| {
                CoreError::Validation(format!(
                    "$ref: field '{}' not found in path '{}'",
                    field, field_path
                ))
            })?;
        }
        Ok(current.clone())
    }
}

/// Dispatch a single op to the appropriate service _in_tx method.
fn dispatch_in_tx(
    conn: &Connection,
    method: &str,
    params: Value,
    entries: &EntryService,
    links: &LinkService,
    chunks: &ChunkService,
) -> Result<Value, CoreError> {
    match method {
        "entry.create" => {
            let p: CreateEntry = serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
            let entry = entries.create_in_tx(conn, p)?;
            serde_json::to_value(&entry).map_err(|e| CoreError::Config(format!("serialize: {e}")))
        }
        "entry.update" => {
            #[derive(Deserialize)]
            struct UpdateParams {
                id: ulid::Ulid,
                #[serde(flatten)]
                fields: UpdateEntry,
            }
            let p: UpdateParams = serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
            let entry = entries.update_in_tx(conn, p.id, p.fields)?;
            serde_json::to_value(&entry).map_err(|e| CoreError::Config(format!("serialize: {e}")))
        }
        "entry.delete" => {
            #[derive(Deserialize)]
            struct IdParams {
                id: ulid::Ulid,
            }
            let p: IdParams = serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
            entries.delete_in_tx(conn, p.id)?;
            Ok(json!({"deleted": true}))
        }
        "chunk.create" => {
            let p: CreateChunk = serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
            let chunk = chunks.create_in_tx(conn, p)?;
            serde_json::to_value(&chunk).map_err(|e| CoreError::Config(format!("serialize: {e}")))
        }
        "chunk.delete" => {
            #[derive(Deserialize)]
            struct IdParams {
                id: ulid::Ulid,
            }
            let p: IdParams = serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
            chunks.delete_in_tx(conn, p.id)?;
            Ok(json!({"deleted": true}))
        }
        "link.create" => {
            let p: CreateLink = serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
            let link = links.create_in_tx(conn, p)?;
            serde_json::to_value(&link).map_err(|e| CoreError::Config(format!("serialize: {e}")))
        }
        "link.delete" => {
            #[derive(Deserialize)]
            struct IdParams {
                id: ulid::Ulid,
            }
            let p: IdParams = serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
            links.delete_in_tx(conn, p.id)?;
            Ok(json!({"deleted": true}))
        }
        "events.purge" => {
            // events.purge is special — it DELETEs from events table directly.
            // For batch, inline the SQL (or call EventService if it has _in_tx).
            // MVP: defer events.purge in batch (return Validation).
            Err(CoreError::Validation(
                "events.purge not yet supported in batch".into(),
            ))
        }
        _ => Err(CoreError::Validation(format!(
            "method '{}' not allowed in batch",
            method
        ))),
    }
}

/// Batch embed all queued texts in a single API call, then write each embedding.
///
/// Called after COMMIT (Mutex released). Collects all entry/chunk texts, calls
/// `embedder.embed` once with the full array (EmbeddingProvider trait supports
/// batch), then writes each embedding via the appropriate service. Embed
/// failure bubbles up as `CoreError::Provider` — same weak-consistency model
/// as single-op path (entries persisted, vec missing).
async fn run_embed_queue(daemon: &Daemon, queue: Vec<EmbedTask>) -> Result<(), CoreError> {
    if queue.is_empty() {
        return Ok(());
    }

    let texts: Vec<&str> = queue.iter().map(|t| t.text.as_str()).collect();
    let embeddings = daemon.cache.embed(&texts).await?;

    for (task, emb) in queue.into_iter().zip(embeddings) {
        match task.target {
            EmbedTarget::Entry => {
                let entries = daemon.entries.clone();
                let id = task.id;
                let emb = emb.clone();
                crate::handlers::entry::blocking(move || entries.write_embedding(id, &emb))
                    .await??;
            }
            EmbedTarget::Chunk => {
                let chunks = daemon.chunks.clone();
                let id = task.id;
                let emb = emb.clone();
                crate::handlers::entry::blocking(move || chunks.write_embedding(id, &emb))
                    .await??;
            }
        }
    }
    Ok(())
}

/// EmbedTask: queued for post-commit batch embedding.
pub(crate) struct EmbedTask {
    pub id: ulid::Ulid,
    pub text: String,
    pub target: EmbedTarget,
}

pub(crate) enum EmbedTarget {
    Entry,
    Chunk,
}

/// Convert CoreError to a JSON-RPC error object (for results array entries).
///
/// Mirrors `crate::rpc::core_error_to_rpc` but takes a reference (CoreError
/// does not implement Clone). Keep the code/message mapping in sync with
/// `core_error_to_rpc` in `crate::rpc`.
fn error_to_rpc(err: &CoreError) -> Value {
    use nomai_protocol::error::{
        CONFIG_ERROR, ENTRY_NOT_FOUND, FS_ERROR, INTERNAL_ERROR, NOMAI_FORMAT_ERROR,
        PROVIDER_ERROR, VALIDATION_ERROR,
    };
    let (code, message) = match err {
        CoreError::NotFound(_) => (ENTRY_NOT_FOUND, "entry not found".to_string()),
        CoreError::Validation(msg) => (VALIDATION_ERROR, msg.clone()),
        CoreError::Provider(p) => (PROVIDER_ERROR, p.message.clone()),
        CoreError::Config(msg) => (CONFIG_ERROR, msg.clone()),
        CoreError::Io(e) => (FS_ERROR, format!("io error: {e}")),
        CoreError::NomaiFormat(pe) => (NOMAI_FORMAT_ERROR, format!("nomai format error: {pe}")),
        CoreError::Storage(e) => (INTERNAL_ERROR, format!("storage error: {e}")),
        CoreError::Migration(msg) => (INTERNAL_ERROR, format!("migration error: {msg}")),
    };
    json!({
        "code": code,
        "message": message,
    })
}
