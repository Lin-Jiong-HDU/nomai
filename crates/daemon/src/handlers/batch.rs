//! batch RPC handler: single-request multi-op with atomic transaction.

use std::collections::HashMap;

use async_trait::async_trait;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{
    ChunkService, CoreError, CreateEntry, CreateLink, EntryService, LinkService, UpdateEntry,
};
#[allow(unused_imports)]
use nomai_providers::EmbeddingProvider;

use crate::daemon::Daemon;
use crate::rpc::{RpcHandler, core_error_to_rpc_ref};

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
    /// The batch is always treated as atomic; non-atomic mode is a future
    /// addition and the field is parsed but not yet branched on.
    #[serde(default = "default_atomic")]
    #[allow(dead_code)]
    pub atomic: bool,
}

fn default_atomic() -> bool {
    true
}

/// Allowed methods in batch (mutation only). chunk.create/delete removed
/// (chunks are auto-derived).
const ALLOWED_METHODS: &[&str] = &[
    "entry.create",
    "entry.update",
    "entry.delete",
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

    fn is_mutating(&self) -> bool {
        // Every batch op mutates the file tree (entry/link create/update/delete
        // → entry writes via triggers). Mark mutating so the dispatcher holds
        // sync_lock for the whole atomic transaction.
        true
    }

    fn description(&self) -> &'static str {
        "Execute multiple mutation ops (entry.create/update/delete, link.create/delete) in a single atomic transaction. Ops may reference each other via {\"$ref\": \"op_id\"} or {\"$ref\": \"op_id.field\"}. Atomic mode only (any failure rolls back the whole batch). See docs/reference.md for full semantics."
    }

    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "ops": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 1000,
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string", "description": "optional handle for $ref referencing"},
                            "method": {
                                "type": "string",
                                "enum": [
                                    "entry.create",
                                    "entry.update",
                                    "entry.delete",
                                    "link.create",
                                    "link.delete",
                                    "events.purge"
                                ]
                            },
                            "params": {
                                "type": "object",
                                "description": "method-specific params; may contain {\"$ref\": \"op_id[.field]\"} placeholders resolved against prior op results"
                            }
                        },
                        "required": ["method", "params"],
                        "additionalProperties": false
                    }
                },
                "atomic": {"type": "boolean", "default": true, "description": "parsed but currently always treated as true"}
            },
            "required": ["ops"],
            "additionalProperties": false
        }))
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
        //
        // commit_outcome: Ok = COMMIT succeeded; OpErr = ROLLBACK with the
        // failing op's (idx, err); CommitErr = COMMIT itself failed (returned
        // verbatim as a Storage error, matching the single-op path).
        enum CommitOutcome {
            Ok,
            OpErr(usize, CoreError),
            CommitErr(CoreError),
        }

        let (results, commit_outcome, deleted_entry_ids): (
            Vec<Value>,
            CommitOutcome,
            Vec<ulid::Ulid>,
        ) = {
            let conn_arc = daemon.entries.conn_for_test();
            let conn = conn_arc.lock().unwrap();

            // Build id → result index map for $ref resolution
            let mut id_to_index: HashMap<String, usize> = HashMap::new();

            let mut results: Vec<Value> = Vec::with_capacity(req.ops.len());
            let mut failed_at: Option<(usize, CoreError)> = None;
            let mut deleted_entry_ids = Vec::new();

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
                        "error": error_to_rpc_value(&err)
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
                            "error": error_to_rpc_value(&e)
                        }));
                        failed_at = Some((i, e));
                        continue;
                    }
                };

                let deleted_entry_id = (op.method == "entry.delete")
                    .then(|| {
                        resolved_params
                            .get("id")
                            .and_then(Value::as_str)
                            .and_then(|id| id.parse::<ulid::Ulid>().ok())
                    })
                    .flatten();

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
                        results.push(json!({"ok": true, "result": value}));
                        if let Some(entry_id) = deleted_entry_id {
                            deleted_entry_ids.push(entry_id);
                        }

                        // Register id for $ref
                        if let Some(ref id) = op.id {
                            id_to_index.insert(id.clone(), i);
                        }
                    }
                    Err(e) => {
                        results.push(json!({
                            "ok": false,
                            "error": error_to_rpc_value(&e)
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

            (results, commit_outcome, deleted_entry_ids)
            // conn + conn_arc drop here, releasing the Mutex.
        };

        // Content deletion is already committed. Clean all adaptive-memory
        // rows in one retryable transaction, but preserve the existing
        // post-commit embedding behavior for other successful batch ops before
        // surfacing a cleanup error.
        let signal_cleanup_error =
            if matches!(&commit_outcome, CommitOutcome::Ok) && !deleted_entry_ids.is_empty() {
                let memory = daemon.memory.clone();
                match crate::handlers::entry::blocking(move || {
                    memory.delete_entries_signals(&deleted_entry_ids)
                })
                .await
                {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) | Err(error) => Some(error),
                }
            } else {
                None
            };

        // 0.2.3: post-commit embed. Collect every entry_id touched by a
        // successful op, dedupe, embed each. Embed failure → provider error
        // (1002); the batch transaction is already committed, so the entry
        // writes stand (atomic guarantees op consistency; embed is
        // post-transaction enrichment and is NOT rolled back).
        if matches!(commit_outcome, CommitOutcome::Ok) {
            let mut touched: HashMap<ulid::Ulid, ()> = HashMap::new();
            for (i, op) in req.ops.iter().enumerate() {
                let ok = results[i]
                    .get("ok")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !ok {
                    continue;
                }
                let id_str = match op.method.as_str() {
                    "entry.create" => results[i]["result"]["id"].as_str(),
                    "entry.update" => op.params.get("id").and_then(|v| v.as_str()),
                    "block.append" => results[i]["result"]["entry_id"]
                        .as_str()
                        .or_else(|| op.params.get("entry_id").and_then(|v| v.as_str())),
                    "block.update" => results[i]["result"]["entry_id"].as_str(),
                    _ => None,
                };
                if let Some(s) = id_str
                    && let Ok(id) = s.parse::<ulid::Ulid>()
                {
                    touched.insert(id, ());
                }
            }
            for entry_id in touched.keys() {
                crate::handlers::embed::embed_entry_chunks(daemon, *entry_id, false).await?;
            }
        }

        if let Some(error) = signal_cleanup_error {
            return Err(CoreError::Config(format!(
                "batch entry deletions committed, but adaptive-memory cleanup failed: {error}; run index.sync or index.rebuild to retry reconciliation"
            )));
        }

        match commit_outcome {
            CommitOutcome::Ok => Ok(json!({
                "results": results,
                "rolled_back": false
            })),
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
    _chunks: &ChunkService,
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

/// Convert CoreError to a JSON-RPC error Value (for results array entries).
///
/// Delegate to `rpc::core_error_to_rpc_ref`
/// instead of duplicating the CoreError → RPC mapping (which previously
/// drifted from the top-level mapping in `rpc.rs`). Wraps the resulting
/// `RpcError` as a Value for direct insertion into the per-op results array.
fn error_to_rpc_value(err: &CoreError) -> Value {
    serde_json::to_value(core_error_to_rpc_ref(err))
        .unwrap_or_else(|_| json!({"code": -32603, "message": "serialize error in batch"}))
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
    fn batch_schema_accepts_minimal_valid() {
        let schema = Batch.input_schema().unwrap();
        let valid = json!({
            "ops": [
                {"method": "entry.create", "params": {"title": "x", "blocks": [{"type": "note", "text": "y"}]}}
            ]
        });
        assert!(validate(&schema, &valid).is_ok());
    }

    #[test]
    fn batch_schema_rejects_missing_ops() {
        let schema = Batch.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn batch_schema_rejects_empty_ops_array() {
        let schema = Batch.input_schema().unwrap();
        assert!(validate(&schema, &json!({"ops": []})).is_err());
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use nomai_core::{
        BlockInput, CreateEntry, CreateSearchSession, EntryService, FeedbackTarget, MemoryPolicy,
        SearchResultTarget,
    };
    use nomai_protocol::{ProviderError, ProviderErrorKind};
    use nomai_providers::{CompletionRequest, CompletionResponse, EmbeddingProvider, LlmProvider};

    use super::*;
    use crate::daemon::DaemonBuilder;

    struct FakeEmbed;

    #[async_trait]
    impl EmbeddingProvider for FakeEmbed {
        async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
            Ok(vec![vec![1.0, 0.0, 0.0, 0.0]; texts.len()])
        }

        fn dim(&self) -> usize {
            4
        }

        fn name(&self) -> &str {
            "fake-embed"
        }
    }

    struct FakeLlm;

    #[async_trait]
    impl LlmProvider for FakeLlm {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            Err(ProviderError::new(
                ProviderErrorKind::Unknown,
                "fake llm",
                None,
            ))
        }

        fn name(&self) -> &str {
            "fake-llm"
        }
    }

    fn daemon() -> Daemon {
        let entries = Arc::new(EntryService::for_test().unwrap());
        DaemonBuilder::new()
            .conn(entries.conn_for_test())
            .content_store(entries.content_store().clone())
            .embedder(Arc::new(FakeEmbed))
            .llm(Arc::new(FakeLlm))
            .embedding_dim(4)
            .chunk_target_size(1024)
            .cache_model("active-model")
            .warn_rows(100_000)
            .memory_policy(MemoryPolicy::default())
            .build()
            .unwrap()
    }

    fn seed_precise_feedback(daemon: &Daemon) -> ulid::Ulid {
        let entry = daemon
            .entries
            .create(CreateEntry {
                title: "batch lifecycle target".into(),
                blocks: vec![BlockInput {
                    r#type: "note".into(),
                    text: "batch lifecycle body".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
                attachments: None,
            })
            .unwrap();
        let block_id = entry.blocks[0].id;
        let chunk_id = daemon.chunks.list(block_id).unwrap().items[0].id;
        let search_id = daemon
            .memory
            .create_search_session(CreateSearchSession {
                raw_query_text: "raw batch query".into(),
                effective_query_text: "effective batch query".into(),
                query_embedding: vec![1.0, 0.0, 0.0, 0.0],
                embedding_model: "active-model".into(),
                results: vec![SearchResultTarget {
                    entry_id: entry.id,
                    matched_block_id: Some(block_id),
                    matched_chunk_id: Some(chunk_id),
                    result_rank: 1,
                }],
            })
            .unwrap();
        daemon
            .memory
            .apply_feedback(
                search_id,
                &[FeedbackTarget {
                    entry_id: entry.id,
                    block_id: Some(block_id),
                    chunk_id: Some(chunk_id),
                }],
            )
            .unwrap();
        entry.id
    }

    fn signal_counts(daemon: &Daemon, entry_id: ulid::Ulid) -> (i64, i64, i64, i64, i64) {
        daemon
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM entry_memory_stats WHERE entry_id = ?1),
                    (SELECT COUNT(*) FROM query_affinities WHERE entry_id = ?1),
                    (SELECT COUNT(*) FROM vec_query_affinities),
                    (SELECT COUNT(*) FROM search_feedback WHERE entry_id = ?1),
                    (SELECT COUNT(*) FROM search_session_results WHERE entry_id = ?1)",
                [entry_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap()
    }

    #[tokio::test]
    async fn batch_entry_delete_removes_every_local_signal_after_commit() {
        let daemon = daemon();
        let entry_id = seed_precise_feedback(&daemon);

        let result = Batch
            .call(
                &daemon,
                json!({
                    "ops": [{
                        "method": "entry.delete",
                        "params": {"id": entry_id},
                    }],
                }),
            )
            .await
            .unwrap();

        assert_eq!(result["rolled_back"], false);
        assert_eq!(result["results"][0]["ok"], true);
        assert_eq!(signal_counts(&daemon, entry_id), (0, 0, 0, 0, 0));
    }

    #[tokio::test]
    async fn batch_entry_delete_cleanup_failure_reports_committed_content_and_retryable_signals() {
        let daemon = daemon();
        let entry_id = seed_precise_feedback(&daemon);
        let conn = daemon.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fail_batch_signal_cleanup
                 BEFORE DELETE ON query_affinities
                 BEGIN SELECT RAISE(FAIL, 'forced batch signal cleanup failure'); END;",
            )
            .unwrap();

        let error = Batch
            .call(
                &daemon,
                json!({
                    "ops": [{
                        "method": "entry.delete",
                        "params": {"id": entry_id},
                    }],
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CoreError::Config(message)
                if message.contains("batch entry deletions committed")
                    && message.contains("adaptive-memory cleanup failed")
        ));
        assert!(daemon.entries.get(entry_id).is_err());
        assert_eq!(signal_counts(&daemon, entry_id), (1, 1, 1, 1, 1));
    }
}
