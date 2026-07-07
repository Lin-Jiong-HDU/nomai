//! batch RPC handler: single-request multi-op with atomic transaction.
//!
//! See spec 2026-06-22-batch-rpc-design.md §3-§6.

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
    /// Task 1 always treats the batch as atomic; non-atomic mode is a future
    /// addition and the field is parsed but not yet branched on.
    #[serde(default = "default_atomic")]
    #[allow(dead_code)]
    pub atomic: bool,
}

fn default_atomic() -> bool {
    true
}

/// Allowed methods in batch (mutation only). Plan 4: chunk.create/delete
/// removed (chunks are auto-derived).
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

        let (results, commit_outcome): (Vec<Value>, CommitOutcome) = {
            let conn_arc = daemon.entries.conn_for_test();
            let conn = conn_arc.lock().unwrap();

            // Build id → result index map for $ref resolution
            let mut id_to_index: HashMap<String, usize> = HashMap::new();

            let mut results: Vec<Value> = Vec::with_capacity(req.ops.len());
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

            (results, commit_outcome)
            // conn + conn_arc drop here, releasing the Mutex.
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
                if let Some(s) = id_str {
                    if let Ok(id) = s.parse::<ulid::Ulid>() {
                        touched.insert(id, ());
                    }
                }
            }
            for entry_id in touched.keys() {
                crate::handlers::embed::embed_entry_chunks(daemon, *entry_id).await?;
            }
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
/// Spec 8 Plan 2 / F-batch-4: delegate to `rpc::core_error_to_rpc_ref`
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
