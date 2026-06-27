//! chunk.* handlers. Plan 4: chunks are auto-derived from blocks.
//! `chunk.create` / `chunk.update` / `chunk.delete` return -32601
//! METHOD_NOT_FOUND (chunks are not user-managed). Only `chunk.list`
//! (block-scoped) and `chunk.get` remain.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{ChunkService, CoreError};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;

pub struct Get;
#[async_trait]
impl RpcHandler for Get {
    fn method(&self) -> &'static str {
        "chunk.get"
    }
    fn description(&self) -> &'static str {
        "Fetch a single chunk by ULID. Returns error 1001 if not found."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::ulid_param_schema())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let chunks: std::sync::Arc<ChunkService> = daemon.chunks.clone();
        let chunk = blocking(move || chunks.get(p.id)).await??;

        serde_json::to_value(&chunk).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "chunk.list"
    }
    fn description(&self) -> &'static str {
        "List all chunks belonging to a block, in ordinal order. Returns {items, total}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": { "block_id": crate::handlers::params::ulid_schema() },
            "required": ["block_id"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            block_id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let chunks = daemon.chunks.clone();
        let result = blocking(move || chunks.list(p.block_id)).await??;

        Ok(json!({
            "items": result.items,
            "total": result.total,
        }))
    }
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params).map_err(|errs| {
            errs.map(|e| format!("{e}")).collect::<Vec<_>>()
        })
    }

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    #[test]
    fn get_schema_accepts_valid_id() {
        let schema = Get.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn get_schema_rejects_missing_id() {
        let schema = Get.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn list_schema_accepts_block_id() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({"block_id": ULID})).is_ok());
    }

    #[test]
    fn list_schema_rejects_missing_block_id() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }
}
