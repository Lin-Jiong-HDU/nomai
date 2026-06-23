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
