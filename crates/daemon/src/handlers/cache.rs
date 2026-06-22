//! cache.* handlers: embedding cache introspection and management.

use async_trait::async_trait;
use serde_json::{Value, json};

use nomai_core::CoreError;
use nomai_providers::ClearOptions;

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

pub struct Stats;
#[async_trait]
impl RpcHandler for Stats {
    fn method(&self) -> &'static str {
        "cache.stats"
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        let stats = daemon.cache.stats()?;
        Ok(json!({
            "embeddings": {
                "model": stats.model,
                "dim": stats.dim,
                "rows": stats.rows,
                "hits": stats.hits,
                "misses": stats.misses,
                "hit_rate": stats.hit_rate(),
                "warn_rows": stats.warn_rows,
                "warning": stats.warning,
            }
        }))
    }
}

pub struct Clear;
#[async_trait]
impl RpcHandler for Clear {
    fn method(&self) -> &'static str {
        "cache.clear"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let opts: ClearOptions = if params.is_null() {
            ClearOptions::default()
        } else {
            serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?
        };
        let result = daemon.cache.clear(opts)?;
        Ok(json!({ "cleared": result.cleared, "by_model": result.by_model }))
    }
}
