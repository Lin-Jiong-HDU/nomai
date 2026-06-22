//! provider.* handlers.

use async_trait::async_trait;
use serde_json::{Value, json};

use nomai_core::CoreError;

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "provider.list"
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        Ok(json!({
            "embedding": {
                "name": daemon.embedder.name(),
                "model": daemon.embedding_model,
            },
            "llm": {
                "name": daemon.llm.name(),
                "model": daemon.llm_model,
            }
        }))
    }
}
