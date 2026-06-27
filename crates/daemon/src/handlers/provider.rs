//! provider.* handlers.

use async_trait::async_trait;
use serde_json::{Value, json};

use nomai_core::CoreError;
use nomai_providers::EmbeddingProvider;

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "provider.list"
    }
    fn description(&self) -> &'static str {
        "List the currently configured embedding and LLM providers with their model names."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        Ok(json!({
            "embedding": {
                "name": daemon.cache.name(),
                "model": daemon.embedding_model,
            },
            "llm": {
                "name": daemon.llm.name(),
                "model": daemon.llm_model,
            }
        }))
    }
}
