//! search.* handlers.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::CoreError;
use nomai_providers::EmbeddingProvider;

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;

fn default_search_limit() -> u32 {
    10
}

#[derive(Deserialize)]
struct FulltextParams {
    query: String,
    #[serde(default = "default_search_limit")]
    limit: u32,
    /// Optional block-type filter (e.g. `"claim"`, `"note"`). When supplied,
    /// restricts matches to blocks of that type.
    #[serde(default)]
    block_type: Option<String>,
}

#[derive(Deserialize)]
struct SemanticParams {
    query: String,
    #[serde(default = "default_search_limit")]
    limit: u32,
    /// Optional block-type filter applied via JOIN blocks.
    #[serde(default)]
    block_type: Option<String>,
}

pub struct Fulltext;
#[async_trait]
impl RpcHandler for Fulltext {
    fn method(&self) -> &'static str {
        "search.fulltext"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: FulltextParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let query = p.query;
        let limit = p.limit;
        let block_type = p.block_type;
        let results =
            blocking(move || entries.fulltext_search(&query, limit, block_type.as_deref()))
                .await??;

        let items: Vec<Value> = results
            .iter()
            .map(|r| json!({ "entry": r.entry, "score": r.score }))
            .collect();
        Ok(json!({ "items": items }))
    }
}

pub struct Semantic;
#[async_trait]
impl RpcHandler for Semantic {
    fn method(&self) -> &'static str {
        "search.semantic"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: SemanticParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        // Embed query, then KNN over chunk embeddings.
        let query_str = p.query;
        let embeddings = daemon.cache.embed(&[&query_str]).await?;
        let qvec = embeddings
            .into_iter()
            .next()
            .ok_or_else(|| CoreError::Config("empty embedding response".into()))?;

        let chunks = daemon.chunks.clone();
        let limit = p.limit;
        let block_type = p.block_type;
        let results =
            blocking(move || chunks.semantic_search(&qvec, limit as usize, block_type.as_deref()))
                .await??;
        let items: Vec<Value> = results
            .iter()
            .map(|r| json!({ "chunk": r.chunk, "score": r.score }))
            .collect();
        Ok(json!({ "items": items }))
    }
}
