//! search.* handlers.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, Granularity};
use nomai_providers::EmbeddingProvider;

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;

fn default_search_limit() -> u32 {
    10
}

#[derive(Deserialize)]
struct SearchParams {
    query: String,
    #[serde(default = "default_search_limit")]
    limit: u32,
    #[serde(default)]
    granularity: Granularity,
}

pub struct Fulltext;
#[async_trait]
impl RpcHandler for Fulltext {
    fn method(&self) -> &'static str {
        "search.fulltext"
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: SearchParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let query = p.query;
        let limit = p.limit;
        let results = blocking(move || entries.fulltext_search(&query, limit)).await??;

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
        let p: SearchParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        // Embed query, then KNN.
        let query_str = p.query;
        let embeddings = daemon.cache.embed(&[&query_str]).await?;
        let qvec = embeddings
            .into_iter()
            .next()
            .ok_or_else(|| CoreError::Config("empty embedding response".into()))?;

        match p.granularity {
            Granularity::Entry => {
                let entries = daemon.entries.clone();
                let limit = p.limit;
                let results = blocking(move || entries.semantic_search(&qvec, limit)).await??;
                let items: Vec<Value> = results
                    .iter()
                    .map(|r| json!({ "entry": r.entry, "score": r.score }))
                    .collect();
                Ok(json!({ "items": items }))
            }
            Granularity::Chunk => {
                let chunks = daemon.chunks.clone();
                let limit = p.limit;
                let results = blocking(move || chunks.semantic_search(&qvec, limit)).await??;
                let items: Vec<Value> = results
                    .iter()
                    .map(|r| json!({ "chunk": r.chunk, "score": r.score }))
                    .collect();
                Ok(json!({ "items": items }))
            }
        }
    }
}
