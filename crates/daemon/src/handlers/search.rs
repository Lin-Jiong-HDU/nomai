//! search.* handlers.

use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::CoreError;

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;

fn default_search_limit() -> u32 {
    10
}

#[derive(Deserialize)]
struct SearchParams {
    query: String,
    #[serde(default = "default_search_limit")]
    limit: u32,
}

pub async fn fulltext(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
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

pub async fn semantic(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    let p: SearchParams = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    // Embed query, then KNN.
    let query_str = p.query;
    let embeddings = daemon.embedder.embed(&[&query_str]).await?;
    let qvec = embeddings
        .into_iter()
        .next()
        .ok_or_else(|| CoreError::Config("empty embedding response".into()))?;

    let entries = daemon.entries.clone();
    let limit = p.limit;
    let results = blocking(move || entries.semantic_search(&qvec, limit)).await??;

    let items: Vec<Value> = results
        .iter()
        .map(|r| json!({ "entry": r.entry, "score": r.score }))
        .collect();
    Ok(json!({ "items": items }))
}
