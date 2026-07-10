//! search.* handlers.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::CoreError;
use nomai_providers::EmbeddingProvider;

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use crate::search_cache::SearchRpc;

fn default_search_limit() -> u32 {
    10
}

#[derive(Deserialize, schemars::JsonSchema)]
struct FulltextParams {
    query: String,
    #[serde(default = "default_search_limit")]
    #[schemars(default = "default_search_limit")]
    limit: u32,
    /// Optional block-type filter (e.g. `"claim"`, `"note"`). When supplied,
    /// restricts matches to blocks of that type.
    #[serde(default)]
    #[schemars(default)]
    block_type: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SemanticParams {
    query: String,
    #[serde(default = "default_search_limit")]
    #[schemars(default = "default_search_limit")]
    limit: u32,
    /// Optional block-type filter applied via JOIN blocks.
    #[serde(default)]
    #[schemars(default)]
    block_type: Option<String>,
}

/// Map a `FulltextSearchResult` to the wire JSON object. Extracted so the
/// field set is unit-testable without spinning up a Daemon.
fn serialize_fulltext_result(r: &nomai_core::FulltextSearchResult) -> serde_json::Value {
    json!({
        "entry": r.entry,
        "score": r.score,
        "match_count": r.match_count,
        "matched_block_ids": r.matched_block_ids,
        "best_match": {
            "block_id": r.best_match.block_id,
            "block_type": r.best_match.block_type,
            "snippet": r.best_match.snippet,
        }
    })
}

pub struct Fulltext;
#[async_trait]
impl RpcHandler for Fulltext {
    fn method(&self) -> &'static str {
        "search.fulltext"
    }
    fn description(&self) -> &'static str {
        "Fulltext search over block text via SQLite FTS5. Returns entries ranked by relevance. Score is relative relevance (not a boolean hit marker): higher means stronger match; near-zero means weak match. Each call is cached per (query, limit, block_type)."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(FulltextParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: FulltextParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let query = p.query;
        let limit = p.limit;
        let block_type = p.block_type;
        // Snapshot the strings we need inside the compute closure. `query`
        // is moved; `block_type` is cloned because we need to also pass an
        // `Option<&str>` to lookup_or_compute below.
        let bt_for_compute = block_type.clone();
        let cached = daemon
            .search_cache
            .lookup_or_compute(
                SearchRpc::Fulltext,
                &query,
                limit,
                block_type.as_deref(),
                || {
                    let entries = entries.clone();
                    let bt = bt_for_compute.clone();
                    let query_for_closure = query.clone();
                    async move {
                        let items_inner = blocking(move || {
                            entries.fulltext_search(&query_for_closure, limit, bt.as_deref())
                        })
                        .await??;
                        // Map to JSON value items matching the existing wire
                        // format.
                        Ok(items_inner
                            .into_iter()
                            .map(|r| serialize_fulltext_result(&r))
                            .collect::<Vec<_>>())
                    }
                },
            )
            .await?;
        Ok(json!({ "items": cached.as_ref() }))
    }
}

pub struct Semantic;
#[async_trait]
impl RpcHandler for Semantic {
    fn method(&self) -> &'static str {
        "search.semantic"
    }
    fn description(&self) -> &'static str {
        "Semantic search over chunk embeddings via sqlite-vec cosine similarity. Queries the embedding provider on each unique query (cached). Returns chunks ranked by similarity."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(SemanticParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: SemanticParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let query_str = p.query;
        let limit = p.limit;
        let block_type = p.block_type;
        let bt_for_compute = block_type.clone();

        let cache = daemon.search_cache.clone();
        let embed = daemon.cache.clone();
        let chunks = daemon.chunks.clone();
        let cached = cache
            .lookup_or_compute(
                SearchRpc::Semantic,
                &query_str,
                limit,
                block_type.as_deref(),
                || {
                    let embed = embed.clone();
                    let chunks = chunks.clone();
                    let query_owned = query_str.clone();
                    let bt = bt_for_compute.clone();
                    async move {
                        let embeddings = embed.embed(&[&query_owned]).await?;
                        let qvec = embeddings
                            .into_iter()
                            .next()
                            .ok_or_else(|| CoreError::Config("empty embedding response".into()))?;
                        let items_inner = blocking(move || {
                            chunks.semantic_search(&qvec, limit as usize, bt.as_deref())
                        })
                        .await??;
                        Ok(items_inner
                            .into_iter()
                            .map(|r| json!({ "chunk": r.chunk, "score": r.score }))
                            .collect::<Vec<_>>())
                    }
                },
            )
            .await?;
        Ok(json!({ "items": cached.as_ref() }))
    }
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
    fn fulltext_schema_accepts_query() {
        let schema = Fulltext.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "hello"})).is_ok());
    }

    #[test]
    fn fulltext_schema_rejects_missing_query() {
        let schema = Fulltext.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn semantic_schema_accepts_query() {
        let schema = Semantic.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "hello"})).is_ok());
    }

    #[test]
    fn semantic_schema_rejects_missing_query() {
        let schema = Semantic.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn fulltext_wire_serializes_new_fields() {
        // Drive a real fulltext_search result so we don't hand-construct
        // Entry/Ulid/DateTime — only the wire mapping is under test here.
        let svc = nomai_core::EntryService::for_test().unwrap();
        svc.create(nomai_core::CreateEntry {
            title: "t".into(),
            blocks: vec![nomai_core::BlockInput {
                r#type: "note".into(),
                text: "the setsid call".into(),
                attrs: None,
            }],
            tags: None,
            attrs: None,
            source: None,
        })
        .unwrap();
        let results = svc.fulltext_search("setsid", 10, None).unwrap();
        let v = serialize_fulltext_result(&results[0]);
        assert!(v.get("entry").is_some());
        assert!(v["match_count"].as_u64().is_some());
        assert!(v["matched_block_ids"].is_array());
        assert_eq!(v["best_match"]["block_type"], "note");
        assert!(
            v["best_match"]["snippet"]
                .as_str()
                .unwrap()
                .contains("**setsid**")
        );
    }
}
