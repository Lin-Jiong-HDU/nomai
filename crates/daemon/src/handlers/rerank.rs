//! rerank.* handlers.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::CoreError;
use nomai_providers::rerank::RerankCandidate;

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

fn default_top_n() -> usize {
    10
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RerankParams {
    query: String,
    candidates: Vec<RerankCandidateWire>,
    #[serde(default = "default_top_n")]
    #[schemars(default = "default_top_n")]
    top_n: usize,
}

/// Wire-level candidate (mirrors providers::RerankCandidate but with
/// schemars JsonSchema derive — the providers type is intentionally
/// schema-free because it's a trait-level type).
#[derive(Deserialize, schemars::JsonSchema)]
struct RerankCandidateWire {
    id: String,
    content: String,
    score: f32,
}

pub struct Rerank;

#[async_trait]
impl RpcHandler for Rerank {
    fn method(&self) -> &'static str {
        "rerank.rerank"
    }
    fn description(&self) -> &'static str {
        "Rerank candidate documents against a query using the configured reranker. Candidates are scored for relevance and returned sorted best-first. Use after search to refine results, or independently with any document set."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(RerankParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: RerankParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        if p.query.trim().is_empty() {
            return Err(CoreError::Validation("query must not be empty".into()));
        }
        if p.candidates.is_empty() {
            return Ok(json!({ "items": [] }));
        }

        let candidates: Vec<RerankCandidate> = p
            .candidates
            .into_iter()
            .map(|c| RerankCandidate {
                id: c.id,
                content: c.content,
                score: c.score,
            })
            .collect();

        let reranked = daemon
            .reranker
            .rerank(&p.query, &candidates, p.top_n)
            .await
            .map_err(CoreError::Provider)?;

        let items: Vec<Value> = reranked
            .into_iter()
            .map(|r| {
                json!({
                    "id": r.id,
                    "content": r.content,
                    "original_score": r.original_score,
                    "rerank_score": r.rerank_score,
                    "reason": r.reason,
                })
            })
            .collect();

        Ok(json!({ "items": items }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params)
            .map_err(|errs| errs.map(|e| format!("{e}")).collect::<Vec<_>>())
    }

    #[test]
    fn schema_accepts_valid_params() {
        let schema = Rerank.input_schema().unwrap();
        assert!(
            validate(
                &schema,
                &json!({
                    "query": "test",
                    "candidates": [{"id": "1", "content": "hello", "score": 0.8}]
                })
            )
            .is_ok()
        );
    }

    #[test]
    fn schema_rejects_missing_query() {
        let schema = Rerank.input_schema().unwrap();
        assert!(
            validate(
                &schema,
                &json!({
                    "candidates": [{"id": "1", "content": "hello", "score": 0.8}]
                })
            )
            .is_err()
        );
    }

    #[test]
    fn schema_accepts_empty_candidates() {
        let schema = Rerank.input_schema().unwrap();
        // Empty candidates array is valid (returns empty items).
        assert!(
            validate(
                &schema,
                &json!({
                    "query": "test",
                    "candidates": []
                })
            )
            .is_ok()
        );
    }
}
