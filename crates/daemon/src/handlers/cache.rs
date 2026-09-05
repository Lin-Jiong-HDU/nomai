//! cache.* handlers: embedding cache + search cache introspection/management.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use nomai_core::CoreError;
use nomai_protocol::method::cache::{CLEAR as CACHE_CLEAR, STATS as CACHE_STATS};
use nomai_providers::ClearOptions;

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

pub struct Stats;
#[async_trait]
impl RpcHandler for Stats {
    fn method(&self) -> &'static str {
        CACHE_STATS
    }
    fn description(&self) -> &'static str {
        "Report embedding-cache and search-cache stats: row counts, hit rates, generation, warnings."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        let stats = daemon.cache.stats()?;
        let s = daemon.search_cache.stats();
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
            },
            "searches": {
                "generation": s.generation,
                "entries": s.entries,
                "hits": s.hits,
                "misses": s.misses,
                "hit_rate": s.hit_rate(),
                "by_rpc": {
                    "semantic": { "hits": s.semantic_hits, "misses": s.semantic_misses },
                    "fulltext": { "hits": s.fulltext_hits, "misses": s.fulltext_misses },
                    "hybrid": { "hits": s.hybrid_hits, "misses": s.hybrid_misses },
                },
            }
        }))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Namespace {
    Embeddings,
    Searches,
    All,
}

impl Default for Namespace {
    /// Default = `Embeddings` so omitting `namespace` clears only `emb_cache`
    /// (Kernel-style fail-safe default + explicit user intent).
    /// Note: the *clearing behavior* is backward compatible; the *response
    /// shape* changed to nest under `embeddings`/`searches` (see docs).
    fn default() -> Self {
        Namespace::Embeddings
    }
}

#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
pub struct ClearParams {
    #[serde(default)]
    #[schemars(default)]
    pub namespace: Namespace,
    // Existing emb_cache filters below — only consulted when namespace ∈
    // {Embeddings, All}.
    #[serde(default)]
    #[schemars(default)]
    pub model: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub before: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    #[schemars(default)]
    pub keep_recent: Option<u64>,
}

impl From<ClearParams> for ClearOptions {
    fn from(p: ClearParams) -> Self {
        ClearOptions {
            model: p.model,
            before: p.before,
            keep_recent: p.keep_recent,
        }
    }
}

pub struct Clear;
#[async_trait]
impl RpcHandler for Clear {
    fn method(&self) -> &'static str {
        CACHE_CLEAR
    }
    fn description(&self) -> &'static str {
        "Clear embedding cache and/or search cache. namespace controls which: embeddings (default), searches, or all. For embeddings, optional model/before/keep_recent filter the rows cleared."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(ClearParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let opts: ClearParams = if params.is_null() {
            ClearParams::default()
        } else {
            serde_json::from_value(params)
                .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?
        };

        let mut embeddings_result = Value::Null;
        let mut searches_result = Value::Null;

        match opts.namespace {
            Namespace::Embeddings => {
                let r = daemon.cache.clear(opts.into())?;
                embeddings_result = json!({ "cleared": r.cleared, "by_model": r.by_model });
            }
            Namespace::Searches => {
                let cleared = daemon.search_cache.clear();
                searches_result = json!({ "cleared": cleared });
            }
            Namespace::All => {
                let r = daemon.cache.clear(opts.into())?;
                embeddings_result = json!({ "cleared": r.cleared, "by_model": r.by_model });
                let cleared = daemon.search_cache.clear();
                searches_result = json!({ "cleared": cleared });
            }
        }

        Ok(json!({
            "embeddings": embeddings_result,
            "searches": searches_result,
        }))
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
    fn stats_schema_accepts_empty_object() {
        let schema = Stats.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_ok());
    }

    #[test]
    fn stats_schema_rejects_extra_props() {
        let schema = Stats.input_schema().unwrap();
        assert!(validate(&schema, &json!({"foo": 1})).is_err());
    }

    #[test]
    fn clear_schema_accepts_empty_defaults_to_embeddings() {
        let schema = Clear.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_ok());
    }

    #[test]
    fn clear_schema_accepts_namespace_all() {
        let schema = Clear.input_schema().unwrap();
        assert!(validate(&schema, &json!({"namespace": "all"})).is_ok());
    }

    #[test]
    fn clear_schema_rejects_unknown_namespace() {
        let schema = Clear.input_schema().unwrap();
        assert!(validate(&schema, &json!({"namespace": "bogus"})).is_err());
    }
}

#[cfg(test)]
mod stats_tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use nomai_core::EntryService;
    use nomai_protocol::{ProviderError, ProviderErrorKind};
    use nomai_providers::{CompletionRequest, CompletionResponse, EmbeddingProvider, LlmProvider};

    use super::*;
    use crate::daemon::DaemonBuilder;
    use crate::search_cache::SearchRpc;

    struct FakeEmbed;

    #[async_trait]
    impl EmbeddingProvider for FakeEmbed {
        async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
            Ok(vec![vec![1.0, 0.0, 0.0, 0.0]; texts.len()])
        }

        fn dim(&self) -> usize {
            4
        }

        fn name(&self) -> &str {
            "fake-embed"
        }
    }

    struct FakeLlm;

    #[async_trait]
    impl LlmProvider for FakeLlm {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            Err(ProviderError::new(
                ProviderErrorKind::Unknown,
                "fake llm",
                None,
            ))
        }

        fn name(&self) -> &str {
            "fake-llm"
        }
    }

    fn daemon() -> Daemon {
        let entries = Arc::new(EntryService::for_test().unwrap());
        DaemonBuilder::new()
            .conn(entries.conn_for_test())
            .content_store(entries.content_store().clone())
            .embedder(Arc::new(FakeEmbed))
            .llm(Arc::new(FakeLlm))
            .embedding_dim(4)
            .chunk_target_size(1024)
            .cache_model("active-model")
            .warn_rows(100_000)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn cache_stats_reports_hybrid_hits_and_misses_by_rpc() {
        let daemon = daemon();
        daemon
            .search_cache
            .lookup_or_compute(
                SearchRpc::Hybrid,
                "hybrid",
                20,
                None,
                None,
                Some(7),
                || async { Ok(vec![json!({"entry": {"id": "cached"}})]) },
            )
            .await
            .unwrap();
        daemon
            .search_cache
            .lookup_or_compute(
                SearchRpc::Hybrid,
                "hybrid",
                20,
                None,
                None,
                Some(7),
                || async { panic!("second lookup must hit the hybrid cache") },
            )
            .await
            .unwrap();

        let stats = Stats.call(&daemon, json!({})).await.unwrap();

        assert_eq!(stats["searches"]["by_rpc"]["hybrid"]["hits"], 1);
        assert_eq!(stats["searches"]["by_rpc"]["hybrid"]["misses"], 1);
    }
}
