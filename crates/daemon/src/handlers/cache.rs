//! cache.* handlers: embedding cache + search cache introspection/management.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
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
                },
            }
        }))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Namespace {
    Embeddings,
    Searches,
    All,
}

impl Default for Namespace {
    /// Default = `Embeddings` so omitting `namespace` clears only `emb_cache`
    /// (Spec §7.2: kernel-style fail-safe default + explicit user intent).
    /// Note: the *clearing behavior* is backward compatible; the *response
    /// shape* changed to nest under `embeddings`/`searches` (see docs).
    fn default() -> Self {
        Namespace::Embeddings
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClearParams {
    #[serde(default)]
    pub namespace: Namespace,
    // Existing emb_cache filters below — only consulted when namespace ∈
    // {Embeddings, All}.
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub before: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
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
        "cache.clear"
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
