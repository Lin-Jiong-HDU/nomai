//! Daemon: owns EntryService + providers; orchestrates RPC handlers.

use std::sync::Arc;

use rusqlite::Connection;

use nomai_core::{CoreError, EntryService};
use nomai_providers::{EmbeddingProvider, LlmProvider, OpenAiCompatibleEmbed, OpenAiCompatibleLlm};

use crate::config::Config;

#[allow(dead_code)] // fields wired in Task 5 (run_stdio) and exercised by integration tests.
pub struct Daemon {
    pub(crate) entries: Arc<EntryService>,
    pub(crate) embedder: Arc<dyn EmbeddingProvider>,
    pub(crate) llm: Arc<dyn LlmProvider>,
    pub(crate) embedding_model: String,
    pub(crate) llm_model: String,
    pub(crate) embedding_dim: usize,
}

impl Daemon {
    pub async fn new(config: Config) -> Result<Self, CoreError> {
        // Open SQLite (creating parent dir if needed).
        let db_path = expand_db_path(&config.data.db_path)?;
        let conn = Connection::open(&db_path)?;

        // Run migrations + ensure vec_embeddings exists.
        let entries = Arc::new(EntryService::new(conn)?);
        entries.ensure_vec_embeddings(config.embedding.dim)?;

        // Read API keys (config.validate already checked env var presence).
        let embedding_key = std::env::var(&config.embedding.api_key_env).map_err(|_| {
            CoreError::Config(format!("missing env: {}", config.embedding.api_key_env))
        })?;
        let llm_key = std::env::var(&config.llm.api_key_env)
            .map_err(|_| CoreError::Config(format!("missing env: {}", config.llm.api_key_env)))?;

        let embedder: Arc<dyn EmbeddingProvider> = Arc::new(OpenAiCompatibleEmbed::new(
            &config.embedding.base_url,
            &embedding_key,
            &config.embedding.model,
            config.embedding.dim,
        ));
        let llm: Arc<dyn LlmProvider> = Arc::new(OpenAiCompatibleLlm::new(
            &config.llm.base_url,
            &llm_key,
            &config.llm.model,
        ));

        Ok(Self {
            entries,
            embedder,
            llm,
            embedding_model: config.embedding.model,
            llm_model: config.llm.model,
            embedding_dim: config.embedding.dim,
        })
    }

    #[cfg(test)]
    #[allow(dead_code)] // exercised by integration tests added in later tasks.
    pub(crate) fn for_test(
        entries: Arc<EntryService>,
        embedder: Arc<dyn EmbeddingProvider>,
        llm: Arc<dyn LlmProvider>,
        embedding_model: String,
        llm_model: String,
        embedding_dim: usize,
    ) -> Self {
        Self {
            entries,
            embedder,
            llm,
            embedding_model,
            llm_model,
            embedding_dim,
        }
    }

    /// Run the NDJSON-over-stdio JSON-RPC loop. Stub in Task 4; full impl in Task 5.
    pub async fn run_stdio(self) -> Result<(), CoreError> {
        // Task 5 implements this; for now just signal clean shutdown.
        let _ = self;
        Ok(())
    }
}

fn expand_db_path(path: &std::path::Path) -> Result<std::path::PathBuf, CoreError> {
    let s = path.to_string_lossy();
    let expanded = if s.starts_with('~') {
        let home = std::env::var("HOME")
            .map(std::path::PathBuf::from)
            .map_err(|_| CoreError::Config("HOME not set; cannot expand ~".into()))?;
        home.join(path.strip_prefix("~").unwrap_or(path))
    } else {
        path.to_path_buf()
    };
    if let Some(parent) = expanded.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| CoreError::Config(format!("create db dir: {e}")))?;
        }
    }
    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_db_path_creates_parent_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("a/b/c/data.sqlite");
        let expanded = expand_db_path(&nested).unwrap();
        assert!(expanded.parent().unwrap().exists());
    }
}
