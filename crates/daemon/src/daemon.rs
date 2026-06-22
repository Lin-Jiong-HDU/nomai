//! Daemon: owns EntryService + providers; orchestrates RPC handlers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use nomai_core::{ChunkService, CoreError, EntryService, EventService, LinkService};
use nomai_providers::{EmbeddingProvider, LlmProvider, OpenAiCompatibleEmbed, OpenAiCompatibleLlm};

use crate::config::Config;
use crate::rpc::RpcHandler;

pub struct Daemon {
    pub(crate) entries: Arc<EntryService>,
    pub(crate) links: Arc<LinkService>,
    pub(crate) events: Arc<EventService>,
    pub(crate) chunks: Arc<ChunkService>,
    pub(crate) embedder: Arc<dyn EmbeddingProvider>,
    pub(crate) llm: Arc<dyn LlmProvider>,
    pub(crate) embedding_model: String,
    pub(crate) llm_model: String,
    // Used by search.semantic (Task 7); keep despite no current reader.
    #[allow(dead_code)]
    pub(crate) embedding_dim: usize,
    pub(crate) handlers: HashMap<&'static str, Arc<dyn RpcHandler>>,
}

impl Daemon {
    pub async fn new(config: Config) -> Result<Self, CoreError> {
        // Open SQLite (creating parent dir if needed).
        let db_path = expand_db_path(&config.data.db_path)?;
        let conn = Connection::open(&db_path)?;
        let conn = Arc::new(Mutex::new(conn));

        // Run migrations + ensure vec_embeddings / vec_chunk_embeddings exist.
        let entries = Arc::new(EntryService::new(conn.clone())?);
        let links = Arc::new(LinkService::new(conn.clone())?);
        let events = Arc::new(EventService::new(conn.clone())?);
        let chunks = Arc::new(ChunkService::new(conn.clone())?);
        chunks.ensure_vec_chunk_embeddings(config.embedding.dim)?;
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
            links,
            events,
            chunks,
            embedder,
            llm,
            embedding_model: config.embedding.model,
            llm_model: config.llm.model,
            embedding_dim: config.embedding.dim,
            handlers: crate::handlers::registry(),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        entries: Arc<EntryService>,
        embedder: Arc<dyn EmbeddingProvider>,
        llm: Arc<dyn LlmProvider>,
        embedding_model: String,
        llm_model: String,
        embedding_dim: usize,
    ) -> Self {
        // Reconstruct the shared connection from EntryService. Since
        // EntryService holds Arc<Mutex<Connection>>, we expose a test-only
        // accessor to share it with LinkService.
        let conn = entries.conn_for_test();
        let links = Arc::new(LinkService::new(conn).unwrap());
        let conn2 = entries.conn_for_test();
        let events = Arc::new(EventService::new(conn2).unwrap());
        let conn3 = entries.conn_for_test();
        let chunks = Arc::new(ChunkService::new(conn3).unwrap());
        Self {
            entries,
            links,
            events,
            chunks,
            embedder,
            llm,
            embedding_model,
            llm_model,
            embedding_dim,
            handlers: crate::handlers::registry(),
        }
    }

    /// Register an additional RPC handler. The handler's `method()` name
    /// must not collide with an existing entry (collisions replace the
    /// prior handler, matching standard HashMap::insert semantics).
    #[allow(dead_code)] // lib-mode extension point; binary daemon doesn't call this
    pub fn register_handler(&mut self, handler: Arc<dyn RpcHandler>) {
        self.handlers.insert(handler.method(), handler);
    }

    /// Run the NDJSON-over-stdio JSON-RPC loop. Stub in Task 4; full impl in Task 5.
    pub async fn run_stdio(self) -> Result<(), CoreError> {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let daemon = Arc::new(self);
        let mut line = String::new();

        loop {
            line.clear();
            let n = reader
                .read_line(&mut line)
                .await
                .map_err(|e| CoreError::Config(format!("stdin read: {e}")))?;
            if n == 0 {
                break; // EOF
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Parse JSON-RPC request.
            let req: nomai_protocol::Request = match serde_json::from_str(trimmed) {
                Ok(r) => r,
                Err(e) => {
                    // Parse error — no reliable id; respond with id=null.
                    let resp = nomai_protocol::Response::err(
                        None,
                        nomai_protocol::RpcError {
                            code: nomai_protocol::error::PARSE_ERROR,
                            message: nomai_protocol::error::MESSAGE_PARSE_ERROR.into(),
                            data: Some(serde_json::Value::String(e.to_string())),
                        },
                    );
                    let _ = crate::io::write_response_line(&resp);
                    continue;
                }
            };

            let is_notification = req.id.is_none();
            let resp = daemon.dispatch(req).await;
            if !is_notification {
                let _ = crate::io::write_response_line(&resp);
            }
        }
        Ok(())
    }

    pub async fn dispatch(&self, req: nomai_protocol::Request) -> nomai_protocol::Response {
        use nomai_protocol::error::{MESSAGE_METHOD_NOT_FOUND, METHOD_NOT_FOUND};
        use nomai_protocol::{Response, RpcError};

        let id = req.id.clone();
        let params = req.params.unwrap_or(serde_json::Value::Null);
        let result = match self.handlers.get(req.method.as_str()) {
            Some(handler) => handler
                .call(self, params)
                .await
                .map_err(crate::rpc::DispatchError::Core),
            None => Err(crate::rpc::DispatchError::MethodNotFound(
                req.method.clone(),
            )),
        };
        match result {
            Ok(value) => Response::ok(id, value),
            Err(crate::rpc::DispatchError::Core(err)) => {
                Response::err(id, crate::rpc::core_error_to_rpc(err))
            }
            Err(crate::rpc::DispatchError::MethodNotFound(method)) => Response::err(
                id,
                RpcError {
                    code: METHOD_NOT_FOUND,
                    message: MESSAGE_METHOD_NOT_FOUND.into(),
                    data: Some(serde_json::json!({ "method": method })),
                },
            ),
        }
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
