//! Daemon: owns EntryService + providers; orchestrates RPC handlers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use nomai_core::{ChunkService, ContentStore, CoreError, EntryService, EventService, LinkService};
use nomai_providers::{
    CachedEmbedder, EmbeddingProvider, LlmProvider, OpenAiCompatibleEmbed, OpenAiCompatibleLlm,
};

use crate::config::Config;
use crate::rpc::RpcHandler;

pub struct Daemon {
    pub(crate) entries: Arc<EntryService>,
    pub(crate) links: Arc<LinkService>,
    pub(crate) events: Arc<EventService>,
    pub(crate) chunks: Arc<ChunkService>,
    /// Cached embedding provider. Transparent wrapper around the configured
    /// `OpenAiCompatibleEmbed` (or any `EmbeddingProvider` in lib mode) that
    /// persists embeddings in the `emb_cache` table. Accessed as both the
    /// typed `cache` (for `cache.stats` / `cache.clear` RPCs) and via the
    /// `EmbeddingProvider` trait (transparent delegation to inner).
    pub(crate) cache: Arc<CachedEmbedder>,
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

        // Construct FS-backed ContentStore from config.data.knowledge_root
        // (or the default <data_dir>/store/). Created here so it can be
        // shared across EntryService + future Plan 5 index.sync.
        let default_root = crate::config::default_knowledge_root();
        let knowledge_root = expand_knowledge_root(
            config
                .data
                .knowledge_root
                .as_deref()
                .unwrap_or(&default_root),
        )?;
        let content_store = Arc::new(ContentStore::new(knowledge_root));

        // Run migrations + ensure vec_chunk_embeddings exist (Plan 4:
        // entry-level vec_embeddings was dropped in V8; chunk-level is the
        // sole embedding surface).
        let entries = Arc::new(EntryService::new(conn.clone(), content_store)?);
        let links = Arc::new(LinkService::new(conn.clone())?);
        let events = Arc::new(EventService::new(conn.clone())?);
        let chunks = Arc::new(ChunkService::new(conn.clone())?);
        chunks.ensure_vec_chunk_embeddings(config.embedding.dim)?;

        // Read API keys (config.validate already checked env var presence).
        let embedding_key = std::env::var(&config.embedding.api_key_env).map_err(|_| {
            CoreError::Config(format!("missing env: {}", config.embedding.api_key_env))
        })?;
        let llm_key = std::env::var(&config.llm.api_key_env)
            .map_err(|_| CoreError::Config(format!("missing env: {}", config.llm.api_key_env)))?;

        let inner: Arc<dyn EmbeddingProvider> = Arc::new(OpenAiCompatibleEmbed::new(
            &config.embedding.base_url,
            &embedding_key,
            &config.embedding.model,
            config.embedding.dim,
        ));
        // Wrap inner with CachedEmbedder so all embed() calls consult emb_cache.
        let cache = Arc::new(CachedEmbedder::new(
            inner,
            conn.clone(),
            &config.embedding.model,
            config.cache.warn_rows,
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
            cache,
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
        // Wrap embedder in CachedEmbedder using the shared connection so
        // tests exercise the same code path as production.
        let cache = Arc::new(CachedEmbedder::new(
            embedder,
            entries.conn_for_test(),
            embedding_model.as_str(),
            100_000,
        ));
        Self {
            entries,
            links,
            events,
            chunks,
            cache,
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
    ///
    /// Custom handlers appear in MCP `tools/list` automatically.
    #[allow(dead_code)] // lib-mode extension point; binary daemon doesn't call this
    pub fn register_handler(&mut self, handler: Arc<dyn RpcHandler>) {
        self.handlers.insert(handler.method(), handler);
    }

    // --- Public accessors for custom RpcHandler implementations ---
    // Binary daemon doesn't call these; they exist for lib-mode users
    // who implement RpcHandler and need access to services via &Daemon.

    #[allow(dead_code)]
    pub fn entries(&self) -> &Arc<EntryService> {
        &self.entries
    }
    #[allow(dead_code)]
    pub fn links(&self) -> &Arc<LinkService> {
        &self.links
    }
    #[allow(dead_code)]
    pub fn events(&self) -> &Arc<EventService> {
        &self.events
    }
    #[allow(dead_code)]
    pub fn chunks(&self) -> &Arc<ChunkService> {
        &self.chunks
    }
    /// Access the cached embedding provider. Trait methods (`embed`, `dim`,
    /// `name`) delegate transparently to the inner provider; the concrete
    /// `CachedEmbedder` type exposes `stats()` and `clear()` for cache RPCs.
    #[allow(dead_code)]
    pub fn cache(&self) -> &Arc<CachedEmbedder> {
        &self.cache
    }
    #[allow(dead_code)]
    pub fn llm(&self) -> &Arc<dyn LlmProvider> {
        &self.llm
    }

    /// Construct a Daemon from pre-built services + providers, without
    /// reading a config.toml file. For lib-mode users who construct their
    /// own EntryService / EmbeddingProvider / LlmProvider.
    ///
    /// The caller supplies the FS-backed `ContentStore` so it can be shared
    /// with any other services they manage outside the daemon. The
    /// `cache_model` name namespacing the `emb_cache` rows; pass the
    /// underlying model identifier so cache stats and clears target the
    /// right namespace. `warn_rows` is the soft capacity threshold at which
    /// `cache.stats` starts returning `warning: true` (use `100_000` as a
    /// sensible default).
    #[allow(dead_code)]
    pub fn from_services(
        conn: Arc<std::sync::Mutex<Connection>>,
        content_store: Arc<ContentStore>,
        embedder: Arc<dyn EmbeddingProvider>,
        llm: Arc<dyn LlmProvider>,
        embedding_dim: usize,
        cache_model: impl Into<String>,
        warn_rows: u64,
    ) -> Result<Self, CoreError> {
        let entries = Arc::new(EntryService::new(conn.clone(), content_store)?);
        let links = Arc::new(LinkService::new(conn.clone())?);
        let events = Arc::new(EventService::new(conn.clone())?);
        let chunks = Arc::new(ChunkService::new(conn.clone())?);
        chunks.ensure_vec_chunk_embeddings(embedding_dim)?;
        let cache = Arc::new(CachedEmbedder::new(embedder, conn, cache_model, warn_rows));
        let handlers = crate::handlers::registry();
        Ok(Self {
            entries,
            links,
            events,
            chunks,
            cache,
            llm,
            embedding_model: String::new(),
            llm_model: String::new(),
            embedding_dim,
            handlers,
        })
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

/// Resolve `knowledge_root` (FS-backed content storage root): expand `~` to
/// `$HOME` and `create_dir_all` the path. Unlike `expand_db_path`, the path
/// itself is the storage directory (not a file with a parent dir).
fn expand_knowledge_root(path: &std::path::Path) -> Result<std::path::PathBuf, CoreError> {
    let s = path.to_string_lossy();
    let expanded = if s.starts_with('~') {
        let home = std::env::var("HOME")
            .map(std::path::PathBuf::from)
            .map_err(|_| CoreError::Config("HOME not set; cannot expand ~".into()))?;
        home.join(path.strip_prefix("~").unwrap_or(path))
    } else {
        path.to_path_buf()
    };
    std::fs::create_dir_all(&expanded)
        .map_err(|e| CoreError::Config(format!("create knowledge_root dir: {e}")))?;
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

    #[test]
    fn expand_knowledge_root_creates_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("x/y/z/store");
        let expanded = expand_knowledge_root(&nested).unwrap();
        assert!(expanded.exists());
        assert!(expanded.is_dir());
    }
}
