//! Daemon: owns EntryService + providers; orchestrates RPC handlers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, params};

use nomai_core::{
    ChunkService, ContentStore, CoreError, EntryService, EventService, LinkService,
    chunk_model::DimReconciliation,
};
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
    /// In-memory search-results cache (Spec 7). Bumped on every mutation
    /// that affects search results; see `search_cache::SearchCache`.
    pub(crate) search_cache: Arc<crate::search_cache::SearchCache>,
    pub(crate) llm: Arc<dyn LlmProvider>,
    pub(crate) embedding_model: String,
    pub(crate) llm_model: String,
    // Used by search.semantic (Task 7); keep despite no current reader.
    #[allow(dead_code)]
    pub(crate) embedding_dim: usize,
    /// Configured chunk target size (characters). Stored for symmetry with
    /// `embedding_dim` and future introspection RPCs; the value has already
    /// been baked into the `EntryService` block_service at construction.
    #[allow(dead_code)]
    pub(crate) chunk_target_size: usize,
    pub(crate) handlers: HashMap<&'static str, Arc<dyn RpcHandler>>,
}

impl Daemon {
    pub async fn new(config: Config) -> Result<Self, CoreError> {
        // Open SQLite (creating parent dir if needed).
        let db_path = expand_db_path(&config.data.db_path)?;
        let conn = Connection::open(&db_path)?;
        // Defensive: single-daemon model won't hit SQLITE_BUSY in-process, but
        // if an external `sqlite3` CLI ever opens the db concurrently, wait up
        // to 5s rather than failing immediately. Spec §5.
        conn.pragma_update(None, "busy_timeout", 5000_u32)?;
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
        let chunk_target_size = config.chunking.target_size;
        eprintln!("info: chunk_target_size={chunk_target_size} chars");
        let entries = Arc::new(EntryService::new(
            conn.clone(),
            content_store,
            chunk_target_size,
        )?);
        let links = Arc::new(LinkService::new(conn.clone())?);
        let events = Arc::new(EventService::new(conn.clone())?);
        let chunks = Arc::new(ChunkService::new(conn.clone())?);
        let dim_result = chunks.ensure_vec_chunk_embeddings(config.embedding.dim)?;
        match dim_result {
            DimReconciliation::Created { dim } => {
                eprintln!("info: created vec_chunk_embeddings with dim={dim}");
            }
            DimReconciliation::Consistent { dim: _ } => {
                // Quiet — table already matches; nothing to report at boot.
            }
            DimReconciliation::Recreated { from, to } => {
                eprintln!(
                    "warn: recreated vec_chunk_embeddings (dim {from} → {to}); \
                     embeddings will re-populate from emb_cache on next search"
                );
            }
        }

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

        // Spec §9.1: sync FS → index at startup. Best-effort; failures log
        // warning to stderr and do not abort the boot (single-user tool; an
        // empty/STALE index still lets the daemon serve RPCs, and a later
        // `index.sync` or `index.rebuild` call can recover).
        run_startup_sync(&entries);

        Ok(Self {
            entries,
            links,
            events,
            chunks,
            cache,
            search_cache: Arc::new(crate::search_cache::SearchCache::new()),
            llm,
            embedding_model: config.embedding.model,
            llm_model: config.llm.model,
            embedding_dim: config.embedding.dim,
            chunk_target_size,
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
        chunk_target_size: usize,
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
        // Mirror Daemon::new: run startup FS→index sync so a test that
        // pre-populates the content store sees the entries indexed.
        run_startup_sync(&entries);
        Self {
            entries,
            links,
            events,
            chunks,
            cache,
            search_cache: Arc::new(crate::search_cache::SearchCache::new()),
            llm,
            embedding_model,
            llm_model,
            embedding_dim,
            chunk_target_size,
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
    #[allow(dead_code, clippy::too_many_arguments)]
    pub fn from_services(
        conn: Arc<std::sync::Mutex<Connection>>,
        content_store: Arc<ContentStore>,
        embedder: Arc<dyn EmbeddingProvider>,
        llm: Arc<dyn LlmProvider>,
        embedding_dim: usize,
        chunk_target_size: usize,
        cache_model: impl Into<String>,
        warn_rows: u64,
    ) -> Result<Self, CoreError> {
        let entries = Arc::new(EntryService::new(
            conn.clone(),
            content_store,
            chunk_target_size,
        )?);
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
            search_cache: Arc::new(crate::search_cache::SearchCache::new()),
            llm,
            embedding_model: String::new(),
            llm_model: String::new(),
            embedding_dim,
            chunk_target_size,
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

    /// Run the resident daemon: accept multiple Unix-socket clients and
    /// dispatch their NDJSON JSON-RPC lines via the same `dispatch` as
    /// `run_stdio`, writing responses back to **each connection**. Exits when
    /// either (a) `idle_timeout` elapses with zero active connections, or
    /// (b) SIGTERM / SIGINT arrives. Spec §5.
    #[cfg(unix)]
    pub async fn run_serve(
        self,
        listener: tokio::net::UnixListener,
        idle_timeout: std::time::Duration,
    ) -> Result<(), CoreError> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::{Notify, broadcast};

        let daemon = Arc::new(self);
        let active = Arc::new(AtomicUsize::new(0));
        let idle_notify = Arc::new(Notify::new());
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1);

        // Signal watcher: forward ctrl_c / SIGTERM to the shutdown channel.
        let sig_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm_signal() => {}
            }
            let _ = sig_tx.send(());
        });

        loop {
            let active_now = active.load(Ordering::SeqCst);
            let idle_fut = async {
                if active_now == 0 {
                    tokio::time::sleep(idle_timeout).await;
                } else {
                    idle_notify.notified().await;
                }
            };

            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => break,
                res = listener.accept() => {
                    let (stream, _addr) =
                        res.map_err(|e| CoreError::Config(format!("accept: {e}")))?;
                    active.fetch_add(1, Ordering::SeqCst);
                    let daemon_c = Arc::clone(&daemon);
                    let active_c = Arc::clone(&active);
                    let notify_c = Arc::clone(&idle_notify);
                    tokio::spawn(async move {
                        handle_conn(daemon_c, stream).await;
                        if active_c.fetch_sub(1, Ordering::SeqCst) == 1 {
                            notify_c.notify_one();
                        }
                    });
                }
                _ = idle_fut => {
                    // Re-check: a client may have arrived in the same window.
                    if active.load(Ordering::SeqCst) == 0 {
                        break;
                    }
                }
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

/// Builder for `Daemon`. Spec 8 Plan 2 / F-lib-2: provides a fluent
/// alternative to `Daemon::from_services`'s 8 positional arguments.
/// `Daemon::from_services` is kept for backward compatibility.
///
/// All fields are required; `build()` returns `Err(CoreError::Config)` if any
/// field is unset (caught by `Option::ok_or`).
#[allow(dead_code)] // lib-mode extension point; binary daemon doesn't use this
pub struct DaemonBuilder {
    conn: Option<Arc<std::sync::Mutex<Connection>>>,
    content_store: Option<Arc<ContentStore>>,
    embedder: Option<Arc<dyn EmbeddingProvider>>,
    llm: Option<Arc<dyn LlmProvider>>,
    embedding_dim: Option<usize>,
    chunk_target_size: Option<usize>,
    cache_model: Option<String>,
    warn_rows: Option<u64>,
}

#[allow(dead_code)] // lib-mode extension point; binary daemon doesn't use this
impl DaemonBuilder {
    pub fn new() -> Self {
        Self {
            conn: None,
            content_store: None,
            embedder: None,
            llm: None,
            embedding_dim: None,
            chunk_target_size: None,
            cache_model: None,
            warn_rows: None,
        }
    }

    pub fn conn(mut self, v: Arc<std::sync::Mutex<Connection>>) -> Self {
        self.conn = Some(v);
        self
    }
    pub fn content_store(mut self, v: Arc<ContentStore>) -> Self {
        self.content_store = Some(v);
        self
    }
    pub fn embedder(mut self, v: Arc<dyn EmbeddingProvider>) -> Self {
        self.embedder = Some(v);
        self
    }
    pub fn llm(mut self, v: Arc<dyn LlmProvider>) -> Self {
        self.llm = Some(v);
        self
    }
    pub fn embedding_dim(mut self, v: usize) -> Self {
        self.embedding_dim = Some(v);
        self
    }
    pub fn chunk_target_size(mut self, v: usize) -> Self {
        self.chunk_target_size = Some(v);
        self
    }
    pub fn cache_model(mut self, v: impl Into<String>) -> Self {
        self.cache_model = Some(v.into());
        self
    }
    pub fn warn_rows(mut self, v: u64) -> Self {
        self.warn_rows = Some(v);
        self
    }

    pub fn build(self) -> Result<Daemon, CoreError> {
        Daemon::from_services(
            self.conn
                .ok_or_else(|| CoreError::Config("DaemonBuilder: conn required".into()))?,
            self.content_store
                .ok_or_else(|| CoreError::Config("DaemonBuilder: content_store required".into()))?,
            self.embedder
                .ok_or_else(|| CoreError::Config("DaemonBuilder: embedder required".into()))?,
            self.llm
                .ok_or_else(|| CoreError::Config("DaemonBuilder: llm required".into()))?,
            self.embedding_dim
                .ok_or_else(|| CoreError::Config("DaemonBuilder: embedding_dim required".into()))?,
            self.chunk_target_size.ok_or_else(|| {
                CoreError::Config("DaemonBuilder: chunk_target_size required".into())
            })?,
            self.cache_model
                .ok_or_else(|| CoreError::Config("DaemonBuilder: cache_model required".into()))?,
            self.warn_rows
                .ok_or_else(|| CoreError::Config("DaemonBuilder: warn_rows required".into()))?,
        )
    }
}

impl Default for DaemonBuilder {
    fn default() -> Self {
        Self::new()
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

/// Spec §9.1: best-effort FS→index reconciliation at daemon startup. Logs
/// counts to stderr when the sync made changes, and emits an `index.synced`
/// event into the events log so `events.list` consumers can observe boot
/// reconciliations. Failures are swallowed: a stale/empty index still lets
/// the daemon serve RPCs, and a later `index.sync` / `index.rebuild` call
/// can recover. Shared between `Daemon::new` (production) and `for_test`
/// (tests) so both paths exercise the same boot contract.
///
/// Plan 6 Task 5: the `index.synced` event is emitted only when the boot
/// scan actually changed something (`added + updated + removed > 0`). A
/// quiet boot — empty FS, or an FS already matching the index — produces
/// no event so the audit log does not grow on every restart.
fn run_startup_sync(entries: &EntryService) {
    let sync_result = entries.sync_from_fs().unwrap_or_else(|e| {
        eprintln!("warn: startup index.sync failed: {e}");
        nomai_core::SyncResult::default()
    });
    if sync_result.added > 0 || sync_result.updated > 0 || sync_result.removed > 0 {
        eprintln!(
            "info: index.synced: +{} ~{} -{} ({} unchanged)",
            sync_result.added, sync_result.updated, sync_result.removed, sync_result.unchanged
        );
        // Emit `index.synced` event. Best-effort: a write failure (e.g.
        // events table missing) only means we lose the audit row, not the
        // sync itself. The events table is created by V6+ migrations which
        // EntryService::new has already applied by this point. `target_id`
        // is the all-zero ULID — EventService parses every events.target_id
        // as a ULID, and system-level events have no natural target.
        let conn = entries.conn_for_test();
        if let Ok(guard) = conn.lock() {
            let payload = serde_json::to_string(&sync_result).unwrap_or_default();
            let _ = guard.execute(
                "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    ulid::Ulid::new().to_string(),
                    "index.synced",
                    "system",
                    "00000000000000000000000000",
                    payload,
                    chrono::Utc::now().to_rfc3339(),
                ],
            );
        }
    }
}

/// Serve one client connection: read NDJSON requests, dispatch, write the
/// response back to the stream. Exits on EOF or read error.
#[cfg(unix)]
async fn handle_conn(daemon: Arc<Daemon>, stream: tokio::net::UnixStream) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(_) => break,
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: nomai_protocol::Request = match serde_json::from_str(trimmed) {
            Ok(r) => r,
            Err(e) => {
                let resp = nomai_protocol::Response::err(
                    None,
                    nomai_protocol::RpcError {
                        code: nomai_protocol::error::PARSE_ERROR,
                        message: nomai_protocol::error::MESSAGE_PARSE_ERROR.into(),
                        data: Some(serde_json::Value::String(e.to_string())),
                    },
                );
                let _ = write_response_to(&mut write_half, &resp).await;
                continue;
            }
        };
        let is_notification = req.id.is_none();
        let resp = daemon.dispatch(req).await;
        if !is_notification {
            let _ = write_response_to(&mut write_half, &resp).await;
        }
    }
}

/// Serialize a response as one JSON line and write it to an async writer.
#[cfg(unix)]
async fn write_response_to<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    resp: &nomai_protocol::Response,
) -> Result<(), std::io::Error> {
    use tokio::io::AsyncWriteExt;
    // Our Response is always serializable; unwrap is an invariant assertion.
    let mut buf = serde_json::to_string(resp).expect("response serializes");
    buf.push('\n');
    writer.write_all(buf.as_bytes()).await
}

/// Block until SIGTERM is received. If the handler can't be installed, block
/// forever so ctrl_c (handled separately) remains the only signal path.
#[cfg(unix)]
async fn sigterm_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut s) => {
            s.recv().await;
        }
        Err(_) => std::future::pending::<()>().await,
    }
}

/// Resolved (tilde-expanded, parent-dir-created) db path from config. Used by
/// `serve::run` / `shim::run` to derive socket paths alongside the database.
#[allow(dead_code)] // consumed by Task 3 (serve::run) / Task 4 (shim::run).
pub(crate) fn resolved_db_path(
    config: &crate::config::Config,
) -> Result<std::path::PathBuf, CoreError> {
    expand_db_path(&config.data.db_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    use nomai_core::EntryService;
    use nomai_providers::{EmbeddingProvider, LlmProvider};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    async fn null_daemon() -> super::Daemon {
        let entries = Arc::new(EntryService::for_test().unwrap());
        let conn = entries.conn_for_test();
        let content_store = entries.content_store().clone();

        struct NullEmbed;
        #[async_trait::async_trait]
        impl EmbeddingProvider for NullEmbed {
            async fn embed(
                &self,
                _texts: &[&str],
            ) -> Result<Vec<Vec<f32>>, nomai_protocol::ProviderError> {
                Ok(vec![])
            }
            fn dim(&self) -> usize {
                8
            }
            fn name(&self) -> &str {
                "null"
            }
        }
        struct NullLlm;
        #[async_trait::async_trait]
        impl LlmProvider for NullLlm {
            async fn complete(
                &self,
                _req: nomai_providers::CompletionRequest,
            ) -> Result<nomai_providers::CompletionResponse, nomai_protocol::ProviderError>
            {
                Err(nomai_protocol::ProviderError::new(
                    nomai_protocol::ProviderErrorKind::Unknown,
                    "null",
                    None,
                ))
            }
            fn name(&self) -> &str {
                "null"
            }
        }

        super::DaemonBuilder::new()
            .conn(conn)
            .content_store(content_store)
            .embedder(Arc::new(NullEmbed))
            .llm(Arc::new(NullLlm))
            .embedding_dim(8)
            .chunk_target_size(1024)
            .cache_model("test-model")
            .warn_rows(100_000)
            .build()
            .unwrap()
    }

    fn req_line(id: u64, method: &str) -> String {
        format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"{method}","params":{{}}}}"#)
    }

    async fn read_one_response(
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    ) -> serde_json::Value {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    #[tokio::test]
    async fn run_serve_routes_responses_to_correct_clients() {
        let sock =
            std::env::temp_dir().join(format!("nomai-serve-route-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        let listener = tokio::net::UnixListener::bind(&sock).unwrap();
        let daemon = null_daemon().await;

        let serve_task = tokio::spawn(async move {
            daemon
                .run_serve(listener, std::time::Duration::from_secs(30))
                .await
                .unwrap();
        });

        // Two concurrent clients; each sends entry.list with a distinct id.
        let mut a = tokio::net::UnixStream::connect(&sock).await.unwrap();
        let mut b = tokio::net::UnixStream::connect(&sock).await.unwrap();
        a.write_all(req_line(10, "entry.list").as_bytes())
            .await
            .unwrap();
        a.write_all(b"\n").await.unwrap();
        b.write_all(req_line(20, "entry.list").as_bytes())
            .await
            .unwrap();
        b.write_all(b"\n").await.unwrap();

        let (ar, _aw) = a.into_split();
        let (br, _bw) = b.into_split();
        let mut ar = BufReader::new(ar);
        let mut br = BufReader::new(br);
        let ra = tokio::spawn(async move { read_one_response(&mut ar).await });
        let rb = tokio::spawn(async move { read_one_response(&mut br).await });
        let va = ra.await.unwrap();
        let vb = rb.await.unwrap();
        // entry.list returns an array; each client gets its own id back.
        assert_eq!(va["id"], 10);
        assert_eq!(vb["id"], 20);

        serve_task.abort();
        let _ = std::fs::remove_file(&sock);
    }

    #[tokio::test]
    async fn run_serve_exits_after_idle_timeout() {
        let sock =
            std::env::temp_dir().join(format!("nomai-serve-idle-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        let listener = tokio::net::UnixListener::bind(&sock).unwrap();
        let daemon = null_daemon().await;

        // No client ever connects; idle_timeout = 80ms → run_serve returns.
        let start = std::time::Instant::now();
        daemon
            .run_serve(listener, std::time::Duration::from_millis(80))
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= std::time::Duration::from_millis(80));
        assert!(elapsed < std::time::Duration::from_secs(2));
        let _ = std::fs::remove_file(&sock);
    }

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

    #[tokio::test]
    async fn daemon_builder_constructs_with_all_fields_set() {
        use nomai_providers::{EmbeddingProvider, LlmProvider};

        // Reuse EntryService::for_test for sqlite-vec init + migrations.
        // We pull the conn + content_store back out via public accessors.
        let entries = Arc::new(EntryService::for_test().unwrap());
        let conn = entries.conn_for_test();
        let content_store = entries.content_store().clone();

        struct NullEmbed;
        #[async_trait::async_trait]
        impl EmbeddingProvider for NullEmbed {
            async fn embed(
                &self,
                _texts: &[&str],
            ) -> Result<Vec<Vec<f32>>, nomai_protocol::ProviderError> {
                Ok(vec![])
            }
            fn dim(&self) -> usize {
                8
            }
            fn name(&self) -> &str {
                "null"
            }
        }
        struct NullLlm;
        #[async_trait::async_trait]
        impl LlmProvider for NullLlm {
            async fn complete(
                &self,
                _req: nomai_providers::CompletionRequest,
            ) -> Result<nomai_providers::CompletionResponse, nomai_protocol::ProviderError>
            {
                Err(nomai_protocol::ProviderError::new(
                    nomai_protocol::ProviderErrorKind::Unknown,
                    "null",
                    None,
                ))
            }
            fn name(&self) -> &str {
                "null"
            }
        }

        let daemon = DaemonBuilder::new()
            .conn(conn)
            .content_store(content_store)
            .embedder(Arc::new(NullEmbed))
            .llm(Arc::new(NullLlm))
            .embedding_dim(8)
            .chunk_target_size(1024)
            .cache_model("test-model")
            .warn_rows(100_000)
            .build()
            .unwrap();

        // Sanity: handlers registry is populated (same as from_services path).
        assert!(daemon.handlers.contains_key("entry.create"));
    }

    #[test]
    fn daemon_builder_errors_when_required_field_missing() {
        // Daemon: !Debug, so we cannot use Result::unwrap_err. Match instead.
        let result = DaemonBuilder::new().build();
        match result {
            Ok(_) => panic!("expected Config error, got Ok(Daemon)"),
            Err(err) => {
                assert!(matches!(err, nomai_core::CoreError::Config(_)));
                assert!(err.to_string().contains("conn required"));
            }
        }
    }
}
