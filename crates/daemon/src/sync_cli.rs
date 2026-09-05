//! `nomai-daemon --sync` / `--sync-init <url>`: thin client that connects to
//! the resident daemon over its socket transport and dispatches a single
//! sync RPC. All git work happens in the daemon's sync handlers;
//! this module only ferries one NDJSON request and reads one NDJSON response.
//!
//! Transparent wrapper: reuses `shim::ensure_daemon` (which spawns `--serve`
//! if no resident daemon is listening) and the existing `DaemonStream`
//! transport. No git invocations happen here.

use std::time::Duration;

use nomai_core::CoreError;
use nomai_protocol::{Id, JSONRPC_VERSION, Request, Response, method::sync};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::config::Config;
use crate::socket::{self, DaemonStream};

/// Which sync RPC to dispatch.
pub enum SyncCmd {
    /// `sync.init`: configure `knowledge_root` as a git repo for multi-device
    /// sync. `remote` is the git URL; `branch` overrides the default `main`.
    Init {
        remote: String,
        branch: Option<String>,
    },
    /// `sync.run`: commit local changes, pull --rebase, push, reindex.
    Run,
}

/// Connect to the resident daemon (spawning `--serve` if absent) and dispatch
/// one sync RPC. Prints the JSON `result` to stdout on success, or the JSON
/// `error` to stderr on failure.
///
/// Never panics: connect / IO / parse failures map to `CoreError`; the CLI
/// exits non-zero via `main`'s error → `ExitCode::FAILURE` path.
pub async fn run(config: Config, cmd: SyncCmd) -> Result<(), CoreError> {
    let db_path = crate::daemon::resolved_db_path(&config)?;
    let (socket_path, _pidfile) = socket::socket_paths(&db_path)
        .map_err(|e| CoreError::Config(format!("socket paths: {e}")))?;
    let mut stream = ensure_connected(&socket_path).await?;

    let (method, params) = match &cmd {
        SyncCmd::Init { remote, branch } => (
            sync::INIT,
            match branch {
                Some(b) => json!({ "remote": remote, "branch": b }),
                None => json!({ "remote": remote }),
            },
        ),
        SyncCmd::Run => (sync::RUN, json!({})),
    };
    let req = Request {
        jsonrpc: JSONRPC_VERSION.into(),
        id: Some(Id::Number(1)),
        method: method.into(),
        params: Some(params),
    };
    let line = serde_json::to_string(&req)
        .map_err(|e| CoreError::Config(format!("serialize request: {e}")))?;

    // Framing: the daemon's serve loop (`handle_conn_halves`) reads NDJSON
    // requests in a loop until EOF and keeps the connection open for multiple
    // requests. We send exactly one request line, then half-close the write
    // side so the daemon drains our request, writes its one response, and
    // observes EOF on its next `read_line` (closing its write half cleanly).
    stream.write_all((line + "\n").as_bytes()).await?;
    stream.shutdown().await?;

    // Read exactly one response line. `DaemonStream` forwards AsyncRead to
    // the inner platform stream (see `socket.rs`).
    let mut reader = BufReader::new(&mut stream);
    let mut buf = String::new();
    reader.read_line(&mut buf).await?;
    let resp: Response = serde_json::from_str(&buf)
        .map_err(|e| CoreError::Config(format!("parse response: {e}")))?;

    match resp.result {
        Some(v) => {
            println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            Ok(())
        }
        None => {
            eprintln!(
                "sync error: {}",
                resp.error
                    .map(|e| serde_json::to_string(&e).unwrap_or_default())
                    .unwrap_or_default()
            );
            Err(CoreError::Validation("sync RPC returned error".into()))
        }
    }
}

/// Attach to the resident daemon (spawning `--serve` if absent) and return a
/// connected `DaemonStream`. Reuses `shim::ensure_daemon` with
/// `shim::spawn_serve` as the spawn strategy.
async fn ensure_connected(socket_path: &std::path::Path) -> Result<DaemonStream, CoreError> {
    crate::shim::ensure_daemon(
        socket_path,
        Duration::from_secs(15),
        crate::shim::spawn_serve,
    )
    .await
    .map_err(|e| CoreError::Config(format!("attach resident daemon: {e:?}")))
}
