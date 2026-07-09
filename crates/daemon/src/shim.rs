#![cfg(unix)]
//! Stdio↔socket shim. CC spawns this binary with no args; it attaches to the
//! resident daemon over a Unix socket (spawning one if absent), then bridges
//! stdin/stdout to the socket byte-for-byte. If the resident daemon can't be
//! reached/started, the caller falls back to in-process stdio serve.

use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UnixStream;

use nomai_core::CoreError;

const READY_POLL: Duration = Duration::from_millis(50);
/// Default ready timeout for `ensure_daemon` in `shim::run`.
const READY_TIMEOUT: Duration = Duration::from_secs(2);

/// Failure to attach a resident daemon via `ensure_daemon`.
#[derive(Debug)]
pub enum SpawnError {
    /// Spawning the `--serve` child failed.
    #[allow(dead_code)] // surfaced via Debug in `shim::run`'s fallback message
    SpawnFailed(std::io::Error),
    /// The child was spawned but never became ready within the timeout.
    NotReady,
}

/// Attach to the resident daemon at `socket_path`. If nothing is listening,
/// invoke `spawn` (which launches a `--serve` child) and poll until ready
/// (or `ready_timeout`). `spawn` is injected so tests can stub it without
/// spawning a real `--serve`.
pub async fn ensure_daemon<F>(
    socket_path: &std::path::Path,
    ready_timeout: Duration,
    spawn: F,
) -> Result<UnixStream, SpawnError>
where
    F: FnOnce() -> std::io::Result<()>,
{
    if let Ok(stream) = UnixStream::connect(socket_path).await {
        return Ok(stream);
    }
    spawn().map_err(SpawnError::SpawnFailed)?;

    let deadline = tokio::time::Instant::now() + ready_timeout;
    loop {
        if let Ok(stream) = UnixStream::connect(socket_path).await {
            return Ok(stream);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(SpawnError::NotReady);
        }
        tokio::time::sleep(READY_POLL).await;
    }
}

/// Spawn `nomai-daemon --serve --config <NOMAI_CONFIG_PATH>` detached. Uses
/// `std::process` (a one-shot spawn; we never wait) — no tokio `process`
/// feature needed. The child detaches itself via `serve::run`'s `setsid`.
pub fn spawn_serve() -> std::io::Result<()> {
    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("--serve");
    if let Some(cfg) = std::env::var_os("NOMAI_CONFIG_PATH") {
        cmd.arg("--config").arg(cfg);
    }
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    // Drop the child handle: we never wait. serve detaches via setsid, so it
    // survives this (shim) process exiting.
    let _child = cmd.spawn()?;
    Ok(())
}

/// Pump bytes between stdio and the socket in both directions until either
/// side hits EOF. Generic over the stdio halves so it's unit-testable with
/// in-memory pipes; production wires it to `tokio::io::stdin()` / `stdout()`.
pub async fn bridge<R, W>(mut stdin: R, mut stdout: W, stream: UnixStream) -> Result<(), CoreError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (mut sock_read, mut sock_write) = stream.into_split();
    let to_socket = tokio::io::copy(&mut stdin, &mut sock_write);
    let from_socket = tokio::io::copy(&mut sock_read, &mut stdout);
    // Either direction completing (EOF/error) ends the bridge.
    let _ = tokio::try_join!(to_socket, from_socket);
    Ok(())
}

use crate::config::Config;
use crate::daemon::Daemon;

/// Shim entry point: attach to (or spawn) the resident daemon and bridge
/// stdio to it; fall back to in-process stdio serve if unreachable.
pub async fn run(config: Config) -> Result<(), CoreError> {
    let db_path = crate::daemon::resolved_db_path(&config)?;
    let (socket_path, _pidfile) = crate::socket::socket_paths(&db_path)
        .map_err(|e| CoreError::Config(format!("socket paths: {e}")))?;

    match ensure_daemon(&socket_path, READY_TIMEOUT, spawn_serve).await {
        Ok(stream) => bridge(tokio::io::stdin(), tokio::io::stdout(), stream).await,
        Err(e) => {
            eprintln!("nomai-shim: resident daemon unavailable ({e:?}); falling back to stdio");
            let daemon = Daemon::new(config).await?;
            daemon.run_stdio().await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn ensure_daemon_connects_if_already_listening() {
        let sock =
            std::env::temp_dir().join(format!("nomai-shim-listen-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        let _listener = tokio::net::UnixListener::bind(&sock).unwrap();

        // Pass a spawn stub; connect succeeds first so it must NOT run.
        let outcome = ensure_daemon(&sock, Duration::from_secs(1), || Ok(())).await;
        assert!(outcome.is_ok(), "should connect to live listener");
        let _ = std::fs::remove_file(&sock);
    }

    #[tokio::test]
    async fn ensure_daemon_spawns_then_connects_when_listener_appears() {
        let sock =
            std::env::temp_dir().join(format!("nomai-shim-spawn-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);

        // Simulate the spawned daemon coming up: when `ensure_daemon` invokes
        // our `spawn` closure, bind the listener shortly after. The listener
        // is held for the life of the task (a dropped listener accepts no
        // connects).
        let sock_clone = sock.clone();
        let spawn = move || {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _listener = tokio::net::UnixListener::bind(&sock_clone).unwrap();
                std::future::pending::<()>().await;
            });
            Ok(())
        };

        let outcome = ensure_daemon(&sock, Duration::from_secs(2), spawn).await;
        assert!(outcome.is_ok(), "should connect after spawn+poll");
        let _ = std::fs::remove_file(&sock);
    }

    #[tokio::test]
    async fn ensure_daemon_returns_not_ready_on_timeout() {
        let sock =
            std::env::temp_dir().join(format!("nomai-shim-timeout-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        // Nothing ever listens; spawn is a no-op.
        let outcome = ensure_daemon(&sock, Duration::from_millis(120), || Ok(())).await;
        assert!(matches!(outcome, Err(SpawnError::NotReady)));
        let _ = std::fs::remove_file(&sock);
    }

    #[tokio::test]
    async fn bridge_pumps_both_directions_until_eof() {
        let (a, b) = tokio::net::UnixStream::pair().unwrap();
        let (mut stdin_w, stdin_r) = tokio::io::duplex(64);
        let (stdout_w, mut stdout_r) = tokio::io::duplex(64);

        let bridge_task = tokio::spawn(bridge(stdin_r, stdout_w, a));

        // Peer: read "ping" from socket, echo "pong" back.
        let peer = tokio::spawn(async move {
            let (read_half, mut write_half) = b.into_split();
            let mut reader = BufReader::new(read_half);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(line, "ping\n");
            write_half.write_all(b"pong\n").await.unwrap();
            write_half.flush().await.unwrap();
        });

        stdin_w.write_all(b"ping\n").await.unwrap();
        stdin_w.flush().await.unwrap();

        let mut got = String::new();
        let mut sr = BufReader::new(&mut stdout_r);
        sr.read_line(&mut got).await.unwrap();
        assert_eq!(got, "pong\n");

        peer.await.unwrap();
        drop(stdin_w);
        drop(stdout_r);
        let _ = bridge_task.await;
    }
}
