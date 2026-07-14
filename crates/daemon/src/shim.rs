//! Stdio↔socket shim. CC spawns this binary with no args; it attaches to the
//! resident daemon over a platform transport (Unix socket / loopback TCP),
//! spawning one if absent, then bridges
//! stdin/stdout to the socket byte-for-byte. If the resident daemon can't be
//! reached/started, the caller falls back to in-process stdio serve.

use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

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
) -> Result<crate::socket::DaemonStream, SpawnError>
where
    F: FnOnce() -> std::io::Result<()>,
{
    if let Ok(stream) = crate::socket::connect(socket_path).await {
        return Ok(stream);
    }
    spawn().map_err(SpawnError::SpawnFailed)?;

    let deadline = tokio::time::Instant::now() + ready_timeout;
    loop {
        if let Ok(stream) = crate::socket::connect(socket_path).await {
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
/// feature needed.
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

/// Pump bytes between stdio and the socket in both directions until both
/// halves close. Generic over the stdio halves so it's unit-testable with
/// in-memory pipes; production wires it to `tokio::io::stdin()` / `stdout()`.
///
/// Half-close semantics: when stdin reaches EOF, the socket write half is
/// shut down so the resident daemon's connection handler observes EOF,
/// finishes draining, and closes its write half. Without this the daemon
/// keeps the socket open (idle) and the shim never exits — a half-close
/// deadlock. The socket→stdout pump then drains the daemon's final response
/// and returns once the daemon closes its end.
pub async fn bridge<R, W>(
    stdin: R,
    stdout: W,
    stream: crate::socket::DaemonStream,
) -> Result<(), CoreError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    match stream {
        #[cfg(unix)]
        crate::socket::DaemonStream::Unix(stream) => bridge_stream(stdin, stdout, stream).await,
        #[cfg(windows)]
        crate::socket::DaemonStream::Tcp(stream) => bridge_stream(stdin, stdout, stream).await,
    }
}

async fn bridge_stream<R, W, S>(mut stdin: R, mut stdout: W, stream: S) -> Result<(), CoreError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    let (mut sock_read, mut sock_write) = tokio::io::split(stream);
    // stdin → socket; on EOF, shut the write half so the daemon sees EOF.
    // A copy error here (e.g. the daemon RSTs mid-stream) is non-fatal — the
    // other pump still drains — but is logged so the shim never exits silent.
    let to_socket = async {
        if let Err(e) = tokio::io::copy(&mut stdin, &mut sock_write).await {
            eprintln!("nomai-shim: bridge stdin→socket copy error: {e}");
        }
        let _ = sock_write.shutdown().await;
    };
    // socket → stdout (drains until the daemon closes its write half).
    let from_socket = async {
        if let Err(e) = tokio::io::copy(&mut sock_read, &mut stdout).await {
            eprintln!("nomai-shim: bridge socket→stdout copy error: {e}");
        }
    };
    let _ = tokio::join!(to_socket, from_socket);
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

    #[cfg(unix)]
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

    #[cfg(unix)]
    #[tokio::test]
    async fn bridge_pumps_both_directions_until_eof() {
        let (a, b) = tokio::net::UnixStream::pair().unwrap();
        let (mut stdin_w, stdin_r) = tokio::io::duplex(64);
        let (stdout_w, mut stdout_r) = tokio::io::duplex(64);

        let bridge_task = tokio::spawn(bridge(
            stdin_r,
            stdout_w,
            crate::socket::DaemonStream::Unix(a),
        ));

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

    /// An `AsyncRead` whose every `poll_read` fails — stands in for a stdio
    /// half whose underlying copy blows up (e.g. the daemon RSTs mid-stream).
    #[cfg(unix)]
    struct ErrorReader;
    #[cfg(unix)]
    impl tokio::io::AsyncRead for ErrorReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("stdin copy boom")))
        }
    }

    /// Contract guard for issue #3 item ②: a copy error in ONE direction must
    /// not propagate, panic, or starve the other direction — `bridge` still
    /// drains the daemon's response and returns Ok. (The accompanying
    /// `eprintln!` diagnostic is a stderr side-effect this suite can't capture;
    /// the error *isolation* is the testable contract.)
    #[cfg(unix)]
    #[tokio::test]
    async fn bridge_keeps_draining_socket_when_stdin_copy_errors() {
        let (a, b) = tokio::net::UnixStream::pair().unwrap();
        let (stdout_w, mut stdout_r) = tokio::io::duplex(64);

        let bridge_task = tokio::spawn(bridge(
            ErrorReader,
            stdout_w,
            crate::socket::DaemonStream::Unix(a),
        ));

        // Peer (daemon side): emit a response, then close → from_socket EOFs.
        let peer = tokio::spawn(async move {
            let (_, mut write_half) = b.into_split();
            write_half.write_all(b"response\n").await.unwrap();
            write_half.flush().await.unwrap();
        });

        let mut got = String::new();
        let mut sr = BufReader::new(&mut stdout_r);
        sr.read_line(&mut got).await.unwrap();
        assert_eq!(got, "response\n");

        peer.await.unwrap();
        drop(stdout_r);
        let outcome = bridge_task.await.unwrap();
        assert!(
            outcome.is_ok(),
            "bridge must not propagate copy errors: {outcome:?}"
        );
    }
}
