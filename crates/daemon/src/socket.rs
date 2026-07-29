//! Transport lifecycle primitives for the resident `--serve` daemon.
//! Unix uses a Unix domain socket; Windows uses a loopback TCP port derived
//! from the resolved database path.

use std::io;
use std::path::{Path, PathBuf};

/// Transport listener for the resident daemon.
pub enum DaemonListener {
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
    #[cfg(windows)]
    Tcp(tokio::net::TcpListener),
}

impl DaemonListener {
    pub async fn accept(&mut self) -> io::Result<DaemonStream> {
        match self {
            #[cfg(unix)]
            DaemonListener::Unix(listener) => {
                listener.accept().await.map(|(s, _)| DaemonStream::Unix(s))
            }
            #[cfg(windows)]
            DaemonListener::Tcp(listener) => {
                listener.accept().await.map(|(s, _)| DaemonStream::Tcp(s))
            }
        }
    }
}

/// Transport stream for client<->daemon bridging.
pub enum DaemonStream {
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    #[cfg(windows)]
    Tcp(tokio::net::TcpStream),
}

// Forward `AsyncRead` / `AsyncWrite` to the inner platform stream so callers
// (e.g. `sync_cli`) can use a `DaemonStream` directly for NDJSON line I/O
// without destructuring per platform. Both `UnixStream` and `TcpStream` are
// `Unpin`, so `DaemonStream` is auto-`Unpin`; `Pin::get_mut` is therefore
// available and the inner `Pin::new(s)` forwarding below is sound.
#[cfg(unix)]
impl tokio::io::AsyncRead for DaemonStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let Self::Unix(s) = self.get_mut();
        std::pin::Pin::new(s).poll_read(cx, buf)
    }
}

#[cfg(windows)]
impl tokio::io::AsyncRead for DaemonStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let Self::Tcp(s) = self.get_mut();
        std::pin::Pin::new(s).poll_read(cx, buf)
    }
}

#[cfg(unix)]
impl tokio::io::AsyncWrite for DaemonStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        let Self::Unix(s) = self.get_mut();
        std::pin::Pin::new(s).poll_write(cx, buf)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let Self::Unix(s) = self.get_mut();
        std::pin::Pin::new(s).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let Self::Unix(s) = self.get_mut();
        std::pin::Pin::new(s).poll_shutdown(cx)
    }
}

#[cfg(windows)]
impl tokio::io::AsyncWrite for DaemonStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        let Self::Tcp(s) = self.get_mut();
        std::pin::Pin::new(s).poll_write(cx, buf)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let Self::Tcp(s) = self.get_mut();
        std::pin::Pin::new(s).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let Self::Tcp(s) = self.get_mut();
        std::pin::Pin::new(s).poll_shutdown(cx)
    }
}

/// Derived `(socket, pidfile)` paths for a resolved `db_path`. Both live in
/// `<db_path.parent()>/run/` so the socket sits next to the database
/// (persistent, permission-controlled, single source of truth from config).
pub fn socket_paths(db_path: &Path) -> io::Result<(PathBuf, PathBuf)> {
    let run_dir = db_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("run");
    std::fs::create_dir_all(&run_dir)?;
    #[cfg(unix)]
    let socket = run_dir.join("nomai.sock");
    #[cfg(windows)]
    let socket = run_dir.join("nomai.port");
    Ok((socket, run_dir.join("nomai.pid")))
}

/// Outcome of attempting to claim the resident-daemon socket.
pub enum BindOutcome {
    /// We own the socket; ready to accept.
    ///
    /// The listener is consumed by `serve` (Task 2); until then the field is
    /// unread here, so we silence the dead-code lint at the variant level.
    #[allow(dead_code)]
    Bound(DaemonListener),
    /// A live daemon already holds it. Caller MUST exit (do NOT fall back to
    /// stdio — that reintroduces multi-process conflict).
    AlreadyRunning,
}

/// Bind with bind/connect double-probe arbitration.
///
/// - bind succeeds → own it.
/// - bind fails `EADDRINUSE` → `connect` to probe: success means a live
///   daemon (`AlreadyRunning`); failure means a stale leftover → `unlink` +
///   retry bind.
pub async fn bind_or_probe(socket_path: &Path) -> io::Result<BindOutcome> {
    #[cfg(unix)]
    {
        match tokio::net::UnixListener::bind(socket_path) {
            Ok(listener) => {
                restrict_perms(socket_path)?;
                Ok(BindOutcome::Bound(DaemonListener::Unix(listener)))
            }
            Err(first) if first.raw_os_error() == Some(libc::EADDRINUSE) => {
                match tokio::net::UnixStream::connect(socket_path).await {
                    Ok(_) => Ok(BindOutcome::AlreadyRunning),
                    Err(_) => {
                        std::fs::remove_file(socket_path)?;
                        let listener = tokio::net::UnixListener::bind(socket_path)?;
                        restrict_perms(socket_path)?;
                        Ok(BindOutcome::Bound(DaemonListener::Unix(listener)))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }
    #[cfg(windows)]
    {
        let addr = tcp_addr(socket_path);
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                write_addr_marker(socket_path, addr)?;
                Ok(BindOutcome::Bound(DaemonListener::Tcp(listener)))
            }
            Err(first) if first.kind() == io::ErrorKind::AddrInUse => {
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(_) => Ok(BindOutcome::AlreadyRunning),
                    Err(_) => {
                        let listener = tokio::net::TcpListener::bind(addr).await?;
                        write_addr_marker(socket_path, addr)?;
                        Ok(BindOutcome::Bound(DaemonListener::Tcp(listener)))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }
}

/// Connect to the resident daemon endpoint identified by `socket_path`.
pub async fn connect(socket_path: &Path) -> io::Result<DaemonStream> {
    #[cfg(unix)]
    {
        tokio::net::UnixStream::connect(socket_path)
            .await
            .map(DaemonStream::Unix)
    }
    #[cfg(windows)]
    {
        tokio::net::TcpStream::connect(tcp_addr(socket_path))
            .await
            .map(DaemonStream::Tcp)
    }
}

/// Constrain the socket to 0600 so other OS users can't connect.
fn restrict_perms(socket_path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
    }
    #[cfg(windows)]
    {
        let _ = socket_path;
        Ok(())
    }
}

/// Write our pid to the pidfile (diagnostic/cleanup aid only — NOT used for
/// arbitration; the socket double-probe is authoritative).
pub fn write_pidfile(pidfile: &Path) -> io::Result<()> {
    std::fs::write(pidfile, std::process::id().to_string())
}

/// Best-effort removal of socket + pidfile on shutdown / crash recovery.
pub fn cleanup(socket_path: &Path, pidfile: &Path) {
    let _ = std::fs::remove_file(socket_path);
    let _ = std::fs::remove_file(pidfile);
}

#[cfg(windows)]
fn tcp_addr(socket_path: &Path) -> std::net::SocketAddr {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    socket_path.to_string_lossy().hash(&mut hasher);
    let port = 40_000 + (hasher.finish() % 20_000) as u16;
    std::net::SocketAddr::from(([127, 0, 0, 1], port))
}

#[cfg(windows)]
fn write_addr_marker(socket_path: &Path, addr: std::net::SocketAddr) -> io::Result<()> {
    std::fs::write(socket_path, addr.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn socket_paths_creates_run_dir_next_to_db() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("db.sqlite");
        let (sock, pid) = socket_paths(&db).unwrap();
        #[cfg(unix)]
        assert!(sock.ends_with("run/nomai.sock"));
        #[cfg(windows)]
        assert!(sock.ends_with("run/nomai.port"));
        assert!(pid.ends_with("run/nomai.pid"));
        assert!(tmp.path().join("run").exists());
    }

    #[tokio::test]
    async fn bind_or_probe_binds_when_free() {
        let tmp = TempDir::new().unwrap();
        let sock = tmp.path().join("s.sock");
        match bind_or_probe(&sock).await.unwrap() {
            BindOutcome::Bound(_) => {}
            BindOutcome::AlreadyRunning => panic!("should bind when free"),
        }
        assert!(sock.exists());
    }

    #[tokio::test]
    async fn bind_or_probe_detects_live_daemon() {
        let tmp = TempDir::new().unwrap();
        let sock = tmp.path().join("s.sock");
        let _live = match bind_or_probe(&sock).await.unwrap() {
            BindOutcome::Bound(listener) => listener,
            BindOutcome::AlreadyRunning => panic!("first bind should succeed"),
        };
        match bind_or_probe(&sock).await.unwrap() {
            BindOutcome::AlreadyRunning => {}
            BindOutcome::Bound(_) => panic!("should detect already-running"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_or_probe_reclaims_dead_socket() {
        let tmp = TempDir::new().unwrap();
        let sock = tmp.path().join("s.sock");
        std::fs::write(&sock, b"stale").unwrap(); // leftover file, no listener
        match bind_or_probe(&sock).await.unwrap() {
            BindOutcome::Bound(_) => {}
            BindOutcome::AlreadyRunning => panic!("should reclaim dead socket"),
        }
    }

    #[test]
    fn write_pidfile_then_cleanup() {
        let tmp = TempDir::new().unwrap();
        let sock = tmp.path().join("s.sock");
        let pid = tmp.path().join("s.pid");
        std::fs::write(&sock, b"x").unwrap();
        write_pidfile(&pid).unwrap();
        assert!(pid.exists());
        cleanup(&sock, &pid);
        assert!(!sock.exists());
        assert!(!pid.exists());
    }

    #[cfg(windows)]
    #[test]
    fn tcp_addr_is_stable_for_same_path() {
        let p = PathBuf::from(r"C:\tmp\nomai\run\nomai.port");
        assert_eq!(tcp_addr(&p), tcp_addr(&p));
    }
}
