#![cfg(unix)]
//! Unix socket lifecycle primitives for the resident `--serve` daemon.

use std::io;
use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};

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
    Ok((run_dir.join("nomai.sock"), run_dir.join("nomai.pid")))
}

/// Outcome of attempting to claim the resident-daemon socket.
pub enum BindOutcome {
    /// We own the socket; ready to accept.
    ///
    /// The listener is consumed by `serve` (Task 2); until then the field is
    /// unread here, so we silence the dead-code lint at the variant level.
    #[allow(dead_code)]
    Bound(UnixListener),
    /// A live daemon already holds it. Caller MUST exit (do NOT fall back to
    /// stdio — that reintroduces multi-process conflict; spec §6).
    AlreadyRunning,
}

/// Bind with bind/connect double-probe arbitration (spec §6).
///
/// - bind succeeds → own it.
/// - bind fails `EADDRINUSE` → `connect` to probe: success means a live
///   daemon (`AlreadyRunning`); failure means a stale leftover → `unlink` +
///   retry bind.
pub async fn bind_or_probe(socket_path: &Path) -> io::Result<BindOutcome> {
    match UnixListener::bind(socket_path) {
        Ok(listener) => {
            restrict_perms(socket_path)?;
            Ok(BindOutcome::Bound(listener))
        }
        Err(first) if first.raw_os_error() == Some(libc::EADDRINUSE) => {
            match UnixStream::connect(socket_path).await {
                Ok(_) => Ok(BindOutcome::AlreadyRunning),
                Err(_) => {
                    std::fs::remove_file(socket_path)?;
                    let listener = UnixListener::bind(socket_path)?;
                    restrict_perms(socket_path)?;
                    Ok(BindOutcome::Bound(listener))
                }
            }
        }
        Err(e) => Err(e),
    }
}

/// Constrain the socket to 0600 so other OS users can't connect.
fn restrict_perms(socket_path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn socket_paths_creates_run_dir_next_to_db() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("db.sqlite");
        let (sock, pid) = socket_paths(&db).unwrap();
        assert!(sock.ends_with("run/nomai.sock"));
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
        let _live = UnixListener::bind(&sock).unwrap(); // a daemon is listening
        match bind_or_probe(&sock).await.unwrap() {
            BindOutcome::AlreadyRunning => {}
            BindOutcome::Bound(_) => panic!("should detect already-running"),
        }
    }

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
}
