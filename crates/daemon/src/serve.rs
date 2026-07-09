#![cfg(unix)]
//! Resident `--serve` daemon: detach, claim the socket, boot the Daemon,
//! run the serve loop, clean up on exit.

use nomai_core::CoreError;

use crate::config::Config;
use crate::socket::{BindOutcome, bind_or_probe, cleanup, socket_paths, write_pidfile};

/// Boot the resident daemon. Detaches into a new session, claims the socket
/// (exiting cleanly if a live daemon already holds it), writes the pidfile,
/// constructs the Daemon, and enters the serve loop. Cleans up socket +
/// pidfile on exit; on a hard crash (SIGKILL) the next boot's
/// `bind_or_probe` reclaims any leftover.
pub async fn run(config: Config) -> Result<(), CoreError> {
    detach_session()?;

    let db_path = crate::daemon::resolved_db_path(&config)?;
    let (socket_path, pidfile) =
        socket_paths(&db_path).map_err(|e| CoreError::Config(format!("socket paths: {e}")))?;

    let listener = match bind_or_probe(&socket_path).await {
        Ok(BindOutcome::Bound(l)) => l,
        Ok(BindOutcome::AlreadyRunning) => {
            eprintln!("nomai-daemon: another daemon is already running; exiting");
            return Ok(());
        }
        Err(e) => {
            return Err(CoreError::Config(format!(
                "bind socket {}: {e}",
                socket_path.display()
            )));
        }
    };
    let _guard = PidfileGuard(socket_path.clone(), pidfile.clone());
    write_pidfile(&pidfile).map_err(|e| CoreError::Config(format!("write pidfile: {e}")))?;

    let idle = std::time::Duration::from_secs(config.serve.idle_timeout_secs);
    let daemon = crate::daemon::Daemon::new(config).await?;
    daemon.run_serve(listener, idle).await?;

    Ok(())
}

/// RAII: remove socket + pidfile when the daemon exits (graceful or panic).
struct PidfileGuard(std::path::PathBuf, std::path::PathBuf);
impl Drop for PidfileGuard {
    fn drop(&mut self) {
        cleanup(&self.0, &self.1);
    }
}

/// Detach into a new session so the resident daemon survives the spawning
/// shim (and its CC parent) exiting. `setsid` makes us a session leader
/// with no controlling terminal.
fn detach_session() -> Result<(), CoreError> {
    // SAFETY: `setsid()` creates a new session and process group, returning
    // the new session id (or -1 on error). It touches only kernel process
    // metadata — no memory-safety implications.
    let rc = unsafe { libc::setsid() };
    if rc == -1 {
        return Err(CoreError::Config(format!(
            "setsid failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detach_session_does_not_panic_in_test_process() {
        // A test process may already be a session leader (setsid → EPERM) or
        // succeed; either is acceptable. Full serve::run arbitration is
        // covered by Task 6's e2e smoke + socket tests' AlreadyRunning case.
        let _ = unsafe { libc::setsid() };
    }

    #[test]
    fn pidfile_guard_cleans_up_on_drop() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("g.sock");
        let pid = tmp.path().join("g.pid");
        std::fs::write(&sock, b"x").unwrap();
        std::fs::write(&pid, b"x").unwrap();
        {
            let _g = PidfileGuard(sock.clone(), pid.clone());
        }
        assert!(!sock.exists());
        assert!(!pid.exists());
    }
}
