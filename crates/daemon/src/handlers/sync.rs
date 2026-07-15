//! sync.* handlers + git CLI wrapper. Multi-device sync via a git remote
//! (spec 2026-07-16-git-sync). This module owns ALL git subprocess invocation;
//! core stays git-agnostic. Task 3 ships only the wrapper layer + helpers;
//! `sync.init` / `sync.run` `RpcHandler` impls arrive in Tasks 4/5.

use std::path::Path;
use std::process::Stdio;

use tokio::process::Command;

use nomai_core::CoreError;

/// Output of a git subprocess where the caller needs to inspect status.
#[derive(Debug)]
pub(crate) struct GitOutput {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
}

/// Run `git -C <root> <args>`; return trimmed stdout on success.
/// `git` binary missing → `Validation`. Non-zero exit → `Validation` with
/// a stderr snippet (caller that needs conflict detection uses `git_allow_fail`).
#[allow(dead_code)] // consumed by Task 4/5 sync.* handlers
pub(crate) async fn git(root: &Path, args: &[&str]) -> Result<String, CoreError> {
    let out = git_allow_fail(root, args).await?;
    if out.success {
        Ok(out.stdout.trim().to_owned())
    } else {
        Err(CoreError::Validation(format!(
            "git {} failed: {}",
            args.join(" "),
            out.stderr.trim()
        )))
    }
}

/// Run git, return raw output without treating non-zero exit as error.
/// Spawn-time failures (binary not found, etc.) still map to `CoreError`:
/// `NotFound` → `Validation` (missing git is a user-fixable environment
/// problem, not an internal I/O fault); any other `io::Error` → `Io`.
#[allow(dead_code)] // consumed by Task 4/5 sync.* handlers
pub(crate) async fn git_allow_fail(root: &Path, args: &[&str]) -> Result<GitOutput, CoreError> {
    spawn("git", root, args).await
}

/// Spawn `<program> -C <root> <args>`, return raw output. Factored out so
/// the binary-not-found mapping can be exercised against the OS (passing a
/// guaranteed-absent program name) instead of a hand-built `io::Error`.
async fn spawn(program: &str, root: &Path, args: &[&str]) -> Result<GitOutput, CoreError> {
    let output = Command::new(program)
        .arg("-C")
        .arg(root)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                CoreError::Validation(format!("{program} not found on PATH"))
            } else {
                CoreError::Io(e)
            }
        })?;
    Ok(GitOutput {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        success: output.status.success(),
    })
}

/// Whether `git lfs` is installed and callable. Best-effort: any spawn or
/// status failure collapses to `false` (sync degrades to plain git).
#[allow(dead_code)] // consumed by Task 4/5 sync.* handlers
pub(crate) async fn probe_lfs() -> bool {
    Command::new("git")
        .arg("lfs")
        .arg("version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Whether a `git rebase` is mid-flight (conflict left over from a prior sync).
/// Checks `<root>/.git/rebase-merge` per `git`'s own resume logic.
#[allow(dead_code)] // consumed by Task 4/5 sync.* handlers
pub(crate) fn has_rebase_in_progress(root: &Path) -> bool {
    root.join(".git").join("rebase-merge").exists()
}

/// List unmerged (conflicting) file paths, relative to `<root>`. Returns an
/// empty vec if git itself fails to run — sync callers treat that as "no
/// auto-detectable conflicts" and surface the underlying git error elsewhere.
#[allow(dead_code)] // consumed by Task 4/5 sync.* handlers
pub(crate) async fn conflicted_files(root: &Path) -> Vec<String> {
    match git_allow_fail(root, &["diff", "--name-only", "--diff-filter=U"]).await {
        Ok(out) => out
            .stdout
            .lines()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect(),
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a throwaway git repo: `git init` + an empty commit so HEAD
    /// exists. Uses `std::process::Command` (sync) — only the daemon-side
    /// wrappers under test use tokio. Real `git` on PATH required.
    fn repo_tmp() -> tempfile::TempDir {
        let td = tempfile::tempdir().unwrap();
        let root = td.path();
        std::process::Command::new("git")
            .arg("init")
            .arg(root)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-C",
                root.to_str().unwrap(),
                "commit",
                "--allow-empty",
                "-m",
                "x",
            ])
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .unwrap();
        td
    }

    #[tokio::test]
    async fn git_runs_and_returns_stdout() {
        let td = repo_tmp();
        let out = git(td.path(), &["rev-parse", "--is-inside-work-tree"])
            .await
            .unwrap();
        assert_eq!(out, "true");
    }

    #[tokio::test]
    async fn git_missing_binary_maps_to_validation() {
        // Drive the real subprocess-spawn path with a binary name guaranteed
        // absent from PATH. This exercises the NotFound → Validation mapping
        // against the OS, not a hand-built `io::Error` (which would only
        // re-test the match expression in `spawn`). When `program = "git"`
        // the same code path produces `"git not found on PATH"`.
        let td = tempfile::tempdir().unwrap();
        let res = spawn(
            "nomai-definitely-not-on-path-9f3a2b7c",
            td.path(),
            &["--version"],
        )
        .await;
        assert!(matches!(res, Err(CoreError::Validation(_))), "{res:?}");
    }

    #[tokio::test]
    async fn git_nonzero_is_error() {
        let td = repo_tmp();
        let res = git(td.path(), &["show", "nope-nope-nope"]).await;
        assert!(matches!(res, Err(CoreError::Validation(_))));
    }

    #[test]
    fn has_rebase_in_progress_false_on_clean_repo() {
        let td = repo_tmp();
        assert!(!has_rebase_in_progress(td.path()));
    }

    #[test]
    fn has_rebase_in_progress_true_when_marker_exists() {
        let td = repo_tmp();
        std::fs::create_dir_all(td.path().join(".git").join("rebase-merge")).unwrap();
        assert!(has_rebase_in_progress(td.path()));
    }
}
