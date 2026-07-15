//! sync.* handlers + git CLI wrapper. Multi-device sync via a git remote
//! (spec 2026-07-16-git-sync). This module owns ALL git subprocess invocation;
//! core stays git-agnostic. Task 3 ships only the wrapper layer + helpers;
//! `sync.init` / `sync.run` `RpcHandler` impls arrive in Tasks 4/5.

use std::path::Path;
use std::process::Stdio;

use async_trait::async_trait;
use serde_json::Value;
use tokio::process::Command;

use nomai_core::CoreError;
use nomai_protocol::method::sync::{INIT as SYNC_INIT, RUN as SYNC_RUN};

/// `.gitignore` — exclude atomic_write temp files and other sync-unfriendly
/// artifacts from the work-tree.
const GITIGNORE: &str = "*.tmp\n*.tmp.*\n";

/// `.gitattributes` — binary attachments route through Git LFS (keeps the
/// repository small); `.nomai` files stay plain text so git merge works.
const GITATTRIBUTES: &str = "# large binary attachments go through Git LFS\n\
*.pdf  filter=lfs diff=lfs merge=lfs -text\n\
*.png  filter=lfs diff=lfs merge=lfs -text\n\
*.jpg  filter=lfs diff=lfs merge=lfs -text\n\
*.jpeg filter=lfs diff=lfs merge=lfs -text\n\
*.gif  filter=lfs diff=lfs merge=lfs -text\n\
*.webp filter=lfs diff=lfs merge=lfs -text\n\
*.zip  filter=lfs diff=lfs merge=lfs -text\n\
*.mp4  filter=lfs diff=lfs merge=lfs -text\n\
*.mov  filter=lfs diff=lfs merge=lfs -text\n\
# entry.nomai stays plain text (git-merge friendly)\n\
*.nomai text\n";

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
/// Checks both `<root>/.git/rebase-merge` (interactive/merge-based rebases) and
/// `<root>/.git/rebase-apply` (am-based / non-interactive rebases such as
/// `git pull --rebase` in some configurations), per `git`'s own resume logic.
#[allow(dead_code)] // consumed by Task 4/5 sync.* handlers
pub(crate) fn has_rebase_in_progress(root: &Path) -> bool {
    root.join(".git").join("rebase-merge").exists()
        || root.join(".git").join("rebase-apply").exists()
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

/// `sync.init` — turn the FS-backed knowledge_root into a git repository
/// configured for multi-device sync. Steps (all failures → `CoreError`,
/// daemon never panics):
///
/// 1. Reject if `root/.git` already exists (idempotency; no mutation).
/// 2. Reject if `git lfs` is unavailable (probe before any write).
/// 3. `git init --initial-branch=<branch>`.
/// 4. Configure `origin` remote.
/// 5. `git lfs install` (per-repo hooks).
/// 6. Write `.gitignore` + `.gitattributes`.
/// 7. `git add -A` + initial commit (identity injected via `-c` so a
///    user without global git config still succeeds).
pub struct Init;

#[async_trait]
impl crate::rpc::RpcHandler for Init {
    fn method(&self) -> &'static str {
        SYNC_INIT
    }
    fn description(&self) -> &'static str {
        "Initialize the knowledge_root as a git repository for multi-device \
         sync. Configures the remote, writes .gitignore + .gitattributes (LFS \
         rules), runs git lfs install, and makes an initial commit. \
         Idempotent — rejects if .git already exists."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(serde_json::json!({
            "type": "object",
            "properties": {
                "remote": { "type": "string", "description": "git remote URL (SSH or HTTPS)" },
                "branch": { "type": "string", "description": "branch name (default 'main')" }
            },
            "required": ["remote"],
            "additionalProperties": false
        }))
    }
    async fn call(
        &self,
        daemon: &crate::daemon::Daemon,
        params: Value,
    ) -> Result<Value, CoreError> {
        let remote = params
            .get("remote")
            .and_then(|v| v.as_str())
            .ok_or_else(|| CoreError::Validation("sync.init requires 'remote' string".into()))?;
        let branch = params
            .get("branch")
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        let root = daemon.content_store.root();

        // (1) Idempotency: bail before touching anything if already a repo.
        if root.join(".git").exists() {
            return Err(CoreError::Validation(
                "already a git repository; remove .git to re-init".into(),
            ));
        }
        // (2) LFS probe before any mutation so we never leave a half-initialized
        //     repo behind on a machine without git-lfs.
        if !probe_lfs().await {
            return Err(CoreError::Validation(
                "git-lfs not found; install from https://git-lfs.com".into(),
            ));
        }

        git(root, &["init", "--initial-branch", branch]).await?;
        git(root, &["remote", "add", "origin", remote]).await?;
        git(root, &["lfs", "install"]).await?;
        // Small text files; std::fs::write is fine in this async context
        // (tokio::fs is gated off by the daemon crate's feature set).
        std::fs::write(root.join(".gitignore"), GITIGNORE).map_err(CoreError::from)?;
        std::fs::write(root.join(".gitattributes"), GITATTRIBUTES).map_err(CoreError::from)?;
        git_with_identity(root, &["add", "-A"]).await?;
        git_with_identity(root, &["commit", "-m", "nomai sync init"]).await?;

        Ok(serde_json::json!({
            "initialized": true,
            "knowledge_root": root,
            "remote": remote,
            "branch": branch,
            "lfs_ready": true,
        }))
    }
}

/// `sync.run` — synchronize the knowledge_root with its git remote. The
/// full operation runs under `daemon.sync_lock` so it is serialized against
/// in-process write RPCs that mutate entry files. Flow (every failure →
/// `CoreError`, daemon never panics; the lock guard is released on every
/// return path by dropping):
///
/// 1. Reject if `root/.git` is missing (run `sync.init` first).
/// 2. Read current branch via `git rev-parse --abbrev-ref HEAD`.
/// 3. If a rebase is mid-flight (`has_rebase_in_progress`), resume via
///    `git rebase --continue` and skip the commit step (the conflict being
///    resumed already has staged edits). Otherwise `git add -A` and, only
///    when `git status --porcelain` is non-empty, commit via
///    `git_with_identity` (works without global git config).
/// 4. Normal path: `git pull --rebase origin <branch>`. Resume path:
///    `git rebase --continue`. On non-zero exit, a real rebase conflict
///    leaves `.git/rebase-merge`/`rebase-apply` in place — detect that and
///    return `CoreError::SyncConflict` WITHOUT pushing or reindexing, so
///    the user can resolve in an editor and re-run to resume. A non-zero
///    exit WITHOUT a mid-flight rebase is something else (e.g. first push
///    to an empty remote: "couldn't find remote ref") — fall through to
///    push rather than misreport a conflict.
/// 5. `git push origin <branch>`.
/// 6. Reuse the `index.sync` handler (looked up in `daemon.handlers`) to
///    rebuild the local SQLite index from the reconciled FS.
pub struct Run;

#[async_trait]
impl crate::rpc::RpcHandler for Run {
    fn method(&self) -> &'static str {
        SYNC_RUN
    }
    fn description(&self) -> &'static str {
        "Sync the knowledge_root with its git remote: commit local changes, \
         pull --rebase, push, then rebuild the local SQLite index via index.sync. \
         On rebase conflict, returns the conflicted files and leaves the repo \
         mid-rebase; resolve in an editor and re-run to continue."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(
        &self,
        daemon: &crate::daemon::Daemon,
        _params: Value,
    ) -> Result<Value, CoreError> {
        // Held for the whole op; released by drop on every return path
        // (including the `?` early-returns and the conflict early-return).
        let _lock = daemon.sync_lock.lock().await;
        let root = daemon.content_store.root().to_path_buf();

        if !root.join(".git").exists() {
            return Err(CoreError::Validation(
                "not a git repository; run sync.init <remote> first".into(),
            ));
        }

        let branch = git(&root, &["rev-parse", "--abbrev-ref", "HEAD"]).await?;

        let resuming = has_rebase_in_progress(&root);
        let mut committed = false;
        let mut commit_hash = serde_json::Value::Null;

        if !resuming {
            git(&root, &["add", "-A"]).await?;
            let status = git(&root, &["status", "--porcelain"]).await?;
            if !status.is_empty() {
                let hash = git_with_identity(&root, &["commit", "-m", "nomai sync"]).await?;
                committed = true;
                commit_hash = serde_json::Value::String(hash);
            }
        }

        // 正常路径: pull --rebase；恢复路径: rebase --continue
        let rebase_args: Vec<&str> = if resuming {
            vec!["rebase", "--continue"]
        } else {
            vec!["pull", "--rebase", "origin", branch.as_str()]
        };
        let rb = git_allow_fail(&root, &rebase_args).await?;
        if !rb.success {
            // Only a genuine rebase conflict leaves the rebase mid-flight. A
            // failed pull that did NOT start a rebase (empty remote on first
            // push, network blip) is not a conflict — fall through to push
            // and let push either succeed (first push) or surface its error.
            if has_rebase_in_progress(&root) {
                let files = conflicted_files(&root).await;
                return Err(CoreError::SyncConflict {
                    message: "rebase conflict; resolve in editor and re-run nomai sync".into(),
                    conflicted_files: files,
                });
            }
        }

        git(&root, &["push", "origin", branch.as_str()]).await?;

        // 复用 index.sync handler 重建本地索引（FS 已变）。
        let index_sync = daemon
            .handlers
            .get(nomai_protocol::method::index::SYNC)
            .ok_or_else(|| CoreError::Config("index.sync handler missing".into()))?;
        index_sync.call(daemon, Value::Null).await?;

        Ok(serde_json::json!({
            "committed": committed,
            "commit": commit_hash,
            "pushed": true,
            "reindexed": true,
        }))
    }
}

/// Like `git`, but injects a fallback identity via `-c` so `git commit`
/// works even when the user has no global git config. One-shot: the
/// `-c` values apply only to this invocation and are NOT written to the
/// user's git config.
async fn git_with_identity(root: &Path, args: &[&str]) -> Result<String, CoreError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["-c", "user.email=nomai@local", "-c", "user.name=nomai"])
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                CoreError::Validation("git not found on PATH".into())
            } else {
                CoreError::Io(e)
            }
        })?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(CoreError::Validation(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;

    use nomai_core::EntryService;
    use nomai_protocol::error::{SYNC_ERROR, VALIDATION_ERROR};
    use nomai_protocol::method::sync::{INIT as SYNC_INIT, RUN as SYNC_RUN};
    use nomai_protocol::{Id, JSONRPC_VERSION, Request};
    use nomai_providers::{EmbeddingProvider, LlmProvider};
    use serde_json::{Value, json};

    fn req(method: &str, params: Value) -> Request {
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: Some(Id::Number(1)),
            method: method.into(),
            params: Some(params),
        }
    }

    /// Minimal daemon + a bare git remote under tempfile. All data lives in
    /// `TempDir`s — nothing touches `~/.local/share/nomai`.
    struct SyncTestHarness {
        knowledge_root: std::path::PathBuf,
        bare: tempfile::TempDir,
        _store_tmp: tempfile::TempDir,
    }

    impl SyncTestHarness {
        async fn new() -> Self {
            let store_tmp = tempfile::tempdir().unwrap();
            let knowledge_root = store_tmp.path().join("store");
            std::fs::create_dir_all(&knowledge_root).unwrap();
            let bare = tempfile::tempdir().unwrap();
            std::process::Command::new("git")
                .args([
                    "init",
                    "--bare",
                    "--initial-branch",
                    "main",
                    bare.path().to_str().unwrap(),
                ])
                .output()
                .unwrap();
            Self {
                knowledge_root,
                bare,
                _store_tmp: store_tmp,
            }
        }

        fn knowledge_root(&self) -> &Path {
            &self.knowledge_root
        }

        fn clone_url(&self) -> String {
            self.bare.path().to_str().unwrap().to_owned()
        }

        /// Build a Daemon whose `content_store.root()` points at
        /// `self.knowledge_root`. Mirrors the Task 2 test
        /// (`daemon_exposes_content_store_root_and_sync_lock`): use
        /// `EntryService::for_test` for an in-memory SQLite with migrations
        /// and sqlite-vec, then re-route the content store at our tempdir
        /// via `DaemonBuilder`. Null providers — no HTTP.
        fn daemon(&self) -> crate::daemon::Daemon {
            let entries = Arc::new(EntryService::for_test().unwrap());
            let conn = entries.conn_for_test();
            let store = Arc::new(nomai_core::ContentStore::new(self.knowledge_root.clone()));

            struct NullEmbed;
            #[async_trait]
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
            #[async_trait]
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

            crate::daemon::DaemonBuilder::new()
                .conn(conn)
                .content_store(store)
                .embedder(Arc::new(NullEmbed))
                .llm(Arc::new(NullLlm))
                .embedding_dim(8)
                .chunk_target_size(1024)
                .cache_model("test-model")
                .warn_rows(100_000)
                .build()
                .unwrap()
        }
    }

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
        // Interactive / merge-based rebases write to rebase-merge/.
        let td = repo_tmp();
        std::fs::create_dir_all(td.path().join(".git").join("rebase-merge")).unwrap();
        assert!(has_rebase_in_progress(td.path()));

        // am-based / non-interactive rebases (e.g. some `git pull --rebase`
        // configurations) write to rebase-apply/ instead — must also be detected.
        let td2 = repo_tmp();
        std::fs::create_dir_all(td2.path().join(".git").join("rebase-apply")).unwrap();
        assert!(has_rebase_in_progress(td2.path()));
    }

    // ----- sync.init e2e (Task 4) -----

    #[tokio::test]
    async fn init_creates_git_repo_and_attributes() {
        let harness = SyncTestHarness::new().await;
        let daemon = harness.daemon();

        let params = json!({ "remote": harness.clone_url(), "branch": "main" });
        let resp = daemon.dispatch(req(SYNC_INIT, params)).await;
        assert!(
            resp.result.is_some(),
            "init should succeed: {:?}",
            resp.error
        );

        let root = harness.knowledge_root();
        assert!(root.join(".git").exists());
        assert!(root.join(".gitattributes").exists());
        assert!(root.join(".gitignore").exists());
        let attrs = std::fs::read_to_string(root.join(".gitattributes")).unwrap();
        assert!(attrs.contains("filter=lfs"));
        assert!(attrs.contains("*.nomai text"));
    }

    #[tokio::test]
    async fn init_rejects_already_a_repo() {
        let harness = SyncTestHarness::new().await;
        let daemon = harness.daemon();
        let params = json!({ "remote": harness.clone_url(), "branch": "main" });
        let resp1 = daemon.dispatch(req(SYNC_INIT, params.clone())).await;
        // Precondition: the first init must succeed, otherwise the "already a
        // git repository" assertion below can mask a missing-git-lfs failure.
        assert!(
            resp1.result.is_some(),
            "prereq sync.init failed: {:?}",
            resp1.error
        );
        let resp2 = daemon.dispatch(req(SYNC_INIT, params)).await;
        let err = resp2.error.unwrap();
        assert_eq!(err.code, VALIDATION_ERROR);
        assert!(err.message.contains("already a git repository"));
    }

    // ----- sync.run e2e (Task 5) -----

    #[tokio::test]
    async fn run_commits_and_pushes_local_changes() {
        let harness = SyncTestHarness::new().await;
        let daemon = harness.daemon();
        let resp_init = daemon
            .dispatch(req(
                SYNC_INIT,
                json!({ "remote": harness.clone_url(), "branch": "main" }),
            ))
            .await;
        assert!(
            resp_init.result.is_some(),
            "prereq sync.init failed: {:?}",
            resp_init.error
        );

        // 造一个 entry 文件（合成数据，不碰真实 KB）
        let dir = harness
            .knowledge_root()
            .join("entries")
            .join("01TEST00000000000000000001");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("entry.nomai"),
            "#format_version 1\n#id 01TEST00000000000000000001\n#title t\n#created_at 2026-07-16T00:00:00Z\n#updated_at 2026-07-16T00:00:00Z\n\n@note\nhello\n",
        )
        .unwrap();

        let resp = daemon.dispatch(req(SYNC_RUN, json!({}))).await;
        assert!(
            resp.result.is_some(),
            "run should succeed: {:?}",
            resp.error
        );
        let result = resp.result.unwrap();
        assert_eq!(result["committed"], true);
        assert_eq!(result["pushed"], true);

        // clone remote 验证 entry 已推送
        let clone_tmp = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .args([
                "clone",
                &harness.clone_url(),
                clone_tmp.path().to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(clone_tmp.path().join("entries").exists());
    }

    #[tokio::test]
    async fn run_without_local_changes_skips_commit() {
        let harness = SyncTestHarness::new().await;
        let daemon = harness.daemon();
        let resp_init = daemon
            .dispatch(req(
                SYNC_INIT,
                json!({ "remote": harness.clone_url(), "branch": "main" }),
            ))
            .await;
        assert!(
            resp_init.result.is_some(),
            "prereq sync.init failed: {:?}",
            resp_init.error
        );
        let resp = daemon.dispatch(req(SYNC_RUN, json!({}))).await;
        let result = resp
            .result
            .unwrap_or_else(|| panic!("run should succeed: {:?}", resp.error));
        assert_eq!(result["committed"], false);
    }

    #[tokio::test]
    async fn run_returns_conflict_on_diverged_same_file() {
        // 两个 work-tree 共享一个 bare remote；两端改同一文件 → 第二端冲突。
        // --initial-branch main 让 bare 的 HEAD 与本测试使用的 main 分支一致，
        // 否则在 init.defaultBranch 未设的系统上 clone 会落到 master 而 push main 失败。
        let bare = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .args([
                "init",
                "--bare",
                "--initial-branch",
                "main",
                bare.path().to_str().unwrap(),
            ])
            .output()
            .unwrap();

        let harness = SyncTestHarness::new().await;
        let daemon = harness.daemon();
        let resp_init = daemon
            .dispatch(req(
                SYNC_INIT,
                json!({ "remote": bare.path().to_str().unwrap(), "branch": "main" }),
            ))
            .await;
        assert!(
            resp_init.result.is_some(),
            "prereq sync.init failed: {:?}",
            resp_init.error
        );

        // 直接在 bare 里用第二个 work-tree 制造分叉：clone、改同一 entry、push
        let other = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .args([
                "clone",
                bare.path().to_str().unwrap(),
                other.path().to_str().unwrap(),
            ])
            .output()
            .unwrap();
        let f = other.path().join("entries").join("X").join("entry.nomai");
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(
            &f,
            "#format_version 1\n#id X\n#title from-other\n\n@note\nother\n",
        )
        .unwrap();
        let _ = std::process::Command::new("git")
            .args(["-C", other.path().to_str().unwrap(), "add", "-A"])
            .output()
            .unwrap();
        let _ = std::process::Command::new("git")
            .args([
                "-C",
                other.path().to_str().unwrap(),
                "-c",
                "user.email=o@o",
                "-c",
                "user.name=o",
                "commit",
                "-m",
                "other",
            ])
            .output()
            .unwrap();
        let _ = std::process::Command::new("git")
            .args([
                "-C",
                other.path().to_str().unwrap(),
                "push",
                "origin",
                "main",
            ])
            .output()
            .unwrap();

        // 本端也改"同一文件"（path 相同才能冲突）然后 run
        let local_f = harness
            .knowledge_root()
            .join("entries")
            .join("X")
            .join("entry.nomai");
        std::fs::create_dir_all(local_f.parent().unwrap()).unwrap();
        std::fs::write(
            &local_f,
            "#format_version 1\n#id X\n#title from-local\n\n@note\nlocal\n",
        )
        .unwrap();

        let resp = daemon.dispatch(req(SYNC_RUN, json!({}))).await;
        let err = resp.error.expect("expected conflict");
        assert_eq!(err.code, SYNC_ERROR);
        assert!(err.data.unwrap()["conflicted_files"].is_array());
    }
}
