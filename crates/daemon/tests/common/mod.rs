//! Shared harness for sync e2e tests (`tests/sync_e2e.rs`).
//!
//! One `SyncTestHarness` instance models one "device": its own FS-backed
//! `knowledge_root` under a tempfile + its own in-memory SQLite + its own
//! `Daemon`. Two instances sharing one bare git remote model two devices
//! syncing through a hub. Built entirely from the daemon crate's **public**
//! API (`Daemon::from_services` + `EntryService::for_test` + public
//! providers/protocol items) — no crate-private access needed.
//!
//! All data lives in `TempDir`s; nothing touches `~/.local/share/nomai`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use nomai_daemon::daemon::Daemon;
use nomai_protocol::{Id, JSONRPC_VERSION, Request, Response, method::sync};
use serde_json::{Value, json};
use tempfile::TempDir;

/// Minimal daemon + a knowledge_root under a tempfile. The bare remote is
/// owned by the test (shared between both harnesses); `bare_url` records
/// where `sync.init` should point `origin`.
pub struct SyncTestHarness {
    knowledge_root: PathBuf,
    _store_tmp: TempDir,
    daemon: Daemon,
    bare_url: String,
}

impl SyncTestHarness {
    /// Build a device whose `content_store.root()` is a fresh tempdir and
    /// whose `origin` will be configured to `bare`. Real `git` on PATH is
    /// required — every sync RPC shells out to it.
    pub async fn new_with_remote(bare: &Path) -> Self {
        let store_tmp = tempfile::tempdir().unwrap();
        let knowledge_root = store_tmp.path().join("store");
        std::fs::create_dir_all(&knowledge_root).unwrap();

        // Share one in-memory SQLite + one FS store across the daemon's
        // services. EntryService::for_test is #[doc(hidden)] pub precisely
        // so downstream crates' tests can reach it across the crate boundary
        // (cfg(test) does not propagate cross-crate).
        let entries = Arc::new(nomai_core::EntryService::for_test().unwrap());
        let conn = entries.conn_for_test();
        let store = Arc::new(nomai_core::ContentStore::new(knowledge_root.clone()));

        struct NullEmbed;
        #[async_trait]
        impl nomai_providers::EmbeddingProvider for NullEmbed {
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
                "null-embed"
            }
        }
        struct NullLlm;
        #[async_trait]
        impl nomai_providers::LlmProvider for NullLlm {
            async fn complete(
                &self,
                _req: nomai_providers::CompletionRequest,
            ) -> Result<nomai_providers::CompletionResponse, nomai_protocol::ProviderError>
            {
                Err(nomai_protocol::ProviderError::new(
                    nomai_protocol::ProviderErrorKind::Unknown,
                    "null llm",
                    None,
                ))
            }
            fn name(&self) -> &str {
                "null-llm"
            }
        }

        // sync.* RPCs are git-only; null providers never get called.
        let daemon = Daemon::from_services(
            conn,
            store,
            Arc::new(NullEmbed),
            Arc::new(NullLlm),
            8,
            1024,
            "test-embed",
            100_000,
            10 * 1024 * 1024,
        )
        .expect("daemon builds");

        Self {
            knowledge_root,
            _store_tmp: store_tmp,
            daemon,
            bare_url: bare.to_str().unwrap().to_owned(),
        }
    }

    #[allow(dead_code)]
    pub fn knowledge_root(&self) -> &Path {
        &self.knowledge_root
    }

    /// Path to the entry directory for `id` under this device's knowledge_root.
    pub fn entry_dir(&self, id: &str) -> PathBuf {
        self.knowledge_root.join("entries").join(id)
    }

    /// Whether a git rebase is mid-flight on this device's work-tree
    /// (conflict left over from a prior `sync.run`). Mirrors the daemon-side
    /// check so the e2e test can assert state without reaching into the
    /// handler's crate-private helper.
    pub fn rebase_in_progress(&self) -> bool {
        self.knowledge_root
            .join(".git")
            .join("rebase-merge")
            .exists()
            || self
                .knowledge_root
                .join(".git")
                .join("rebase-apply")
                .exists()
    }

    /// Dispatch `sync.init` with this device's bare URL + `main` branch.
    /// Panics the test if init fails (the caller's precondition).
    pub async fn dispatch_init(&self) {
        let resp = self
            .daemon
            .dispatch(req(
                sync::INIT,
                json!({ "remote": self.bare_url, "branch": "main" }),
            ))
            .await;
        assert!(
            resp.result.is_some(),
            "sync.init precondition failed: {:?}",
            resp.error
        );
    }

    /// Dispatch `sync.run` and return the `result` object. Panics if the
    /// RPC returned an error — use `dispatch_run_err` for the conflict path.
    pub async fn dispatch_run(&self) -> Value {
        let resp = self.daemon.dispatch(req(sync::RUN, json!({}))).await;
        resp.result
            .unwrap_or_else(|| panic!("sync.run should succeed: {:?}", resp.error))
    }

    /// Dispatch `sync.run` and return the `error` object (for the conflict
    /// test, which expects code 1007).
    pub async fn dispatch_run_err(&self) -> nomai_protocol::RpcError {
        let resp = self.daemon.dispatch(req(sync::RUN, json!({}))).await;
        resp.error
            .unwrap_or_else(|| panic!("sync.run should error: {:?}", resp.result))
    }

    /// Write a minimal `.nomai` entry directly to the FS (bypasses RPCs).
    /// `sync.run`'s `git add -A` picks it up and commits it. Uses a valid
    /// 26-char Crockford-base32 `id` so `index.sync` (invoked at the end of
    /// `sync.run`) can parse it without raising a format error.
    pub fn write_entry(&self, id: &str, title: &str, body: &str) {
        let dir = self.entry_dir(id);
        std::fs::create_dir_all(&dir).unwrap();
        let content = format!(
            "#format_version 1\n\
             #id {id}\n\
             #title {title}\n\
             #created_at 2026-07-16T00:00:00Z\n\
             #updated_at 2026-07-16T00:00:00Z\n\
             \n\
             @note\n\
             {body}\n"
        );
        std::fs::write(dir.join("entry.nomai"), content).unwrap();
    }

    /// Overwrite an entry's `entry.nomai` with new body text (simulates a
    /// user editing on this device). Used to diverge the same entry across
    /// two devices for the conflict test.
    pub fn edit_entry_body(&self, id: &str, title: &str, body: &str) {
        let file = self.entry_dir(id).join("entry.nomai");
        let content = format!(
            "#format_version 1\n\
             #id {id}\n\
             #title {title}\n\
             #created_at 2026-07-16T00:00:00Z\n\
             #updated_at 2026-07-16T12:00:00Z\n\
             \n\
             @note\n\
             {body}\n"
        );
        std::fs::write(file, content).unwrap();
    }

    /// Resolve a rebase conflict on `id`'s entry the way a user would: drop
    /// the conflict markers by overwriting with clean content, then `git add`
    /// the file so `git rebase --continue` (run by the next `sync.run`) can
    /// proceed. The `sync.run` resume path intentionally skips `git add`
    /// (the comment in `handlers/sync.rs` states "the conflict being resumed
    /// already has staged edits") — staging is the user's job.
    pub fn resolve_and_stage(&self, id: &str, title: &str, body: &str) {
        let file = self.entry_dir(id).join("entry.nomai");
        let content = format!(
            "#format_version 1\n\
             #id {id}\n\
             #title {title}\n\
             #created_at 2026-07-16T00:00:00Z\n\
             #updated_at 2026-07-16T12:00:00Z\n\
             \n\
             @note\n\
             {body}\n"
        );
        std::fs::write(&file, content).unwrap();
        let status = Command::new("git")
            .arg("-C")
            .arg(self.knowledge_root.to_str().unwrap())
            .args(["add", &format!("entries/{id}/entry.nomai")])
            .status()
            .expect("git add to resolve conflict");
        assert!(status.success(), "git add (resolve) failed");
    }
}

/// Build a JSON-RPC 2.0 request with numeric id 1.
fn req(method: &str, params: Value) -> Request {
    Request {
        jsonrpc: JSONRPC_VERSION.into(),
        id: Some(Id::Number(1)),
        method: method.into(),
        params: Some(params),
    }
}

/// Drive the daemon and return the full `Response` (kept for symmetry with
/// `req`; the harness dispatch methods are the usual entry points).
#[allow(dead_code)]
pub async fn dispatch_raw(daemon: &Daemon, method: &str, params: Value) -> Response {
    daemon.dispatch(req(method, params)).await
}
