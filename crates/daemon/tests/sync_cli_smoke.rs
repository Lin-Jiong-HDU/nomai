//! Smoke test: `--sync-init` then `--sync` end-to-end via the real
//! `nomai-daemon` binary against a tempfile config, tempfile knowledge_root,
//! and tempfile bare remote. All data lives in TempDirs — nothing touches
//! `~/.local/share/nomai`. Requires `git` and `git-lfs` on PATH; marked
//! `#[ignore]` so it never runs in normal `cargo test`.

#![cfg(unix)]

use std::path;
use std::process::{Command, Stdio};

/// Cargo sets `CARGO_BIN_EXE_<bin-name>` with hyphens preserved.
fn binary() -> path::PathBuf {
    path::PathBuf::from(env!("CARGO_BIN_EXE_nomai-daemon"))
}

/// Write a minimal config whose `db_path` + `knowledge_root` live under `dir`
/// (a TempDir). No HTTP is ever made — the sync RPCs are git-only, and
/// `entry.list` / `index.sync` need no embedding call for this flow.
fn write_config(dir: &path::Path, knowledge_root: &path::Path) -> path::PathBuf {
    let cfg = dir.join("config.toml");
    let db = dir.join("db.sqlite");
    let toml = format!(
        r#"
[data]
db_path = "{db}"
knowledge_root = "{kr}"

[embedding]
base_url = "https://example.invalid/v1"
api_key_env = "NOMAI_SMOKE_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.invalid/v1"
api_key_env = "NOMAI_SMOKE_KEY"
model = "x"

[serve]
idle_timeout_secs = 5
"#,
        db = db.display(),
        kr = knowledge_root.display(),
    );
    std::fs::write(&cfg, toml).unwrap();
    cfg
}

#[test]
#[ignore = "needs git + git-lfs on PATH and spawns the binary; run with --ignored"]
fn sync_init_then_run_via_binary() {
    let dir = tempfile::TempDir::new().unwrap();
    let knowledge_root = dir.path().join("store");
    std::fs::create_dir_all(&knowledge_root).unwrap();
    let cfg = write_config(dir.path(), &knowledge_root);

    // Bare remote that sync.init configures as `origin` and sync.run pushes to.
    let bare = tempfile::TempDir::new().unwrap();
    let git_init = Command::new("git")
        .args([
            "init",
            "--bare",
            "--initial-branch",
            "main",
            bare.path().to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn git init --bare");
    assert!(git_init.success(), "git init --bare failed");

    let env = [("NOMAI_SMOKE_KEY", "k")];

    // --sync-init: the CLI attaches to (spawns) the resident daemon, which
    // runs `git init` + remote + LFS install + initial commit under the
    // configured knowledge_root. Must exit 0.
    let init = Command::new(binary())
        .args([
            "--sync-init",
            bare.path().to_str().unwrap(),
            "--config",
            cfg.to_str().unwrap(),
        ])
        .envs(env.iter().copied())
        .output()
        .expect("spawn nomai-daemon --sync-init");
    assert!(
        init.status.success(),
        "--sync-init failed (exit {:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
        init.status.code(),
        String::from_utf8_lossy(&init.stdout),
        String::from_utf8_lossy(&init.stderr),
    );

    // Strong assertion: the daemon's sync.init handler materialized .git.
    assert!(
        knowledge_root.join(".git").exists(),
        ".git missing after --sync-init"
    );

    // --sync: commit (nothing new since init) → pull --rebase (empty remote,
    // falls through) → push → index.sync. Must exit 0.
    let run = Command::new(binary())
        .args(["--sync", "--config", cfg.to_str().unwrap()])
        .envs(env.iter().copied())
        .output()
        .expect("spawn nomai-daemon --sync");
    assert!(
        run.status.success(),
        "--sync failed (exit {:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
        run.status.code(),
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr),
    );
}
