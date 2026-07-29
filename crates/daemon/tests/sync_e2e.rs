//! End-to-end sync tests: two `SyncTestHarness` instances (each an
//! independent "device" — own `knowledge_root` tempfile + own in-memory
//! SQLite + own `Daemon`) sharing one bare git remote. Exercises the full
//! `sync.init` → `sync.run` → real `git` → `index.sync` pipeline through
//! the daemon's public `dispatch` surface, exactly as a resident daemon
//! would. All data lives in `TempDir`s; nothing touches
//! `~/.local/share/nomai`. Requires `git` + `git-lfs` on PATH (same as the
//! unit tests in `handlers/sync.rs`).
//!
//! Coverage spans core scenarios: clean init, one-way pull, two-way converge,
//! conflict surfaces `SyncConflict`, and conflict resolved →
//! `rebase --continue` resumes and converges.

mod common;

use std::process::Command;

use common::SyncTestHarness;
use nomai_protocol::error::SYNC_ERROR;

/// Build a bare git remote under a tempfile and return its path plus the
/// TempDir guard that owns it. `--initial-branch main` aligns the bare
/// repo's HEAD with the branch every sync RPC uses, so the first push to
/// it lands on `main` (not `master`) regardless of the system's
/// `init.defaultBranch`.
fn bare_remote() -> (tempfile::TempDir, std::path::PathBuf) {
    let bare = tempfile::tempdir().unwrap();
    let path = bare.path().to_path_buf();
    let status = Command::new("git")
        .args([
            "init",
            "--bare",
            "--initial-branch",
            "main",
            path.to_str().unwrap(),
        ])
        .status()
        .expect("spawn git init --bare");
    assert!(status.success(), "git init --bare failed");
    (bare, path)
}

/// Two devices converge: A pushes an entry, B pulls it and pushes its own,
/// A pulls B's. Verifies real bidirectional sync through the bare remote.
#[tokio::test]
async fn two_devices_converge() {
    let (_bare_guard, bare) = bare_remote();
    let a = SyncTestHarness::new_with_remote(&bare).await;
    let b = SyncTestHarness::new_with_remote(&bare).await;

    // Valid 26-char Crockford-base32 ULIDs (T/E/S/A/B all in-alphabet).
    const ID_A: &str = "01TEST0000000000000000000A";
    const ID_B: &str = "01TEST0000000000000000000B";

    // A: init + write entry + sync (first push to empty remote; pull fails
    // fast on "no remote ref" and the handler falls through to push).
    a.dispatch_init().await;
    a.write_entry(ID_A, "from-a", "hello from A");
    a.dispatch_run().await;

    // B: init + sync. B's own initial commit (.gitignore + .gitattributes)
    // is content-identical to A's, so `pull --rebase` skips it via git's
    // cherry-pick detection and B lands on A's history. B then writes its
    // own entry and syncs back.
    b.dispatch_init().await;
    b.dispatch_run().await;
    assert!(
        b.entry_dir(ID_A).join("entry.nomai").exists(),
        "B should pull A's entry after sync"
    );

    b.write_entry(ID_B, "from-b", "hello from B");
    b.dispatch_run().await;

    // A syncs again and must observe B's entry.
    a.dispatch_run().await;
    assert!(
        a.entry_dir(ID_B).join("entry.nomai").exists(),
        "A should pull B's entry after re-sync"
    );
}

/// Conflict → resolve → recover: the same entry diverges on two devices;
/// the second device's `sync.run` returns `SyncConflict` (code 1007) and
/// leaves the repo mid-rebase. Resolving the conflict markers in the file
/// and re-running `sync.run` drives the `rebase --continue` resume path to
/// completion, converges both devices, and leaves no residual rebase state.
#[tokio::test]
async fn conflict_then_resolve_recovers() {
    let (_bare_guard, bare) = bare_remote();
    let a = SyncTestHarness::new_with_remote(&bare).await;
    let b = SyncTestHarness::new_with_remote(&bare).await;

    const ID: &str = "01TEST0000000000000000000C";

    // Seed: A creates the entry and pushes; B pulls it.
    a.dispatch_init().await;
    a.write_entry(ID, "seed", "seed body");
    a.dispatch_run().await;

    b.dispatch_init().await;
    b.dispatch_run().await;
    assert!(
        b.entry_dir(ID).join("entry.nomai").exists(),
        "B should have the seeded entry before diverging"
    );

    // Diverge: A edits + pushes a new version; B edits its local copy of the
    // SAME entry in a conflicting way. B's next sync.run commits B's version
    // then `pull --rebase` rebases it onto A's pushed version — same file,
    // overlapping lines → rebase conflict.
    a.edit_entry_body(ID, "from-a", "version from A");
    a.dispatch_run().await;

    b.edit_entry_body(ID, "from-b", "version from B");
    let err = b.dispatch_run_err().await;

    // error.rs: rebase conflict maps to SYNC_ERROR (1007) with
    // the conflicted file paths in `data.conflicted_files`.
    assert_eq!(
        err.code, SYNC_ERROR,
        "expected sync conflict (1007), got code {}: {}",
        err.code, err.message
    );
    let files = err
        .data
        .as_ref()
        .and_then(|d| d.get("conflicted_files"))
        .expect("conflict error carries conflicted_files");
    assert!(
        files.is_array(),
        "conflicted_files should be an array, got: {files}"
    );
    assert!(
        !files.as_array().unwrap().is_empty(),
        "conflicted_files should list the diverged entry"
    );
    assert!(
        b.rebase_in_progress(),
        "repo should be mid-rebase after the conflict"
    );

    // Resolve the way a user would: overwrite the conflicted file with clean
    // content (dropping the `<<<<<<<`/`=======`/`>>>>>>>` markers), then
    // `git add` it. The sync.run resume path intentionally does NOT re-add
    // (its comment notes the resumed conflict "already has staged edits"),
    // so staging is the user's job.
    b.resolve_and_stage(ID, "resolved", "merged resolution");

    // Re-run: the handler detects the mid-flight rebase and runs
    // `git rebase --continue` → push → index.sync. Must succeed and clear
    // the rebase state.
    let result = b.dispatch_run().await;
    assert_eq!(result["pushed"], true, "resolved sync should push");
    assert!(
        !b.rebase_in_progress(),
        "no residual .git/rebase-merge after resolution"
    );

    // Convergence: A pulls B's resolved version and observes the merged body.
    a.dispatch_run().await;
    let body = std::fs::read_to_string(a.entry_dir(ID).join("entry.nomai")).unwrap();
    assert!(
        body.contains("merged resolution"),
        "A should converge to B's resolved body: {body}"
    );
}
