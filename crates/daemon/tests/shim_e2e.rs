//! End-to-end smoke: spawn the real `nomai-daemon` in shim mode and confirm
//! the resident-daemon model serves a read-only RPC over stdio. Lives in
//! `tests/` (separate integration target) so unit suites stay fast.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

fn binary() -> std::path::PathBuf {
    // Cargo sets CARGO_BIN_EXE_<name> with the binary name EXACTLY as declared
    // (hyphens preserved), unlike CARGO_CRATE_NAME which uses underscores.
    // The bin name is `nomai-daemon`, so the env var is `CARGO_BIN_EXE_nomai-daemon`.
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_nomai-daemon"))
}

fn write_config(dir: &std::path::Path) -> std::path::PathBuf {
    let cfg = dir.join("config.toml");
    let db = dir.join("db.sqlite");
    let toml = format!(
        r#"
[data]
db_path = "{db}"

[embedding]
base_url = "https://example.invalid/v1"
api_key_env = "NOMAI_E2E_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.invalid/v1"
api_key_env = "NOMAI_E2E_KEY"
model = "x"

[serve]
idle_timeout_secs = 2
"#,
        db = db.display()
    );
    std::fs::write(&cfg, toml).unwrap();
    cfg
}

#[test]
fn shim_serves_a_readonly_rpc_via_resident_daemon() {
    let dir = tempfile::TempDir::new().unwrap();
    let cfg = write_config(dir.path());

    let mut shim = Command::new(binary())
        .arg("--config")
        .arg(&cfg)
        .env("NOMAI_E2E_KEY", "k")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn shim");

    // Wait for the resident daemon to be accepting on its socket — poll
    // instead of a fixed sleep so we proceed the instant it's ready (and fail
    // loudly if it never comes up), removing the flake risk of a blind delay.
    // The probe stream is held for the whole test so the daemon always sees an
    // active connection and can't idle out (idle_timeout_secs = 2) before we
    // drive the shim.
    let sock = dir.path().join("run").join("nomai.sock");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let _probe = loop {
        match std::os::unix::net::UnixStream::connect(&sock) {
            Ok(s) => break s,
            Err(_) => {
                if std::time::Instant::now() > deadline {
                    panic!("resident daemon socket never came up at {sock:?}");
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
    };

    let mut stdin = shim.stdin.take().unwrap();
    // entry.list is read-only — no embedding call.
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"entry.list","params":{{}}}}"#
    )
    .unwrap();
    let _ = stdin.flush();

    let stdout = shim.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read response");
    let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(v["id"], 1);
    // Strong assertion: entry.list is read-only and must succeed — accepting
    // an `error` here (the old weak `result || error` form) would mask a
    // regression in the resident-daemon serve path.
    assert!(v.get("result").is_some(), "entry.list must succeed: {v}");

    // The poll-connect above already proved the resident-daemon path was taken
    // (not the in-process fallback): the spawned `--serve` child bound the
    // socket and accepted our probe. A separate `sock.exists()` check would be
    // strictly weaker and redundant.

    drop(stdin); // → shim exits → resident daemon idles out (2s).
    let _ = shim.wait();
}
