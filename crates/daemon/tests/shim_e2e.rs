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

    // Give the shim a moment to bring up the resident daemon.
    std::thread::sleep(std::time::Duration::from_millis(500));

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
    assert!(v.get("result").is_some() || v.get("error").is_some());

    // Confirm the resident-daemon path was taken (not the in-process fallback):
    // a live `nomai.sock` next to the db means the spawned `--serve` child
    // bound the socket and is serving us over it.
    let sock = dir.path().join("run").join("nomai.sock");
    assert!(sock.exists(), "resident daemon socket missing at {sock:?}");

    drop(stdin); // → shim exits → resident daemon idles out (2s).
    let _ = shim.wait();
}
