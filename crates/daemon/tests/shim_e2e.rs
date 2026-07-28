//! End-to-end smoke: spawn the real `nomai-daemon` in shim mode and confirm
//! the resident-daemon model serves a read-only RPC over stdio. Lives in
//! `tests/` (separate integration target) so unit suites stay fast.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use serde_json::json;

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

#[tokio::test]
async fn search_hybrid_returns_entry_granular_results() {
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Start a mock embedding provider on a random port.
    let mock_server = MockServer::start().await;
    let dim: usize = 1536;
    let embedding: Vec<f64> = (0..dim).map(|i| (i % 128) as f64 / 128.0).collect();
    Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [{"object": "embedding", "index": 0, "embedding": embedding}],
            "model": "test",
            "usage": {"prompt_tokens": 1, "total_tokens": 1}
        })))
        .mount(&mock_server)
        .await;

    // Build config pointing at the mock provider.
    let dir = tempfile::TempDir::new().unwrap();
    let db = dir.path().join("db.sqlite");
    let cfg = dir.path().join("config.toml");
    let toml = format!(
        r#"
[data]
db_path = "{db}"

[embedding]
base_url = "{base}"
api_key_env = "NOMAI_E2E_KEY"
model = "test"
dim = {dim}

[llm]
base_url = "{base}"
api_key_env = "NOMAI_E2E_KEY"
model = "test"

[serve]
idle_timeout_secs = 10
"#,
        db = db.display(),
        base = mock_server.uri(),
    );
    std::fs::write(&cfg, toml).unwrap();

    let mut child = Command::new(binary())
        .arg("--config")
        .arg(&cfg)
        .env("NOMAI_E2E_KEY", "k")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn shim");

    let stdin = child.stdin.take().unwrap();
    let reader = BufReader::new(child.stdout.take().unwrap());

    // Wait for the resident daemon socket.
    let sock = dir.path().join("run").join("nomai.sock");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match std::os::unix::net::UnixStream::connect(&sock) {
            Ok(_) => break,
            Err(_) => {
                if std::time::Instant::now() > deadline {
                    panic!("daemon never came up at {sock:?}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }

    // Helper: send JSON-RPC request, return parsed response.
    let mut stdin = stdin;
    let mut reader = reader;
    let mut send = |method: &str, params: serde_json::Value| -> serde_json::Value {
        let req = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        writeln!(stdin, "{req}").unwrap();
        stdin.flush().unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    };

    // Create two entries with different content.
    let e1 = send(
        "entry.create",
        json!({
            "title": "Rust Ownership",
            "blocks": [{"type": "note", "text": "Rust ownership is a unique feature of the language"}]
        }),
    );
    assert!(
        e1.get("result").is_some(),
        "entry.create e1 must succeed: {e1}"
    );

    let e2 = send(
        "entry.create",
        json!({
            "title": "Python Garbage Collection",
            "blocks": [{"type": "note", "text": "Python uses reference counting and generational GC"}]
        }),
    );
    assert!(
        e2.get("result").is_some(),
        "entry.create e2 must succeed: {e2}"
    );

    // Verify the mock server was called for each creation (two chunks → two calls).
    let emb_reqs = mock_server.received_requests().await;
    let n_emb = emb_reqs.as_ref().map(|v| v.len()).unwrap_or(0);
    assert!(
        n_emb >= 2,
        "expected at least 2 embedding requests, got {n_emb}"
    );

    // Search hybrid.
    let resp = send(
        "search.hybrid",
        json!({
            "query": "memory management",
            "limit": 5
        }),
    );
    let result = resp
        .get("result")
        .unwrap_or_else(|| panic!("search.hybrid must succeed: {resp}"));
    let items = result["items"].as_array().unwrap();
    assert!(!items.is_empty(), "expected at least one hybrid result");
    for item in items {
        assert!(item["entry"]["id"].is_string());
        assert!(item["fusion_score"].as_f64().is_some());
        assert!(item["fulltext_rank"].as_u64().is_some());
        assert!(item["semantic_rank"].as_u64().is_some());
        assert!(item["fulltext_score"].as_f64().is_some());
        assert!(item["semantic_score"].as_f64().is_some());
    }

    drop(stdin);
    let _ = child.wait();
}
