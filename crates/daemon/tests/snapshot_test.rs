//! Snapshot test: tools/list output must match the committed JSON file.
//!
//! To regenerate the snapshot (after intentional descriptor changes):
//!     UPDATE_SNAPSHOTS=1 cargo test --test snapshot_test

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use nomai_daemon::daemon::Daemon;
use nomai_protocol::{Id, JSONRPC_VERSION, Request};

fn snapshot_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("tools_list_snapshot.json")
}

async fn build_test_daemon() -> Daemon {
    let entries = Arc::new(nomai_core::EntryService::for_test().unwrap());

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
        ) -> Result<nomai_providers::CompletionResponse, nomai_protocol::ProviderError> {
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

    Daemon::from_services(
        entries.conn_for_test(),
        entries.content_store().clone(),
        Arc::new(NullEmbed),
        Arc::new(NullLlm),
        8,
        1024,
        "test-embed",
        100_000,
    )
    .expect("daemon builds")
}

#[tokio::test]
async fn tools_list_matches_snapshot() {
    let daemon = build_test_daemon().await;
    let req = Request {
        jsonrpc: JSONRPC_VERSION.into(),
        id: Some(Id::Number(1)),
        method: "tools/list".into(),
        params: None,
    };
    let resp = daemon.dispatch(req).await;
    let result = resp.result.expect("tools/list returns a result");

    // Pretty-print with sorted keys for stable diffs.
    let pretty = serde_json::to_string_pretty(&result).unwrap();

    let path = snapshot_path();
    if env::var("UPDATE_SNAPSHOTS").is_ok() {
        fs::write(&path, &pretty).expect("write snapshot");
        eprintln!("snapshot updated: {}", path.display());
        return;
    }

    let expected = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "read snapshot {}: {e}\nHINT: run UPDATE_SNAPSHOTS=1 cargo test --test snapshot_test to bootstrap",
            path.display()
        )
    });
    assert_eq!(
        pretty.trim(),
        expected.trim(),
        "tools/list snapshot drift.\nHINT: if intentional, run UPDATE_SNAPSHOTS=1 cargo test --test snapshot_test\npath: {}",
        path.display()
    );
}
