//! e2e dispatch tests for attachment.read / attachment.list (Plan 3 Task 1).
//!
//! Task 1 ships the read/list RPCs only; the base64 `attachments` param on
//! entry.create / block.append / block.update is Task 2/3. So this test seeds
//! an attachment via the core ContentStore directly, then exercises the new
//! RPCs end-to-end through `Daemon::dispatch` (same construction pattern as
//! snapshot_test.rs).

use std::sync::Arc;

use async_trait::async_trait;
use nomai_daemon::daemon::Daemon;
use nomai_protocol::{Id, JSONRPC_VERSION, Request};
use serde_json::{Value, json};

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

fn req(method: &str, params: Value) -> Request {
    Request {
        jsonrpc: JSONRPC_VERSION.into(),
        id: Some(Id::Number(1)),
        method: method.into(),
        params: Some(params),
    }
}

#[tokio::test]
async fn attachment_list_and_read_round_trip() {
    use base64::prelude::*;

    let daemon = build_test_daemon().await;

    // 1. Create an entry via RPC (no attachments param yet — that's Task 2).
    //    We then seed the attachment directly via core's ContentStore, which
    //    is the same FS path entry.create's `attachments` will write through.
    let create = daemon
        .dispatch(req(
            "entry.create",
            json!({"title":"img","blocks":[{"type":"note","text":"see sunset.png"}]}),
        ))
        .await;
    assert!(create.error.is_none(), "{:?}", create.error);
    let entry_id_str = create.result.unwrap()["id"].as_str().unwrap().to_string();
    let entry_id: ulid::Ulid = entry_id_str.parse().unwrap();

    // Seed the attachment via core (the FS source-of-truth).
    let png_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00".to_vec();
    daemon
        .entries()
        .content_store()
        .write_attachment(entry_id, "sunset.png", &png_bytes)
        .expect("seed attachment");

    // 2. attachment.list → items contains sunset.png with correct size.
    let list = daemon
        .dispatch(req("attachment.list", json!({ "entry_id": entry_id_str })))
        .await;
    assert!(list.error.is_none(), "{:?}", list.error);
    let items = list.result.unwrap()["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["filename"], "sunset.png");
    assert_eq!(items[0]["size"], png_bytes.len() as u64);

    // 3. attachment.read → mime from ext + base64 round-trips back to bytes.
    let b64_expected = BASE64_STANDARD.encode(&png_bytes);
    let read = daemon
        .dispatch(req(
            "attachment.read",
            json!({ "entry_id": entry_id_str, "filename": "sunset.png" }),
        ))
        .await;
    assert!(read.error.is_none(), "{:?}", read.error);
    let result = read.result.unwrap();
    assert_eq!(result["filename"], "sunset.png");
    assert_eq!(result["mime"], "image/png");
    assert_eq!(result["base64"], b64_expected);
    // Decode the returned base64 and confirm byte-for-byte round-trip.
    let decoded = BASE64_STANDARD
        .decode(result["base64"].as_str().unwrap())
        .unwrap();
    assert_eq!(decoded, png_bytes);
}

#[tokio::test]
async fn attachment_read_missing_file_returns_validation_error() {
    let daemon = build_test_daemon().await;

    let create = daemon
        .dispatch(req(
            "entry.create",
            json!({"title":"empty","blocks":[{"type":"note","text":"x"}]}),
        ))
        .await;
    let entry_id = create.result.unwrap()["id"].as_str().unwrap().to_string();

    // read on a never-written attachment → core returns Validation → RPC 1003.
    let read = daemon
        .dispatch(req(
            "attachment.read",
            json!({ "entry_id": entry_id, "filename": "ghost.png" }),
        ))
        .await;
    let err = read.error.expect("missing attachment should error");
    assert_eq!(err.code, 1003);
}

#[tokio::test]
async fn attachment_list_empty_when_no_attachments() {
    let daemon = build_test_daemon().await;

    let create = daemon
        .dispatch(req(
            "entry.create",
            json!({"title":"bare","blocks":[{"type":"note","text":"x"}]}),
        ))
        .await;
    let entry_id = create.result.unwrap()["id"].as_str().unwrap().to_string();

    let list = daemon
        .dispatch(req("attachment.list", json!({ "entry_id": entry_id })))
        .await;
    assert!(list.error.is_none(), "{:?}", list.error);
    let items = list.result.unwrap()["items"].as_array().unwrap().clone();
    assert!(items.is_empty(), "freshly created entry has no attachments");
}

#[tokio::test]
async fn attachment_read_mime_for_other_extensions() {
    // mime_for_ext covers pdf/html/txt/md/gif/webp/etc. Smoke a non-image
    // type end-to-end to confirm the whitelist branch wires through.
    let daemon = build_test_daemon().await;

    let create = daemon
        .dispatch(req(
            "entry.create",
            json!({"title":"doc","blocks":[{"type":"note","text":"x"}]}),
        ))
        .await;
    let entry_id: ulid::Ulid = create.result.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    daemon
        .entries()
        .content_store()
        .write_attachment(entry_id, "report.pdf", b"%PDF-1.4")
        .unwrap();

    let read = daemon
        .dispatch(req(
            "attachment.read",
            json!({ "entry_id": entry_id.to_string(), "filename": "report.pdf" }),
        ))
        .await;
    let result = read.result.unwrap();
    assert_eq!(result["mime"], "application/pdf");
}
