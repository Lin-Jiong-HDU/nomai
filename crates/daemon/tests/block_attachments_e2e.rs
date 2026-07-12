//! e2e dispatch tests for `block.append` / `block.update` `attachments`
//! (Plan 3 Task 3).
//!
//! These exercise the full RPC path: client sends base64 strings in
//! `attachments`, daemon decodes via `decode_attachments`, pre-validates via
//! `EntryService::write_attachments_and_validate` (BEFORE the block op, so a
//! validation failure leaves no block row), then `BlockService::append/update`
//! runs and the .nomai file is re-rendered.
//!
//! Order in-handler: decode → write_attachments_and_validate →
//! block_service().append/update → rerender → embed.

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

/// Create a minimal entry (one note block) and return its id string.
/// `block.append` tests use this as the seed entry to append onto.
async fn seed_entry(daemon: &Daemon) -> String {
    let create = daemon
        .dispatch(req(
            "entry.create",
            json!({"title":"seed","blocks":[{"type":"note","text":"seed"}]}),
        ))
        .await;
    assert!(create.error.is_none(), "{:?}", create.error);
    create.result.unwrap()["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn block_append_with_attachments_writes_sibling_and_validates() {
    use base64::prelude::*;

    let daemon = build_test_daemon().await;
    let entry_id = seed_entry(&daemon).await;

    // Client sends a PNG as base64 alongside an @image block referencing it.
    let png_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00".to_vec();
    let b64 = BASE64_STANDARD.encode(&png_bytes);

    let resp = daemon
        .dispatch(req(
            "block.append",
            json!({
                "entry_id": entry_id,
                "type": "image",
                "text": "cap",
                "attrs": {"src": "y.png"},
                "attachments": {"y.png": b64}
            }),
        ))
        .await;
    assert!(resp.error.is_none(), "{:?}", resp.error);
    let block = resp.result.unwrap();
    assert_eq!(block["type"], "image");

    // Sibling was written: attachment.read returns the same base64.
    let read = daemon
        .dispatch(req(
            "attachment.read",
            json!({ "entry_id": entry_id, "filename": "y.png" }),
        ))
        .await;
    assert!(read.error.is_none(), "{:?}", read.error);
    assert_eq!(read.result.unwrap()["base64"], b64);
}

#[tokio::test]
async fn block_append_rejects_image_missing_src_before_block_created() {
    let daemon = build_test_daemon().await;
    let entry_id = seed_entry(&daemon).await;

    // @image block with no src, no attachments → Validation. AND because
    // pre-validate runs before BlockService::append, no block row is created:
    // the entry still has only its seed block.
    let resp = daemon
        .dispatch(req(
            "block.append",
            json!({
                "entry_id": entry_id,
                "type": "image",
                "text": "cap"
            }),
        ))
        .await;
    let err = resp.error.expect("missing src must error");
    assert_eq!(err.code, 1003, "{:?}", err);
    assert_eq!(err.message, "image block missing required attr: src");

    // Atomicity: list still has the single seed block (no new row).
    let list = daemon
        .dispatch(req("block.list", json!({ "entry_id": entry_id })))
        .await;
    assert!(list.error.is_none(), "{:?}", list.error);
    assert_eq!(
        list.result.unwrap()["total"],
        1,
        "no block created on validation failure"
    );
}

#[tokio::test]
async fn block_append_rejects_image_src_not_found_before_block_created() {
    // Companion to the missing-src case: src IS provided but no attachment
    // supplies it → "declared source not found", and still no block row.
    let daemon = build_test_daemon().await;
    let entry_id = seed_entry(&daemon).await;

    let resp = daemon
        .dispatch(req(
            "block.append",
            json!({
                "entry_id": entry_id,
                "type": "image",
                "text": "cap",
                "attrs": {"src": "ghost.png"}
            }),
        ))
        .await;
    let err = resp.error.expect("dangling src must error");
    assert_eq!(err.code, 1003, "{:?}", err);
    assert!(err.message.contains("declared source not found: ghost.png"));

    let list = daemon
        .dispatch(req("block.list", json!({ "entry_id": entry_id })))
        .await;
    assert_eq!(list.result.unwrap()["total"], 1);
}

#[tokio::test]
async fn block_update_changes_src_and_validates_new_attachment() {
    use base64::prelude::*;

    let daemon = build_test_daemon().await;

    // Create an entry whose first block is an @image with src=old.png, and
    // supply old.png so creation validates.
    let png_old = b"\x89PNG old".to_vec();
    let png_new = b"\x89PNG new".to_vec();
    let b64_old = BASE64_STANDARD.encode(&png_old);
    let b64_new = BASE64_STANDARD.encode(&png_new);

    let create = daemon
        .dispatch(req(
            "entry.create",
            json!({
                "title": "img",
                "blocks": [
                    {"type": "image", "text": "v1", "attrs": {"src": "old.png"}}
                ],
                "attachments": {"old.png": b64_old}
            }),
        ))
        .await;
    assert!(create.error.is_none(), "{:?}", create.error);
    let entry_id = create.result.unwrap()["id"].as_str().unwrap().to_string();

    // Find the image block's id.
    let list = daemon
        .dispatch(req("block.list", json!({ "entry_id": entry_id })))
        .await;
    let block_id = list.result.unwrap()["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Update src → new.png, supplying new.png as an attachment. Pre-validate
    // uses the post-update src, so new.png must be written + resolve.
    let resp = daemon
        .dispatch(req(
            "block.update",
            json!({
                "id": block_id,
                "attrs": {"src": "new.png"},
                "attachments": {"new.png": b64_new}
            }),
        ))
        .await;
    assert!(resp.error.is_none(), "{:?}", resp.error);

    // new.png is readable, block.get shows src=new.png.
    let read = daemon
        .dispatch(req(
            "attachment.read",
            json!({ "entry_id": entry_id, "filename": "new.png" }),
        ))
        .await;
    assert_eq!(read.result.unwrap()["base64"], b64_new);

    let get = daemon
        .dispatch(req("block.get", json!({ "id": block_id })))
        .await;
    assert_eq!(get.result.unwrap()["attrs"]["src"], "new.png");
}

#[tokio::test]
async fn block_update_to_image_without_src_rejects_and_leaves_block_unchanged() {
    let daemon = build_test_daemon().await;
    let entry_id = seed_entry(&daemon).await;

    // Grab the seed note block's id.
    let list = daemon
        .dispatch(req("block.list", json!({ "entry_id": entry_id })))
        .await;
    let block_id = list.result.unwrap()["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let original_text = "seed";

    // Update note → image with no src → Validation. Block must be unchanged
    // after (pre-validate fires before BlockService::update).
    let resp = daemon
        .dispatch(req(
            "block.update",
            json!({
                "id": block_id,
                "type": "image"
            }),
        ))
        .await;
    let err = resp.error.expect("note→image w/o src must error");
    assert_eq!(err.code, 1003, "{:?}", err);
    assert_eq!(err.message, "image block missing required attr: src");

    // Block is unchanged.
    let get = daemon
        .dispatch(req("block.get", json!({ "id": block_id })))
        .await;
    let block = get.result.unwrap();
    assert_eq!(block["type"], "note", "block type unchanged");
    assert_eq!(block["text"], original_text, "block text unchanged");
}
