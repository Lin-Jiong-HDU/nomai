//! attachment.read / attachment.list RPC handlers.
//!
//! Binary attachments are sibling files under the entry directory (Spec 6 §11,
//! multimodal-image Plan 1-3). base64 ↔ bytes conversion happens here at the
//! daemon boundary; core handles only raw bytes.

use std::collections::HashMap;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde_json::{Value, json};

use nomai_core::CoreError;
use nomai_protocol::method::attachment::{LIST, READ};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::handlers::params::ulid_field_schema;
use crate::rpc::RpcHandler;

/// Decode a `{filename: base64_string}` map into `{filename: bytes}`. Any
/// malformed base64 → `Validation("invalid base64 for attachment: <name>")`.
///
/// Shared by `entry.create` / `block.append` / `block.update` wiring
/// (Task 2/3) — those handlers accept base64 over the wire (MCP tools/call
/// is text-only) and call this to recover the raw bytes core expects.
pub(crate) fn decode_attachments(
    atts: HashMap<String, String>,
) -> Result<HashMap<String, Vec<u8>>, CoreError> {
    use base64::prelude::*;
    let mut out = HashMap::with_capacity(atts.len());
    for (filename, b64) in atts {
        let bytes = BASE64_STANDARD.decode(b64.as_bytes()).map_err(|_| {
            CoreError::Validation(format!("invalid base64 for attachment: {filename}"))
        })?;
        out.insert(filename, bytes);
    }
    Ok(out)
}

/// Static MIME lookup from filename extension. Unknown / no extension →
/// `application/octet-stream`. Deliberately a small whitelist (the common
/// attachment types); not an exhaustive media registry.
pub(crate) fn mime_for_ext(filename: &str) -> &'static str {
    let ext = filename.rsplit('.').next().map(|e| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("pdf") => "application/pdf",
        Some("html") | Some("htm") => "text/html",
        Some("txt") | Some("md") => "text/plain",
        _ => "application/octet-stream",
    }
}

#[derive(serde::Deserialize, JsonSchema)]
pub struct ReadParams {
    #[schemars(schema_with = "ulid_field_schema")]
    pub entry_id: ulid::Ulid,
    pub filename: String,
}

pub struct Read;

#[async_trait]
impl RpcHandler for Read {
    fn method(&self) -> &'static str {
        READ
    }
    fn description(&self) -> &'static str {
        "Read a sibling attachment file (image/PDF/...) for an entry. Returns {filename, mime, base64}. The file must already exist under the entry directory (written via entry.create/block.append/block.update `attachments`, or pre-placed)."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(ReadParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: ReadParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let store = daemon.entries.content_store().clone();
        let entry_id = p.entry_id;
        let filename = p.filename.clone();
        let bytes = blocking(move || store.read_attachment(entry_id, &filename)).await??;
        let mime = mime_for_ext(&p.filename);
        // Encode bytes → base64 for transport (MCP tools/call is text-only).
        use base64::prelude::*;
        let b64 = BASE64_STANDARD.encode(&bytes);
        Ok(json!({ "filename": p.filename, "mime": mime, "base64": b64 }))
    }
}

#[derive(serde::Deserialize, JsonSchema)]
pub struct ListParams {
    #[schemars(schema_with = "ulid_field_schema")]
    pub entry_id: ulid::Ulid,
}

pub struct List;

#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        LIST
    }
    fn description(&self) -> &'static str {
        "List sibling attachment files for an entry (excludes entry.nomai). Returns {items: [{filename, size, modified}]}. Empty list if the entry has no attachments."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(ListParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: ListParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let store = daemon.entries.content_store().clone();
        let entry_id = p.entry_id;
        let items = blocking(move || store.list_attachments(entry_id)).await??;
        Ok(json!({ "items": items }))
    }
}
