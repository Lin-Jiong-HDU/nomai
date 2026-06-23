//! block.* handlers. Plan 5 introduces block-level RPCs on top of the
//! Plan 3 blocks storage. `block.append` adds a block to an existing entry
//! (computing the next ordinal) and re-renders the entry's `.nomai` file.
//!
//! The daemon accesses `BlockService` and `ContentStore` through the
//! EntryService accessors (`entries().block_service()`, `entries().content_store()`)
//! because both are co-owned by `EntryService` and share the SQLite connection.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use nomai_core::nomai_format::{Block as ParserBlock, BlockType, NomaiDoc};
use nomai_core::{CoreError, EntryService};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use nomai_protocol::method::block::APPEND as BLOCK_APPEND;
use nomai_protocol::method::block::UPDATE as BLOCK_UPDATE;

pub struct Append;

#[async_trait]
impl RpcHandler for Append {
    fn method(&self) -> &'static str {
        BLOCK_APPEND
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            entry_id: ulid::Ulid,
            r#type: String,
            text: String,
            #[serde(default)]
            attrs: Option<Value>,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries: Arc<EntryService> = daemon.entries.clone();
        let entry_id = p.entry_id;
        let ty = p.r#type;
        let text = p.text;
        let attrs = p.attrs;
        let block = {
            let entries = entries.clone();
            blocking(move || entries.block_service().append(entry_id, ty, text, attrs)).await??
        };

        // Re-render the entry's .nomai (block list changed). Runs in the same
        // spawn_blocking pattern as the rest of this handler family.
        rerender_entry_nomai(&entries, entry_id).await?;

        serde_json::to_value(&block).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Update;

#[async_trait]
impl RpcHandler for Update {
    fn method(&self) -> &'static str {
        BLOCK_UPDATE
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
            #[serde(default)]
            r#type: Option<String>,
            #[serde(default)]
            text: Option<String>,
            #[serde(default)]
            attrs: Option<Value>,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries: Arc<EntryService> = daemon.entries.clone();
        let id = p.id;
        let ty = p.r#type;
        let text = p.text;
        let attrs = p.attrs;
        let block = {
            let entries = entries.clone();
            blocking(move || entries.block_service().update(id, ty, text, attrs)).await??
        };

        // Re-render the entry's .nomai (block text/type/attrs may have
        // changed). Runs in the same spawn_blocking pattern as Append.
        rerender_entry_nomai(&entries, block.entry_id).await?;

        serde_json::to_value(&block).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

/// Load entry metadata + blocks via EntryService, render a `NomaiDoc`, and
/// atomically overwrite the entry's `.nomai` file via `ContentStore`.
///
/// Called after any block-level mutation (append, update, delete) so the
/// FS representation stays in sync with SQLite. Uses `EntryService::get`,
/// which already populates `entry.blocks`.
pub(crate) async fn rerender_entry_nomai(
    entries: &Arc<EntryService>,
    entry_id: ulid::Ulid,
) -> Result<(), CoreError> {
    let entries = entries.clone();
    blocking(move || -> Result<(), CoreError> {
        let entry = entries.get(entry_id)?;
        let parser_blocks: Vec<ParserBlock> = entry
            .blocks
            .iter()
            .map(|b| -> Result<ParserBlock, CoreError> {
                Ok(ParserBlock {
                    r#type: BlockType::from_str(&b.r#type).ok_or_else(|| {
                        CoreError::Validation(format!("unknown block type: {}", b.r#type))
                    })?,
                    text: format!("{}\n", b.text),
                    attrs: b.attrs.as_object().cloned().unwrap_or_default(),
                })
            })
            .collect::<Result<_, _>>()?;
        let doc = NomaiDoc {
            format_version: 1,
            id: entry.id,
            title: entry.title,
            tags: entry.tags,
            attrs: entry.attrs.as_object().cloned().unwrap_or_default(),
            source: entry.source,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            blocks: parser_blocks,
        };
        entries.content_store().write_entry(entry_id, &doc)
    })
    .await?
}
