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
use serde_json::{Value, json};

use nomai_core::nomai_format::{Block as ParserBlock, BlockType, NomaiDoc};
use nomai_core::{CoreError, EntryService};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use nomai_protocol::method::block::APPEND as BLOCK_APPEND;
use nomai_protocol::method::block::DELETE as BLOCK_DELETE;
use nomai_protocol::method::block::LIST as BLOCK_LIST;
use nomai_protocol::method::block::UPDATE as BLOCK_UPDATE;

#[derive(serde::Deserialize, schemars::JsonSchema)]
pub struct AppendParams {
    #[schemars(schema_with = "crate::handlers::params::ulid_field_schema")]
    pub entry_id: ulid::Ulid,
    pub r#type: String,
    pub text: String,
    #[serde(default)]
    #[schemars(default)]
    pub attrs: Option<serde_json::Value>,
}

pub struct Append;

#[async_trait]
impl RpcHandler for Append {
    fn method(&self) -> &'static str {
        BLOCK_APPEND
    }
    fn description(&self) -> &'static str {
        "Append a new block (type, text, optional attrs) to an entry. Computes the next ordinal and re-renders the entry's .nomai file. Invalidates search cache. Returns the created block."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(AppendParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: AppendParams = serde_json::from_value(params)
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

        // Spec 7: invalidate search cache.
        daemon.search_cache.bump_generation();

        serde_json::to_value(&block).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
pub struct UpdateParams {
    #[schemars(schema_with = "crate::handlers::params::ulid_field_schema")]
    pub id: ulid::Ulid,
    #[serde(default)]
    #[schemars(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub text: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub attrs: Option<serde_json::Value>,
}

pub struct Update;

#[async_trait]
impl RpcHandler for Update {
    fn method(&self) -> &'static str {
        BLOCK_UPDATE
    }
    fn description(&self) -> &'static str {
        "Update a block's type, text, or attrs by ULID. At least one of type/text/attrs must be present. Re-renders the entry's .nomai file. Invalidates search cache. Returns the updated block."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(UpdateParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: UpdateParams = serde_json::from_value(params)
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

        // Spec 7: invalidate search cache.
        daemon.search_cache.bump_generation();

        serde_json::to_value(&block).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Delete;

#[async_trait]
impl RpcHandler for Delete {
    fn method(&self) -> &'static str {
        BLOCK_DELETE
    }
    fn description(&self) -> &'static str {
        "Delete a block by ULID. Re-renders the parent entry's .nomai file and invalidates the search cache. Returns {deleted: true, id}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::ulid_param_schema())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries: Arc<EntryService> = daemon.entries.clone();
        let id = p.id;
        let block = {
            let entries = entries.clone();
            blocking(move || entries.block_service().delete(id)).await??
        };

        // Re-render the entry's .nomai (block list changed). Runs in the
        // same spawn_blocking pattern as Append/Update. The chunks_ad trigger
        // (V9) cleans vec_chunk_embeddings when CASCADE removes the block's
        // chunks; no manual loop needed here.
        rerender_entry_nomai(&entries, block.entry_id).await?;

        // Spec 7: invalidate search cache.
        daemon.search_cache.bump_generation();

        Ok(json!({"deleted": true, "id": id.to_string()}))
    }
}

pub struct List;

#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        BLOCK_LIST
    }
    fn description(&self) -> &'static str {
        "List all blocks of an entry, in ordinal order. Returns {items, total}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": { "entry_id": crate::handlers::params::ulid_schema() },
            "required": ["entry_id"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            entry_id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries: Arc<EntryService> = daemon.entries.clone();
        let entry_id = p.entry_id;
        let result = {
            let entries = entries.clone();
            blocking(move || entries.block_service().list(entry_id)).await??
        };

        Ok(json!({
            "items": result.items,
            "total": result.total,
        }))
    }
}

/// Load entry metadata + blocks via EntryService, render a `NomaiDoc`, and
/// atomically overwrite the entry's `.nomai` file via `ContentStore`.
///
/// Called after any block-level mutation (append, update, delete) so the
/// FS representation stays in sync with SQLite. Uses `EntryService::get`,
/// which already populates `entry.blocks`.
///
/// Plan 5 final review (I1): also refreshes `entries.fs_mtime` to match the
/// newly-written file. Without this, the next `sync_from_fs` would see a
/// stale mtime and trigger a full reindex of the entry on every boot.
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
        entries.content_store().write_entry(entry_id, &doc)?;
        // Plan 5 final review (I1): refresh entries.fs_mtime to match the
        // newly-written .nomai file. Without this, the next sync_from_fs
        // sees a stale mtime and triggers a full reindex of the entry on
        // every daemon boot, undermining the trigger-based cleanup shipped
        // in Plan 5 Task 1. We also bump entries.updated_at so the row
        // reflects the latest mutation.
        let new_mtime = entries
            .content_store()
            .entry_mtime(entry_id)
            .ok_or_else(|| CoreError::Storage(rusqlite::Error::ExecuteReturnedResults))?;
        let now = chrono::Utc::now().to_rfc3339();
        {
            let conn = entries.conn_for_test();
            let guard = conn.lock().unwrap();
            guard
                .execute(
                    "UPDATE entries SET fs_mtime = ?1, updated_at = ?2 WHERE id = ?3",
                    rusqlite::params![new_mtime.to_rfc3339(), &now, entry_id.to_string()],
                )
                .map_err(CoreError::Storage)?;
        }
        Ok(())
    })
    .await?
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params)
            .map_err(|errs| errs.map(|e| format!("{e}")).collect::<Vec<_>>())
    }

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    #[test]
    fn append_schema_accepts_valid() {
        let schema = Append.input_schema().unwrap();
        let valid = json!({
            "entry_id": ULID,
            "type": "note",
            "text": "hello"
        });
        assert!(validate(&schema, &valid).is_ok());
    }

    #[test]
    fn append_schema_rejects_missing_entry_id() {
        let schema = Append.input_schema().unwrap();
        let invalid = json!({"type": "note", "text": "hi"});
        assert!(validate(&schema, &invalid).is_err());
    }

    #[test]
    fn append_schema_rejects_missing_type() {
        let schema = Append.input_schema().unwrap();
        let invalid = json!({"entry_id": ULID, "text": "hi"});
        assert!(validate(&schema, &invalid).is_err());
    }

    #[test]
    fn append_schema_rejects_missing_text() {
        let schema = Append.input_schema().unwrap();
        let invalid = json!({"entry_id": ULID, "type": "note"});
        assert!(validate(&schema, &invalid).is_err());
    }

    #[test]
    fn update_schema_accepts_only_id() {
        let schema = Update.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn update_schema_rejects_missing_id() {
        let schema = Update.input_schema().unwrap();
        assert!(validate(&schema, &json!({"text": "x"})).is_err());
    }

    #[test]
    fn delete_schema_accepts_valid_id() {
        let schema = Delete.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn delete_schema_rejects_missing_id() {
        let schema = Delete.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn list_schema_accepts_entry_id() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({"entry_id": ULID})).is_ok());
    }

    #[test]
    fn list_schema_rejects_missing_entry_id() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }
}
