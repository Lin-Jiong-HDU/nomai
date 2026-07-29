//! entry.* handlers. Embedding orchestration on create/update lives here
//! (not in core) so core remains sync.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, CreateEntry, EntryListQuery, UpdateEntry};

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

/// Daemon-layer input for `entry.create`. Mirrors core `CreateEntry` except
/// `attachments` carries base64 strings (MCP tools/call is text-only) instead
/// of raw bytes. `call` decodes via `attachment::decode_attachments` before
/// constructing the core struct. `serde::Deserialize` only — `Create` uses a
/// hand-written `json!` schema (see `Create::input_schema`).
#[derive(Deserialize)]
struct CreateEntryInput {
    title: String,
    blocks: Vec<nomai_core::block_model::BlockInput>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    attrs: Option<Value>,
    #[serde(default)]
    source: Option<String>,
    /// `{filename: base64_string}` — decoded to bytes before reaching core.
    #[serde(default)]
    attachments: Option<std::collections::HashMap<String, String>>,
}

/// Wrap a sync closure as a spawn_blocking task, mapping JoinError to CoreError.
///
/// Returns a nested `Result<Result<T, CoreError>, CoreError>` so callers can use
/// `??` to flatten both the join-error layer and the inner core-error layer.
pub(crate) async fn blocking<F, T>(f: F) -> Result<Result<T, CoreError>, CoreError>
where
    F: FnOnce() -> Result<T, CoreError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| CoreError::Config(format!("blocking task join error: {e}")))
}

pub struct Create;
#[async_trait]
impl RpcHandler for Create {
    fn method(&self) -> &'static str {
        "entry.create"
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Create a new knowledge entry with title and content blocks. Chunks and embeddings are derived automatically. Returns the created entry with its generated ULID."
    }
    fn input_schema(&self) -> Option<Value> {
        let block_input = json!({
            "type": "object",
            "properties": {
                "type": {"type": "string", "description": "block type (e.g. \"note\", \"claim\", \"question\")"},
                "text": {"type": "string"},
                "attrs": {"type": "object"}
            },
            "required": ["type", "text"],
            "additionalProperties": false
        });
        Some(json!({
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "blocks": {"type": "array", "items": block_input, "minItems": 1},
                "tags": {"type": "array", "items": {"type": "string"}},
                "attrs": {"type": "object"},
                "source": {"type": "string"},
                "attachments": {
                    "type": "object",
                    "description": "Sibling attachment files (image/PDF/...), keyed by filename. Values are base64-encoded bytes. Referenced by @image/@source block `src` attrs.",
                    "additionalProperties": {"type": "string"}
                }
            },
            "required": ["title", "blocks"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let input: CreateEntryInput = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let attachments = crate::handlers::attachment::decode_attachments(
            input.attachments.unwrap_or_default(),
            daemon.attachment_max_bytes,
        )?;
        let create = CreateEntry {
            title: input.title,
            blocks: input.blocks,
            tags: input.tags,
            attrs: input.attrs,
            source: input.source,
            attachments: Some(attachments),
        };

        let entries = daemon.entries.clone();
        let entry = blocking(move || entries.create(create)).await??;

        // 0.2.3: embed the entry's chunks so search.semantic works. Was a v1
        // gap (the "background embedder" was never implemented); now done
        // synchronously in-handler. Entry is already committed; embed failure
        // returns provider error (1002) without rolling back the entry.
        crate::handlers::embed::embed_entry_chunks(daemon, entry.id, false).await?;

        // Invalidate search cache (new entry affects both search RPCs).
        daemon.search_cache.bump_generation();

        serde_json::to_value(&entry).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Get;
#[async_trait]
impl RpcHandler for Get {
    fn method(&self) -> &'static str {
        "entry.get"
    }
    fn description(&self) -> &'static str {
        "Fetch a single entry by ULID. Returns error 1001 if not found."
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

        let entries = daemon.entries.clone();
        let include_benchmark = daemon
            .benchmark
            .as_ref()
            .is_some_and(|runtime| runtime.is_active());
        let entry = blocking(move || {
            if include_benchmark {
                entries.get_with_benchmark(p.id)
            } else {
                entries.get(p.id)
            }
        })
        .await??;

        serde_json::to_value(&entry).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Update;
#[async_trait]
impl RpcHandler for Update {
    fn method(&self) -> &'static str {
        "entry.update"
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Update an entry's metadata (title, tags, attrs, source) by ULID. Cannot change blocks; use block.* for that. Invalidates search cache. Returns the updated entry."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "id": crate::handlers::params::ulid_schema(),
                "title": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "attrs": {"type": "object"},
                "source": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "null"}
                    ],
                    "description": "Set source (string) or clear it (null). Omit to leave unchanged."
                }
            },
            "required": ["id"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
            #[serde(flatten)]
            fields: UpdateEntry,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let id_for_update = p.id;
        let fields = p.fields;
        let updated = blocking(move || entries.update(id_for_update, fields)).await??;

        // entry.update touches metadata only; FTS is per-block and
        // updated automatically when blocks change. No embedding re-trigger
        // is needed at this layer.

        // Invalidate search cache (fulltext returns entry snapshot).
        daemon.search_cache.bump_generation();

        serde_json::to_value(&updated).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Delete;
#[async_trait]
impl RpcHandler for Delete {
    fn method(&self) -> &'static str {
        "entry.delete"
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Delete an entry by ULID. CASCADE removes its blocks and chunks; the search cache is invalidated. Returns {deleted: true, id}."
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
        let id_for_ack = p.id;

        // Deleting the entry CASCADEs blocks → chunks; the V9
        // chunks_ad AFTER DELETE trigger cleans vec_chunk_embeddings when
        // each chunk row goes away. No manual N+1 walk needed here.
        let entries = daemon.entries.clone();
        blocking(move || entries.delete(p.id)).await??;

        // Invalidate search cache.
        daemon.search_cache.bump_generation();

        // Mirror block.delete ack shape — include the id.
        Ok(json!({ "deleted": true, "id": id_for_ack.to_string() }))
    }
}

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "entry.list"
    }
    fn description(&self) -> &'static str {
        "List entries with optional tag filter, pagination, and ordering. Default limit 50, offset 0, order created_desc. Set include_blocks=true to inline block content (avoids N+1 follow-up entry.get calls). Returns {items, total, has_more}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "tag": {"type": "string"},
                "limit": {"type": "integer", "minimum": 0, "default": 50},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
                "order": {
                    "type": "string",
                    "enum": ["created_desc", "created_asc", "updated_desc", "updated_asc"],
                    "default": "created_desc"
                },
                "include_blocks": {"type": "boolean"},
                "transient": {
                    "type": "boolean",
                    "description": "Filter by short-term marker: true → only transient entries, false → only long-term, omit → all."
                }
            },
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: EntryListQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let include_benchmark = daemon
            .benchmark
            .as_ref()
            .is_some_and(|runtime| runtime.is_active());
        let result = blocking(move || {
            if include_benchmark {
                entries.list_with_benchmark(query)
            } else {
                entries.list(query)
            }
        })
        .await??;

        Ok(json!({
            "items": result.items,
            "total": result.total,
            "has_more": result.has_more,
        }))
    }
}

pub struct PurgeTransient;
#[async_trait]
impl RpcHandler for PurgeTransient {
    fn method(&self) -> &'static str {
        "entries.purge_transient"
    }
    fn is_mutating(&self) -> bool {
        // Real (dry_run=false) deletes remove entry directories from the
        // work-tree; mark mutating so the dispatcher serializes against
        // sync.run's rebase. dry_run=true previews acquire the lock too —
        // harmless (no write), and cheaper/safer than branching on params.
        true
    }
    fn description(&self) -> &'static str {
        "Purge transient (short-term) entries — those created with attrs.transient=true. SAFE BY DEFAULT: returns a preview without deleting (dry_run defaults to true). Call again with dry_run=false to actually delete. Only affects transient entries; permanent entries are never touched. Use older_than_secs to limit to entries older than a threshold (e.g. 604800 for 7 days). Returns {dry_run, count, entries:[{id,title,created_at}...max50], truncated} in preview mode, or {dry_run:false, deleted, ids, failed:[{id,error}]} in real mode."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "older_than_secs": {"type": "integer", "minimum": 0,
                    "description": "Only purge transient entries older than this many seconds. Omit = all transient entries."},
                "dry_run": {"type": "boolean", "default": true,
                    "description": "If true (default), return a preview without deleting. Set false to actually delete."}
            },
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(serde::Deserialize)]
        struct P {
            #[serde(default)]
            older_than_secs: Option<u64>,
            #[serde(default = "default_dry_run")]
            dry_run: bool,
        }
        fn default_dry_run() -> bool {
            true
        }

        let p: P = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let older_than = p.older_than_secs.map(std::time::Duration::from_secs);

        let entries = daemon.entries.clone();
        let result = blocking(move || entries.purge_transient(older_than, p.dry_run)).await??;

        // Real deletes invalidate the search cache (purged entries may still
        // appear in cached hits). Bump generation so the next search recomputes.
        if !result.dry_run && !result.deleted_ids.is_empty() {
            daemon.search_cache.bump_generation();
        }

        if result.dry_run {
            let entries_json: Vec<Value> = result
                .candidates
                .iter()
                .map(|c| json!({ "id": c.id, "title": c.title, "created_at": c.created_at }))
                .collect();
            Ok(json!({
                "dry_run": true,
                "count": result.total_candidates,
                "entries": entries_json,
                "truncated": result.truncated,
            }))
        } else {
            let ids: Vec<String> = result.deleted_ids.iter().map(|u| u.to_string()).collect();
            let failed: Vec<Value> = result
                .failed
                .iter()
                .map(|(id, e)| json!({ "id": id, "error": e }))
                .collect();
            Ok(json!({
                "dry_run": false,
                "deleted": result.deleted_ids.len(),
                "ids": ids,
                "failed": failed,
            }))
        }
    }
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
    fn create_schema_accepts_title_and_blocks() {
        let schema = Create.input_schema().unwrap();
        let valid = json!({
            "title": "note",
            "blocks": [{"type": "note", "text": "hi"}]
        });
        assert!(validate(&schema, &valid).is_ok());
    }

    #[test]
    fn create_schema_rejects_missing_title() {
        let schema = Create.input_schema().unwrap();
        let invalid = json!({"blocks": [{"type": "note", "text": "hi"}]});
        assert!(validate(&schema, &invalid).is_err());
    }

    #[test]
    fn create_schema_rejects_missing_blocks() {
        let schema = Create.input_schema().unwrap();
        let invalid = json!({"title": "note"});
        assert!(validate(&schema, &invalid).is_err());
    }

    #[test]
    fn get_schema_accepts_valid_id() {
        let schema = Get.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn get_schema_rejects_missing_id() {
        let schema = Get.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn update_schema_accepts_only_id() {
        let schema = Update.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn update_schema_rejects_missing_id() {
        let schema = Update.input_schema().unwrap();
        assert!(validate(&schema, &json!({"title": "x"})).is_err());
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
    fn list_schema_accepts_empty_object() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_ok());
    }

    #[test]
    fn list_schema_accepts_limit() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({"limit": 10})).is_ok());
    }

    #[test]
    fn list_schema_accepts_transient_filter() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({"transient": true})).is_ok());
        assert!(validate(&schema, &json!({"transient": false})).is_ok());
        assert!(validate(&schema, &json!({})).is_ok());
    }

    #[test]
    fn purge_transient_schema_defaults_dry_run_true() {
        let schema = PurgeTransient.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_ok());
        assert!(validate(&schema, &json!({"older_than_secs": 3600})).is_ok());
        assert!(validate(&schema, &json!({"dry_run": false})).is_ok());
    }
}
