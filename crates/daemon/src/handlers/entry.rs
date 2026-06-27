//! entry.* handlers. Embedding orchestration on create/update lives here
//! (not in core) so core remains sync.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, CreateEntry, EntryListQuery, UpdateEntry};

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

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
                "source": {"type": "string"}
            },
            "required": ["title", "blocks"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let input: CreateEntry = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let entry = blocking(move || entries.create(input)).await??;

        // Plan 4: entry-level embeddings are retired; chunk-level embeddings
        // are managed via BlockService::create_in_tx + a separate background
        // embedder. For v1, entry.create no longer triggers embedding work.

        // Spec 7: invalidate search cache (new entry affects both search RPCs).
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
        let entry = blocking(move || entries.get(p.id)).await??;

        serde_json::to_value(&entry).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Update;
#[async_trait]
impl RpcHandler for Update {
    fn method(&self) -> &'static str {
        "entry.update"
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

        // Plan 4: entry.update touches metadata only; FTS is per-block and
        // updated automatically when blocks change. No embedding re-trigger
        // is needed at this layer.

        // Spec 7: invalidate search cache (fulltext returns entry snapshot).
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

        // Plan 5: deleting the entry CASCADEs blocks → chunks; the V9
        // chunks_ad AFTER DELETE trigger cleans vec_chunk_embeddings when
        // each chunk row goes away. No manual N+1 walk needed here.
        let entries = daemon.entries.clone();
        blocking(move || entries.delete(p.id)).await??;

        // Spec 7: invalidate search cache.
        daemon.search_cache.bump_generation();

        // F-entry-1: mirror block.delete ack shape — include the id.
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
                "include_blocks": {"type": "boolean"}
            },
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: EntryListQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        let result = blocking(move || entries.list(query)).await??;

        Ok(json!({
            "items": result.items,
            "total": result.total,
            "has_more": result.has_more,
        }))
    }
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params).map_err(|errs| {
            errs.map(|e| format!("{e}")).collect::<Vec<_>>()
        })
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
}
