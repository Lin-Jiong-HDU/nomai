//! conversation.* handlers — CRUD + append turns + FTS search.
//!
//! All handlers follow the zero-sized-struct + `RpcHandler` trait pattern.
//! SQLite calls use `tokio::task::spawn_blocking` via the `blocking` helper.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{
    AppendTurns, ConversationListQuery, CoreError, CreateConversation, UpdateConversation,
};

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

use super::entry::blocking;

// ── Create ──────────────────────────────────────────────────────────

pub struct Create;
#[async_trait]
impl RpcHandler for Create {
    fn method(&self) -> &'static str {
        "conversation.create"
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Create a new conversation session with optional title, tags, attrs, and initial turns. Returns the created conversation with its generated ULID and any turns."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Conversation title (optional)"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "attrs": {"type": "object"},
                "turns": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "role": {"type": "string", "description": "\"user\", \"assistant\", \"system\", or \"tool\""},
                            "content": {"type": "string", "description": "Turn content in markdown"},
                            "attrs": {"type": "object"}
                        },
                        "required": ["role", "content"],
                        "additionalProperties": false
                    },
                    "description": "Optional initial turns, created atomically with the conversation"
                }
            },
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let create: CreateConversation = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let svc = daemon.conversations.clone();
        let result = blocking(move || svc.create(create)).await??;

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

// ── Get ─────────────────────────────────────────────────────────────

pub struct Get;
#[async_trait]
impl RpcHandler for Get {
    fn method(&self) -> &'static str {
        "conversation.get"
    }
    fn description(&self) -> &'static str {
        "Fetch a single conversation by ULID, including all its turns in ordinal order. Returns error 1001 if not found."
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

        let svc = daemon.conversations.clone();
        let result = blocking(move || svc.get(p.id)).await??;

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

// ── Append Turns ────────────────────────────────────────────────────

pub struct Append;
#[async_trait]
impl RpcHandler for Append {
    fn method(&self) -> &'static str {
        "conversation.append"
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Append one or more turns to an existing conversation. The turns array specifies role and content for each turn. Returns the created turns with their generated ULIDs."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "conversation_id": crate::handlers::params::ulid_schema(),
                "turns": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "role": {"type": "string", "description": "\"user\", \"assistant\", \"system\", or \"tool\""},
                            "content": {"type": "string", "description": "Turn content in markdown"},
                            "attrs": {"type": "object"}
                        },
                        "required": ["role", "content"],
                        "additionalProperties": false
                    },
                    "minItems": 1
                }
            },
            "required": ["conversation_id", "turns"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let append: AppendTurns = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let svc = daemon.conversations.clone();
        let result = blocking(move || svc.append_turns(append)).await??;

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

// ── List ────────────────────────────────────────────────────────────

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "conversation.list"
    }
    fn description(&self) -> &'static str {
        "List conversations with optional tag filter, pagination, and ordering. Default limit 50, offset 0, order created_desc. Returns {items, total, has_more}."
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
                "transient": {
                    "type": "boolean",
                    "description": "Filter by short-term marker: true → only transient conversations, false → only long-term, omit → all."
                }
            },
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: ConversationListQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let svc = daemon.conversations.clone();
        let result = blocking(move || svc.list(query)).await??;

        Ok(json!({
            "items": result.items,
            "total": result.total,
            "has_more": result.has_more,
        }))
    }
}

// ── Update ──────────────────────────────────────────────────────────

pub struct Update;
#[async_trait]
impl RpcHandler for Update {
    fn method(&self) -> &'static str {
        "conversation.update"
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Update a conversation's metadata (title, tags, attrs) by ULID. Cannot change turns; use conversation.append for that. Returns the updated conversation."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "id": crate::handlers::params::ulid_schema(),
                "title": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "attrs": {"type": "object"}
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
            fields: UpdateConversation,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let svc = daemon.conversations.clone();
        let result = blocking(move || svc.update(p.id, p.fields)).await??;

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

// ── Delete ──────────────────────────────────────────────────────────

pub struct Delete;
#[async_trait]
impl RpcHandler for Delete {
    fn method(&self) -> &'static str {
        "conversation.delete"
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Delete a conversation by ULID. CASCADE removes its turns and FTS entries. Returns {deleted: true, id}."
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

        let svc = daemon.conversations.clone();
        blocking(move || svc.delete(p.id)).await??;

        Ok(json!({ "deleted": true, "id": id_for_ack.to_string() }))
    }
}

// ── Search ──────────────────────────────────────────────────────────

/// Default limit for conversation.search (10).
fn default_search_limit() -> u32 {
    10
}

pub struct Search;
#[async_trait]
impl RpcHandler for Search {
    fn method(&self) -> &'static str {
        "conversation.search"
    }
    fn description(&self) -> &'static str {
        "Full-text search through conversation turn content. Returns matching turns with their parent conversation, a highlighted snippet, and relevance score. Short queries (< 3 characters) fall back to LIKE-based matching. Default limit 10."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query — passed directly to FTS5 MATCH for queries >= 3 characters, otherwise LIKE-based substring match"},
                "limit": {"type": "integer", "minimum": 0, "default": 10}
            },
            "required": ["query"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct SearchParams {
            query: String,
            #[serde(default = "default_search_limit")]
            limit: u32,
        }
        let p: SearchParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let svc = daemon.conversations.clone();
        let results = blocking(move || svc.search(&p.query, p.limit)).await??;

        let items: Vec<Value> = results
            .into_iter()
            .map(|r| {
                json!({
                    "conversation": r.conversation,
                    "turn": r.turn,
                    "snippet": r.snippet,
                    "score": r.score,
                })
            })
            .collect();

        Ok(json!({ "items": items }))
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
    fn create_schema_accepts_empty_object() {
        let schema = Create.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_ok());
    }

    #[test]
    fn create_schema_accepts_title_and_turns() {
        let schema = Create.input_schema().unwrap();
        let valid = json!({
            "title": "Chat",
            "turns": [{"role": "user", "content": "hello"}]
        });
        assert!(validate(&schema, &valid).is_ok());
    }

    #[test]
    fn create_schema_rejects_turn_without_content() {
        let schema = Create.input_schema().unwrap();
        let invalid = json!({"turns": [{"role": "user"}]});
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
    fn append_schema_requires_conversation_id_and_turns() {
        let schema = Append.input_schema().unwrap();
        assert!(
            validate(
                &schema,
                &json!({
                    "conversation_id": ULID,
                    "turns": [{"role": "user", "content": "hi"}]
                })
            )
            .is_ok()
        );
        assert!(validate(&schema, &json!({})).is_err());
        assert!(validate(&schema, &json!({"conversation_id": ULID})).is_err());
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
    fn search_schema_requires_query() {
        let schema = Search.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "rust"})).is_ok());
        assert!(validate(&schema, &json!({"query": "rust", "limit": 5})).is_ok());
        assert!(validate(&schema, &json!({})).is_err());
    }
}
