//! link.* handlers. Pure pass-through to LinkService (no embedding
//! orchestration needed; links are not embedded).

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, CreateLink, LinkService};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;

pub struct Create;
#[async_trait]
impl RpcHandler for Create {
    fn method(&self) -> &'static str {
        "link.create"
    }
    fn description(&self) -> &'static str {
        "Create a directed, typed link between two entries (e.g. references, supports, contradicts). Returns the created link."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "source_id": crate::handlers::params::ulid_schema(),
                "target_id": crate::handlers::params::ulid_schema(),
                "relation": {"type": "string"},
                "attrs": {"type": "object"}
            },
            "required": ["source_id", "target_id", "relation"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let input: CreateLink = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let links: std::sync::Arc<LinkService> = daemon.links.clone();
        let link = blocking(move || links.create(input)).await??;

        serde_json::to_value(&link).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Get;
#[async_trait]
impl RpcHandler for Get {
    fn method(&self) -> &'static str {
        "link.get"
    }
    fn description(&self) -> &'static str {
        "Fetch a single link by ULID. Returns error 1001 if not found."
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

        let links = daemon.links.clone();
        let link = blocking(move || links.get(p.id)).await??;

        serde_json::to_value(&link).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Delete;
#[async_trait]
impl RpcHandler for Delete {
    fn method(&self) -> &'static str {
        "link.delete"
    }
    fn description(&self) -> &'static str {
        "Delete a link by ULID. Returns {deleted: true}."
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

        let links = daemon.links.clone();
        blocking(move || links.delete(p.id)).await??;
        Ok(json!({ "deleted": true }))
    }
}

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "link.list"
    }
    fn description(&self) -> &'static str {
        "List links matching filters. At least one of from/to/relation must be supplied (listing all links is rejected). Default limit 50, offset 0. Returns {items, total}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "from": crate::handlers::params::ulid_schema(),
                "to": crate::handlers::params::ulid_schema(),
                "relation": {"type": "string"},
                "limit": {"type": "integer", "minimum": 0, "default": 50},
                "offset": {"type": "integer", "minimum": 0, "default": 0}
            },
            "anyOf": [
                {"required": ["from"]},
                {"required": ["to"]},
                {"required": ["relation"]}
            ],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: nomai_core::ListLinkQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let links = daemon.links.clone();
        let result = blocking(move || links.list(query)).await??;

        Ok(json!({
            "items": result.items,
            "total": result.total,
        }))
    }
}

pub struct Neighbors;
#[async_trait]
impl RpcHandler for Neighbors {
    fn method(&self) -> &'static str {
        "link.neighbors"
    }
    fn description(&self) -> &'static str {
        "List entries neighboring the given id via links. direction: out (id is source), in (id is target), or both (default). Returns {entries, links}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "id": crate::handlers::params::ulid_schema(),
                "relation": {"type": "string"},
                "direction": {
                    "type": "string",
                    "enum": ["out", "in", "both"],
                    "default": "both"
                },
                "limit": {"type": "integer", "minimum": 0, "default": 50}
            },
            "required": ["id"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: nomai_core::NeighborsQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let links = daemon.links.clone();
        let result = blocking(move || links.neighbors(query)).await??;

        Ok(json!({
            "entries": result.entries,
            "links": result.links,
        }))
    }
}
