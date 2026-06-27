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
    fn create_schema_accepts_valid() {
        let schema = Create.input_schema().unwrap();
        let valid = json!({
            "source_id": ULID,
            "target_id": ULID,
            "relation": "references"
        });
        assert!(validate(&schema, &valid).is_ok());
    }

    #[test]
    fn create_schema_rejects_missing_relation() {
        let schema = Create.input_schema().unwrap();
        let invalid = json!({"source_id": ULID, "target_id": ULID});
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
    fn list_schema_accepts_from_filter() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({"from": ULID})).is_ok());
    }

    #[test]
    fn list_schema_rejects_empty_object_no_anyof_match() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn neighbors_schema_accepts_id() {
        let schema = Neighbors.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn neighbors_schema_rejects_missing_id() {
        let schema = Neighbors.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }
}
