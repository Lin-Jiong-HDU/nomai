//! events.* handlers. Pure pass-through to EventService.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, ListEventsQuery, PurgeQuery};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "events.list"
    }
    fn description(&self) -> &'static str {
        "List events matching filters, ordered by ULID (time-ordered). since is exclusive (returns id > since). Use for incremental sync: client tracks last_seen_id and pulls via since=last_seen_id. Default limit 100, order asc. Returns {items, has_more, total}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "since": crate::handlers::params::ulid_schema(),
                "type": {"type": "string", "description": "event type filter, e.g. \"entry.created\""},
                "target_type": {"type": "string", "description": "\"entry\" or \"link\""},
                "target_id": crate::handlers::params::ulid_schema(),
                "limit": {"type": "integer", "minimum": 0, "default": 100},
                "order": {"type": "string", "enum": ["asc", "desc"], "default": "asc"}
            },
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: ListEventsQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let events = daemon.events.clone();
        let result = blocking(move || events.list(query)).await??;

        Ok(json!({
            "items": result.items,
            "has_more": result.has_more,
            "total": result.total,
        }))
    }
}

pub struct Get;
#[async_trait]
impl RpcHandler for Get {
    fn method(&self) -> &'static str {
        "events.get"
    }
    fn description(&self) -> &'static str {
        "Fetch a single event by ULID. Returns error 1001 if not found."
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

        let events = daemon.events.clone();
        let event = blocking(move || events.get(p.id)).await??;

        serde_json::to_value(&event).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Purge;
#[async_trait]
impl RpcHandler for Purge {
    fn method(&self) -> &'static str {
        "events.purge"
    }
    fn description(&self) -> &'static str {
        "Delete events with id < before (exclusive). Optional type filter (e.g. \"entry.created\"). For retention/cleanup. Returns {deleted: N}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "before": crate::handlers::params::ulid_schema(),
                "type": {"type": "string"}
            },
            "required": ["before"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: PurgeQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let events = daemon.events.clone();
        let deleted = blocking(move || events.purge(query)).await??;

        Ok(json!({ "deleted": deleted }))
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
    fn list_schema_accepts_empty_object() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_ok());
    }

    #[test]
    fn list_schema_accepts_since_filter() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({"since": ULID})).is_ok());
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
    fn purge_schema_accepts_before() {
        let schema = Purge.input_schema().unwrap();
        assert!(validate(&schema, &json!({"before": ULID})).is_ok());
    }

    #[test]
    fn purge_schema_rejects_missing_before() {
        let schema = Purge.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }
}
