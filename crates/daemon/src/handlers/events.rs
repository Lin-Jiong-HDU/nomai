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
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let query: PurgeQuery = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let events = daemon.events.clone();
        let deleted = blocking(move || events.purge(query)).await??;

        Ok(json!({ "deleted": deleted }))
    }
}
