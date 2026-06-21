//! link.* handlers. Pure pass-through to LinkService (no embedding
//! orchestration needed; links are not embedded).

use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, CreateLink, LinkService};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;

pub async fn create(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    let input: CreateLink = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    let links: std::sync::Arc<LinkService> = daemon.links.clone();
    let link = blocking(move || links.create(input)).await??;

    serde_json::to_value(&link).map_err(|e| CoreError::Config(format!("serialize: {e}")))
}

pub async fn get(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
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

pub async fn delete(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
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

pub async fn list(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    let query: nomai_core::ListLinkQuery = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    let links = daemon.links.clone();
    let result = blocking(move || links.list(query)).await??;

    Ok(json!({
        "items": result.items,
        "total": result.total,
    }))
}

pub async fn neighbors(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    let query: nomai_core::NeighborsQuery = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    let links = daemon.links.clone();
    let result = blocking(move || links.neighbors(query)).await??;

    Ok(json!({
        "entries": result.entries,
        "links": result.links,
    }))
}
