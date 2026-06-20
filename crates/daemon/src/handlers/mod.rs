//! JSON-RPC method dispatch table.

use serde_json::Value;

use nomai_protocol::Request;

use crate::daemon::Daemon;
use crate::rpc::DispatchError;

pub mod entry;
pub mod provider;
pub mod qa;
pub mod search;

pub async fn route(daemon: &Daemon, req: Request) -> Result<Value, DispatchError> {
    let params = req.params.unwrap_or(Value::Null);
    let result: Result<Value, nomai_core::CoreError> = match req.method.as_str() {
        "entry.create" => entry::create(daemon, params).await,
        "entry.get" => entry::get(daemon, params).await,
        "entry.update" => entry::update(daemon, params).await,
        "entry.delete" => entry::delete(daemon, params).await,
        "entry.list" => entry::list(daemon, params).await,
        "search.fulltext" => search::fulltext(daemon, params).await,
        "search.semantic" => search::semantic(daemon, params).await,
        "qa.ask" => qa::ask(daemon, params).await,
        "provider.list" => provider::list(daemon, params).await,
        // Reserved method names per spec §6: return -32601.
        "search.hybrid" | "provider.set" => {
            return Err(DispatchError::MethodNotFound(req.method.clone()));
        }
        _ => return Err(DispatchError::MethodNotFound(req.method.clone())),
    };
    result.map_err(DispatchError::Core)
}
