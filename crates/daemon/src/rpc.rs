//! CoreError → RpcError mapping, the DispatchError enum, and the
//! RpcHandler trait that every JSON-RPC method handler implements.

use async_trait::async_trait;
use serde_json::Value;

use nomai_core::CoreError;
use nomai_protocol::RpcError;
use nomai_protocol::error::{
    CONFIG_ERROR, CONFLICT_ERROR, ENTRY_NOT_FOUND, FS_ERROR, INTERNAL_ERROR, NOMAI_FORMAT_ERROR,
    PROVIDER_ERROR, SYNC_ERROR, VALIDATION_ERROR,
};
use serde_json::json;

#[derive(Debug)]
pub enum DispatchError {
    Core(CoreError),
    MethodNotFound(String),
}

/// A JSON-RPC method handler.
///
/// Each RPC method is implemented as a zero-sized struct that implements
/// this trait. Handlers are registered in `handlers::registry()` and
/// dispatched via the Daemon's `HashMap<&'static str, Arc<dyn RpcHandler>>`.
#[async_trait]
pub trait RpcHandler: Send + Sync {
    /// The JSON-RPC method name (e.g. `"entry.create"`).
    fn method(&self) -> &'static str;

    /// 1-3 sentence English description surfaced to MCP clients via
    /// `tools/list`. Empty string (default) → `tools/list` omits the field
    /// and clients fall back to the method name.
    fn description(&self) -> &'static str {
        ""
    }

    /// JSON Schema for the `params` object.
    /// `None` (default) → `tools/list` emits `{"type": "object"}` (current
    /// behavior, preserves backward compat for plugins).
    /// `Some(v)` → `tools/list` emits `v` as `inputSchema`.
    fn input_schema(&self) -> Option<serde_json::Value> {
        None
    }

    /// Whether this handler mutates the `knowledge_root` file tree
    /// (entry/block writes, batch). The dispatcher holds `sync_lock` for
    /// mutating calls so they cannot run concurrently with `sync.run`'s
    /// rebase — git's checkout would otherwise overwrite in-flight writes.
    /// Default `false`.
    ///
    /// NOTE: `sync.run` manages its own lock acquisition internally (see
    /// `Run::call`) and returns `false` here to avoid re-entrant deadlock.
    /// `index.sync`/`index.rebuild` mutate only the derived SQLite index
    /// (not the file tree) and also return `false`. Crucially, `sync.run`'s
    /// nested `index.sync` invocation goes through `handler.call(...)` directly
    /// (NOT `Daemon::dispatch`), so even if these were mis-marked `true` the
    /// dispatcher would never be re-entered — but keeping them `false` makes
    /// the invariant local and self-documenting.
    fn is_mutating(&self) -> bool {
        false
    }

    /// Invoke the handler with the parsed params JSON value.
    async fn call(&self, daemon: &crate::daemon::Daemon, params: Value)
    -> Result<Value, CoreError>;
}

/// Reference version of `core_error_to_rpc`. Used by callers that need
/// to keep the original CoreError (e.g. batch.rs inserts per-op errors
/// into the results array AND returns the last error as the top-level
/// RPC error).
pub fn core_error_to_rpc_ref(err: &CoreError) -> RpcError {
    match err {
        CoreError::NotFound(id) => RpcError {
            code: ENTRY_NOT_FOUND,
            message: "entry not found".into(),
            data: Some(json!({ "id": id.to_string() })),
        },
        CoreError::ResourceNotFound { resource, id } => RpcError {
            code: ENTRY_NOT_FOUND,
            message: format!("{resource} not found"),
            data: Some(json!({
                "resource": resource,
                "id": id.to_string(),
            })),
        },
        CoreError::Validation(msg) => RpcError {
            code: VALIDATION_ERROR,
            message: msg.clone(),
            data: None,
        },
        CoreError::Conflict(msg) => RpcError {
            code: CONFLICT_ERROR,
            message: msg.clone(),
            data: None,
        },
        CoreError::Provider(p) => RpcError {
            code: PROVIDER_ERROR,
            message: p.message.clone(),
            data: Some(json!({
                "kind": p.kind,
                "status": p.status,
            })),
        },
        CoreError::Config(msg) => RpcError {
            code: CONFIG_ERROR,
            message: msg.clone(),
            data: None,
        },
        CoreError::Io(e) => RpcError {
            code: FS_ERROR,
            message: format!("io error: {e}"),
            // P2-7: enrich FS_ERROR data with the io::ErrorKind so callers
            // can distinguish not-found / permission-denied / etc. without
            // scraping the message string.
            data: Some(json!({ "kind": format!("{:?}", e.kind()) })),
        },
        CoreError::NomaiFormat(pe) => RpcError {
            code: NOMAI_FORMAT_ERROR,
            message: format!("nomai format error: {pe}"),
            data: Some(json!({ "parse_error": pe.to_string() })),
        },
        CoreError::Storage(e) => RpcError {
            code: INTERNAL_ERROR,
            message: format!("storage error: {e}"),
            data: None,
        },
        CoreError::Migration(msg) => RpcError {
            code: INTERNAL_ERROR,
            message: format!("migration error: {msg}"),
            data: None,
        },
        CoreError::SyncConflict {
            message,
            conflicted_files,
        } => RpcError {
            code: SYNC_ERROR,
            message: message.clone(),
            data: Some(json!({ "conflicted_files": conflicted_files })),
        },
    }
}

/// Convenience wrapper: convert owned CoreError to RpcError.
pub fn core_error_to_rpc(err: CoreError) -> RpcError {
    core_error_to_rpc_ref(&err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nomai_protocol::ProviderErrorKind;

    #[test]
    fn not_found_maps_to_1001() {
        let id: ulid::Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let rpc = core_error_to_rpc(CoreError::NotFound(id));
        assert_eq!(rpc.code, 1001);
        assert_eq!(rpc.message, "entry not found");
        let data = rpc.data.unwrap();
        assert_eq!(data["id"], "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert!(data.get("resource").is_none());
    }

    #[test]
    fn resource_not_found_maps_to_1001_with_resource_and_id() {
        let id: ulid::Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let rpc = core_error_to_rpc(CoreError::ResourceNotFound {
            resource: "search session",
            id,
        });
        assert_eq!(rpc.code, ENTRY_NOT_FOUND);
        assert_eq!(rpc.message, "search session not found");
        let data = rpc.data.unwrap();
        assert_eq!(data["resource"], "search session");
        assert_eq!(data["id"], "01ARZ3NDEKTSV4RRFFQ69G5FAV");
    }

    #[test]
    fn conflict_maps_to_1008_with_message() {
        let rpc = core_error_to_rpc(CoreError::Conflict("search session expired".into()));
        assert_eq!(rpc.code, nomai_protocol::error::CONFLICT_ERROR);
        assert_eq!(rpc.message, "search session expired");
        assert!(rpc.data.is_none());
    }

    #[test]
    fn provider_maps_to_1002_with_kind() {
        let p = nomai_protocol::ProviderError::new(ProviderErrorKind::Auth, "bad key", Some(401));
        let rpc = core_error_to_rpc(CoreError::Provider(p));
        assert_eq!(rpc.code, 1002);
        assert_eq!(rpc.data.unwrap()["kind"], "auth");
    }

    #[test]
    fn validation_maps_to_1003() {
        let rpc = core_error_to_rpc(CoreError::Validation("attrs must be object".into()));
        assert_eq!(rpc.code, 1003);
    }

    #[test]
    fn storage_maps_to_internal_error() {
        // Synthesize a storage error via From.
        let storage_err = rusqlite::Error::InvalidParameterName("x".into());
        let rpc = core_error_to_rpc(CoreError::Storage(storage_err));
        assert_eq!(rpc.code, -32603);
    }

    #[test]
    fn io_error_maps_to_1005() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let rpc = core_error_to_rpc(CoreError::Io(io_err));
        assert_eq!(rpc.code, FS_ERROR);
        assert!(rpc.message.contains("file missing"));
        // P2-7: data field carries the io::ErrorKind for programmatic use.
        assert_eq!(rpc.data.unwrap()["kind"], "NotFound");
    }

    #[test]
    fn nomai_format_error_maps_to_1006() {
        let parse_err = nomai_core::ParseError::EmptyInput;
        let rpc = core_error_to_rpc(CoreError::NomaiFormat(parse_err));
        assert_eq!(rpc.code, NOMAI_FORMAT_ERROR);
        assert!(rpc.message.contains("empty input"));
    }

    #[test]
    fn core_error_to_rpc_ref_matches_by_value() {
        // Ref version produces same code/data as
        // the by-value version. CoreError isn't Clone, so reconstruct the
        // owned path's expectations by hand.
        let id: ulid::Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = CoreError::NotFound(id);
        let by_ref = core_error_to_rpc_ref(&err);
        assert_eq!(by_ref.code, 1001);
        assert!(by_ref.data.unwrap().get("id").is_some());
    }

    #[test]
    fn sync_conflict_maps_to_1007_with_files() {
        let err = CoreError::SyncConflict {
            message: "rebase conflict".into(),
            conflicted_files: vec!["entries/01K/entry.nomai".into()],
        };
        let rpc = core_error_to_rpc(err);
        assert_eq!(rpc.code, SYNC_ERROR);
        assert_eq!(rpc.message, "rebase conflict");
        assert_eq!(
            rpc.data.unwrap()["conflicted_files"][0],
            "entries/01K/entry.nomai"
        );
    }
}
