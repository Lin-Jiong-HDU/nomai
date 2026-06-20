//! CoreError → RpcError mapping and the DispatchError enum.

use nomai_core::CoreError;
use nomai_protocol::RpcError;
use nomai_protocol::error::{
    CONFIG_ERROR, ENTRY_NOT_FOUND, INTERNAL_ERROR, PROVIDER_ERROR, VALIDATION_ERROR,
};
use serde_json::json;

#[derive(Debug)]
pub enum DispatchError {
    Core(CoreError),
    MethodNotFound(String),
}

pub fn core_error_to_rpc(err: CoreError) -> RpcError {
    match err {
        CoreError::NotFound(id) => RpcError {
            code: ENTRY_NOT_FOUND,
            message: "entry not found".into(),
            data: Some(json!({ "id": id.to_string() })),
        },
        CoreError::Validation(msg) => RpcError {
            code: VALIDATION_ERROR,
            message: msg,
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
            message: msg,
            data: None,
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
    }
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
        assert!(rpc.data.unwrap().get("id").is_some());
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
}
