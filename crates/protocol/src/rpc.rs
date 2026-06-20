//! JSON-RPC 2.0 envelope types.
//!
//! See <https://www.jsonrpc.org/specification>.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Always `"2.0"` per the JSON-RPC 2.0 spec.
pub const JSONRPC_VERSION: &str = "2.0";

/// JSON-RPC request id. The spec allows Numbers or Strings; booleans,
/// arrays, and objects are invalid. `Option<Id>` is used at the `Request`
/// level so `None` can represent a Notification (no response expected).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Id {
    Number(i64),
    String(String),
}

/// A JSON-RPC 2.0 request. `id == None` indicates a Notification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Id>,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// A JSON-RPC 2.0 response. Exactly one of `result` / `error` is `Some`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

/// JSON-RPC error object. `data` is optional and application-defined.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Request {
    /// Build a Notification (no id, no response expected).
    pub fn notification(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id: None,
            method: method.into(),
            params,
        }
    }
}

impl Response {
    /// Successful response.
    pub fn ok(id: Option<Id>, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Error response.
    pub fn err(id: Option<Id>, error: RpcError) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_with_number_id_roundtrips() {
        let raw = r#"{"jsonrpc":"2.0","id":1,"method":"entry.create","params":{"title":"x"}}"#;
        let req: Request = serde_json::from_str(raw).unwrap();
        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.id, Some(Id::Number(1)));
        assert_eq!(req.method, "entry.create");
        assert_eq!(req.params, Some(json!({"title": "x"})));

        let reserialized = serde_json::to_string(&req).unwrap();
        let again: Request = serde_json::from_str(&reserialized).unwrap();
        assert_eq!(req, again);
    }

    #[test]
    fn request_with_string_id_roundtrips() {
        let raw = r#"{"jsonrpc":"2.0","id":"abc","method":"entry.get","params":{"id":"x"}}"#;
        let req: Request = serde_json::from_str(raw).unwrap();
        assert_eq!(req.id, Some(Id::String("abc".to_string())));
    }

    #[test]
    fn request_notification_has_no_id_field() {
        let raw = r#"{"jsonrpc":"2.0","method":"entry.created","params":{"id":"x"}}"#;
        let req: Request = serde_json::from_str(raw).unwrap();
        assert!(req.id.is_none());

        // The top-level `id` field must be absent; params may legitimately
        // contain its own `"id"` key, so parse the reserialized object and
        // assert the key is missing rather than grepping the raw string.
        let reserialized = serde_json::to_string(&req).unwrap();
        let as_obj: serde_json::Map<String, Value> = serde_json::from_str(&reserialized).unwrap();
        assert!(!as_obj.contains_key("id"));
    }

    #[test]
    fn request_without_params_serializes_no_params_field() {
        let req = Request::notification("noop", None);
        let s = serde_json::to_string(&req).unwrap();
        assert!(!s.contains(r#""params":"#));
    }

    #[test]
    fn response_ok_skips_error_field() {
        let resp = Response::ok(Some(Id::Number(1)), json!({"deleted": true}));
        let s = serde_json::to_string(&resp).unwrap();
        assert!(s.contains(r#""result":{"deleted":true}"#));
        assert!(!s.contains(r#""error":"#));
    }

    #[test]
    fn response_err_skips_result_field() {
        let resp = Response::err(
            Some(Id::Number(1)),
            RpcError {
                code: -32601,
                message: "Method not found".into(),
                data: None,
            },
        );
        let s = serde_json::to_string(&resp).unwrap();
        assert!(s.contains(r#""error":{"code":-32601,"message":"Method not found"}"#));
        assert!(!s.contains(r#""result":"#));
    }

    #[test]
    fn rpc_error_with_data_serializes_data_field() {
        let err = RpcError {
            code: 1002,
            message: "provider error".into(),
            data: Some(json!({"kind": "Auth"})),
        };
        let s = serde_json::to_string(&err).unwrap();
        assert!(s.contains(r#""data":{"kind":"Auth"}"#));
    }

    #[test]
    fn id_untagged_number_or_string() {
        let n: Id = serde_json::from_str("42").unwrap();
        assert_eq!(n, Id::Number(42));
        let s: Id = serde_json::from_str(r#""req-1""#).unwrap();
        assert_eq!(s, Id::String("req-1".into()));
    }
}
