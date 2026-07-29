//! JSON-RPC error-code constants and canonical messages.
//!
//! Code ranges (per JSON-RPC 2.0):
//!   -32700..=-32603  protocol errors (reserved by JSON-RPC)
//!   1001             entry not found
//!   1002             provider error (error.data contains `kind`)
//!   1003             validation error
//!   1004             config error
//!   1005             filesystem error
//!   1006             .nomai parse error
//!   1007             sync error (conflict)

// JSON-RPC 2.0 reserved range.
pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;

// nomai business errors.
pub const ENTRY_NOT_FOUND: i32 = 1001;
pub const PROVIDER_ERROR: i32 = 1002;
pub const VALIDATION_ERROR: i32 = 1003;
pub const CONFIG_ERROR: i32 = 1004;
pub const FS_ERROR: i32 = 1005;
pub const NOMAI_FORMAT_ERROR: i32 = 1006;
pub const SYNC_ERROR: i32 = 1007;

/// Canonical messages for the JSON-RPC reserved range. Business errors
/// carry their own messages and do not have constants here.
pub const MESSAGE_PARSE_ERROR: &str = "Parse error";
pub const MESSAGE_INVALID_REQUEST: &str = "Invalid Request";
pub const MESSAGE_METHOD_NOT_FOUND: &str = "Method not found";
pub const MESSAGE_INVALID_PARAMS: &str = "Invalid params";
pub const MESSAGE_INTERNAL_ERROR: &str = "Internal error";
pub const MESSAGE_SYNC_ERROR: &str = "Sync error";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonrpc_reserved_codes_match_spec() {
        assert_eq!(PARSE_ERROR, -32700);
        assert_eq!(INVALID_REQUEST, -32600);
        assert_eq!(METHOD_NOT_FOUND, -32601);
        assert_eq!(INVALID_PARAMS, -32602);
        assert_eq!(INTERNAL_ERROR, -32603);
    }

    #[test]
    fn business_codes_match_design_doc() {
        assert_eq!(ENTRY_NOT_FOUND, 1001);
        assert_eq!(PROVIDER_ERROR, 1002);
        assert_eq!(VALIDATION_ERROR, 1003);
        assert_eq!(CONFIG_ERROR, 1004);
        assert_eq!(FS_ERROR, 1005);
        assert_eq!(NOMAI_FORMAT_ERROR, 1006);
    }

    #[test]
    fn canonical_messages_match_jsonrpc_spec() {
        assert_eq!(MESSAGE_PARSE_ERROR, "Parse error");
        assert_eq!(MESSAGE_INVALID_REQUEST, "Invalid Request");
        assert_eq!(MESSAGE_METHOD_NOT_FOUND, "Method not found");
        assert_eq!(MESSAGE_INVALID_PARAMS, "Invalid params");
        assert_eq!(MESSAGE_INTERNAL_ERROR, "Internal error");
    }
}
