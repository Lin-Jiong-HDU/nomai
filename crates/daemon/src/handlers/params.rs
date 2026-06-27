//! Shared JSON Schema fragments for handler params.
//!
//! Used by `RpcHandler::input_schema()` implementations across handlers.

use serde_json::{Value, json};

/// Canonical ULID schema: Crockford Base32, 26 chars.
/// See <https://github.com/ulid/spec>.
pub fn ulid_schema() -> Value {
    json!({"type": "string", "pattern": "^[0-9A-HJKMNP-TV-Z]{26}$"})
}

/// `{id: Ulid}` object schema — used by single-ULID-arg handlers
/// (entry.get, link.delete, chunk.get, events.get, ...).
pub fn ulid_param_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "id": ulid_schema() },
        "required": ["id"],
        "additionalProperties": false
    })
}

/// Empty object schema — used by no-arg handlers
/// (provider.list, cache.stats, index.*, system.export_to_fs).
pub fn empty_param_schema() -> Value {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

/// `schemars::Schema` for ULID-typed fields. Use via
/// `#[schemars(with = "crate::handlers::params::ulid_field_schema")]`
/// on any `ulid::Ulid` field inside a `#[derive(JsonSchema)]` struct,
/// since the `ulid` crate does not implement `JsonSchema` itself.
pub fn ulid_field_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    use schemars::Schema;
    let _ = _gen; // suppress unused-var warning if generator not needed
    Schema::try_from(ulid_schema()).expect("ulid_schema is a valid JSON Schema object")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_schema_has_crockford_pattern() {
        let s = ulid_schema();
        assert_eq!(s["type"], "string");
        assert_eq!(s["pattern"], "^[0-9A-HJKMNP-TV-Z]{26}$");
    }

    #[test]
    fn ulid_param_schema_requires_id() {
        let s = ulid_param_schema();
        assert_eq!(s["type"], "object");
        assert_eq!(s["required"][0], "id");
        assert_eq!(s["additionalProperties"], false);
    }

    #[test]
    fn empty_param_schema_forbids_props() {
        let s = empty_param_schema();
        assert_eq!(s["additionalProperties"], false);
        assert!(s["properties"].as_object().unwrap().is_empty());
    }
}
