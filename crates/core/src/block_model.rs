//! Block data model: typed-block primitive for the new content storage model
//! (Spec 6 §5.1). One entry → many blocks. Block types are a closed set:
//! claim / evidence / question / source / note / connection.
//!
//! `r#type` is a String (not the `BlockType` enum from `nomai_format`) because
//! storage roundtrips through SQLite TEXT; the parser layer validates against
//! the enum. This mirrors how `Chunk.ordinal` is `u32` regardless of upstream
//! producer.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

/// A typed block belonging to an entry. Blocks are immutable per spec §6.1
/// (`block.update` is a delete + create at the RPC layer in Plan 3+).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub id: Ulid,
    pub entry_id: Ulid,
    pub ordinal: u32,
    pub r#type: String,
    pub text: String,
    pub attrs: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Input for `BlockService::create`. `attrs` defaults to `{}` if omitted.
#[derive(Debug, Deserialize)]
pub struct CreateBlock {
    pub entry_id: Ulid,
    pub ordinal: u32,
    pub r#type: String,
    pub text: String,
    #[serde(default)]
    pub attrs: Option<Value>,
}

/// Result of `BlockService::list`.
#[derive(Debug)]
pub struct BlockListResult {
    pub items: Vec<Block>,
    pub total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn block_roundtrips_through_json() {
        let block = Block {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            entry_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 0,
            r#type: "claim".into(),
            text: "Earth orbits the sun.".into(),
            attrs: json!({"confidence": "high"}),
            created_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            updated_at: "2026-06-23T10:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&block).unwrap();
        let back: Block = serde_json::from_str(&s).unwrap();
        assert_eq!(block, back);
    }

    #[test]
    fn create_block_allows_missing_attrs() {
        let json =
            r#"{"entry_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","ordinal":0,"type":"note","text":"x"}"#;
        let c: CreateBlock = serde_json::from_str(json).unwrap();
        assert!(c.attrs.is_none());
    }

    #[test]
    fn block_serializes_type_as_type_field() {
        let block = Block {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            entry_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 0,
            r#type: "claim".into(),
            text: "x".into(),
            attrs: json!({}),
            created_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            updated_at: "2026-06-23T10:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&block).unwrap();
        // serde translates r#type → "type" in JSON
        assert!(s.contains(r#""type":"claim""#));
    }
}
