//! Conversions between parser-layer `Block` (`nomai_format::Block`, with the
//! `BlockType` enum) and storage-layer `Block` (`block_model::Block`, with
//! `String` type + id/timestamps). Centralizes the layer-flip logic so other
//! modules don't reimplement it.

use crate::block_model::Block;
use crate::nomai_format::{Block as ParserBlock, BlockType};

/// Convert a storage `Block` into a parser-layer `Block` for rendering.
/// Storage-only fields (id, entry_id, ordinal, timestamps) are dropped — the
/// parser format doesn't carry them. `attrs` must be a JSON object; if it
/// isn't, an empty `Map` is substituted.
pub fn storage_block_to_parser_block(b: &Block) -> ParserBlock {
    ParserBlock {
        r#type: BlockType::from_str(&b.r#type).unwrap_or(BlockType::Note),
        text: b.text.clone(),
        attrs: b.attrs.as_object().cloned().unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn storage_to_parser_round_trip_type() {
        let storage = Block {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            entry_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 0,
            r#type: "claim".into(),
            text: "x".into(),
            attrs: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let p = storage_block_to_parser_block(&storage);
        assert_eq!(p.r#type, BlockType::Claim);
        assert_eq!(p.text, "x");
    }

    #[test]
    fn unknown_storage_type_falls_back_to_note() {
        let storage = Block {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            entry_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 0,
            r#type: "definition".into(), // not in v1 enum
            text: "x".into(),
            attrs: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let p = storage_block_to_parser_block(&storage);
        assert_eq!(p.r#type, BlockType::Note); // fallback
    }

    #[test]
    fn storage_attrs_non_object_falls_back_to_empty_map() {
        let storage = Block {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            entry_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 0,
            r#type: "note".into(),
            text: "x".into(),
            attrs: json!([1, 2, 3]), // array, not object
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let p = storage_block_to_parser_block(&storage);
        assert!(p.attrs.is_empty());
    }
}
