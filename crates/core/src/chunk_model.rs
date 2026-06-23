//! Chunk data model: a block split into N embeddable pieces.
//!
//! Plan 4: chunks are auto-derived from blocks (Spec §10 chunking algorithm).
//! `CreateChunk` is gone — chunks are not user-created. `entry_id` is gone —
//! chunks are block-addressed; reach the entry via JOIN chunks→blocks→entries.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

/// A chunk of a block. Chunks are immutable (auto-derived from block text).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    pub id: Ulid,
    pub block_id: Ulid,
    pub ordinal: u32,
    pub text: String,
    pub attrs: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Result of `ChunkService::list`.
#[derive(Debug)]
pub struct ChunkListResult {
    pub items: Vec<Chunk>,
    pub total: u64,
}

/// Result of `ChunkService::semantic_search`.
#[derive(Debug)]
pub struct ChunkSearchResult {
    pub chunk: Chunk,
    pub score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn chunk_roundtrips_through_json() {
        let chunk = Chunk {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            block_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 0,
            text: "chunk content".into(),
            attrs: json!({"parent_block_type": "note"}),
            created_at: "2026-06-23T12:00:00Z".parse().unwrap(),
            updated_at: "2026-06-23T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&chunk).unwrap();
        let back: Chunk = serde_json::from_str(&s).unwrap();
        assert_eq!(chunk, back);
    }

    #[test]
    fn chunk_serializes_block_id_field() {
        let chunk = Chunk {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            block_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 0,
            text: "x".into(),
            attrs: json!({}),
            created_at: "2026-06-23T12:00:00Z".parse().unwrap(),
            updated_at: "2026-06-23T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&chunk).unwrap();
        assert!(s.contains(r#""block_id":"01ARZ3NDEKTSV4RRFFQ69G5FAX""#));
        assert!(!s.contains(r#""entry_id""#));
    }
}
