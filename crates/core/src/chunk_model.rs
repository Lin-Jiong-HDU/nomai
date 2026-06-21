//! Chunk data model: entry split into N independently embeddable pieces.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

/// A chunk of an entry. Chunks are immutable (no update); re-chunking is
/// delete + create. See spec §4-§5.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    pub id: Ulid,
    pub entry_id: Ulid,
    pub ordinal: u32,
    pub text: String,
    pub attrs: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Input for `ChunkService::create`. `attrs` defaults to `{}` if omitted.
#[derive(Debug, Deserialize)]
pub struct CreateChunk {
    pub entry_id: Ulid,
    pub ordinal: u32,
    pub text: String,
    #[serde(default)]
    pub attrs: Option<Value>,
}

/// Result of `ChunkService::list`.
#[derive(Debug)]
pub struct ChunkListResult {
    pub items: Vec<Chunk>,
    pub total: u64,
}

/// Result of `ChunkService::semantic_search` (chunk-level KNN).
#[derive(Debug)]
pub struct ChunkSearchResult {
    pub chunk: Chunk,
    pub score: f32,
}

/// Granularity selector for `search.semantic`. Defaults to `Entry` for
/// backward compatibility; `Chunk` routes to chunk-level KNN.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    #[default]
    Entry,
    Chunk,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn chunk_roundtrips_through_json() {
        let chunk = Chunk {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            entry_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 0,
            text: "chunk content".into(),
            attrs: json!({"section": "intro"}),
            created_at: "2026-06-21T12:00:00Z".parse().unwrap(),
            updated_at: "2026-06-21T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&chunk).unwrap();
        let back: Chunk = serde_json::from_str(&s).unwrap();
        assert_eq!(chunk, back);
    }

    #[test]
    fn create_chunk_allows_missing_attrs() {
        let json = r#"{"entry_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","ordinal":0,"text":"x"}"#;
        let c: CreateChunk = serde_json::from_str(json).unwrap();
        assert!(c.attrs.is_none());
    }

    #[test]
    fn create_chunk_serializes_ordinal_as_number() {
        let chunk = Chunk {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            entry_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            ordinal: 42,
            text: "x".into(),
            attrs: json!({}),
            created_at: "2026-06-21T12:00:00Z".parse().unwrap(),
            updated_at: "2026-06-21T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&chunk).unwrap();
        assert!(s.contains(r#""ordinal":42"#));
    }
}
