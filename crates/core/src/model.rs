//! Core data model. Entry is the only first-class entity.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub id: Ulid,
    pub title: String,
    pub blocks: Vec<crate::block_model::Block>,
    pub tags: Vec<String>,
    pub attrs: Value,
    pub source: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn entry_roundtrips_through_json() {
        let entry = Entry {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            title: "Hello".into(),
            blocks: vec![],
            tags: vec!["a".into(), "b".into()],
            attrs: json!({"k": "v"}),
            source: Some("test".into()),
            created_at: "2026-06-20T12:00:00Z".parse().unwrap(),
            updated_at: "2026-06-20T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&entry).unwrap();
        let back: Entry = serde_json::from_str(&s).unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn entry_serializes_id_as_ulid_string() {
        let entry = Entry {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            title: "t".into(),
            blocks: vec![],
            tags: vec![],
            attrs: json!({}),
            source: None,
            created_at: "2026-06-20T12:00:00Z".parse().unwrap(),
            updated_at: "2026-06-20T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&entry).unwrap();
        assert!(s.contains(r#""id":"01ARZ3NDEKTSV4RRFFQ69G5FAV""#));
    }
}
