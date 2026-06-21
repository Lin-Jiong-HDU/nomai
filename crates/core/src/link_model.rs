//! Link data model. Links are directed edges between entries.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

/// A directed link between two entries.
///
/// See spec §4 for schema rationale (directed edges, free-form relation,
/// UNIQUE constraint, FK CASCADE).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Link {
    pub id: Ulid,
    pub source_id: Ulid,
    pub target_id: Ulid,
    pub relation: String,
    pub attrs: Value,
    pub created_at: DateTime<Utc>,
}

/// Input for `LinkService::create`. `attrs` defaults to `{}` if omitted.
#[derive(Debug, Deserialize)]
pub struct CreateLink {
    pub source_id: Ulid,
    pub target_id: Ulid,
    pub relation: String,
    #[serde(default)]
    pub attrs: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn link_roundtrips_through_json() {
        let link = Link {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            source_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            target_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            relation: "references".into(),
            attrs: json!({"weight": 0.8}),
            created_at: "2026-06-21T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&link).unwrap();
        let back: Link = serde_json::from_str(&s).unwrap();
        assert_eq!(link, back);
    }

    #[test]
    fn link_serializes_id_as_ulid_string() {
        let link = Link {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            source_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            target_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            relation: "x".into(),
            attrs: json!({}),
            created_at: "2026-06-21T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&link).unwrap();
        assert!(s.contains(r#""id":"01ARZ3NDEKTSV4RRFFQ69G5FAV""#));
        assert!(s.contains(r#""source_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV""#));
        assert!(s.contains(r#""target_id":"01ARZ3NDEKTSV4RRFFQ69G5FAX""#));
    }

    #[test]
    fn create_link_allows_missing_attrs() {
        let json = r#"{"source_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","target_id":"01ARZ3NDEKTSV4RRFFQ69G5FAX","relation":"references"}"#;
        let cl: CreateLink = serde_json::from_str(json).unwrap();
        assert!(cl.attrs.is_none());
    }
}
