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

fn default_limit() -> u32 {
    50
}

/// Input for `LinkService::list`. At least one of `from` / `to` must be
/// `Some` — "list all links" is rejected (spec §5).
#[derive(Debug, Default, Deserialize)]
pub struct ListLinkQuery {
    #[serde(default)]
    pub from: Option<Ulid>,
    #[serde(default)]
    pub to: Option<Ulid>,
    #[serde(default)]
    pub relation: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

#[derive(Debug)]
pub struct ListLinkResult {
    pub items: Vec<Link>,
    pub total: u64,
}

/// Direction filter for `neighbors`. `Out` = links where id is source;
/// `In` = links where id is target; `Both` = either.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Out,
    In,
    #[default]
    Both,
}

#[derive(Debug, Deserialize)]
pub struct NeighborsQuery {
    pub id: Ulid,
    #[serde(default)]
    pub relation: Option<String>,
    #[serde(default)]
    pub direction: Direction,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

#[derive(Debug)]
pub struct NeighborsResult {
    pub entries: Vec<crate::model::Entry>,
    pub links: Vec<Link>,
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

    #[test]
    fn list_link_query_allows_missing_optionals() {
        let json = r#"{}"#;
        let q: ListLinkQuery = serde_json::from_str(json).unwrap();
        assert!(q.from.is_none());
        assert!(q.to.is_none());
        assert!(q.relation.is_none());
        assert_eq!(q.limit, 50);
        assert_eq!(q.offset, 0);
    }

    #[test]
    fn direction_serializes_as_snake_case() {
        let out = serde_json::to_string(&Direction::Out).unwrap();
        assert_eq!(out, r#""out""#);
        let incoming = serde_json::to_string(&Direction::In).unwrap();
        assert_eq!(incoming, r#""in""#);
        let both = serde_json::to_string(&Direction::Both).unwrap();
        assert_eq!(both, r#""both""#);
    }

    #[test]
    fn direction_deserializes_from_snake_case() {
        let d: Direction = serde_json::from_str(r#""in""#).unwrap();
        assert_eq!(d, Direction::In);
    }

    #[test]
    fn neighbors_query_defaults_direction_to_both() {
        let json = r#"{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}"#;
        let q: NeighborsQuery = serde_json::from_str(json).unwrap();
        assert_eq!(q.direction, Direction::Both);
        assert_eq!(q.limit, 50);
    }
}
