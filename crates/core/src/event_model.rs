//! Event data model: append-only log of mutations on entries and links.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

/// An event in the append-only mutation log.
///
/// See spec §4-§6 for schema and RPC contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: Ulid,
    #[serde(rename = "type")]
    pub type_: String,         // JSON field is "type" (Rust keyword clash)
    pub target_type: String,   // "entry" | "link"
    pub target_id: Ulid,
    pub payload: Value,        // full snapshot JSON
    pub created_at: DateTime<Utc>,
}

/// Input for `EventService::list`. `since` is exclusive (returns id > since).
#[derive(Debug, Deserialize)]
pub struct ListEventsQuery {
    #[serde(default)]
    pub since: Option<Ulid>,
    #[serde(default)]
    pub type_: Option<String>,
    #[serde(default)]
    pub target_type: Option<String>,
    #[serde(default)]
    pub target_id: Option<Ulid>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub order: ListOrder,
}

impl Default for ListEventsQuery {
    fn default() -> Self {
        Self {
            since: None,
            type_: None,
            target_type: None,
            target_id: None,
            limit: default_limit(),
            order: ListOrder::default(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug)]
pub struct ListEventsResult {
    pub items: Vec<Event>,
    pub has_more: bool,
}

/// Input for `EventService::purge`. `before` is exclusive (deletes id < before).
#[derive(Debug, Deserialize)]
pub struct PurgeQuery {
    pub before: Ulid,
    #[serde(default)]
    pub type_: Option<String>,
}

fn default_limit() -> u32 {
    100
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_roundtrips_through_json() {
        let event = Event {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            type_: "entry.created".into(),
            target_type: "entry".into(),
            target_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".parse().unwrap(),
            payload: json!({"title": "x"}),
            created_at: "2026-06-21T12:00:00Z".parse().unwrap(),
        };
        let s = serde_json::to_string(&event).unwrap();
        // Critical: serde rename "type_" → "type" must round-trip.
        assert!(s.contains(r#""type":"entry.created""#));
        let back: Event = serde_json::from_str(&s).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn list_events_query_defaults() {
        let q: ListEventsQuery = serde_json::from_str(r#"{}"#).unwrap();
        assert!(q.since.is_none());
        assert!(q.type_.is_none());
        assert_eq!(q.limit, 100);
        assert_eq!(q.order, ListOrder::Asc);
    }

    #[test]
    fn list_order_serializes_as_snake_case() {
        assert_eq!(serde_json::to_string(&ListOrder::Asc).unwrap(), r#""asc""#);
        assert_eq!(serde_json::to_string(&ListOrder::Desc).unwrap(), r#""desc""#);
    }
}
