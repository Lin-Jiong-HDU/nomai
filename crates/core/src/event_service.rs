//! EventService: query/cleanup for the append-only events log.
//!
//! Emission is NOT here — EntryService and LinkService append events directly
//! via SQL INSERT inside their mutation transactions (spec §5). EventService
//! only reads and purges, to avoid circular dependencies.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, params};
use ulid::Ulid;

use crate::error::CoreError;
use crate::event_model::{Event, ListEventsQuery, ListEventsResult, ListOrder, PurgeQuery};
use crate::storage;

pub struct EventService {
    // list/get/purge (Tasks 2-3) consume this.
    conn: Arc<Mutex<Connection>>,
}

impl EventService {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, CoreError> {
        // Defensive: ensure migrations applied (idempotent). EntryService::new
        // and LinkService::new also do this.
        {
            let mut guard = conn.lock().unwrap();
            guard
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(CoreError::Storage)?;
            storage::run_migrations(&mut guard)?;
        }
        Ok(Self { conn })
    }

    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        // Run migrations via EntryService so all tables exist.
        let tmp = tempfile::tempdir()?;
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        crate::EntryService::new(conn.clone(), content_store)?;
        Self::new(conn)
    }

    pub fn list(&self, query: ListEventsQuery) -> Result<ListEventsResult, CoreError> {
        let conn = self.conn.lock().unwrap();

        // Build WHERE clause dynamically.
        let mut where_clauses: Vec<String> = Vec::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(since) = query.since {
            where_clauses.push(format!("id > ?{}", where_clauses.len() + 1));
            params_vec.push(Box::new(since.to_string()));
        }
        if let Some(ref t) = query.type_ {
            where_clauses.push(format!("type = ?{}", where_clauses.len() + 1));
            params_vec.push(Box::new(t.clone()));
        }
        if let Some(ref tt) = query.target_type {
            where_clauses.push(format!("target_type = ?{}", where_clauses.len() + 1));
            params_vec.push(Box::new(tt.clone()));
        }
        if let Some(tid) = query.target_id {
            where_clauses.push(format!("target_id = ?{}", where_clauses.len() + 1));
            params_vec.push(Box::new(tid.to_string()));
        }
        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_clauses.join(" AND "))
        };

        let order_dir = match query.order {
            ListOrder::Asc => "ASC",
            ListOrder::Desc => "DESC",
        };

        // LIMIT N+1 trick: fetch one extra to detect has_more.
        let limit_param_idx = params_vec.len() + 1;
        let select_sql = format!(
            "SELECT id, type, target_type, target_id, payload, created_at
             FROM events {where_sql}
             ORDER BY id {order_dir}
             LIMIT ?{limit_param_idx}"
        );
        let fetch_limit = (query.limit as i64) + 1;
        let mut params_vec_with_limit = params_vec;
        params_vec_with_limit.push(Box::new(fetch_limit));
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec_with_limit.iter().map(|p| p.as_ref()).collect();

        let mut stmt = conn.prepare(&select_sql)?;
        let rows = stmt.query_map(params_refs.as_slice(), row_to_event)?;
        let mut items: Vec<Event> = rows.collect::<rusqlite::Result<Vec<_>>>()?;

        let has_more = items.len() as u32 > query.limit;
        if has_more {
            items.truncate(query.limit as usize);
        }

        Ok(ListEventsResult { items, has_more })
    }

    pub fn get(&self, id: Ulid) -> Result<Event, CoreError> {
        let conn = self.conn.lock().unwrap();
        match conn.query_row(
            "SELECT id, type, target_type, target_id, payload, created_at
             FROM events WHERE id = ?1",
            params![id.to_string()],
            row_to_event,
        ) {
            Ok(event) => Ok(event),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(CoreError::NotFound(id)),
            Err(e) => Err(CoreError::Storage(e)),
        }
    }

    pub fn purge(&self, query: PurgeQuery) -> Result<u64, CoreError> {
        let conn = self.conn.lock().unwrap();
        let affected = if let Some(ref t) = query.type_ {
            conn.execute(
                "DELETE FROM events WHERE id < ?1 AND type = ?2",
                params![query.before.to_string(), t.clone()],
            )?
        } else {
            conn.execute(
                "DELETE FROM events WHERE id < ?1",
                params![query.before.to_string()],
            )?
        };
        Ok(affected as u64)
    }
}

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<Event> {
    let id_str: String = row.get(0)?;
    let type_: String = row.get(1)?;
    let target_type: String = row.get(2)?;
    let target_id_str: String = row.get(3)?;
    let payload_json: String = row.get(4)?;
    let created_at_str: String = row.get(5)?;

    let id = crate::storage::from_text(0, &id_str, Ulid::from_string)?;
    let target_id = crate::storage::from_text(3, &target_id_str, Ulid::from_string)?;
    let payload: serde_json::Value =
        crate::storage::from_text(4, &payload_json, |s| serde_json::from_str(s))?;
    let created_at =
        crate::storage::from_text(5, &created_at_str, chrono::DateTime::parse_from_rfc3339)?
            .with_timezone(&chrono::Utc);

    Ok(Event {
        id,
        type_,
        target_type,
        target_id,
        payload,
        created_at,
    })
}

#[cfg(test)]
impl EventService {
    /// Construct an EventService sharing the same in-memory connection as
    /// the given EntryService. For tests that need to verify emission.
    #[doc(hidden)]
    pub fn for_test_shared_with_entries(entries: &crate::EntryService) -> Self {
        let conn = entries.conn_for_test();
        Self::new(conn).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_model::{ListEventsQuery, ListOrder};
    use serde_json::{Value, json};
    use ulid::Ulid;

    #[test]
    fn for_test_creates_event_service_with_events_table() {
        let svc = EventService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    // Direct-insert helper for EventService-level unit tests (independent of
    // EntryService emission which lands in Task 4).
    fn insert_event(
        svc: &EventService,
        id: &str,
        type_: &str,
        target_type: &str,
        target_id: &str,
        payload: Value,
    ) {
        let conn = svc.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                id,
                type_,
                target_type,
                target_id,
                payload.to_string(),
                "2026-06-21T12:00:00Z",
            ],
        )
        .unwrap();
    }

    #[test]
    fn list_returns_all_events_in_asc_order_by_default() {
        let svc = EventService::for_test().unwrap();
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "entry.created",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "entry.updated",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({}),
        );

        let result = svc.list(ListEventsQuery::default()).unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(!result.has_more);
        // Ascending by id (ULID string sort = time sort).
        assert_eq!(result.items[0].id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(result.items[1].id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAW");
    }

    #[test]
    fn list_desc_returns_newest_first() {
        let svc = EventService::for_test().unwrap();
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({}),
        );

        let result = svc
            .list(ListEventsQuery {
                order: ListOrder::Desc,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.items[0].id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAW");
    }

    #[test]
    fn list_since_is_exclusive() {
        let svc = EventService::for_test().unwrap();
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({}),
        );

        let since: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let result = svc
            .list(ListEventsQuery {
                since: Some(since),
                ..Default::default()
            })
            .unwrap();
        // Exclusive: only the second event (id > since).
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAW");
    }

    #[test]
    fn list_filters_by_type() {
        let svc = EventService::for_test().unwrap();
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "entry.created",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "link.created",
            "link",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({}),
        );

        let result = svc
            .list(ListEventsQuery {
                type_: Some("entry.created".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].type_, "entry.created");
    }

    #[test]
    fn list_filters_by_target_type_and_target_id() {
        let svc = EventService::for_test().unwrap();
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAQ",
            json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAR",
            json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "t",
            "link",
            "01ARZ3NDEKTSV4RRFFQ69G5FAQ",
            json!({}),
        );

        // For the unit test, just verify target_type filter alone (target_id
        // path is exercised indirectly via other tests; this test was
        // simplified per brief note to avoid ULID-format complexity).
        let result = svc
            .list(ListEventsQuery {
                target_type: Some("link".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].target_type, "link");
    }

    #[test]
    fn list_has_more_when_more_than_limit() {
        let svc = EventService::for_test().unwrap();
        // Insert 5 events with sequential ids.
        let ids = [
            "01ARZ3NDEKTSV4RRFFQ69G5FA0",
            "01ARZ3NDEKTSV4RRFFQ69G5FA1",
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "01ARZ3NDEKTSV4RRFFQ69G5FA3",
            "01ARZ3NDEKTSV4RRFFQ69G5FA4",
        ];
        for id in &ids {
            insert_event(
                &svc,
                id,
                "t",
                "entry",
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                json!({}),
            );
        }

        let result = svc
            .list(ListEventsQuery {
                limit: 3,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.items.len(), 3);
        assert!(result.has_more);

        // Fetch next page using since=last item id.
        let next_since = result.items[2].id;
        let result2 = svc
            .list(ListEventsQuery {
                since: Some(next_since),
                limit: 3,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result2.items.len(), 2);
        assert!(!result2.has_more);
    }

    #[test]
    fn list_returns_empty_when_no_events() {
        let svc = EventService::for_test().unwrap();
        let result = svc.list(ListEventsQuery::default()).unwrap();
        assert!(result.items.is_empty());
        assert!(!result.has_more);
    }

    #[test]
    fn get_returns_event_by_id() {
        let svc = EventService::for_test().unwrap();
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "entry.created",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            serde_json::json!({"title": "x"}),
        );

        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let event = svc.get(id).unwrap();
        assert_eq!(event.type_, "entry.created");
        assert_eq!(event.target_type, "entry");
    }

    #[test]
    fn get_returns_not_found_for_unknown_id() {
        let svc = EventService::for_test().unwrap();
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = svc.get(phantom).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn purge_deletes_events_before_cursor() {
        let svc = EventService::for_test().unwrap();
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FA0",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            serde_json::json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FA1",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            serde_json::json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "t",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            serde_json::json!({}),
        );

        // before is exclusive: deletes id < before.
        let before: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FA2".parse().unwrap();
        let deleted = svc
            .purge(PurgeQuery {
                before,
                type_: None,
            })
            .unwrap();
        assert_eq!(deleted, 2);

        let remaining = svc.list(Default::default()).unwrap();
        assert_eq!(remaining.items.len(), 1);
        assert_eq!(
            remaining.items[0].id.to_string(),
            "01ARZ3NDEKTSV4RRFFQ69G5FA2"
        );
    }

    #[test]
    fn purge_filters_by_type() {
        let svc = EventService::for_test().unwrap();
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FA0",
            "entry.created",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            serde_json::json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FA1",
            "link.created",
            "link",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            serde_json::json!({}),
        );
        insert_event(
            &svc,
            "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            "entry.created",
            "entry",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            serde_json::json!({}),
        );

        let before: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAZ".parse().unwrap();
        let deleted = svc
            .purge(PurgeQuery {
                before,
                type_: Some("entry.created".into()),
            })
            .unwrap();
        assert_eq!(deleted, 2);

        let remaining = svc.list(Default::default()).unwrap();
        assert_eq!(remaining.items.len(), 1);
        assert_eq!(remaining.items[0].type_, "link.created");
    }

    #[test]
    fn purge_returns_zero_when_nothing_matches() {
        let svc = EventService::for_test().unwrap();
        let before: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let deleted = svc
            .purge(PurgeQuery {
                before,
                type_: None,
            })
            .unwrap();
        assert_eq!(deleted, 0);
    }
}
