use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{Connection, params};
use serde::Deserialize;
use serde_json::Value;
use ulid::Ulid;

use crate::error::CoreError;
use crate::model::Entry;
use crate::storage;

pub struct EntryService {
    // Read paths (create/get/list/search/delete) are added in Tasks 3–5.
    conn: Mutex<Connection>,
}

#[derive(Debug, Deserialize)]
pub struct CreateEntry {
    pub title: String,
    pub body: String,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub attrs: Option<Value>,
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct UpdateEntry {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub attrs: Option<Value>,
    #[serde(default, with = "::serde_with::rust::double_option")]
    pub source: Option<Option<String>>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListOrder {
    #[default]
    CreatedDesc,
    CreatedAsc,
    UpdatedDesc,
    UpdatedAsc,
}

#[derive(Debug, Deserialize)]
pub struct EntryListQuery {
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    #[serde(default)]
    pub order: ListOrder,
}

fn default_limit() -> u32 {
    50
}

impl Default for EntryListQuery {
    fn default() -> Self {
        Self {
            tag: None,
            limit: default_limit(),
            offset: 0,
            order: ListOrder::default(),
        }
    }
}

#[derive(Debug)]
pub struct EntryListResult {
    pub items: Vec<Entry>,
    pub total: u64,
}

#[derive(Debug)]
pub struct FulltextSearchResult {
    pub entry: Entry,
    pub score: f32,
}

impl EntryService {
    /// Take ownership of a connection and run pending migrations.
    pub fn new(conn: Connection) -> Result<Self, CoreError> {
        let mut conn = conn;
        storage::run_migrations(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn create(&self, params: CreateEntry) -> Result<Entry, CoreError> {
        let attrs = params
            .attrs
            .unwrap_or_else(|| Value::Object(Default::default()));
        if !attrs.is_object() {
            return Err(CoreError::Validation("attrs must be a JSON object".into()));
        }

        let now = Utc::now();
        let entry = Entry {
            id: Ulid::new(),
            title: params.title,
            body: params.body,
            tags: params.tags.unwrap_or_default(),
            attrs,
            source: params.source,
            created_at: now,
            updated_at: now,
        };

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO entries
               (id, title, body, tags, attrs, source, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                entry.id.to_string(),
                &entry.title,
                &entry.body,
                serde_json::to_string(&entry.tags).expect("tags serialize"),
                entry.attrs.to_string(),
                &entry.source,
                entry.created_at.to_rfc3339(),
                entry.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(entry)
    }

    pub fn get(&self, id: Ulid) -> Result<Entry, CoreError> {
        let conn = self.conn.lock().unwrap();
        match conn.query_row(
            "SELECT id, title, body, tags, attrs, source, created_at, updated_at
             FROM entries WHERE id = ?1",
            params![id.to_string()],
            row_to_entry,
        ) {
            Ok(entry) => Ok(entry),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(CoreError::NotFound(id)),
            Err(e) => Err(CoreError::Storage(e)),
        }
    }

    pub fn update(&self, id: Ulid, params: UpdateEntry) -> Result<Entry, CoreError> {
        let existing = self.get(id)?;

        let attrs = match params.attrs {
            Some(a) if !a.is_object() => {
                return Err(CoreError::Validation("attrs must be a JSON object".into()));
            }
            Some(a) => a,
            None => existing.attrs,
        };
        let title = params.title.unwrap_or(existing.title);
        let body = params.body.unwrap_or(existing.body);
        let tags = params.tags.unwrap_or(existing.tags);
        let source = match params.source {
            Some(s) => s,
            None => existing.source,
        };
        let updated_at = Utc::now();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE entries SET title=?1, body=?2, tags=?3, attrs=?4, source=?5, updated_at=?6
             WHERE id=?7",
            params![
                &title,
                &body,
                serde_json::to_string(&tags).expect("tags serialize"),
                attrs.to_string(),
                &source,
                updated_at.to_rfc3339(),
                id.to_string(),
            ],
        )?;
        drop(conn);

        // Return the row as stored (canonical view).
        self.get(id)
    }

    pub fn delete(&self, id: Ulid) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute("DELETE FROM entries WHERE id=?1", params![id.to_string()])?;
        if affected == 0 {
            Err(CoreError::NotFound(id))
        } else {
            Ok(())
        }
    }

    pub fn list(&self, query: EntryListQuery) -> Result<EntryListResult, CoreError> {
        let order_clause = match query.order {
            ListOrder::CreatedDesc => "created_at DESC",
            ListOrder::CreatedAsc => "created_at ASC",
            ListOrder::UpdatedDesc => "updated_at DESC",
            ListOrder::UpdatedAsc => "updated_at ASC",
        };

        let conn = self.conn.lock().unwrap();

        let items: Vec<Entry> = if let Some(tag) = &query.tag {
            let sql = format!(
                "SELECT e.id, e.title, e.body, e.tags, e.attrs, e.source, e.created_at, e.updated_at
                 FROM entries e, json_each(e.tags) AS t
                 WHERE t.value = ?1
                 ORDER BY e.{order_clause}
                 LIMIT ?2 OFFSET ?3"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![tag, query.limit, query.offset], row_to_entry)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            let sql = format!(
                "SELECT id, title, body, tags, attrs, source, created_at, updated_at
                 FROM entries
                 ORDER BY {order_clause}
                 LIMIT ?1 OFFSET ?2"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![query.limit, query.offset], row_to_entry)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        let total: u64 = if let Some(tag) = &query.tag {
            conn.query_row(
                "SELECT COUNT(*) FROM entries e, json_each(e.tags) AS t WHERE t.value = ?1",
                params![tag],
                |row| row.get::<_, i64>(0),
            )? as u64
        } else {
            conn.query_row("SELECT COUNT(*) FROM entries", [], |row| {
                row.get::<_, i64>(0)
            })? as u64
        };

        Ok(EntryListResult { items, total })
    }

    pub fn fulltext_search(
        &self,
        query: &str,
        limit: u32,
    ) -> Result<Vec<FulltextSearchResult>, CoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.title, e.body, e.tags, e.attrs, e.source, e.created_at, e.updated_at,
                    bm25(fts_entries) AS rank
             FROM fts_entries
             JOIN entries e ON e.id = fts_entries.entry_id
             WHERE fts_entries MATCH ?1
             ORDER BY rank
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![query, limit], |row| {
            let entry = row_to_entry(row)?;
            // bm25 returns negative scores (closer to 0 = better match).
            let rank: f64 = row.get(8)?;
            Ok(FulltextSearchResult {
                entry,
                score: rank.abs() as f32,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(CoreError::Storage)
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Result<Self, CoreError> {
        Self::new(Connection::open_in_memory()?)
    }
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<Entry> {
    let id_str: String = row.get(0)?;
    let title: String = row.get(1)?;
    let body: String = row.get(2)?;
    let tags_json: String = row.get(3)?;
    let attrs_json: String = row.get(4)?;
    let source: Option<String> = row.get(5)?;
    let created_at_str: String = row.get(6)?;
    let updated_at_str: String = row.get(7)?;

    let id = from_text(0, &id_str, Ulid::from_string)?;
    let tags: Vec<String> = from_text(3, &tags_json, |s| serde_json::from_str(s))?;
    let attrs: Value = from_text(4, &attrs_json, |s| serde_json::from_str(s))?;
    let created_at =
        from_text(6, &created_at_str, chrono::DateTime::parse_from_rfc3339)?.with_timezone(&Utc);
    let updated_at =
        from_text(7, &updated_at_str, chrono::DateTime::parse_from_rfc3339)?.with_timezone(&Utc);

    Ok(Entry {
        id,
        title,
        body,
        tags,
        attrs,
        source,
        created_at,
        updated_at,
    })
}

fn from_text<T, E>(
    idx: usize,
    s: &str,
    f: impl for<'a> FnOnce(&'a str) -> Result<T, E>,
) -> rusqlite::Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    f(s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, Box::new(e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_runs_migrations_creating_entries_and_fts() {
        let svc = EntryService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();

        // entries table exists
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);

        // fts_entries virtual table exists
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM fts_entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn new_is_idempotent_across_repeated_calls() {
        // Each for_test() opens a fresh in-memory DB; migrations re-run cleanly.
        let _a = EntryService::for_test().unwrap();
        let _b = EntryService::for_test().unwrap();
    }

    use serde_json::json;
    use ulid::Ulid;

    #[test]
    fn create_round_trips_via_get() {
        let svc = EntryService::for_test().unwrap();
        let created = svc
            .create(CreateEntry {
                title: "Hello".into(),
                body: "Body text".into(),
                tags: Some(vec!["a".into(), "b".into()]),
                attrs: Some(json!({"k": "v"})),
                source: Some("test".into()),
            })
            .unwrap();
        let fetched = svc.get(created.id).unwrap();
        assert_eq!(created, fetched);
    }

    #[test]
    fn create_defaults_attrs_to_empty_object() {
        let svc = EntryService::for_test().unwrap();
        let created = svc
            .create(CreateEntry {
                title: "t".into(),
                body: "b".into(),
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        assert_eq!(created.attrs, json!({}));
        assert!(created.tags.is_empty());
        assert!(created.source.is_none());
    }

    #[test]
    fn create_rejects_non_object_attrs() {
        let svc = EntryService::for_test().unwrap();
        let err = svc
            .create(CreateEntry {
                title: "t".into(),
                body: "b".into(),
                tags: None,
                attrs: Some(json!([1, 2, 3])),
                source: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn get_returns_not_found_for_unknown_id() {
        let svc = EntryService::for_test().unwrap();
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = svc.get(id).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    use crate::service::{EntryListQuery, ListOrder, UpdateEntry};

    fn seed(svc: &EntryService, title: &str, tags: Vec<&str>) -> Entry {
        svc.create(CreateEntry {
            title: title.into(),
            body: format!("body of {title}"),
            tags: Some(tags.into_iter().map(String::from).collect()),
            attrs: None,
            source: None,
        })
        .unwrap()
    }

    #[test]
    fn update_changes_provided_fields_and_bumps_updated_at() {
        let svc = EntryService::for_test().unwrap();
        let created = seed(&svc, "orig", vec![]);
        // sleep tiny amount so updated_at strictly > created_at when serialized
        std::thread::sleep(std::time::Duration::from_millis(10));

        let updated = svc
            .update(
                created.id,
                UpdateEntry {
                    title: Some("new".into()),
                    body: None,
                    tags: Some(vec!["x".into()]),
                    attrs: Some(json!({"k2": "v2"})),
                    source: Some(Some("src".into())),
                },
            )
            .unwrap();

        assert_eq!(updated.title, "new");
        assert_eq!(updated.body, "body of orig"); // unchanged
        assert_eq!(updated.tags, vec!["x".to_string()]);
        assert_eq!(updated.attrs, json!({"k2": "v2"}));
        assert_eq!(updated.source.as_deref(), Some("src"));
        assert!(updated.updated_at > created.updated_at);
        assert_eq!(updated.created_at, created.created_at);
    }

    #[test]
    fn update_can_clear_source_to_null() {
        let svc = EntryService::for_test().unwrap();
        let created = svc
            .create(CreateEntry {
                title: "t".into(),
                body: "b".into(),
                tags: None,
                attrs: None,
                source: Some("orig".into()),
            })
            .unwrap();
        let updated = svc
            .update(
                created.id,
                UpdateEntry {
                    title: None,
                    body: None,
                    tags: None,
                    attrs: None,
                    source: Some(None),
                },
            )
            .unwrap();
        assert!(updated.source.is_none());
    }

    #[test]
    fn update_rejects_non_object_attrs() {
        let svc = EntryService::for_test().unwrap();
        let created = seed(&svc, "t", vec![]);
        let err = svc
            .update(
                created.id,
                UpdateEntry {
                    title: None,
                    body: None,
                    tags: None,
                    attrs: Some(json!([1, 2])),
                    source: None,
                },
            )
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn update_returns_not_found_for_unknown_id() {
        let svc = EntryService::for_test().unwrap();
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = svc
            .update(
                id,
                UpdateEntry {
                    title: None,
                    body: None,
                    tags: None,
                    attrs: None,
                    source: None,
                },
            )
            .unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn delete_returns_not_found_for_unknown_id() {
        let svc = EntryService::for_test().unwrap();
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        assert!(matches!(
            svc.delete(id).unwrap_err(),
            CoreError::NotFound(_)
        ));
    }

    #[test]
    fn list_returns_all_entries_paginated() {
        let svc = EntryService::for_test().unwrap();
        for i in 0..5 {
            seed(&svc, &format!("e{i}"), vec![]);
        }
        let page1 = svc
            .list(EntryListQuery {
                tag: None,
                limit: 2,
                offset: 0,
                order: ListOrder::CreatedAsc,
            })
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.total, 5);

        let page2 = svc
            .list(EntryListQuery {
                tag: None,
                limit: 2,
                offset: 2,
                order: ListOrder::CreatedAsc,
            })
            .unwrap();
        assert_eq!(page2.items.len(), 2);
        assert_ne!(page1.items[0].id, page2.items[0].id);
    }

    #[test]
    fn list_filters_by_tag() {
        let svc = EntryService::for_test().unwrap();
        seed(&svc, "a", vec!["red"]);
        seed(&svc, "b", vec!["blue"]);
        seed(&svc, "c", vec!["red", "blue"]);
        let result = svc
            .list(EntryListQuery {
                tag: Some("red".into()),
                limit: 50,
                offset: 0,
                order: ListOrder::CreatedAsc,
            })
            .unwrap();
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.total, 2);
    }

    // `FulltextSearchResult` is brought into scope via the `use super::*;`
    // glob at the top of `tests`; an explicit `use crate::service::...`
    // would trip `-D warnings` as unused.

    #[test]
    fn fulltext_returns_relevant_entries_ranked() {
        let svc = EntryService::for_test().unwrap();
        svc.create(CreateEntry {
            title: "Rust guide".into(),
            body: "Learn rust programming language".into(),
            tags: None,
            attrs: None,
            source: None,
        })
        .unwrap();
        svc.create(CreateEntry {
            title: "Cooking".into(),
            body: "How to bake bread".into(),
            tags: None,
            attrs: None,
            source: None,
        })
        .unwrap();

        let hits = svc.fulltext_search("rust", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry.title, "Rust guide");
        assert!(hits[0].score > 0.0);
    }

    #[test]
    fn fulltext_returns_empty_when_no_match() {
        let svc = EntryService::for_test().unwrap();
        seed(&svc, "t", vec![]);
        let hits = svc.fulltext_search("nonexistentterm12345", 10).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn delete_removes_entry_and_cascades_fts() {
        // Deferred from Task 4 — requires fulltext_search to verify FTS cleanup.
        let svc = EntryService::for_test().unwrap();
        let e = seed(&svc, "t", vec![]);
        svc.create(CreateEntry {
            title: "title".into(),
            body: "needle in haystack".into(),
            tags: None,
            attrs: None,
            source: None,
        })
        .unwrap();
        let id = e.id;
        svc.delete(id).unwrap();
        assert!(svc.get(id).is_err());

        let hits = svc.fulltext_search("needle", 10).unwrap();
        assert!(hits.iter().all(|h| h.entry.id != id));
    }
}
