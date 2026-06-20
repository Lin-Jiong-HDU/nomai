use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection};
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
        let attrs = params.attrs.unwrap_or_else(|| Value::Object(Default::default()));
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
    let created_at = from_text(6, &created_at_str, chrono::DateTime::parse_from_rfc3339)?
        .with_timezone(&Utc);
    let updated_at = from_text(7, &updated_at_str, chrono::DateTime::parse_from_rfc3339)?
        .with_timezone(&Utc);

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
}
