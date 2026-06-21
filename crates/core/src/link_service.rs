//! LinkService: directed edges between entries.
//!
//! See spec §4-§5 for schema and RPC contract.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, params};
use ulid::Ulid;

use crate::error::CoreError;
use crate::link_model::{CreateLink, Link};
use crate::storage;

pub struct LinkService {
    // Used by business methods in Tasks 5-6 (create/get/delete).
    conn: Arc<Mutex<Connection>>,
}

impl LinkService {
    /// Take a shared connection and assume migrations have already been run
    /// (by `EntryService::new` or `Daemon::new`). Idempotent if migrations
    /// are no-ops.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, CoreError> {
        // Ensure migrations are applied (idempotent). EntryService::new
        // already runs them; this is defensive in case LinkService is
        // constructed directly.
        {
            let mut guard = conn.lock().unwrap();
            guard
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(CoreError::Storage)?;
            storage::run_migrations(&mut guard)?;
        }
        Ok(Self { conn })
    }

    /// Test-only constructor backed by an in-memory SQLite database.
    /// Mirrors `EntryService::for_test`.
    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        // Run migrations via EntryService so both entries and links tables exist.
        crate::EntryService::new(conn.clone())?;
        Self::new(conn)
    }

    /// Create a new directed link between two entries.
    ///
    /// `attrs` defaults to `{}` if omitted; non-object values are rejected
    /// with `CoreError::Validation`. FK violations (source/target does not
    /// exist) and UNIQUE conflicts on (source, target, relation) both map
    /// to `CoreError::Validation` per spec §5. Other SQLite errors map to
    /// `CoreError::Storage`.
    pub fn create(&self, params: CreateLink) -> Result<Link, CoreError> {
        let attrs = params
            .attrs
            .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
        if !attrs.is_object() {
            return Err(CoreError::Validation("attrs must be a JSON object".into()));
        }

        let now = Utc::now();
        let link = Link {
            id: Ulid::new(),
            source_id: params.source_id,
            target_id: params.target_id,
            relation: params.relation,
            attrs,
            created_at: now,
        };

        let conn = self.conn.lock().unwrap();
        let result = conn.execute(
            "INSERT INTO links (id, source_id, target_id, relation, attrs, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                link.id.to_string(),
                link.source_id.to_string(),
                link.target_id.to_string(),
                &link.relation,
                link.attrs.to_string(),
                link.created_at.to_rfc3339(),
            ],
        );

        match result {
            Ok(_) => Ok(link),
            Err(e) => {
                // FK violation (SQLITE_CONSTRAINT ForeignKey) or UNIQUE
                // violation (SQLITE_CONSTRAINT PrimaryKey/Unique) both map to
                // Validation per spec §5.
                if let rusqlite::Error::SqliteFailure(ref fe, _) = e {
                    if fe.code == rusqlite::ErrorCode::ConstraintViolation {
                        return Err(CoreError::Validation(format!(
                            "link constraint violation: {e}"
                        )));
                    }
                }
                Err(CoreError::Storage(e))
            }
        }
    }

    /// Fetch a link by id. Returns `CoreError::NotFound` if no link has this id.
    pub fn get(&self, id: Ulid) -> Result<Link, CoreError> {
        let conn = self.conn.lock().unwrap();
        match conn.query_row(
            "SELECT id, source_id, target_id, relation, attrs, created_at
             FROM links WHERE id = ?1",
            params![id.to_string()],
            row_to_link,
        ) {
            Ok(link) => Ok(link),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(CoreError::NotFound(id)),
            Err(e) => Err(CoreError::Storage(e)),
        }
    }

    /// Delete a link by id. Returns `CoreError::NotFound` if no link has this id.
    pub fn delete(&self, id: Ulid) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute(
            "DELETE FROM links WHERE id = ?1",
            params![id.to_string()],
        )?;
        if affected == 0 {
            Err(CoreError::NotFound(id))
        } else {
            Ok(())
        }
    }
}

fn row_to_link(row: &rusqlite::Row<'_>) -> rusqlite::Result<Link> {
    let id_str: String = row.get(0)?;
    let source_str: String = row.get(1)?;
    let target_str: String = row.get(2)?;
    let relation: String = row.get(3)?;
    let attrs_json: String = row.get(4)?;
    let created_at_str: String = row.get(5)?;

    let id = from_text(0, &id_str, Ulid::from_string)?;
    let source_id = from_text(1, &source_str, Ulid::from_string)?;
    let target_id = from_text(2, &target_str, Ulid::from_string)?;
    let attrs: serde_json::Value = from_text(4, &attrs_json, |s| serde_json::from_str(s))?;
    let created_at = from_text(5, &created_at_str, chrono::DateTime::parse_from_rfc3339)?
        .with_timezone(&Utc);

    Ok(Link {
        id,
        source_id,
        target_id,
        relation,
        attrs,
        created_at,
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
    use crate::{CreateEntry, EntryService};
    use serde_json::json;
    use ulid::Ulid;

    fn seed_entry(svc: &EntryService, title: &str) -> Ulid {
        svc.create(CreateEntry {
            title: title.into(),
            body: "body".into(),
            tags: None,
            attrs: None,
            source: None,
        })
        .unwrap()
        .id
    }

    // Build an EntryService and a LinkService that share a single in-memory
    // SQLite connection. The brief's prose calls `EntryService::for_test()`
    // and `LinkService::for_test()` separately, but each of those opens its
    // own in-memory DB, so links could never reference seeded entries. We
    // need one shared connection for FK to work. The body of each test
    // matches the brief verbatim from the `(entries, links)` pair downward.
    fn setup() -> (EntryService, LinkService) {
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let entries = EntryService::new(conn.clone()).unwrap();
        let links = LinkService::new(conn).unwrap();
        (entries, links)
    }

    #[test]
    fn for_test_creates_link_service_with_links_table() {
        let svc = LinkService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM links", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn create_returns_link_with_generated_id_and_timestamp() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        let link = links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "references".into(),
                attrs: None,
            })
            .unwrap();

        assert_eq!(link.source_id, a);
        assert_eq!(link.target_id, b);
        assert_eq!(link.relation, "references");
        assert_eq!(link.attrs, json!({})); // defaulted to empty object
        assert!(link.created_at <= chrono::Utc::now());
    }

    #[test]
    fn create_persists_link_retrievable_via_direct_query() {
        // Direct SQL query as a stand-in until get() is implemented in Task 6.
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        let link = links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "references".into(),
                attrs: Some(json!({"note": "see also"})),
            })
            .unwrap();

        let conn = links.conn.lock().unwrap();
        let (src, tgt, rel, attrs_json): (String, String, String, String) = conn
            .query_row(
                "SELECT source_id, target_id, relation, attrs FROM links WHERE id = ?1",
                rusqlite::params![link.id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(src, a.to_string());
        assert_eq!(tgt, b.to_string());
        assert_eq!(rel, "references");
        assert_eq!(attrs_json, r#"{"note":"see also"}"#);
    }

    #[test]
    fn create_rejects_non_object_attrs() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        let err = links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "x".into(),
                attrs: Some(json!([1, 2, 3])),
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_returns_validation_when_source_does_not_exist() {
        // FK violation. With PRAGMA foreign_keys = ON, SQLite returns
        // SQLITE_CONSTRAINT ForeignKey. Map this to Validation per spec §5.
        let (entries, links) = setup();
        let b = seed_entry(&entries, "b");
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();

        let err = links
            .create(CreateLink {
                source_id: phantom,
                target_id: b,
                relation: "x".into(),
                attrs: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_returns_validation_when_target_does_not_exist() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();

        let err = links
            .create(CreateLink {
                source_id: a,
                target_id: phantom,
                relation: "x".into(),
                attrs: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_returns_validation_on_duplicate_source_target_relation() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "references".into(),
                attrs: None,
            })
            .unwrap();

        let err = links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "references".into(),
                attrs: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_allows_same_pair_with_different_relation() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "references".into(),
                attrs: None,
            })
            .unwrap();
        // Different relation: should succeed (UNIQUE is on the triple).
        links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "see_also".into(),
                attrs: None,
            })
            .unwrap();
    }

    #[test]
    fn get_returns_link_created_by_create() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let created = links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "references".into(),
                attrs: None,
            })
            .unwrap();

        let fetched = links.get(created.id).unwrap();
        assert_eq!(created, fetched);
    }

    #[test]
    fn get_returns_not_found_for_unknown_id() {
        let (_entries, links) = setup();
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = links.get(phantom).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn delete_removes_link() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let created = links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "references".into(),
                attrs: None,
            })
            .unwrap();

        links.delete(created.id).unwrap();
        let err = links.get(created.id).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn delete_returns_not_found_for_unknown_id() {
        let (_entries, links) = setup();
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = links.delete(phantom).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn deleting_entry_cascades_to_links_as_source() {
        // FK ON DELETE CASCADE: removing entry A should remove all links where
        // A is source_id OR target_id.
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let c = seed_entry(&entries, "c");

        let link_ab = links
            .create(CreateLink {
                source_id: a,
                target_id: b,
                relation: "r".into(),
                attrs: None,
            })
            .unwrap();
        let link_ca = links
            .create(CreateLink {
                source_id: c,
                target_id: a,
                relation: "r".into(),
                attrs: None,
            })
            .unwrap();

        // Delete entry A.
        entries.delete(a).unwrap();

        // Both links referencing A (as source or target) should be gone.
        assert!(matches!(
            links.get(link_ab.id).unwrap_err(),
            CoreError::NotFound(_)
        ));
        assert!(matches!(
            links.get(link_ca.id).unwrap_err(),
            CoreError::NotFound(_)
        ));
    }
}
