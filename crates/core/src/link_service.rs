//! LinkService: directed edges between entries.
//!
//! See spec §4-§5 for schema and RPC contract.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, params};
use ulid::Ulid;

use crate::error::CoreError;
use crate::link_model::{
    CreateLink, Direction, Link, ListLinkQuery, ListLinkResult, NeighborsQuery, NeighborsResult,
};
use crate::model::Entry;
use crate::service;
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
        let tmp = tempfile::tempdir()?;
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        crate::EntryService::new(conn.clone(), content_store)?;
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
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.create_in_tx(&conn, params);
        match result {
            Ok(link) => {
                conn.execute_batch("COMMIT")?;
                Ok(link)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Execute create within an existing transaction. Caller controls BEGIN/COMMIT.
    /// Does NOT lock self.conn (caller already holds the lock).
    /// Does NOT call self.get() or other self methods that lock conn.
    ///
    /// FK + UNIQUE ConstraintViolation → `CoreError::Validation` per spec §5.
    pub fn create_in_tx(&self, conn: &Connection, params: CreateLink) -> Result<Link, CoreError> {
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

        match conn.execute(
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
        ) {
            Ok(_) => {}
            Err(e) => {
                // Map ConstraintViolation (FK + UNIQUE) to Validation per spec.
                if let rusqlite::Error::SqliteFailure(ref fe, _) = e {
                    if fe.code == rusqlite::ErrorCode::ConstraintViolation {
                        return Err(CoreError::Validation(format!(
                            "link constraint violation: {e}"
                        )));
                    }
                }
                return Err(CoreError::Storage(e));
            }
        }

        let event_id = Ulid::new();
        let event_payload = serde_json::to_value(&link).expect("link serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event_id.to_string(),
                "link.created",
                "link",
                link.id.to_string(),
                event_payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(link)
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
        conn.execute_batch("BEGIN")?;
        let result = self.delete_in_tx(&conn, id);
        match result {
            Ok(()) => {
                conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Execute delete within an existing transaction. Caller controls BEGIN/COMMIT.
    /// SELECT before-snapshot, DELETE, INSERT event — all via passed conn.
    pub fn delete_in_tx(&self, conn: &Connection, id: Ulid) -> Result<(), CoreError> {
        let before_link = match conn.query_row(
            "SELECT id, source_id, target_id, relation, attrs, created_at
             FROM links WHERE id = ?1",
            params![id.to_string()],
            row_to_link,
        ) {
            Ok(l) => l,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Err(CoreError::NotFound(id)),
            Err(e) => return Err(CoreError::Storage(e)),
        };

        conn.execute("DELETE FROM links WHERE id = ?1", params![id.to_string()])?;

        let event_id = Ulid::new();
        let event_payload = serde_json::to_value(&before_link).expect("link serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event_id.to_string(),
                "link.deleted",
                "link",
                id.to_string(),
                event_payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(())
    }

    /// List links filtered by source, target, and/or relation.
    ///
    /// At least one of `from` / `to` must be `Some` — "list all links" is
    /// rejected with `CoreError::Validation` (spec §5). Results are ordered by
    /// `created_at, id` and paged via `limit` / `offset`. Returns the page
    /// plus the total count of matching rows (ignoring paging).
    pub fn list(&self, query: ListLinkQuery) -> Result<ListLinkResult, CoreError> {
        if query.from.is_none() && query.to.is_none() {
            return Err(CoreError::Validation(
                "list requires at least one of `from` or `to`".into(),
            ));
        }

        let conn = self.conn.lock().unwrap();

        // Build WHERE clause dynamically based on which filters are present.
        let mut where_clauses: Vec<String> = Vec::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        if let Some(from) = query.from {
            where_clauses.push(format!("source_id = ?{}", where_clauses.len() + 1));
            params_vec.push(Box::new(from.to_string()));
        }
        if let Some(to) = query.to {
            where_clauses.push(format!("target_id = ?{}", where_clauses.len() + 1));
            params_vec.push(Box::new(to.to_string()));
        }
        if let Some(ref relation) = query.relation {
            where_clauses.push(format!("relation = ?{}", where_clauses.len() + 1));
            params_vec.push(Box::new(relation.clone()));
        }
        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_clauses.join(" AND "))
        };

        // Count total.
        let count_sql = format!("SELECT COUNT(*) FROM links {where_sql}");
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let total: i64 = conn.query_row(&count_sql, params_refs.as_slice(), |row| row.get(0))?;

        // Fetch page.
        let limit_idx = params_vec.len() + 1;
        let offset_idx = params_vec.len() + 2;
        let select_sql = format!(
            "SELECT id, source_id, target_id, relation, attrs, created_at
             FROM links {where_sql}
             ORDER BY created_at, id
             LIMIT ?{limit_idx} OFFSET ?{offset_idx}"
        );
        let mut params_vec_with_paging = params_vec;
        params_vec_with_paging.push(Box::new(query.limit as i64));
        params_vec_with_paging.push(Box::new(query.offset as i64));
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec_with_paging.iter().map(|p| p.as_ref()).collect();

        let mut stmt = conn.prepare(&select_sql)?;
        let rows = stmt.query_map(params_refs.as_slice(), row_to_link)?;
        let items: Vec<_> = rows.collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(ListLinkResult {
            items,
            total: total as u64,
        })
    }

    /// List neighbor entries + the links that connect them to `query.id`,
    /// filtered by direction (Out / In / Both) and optional `relation`.
    ///
    /// In Both mode, the same neighbor entry appears at most once in `entries`
    /// even if connected via multiple links (e.g. A→B and B→A); `links`
    /// contains every matching link.
    pub fn neighbors(&self, query: NeighborsQuery) -> Result<NeighborsResult, CoreError> {
        let conn = self.conn.lock().unwrap();

        // Build the WHERE clause based on direction. The query node `id` is
        // matched against source_id (Out), target_id (In), or both (Both).
        let direction_filter = match query.direction {
            Direction::Out => "(source_id = ?1)",
            Direction::In => "(target_id = ?1)",
            Direction::Both => "(source_id = ?1 OR target_id = ?1)",
        };

        let relation_filter = if query.relation.is_some() {
            " AND relation = ?2"
        } else {
            ""
        };

        let limit_param_idx = if query.relation.is_some() { 3 } else { 2 };

        let sql = format!(
            "SELECT l.id, l.source_id, l.target_id, l.relation, l.attrs, l.created_at,
                    e.id, e.title, e.tags, e.attrs, e.source, e.created_at, e.updated_at
             FROM links l
             JOIN entries e ON e.id = CASE WHEN l.source_id = ?1 THEN l.target_id ELSE l.source_id END
             WHERE {direction_filter}{relation_filter}
             ORDER BY l.created_at, l.id
             LIMIT ?{limit_param_idx}"
        );

        let id_str = query.id.to_string();

        // `query_map` borrows from `stmt`; we materialize the rows to a Vec
        // inside each branch so the borrow is released before we iterate and
        // mutate `links` / `entries`.
        let mapped: Vec<rusqlite::Result<(Link, Entry)>> = match query.relation {
            Some(ref rel) => {
                let mut stmt = conn.prepare(&sql)?;
                stmt.query_map(
                    params![id_str, rel.clone(), query.limit as i64],
                    row_to_link_and_entry,
                )?
                .collect()
            }
            None => {
                let mut stmt = conn.prepare(&sql)?;
                stmt.query_map(params![id_str, query.limit as i64], row_to_link_and_entry)?
                    .collect()
            }
        };

        let mut links: Vec<Link> = Vec::new();
        let mut entries: Vec<Entry> = Vec::new();
        let mut seen: HashSet<Ulid> = HashSet::new();

        for row_result in mapped {
            let (link, entry) = row_result?;
            if seen.insert(entry.id) {
                entries.push(entry);
            }
            links.push(link);
        }

        Ok(NeighborsResult { entries, links })
    }
}

fn row_to_link_and_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<(Link, Entry)> {
    // Link columns: 0..6; Entry columns: 6..14.
    let link = Link {
        id: from_text(0, &row.get::<_, String>(0)?, Ulid::from_string)?,
        source_id: from_text(1, &row.get::<_, String>(1)?, Ulid::from_string)?,
        target_id: from_text(2, &row.get::<_, String>(2)?, Ulid::from_string)?,
        relation: row.get(3)?,
        attrs: from_text(4, &row.get::<_, String>(4)?, |s| serde_json::from_str(s))?,
        created_at: from_text(
            5,
            &row.get::<_, String>(5)?,
            chrono::DateTime::parse_from_rfc3339,
        )?
        .with_timezone(&Utc),
    };
    let entry = service::row_to_entry(row, 6)?;
    Ok((link, entry))
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
    let created_at =
        from_text(5, &created_at_str, chrono::DateTime::parse_from_rfc3339)?.with_timezone(&Utc);

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
    use crate::{CreateEntry, EntryService, ListLinkQuery};
    use serde_json::json;
    use ulid::Ulid;

    fn seed_entry(svc: &EntryService, title: &str) -> Ulid {
        svc.create(CreateEntry {
            title: title.into(),
            blocks: vec![crate::block_model::BlockInput {
                r#type: "note".into(),
                text: "body".into(),
                attrs: None,
            }],
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
        let tmp = tempfile::tempdir().unwrap();
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        let entries = EntryService::new(conn.clone(), content_store).unwrap();
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
    fn link_create_in_tx_works_within_external_transaction() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        let conn = links.conn.lock().unwrap();
        conn.execute_batch("BEGIN").unwrap();
        let link = links
            .create_in_tx(
                &conn,
                CreateLink {
                    source_id: a,
                    target_id: b,
                    relation: "r".into(),
                    attrs: None,
                },
            )
            .unwrap();
        conn.execute_batch("COMMIT").unwrap();
        drop(conn);

        let fetched = links.get(link.id).unwrap();
        assert_eq!(fetched.relation, "r");
    }

    #[test]
    fn link_delete_in_tx_works_within_external_transaction() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let created = seed_link(&links, a, b, "r");

        let conn = links.conn.lock().unwrap();
        conn.execute_batch("BEGIN").unwrap();
        links.delete_in_tx(&conn, created).unwrap();
        conn.execute_batch("COMMIT").unwrap();
        drop(conn);

        let err = links.get(created).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    #[test]
    fn delete_returns_not_found_for_unknown_id() {
        let (_entries, links) = setup();
        let phantom: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = links.delete(phantom).unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    fn seed_link(links: &LinkService, src: Ulid, tgt: Ulid, relation: &str) -> Ulid {
        links
            .create(CreateLink {
                source_id: src,
                target_id: tgt,
                relation: relation.into(),
                attrs: None,
            })
            .unwrap()
            .id
    }

    #[test]
    fn list_returns_validation_when_neither_from_nor_to_given() {
        let links = LinkService::for_test().unwrap();
        let err = links
            .list(ListLinkQuery {
                from: None,
                to: None,
                relation: None,
                limit: 50,
                offset: 0,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn list_by_from_returns_all_outgoing_links() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let c = seed_entry(&entries, "c");

        seed_link(&links, a, b, "references");
        seed_link(&links, a, c, "see_also");
        seed_link(&links, b, c, "references"); // not from a

        let result = links
            .list(ListLinkQuery {
                from: Some(a),
                to: None,
                relation: None,
                limit: 50,
                offset: 0,
            })
            .unwrap();
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.total, 2);
        assert!(result.items.iter().all(|l| l.source_id == a));
    }

    #[test]
    fn list_by_to_returns_all_incoming_links() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let c = seed_entry(&entries, "c");

        seed_link(&links, a, c, "r");
        seed_link(&links, b, c, "r");
        seed_link(&links, c, a, "r"); // not to c

        let result = links
            .list(ListLinkQuery {
                from: None,
                to: Some(c),
                relation: None,
                limit: 50,
                offset: 0,
            })
            .unwrap();
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.total, 2);
        assert!(result.items.iter().all(|l| l.target_id == c));
    }

    #[test]
    fn list_filters_by_relation() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        seed_link(&links, a, b, "references");
        seed_link(&links, a, b, "see_also");

        let result = links
            .list(ListLinkQuery {
                from: Some(a),
                to: None,
                relation: Some("references".into()),
                limit: 50,
                offset: 0,
            })
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].relation, "references");
    }

    #[test]
    fn list_paginates_with_limit_and_offset() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let others: Vec<Ulid> = (0..5)
            .map(|i| seed_entry(&entries, &format!("o{i}")))
            .collect();
        for o in &others {
            seed_link(&links, a, *o, "r");
        }

        let page1 = links
            .list(ListLinkQuery {
                from: Some(a),
                to: None,
                relation: None,
                limit: 2,
                offset: 0,
            })
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.total, 5);

        let page2 = links
            .list(ListLinkQuery {
                from: Some(a),
                to: None,
                relation: None,
                limit: 2,
                offset: 2,
            })
            .unwrap();
        assert_eq!(page2.items.len(), 2);
        // Pages should not overlap (links ordered by created_at then id; same
        // millisecond is possible, so just assert count + total here).
    }

    #[test]
    fn list_from_and_to_both_filters_intersection() {
        // from=A AND to=B: only the direct A→B links (any relation).
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let c = seed_entry(&entries, "c");

        seed_link(&links, a, b, "r");
        seed_link(&links, a, c, "r"); // not to b
        seed_link(&links, c, b, "r"); // not from a

        let result = links
            .list(ListLinkQuery {
                from: Some(a),
                to: Some(b),
                relation: None,
                limit: 50,
                offset: 0,
            })
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.total, 1);
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

    use crate::{Direction, NeighborsQuery};

    #[test]
    fn neighbors_out_returns_target_entries_and_links() {
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
        let link_ac = links
            .create(CreateLink {
                source_id: a,
                target_id: c,
                relation: "r".into(),
                attrs: None,
            })
            .unwrap();
        // Irrelevant: someone else's link, and a link into a.
        seed_link(&links, b, c, "r");

        let result = links
            .neighbors(NeighborsQuery {
                id: a,
                relation: None,
                direction: Direction::Out,
                limit: 50,
            })
            .unwrap();
        assert_eq!(result.links.len(), 2);
        assert_eq!(result.entries.len(), 2);
        let neighbor_ids: std::collections::HashSet<Ulid> =
            result.entries.iter().map(|e| e.id).collect();
        assert!(neighbor_ids.contains(&b));
        assert!(neighbor_ids.contains(&c));
        let link_ids: std::collections::HashSet<Ulid> = result.links.iter().map(|l| l.id).collect();
        assert!(link_ids.contains(&link_ab.id));
        assert!(link_ids.contains(&link_ac.id));
    }

    #[test]
    fn neighbors_in_returns_source_entries_and_links() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let c = seed_entry(&entries, "c");

        seed_link(&links, b, a, "r");
        seed_link(&links, c, a, "r");
        // Irrelevant.
        seed_link(&links, a, b, "r");

        let result = links
            .neighbors(NeighborsQuery {
                id: a,
                relation: None,
                direction: Direction::In,
                limit: 50,
            })
            .unwrap();
        assert_eq!(result.entries.len(), 2);
        let neighbor_ids: std::collections::HashSet<Ulid> =
            result.entries.iter().map(|e| e.id).collect();
        assert!(neighbor_ids.contains(&b));
        assert!(neighbor_ids.contains(&c));
    }

    #[test]
    fn neighbors_both_dedupes_entries() {
        // If A → B and B → A both exist, neighbors(both) of A should return
        // B exactly once (in entries), even though there are two links.
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        seed_link(&links, a, b, "r");
        seed_link(&links, b, a, "r");

        let result = links
            .neighbors(NeighborsQuery {
                id: a,
                relation: None,
                direction: Direction::Both,
                limit: 50,
            })
            .unwrap();
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].id, b);
        assert_eq!(result.links.len(), 2); // both links returned
    }

    #[test]
    fn neighbors_filters_by_relation() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");

        seed_link(&links, a, b, "references");
        seed_link(&links, a, b, "see_also");

        let result = links
            .neighbors(NeighborsQuery {
                id: a,
                relation: Some("references".into()),
                direction: Direction::Out,
                limit: 50,
            })
            .unwrap();
        assert_eq!(result.links.len(), 1);
        assert_eq!(result.links[0].relation, "references");
        assert_eq!(result.entries.len(), 1);
    }

    #[test]
    fn neighbors_respects_limit() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");
        let others: Vec<Ulid> = (0..5)
            .map(|i| seed_entry(&entries, &format!("o{i}")))
            .collect();
        for o in &others {
            seed_link(&links, a, *o, "r");
        }

        let result = links
            .neighbors(NeighborsQuery {
                id: a,
                relation: None,
                direction: Direction::Out,
                limit: 3,
            })
            .unwrap();
        assert_eq!(result.links.len(), 3);
        assert_eq!(result.entries.len(), 3);
    }

    #[test]
    fn neighbors_returns_empty_for_isolated_entry() {
        let (entries, links) = setup();
        let a = seed_entry(&entries, "a");

        let result = links
            .neighbors(NeighborsQuery {
                id: a,
                relation: None,
                direction: Direction::Both,
                limit: 50,
            })
            .unwrap();
        assert!(result.entries.is_empty());
        assert!(result.links.is_empty());
    }

    #[test]
    fn create_emits_link_created_event_with_full_snapshot() {
        use crate::EventService;
        use crate::ListEventsQuery;

        let (entries, links) = setup();
        let events = EventService::for_test_shared_with_entries(&entries);
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let link = links
            .create(crate::CreateLink {
                source_id: a,
                target_id: b,
                relation: "references".into(),
                attrs: None,
            })
            .unwrap();

        let result = events.list(ListEventsQuery::default()).unwrap();
        // 3 events: 2x entry.created + 1x link.created
        let link_events: Vec<_> = result
            .items
            .iter()
            .filter(|e| e.type_ == "link.created")
            .collect();
        assert_eq!(link_events.len(), 1);
        let event = link_events[0];
        assert_eq!(event.target_type, "link");
        assert_eq!(event.target_id, link.id);
        assert_eq!(event.payload["relation"], "references");
        assert_eq!(event.payload["source_id"], a.to_string());
        assert_eq!(event.payload["target_id"], b.to_string());
    }

    #[test]
    fn delete_emits_link_deleted_event_with_before_snapshot() {
        use crate::EventService;
        use crate::ListEventsQuery;

        let (entries, links) = setup();
        let events = EventService::for_test_shared_with_entries(&entries);
        let a = seed_entry(&entries, "a");
        let b = seed_entry(&entries, "b");
        let link = links
            .create(crate::CreateLink {
                source_id: a,
                target_id: b,
                relation: "r".into(),
                attrs: None,
            })
            .unwrap();

        links.delete(link.id).unwrap();

        let result = events.list(ListEventsQuery::default()).unwrap();
        let delete_events: Vec<_> = result
            .items
            .iter()
            .filter(|e| e.type_ == "link.deleted")
            .collect();
        assert_eq!(delete_events.len(), 1);
        let event = delete_events[0];
        assert_eq!(event.payload["relation"], "r");
        assert_eq!(event.payload["id"], link.id.to_string());
    }
}
