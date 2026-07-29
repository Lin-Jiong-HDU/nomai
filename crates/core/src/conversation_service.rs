//! ConversationService: CRUD + FTS search for conversations and turns.
//!
//! Conversations are SQLite-only (no FS persistence). Events are emitted
//! for every mutation so external sync consumers can observe conversation
//! lifecycle changes.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use ulid::Ulid;

use crate::conversation_model::{
    AppendTurns, Conversation, ConversationListOrder, ConversationListQuery,
    ConversationListResult, ConversationSearchResult, ConversationWithTurns, CreateConversation,
    CreateTurn, Turn, UpdateConversation,
};
use crate::error::CoreError;
use crate::storage;

/// SQL predicate matching transient conversations (same logic as entries).
const CONV_TRANSIENT_PREDICATE: &str = "(json_extract(c.attrs, '$.transient') = 'true' \
     OR json_extract(c.attrs, '$.transient') = 1)";

pub struct ConversationService {
    conn: Arc<Mutex<Connection>>,
    // No ContentStore — conversations are SQLite-only.
}

impl ConversationService {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, CoreError> {
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
        // Use a temp dir for ContentStore (needed by EntryService for
        // foreign key consistency even though conversations don't use it).
        let tmp = tempfile::tempdir()?;
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        crate::EntryService::new(conn.clone(), content_store, 1024)?;
        Self::new(conn)
    }

    // ── Create ──────────────────────────────────────────────────────

    pub fn create(&self, params: CreateConversation) -> Result<ConversationWithTurns, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.create_in_tx(&conn, params);
        match result {
            Ok(c) => {
                conn.execute_batch("COMMIT")?;
                Ok(c)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn create_in_tx(
        &self,
        conn: &Connection,
        params: CreateConversation,
    ) -> Result<ConversationWithTurns, CoreError> {
        let attrs = params
            .attrs
            .unwrap_or_else(|| Value::Object(Default::default()));
        if !attrs.is_object() {
            return Err(CoreError::Validation("attrs must be a JSON object".into()));
        }

        let now = Utc::now();
        let id = Ulid::new();
        let title = params.title.unwrap_or_default();
        let tags = params.tags.unwrap_or_default();

        conn.execute(
            "INSERT INTO conversations (id, title, tags, attrs, turn_count, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)",
            params![
                id.to_string(),
                &title,
                serde_json::to_string(&tags).expect("tags serialize"),
                attrs.to_string(),
                now.to_rfc3339(),
                now.to_rfc3339(),
            ],
        )?;

        let _conversation = Conversation {
            id,
            title,
            tags,
            attrs,
            turn_count: 0,
            created_at: now,
            updated_at: now,
        };

        // Create initial turns if provided.
        let mut turns = Vec::new();
        if let Some(create_turns) = params.turns {
            for (ordinal, ct) in create_turns.into_iter().enumerate() {
                let turn = self.insert_turn_in_tx(conn, id, ordinal as u32, &ct, now)?;
                turns.push(turn);
            }
            // Refresh turn_count from DB (triggers updated it).
            let _count: i64 = conn.query_row(
                "SELECT turn_count FROM conversations WHERE id = ?1",
                params![id.to_string()],
                |row| row.get(0),
            )?;
            // Re-read for updated_at.
        }

        // Re-read to get trigger-updated values.
        let updated_conv = Self::read_conversation(conn, id)?;

        // Emit event.
        let payload = serde_json::to_value(&ConversationWithTurns {
            conversation: updated_conv.clone(),
            turns: turns.clone(),
        })
        .expect("conversation serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, 'conversation.created', 'conversation', ?2, ?3, ?4)",
            params![
                Ulid::new().to_string(),
                id.to_string(),
                payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(ConversationWithTurns {
            conversation: updated_conv,
            turns,
        })
    }

    // ── Get ─────────────────────────────────────────────────────────

    pub fn get(&self, id: Ulid) -> Result<ConversationWithTurns, CoreError> {
        let conn = self.conn.lock().unwrap();
        let conversation = Self::read_conversation(&conn, id)?;
        let turns = Self::read_turns(&conn, id)?;
        Ok(ConversationWithTurns {
            conversation,
            turns,
        })
    }

    // ── Append turns ────────────────────────────────────────────────

    pub fn append_turns(&self, params: AppendTurns) -> Result<Vec<Turn>, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.append_turns_in_tx(&conn, params);
        match result {
            Ok(turns) => {
                conn.execute_batch("COMMIT")?;
                Ok(turns)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn append_turns_in_tx(
        &self,
        conn: &Connection,
        params: AppendTurns,
    ) -> Result<Vec<Turn>, CoreError> {
        // Verify conversation exists.
        Self::read_conversation(conn, params.conversation_id)?;

        // Determine next ordinal.
        let max_ord: Option<u32> = conn
            .query_row(
                "SELECT MAX(ordinal) FROM turns WHERE conversation_id = ?1",
                params![params.conversation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let next_ordinal = max_ord.map(|m| m + 1).unwrap_or(0);

        let now = Utc::now();
        let mut turns = Vec::with_capacity(params.turns.len());
        for (i, ct) in params.turns.into_iter().enumerate() {
            let turn = self.insert_turn_in_tx(
                conn,
                params.conversation_id,
                next_ordinal + i as u32,
                &ct,
                now,
            )?;
            turns.push(turn);
        }

        // Emit turn.appended event (one event for the batch).
        let payload = serde_json::to_value(&turns).expect("turns serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, 'turn.appended', 'conversation', ?2, ?3, ?4)",
            params![
                Ulid::new().to_string(),
                params.conversation_id.to_string(),
                payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(turns)
    }

    /// Insert a single turn. Does NOT emit events (caller handles that).
    fn insert_turn_in_tx(
        &self,
        conn: &Connection,
        conversation_id: Ulid,
        ordinal: u32,
        ct: &CreateTurn,
        now: chrono::DateTime<Utc>,
    ) -> Result<Turn, CoreError> {
        let id = Ulid::new();
        let attrs = ct
            .attrs
            .clone()
            .unwrap_or_else(|| Value::Object(Default::default()));
        if !attrs.is_object() {
            return Err(CoreError::Validation(
                "turn attrs must be a JSON object".into(),
            ));
        }
        conn.execute(
            "INSERT INTO turns (id, conversation_id, ordinal, role, content, attrs, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id.to_string(),
                conversation_id.to_string(),
                ordinal,
                &ct.role,
                &ct.content,
                attrs.to_string(),
                now.to_rfc3339(),
            ],
        )?;
        Ok(Turn {
            id,
            conversation_id,
            ordinal,
            role: ct.role.clone(),
            content: ct.content.clone(),
            attrs,
            created_at: now,
        })
    }

    // ── List ────────────────────────────────────────────────────────

    pub fn list(&self, query: ConversationListQuery) -> Result<ConversationListResult, CoreError> {
        let order_clause = match query.order {
            ConversationListOrder::CreatedDesc => "created_at DESC",
            ConversationListOrder::CreatedAsc => "created_at ASC",
            ConversationListOrder::UpdatedDesc => "updated_at DESC",
            ConversationListOrder::UpdatedAsc => "updated_at ASC",
        };

        let conn = self.conn.lock().unwrap();

        // Build WHERE clauses dynamically (same pattern as EntryService::list).
        let mut wheres: Vec<String> = Vec::new();
        let mut filter_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(tag) = &query.tag {
            wheres.push("EXISTS (SELECT 1 FROM json_each(c.tags) j WHERE j.value = ?)".into());
            filter_params.push(Box::new(tag.clone()));
        }
        match query.transient {
            Some(true) => wheres.push(CONV_TRANSIENT_PREDICATE.into()),
            Some(false) => wheres.push(
                "(json_extract(c.attrs, '$.transient') IS NULL OR \
                 (json_extract(c.attrs, '$.transient') != 'true' AND \
                  json_extract(c.attrs, '$.transient') != 1))"
                    .to_string(),
            ),
            None => {}
        }

        let where_sql = if wheres.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", wheres.join(" AND "))
        };

        // Items query.
        let items_sql = format!(
            "SELECT c.id, c.title, c.tags, c.attrs, c.turn_count, c.created_at, c.updated_at
             FROM conversations c{where_sql}
             ORDER BY c.{order_clause}
             LIMIT ? OFFSET ?"
        );
        let mut items_params: Vec<Box<dyn rusqlite::ToSql>> = filter_params;
        items_params.push(Box::new(query.limit as i64));
        items_params.push(Box::new(query.offset as i64));
        let items_refs: Vec<&dyn rusqlite::ToSql> =
            items_params.iter().map(|p| p.as_ref()).collect();

        let mut stmt = conn.prepare(&items_sql)?;
        let rows = stmt.query_map(items_refs.as_slice(), |row| row_to_conversation(row, 0))?;
        let items: Vec<Conversation> = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        // Count query.
        let count_sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
        let count_params: Vec<Box<dyn rusqlite::ToSql>> = if let Some(tag) = &query.tag {
            vec![Box::new(tag.clone())]
        } else {
            vec![]
        };
        let count_refs: Vec<&dyn rusqlite::ToSql> =
            count_params.iter().map(|p| p.as_ref()).collect();
        let total: u64 = conn.query_row(&count_sql, count_refs.as_slice(), |row| {
            row.get::<_, i64>(0)
        })? as u64;
        drop(conn);

        let has_more = (query.offset as u64 + items.len() as u64) < total;
        Ok(ConversationListResult {
            items,
            total,
            has_more,
        })
    }

    // ── Update ──────────────────────────────────────────────────────

    pub fn update(&self, id: Ulid, params: UpdateConversation) -> Result<Conversation, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.update_in_tx(&conn, id, params);
        match result {
            Ok(c) => {
                conn.execute_batch("COMMIT")?;
                Ok(c)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn update_in_tx(
        &self,
        conn: &Connection,
        id: Ulid,
        params: UpdateConversation,
    ) -> Result<Conversation, CoreError> {
        let existing = Self::read_conversation(conn, id)?;

        if let Some(ref attrs) = params.attrs {
            if !attrs.is_object() {
                return Err(CoreError::Validation("attrs must be a JSON object".into()));
            }
        }

        let new_title = params.title.unwrap_or_else(|| existing.title.clone());
        let new_tags = params.tags.unwrap_or_else(|| existing.tags.clone());
        let new_attrs = params.attrs.unwrap_or_else(|| existing.attrs.clone());
        let updated_at = Utc::now();

        conn.execute(
            "UPDATE conversations SET title=?1, tags=?2, attrs=?3, updated_at=?4 WHERE id=?5",
            params![
                &new_title,
                serde_json::to_string(&new_tags).expect("tags serialize"),
                new_attrs.to_string(),
                updated_at.to_rfc3339(),
                id.to_string(),
            ],
        )?;

        let updated = Conversation {
            id,
            title: new_title,
            tags: new_tags,
            attrs: new_attrs,
            turn_count: existing.turn_count,
            created_at: existing.created_at,
            updated_at,
        };

        // Emit event.
        let payload = serde_json::to_value(&updated).expect("conversation serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, 'conversation.updated', 'conversation', ?2, ?3, ?4)",
            params![
                Ulid::new().to_string(),
                id.to_string(),
                payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(updated)
    }

    // ── Delete ──────────────────────────────────────────────────────

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

    fn delete_in_tx(&self, conn: &Connection, id: Ulid) -> Result<(), CoreError> {
        let before = Self::read_conversation(conn, id)?;

        // Emit pre-delete snapshot event.
        let payload = serde_json::to_value(&before).expect("conversation serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, 'conversation.deleted', 'conversation', ?2, ?3, ?4)",
            params![
                Ulid::new().to_string(),
                id.to_string(),
                payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        // CASCADE handles turns + fts_turns cleanup via trigger.
        conn.execute(
            "DELETE FROM conversations WHERE id = ?1",
            params![id.to_string()],
        )?;
        Ok(())
    }

    // ── Search ──────────────────────────────────────────────────────

    pub fn search(
        &self,
        query: &str,
        limit: u32,
    ) -> Result<Vec<ConversationSearchResult>, CoreError> {
        let conn = self.conn.lock().unwrap();
        if query.chars().count() < 3 {
            // Short query: LIKE fallback (same pattern as fulltext search).
            let pattern = format!("%{}%", query.replace('%', "\\%").replace('_', "\\_"));
            let sql = "SELECT t.id, t.conversation_id, t.ordinal, t.role, t.content, \
                              t.attrs, t.created_at, \
                              c.id, c.title, c.tags, c.attrs, c.turn_count, \
                              c.created_at, c.updated_at \
                       FROM turns t \
                       JOIN conversations c ON c.id = t.conversation_id \
                       WHERE LOWER(t.content) LIKE LOWER(?) ESCAPE '\\' \
                       ORDER BY t.rowid DESC \
                       LIMIT ?";
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map(params![&pattern, limit as i64], |row| {
                self.row_to_search_result(row, query, 0.0)
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(CoreError::Storage)
        } else {
            // FTS5 path.
            let sql = "SELECT t.id, t.conversation_id, t.ordinal, t.role, t.content, \
                              t.attrs, t.created_at, \
                              c.id, c.title, c.tags, c.attrs, c.turn_count, \
                              c.created_at, c.updated_at, \
                              bm25(fts_turns) AS rank \
                       FROM fts_turns f \
                       JOIN turns t ON t.rowid = f.rowid \
                       JOIN conversations c ON c.id = t.conversation_id \
                       WHERE fts_turns MATCH ? \
                       ORDER BY rank \
                       LIMIT ?";
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map(params![query, limit as i64], |row| {
                let rank: f64 = row.get(14)?;
                self.row_to_search_result(row, query, rank.abs() as f32)
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(CoreError::Storage)
        }
    }

    fn row_to_search_result(
        &self,
        row: &rusqlite::Row<'_>,
        query: &str,
        score: f32,
    ) -> rusqlite::Result<ConversationSearchResult> {
        let turn = row_to_turn(row, 0)?;
        let conversation = row_to_conversation(row, 7)?;
        let snippet = crate::snippet::extract_snippet(&turn.content, query);
        Ok(ConversationSearchResult {
            conversation,
            turn,
            snippet,
            score,
        })
    }

    // ── Helpers ─────────────────────────────────────────────────────

    fn read_conversation(conn: &Connection, id: Ulid) -> Result<Conversation, CoreError> {
        match conn.query_row(
            "SELECT id, title, tags, attrs, turn_count, created_at, updated_at
             FROM conversations WHERE id = ?1",
            params![id.to_string()],
            |row| row_to_conversation(row, 0),
        ) {
            Ok(c) => Ok(c),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(CoreError::NotFound(id)),
            Err(e) => Err(CoreError::Storage(e)),
        }
    }

    fn read_turns(conn: &Connection, conversation_id: Ulid) -> Result<Vec<Turn>, CoreError> {
        let mut stmt = conn.prepare(
            "SELECT id, conversation_id, ordinal, role, content, attrs, created_at
             FROM turns WHERE conversation_id = ?1 ORDER BY ordinal ASC",
        )?;
        let rows = stmt.query_map(params![conversation_id.to_string()], |row| {
            row_to_turn(row, 0)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(CoreError::Storage)
    }

    #[doc(hidden)]
    pub fn conn_for_test(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }
}

// ── Row mappers ─────────────────────────────────────────────────────

fn row_to_conversation(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<Conversation> {
    let id_str: String = row.get(offset)?;
    let title: String = row.get(offset + 1)?;
    let tags_json: String = row.get(offset + 2)?;
    let attrs_json: String = row.get(offset + 3)?;
    let turn_count: u32 = row.get(offset + 4)?;
    let created_str: String = row.get(offset + 5)?;
    let updated_str: String = row.get(offset + 6)?;

    Ok(Conversation {
        id: storage::from_text(offset, &id_str, Ulid::from_string)?,
        title,
        tags: storage::from_text(offset + 2, &tags_json, |s| serde_json::from_str(s))?,
        attrs: storage::from_text(offset + 3, &attrs_json, |s| serde_json::from_str(s))?,
        turn_count,
        created_at: storage::from_text(
            offset + 5,
            &created_str,
            chrono::DateTime::parse_from_rfc3339,
        )?
        .with_timezone(&Utc),
        updated_at: storage::from_text(
            offset + 6,
            &updated_str,
            chrono::DateTime::parse_from_rfc3339,
        )?
        .with_timezone(&Utc),
    })
}

fn row_to_turn(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<Turn> {
    let id_str: String = row.get(offset)?;
    let conv_id_str: String = row.get(offset + 1)?;
    let ordinal: u32 = row.get(offset + 2)?;
    let role: String = row.get(offset + 3)?;
    let content: String = row.get(offset + 4)?;
    let attrs_json: String = row.get(offset + 5)?;
    let created_str: String = row.get(offset + 6)?;

    Ok(Turn {
        id: storage::from_text(offset, &id_str, Ulid::from_string)?,
        conversation_id: storage::from_text(offset + 1, &conv_id_str, Ulid::from_string)?,
        ordinal,
        role,
        content,
        attrs: storage::from_text(offset + 5, &attrs_json, |s| serde_json::from_str(s))?,
        created_at: storage::from_text(
            offset + 6,
            &created_str,
            chrono::DateTime::parse_from_rfc3339,
        )?
        .with_timezone(&Utc),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn note_turn(role: &str, content: &str) -> CreateTurn {
        CreateTurn {
            role: role.into(),
            content: content.into(),
            attrs: None,
        }
    }

    // ── Create + Get ────────────────────────────────────────────────

    #[test]
    fn create_and_get_round_trips() {
        let svc = ConversationService::for_test().unwrap();
        let created = svc
            .create(CreateConversation {
                title: Some("Test Chat".into()),
                tags: Some(vec!["test".into()]),
                attrs: Some(json!({"model": "opus"})),
                turns: Some(vec![
                    note_turn("user", "hello"),
                    note_turn("assistant", "hi there"),
                ]),
            })
            .unwrap();
        assert_eq!(created.conversation.title, "Test Chat");
        assert_eq!(created.turns.len(), 2);
        assert_eq!(created.turns[0].ordinal, 0);
        assert_eq!(created.turns[1].ordinal, 1);

        let fetched = svc.get(created.conversation.id).unwrap();
        assert_eq!(fetched.conversation.title, "Test Chat");
        assert_eq!(fetched.turns.len(), 2);
    }

    #[test]
    fn create_without_initial_turns() {
        let svc = ConversationService::for_test().unwrap();
        let created = svc
            .create(CreateConversation {
                title: Some("Empty".into()),
                tags: None,
                attrs: None,
                turns: None,
            })
            .unwrap();
        assert_eq!(created.conversation.turn_count, 0);
        assert!(created.turns.is_empty());
    }

    #[test]
    fn create_defaults_title_to_empty() {
        let svc = ConversationService::for_test().unwrap();
        let created = svc
            .create(CreateConversation {
                title: None,
                tags: None,
                attrs: None,
                turns: None,
            })
            .unwrap();
        assert_eq!(created.conversation.title, "");
    }

    // ── Append ──────────────────────────────────────────────────────

    #[test]
    fn append_turns_adds_in_order() {
        let svc = ConversationService::for_test().unwrap();
        let conv = svc
            .create(CreateConversation {
                title: Some("Chat".into()),
                tags: None,
                attrs: None,
                turns: Some(vec![note_turn("user", "first")]),
            })
            .unwrap();

        let appended = svc
            .append_turns(AppendTurns {
                conversation_id: conv.conversation.id,
                turns: vec![note_turn("assistant", "second"), note_turn("user", "third")],
            })
            .unwrap();
        assert_eq!(appended.len(), 2);
        assert_eq!(appended[0].ordinal, 1);
        assert_eq!(appended[1].ordinal, 2);

        let refreshed = svc.get(conv.conversation.id).unwrap();
        assert_eq!(refreshed.turns.len(), 3);
        assert_eq!(refreshed.conversation.turn_count, 3);
    }

    #[test]
    fn append_to_nonexistent_conversation_fails() {
        let svc = ConversationService::for_test().unwrap();
        let bad_id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = svc
            .append_turns(AppendTurns {
                conversation_id: bad_id,
                turns: vec![note_turn("user", "hi")],
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    // ── List ────────────────────────────────────────────────────────

    #[test]
    fn list_returns_paginated() {
        let svc = ConversationService::for_test().unwrap();
        for i in 0..5 {
            svc.create(CreateConversation {
                title: Some(format!("conv{i}")),
                tags: None,
                attrs: None,
                turns: None,
            })
            .unwrap();
        }
        let page = svc
            .list(ConversationListQuery {
                limit: 2,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.total, 5);
        assert!(page.has_more);
    }

    // ── Update ──────────────────────────────────────────────────────

    #[test]
    fn update_changes_title() {
        let svc = ConversationService::for_test().unwrap();
        let conv = svc
            .create(CreateConversation {
                title: Some("Old".into()),
                tags: None,
                attrs: None,
                turns: None,
            })
            .unwrap();
        let updated = svc
            .update(
                conv.conversation.id,
                UpdateConversation {
                    title: Some("New".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated.title, "New");
    }

    // ── Delete ──────────────────────────────────────────────────────

    #[test]
    fn delete_cascades_to_turns() {
        let svc = ConversationService::for_test().unwrap();
        let conv = svc
            .create(CreateConversation {
                title: Some("ToDelete".into()),
                tags: None,
                attrs: None,
                turns: Some(vec![note_turn("user", "msg")]),
            })
            .unwrap();
        let conv_id = conv.conversation.id;
        svc.delete(conv_id).unwrap();
        // Getting should fail.
        assert!(matches!(
            svc.get(conv_id).unwrap_err(),
            CoreError::NotFound(_)
        ));
        // Turns should be gone too (verify via raw query).
        let conn = svc.conn_for_test();
        let guard = conn.lock().unwrap();
        let turn_count: i64 = guard
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE conversation_id = ?1",
                params![conv_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(turn_count, 0);
    }

    // ── Search ──────────────────────────────────────────────────────

    #[test]
    fn search_finds_turn_content() {
        let svc = ConversationService::for_test().unwrap();
        svc.create(CreateConversation {
            title: Some("Chat".into()),
            tags: None,
            attrs: None,
            turns: Some(vec![note_turn("user", "Rust ownership is unique")]),
        })
        .unwrap();

        let results = svc.search("ownership", 10).unwrap();
        assert!(!results.is_empty());
        assert!(results[0].snippet.contains("ownership"));
        assert!(results[0].score >= 0.0);
    }

    #[test]
    fn search_short_query_uses_like_fallback() {
        let svc = ConversationService::for_test().unwrap();
        svc.create(CreateConversation {
            title: Some("Chat".into()),
            tags: None,
            attrs: None,
            turns: Some(vec![note_turn("user", "Rust")]),
        })
        .unwrap();

        // 2-char query → LIKE fallback.
        let results = svc.search("Ru", 10).unwrap();
        assert!(!results.is_empty());
    }
}
