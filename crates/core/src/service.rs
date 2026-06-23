use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, params};
use serde::Deserialize;
use serde_json::Value;
use ulid::Ulid;

use crate::error::CoreError;
use crate::model::Entry;
use crate::storage;

pub struct EntryService {
    conn: Arc<Mutex<Connection>>,
    content_store: Arc<crate::content_store::ContentStore>,
    block_service: Arc<crate::block_service::BlockService>,
}

#[derive(Debug, Deserialize)]
pub struct CreateEntry {
    pub title: String,
    pub blocks: Vec<crate::block_model::BlockInput>,
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
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub attrs: Option<Value>,
    #[serde(default, with = "::serde_with::rust::double_option")]
    pub source: Option<Option<String>>,
}

/// Compute the "derived body" of an entry from its blocks. Used as the FTS5
/// index source, chunk derivation source, and embedding input. The join
/// separator is "\n\n" (paragraph break). Block order matters.
///
/// Plan 4 will replace this with per-block FTS + per-block chunks.
fn derived_body_from_blocks(blocks: &[crate::block_model::Block]) -> String {
    blocks
        .iter()
        .map(|b| b.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Same shape but takes parser-layer BlockInput (entry.create input path).
fn derived_body_from_inputs(blocks: &[crate::block_model::BlockInput]) -> String {
    blocks
        .iter()
        .map(|b| b.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Parse a block-type string (from BlockInput) into the parser-layer enum.
/// Unknown strings return CoreError::Validation so the caller can surface a
/// 400-style error rather than a storage failure.
fn parse_block_type(s: &str) -> Result<crate::nomai_format::BlockType, CoreError> {
    crate::nomai_format::BlockType::from_str(s)
        .ok_or_else(|| CoreError::Validation(format!("unknown block type: {s}")))
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

#[derive(Debug)]
pub struct SemanticSearchResult {
    pub entry: Entry,
    pub score: f32,
}

impl EntryService {
    /// Take ownership of a connection, run pending migrations, and wire up the
    /// FS-backed `ContentStore` + sibling `BlockService` used for block-level
    /// storage. The caller owns the `ContentStore` `Arc` and may share it with
    /// other services (daemon pattern: one store per daemon).
    pub fn new(
        conn: Arc<Mutex<Connection>>,
        content_store: Arc<crate::content_store::ContentStore>,
    ) -> Result<Self, CoreError> {
        {
            let mut guard = conn.lock().unwrap();
            guard
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(CoreError::Storage)?;
            storage::run_migrations(&mut guard)?;
            // V1 created `entries_ai/ad/au` triggers that populate fts_entries
            // from entries.body on INSERT/UPDATE/DELETE. Plan 3 stops using
            // entries.body and writes fts_entries directly (derived body from
            // blocks). Drop the triggers idempotently to avoid duplicate FTS
            // rows. V7 migration will also drop them; this covers the window
            // between Task 2 and Task 3.
            let _ = guard.execute_batch(
                "DROP TRIGGER IF EXISTS entries_ai;\
                 DROP TRIGGER IF EXISTS entries_ad;\
                 DROP TRIGGER IF EXISTS entries_au;",
            );
        }
        let block_service = Arc::new(crate::block_service::BlockService::new(conn.clone())?);
        Ok(Self {
            conn,
            content_store,
            block_service,
        })
    }

    pub fn create(&self, params: CreateEntry) -> Result<Entry, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.create_in_tx(&conn, params);
        match result {
            Ok(entry) => {
                conn.execute_batch("COMMIT")?;
                Ok(entry)
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
    /// FS write (.nomai render + atomic_write) happens inside this method. If
    /// the SQLite tx rolls back, the .nomai file is orphaned; Plan 5's
    /// index.sync reconciles. See Spec §7.1.
    pub fn create_in_tx(&self, conn: &Connection, params: CreateEntry) -> Result<Entry, CoreError> {
        let attrs = params
            .attrs
            .unwrap_or_else(|| Value::Object(Default::default()));
        if !attrs.is_object() {
            return Err(CoreError::Validation("attrs must be a JSON object".into()));
        }
        if params.blocks.is_empty() {
            return Err(CoreError::Validation("blocks must not be empty".into()));
        }

        let now = Utc::now();
        let id = Ulid::new();

        // 1. Render .nomai + write FS. FS write happens before the SQLite row
        //    INSERT; if it fails, we bail early without touching SQLite. If the
        //    SQLite side fails below, the .nomai file is orphaned (Plan 5
        //    reconciles via index.sync).
        let parser_blocks: Vec<crate::nomai_format::Block> = params
            .blocks
            .iter()
            .map(|b| {
                Ok(crate::nomai_format::Block {
                    r#type: parse_block_type(&b.r#type)?,
                    text: format!("{}\n", b.text),
                    attrs: b
                        .attrs
                        .as_ref()
                        .map(|v| v.as_object().cloned().unwrap_or_default())
                        .unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        let doc = crate::nomai_format::NomaiDoc {
            format_version: 1,
            id,
            title: params.title.clone(),
            tags: params.tags.clone().unwrap_or_default(),
            attrs: attrs.as_object().cloned().unwrap_or_default(),
            source: params.source.clone(),
            created_at: now,
            updated_at: now,
            blocks: parser_blocks,
        };
        self.content_store.write_entry(id, &doc)?;

        // Pre-compute derived body for FTS5 + entries.body (still NOT NULL in
        // V6 schema; V7 drops the column). The derived body is the canonical
        // source of truth — entries.body is a transitional duplicate.
        let body = derived_body_from_inputs(&params.blocks);

        // 2. INSERT entry row.
        conn.execute(
            "INSERT INTO entries
               (id, title, body, tags, attrs, source, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                id.to_string(),
                &params.title,
                &body,
                serde_json::to_string(&params.tags.unwrap_or_default()).expect("tags serialize"),
                attrs.to_string(),
                &params.source,
                now.to_rfc3339(),
                now.to_rfc3339(),
            ],
        )?;

        // 3. Create each block via BlockService (same conn — no nested tx).
        let mut stored_blocks: Vec<crate::block_model::Block> =
            Vec::with_capacity(params.blocks.len());
        for (ordinal, block_input) in params.blocks.into_iter().enumerate() {
            let create_block = crate::block_model::CreateBlock {
                entry_id: id,
                ordinal: ordinal as u32,
                r#type: block_input.r#type,
                text: block_input.text,
                attrs: block_input.attrs,
            };
            let block = self.block_service.create_in_tx(conn, create_block)?;
            stored_blocks.push(block);
        }

        // 4. Populate fts_entries directly. V1 triggers were dropped in new()
        //    so this is the sole source of FTS5 rows.
        conn.execute(
            "INSERT INTO fts_entries (entry_id, title, body) VALUES (?1, ?2, ?3)",
            params![id.to_string(), &doc.title, &body],
        )?;

        // 5. Emit entry.created event with full snapshot (blocks included).
        let entry_snapshot = Entry {
            id,
            title: doc.title,
            blocks: stored_blocks,
            tags: doc.tags,
            attrs,
            source: doc.source,
            created_at: now,
            updated_at: now,
        };
        let event_id = Ulid::new();
        let event_payload = serde_json::to_value(&entry_snapshot).expect("entry serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event_id.to_string(),
                "entry.created",
                "entry",
                id.to_string(),
                event_payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(entry_snapshot)
    }

    pub fn get(&self, id: Ulid) -> Result<Entry, CoreError> {
        let mut entry = {
            let conn = self.conn.lock().unwrap();
            match conn.query_row(
                "SELECT id, title, body, tags, attrs, source, created_at, updated_at
                 FROM entries WHERE id = ?1",
                params![id.to_string()],
                |row| row_to_entry(row, 0),
            ) {
                Ok(e) => e,
                Err(rusqlite::Error::QueryReturnedNoRows) => return Err(CoreError::NotFound(id)),
                Err(e) => return Err(CoreError::Storage(e)),
            }
        };
        // Populate blocks via BlockService (separate lock acquisition).
        let blocks_result = self.block_service.list(id)?;
        entry.blocks = blocks_result.items;
        Ok(entry)
    }

    pub fn update(&self, id: Ulid, params: UpdateEntry) -> Result<Entry, CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.update_in_tx(&conn, id, params);
        match result {
            Ok(entry) => {
                conn.execute_batch("COMMIT")?;
                Ok(entry)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Execute update within an existing transaction. Caller controls BEGIN/COMMIT.
    /// Does NOT call self.get() — inlines SELECT to avoid re-locking conn.
    ///
    /// Per Spec §6.1, blocks are immutable at this layer; entry.update only
    /// mutates metadata (title/tags/attrs/source). Block updates are a
    /// separate RPC (block.update is delete + create in Plan 3+).
    pub fn update_in_tx(
        &self,
        conn: &Connection,
        id: Ulid,
        params: UpdateEntry,
    ) -> Result<Entry, CoreError> {
        // Inline SELECT existing (same as row_to_entry at offset 0).
        let mut existing = match conn.query_row(
            "SELECT id, title, body, tags, attrs, source, created_at, updated_at
             FROM entries WHERE id = ?1",
            params![id.to_string()],
            |row| row_to_entry(row, 0),
        ) {
            Ok(e) => e,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Err(CoreError::NotFound(id)),
            Err(e) => return Err(CoreError::Storage(e)),
        };
        // Populate blocks so the returned snapshot includes them. The SELECT
        // above leaves blocks empty (BlockService.list is a separate query).
        // We do this via block_service using the same conn's mutex — but
        // block_service.list locks self.conn itself, so we must release our
        // conceptual hold. In practice the caller (update()) has the lock;
        // block_service.list would deadlock. Inline the query instead.
        let blocks_for_snapshot: Vec<crate::block_model::Block> = {
            let mut stmt = conn.prepare(
                "SELECT id, entry_id, ordinal, type, text, attrs, created_at, updated_at
                 FROM blocks WHERE entry_id = ?1
                 ORDER BY ordinal ASC",
            )?;
            let rows = stmt.query_map(params![id.to_string()], |row| {
                crate::block_service::row_to_block(row, 0)
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        existing.blocks = blocks_for_snapshot;

        let new_attrs = match params.attrs {
            Some(a) if !a.is_object() => {
                return Err(CoreError::Validation("attrs must be a JSON object".into()));
            }
            Some(a) => a,
            None => existing.attrs,
        };
        let new_title = params.title.unwrap_or_else(|| existing.title.clone());
        let new_tags = params.tags.unwrap_or_else(|| existing.tags.clone());
        let new_source = match params.source {
            Some(s) => s,
            None => existing.source.clone(),
        };
        let updated_at = Utc::now();

        // Derived body is computed from blocks (which didn't change in this
        // update path). Keep entries.body column in sync for V6 schema; V7
        // drops the column entirely.
        let body = derived_body_from_blocks(&existing.blocks);

        let updated_entry = Entry {
            id,
            title: new_title.clone(),
            blocks: existing.blocks.clone(),
            tags: new_tags.clone(),
            attrs: new_attrs.clone(),
            source: new_source.clone(),
            created_at: existing.created_at,
            updated_at,
        };

        conn.execute(
            "UPDATE entries SET title=?1, body=?2, tags=?3, attrs=?4, source=?5, updated_at=?6
             WHERE id=?7",
            params![
                &new_title,
                &body,
                serde_json::to_string(&new_tags).expect("tags serialize"),
                new_attrs.to_string(),
                &new_source,
                updated_at.to_rfc3339(),
                id.to_string(),
            ],
        )?;

        // FTS5: title may have changed; body is derived from blocks which
        // didn't change here. Rewrite the row.
        conn.execute(
            "DELETE FROM fts_entries WHERE entry_id = ?1",
            params![id.to_string()],
        )?;
        conn.execute(
            "INSERT INTO fts_entries (entry_id, title, body) VALUES (?1, ?2, ?3)",
            params![id.to_string(), &new_title, &body],
        )?;

        let event_id = Ulid::new();
        let event_payload = serde_json::to_value(&updated_entry).expect("entry serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event_id.to_string(),
                "entry.updated",
                "entry",
                id.to_string(),
                event_payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(updated_entry)
    }

    pub fn delete(&self, id: Ulid) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN")?;
        let result = self.delete_in_tx(&conn, id);
        match result {
            Ok(()) => {
                conn.execute_batch("COMMIT")?;
                // After COMMIT, drop the lock and remove the FS directory.
                drop(conn);
                self.content_store.delete_entry(id)?;
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
    ///
    /// Does NOT touch FS — caller controls the transaction; FS cleanup is the
    /// non-`_in_tx` `delete`'s responsibility. This matches Plan 2's pattern.
    pub fn delete_in_tx(&self, conn: &Connection, id: Ulid) -> Result<(), CoreError> {
        // SELECT before-snapshot (blocks populated for event payload).
        let mut before_entry = match conn.query_row(
            "SELECT id, title, body, tags, attrs, source, created_at, updated_at
             FROM entries WHERE id = ?1",
            params![id.to_string()],
            |row| row_to_entry(row, 0),
        ) {
            Ok(e) => e,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Err(CoreError::NotFound(id)),
            Err(e) => return Err(CoreError::Storage(e)),
        };
        // Populate blocks inline (block_service.list would re-lock self.conn).
        let blocks: Vec<crate::block_model::Block> = {
            let mut stmt = conn.prepare(
                "SELECT id, entry_id, ordinal, type, text, attrs, created_at, updated_at
                 FROM blocks WHERE entry_id = ?1
                 ORDER BY ordinal ASC",
            )?;
            let rows = stmt.query_map(params![id.to_string()], |row| {
                crate::block_service::row_to_block(row, 0)
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        before_entry.blocks = blocks;

        conn.execute("DELETE FROM entries WHERE id=?1", params![id.to_string()])?;
        // CASCADE handles blocks, chunks, links, vec_embeddings (via delete_embedding
        // at higher layer), fts_entries (manual cleanup below — no trigger in V6).

        // fts_entries cleanup (V1 triggers dropped; no CASCADE since FTS5 is a
        // separate virtual table).
        conn.execute(
            "DELETE FROM fts_entries WHERE entry_id = ?1",
            params![id.to_string()],
        )?;

        let event_id = Ulid::new();
        let event_payload = serde_json::to_value(&before_entry).expect("entry serialize");
        conn.execute(
            "INSERT INTO events (id, type, target_type, target_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event_id.to_string(),
                "entry.deleted",
                "entry",
                id.to_string(),
                event_payload.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(())
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
            let rows = stmt.query_map(params![tag, query.limit, query.offset], |row| {
                row_to_entry(row, 0)
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            let sql = format!(
                "SELECT id, title, body, tags, attrs, source, created_at, updated_at
                 FROM entries
                 ORDER BY {order_clause}
                 LIMIT ?1 OFFSET ?2"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![query.limit, query.offset], |row| {
                row_to_entry(row, 0)
            })?;
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
            let entry = row_to_entry(row, 0)?;
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

    pub fn ensure_vec_embeddings(&self, dim: usize) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        let exists: bool = conn.query_row(
            "SELECT EXISTS (
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='vec_embeddings'
            )",
            [],
            |row| row.get(0),
        )?;
        if exists {
            return Ok(());
        }
        let sql = format!(
            "CREATE VIRTUAL TABLE vec_embeddings USING vec0(
                entry_id TEXT PRIMARY KEY,
                embedding float[{dim}] distance_metric=cosine
            )"
        );
        conn.execute_batch(&sql)?;
        Ok(())
    }

    /// Delete the stored embedding for an entry, if any.
    ///
    /// Used when an entry's body is cleared (set to empty) so the previous
    /// embedding no longer matches semantic searches. Returns `Ok(())` whether
    /// or not a row existed (delete-by-id is idempotent at the call site).
    pub fn delete_embedding(&self, id: Ulid) -> Result<(), CoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM vec_embeddings WHERE entry_id = ?1",
            params![id.to_string()],
        )?;
        Ok(())
    }

    pub fn write_embedding(&self, id: Ulid, embedding: &[f32]) -> Result<(), CoreError> {
        let bytes: Vec<u8> = embedding.iter().flat_map(|f| f.to_le_bytes()).collect();
        let id_str = id.to_string();
        let conn = self.conn.lock().unwrap();
        // sqlite-vec's vec0 virtual table does not support INSERT OR REPLACE
        // (see https://github.com/asg017/sqlite-vec/issues/259). Emulate the
        // upsert with a DELETE-then-INSERT inside a transaction so the
        // operation is atomic and re-inserting the same id replaces the row.
        conn.execute_batch("BEGIN")?;
        let result = (|| -> rusqlite::Result<()> {
            conn.execute(
                "DELETE FROM vec_embeddings WHERE entry_id = ?1",
                params![&id_str],
            )?;
            conn.execute(
                "INSERT INTO vec_embeddings (entry_id, embedding) VALUES (?1, ?2)",
                params![&id_str, &bytes],
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(e) => {
                // Best-effort rollback; ignore rollback failure to surface the
                // original error.
                let _ = conn.execute_batch("ROLLBACK");
                Err(CoreError::Storage(e))
            }
        }
    }

    pub fn semantic_search(
        &self,
        query: &[f32],
        limit: u32,
    ) -> Result<Vec<SemanticSearchResult>, CoreError> {
        let bytes: Vec<u8> = query.iter().flat_map(|f| f.to_le_bytes()).collect();
        // Phase 1: collect (entry_id, distance) pairs under the lock.
        let pairs: Vec<(Ulid, f64)> = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT v.entry_id, v.distance
                 FROM vec_embeddings v
                 JOIN entries e ON e.id = v.entry_id
                 WHERE v.embedding MATCH ?1
                   AND k = ?2
                 ORDER BY v.distance",
            )?;
            let rows = stmt.query_map(params![bytes, limit], |row| {
                let id_str: String = row.get(0)?;
                let distance: f64 = row.get(1)?;
                Ok((id_str, distance))
            })?;
            let mut out: Vec<(Ulid, f64)> = Vec::new();
            for r in rows {
                let (id_str, distance) = r?;
                let id: Ulid = id_str.parse().map_err(|e: ulid::DecodeError| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;
                out.push((id, distance));
            }
            out
        };
        // Phase 2: populate entry (with blocks) per id via self.get().
        let mut results = Vec::with_capacity(pairs.len());
        for (entry_id, distance) in pairs {
            let entry = self.get(entry_id)?;
            results.push(SemanticSearchResult {
                entry,
                score: (1.0 - distance) as f32,
            });
        }
        Ok(results)
    }

    /// Test-only constructor backed by an in-memory SQLite database.
    ///
    /// Not gated by `cfg(test)` so that downstream crates (e.g. `nomai-daemon`)
    /// can use it from their own `#[cfg(test)]` modules; the `#[doc(hidden)]`
    /// attribute keeps it out of the public API surface. Also registers the
    /// sqlite-vec extension globally so `vec0` virtual tables work without the
    /// caller having to remember `storage::init_sqlite_extensions()`.
    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        let tmp_dir = std::env::temp_dir().join(format!("nomai-test-{}", Ulid::new()));
        let content_store = Arc::new(crate::content_store::ContentStore::new(tmp_dir));
        Self::new(conn, content_store)
    }

    /// Test-only accessor for the shared connection.
    ///
    /// Not gated by `#[cfg(test)]` because that attribute does not propagate
    /// across crate boundaries — the daemon crate's `#[cfg(test)]` modules
    /// would not see it. `#[doc(hidden)] pub` matches the existing
    /// `EntryService::for_test` convention so callers can build sibling
    /// services (e.g. `LinkService`) against the same shared connection.
    #[doc(hidden)]
    pub fn conn_for_test(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }
}

pub(crate) fn row_to_entry(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<Entry> {
    let id_str: String = row.get(offset)?;
    let title: String = row.get(offset + 1)?;
    // entries.body still exists in V6 schema (NOT NULL) but is unused. We read
    // it here to advance the column cursor, then discard. V7 drops the column
    // and this read goes away.
    let _body: String = row.get(offset + 2)?;
    let tags_json: String = row.get(offset + 3)?;
    let attrs_json: String = row.get(offset + 4)?;
    let source: Option<String> = row.get(offset + 5)?;
    let created_at_str: String = row.get(offset + 6)?;
    let updated_at_str: String = row.get(offset + 7)?;

    let id = from_text(offset, &id_str, Ulid::from_string)?;
    let tags: Vec<String> = from_text(offset + 3, &tags_json, |s| serde_json::from_str(s))?;
    let attrs: Value = from_text(offset + 4, &attrs_json, |s| serde_json::from_str(s))?;
    let created_at = from_text(
        offset + 6,
        &created_at_str,
        chrono::DateTime::parse_from_rfc3339,
    )?
    .with_timezone(&Utc);
    let updated_at = from_text(
        offset + 7,
        &updated_at_str,
        chrono::DateTime::parse_from_rfc3339,
    )?
    .with_timezone(&Utc);

    Ok(Entry {
        id,
        title,
        blocks: Vec::new(), // populated by get() via BlockService, not by SELECT
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

    #[test]
    fn new_enables_foreign_keys_pragma() {
        let svc = EntryService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        let fk_enabled: bool = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert!(
            fk_enabled,
            "PRAGMA foreign_keys must be ON after EntryService::new"
        );
    }

    #[test]
    fn v2_migration_creates_links_table() {
        let svc = EntryService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM links", [], |row| row.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn v2_migration_links_table_has_expected_schema() {
        let svc = EntryService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        // Verify FK and UNIQUE constraints are registered.
        let mut stmt = conn
            .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='links'")
            .unwrap();
        let sql: String = stmt.query_row([], |row| row.get(0)).unwrap();
        assert!(sql.contains("FOREIGN KEY (source_id) REFERENCES entries(id) ON DELETE CASCADE"));
        assert!(sql.contains("UNIQUE(source_id, target_id, relation)"));
    }

    use serde_json::json;
    use ulid::Ulid;

    use crate::block_model::BlockInput;
    use crate::service::{EntryListQuery, ListOrder, UpdateEntry};

    /// Build a single-note block with the given text.
    fn note_block(text: impl Into<String>) -> BlockInput {
        BlockInput {
            r#type: "note".into(),
            text: text.into(),
            attrs: None,
        }
    }

    #[test]
    fn create_round_trips_via_get() {
        let svc = EntryService::for_test().unwrap();
        let created = svc
            .create(CreateEntry {
                title: "Hello".into(),
                blocks: vec![note_block("Body text")],
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
                blocks: vec![note_block("b")],
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
                blocks: vec![note_block("b")],
                tags: None,
                attrs: Some(json!([1, 2, 3])),
                source: None,
            })
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn create_rejects_empty_blocks() {
        let svc = EntryService::for_test().unwrap();
        let err = svc
            .create(CreateEntry {
                title: "t".into(),
                blocks: vec![],
                tags: None,
                attrs: None,
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

    fn seed(svc: &EntryService, title: &str, tags: Vec<&str>) -> Entry {
        svc.create(CreateEntry {
            title: title.into(),
            blocks: vec![note_block(format!("body of {title}"))],
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
                    tags: Some(vec!["x".into()]),
                    attrs: Some(json!({"k2": "v2"})),
                    source: Some(Some("src".into())),
                },
            )
            .unwrap();

        assert_eq!(updated.title, "new");
        // Blocks unchanged (update doesn't touch blocks).
        assert_eq!(updated.blocks.len(), 1);
        assert_eq!(updated.blocks[0].text, "body of orig");
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
                blocks: vec![note_block("b")],
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
            blocks: vec![note_block("Learn rust programming language")],
            tags: None,
            attrs: None,
            source: None,
        })
        .unwrap();
        svc.create(CreateEntry {
            title: "Cooking".into(),
            blocks: vec![note_block("How to bake bread")],
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
            blocks: vec![note_block("needle in haystack")],
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

    // vec0 virtual tables require sqlite-vec to be auto-registered before the
    // in-memory connection is opened. `for_test()` does not do this itself.

    #[test]
    fn ensure_vec_embeddings_is_idempotent() {
        storage::init_sqlite_extensions();
        let svc = EntryService::for_test().unwrap();
        svc.ensure_vec_embeddings(4).unwrap();
        svc.ensure_vec_embeddings(4).unwrap();
        // Sanity: insert should work.
        let entry = svc
            .create(CreateEntry {
                title: "t".into(),
                blocks: vec![note_block("b")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        svc.write_embedding(entry.id, &[0.1, 0.2, 0.3, 0.4])
            .unwrap();
    }

    #[test]
    fn write_embedding_upserts() {
        storage::init_sqlite_extensions();
        let svc = EntryService::for_test().unwrap();
        svc.ensure_vec_embeddings(2).unwrap();
        let e = svc
            .create(CreateEntry {
                title: "t".into(),
                blocks: vec![note_block("b")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        svc.write_embedding(e.id, &[1.0, 0.0]).unwrap();
        // Second write replaces, not duplicates.
        svc.write_embedding(e.id, &[0.0, 1.0]).unwrap();

        let hits = svc.semantic_search(&[0.0, 1.0], 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry.id, e.id);
    }

    #[test]
    fn semantic_search_ranks_by_cosine_similarity() {
        storage::init_sqlite_extensions();
        let svc = EntryService::for_test().unwrap();
        svc.ensure_vec_embeddings(3).unwrap();

        let a = svc
            .create(CreateEntry {
                title: "a".into(),
                blocks: vec![note_block("near")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let b = svc
            .create(CreateEntry {
                title: "b".into(),
                blocks: vec![note_block("far")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        svc.write_embedding(a.id, &[1.0, 0.0, 0.0]).unwrap();
        svc.write_embedding(b.id, &[0.0, 0.0, 1.0]).unwrap();

        // Query close to A.
        let hits = svc.semantic_search(&[0.9, 0.1, 0.0], 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].entry.id, a.id, "a should rank first");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn semantic_search_returns_empty_when_no_embeddings() {
        storage::init_sqlite_extensions();
        let svc = EntryService::for_test().unwrap();
        svc.ensure_vec_embeddings(2).unwrap();
        let hits = svc.semantic_search(&[1.0, 0.0], 10).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn delete_embedding_removes_row() {
        storage::init_sqlite_extensions();
        let svc = EntryService::for_test().unwrap();
        svc.ensure_vec_embeddings(2).unwrap();

        let e = svc
            .create(CreateEntry {
                title: "t".into(),
                blocks: vec![note_block("b")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        svc.write_embedding(e.id, &[1.0, 0.0]).unwrap();

        // Precondition: search finds the row.
        let hits = svc.semantic_search(&[1.0, 0.0], 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry.id, e.id);

        // Delete, then search returns empty.
        svc.delete_embedding(e.id).unwrap();
        let hits = svc.semantic_search(&[1.0, 0.0], 10).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn create_emits_entry_created_event_with_full_snapshot() {
        use crate::event_model::ListEventsQuery;
        use crate::event_service::EventService;

        let entries = EntryService::for_test().unwrap();
        let events = EventService::for_test_shared_with_entries(&entries);
        let created = entries
            .create(CreateEntry {
                title: "Hello".into(),
                blocks: vec![BlockInput {
                    r#type: "note".into(),
                    text: "World".into(),
                    attrs: None,
                }],
                tags: Some(vec!["a".into()]),
                attrs: Some(serde_json::json!({"k": "v"})),
                source: Some("test".into()),
            })
            .unwrap();

        // Filter to entry.* events only — create() also emits block.created
        // events for each block, which we don't want to assert on here.
        let result = events
            .list(ListEventsQuery {
                type_: Some("entry.created".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.items.len(), 1);
        let event = &result.items[0];
        assert_eq!(event.type_, "entry.created");
        assert_eq!(event.target_type, "entry");
        assert_eq!(event.target_id, created.id);
        assert_eq!(event.payload["title"], "Hello");
        // Snapshot now includes blocks (not body).
        assert_eq!(event.payload["blocks"][0]["text"], "World");
        assert_eq!(event.payload["id"], created.id.to_string());
    }

    #[test]
    fn update_emits_entry_updated_event_with_after_snapshot() {
        use crate::event_model::ListEventsQuery;
        use crate::event_service::EventService;

        let entries = EntryService::for_test().unwrap();
        let events = EventService::for_test_shared_with_entries(&entries);
        let created = entries
            .create(CreateEntry {
                title: "orig".into(),
                blocks: vec![note_block("b")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();

        let _updated = entries
            .update(
                created.id,
                crate::UpdateEntry {
                    title: Some("new".into()),
                    tags: None,
                    attrs: None,
                    source: None,
                },
            )
            .unwrap();

        // Filter to entry.updated only (create also emitted entry.created + block.created).
        let result = events
            .list(ListEventsQuery {
                type_: Some("entry.updated".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.items.len(), 1);
        let event = &result.items[0];
        assert_eq!(event.type_, "entry.updated");
        assert_eq!(event.payload["title"], "new");
        // Blocks unchanged from created.
        assert_eq!(event.payload["blocks"][0]["text"], "b");
    }

    #[test]
    fn delete_emits_entry_deleted_event_with_before_snapshot() {
        use crate::event_model::ListEventsQuery;
        use crate::event_service::EventService;

        let entries = EntryService::for_test().unwrap();
        let events = EventService::for_test_shared_with_entries(&entries);
        let created = entries
            .create(CreateEntry {
                title: "to be deleted".into(),
                blocks: vec![note_block("body")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();

        entries.delete(created.id).unwrap();

        // Filter to entry.deleted only.
        let result = events
            .list(ListEventsQuery {
                type_: Some("entry.deleted".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.items.len(), 1);
        let event = &result.items[0];
        assert_eq!(event.type_, "entry.deleted");
        // Payload is the BEFORE snapshot (entry no longer exists in entries table,
        // but the event retains it for audit).
        assert_eq!(event.payload["title"], "to be deleted");
        assert_eq!(event.payload["blocks"][0]["text"], "body");
    }

    #[test]
    fn create_in_tx_works_within_external_transaction() {
        let svc = EntryService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        conn.execute_batch("BEGIN").unwrap();

        let entry = svc
            .create_in_tx(
                &conn,
                CreateEntry {
                    title: "batch test".into(),
                    blocks: vec![note_block("body")],
                    tags: None,
                    attrs: None,
                    source: None,
                },
            )
            .unwrap();

        conn.execute_batch("COMMIT").unwrap();
        drop(conn);

        // Verify entry persisted
        let fetched = svc.get(entry.id).unwrap();
        assert_eq!(fetched.title, "batch test");
    }

    #[test]
    fn create_in_tx_rolls_back_with_external_transaction() {
        let svc = EntryService::for_test().unwrap();
        let conn = svc.conn.lock().unwrap();
        conn.execute_batch("BEGIN").unwrap();

        let _ = svc
            .create_in_tx(
                &conn,
                CreateEntry {
                    title: "will rollback".into(),
                    blocks: vec![note_block("body")],
                    tags: None,
                    attrs: None,
                    source: None,
                },
            )
            .unwrap();

        conn.execute_batch("ROLLBACK").unwrap();
        drop(conn);

        // Entry should NOT exist after rollback
        let count: i64 = svc
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn delete_in_tx_captures_before_snapshot() {
        let svc = EntryService::for_test().unwrap();
        let created = svc
            .create(CreateEntry {
                title: "to delete".into(),
                blocks: vec![note_block("body")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();

        let conn = svc.conn.lock().unwrap();
        conn.execute_batch("BEGIN").unwrap();
        svc.delete_in_tx(&conn, created.id).unwrap();
        conn.execute_batch("COMMIT").unwrap();
        drop(conn);

        // Entry gone
        assert!(svc.get(created.id).is_err());
        // Event emitted
        let count: i64 = svc
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE type='entry.deleted'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn create_writes_nomai_file_to_content_store() {
        // Plan 3: create() writes .nomai file via ContentStore.
        let svc = EntryService::for_test().unwrap();
        let entry = svc
            .create(CreateEntry {
                title: "fs test".into(),
                blocks: vec![note_block("body text")],
                tags: Some(vec!["x".into()]),
                attrs: None,
                source: None,
            })
            .unwrap();

        let doc = svc.content_store.read_entry(entry.id).unwrap();
        assert_eq!(doc.title, "fs test");
        assert_eq!(doc.tags, vec!["x".to_string()]);
        assert_eq!(doc.blocks.len(), 1);
        assert_eq!(doc.blocks[0].text, "body text\n");
    }

    #[test]
    fn delete_removes_nomai_file_from_content_store() {
        let svc = EntryService::for_test().unwrap();
        let entry = svc
            .create(CreateEntry {
                title: "fs test".into(),
                blocks: vec![note_block("body text")],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        assert!(svc.content_store.read_entry(entry.id).is_ok());
        svc.delete(entry.id).unwrap();
        assert!(svc.content_store.read_entry(entry.id).is_err());
    }
}
