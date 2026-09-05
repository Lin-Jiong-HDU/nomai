//! block.* handlers: block-level RPCs on top of the
//! blocks storage. `block.append` adds a block to an existing entry
//! (computing the next ordinal) and re-renders the entry's `.nomai` file.
//!
//! The daemon accesses `BlockService` and `ContentStore` through the
//! EntryService accessors (`entries().block_service()`, `entries().content_store()`)
//! because both are co-owned by `EntryService` and share the SQLite connection.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::nomai_format::{Block as ParserBlock, BlockType, NomaiDoc};
use nomai_core::{CoreError, EntryService};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use nomai_protocol::method::block::APPEND as BLOCK_APPEND;
use nomai_protocol::method::block::DELETE as BLOCK_DELETE;
use nomai_protocol::method::block::GET as BLOCK_GET;
use nomai_protocol::method::block::LIST as BLOCK_LIST;
use nomai_protocol::method::block::UPDATE as BLOCK_UPDATE;

#[derive(serde::Deserialize, schemars::JsonSchema)]
pub struct AppendParams {
    #[schemars(schema_with = "crate::handlers::params::ulid_field_schema")]
    pub entry_id: ulid::Ulid,
    pub r#type: String,
    pub text: String,
    #[serde(default)]
    #[schemars(default)]
    pub attrs: Option<serde_json::Value>,
    /// `{filename: base64_string}` — decoded to bytes and written as sibling
    /// files before the block is appended.
    #[serde(default)]
    #[schemars(default)]
    pub attachments: Option<std::collections::HashMap<String, String>>,
}

pub struct Append;

#[async_trait]
impl RpcHandler for Append {
    fn method(&self) -> &'static str {
        BLOCK_APPEND
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Append a new block (type, text, optional attrs) to an entry. Computes the next ordinal and re-renders the entry's .nomai file. Invalidates search cache. Returns the created block."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(AppendParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: AppendParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries: Arc<EntryService> = daemon.entries.clone();
        let entry_id = p.entry_id;

        // Pre-validate attachments + src BEFORE appending the block.
        // `BlockService::append` commits its own transaction, so the
        // only way to keep `block.append` atomic (no block row left on a src-
        // validation failure) is to validate before the append. Order:
        // decode → write_attachments_and_validate → append → rerender → embed.
        //
        // The single-call form: always validate (decode to a map — empty when
        // no attachments). `@image` blocks must resolve src on disk even when
        // the client sends no attachments (e.g. the file was pre-placed or
        // written by a prior block); for non-image types the helper's src
        // check is a no-op, so the call is harmless.
        let decoded = crate::handlers::attachment::decode_attachments(
            p.attachments.clone().unwrap_or_default(),
            daemon.attachment_max_bytes,
        )?;
        let src = p
            .attrs
            .as_ref()
            .and_then(|a| a.get("src"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let block_sources = vec![(p.r#type.clone(), src)];
        {
            let entries = entries.clone();
            blocking(move || {
                entries.write_attachments_and_validate(entry_id, &block_sources, &decoded)
            })
            .await??;
        }

        let ty = p.r#type;
        let text = p.text;
        let attrs = p.attrs;
        let block = {
            let entries = entries.clone();
            blocking(move || entries.block_service().append(entry_id, ty, text, attrs)).await??
        };

        // Re-render the entry's .nomai (block list changed). Runs in the same
        // spawn_blocking pattern as the rest of this handler family.
        rerender_entry_nomai(&entries, entry_id).await?;

        // 0.2.3: embed the new block's chunk(s) so search.semantic works.
        crate::handlers::embed::embed_entry_chunks(daemon, entry_id, false).await?;

        // Invalidate search cache.
        daemon.search_cache.bump_generation();

        serde_json::to_value(&block).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
pub struct UpdateParams {
    #[schemars(schema_with = "crate::handlers::params::ulid_field_schema")]
    pub id: ulid::Ulid,
    #[serde(default)]
    #[schemars(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub text: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub attrs: Option<serde_json::Value>,
    /// `{filename: base64_string}` — decoded to bytes and written as sibling
    /// files before the block is updated.
    #[serde(default)]
    #[schemars(default)]
    pub attachments: Option<std::collections::HashMap<String, String>>,
}

pub struct Update;

#[async_trait]
impl RpcHandler for Update {
    fn method(&self) -> &'static str {
        BLOCK_UPDATE
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Update a block's type, text, or attrs by ULID. At least one of type/text/attrs must be present. Re-renders the entry's .nomai file. Invalidates search cache. Returns the updated block."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(UpdateParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: UpdateParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries: Arc<EntryService> = daemon.entries.clone();
        let id = p.id;

        // Fetch existing FIRST to compute the post-update type/src for
        // pre-validation. If the update doesn't change type/attrs, the
        // existing values are what must still validate.
        let existing = {
            let entries = entries.clone();
            blocking(move || entries.block_service().get(id)).await??
        };
        let post_type = p.r#type.clone().unwrap_or_else(|| existing.r#type.clone());
        let post_attrs = p.attrs.clone().unwrap_or_else(|| existing.attrs.clone());
        let post_src = post_attrs
            .get("src")
            .and_then(|v| v.as_str())
            .map(String::from);
        let entry_id = existing.entry_id;
        let text_changed = p
            .text
            .as_deref()
            .is_some_and(|text| text != existing.text.as_str());
        let old_chunk_ids = if p.text.is_some() {
            let chunks = daemon.chunks.clone();
            blocking(move || {
                Ok(chunks
                    .list(id)?
                    .items
                    .into_iter()
                    .map(|chunk| chunk.id)
                    .collect::<Vec<_>>())
            })
            .await??
        } else {
            Vec::new()
        };

        // Pre-validate (decode → write_attachments_and_validate) BEFORE
        // BlockService::update, so a validation failure leaves the block row
        // untouched. Mirrors Append::call. `BlockService::update` commits its
        // own tx; pre-validate is the only atomicity lever.
        let decoded = crate::handlers::attachment::decode_attachments(
            p.attachments.clone().unwrap_or_default(),
            daemon.attachment_max_bytes,
        )?;
        let block_sources = vec![(post_type, post_src)];
        {
            let entries = entries.clone();
            blocking(move || {
                entries.write_attachments_and_validate(entry_id, &block_sources, &decoded)
            })
            .await??;
        }

        let ty = p.r#type;
        let text = p.text;
        let attrs = p.attrs;
        let block = {
            let entries = entries.clone();
            blocking(move || entries.block_service().update(id, ty, text, attrs)).await??
        };

        // The DB mutation is committed. Invalidate immediately so every
        // subsequent error path leaves prior search results unreachable.
        daemon.search_cache.bump_generation();

        // A successful text change has replaced the old derived Chunk IDs.
        // Preserve the still-valid Block target while clearing only those
        // captured old Chunk references. Save cleanup failure so canonical
        // file/embedding finalization still runs. Metadata-only updates retain
        // IDs and have no signal cleanup.
        let signal_cleanup = if text_changed {
            let memory = daemon.memory.clone();
            blocking(move || memory.degrade_chunk_precision(&old_chunk_ids))
                .await
                .and_then(|result| result)
        } else {
            Ok(())
        };

        // Re-render the entry's .nomai (block text/type/attrs may have
        // changed). Runs in the same spawn_blocking pattern as Append.
        rerender_entry_nomai(&entries, block.entry_id).await?;

        // 0.2.3: re-embed (text may have changed → chunks re-derived →
        // chunks_ad cleaned old embeddings, new ones need embedding).
        crate::handlers::embed::embed_entry_chunks(daemon, block.entry_id, false).await?;

        // Canonical content is finalized. Surface any saved local-signal
        // cleanup error only after rerender and embedding succeeded.
        signal_cleanup?;

        serde_json::to_value(&block).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Delete;

#[async_trait]
impl RpcHandler for Delete {
    fn method(&self) -> &'static str {
        BLOCK_DELETE
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn description(&self) -> &'static str {
        "Delete a block by ULID. Re-renders the parent entry's .nomai file and invalidates the search cache. Returns {deleted: true, id}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::ulid_param_schema())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries: Arc<EntryService> = daemon.entries.clone();
        let id = p.id;
        let old_chunk_ids = {
            let chunks = daemon.chunks.clone();
            blocking(move || {
                Ok(chunks
                    .list(id)?
                    .items
                    .into_iter()
                    .map(|chunk| chunk.id)
                    .collect::<Vec<_>>())
            })
            .await??
        };
        let block = {
            let entries = entries.clone();
            blocking(move || entries.block_service().delete(id)).await??
        };

        // The DB mutation is committed. Invalidate before any fallible local
        // cleanup or canonical file finalization.
        daemon.search_cache.bump_generation();

        // The Block and its derived Chunks are gone, but the learned
        // association remains useful at Entry precision. Save cleanup failure
        // until the canonical .nomai file has still been finalized.
        let memory = daemon.memory.clone();
        let signal_cleanup = blocking(move || memory.degrade_block_precision(id, &old_chunk_ids))
            .await
            .and_then(|result| result);

        // Re-render the entry's .nomai (block list changed). Runs in the
        // same spawn_blocking pattern as Append/Update. The chunks_ad trigger
        // (V9) cleans vec_chunk_embeddings when CASCADE removes the block's
        // chunks; no manual loop needed here.
        rerender_entry_nomai(&entries, block.entry_id).await?;

        signal_cleanup?;

        Ok(json!({"deleted": true, "id": id.to_string()}))
    }
}

pub struct List;

#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        BLOCK_LIST
    }
    fn description(&self) -> &'static str {
        "List all blocks of an entry, in ordinal order. Returns {items, total}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": { "entry_id": crate::handlers::params::ulid_schema() },
            "required": ["entry_id"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            entry_id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries: Arc<EntryService> = daemon.entries.clone();
        let entry_id = p.entry_id;
        let include_benchmark = daemon
            .benchmark
            .as_ref()
            .is_some_and(|runtime| runtime.is_active());
        let result = {
            let entries = entries.clone();
            blocking(move || {
                if include_benchmark {
                    entries.block_service().list_with_benchmark(entry_id)
                } else {
                    entries.block_service().list(entry_id)
                }
            })
            .await??
        };

        Ok(json!({
            "items": result.items,
            "total": result.total,
        }))
    }
}

/// 0.2.2: fetch a single block by ULID. Namespace completeness — the other
/// four primitives (entry/link/chunk/events) all have `get`. Read-only: no
/// `.nomai` rerender, no search-cache bump.
pub struct Get;

#[async_trait]
impl RpcHandler for Get {
    fn method(&self) -> &'static str {
        BLOCK_GET
    }
    fn description(&self) -> &'static str {
        "Fetch a single block by ULID. Returns error 1001 if not found."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::ulid_param_schema())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            id: ulid::Ulid,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let entries: Arc<EntryService> = daemon.entries.clone();
        let id = p.id;
        let include_benchmark = daemon
            .benchmark
            .as_ref()
            .is_some_and(|runtime| runtime.is_active());
        let block = {
            let entries = entries.clone();
            blocking(move || {
                if include_benchmark {
                    entries.block_service().get_with_benchmark(id)
                } else {
                    entries.block_service().get(id)
                }
            })
            .await??
        };
        serde_json::to_value(&block).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

/// Load entry metadata + blocks via EntryService, render a `NomaiDoc`, and
/// atomically overwrite the entry's `.nomai` file via `ContentStore`.
///
/// Called after any block-level mutation (append, update, delete) so the
/// FS representation stays in sync with SQLite. Uses `EntryService::get`,
/// which already populates `entry.blocks`.
///
/// Also refreshes `entries.fs_mtime` to match the
/// newly-written file. Without this, the next `sync_from_fs` would see a
/// stale mtime and trigger a full reindex of the entry on every boot.
pub(crate) async fn rerender_entry_nomai(
    entries: &Arc<EntryService>,
    entry_id: ulid::Ulid,
) -> Result<(), CoreError> {
    let entries = entries.clone();
    blocking(move || -> Result<(), CoreError> {
        let entry = entries.get(entry_id)?;
        let parser_blocks: Vec<ParserBlock> = entry
            .blocks
            .iter()
            .map(|b| -> Result<ParserBlock, CoreError> {
                Ok(ParserBlock {
                    r#type: BlockType::from_str(&b.r#type).ok_or_else(|| {
                        CoreError::Validation(format!("unknown block type: {}", b.r#type))
                    })?,
                    text: format!("{}\n", b.text),
                    attrs: b.attrs.as_object().cloned().unwrap_or_default(),
                })
            })
            .collect::<Result<_, _>>()?;
        let doc = NomaiDoc {
            format_version: 1,
            id: entry.id,
            title: entry.title,
            tags: entry.tags,
            attrs: entry.attrs.as_object().cloned().unwrap_or_default(),
            source: entry.source,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            blocks: parser_blocks,
        };
        entries.content_store().write_entry(entry_id, &doc)?;
        // Refresh entries.fs_mtime to match the
        // newly-written .nomai file. Without this, the next sync_from_fs
        // sees a stale mtime and triggers a full reindex of the entry on
        // every daemon boot, undermining the trigger-based cleanup shipped.
        // We also bump entries.updated_at so the row
        // reflects the latest mutation.
        let new_mtime = entries
            .content_store()
            .entry_mtime(entry_id)
            .ok_or_else(|| CoreError::Storage(rusqlite::Error::ExecuteReturnedResults))?;
        let now = chrono::Utc::now().to_rfc3339();
        {
            let conn = entries.conn_for_test();
            let guard = conn.lock().unwrap();
            guard
                .execute(
                    "UPDATE entries SET fs_mtime = ?1, updated_at = ?2 WHERE id = ?3",
                    rusqlite::params![new_mtime.to_rfc3339(), &now, entry_id.to_string()],
                )
                .map_err(CoreError::Storage)?;
        }
        Ok(())
    })
    .await?
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params)
            .map_err(|errs| errs.map(|e| format!("{e}")).collect::<Vec<_>>())
    }

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    #[test]
    fn append_schema_accepts_valid() {
        let schema = Append.input_schema().unwrap();
        let valid = json!({
            "entry_id": ULID,
            "type": "note",
            "text": "hello"
        });
        assert!(validate(&schema, &valid).is_ok());
    }

    #[test]
    fn append_schema_rejects_missing_entry_id() {
        let schema = Append.input_schema().unwrap();
        let invalid = json!({"type": "note", "text": "hi"});
        assert!(validate(&schema, &invalid).is_err());
    }

    #[test]
    fn append_schema_rejects_missing_type() {
        let schema = Append.input_schema().unwrap();
        let invalid = json!({"entry_id": ULID, "text": "hi"});
        assert!(validate(&schema, &invalid).is_err());
    }

    #[test]
    fn append_schema_rejects_missing_text() {
        let schema = Append.input_schema().unwrap();
        let invalid = json!({"entry_id": ULID, "type": "note"});
        assert!(validate(&schema, &invalid).is_err());
    }

    #[test]
    fn update_schema_accepts_only_id() {
        let schema = Update.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn update_schema_rejects_missing_id() {
        let schema = Update.input_schema().unwrap();
        assert!(validate(&schema, &json!({"text": "x"})).is_err());
    }

    #[test]
    fn delete_schema_accepts_valid_id() {
        let schema = Delete.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn delete_schema_rejects_missing_id() {
        let schema = Delete.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn list_schema_accepts_entry_id() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({"entry_id": ULID})).is_ok());
    }

    #[test]
    fn list_schema_rejects_missing_entry_id() {
        let schema = List.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn get_schema_accepts_valid_id() {
        let schema = Get.input_schema().unwrap();
        assert!(validate(&schema, &json!({"id": ULID})).is_ok());
    }

    #[test]
    fn get_schema_rejects_missing_id() {
        let schema = Get.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use nomai_core::{
        BlockInput, CreateEntry, CreateSearchSession, EntryService, FeedbackTarget, MemoryPolicy,
        SearchResultTarget,
    };
    use nomai_protocol::{ProviderError, ProviderErrorKind};
    use nomai_providers::{CompletionRequest, CompletionResponse, EmbeddingProvider, LlmProvider};

    use super::*;
    use crate::daemon::DaemonBuilder;

    struct FakeEmbed;

    #[async_trait]
    impl EmbeddingProvider for FakeEmbed {
        async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
            Ok(vec![vec![1.0, 0.0, 0.0, 0.0]; texts.len()])
        }

        fn dim(&self) -> usize {
            4
        }

        fn name(&self) -> &str {
            "fake-embed"
        }
    }

    struct FakeLlm;

    #[async_trait]
    impl LlmProvider for FakeLlm {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            Err(ProviderError::new(
                ProviderErrorKind::Unknown,
                "fake llm",
                None,
            ))
        }

        fn name(&self) -> &str {
            "fake-llm"
        }
    }

    fn daemon() -> Daemon {
        let entries = Arc::new(EntryService::for_test().unwrap());
        DaemonBuilder::new()
            .conn(entries.conn_for_test())
            .content_store(entries.content_store().clone())
            .embedder(Arc::new(FakeEmbed))
            .llm(Arc::new(FakeLlm))
            .embedding_dim(4)
            .chunk_target_size(1024)
            .cache_model("active-model")
            .warn_rows(100_000)
            .memory_policy(MemoryPolicy::default())
            .build()
            .unwrap()
    }

    fn seed_precise_feedback(daemon: &Daemon) -> (ulid::Ulid, ulid::Ulid, ulid::Ulid, ulid::Ulid) {
        let entry = daemon
            .entries
            .create(CreateEntry {
                title: "block lifecycle target".into(),
                blocks: vec![BlockInput {
                    r#type: "note".into(),
                    text: "old block text".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
                attachments: None,
            })
            .unwrap();
        let block_id = entry.blocks[0].id;
        let chunk_id = daemon.chunks.list(block_id).unwrap().items[0].id;
        let search_id = daemon
            .memory
            .create_search_session(CreateSearchSession {
                raw_query_text: "raw block query".into(),
                effective_query_text: "effective block query".into(),
                query_embedding: vec![1.0, 0.0, 0.0, 0.0],
                embedding_model: "active-model".into(),
                results: vec![SearchResultTarget {
                    entry_id: entry.id,
                    matched_block_id: Some(block_id),
                    matched_chunk_id: Some(chunk_id),
                    result_rank: 1,
                }],
            })
            .unwrap();
        daemon
            .memory
            .apply_feedback(
                search_id,
                &[FeedbackTarget {
                    entry_id: entry.id,
                    block_id: Some(block_id),
                    chunk_id: Some(chunk_id),
                }],
            )
            .unwrap();
        (entry.id, block_id, chunk_id, search_id)
    }

    type MaterializedTargets = (
        (Option<String>, Option<String>),
        (Option<String>, Option<String>),
    );

    fn materialized_targets(
        daemon: &Daemon,
        entry_id: ulid::Ulid,
        search_id: ulid::Ulid,
    ) -> MaterializedTargets {
        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let affinity = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities WHERE entry_id = ?1",
                [entry_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let session = conn
            .query_row(
                "SELECT matched_block_id, matched_chunk_id
                 FROM search_session_results WHERE search_id = ?1 AND entry_id = ?2",
                rusqlite::params![search_id.to_string(), entry_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        (affinity, session)
    }

    #[tokio::test]
    async fn delete_degrades_block_and_chunk_precision_to_entry() {
        let daemon = daemon();
        let (entry_id, block_id, _chunk_id, search_id) = seed_precise_feedback(&daemon);

        Delete.call(&daemon, json!({"id": block_id})).await.unwrap();

        assert_eq!(
            materialized_targets(&daemon, entry_id, search_id),
            ((None, None), (None, None))
        );
        let conn = daemon.entries.conn_for_test();
        assert_eq!(
            conn.lock()
                .unwrap()
                .query_row(
                    "SELECT reinforcement_count FROM query_affinities WHERE entry_id = ?1",
                    [entry_id.to_string()],
                    |row| row.get::<_, u8>(0),
                )
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn delete_clears_semantic_only_session_chunk_precision() {
        let daemon = daemon();
        let (entry_id, block_id, chunk_id, search_id) = seed_precise_feedback(&daemon);
        daemon
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .execute(
                "UPDATE search_session_results SET matched_block_id = NULL
                 WHERE search_id = ?1 AND entry_id = ?2",
                rusqlite::params![search_id.to_string(), entry_id.to_string()],
            )
            .unwrap();
        assert_eq!(
            materialized_targets(&daemon, entry_id, search_id).1,
            (None, Some(chunk_id.to_string()))
        );

        Delete.call(&daemon, json!({"id": block_id})).await.unwrap();

        assert_eq!(
            materialized_targets(&daemon, entry_id, search_id),
            ((None, None), (None, None))
        );
    }

    #[tokio::test]
    async fn text_update_degrades_only_old_chunk_precision() {
        let daemon = daemon();
        let (entry_id, block_id, old_chunk_id, search_id) = seed_precise_feedback(&daemon);

        Update
            .call(
                &daemon,
                json!({"id": block_id, "text": "new block text with replacement chunk"}),
            )
            .await
            .unwrap();

        let new_chunk_id = daemon.chunks.list(block_id).unwrap().items[0].id;
        assert_ne!(new_chunk_id, old_chunk_id);
        let expected_block = Some(block_id.to_string());
        assert_eq!(
            materialized_targets(&daemon, entry_id, search_id),
            ((expected_block.clone(), None), (expected_block, None),)
        );
    }

    #[tokio::test]
    async fn update_cleanup_failure_still_finalizes_nomai_mtime_embedding_and_cache() {
        let daemon = daemon();
        let (entry_id, block_id, _old_chunk_id, _search_id) = seed_precise_feedback(&daemon);
        let conn = daemon.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(&format!(
                "UPDATE entries SET fs_mtime = '2000-01-01T00:00:00Z'
                 WHERE id = '{entry_id}';
                 CREATE TEMP TRIGGER fail_update_signal_cleanup
                 BEFORE UPDATE OF matched_chunk_id ON search_session_results
                 WHEN OLD.entry_id = '{entry_id}'
                 BEGIN SELECT RAISE(ABORT, 'forced update signal cleanup failure'); END;"
            ))
            .unwrap();
        let generation = daemon.search_cache.generation();

        let error = Update
            .call(
                &daemon,
                json!({"id": block_id, "text": "finalized replacement text"}),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, CoreError::Storage(_)));
        assert_eq!(daemon.search_cache.generation(), generation + 1);
        let doc = daemon.entries.content_store().read_entry(entry_id).unwrap();
        assert_eq!(doc.blocks[0].text.trim_end(), "finalized replacement text");
        let new_chunk_id = daemon.chunks.list(block_id).unwrap().items[0].id;
        let conn = conn.lock().unwrap();
        let indexed_mtime = conn
            .query_row(
                "SELECT fs_mtime FROM entries WHERE id = ?1",
                [entry_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_ne!(indexed_mtime, "2000-01-01T00:00:00Z");
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM vec_chunk_embeddings WHERE chunk_id = ?1",
                [new_chunk_id.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn delete_cleanup_failure_still_finalizes_nomai_mtime_and_cache() {
        let daemon = daemon();
        let (entry_id, block_id, _chunk_id, _search_id) = seed_precise_feedback(&daemon);
        let conn = daemon.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(&format!(
                "UPDATE entries SET fs_mtime = '2000-01-01T00:00:00Z'
                 WHERE id = '{entry_id}';
                 CREATE TEMP TRIGGER fail_delete_signal_cleanup
                 BEFORE UPDATE ON search_session_results
                 WHEN OLD.entry_id = '{entry_id}'
                 BEGIN SELECT RAISE(ABORT, 'forced delete signal cleanup failure'); END;"
            ))
            .unwrap();
        let generation = daemon.search_cache.generation();

        let error = Delete
            .call(&daemon, json!({"id": block_id}))
            .await
            .unwrap_err();

        assert!(matches!(error, CoreError::Storage(_)));
        assert_eq!(daemon.search_cache.generation(), generation + 1);
        let doc = daemon.entries.content_store().read_entry(entry_id).unwrap();
        assert!(doc.blocks.is_empty());
        let indexed_mtime = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT fs_mtime FROM entries WHERE id = ?1",
                [entry_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_ne!(indexed_mtime, "2000-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn metadata_only_update_retains_valid_chunk_precision() {
        let daemon = daemon();
        let (entry_id, block_id, chunk_id, search_id) = seed_precise_feedback(&daemon);

        Update
            .call(
                &daemon,
                json!({"id": block_id, "attrs": {"reviewed": true}}),
            )
            .await
            .unwrap();

        let expected = (Some(block_id.to_string()), Some(chunk_id.to_string()));
        assert_eq!(
            materialized_targets(&daemon, entry_id, search_id),
            (expected.clone(), expected)
        );
        assert_eq!(daemon.chunks.list(block_id).unwrap().items[0].id, chunk_id);
    }

    #[tokio::test]
    async fn equal_text_update_retains_precise_chunk_ids() {
        let daemon = daemon();
        let (entry_id, block_id, chunk_id, search_id) = seed_precise_feedback(&daemon);

        Update
            .call(&daemon, json!({"id": block_id, "text": "old block text"}))
            .await
            .unwrap();

        let expected = (Some(block_id.to_string()), Some(chunk_id.to_string()));
        assert_eq!(
            materialized_targets(&daemon, entry_id, search_id),
            (expected.clone(), expected)
        );
        assert_eq!(daemon.chunks.list(block_id).unwrap().items[0].id, chunk_id);
    }
}
