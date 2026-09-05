//! index.* handlers: index-reconciliation RPCs that treat
//! the filesystem as source-of-truth and bring the SQLite index
//! into agreement with it.
//!
//! `index.sync` walks the content store, diffs each entry's `.nomai` mtime
//! against the indexed `fs_mtime`, and reconciles via
//! `EntryService::reindex_one` / direct DELETE. Returns per-bucket counts.
//!
//! `index.rebuild` is the nuclear option: wipes every derived table, then
//! re-indexes every FS entry. Used to recover from index corruption.
//!
//! `index.verify` is a read-only drift report: same scan/diff
//! as `index.sync` but never mutates. Useful for surfacing drift to the user
//! before deciding whether to run sync/rebuild.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use nomai_core::{CoreError, EntryService, RebuildResult, SyncResult, VerifyResult};

use crate::daemon::Daemon;
use crate::handlers::embed::{embed_entry_chunks, reembed_query_affinities};
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use nomai_protocol::method::index::{
    REBUILD as INDEX_REBUILD, SYNC as INDEX_SYNC, VERIFY as INDEX_VERIFY,
};

pub struct Sync;

#[async_trait]
impl RpcHandler for Sync {
    fn method(&self) -> &'static str {
        INDEX_SYNC
    }
    fn description(&self) -> &'static str {
        "Reconcile the SQLite index with the filesystem source-of-truth. Walks every entry's .nomai file, diffing mtime; adds/updates/removes rows as needed. Returns per-bucket counts."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Clone the Arc before spawning so the closure is 'static. The sync
        // pass takes per-entry locks internally; we don't hold any lock here.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let result: SyncResult = { blocking(move || entries.sync_from_fs()).await?? };

        // Invalidate immediately after the committed Core mutation. Signal
        // reconciliation and provider work are fallible post-commit phases and
        // must never leave the old generation reachable.
        if result.added + result.updated + result.removed > 0 {
            daemon.search_cache.bump_generation();
        }

        // Signal references are long-lived local state, so reconcile only
        // after sync_from_fs has finished every temporary delete/reinsert and
        // final orphan sweep. Normal sync deliberately does not re-embed
        // query affinities: the active model/dimension have not changed.
        let memory = daemon.memory.clone();
        let reconcile_error = match blocking(move || memory.reconcile_content_references()).await {
            Ok(Ok(_)) => None,
            Ok(Err(error)) | Err(error) => Some(error),
        };

        // reindex_one (run for added/updated entries) clears their
        // vec_chunk_embeddings via CASCADE but does not re-embed — without
        // this, an entry that was previously searchable goes silent after a
        // sync, and newly-synced entries never become searchable (sibling of
        // issue #1). emb_cache keeps this near-zero API cost for unchanged
        // bodies. Mirrors entry.create: an embed failure surfaces as a
        // provider error without rolling back the already-committed reindex.
        for id in &result.reindexed_ids {
            embed_entry_chunks(daemon, *id, false).await?;
        }

        if let Some(error) = reconcile_error {
            return Err(error);
        }

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Rebuild;

#[async_trait]
impl RpcHandler for Rebuild {
    fn method(&self) -> &'static str {
        INDEX_REBUILD
    }
    fn description(&self) -> &'static str {
        "Wipe every derived table and re-index every entry from the filesystem. Use to recover from index corruption; heavier than index.sync."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Clone the Arc before spawning so the closure is 'static. The
        // rebuild takes per-entry locks internally during the reindex phase.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let mut result: RebuildResult = { blocking(move || entries.rebuild_index()).await?? };
        let content_rebuild_complete = result.errors.is_empty();

        // Rebuild always invalidates immediately — even an empty or partial
        // rebuild has wiped/replaced derived rows before post-commit cleanup.
        daemon.search_cache.bump_generation();

        // Reconcile only when every filesystem Entry was reinserted. A partial
        // rebuild leaves failed-but-present .nomai Entries absent from the
        // derived index; treating that temporary absence as deletion would
        // destroy their retryable adaptive state.
        if content_rebuild_complete {
            let memory = daemon.memory.clone();
            match blocking(move || memory.reconcile_content_references()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) | Err(error) => {
                    result
                        .errors
                        .push(format!("memory reconciliation: {error}"));
                }
            }
        }

        // reindex_one (run per entry by rebuild_index) re-derives chunks + FTS
        // but does NOT re-embed — rebuild_index phase 1 wiped
        // vec_chunk_embeddings, so semantic search would return empty without
        // this pass (issue #1). emb_cache (intentionally untouched by rebuild)
        // makes this near-zero API cost for unchanged bodies; only genuinely
        // new chunk texts hit the provider. Embed failures are collected into
        // `errors` (best-effort, mirroring rebuild_index's own handling) so
        // one bad entry doesn't abort the rest of the KB.
        let entry_ids = {
            let entries = daemon.entries.clone();
            blocking(move || entries.list_ids()).await??
        };
        for id in entry_ids {
            if let Err(e) = embed_entry_chunks(daemon, id, false).await {
                result.errors.push(format!("re-embed entry {id}: {e}"));
            }
        }

        // Query associations move to the active provider only after a complete
        // Core rebuild and all content chunk embedding attempts. A partial
        // rebuild keeps ordinary rows and vectors exactly as retry state.
        if content_rebuild_complete && let Err(error) = reembed_query_affinities(daemon).await {
            result.errors.push(format!("query affinities: {error}"));
        }

        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

pub struct Verify;

#[async_trait]
impl RpcHandler for Verify {
    fn method(&self) -> &'static str {
        INDEX_VERIFY
    }
    fn description(&self) -> &'static str {
        "Read-only drift report between the filesystem and the SQLite index. Same scan as index.sync but never mutates; use to preview drift before deciding to sync or rebuild."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Read-only drift report. verify_fs snapshots the index under one
        // short lock and then walks the FS without taking any lock; safe to
        // run on a live daemon.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let result: VerifyResult = { blocking(move || entries.verify_fs()).await?? };
        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params)
            .map_err(|errs| errs.map(|e| format!("{e}")).collect::<Vec<_>>())
    }

    #[test]
    fn sync_schema_accepts_empty_object() {
        let schema = Sync.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({})).is_ok());
    }

    #[test]
    fn sync_schema_rejects_extra_props() {
        let schema = Sync.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({"foo": 1})).is_err());
    }

    #[test]
    fn rebuild_schema_accepts_empty_object() {
        let schema = Rebuild.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({})).is_ok());
    }

    #[test]
    fn rebuild_schema_rejects_extra_props() {
        let schema = Rebuild.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({"foo": 1})).is_err());
    }

    #[test]
    fn verify_schema_accepts_empty_object() {
        let schema = Verify.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({})).is_ok());
    }

    #[test]
    fn verify_schema_rejects_extra_props() {
        let schema = Verify.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({"foo": 1})).is_err());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use nomai_core::{
        BlockInput, CreateEntry, CreateSearchSession, Entry, FeedbackTarget, MemoryPolicy,
        SearchResultTarget,
    };
    use nomai_protocol::{ProviderError, ProviderErrorKind};
    use nomai_providers::{CompletionRequest, CompletionResponse, EmbeddingProvider, LlmProvider};

    use super::*;
    use crate::daemon::DaemonBuilder;

    #[derive(Default)]
    struct FakeEmbed {
        calls: Mutex<Vec<Vec<String>>>,
        fail_on: Mutex<Option<String>>,
    }

    impl FakeEmbed {
        fn fail_on(&self, text: &str) {
            *self.fail_on.lock().unwrap() = Some(text.into());
        }

        fn clear_calls(&self) {
            self.calls.lock().unwrap().clear();
        }

        fn observed_text_count(&self, expected: &str) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .filter(|text| text.as_str() == expected)
                .count()
        }
    }

    #[async_trait]
    impl EmbeddingProvider for FakeEmbed {
        async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
            self.calls
                .lock()
                .unwrap()
                .push(texts.iter().map(|text| (*text).to_string()).collect());
            if self
                .fail_on
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|needle| texts.iter().any(|text| text.contains(needle.as_str())))
            {
                return Err(ProviderError::new(
                    ProviderErrorKind::Network,
                    "forced affinity embedding failure",
                    None,
                ));
            }
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

    fn daemon() -> (Daemon, Arc<FakeEmbed>) {
        let entries = Arc::new(EntryService::for_test().unwrap());
        let embedder = Arc::new(FakeEmbed::default());
        let daemon = DaemonBuilder::new()
            .conn(entries.conn_for_test())
            .content_store(entries.content_store().clone())
            .embedder(embedder.clone())
            .llm(Arc::new(FakeLlm))
            .embedding_dim(4)
            .chunk_target_size(1024)
            .cache_model("active-model")
            .warn_rows(100_000)
            .memory_policy(MemoryPolicy::default())
            .build()
            .unwrap();
        (daemon, embedder)
    }

    fn seed_precise_feedback(
        daemon: &Daemon,
        embedding_model: &str,
        effective_query_text: &str,
    ) -> (Entry, ulid::Ulid, ulid::Ulid) {
        let entry = daemon
            .entries
            .create(CreateEntry {
                title: "index lifecycle target".into(),
                blocks: vec![BlockInput {
                    r#type: "note".into(),
                    text: "index lifecycle body".into(),
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
                raw_query_text: format!("raw {effective_query_text}"),
                effective_query_text: effective_query_text.into(),
                query_embedding: vec![1.0, 0.0, 0.0, 0.0],
                embedding_model: embedding_model.into(),
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
        (entry, block_id, chunk_id)
    }

    fn entry_signal_counts(daemon: &Daemon, entry_id: ulid::Ulid) -> (i64, i64, i64, i64) {
        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        conn.query_row(
            "SELECT
                (SELECT COUNT(*) FROM entry_memory_stats WHERE entry_id = ?1),
                (SELECT COUNT(*) FROM query_affinities WHERE entry_id = ?1),
                (SELECT COUNT(*) FROM search_feedback WHERE entry_id = ?1),
                (SELECT COUNT(*) FROM search_session_results WHERE entry_id = ?1)",
            [entry_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sync_removes_signals_for_an_entry_removed_from_the_filesystem() {
        let (daemon, _embedder) = daemon();
        let (entry, _block_id, _chunk_id) =
            seed_precise_feedback(&daemon, "active-model", "removed entry query");
        daemon
            .entries
            .content_store()
            .delete_entry(entry.id)
            .unwrap();

        Sync.call(&daemon, serde_json::json!({})).await.unwrap();

        assert_eq!(entry_signal_counts(&daemon, entry.id), (0, 0, 0, 0));
        let conn = daemon.entries.conn_for_test();
        assert_eq!(
            conn.lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM vec_query_affinities", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn normal_sync_reconciles_after_reindex_without_reembedding_affinity_queries() {
        let (daemon, embedder) = daemon();
        let (entry, old_block_id, old_chunk_id) =
            seed_precise_feedback(&daemon, "active-model", "normal sync affinity query");
        {
            let conn = daemon.entries.conn_for_test();
            conn.lock()
                .unwrap()
                .execute(
                    "UPDATE entries SET fs_mtime = '2000-01-01T00:00:00Z' WHERE id = ?1",
                    [entry.id.to_string()],
                )
                .unwrap();
        }
        embedder.clear_calls();

        let result = Sync.call(&daemon, serde_json::json!({})).await.unwrap();

        assert_eq!(result["updated"], 1);
        assert_eq!(
            embedder.observed_text_count("normal sync affinity query"),
            0,
            "normal sync must retain compatible query vectors without provider work"
        );
        assert_eq!(entry_signal_counts(&daemon, entry.id), (1, 1, 1, 1));
        let current_block_id = daemon.entries.get(entry.id).unwrap().blocks[0].id;
        assert_ne!(current_block_id, old_block_id);
        assert!(daemon.chunks.get(old_chunk_id).is_err());
        let conn = daemon.entries.conn_for_test();
        let targets = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities WHERE entry_id = ?1",
                [entry.id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(targets, (None, None));
    }

    #[tokio::test]
    async fn sync_reconcile_failure_invalidates_cache_and_still_reembeds_changed_chunks() {
        let (daemon, embedder) = daemon();
        let (entry, _old_block_id, _old_chunk_id) =
            seed_precise_feedback(&daemon, "active-model", "sync reconcile failure query");
        let conn = daemon.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(&format!(
                "UPDATE entries SET fs_mtime = '2000-01-01T00:00:00Z'
                 WHERE id = '{}';
                 CREATE TEMP TRIGGER fail_sync_signal_reconcile
                 BEFORE UPDATE OF block_id ON query_affinities
                 WHEN OLD.entry_id = '{}'
                 BEGIN SELECT RAISE(ABORT, 'forced sync reconcile failure'); END;",
                entry.id, entry.id
            ))
            .unwrap();
        embedder.clear_calls();
        let generation = daemon.search_cache.generation();

        let error = Sync.call(&daemon, serde_json::json!({})).await.unwrap_err();

        assert!(matches!(error, CoreError::Storage(_)));
        assert!(error.to_string().contains("forced sync reconcile failure"));
        assert_eq!(daemon.search_cache.generation(), generation + 1);
        let current_chunk_id = daemon
            .chunks
            .list(daemon.entries.get(entry.id).unwrap().blocks[0].id)
            .unwrap()
            .items[0]
            .id;
        assert_eq!(
            conn.lock()
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM vec_chunk_embeddings WHERE chunk_id = ?1",
                    [current_chunk_id.to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(embedder.observed_text_count("index lifecycle body"), 1);
    }

    #[tokio::test]
    async fn sync_embed_error_takes_priority_over_saved_reconcile_error() {
        let (daemon, embedder) = daemon();
        let (entry, _old_block_id, _old_chunk_id) =
            seed_precise_feedback(&daemon, "active-model", "sync error priority query");
        let conn = daemon.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(&format!(
                "UPDATE entries SET fs_mtime = '2000-01-01T00:00:00Z'
                 WHERE id = '{}';
                 CREATE TEMP TRIGGER fail_sync_priority_reconcile
                 BEFORE UPDATE OF block_id ON query_affinities
                 WHEN OLD.entry_id = '{}'
                 BEGIN SELECT RAISE(ABORT, 'lower-priority reconcile failure'); END;",
                entry.id, entry.id
            ))
            .unwrap();
        embedder.fail_on("index lifecycle body");

        let error = Sync.call(&daemon, serde_json::json!({})).await.unwrap_err();

        assert!(matches!(error, CoreError::Provider(_)));
        assert!(
            error
                .to_string()
                .contains("forced affinity embedding failure")
        );
        assert!(!error.to_string().contains("lower-priority"));
    }

    #[tokio::test]
    async fn rebuild_preserves_entry_signal_strength_and_reembeds_degraded_affinity() {
        let (daemon, _embedder) = daemon();
        let (entry, old_block_id, old_chunk_id) =
            seed_precise_feedback(&daemon, "old-model", "retained affinity query");
        let before = {
            let conn = daemon.entries.conn_for_test();
            let conn = conn.lock().unwrap();
            conn.query_row(
                "SELECT reinforcement_count, last_reinforced_at
                 FROM entry_memory_stats WHERE entry_id = ?1",
                [entry.id.to_string()],
                |row| Ok((row.get::<_, u8>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap()
        };

        let result = Rebuild.call(&daemon, serde_json::json!({})).await.unwrap();
        assert_eq!(result["errors"], serde_json::json!([]));

        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let after = conn
            .query_row(
                "SELECT reinforcement_count, last_reinforced_at
                 FROM entry_memory_stats WHERE entry_id = ?1",
                [entry.id.to_string()],
                |row| Ok((row.get::<_, u8>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap();
        assert_eq!(after, before);
        let affinity = conn
            .query_row(
                "SELECT embedding_model, embedding_dim, block_id, chunk_id
                 FROM query_affinities WHERE entry_id = ?1",
                [entry.id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(affinity, ("active-model".into(), 4, None, None));
        drop(conn);
        assert_ne!(
            old_block_id,
            daemon.entries.get(entry.id).unwrap().blocks[0].id
        );
        assert!(daemon.chunks.get(old_chunk_id).is_err());

        let hits = daemon
            .memory
            .affinity_candidates(&[1.0, 0.0, 0.0, 0.0], "active-model", 5, None, None, false)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry_id, entry.id);
        assert_eq!(hits[0].block_id, None);
        assert_eq!(hits[0].chunk_id, None);
    }

    #[tokio::test]
    async fn partial_rebuild_preserves_failed_entry_signals_and_skips_affinity_reembedding() {
        let (daemon, embedder) = daemon();
        let (failed_entry, old_block_id, old_chunk_id) =
            seed_precise_feedback(&daemon, "old-model", "partial rebuild affinity query");
        daemon
            .entries
            .create(CreateEntry {
                title: "healthy rebuild entry".into(),
                blocks: vec![BlockInput {
                    r#type: "note".into(),
                    text: "healthy rebuild body".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
                attachments: None,
            })
            .unwrap();
        std::fs::write(
            daemon.entries.content_store().entry_file(failed_entry.id),
            "not a valid nomai document",
        )
        .unwrap();
        embedder.clear_calls();

        let result = Rebuild.call(&daemon, serde_json::json!({})).await.unwrap();

        let errors = result["errors"].as_array().unwrap();
        assert!(errors.iter().any(|error| {
            error
                .as_str()
                .is_some_and(|message| message.starts_with(&format!("entry {}:", failed_entry.id)))
        }));
        assert!(!errors.iter().any(|error| {
            error
                .as_str()
                .is_some_and(|message| message.starts_with("query affinities:"))
        }));
        assert_eq!(
            embedder.observed_text_count("partial rebuild affinity query"),
            0
        );
        assert_eq!(
            entry_signal_counts(&daemon, failed_entry.id),
            (1, 1, 1, 1),
            "failed-but-present filesystem Entries retain retryable signal state"
        );
        let conn = daemon.entries.conn_for_test();
        let stored = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT embedding_model, block_id, chunk_id,
                        (SELECT COUNT(*) FROM vec_query_affinities
                         WHERE affinity_id = query_affinities.id)
                 FROM query_affinities WHERE entry_id = ?1",
                [failed_entry.id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            stored,
            (
                "old-model".into(),
                Some(old_block_id.to_string()),
                Some(old_chunk_id.to_string()),
                1,
            )
        );
    }

    #[tokio::test]
    async fn rebuild_reports_affinity_provider_failure_without_changing_ordinary_rows() {
        let (daemon, embedder) = daemon();
        let (entry, _block_id, _chunk_id) =
            seed_precise_feedback(&daemon, "old-model", "provider failure affinity query");
        embedder.fail_on("provider failure affinity query");

        let result = Rebuild.call(&daemon, serde_json::json!({})).await.unwrap();

        assert!(result["errors"].as_array().unwrap().iter().any(|error| {
            error.as_str().is_some_and(|message| {
                message.starts_with("query affinities: ")
                    && message.contains("forced affinity embedding failure")
            })
        }));
        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT embedding_model, reinforcement_count, raw_query_text,
                        effective_query_text
                 FROM query_affinities WHERE entry_id = ?1",
                [entry.id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u8>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            row,
            (
                "old-model".into(),
                1,
                "raw provider failure affinity query".into(),
                "provider failure affinity query".into(),
            )
        );
    }

    #[tokio::test]
    async fn rebuild_collects_reconcile_and_affinity_errors_after_cache_and_chunk_finalization() {
        let (daemon, embedder) = daemon();
        let (entry, _old_block_id, _old_chunk_id) = seed_precise_feedback(
            &daemon,
            "old-model",
            "forced rebuild affinity failure query",
        );
        let conn = daemon.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(&format!(
                "CREATE TEMP TRIGGER fail_rebuild_signal_reconcile
                 BEFORE UPDATE OF block_id ON query_affinities
                 WHEN OLD.entry_id = '{}'
                 BEGIN SELECT RAISE(ABORT, 'forced rebuild reconcile failure'); END;",
                entry.id
            ))
            .unwrap();
        embedder.clear_calls();
        embedder.fail_on("forced rebuild affinity failure query");
        let generation = daemon.search_cache.generation();

        let result = Rebuild.call(&daemon, serde_json::json!({})).await.unwrap();

        let errors = result["errors"].as_array().unwrap();
        assert!(errors.iter().any(|error| {
            error.as_str().is_some_and(|message| {
                message.starts_with("memory reconciliation: ")
                    && message.contains("forced rebuild reconcile failure")
            })
        }));
        assert!(errors.iter().any(|error| {
            error.as_str().is_some_and(|message| {
                message.starts_with("query affinities: ")
                    && message.contains("forced affinity embedding failure")
            })
        }));
        assert_eq!(daemon.search_cache.generation(), generation + 1);
        let current_chunk_id = daemon
            .chunks
            .list(daemon.entries.get(entry.id).unwrap().blocks[0].id)
            .unwrap()
            .items[0]
            .id;
        let counts: (i64, i64) = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM vec_chunk_embeddings WHERE chunk_id = ?1),
                    (SELECT COUNT(*) FROM vec_query_affinities)",
                [current_chunk_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 1));
        assert_eq!(embedder.observed_text_count("index lifecycle body"), 1);
    }

    #[tokio::test]
    async fn rebuild_deduplicates_identical_effective_text_before_provider_call() {
        let (daemon, embedder) = daemon();
        let (first, _block_id, _chunk_id) =
            seed_precise_feedback(&daemon, "old-model-a", "shared affinity query");
        let (second, _block_id, _chunk_id) =
            seed_precise_feedback(&daemon, "old-model-b", "shared affinity query");
        assert_ne!(first.id, second.id);
        embedder.clear_calls();

        let result = Rebuild.call(&daemon, serde_json::json!({})).await.unwrap();

        assert_eq!(result["errors"], serde_json::json!([]));
        assert_eq!(
            embedder.observed_text_count("shared affinity query"),
            1,
            "the provider should receive each effective affinity text once"
        );
        let conn = daemon.entries.conn_for_test();
        assert_eq!(
            conn.lock()
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM query_affinities
                     WHERE embedding_model = 'active-model' AND embedding_dim = 4",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2,
            "deduplicated provider work still maps a vector to each survivor"
        );
    }
}
