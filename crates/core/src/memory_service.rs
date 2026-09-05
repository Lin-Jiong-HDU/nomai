use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use ulid::Ulid;

use crate::chunk_model::DimReconciliation;
use crate::error::CoreError;
use crate::memory_model::{
    AffinityEmbeddingInput, AffinityEmbeddingPlan, AffinityHit, AppliedFeedback, Clock,
    CreateSearchSession, EntryMemorySignal, FeedbackResult, FeedbackTarget, MemoryPolicy,
};
use crate::service::BENCHMARK_ENTRY_PREDICATE;
use crate::storage;

const MAX_PERSISTED_REINFORCEMENTS: u8 = 3;

/// Local SQLite persistence and live lookup for adaptive-memory signals.
pub struct MemorySignalsService {
    conn: Arc<Mutex<Connection>>,
    policy: MemoryPolicy,
    clock: Arc<dyn Clock>,
}

/// Row-level effects from reconciling local adaptive-memory state with the
/// current content index.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SignalReconcileResult {
    pub entry_stats_deleted: u64,
    pub affinities_deleted: u64,
    pub feedback_deleted: u64,
    pub session_results_deleted: u64,
    pub affinities_degraded: u64,
    pub session_results_degraded: u64,
    pub vectors_deleted: u64,
}

impl MemorySignalsService {
    pub fn new(
        conn: Arc<Mutex<Connection>>,
        policy: MemoryPolicy,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, CoreError> {
        policy.validate()?;
        {
            let mut guard = conn.lock().unwrap();
            guard
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(CoreError::Storage)?;
            storage::run_migrations(&mut guard)?;
        }
        Ok(Self {
            conn,
            policy,
            clock,
        })
    }

    pub fn policy(&self) -> &MemoryPolicy {
        &self.policy
    }

    /// Ensure the runtime-owned affinity vector table has the active model's
    /// dimension. Recreating it intentionally retains ordinary affinity rows
    /// for later provider-backed re-embedding.
    pub fn ensure_vec_query_affinities(&self, dim: usize) -> Result<DimReconciliation, CoreError> {
        if dim == 0 {
            return Err(CoreError::Validation(
                "affinity embedding dimension must be non-zero".into(),
            ));
        }

        let conn = self.conn.lock().unwrap();
        let existing_sql: Option<String> = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='vec_query_affinities'",
                [],
                |row| row.get(0),
            )
            .ok();

        let Some(sql) = existing_sql else {
            conn.execute_batch(&format!(
                "CREATE VIRTUAL TABLE vec_query_affinities USING vec0(
                    affinity_id TEXT PRIMARY KEY,
                    embedding FLOAT[{dim}] distance_metric=cosine
                )"
            ))
            .map_err(CoreError::Storage)?;
            return Ok(DimReconciliation::Created { dim });
        };

        let actual_dim = storage::parse_vec_dim(&sql).ok_or_else(|| {
            CoreError::Storage(rusqlite::Error::InvalidParameterName(format!(
                "cannot parse dim from vec_query_affinities SQL: {sql}"
            )))
        })?;
        if actual_dim == dim {
            return Ok(DimReconciliation::Consistent { dim });
        }

        conn.execute_batch("DROP TABLE vec_query_affinities")
            .map_err(CoreError::Storage)?;
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE vec_query_affinities USING vec0(
                affinity_id TEXT PRIMARY KEY,
                embedding FLOAT[{dim}] distance_metric=cosine
            )"
        ))
        .map_err(CoreError::Storage)?;

        Ok(DimReconciliation::Recreated {
            from: actual_dim,
            to: dim,
        })
    }

    /// Remove vector rows that cannot participate under the active model
    /// while retaining their ordinary affinity records for future re-embedding.
    /// The daemon calls this after reconciling the vector table on model change.
    pub fn remove_stale_affinity_vectors(&self, embedding_model: &str) -> Result<u64, CoreError> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM vec_query_affinities
             WHERE affinity_id IN (
                 SELECT vec.affinity_id
                 FROM vec_query_affinities vec
                 LEFT JOIN query_affinities affinity ON affinity.id = vec.affinity_id
                 WHERE affinity.id IS NULL OR affinity.embedding_model != ?1
             )",
            [embedding_model],
        )?;
        Ok(deleted as u64)
    }

    /// Plan provider work for every affinity that will survive a model-change
    /// coalescing pass. This is intentionally read-only: a provider failure
    /// after this call must leave all ordinary rows available for retry.
    pub fn list_affinities_for_reembedding(&self) -> Result<AffinityEmbeddingPlan, CoreError> {
        let conn = self.conn.lock().unwrap();
        let rows = load_affinity_reembedding_rows(&conn)?;
        let fingerprint = affinity_reembedding_fingerprint(&rows);
        let inputs = affinity_reembedding_groups(rows)?
            .into_iter()
            .map(|group| {
                Ok(AffinityEmbeddingInput {
                    affinity_id: storage::from_text(0, &group.survivor_id, Ulid::from_string)?,
                    effective_query_text: group.effective_query_text,
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        Ok(AffinityEmbeddingPlan {
            inputs,
            fingerprint,
        })
    }

    /// Atomically coalesce ordinary affinity rows onto `embedding_model` and
    /// replace the complete affinity vector index with provider-computed
    /// vectors for the retained survivors.
    ///
    /// The caller must compute every vector first. Any missing, duplicate, or
    /// malformed vector is rejected before SQL mutation; any SQL failure rolls
    /// the ordinary rows and prior vector table contents back together.
    pub fn replace_affinity_vectors(
        &self,
        embedding_model: &str,
        embedding_dim: usize,
        expected_fingerprint: &str,
        vectors: &[(Ulid, Vec<f32>)],
    ) -> Result<(), CoreError> {
        if embedding_model.is_empty() {
            return Err(CoreError::Validation(
                "affinity embedding model must not be empty".into(),
            ));
        }
        if embedding_dim == 0 {
            return Err(CoreError::Validation(
                "affinity embedding dimension must be non-zero".into(),
            ));
        }

        let mut vectors_by_id = HashMap::with_capacity(vectors.len());
        for (affinity_id, vector) in vectors {
            if vector.len() != embedding_dim {
                return Err(CoreError::Validation(format!(
                    "affinity vector {} has dimension {}, expected {embedding_dim}",
                    affinity_id,
                    vector.len()
                )));
            }
            if !vector.iter().all(|value| value.is_finite()) {
                return Err(CoreError::Validation(format!(
                    "affinity vector {affinity_id} contains a non-finite value"
                )));
            }
            if vectors_by_id
                .insert(affinity_id.to_string(), vector)
                .is_some()
            {
                return Err(CoreError::Validation(format!(
                    "duplicate affinity vector: {affinity_id}"
                )));
            }
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let rows = load_affinity_reembedding_rows(&tx)?;
        let actual_fingerprint = affinity_reembedding_fingerprint(&rows);
        if actual_fingerprint != expected_fingerprint {
            return Err(CoreError::Conflict(
                "query affinities changed during affinity re-embedding".into(),
            ));
        }
        let groups = affinity_reembedding_groups(rows)?;
        let expected_ids = groups
            .iter()
            .map(|group| group.survivor_id.as_str())
            .collect::<HashSet<_>>();
        if expected_ids.len() != vectors_by_id.len()
            || expected_ids
                .iter()
                .any(|affinity_id| !vectors_by_id.contains_key(*affinity_id))
        {
            return Err(CoreError::Validation(
                "affinity vector set does not match retained affinity rows".into(),
            ));
        }

        tx.execute("DELETE FROM vec_query_affinities", [])?;
        for group in groups {
            for affinity_id in &group.member_ids {
                if affinity_id != &group.survivor_id {
                    tx.execute("DELETE FROM query_affinities WHERE id = ?1", [affinity_id])?;
                }
            }
            tx.execute(
                "UPDATE query_affinities
                 SET raw_query_text = ?2,
                     effective_query_text = ?3,
                     embedding_model = ?4,
                     embedding_dim = ?5,
                     reinforcement_count = ?6,
                     last_reinforced_at = ?7,
                     created_at = ?8,
                     updated_at = ?9
                 WHERE id = ?1",
                params![
                    &group.survivor_id,
                    &group.raw_query_text,
                    &group.effective_query_text,
                    embedding_model,
                    embedding_dim as i64,
                    group.reinforcement_count,
                    &group.last_reinforced_at,
                    &group.created_at,
                    &group.updated_at,
                ],
            )?;
            let vector = vectors_by_id
                .get(&group.survivor_id)
                .expect("vector set checked against affinity plan");
            let bytes = vector
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect::<Vec<_>>();
            tx.execute(
                "INSERT INTO vec_query_affinities (affinity_id, embedding) VALUES (?1, ?2)",
                params![&group.survivor_id, bytes],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn create_search_session(&self, input: CreateSearchSession) -> Result<Ulid, CoreError> {
        if input.query_embedding.is_empty() {
            return Err(CoreError::Validation(
                "query embedding must be non-empty".into(),
            ));
        }

        let search_id = Ulid::new();
        let embedding: Vec<u8> = input
            .query_embedding
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = self.clock.now();
        let now_text = now.to_rfc3339();
        let expires_at = now + Duration::hours(self.policy.session_ttl_hours);
        purge_expired_sessions_in_tx(&tx, &now_text, 100, None)?;
        tx.execute(
            "INSERT INTO search_sessions (
                id, raw_query_text, effective_query_text, query_embedding,
                embedding_model, embedding_dim, created_at, expires_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                search_id.to_string(),
                input.raw_query_text,
                input.effective_query_text,
                embedding,
                input.embedding_model,
                input.query_embedding.len() as i64,
                now_text,
                expires_at.to_rfc3339(),
            ],
        )?;
        for result in input.results {
            tx.execute(
                "INSERT INTO search_session_results (
                    search_id, entry_id, matched_block_id, matched_chunk_id, result_rank
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    search_id.to_string(),
                    result.entry_id.to_string(),
                    result.matched_block_id.map(|id| id.to_string()),
                    result.matched_chunk_id.map(|id| id.to_string()),
                    result.result_rank as i64,
                ],
            )?;
        }
        tx.commit()?;
        Ok(search_id)
    }

    /// Delete up to `limit` expired search sessions, oldest first.
    pub fn purge_expired_sessions(&self, limit: u64) -> Result<u64, CoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = self.clock.now().to_rfc3339();
        let deleted = purge_expired_sessions_in_tx(&tx, &now, limit, None)?;
        tx.commit()?;
        Ok(deleted)
    }

    /// Apply positive feedback once per `(search_id, entry_id)` receipt.
    ///
    /// The session and every target are validated before receipt or signal
    /// state is written. A retry therefore returns the existing receipt
    /// without refreshing any reinforcement timestamps.
    pub fn apply_feedback(
        &self,
        search_id: Ulid,
        targets: &[FeedbackTarget],
    ) -> Result<FeedbackResult, CoreError> {
        self.apply_feedback_inner(search_id, targets, None)
    }

    /// Apply feedback only when the stored search session was produced by the
    /// caller's active embedding model and dimension. Daemon callers use this
    /// gate so an old session can never write its vector into a newly
    /// reconciled table; direct Core callers retain the ergonomic
    /// [`Self::apply_feedback`] API when they own provider compatibility.
    pub fn apply_feedback_for_embedding(
        &self,
        search_id: Ulid,
        targets: &[FeedbackTarget],
        active_embedding_model: &str,
        active_embedding_dim: usize,
    ) -> Result<FeedbackResult, CoreError> {
        self.apply_feedback_inner(
            search_id,
            targets,
            Some((active_embedding_model, active_embedding_dim)),
        )
    }

    fn apply_feedback_inner(
        &self,
        search_id: Ulid,
        targets: &[FeedbackTarget],
        active_embedding: Option<(&str, usize)>,
    ) -> Result<FeedbackResult, CoreError> {
        let search_id_text = search_id.to_string();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = self.clock.now();
        let now_text = now.to_rfc3339();

        purge_expired_sessions_in_tx(&tx, &now_text, 100, Some(&search_id_text))?;

        let session = tx
            .query_row(
                "SELECT raw_query_text, effective_query_text, query_embedding,
                        embedding_model, embedding_dim, expires_at
                 FROM search_sessions WHERE id = ?1",
                [&search_id_text],
                |row| {
                    let expires_at: String = row.get(5)?;
                    Ok(FeedbackSession {
                        raw_query_text: row.get(0)?,
                        effective_query_text: row.get(1)?,
                        query_embedding: row.get(2)?,
                        embedding_model: row.get(3)?,
                        embedding_dim: row.get(4)?,
                        expires_at: storage::from_text(
                            5,
                            &expires_at,
                            DateTime::parse_from_rfc3339,
                        )?
                        .with_timezone(&Utc),
                    })
                },
            )
            .optional()?
            .ok_or(CoreError::ResourceNotFound {
                resource: "search session",
                id: search_id,
            })?;

        if now >= session.expires_at {
            return Err(CoreError::Conflict("search session expired".into()));
        }
        if targets.is_empty() {
            return Err(CoreError::Validation(
                "feedback targets must not be empty".into(),
            ));
        }

        let mut seen_entries = HashSet::with_capacity(targets.len());
        for target in targets {
            if !seen_entries.insert(target.entry_id) {
                return Err(CoreError::Validation(format!(
                    "duplicate feedback target: {}",
                    target.entry_id
                )));
            }
        }

        if let Some((active_model, active_dim)) = active_embedding
            && (session.embedding_model != active_model
                || usize::try_from(session.embedding_dim).ok() != Some(active_dim))
        {
            return Err(CoreError::Conflict(format!(
                "search session embedding model/dimension mismatch: session {}:{}, active {active_model}:{active_dim}; run search.hybrid again",
                session.embedding_model, session.embedding_dim
            )));
        }

        // Receipt lookup deliberately precedes current precision/ownership
        // validation. A previously accepted request remains idempotent after a
        // reindex regenerates IDs or later content mutation invalidates them.
        // New receipts still validate the recorded result and all current
        // ownership before any target in this request is mutated.
        let mut already_applied = Vec::new();
        let mut validated_targets = Vec::with_capacity(targets.len());
        for target in targets {
            let receipt_exists: bool = tx.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM search_feedback
                    WHERE search_id = ?1 AND entry_id = ?2
                 )",
                params![&search_id_text, target.entry_id.to_string()],
                |row| row.get(0),
            )?;
            if receipt_exists {
                already_applied.push(target.entry_id);
            } else {
                validated_targets.push(validate_feedback_target(&tx, &search_id_text, target)?);
            }
        }

        let normalized_query = session
            .effective_query_text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase();
        let normalized_query_hash = blake3::hash(normalized_query.as_bytes())
            .to_hex()
            .to_string();
        let max_reinforcements = i64::from(self.policy.max_reinforcements);
        let mut applied = Vec::new();

        for target in validated_targets {
            let entry_id = target.entry_id.to_string();
            let block_id = target.block_id.map(|id| id.to_string());
            let chunk_id = target.chunk_id.map(|id| id.to_string());
            let inserted = tx.execute(
                "INSERT INTO search_feedback (
                    id, search_id, entry_id, block_id, chunk_id, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(search_id, entry_id) DO NOTHING",
                params![
                    Ulid::new().to_string(),
                    &search_id_text,
                    &entry_id,
                    &block_id,
                    &chunk_id,
                    &now_text,
                ],
            )?;
            if inserted == 0 {
                already_applied.push(target.entry_id);
                continue;
            }

            let reinforcement_count: u8 = tx.query_row(
                "INSERT INTO entry_memory_stats (
                    entry_id, reinforcement_count, last_reinforced_at, updated_at
                 ) VALUES (?1, 1, ?2, ?2)
                 ON CONFLICT(entry_id) DO UPDATE SET
                    reinforcement_count = CASE
                        WHEN entry_memory_stats.reinforcement_count < ?3
                        THEN entry_memory_stats.reinforcement_count + 1
                        ELSE entry_memory_stats.reinforcement_count
                    END,
                    last_reinforced_at = excluded.last_reinforced_at,
                    updated_at = excluded.updated_at
                 RETURNING reinforcement_count",
                params![&entry_id, &now_text, max_reinforcements],
                |row| row.get(0),
            )?;

            let (affinity_id, affinity_count): (String, u8) = tx.query_row(
                "INSERT INTO query_affinities (
                    id, normalized_query_hash, raw_query_text, effective_query_text,
                    embedding_model, embedding_dim, entry_id, block_id, chunk_id,
                    reinforcement_count, last_reinforced_at, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, ?10, ?10)
                 ON CONFLICT DO UPDATE SET
                    raw_query_text = excluded.raw_query_text,
                    effective_query_text = excluded.effective_query_text,
                    embedding_dim = excluded.embedding_dim,
                    reinforcement_count = CASE
                        WHEN query_affinities.reinforcement_count < ?11
                        THEN query_affinities.reinforcement_count + 1
                        ELSE query_affinities.reinforcement_count
                    END,
                    last_reinforced_at = excluded.last_reinforced_at,
                    updated_at = excluded.updated_at
                 RETURNING id, reinforcement_count",
                params![
                    Ulid::new().to_string(),
                    &normalized_query_hash,
                    &session.raw_query_text,
                    &session.effective_query_text,
                    &session.embedding_model,
                    session.embedding_dim,
                    &entry_id,
                    &block_id,
                    &chunk_id,
                    &now_text,
                    max_reinforcements,
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            tx.execute(
                "DELETE FROM vec_query_affinities WHERE affinity_id = ?1",
                [&affinity_id],
            )?;
            tx.execute(
                "INSERT INTO vec_query_affinities (affinity_id, embedding) VALUES (?1, ?2)",
                params![&affinity_id, &session.query_embedding],
            )?;

            applied.push(AppliedFeedback {
                entry_id: target.entry_id,
                reinforcement_count,
                affinity_count,
                last_reinforced_at: now,
            });
        }

        tx.commit()?;
        Ok(FeedbackResult {
            applied,
            already_applied,
        })
    }

    /// Remove every local adaptive-memory row owned by an Entry.
    pub fn delete_entry_signals(&self, entry_id: Ulid) -> Result<(), CoreError> {
        self.delete_entries_signals(&[entry_id])
    }

    /// Remove every local adaptive-memory row owned by the supplied Entries in
    /// one transaction. Bulk daemon operations call this only after their
    /// content deletions commit; on failure the complete signal cleanup rolls
    /// back so later index reconciliation can retry from an intact state.
    pub fn delete_entries_signals(&self, entry_ids: &[Ulid]) -> Result<(), CoreError> {
        let mut entry_ids = entry_ids
            .iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if entry_ids.is_empty() {
            return Ok(());
        }
        entry_ids.sort();
        let placeholders = std::iter::repeat_n("?", entry_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            &format!(
                "DELETE FROM vec_query_affinities
             WHERE affinity_id IN (
                 SELECT id FROM query_affinities WHERE entry_id IN ({placeholders})
             )"
            ),
            rusqlite::params_from_iter(entry_ids.iter()),
        )?;
        tx.execute(
            &format!("DELETE FROM query_affinities WHERE entry_id IN ({placeholders})"),
            rusqlite::params_from_iter(entry_ids.iter()),
        )?;
        tx.execute(
            &format!("DELETE FROM entry_memory_stats WHERE entry_id IN ({placeholders})"),
            rusqlite::params_from_iter(entry_ids.iter()),
        )?;
        tx.execute(
            &format!("DELETE FROM search_feedback WHERE entry_id IN ({placeholders})"),
            rusqlite::params_from_iter(entry_ids.iter()),
        )?;
        tx.execute(
            &format!("DELETE FROM search_session_results WHERE entry_id IN ({placeholders})"),
            rusqlite::params_from_iter(entry_ids.iter()),
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Degrade materialized precision for a deleted Block and its captured
    /// Chunk IDs in one transaction while retaining immutable feedback
    /// receipt history.
    pub fn degrade_block_precision(
        &self,
        block_id: Ulid,
        chunk_ids: &[Ulid],
    ) -> Result<(), CoreError> {
        let block_id = block_id.to_string();
        let chunk_ids = chunk_ids
            .iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let mut candidates = {
            let mut stmt =
                tx.prepare("SELECT id FROM query_affinities WHERE block_id = ?1 ORDER BY id")?;
            stmt.query_map([&block_id], |row| {
                Ok(AffinityPrecisionUpdate {
                    affinity_id: row.get(0)?,
                    block_id: None,
                    chunk_id: None,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        if !chunk_ids.is_empty() {
            let seen = candidates
                .iter()
                .map(|candidate| candidate.affinity_id.clone())
                .collect::<HashSet<_>>();
            let placeholders = std::iter::repeat_n("?", chunk_ids.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT id, block_id FROM query_affinities
                 WHERE chunk_id IN ({placeholders}) ORDER BY id"
            );
            let mut stmt = tx.prepare(&sql)?;
            let chunk_candidates = stmt
                .query_map(rusqlite::params_from_iter(chunk_ids.iter()), |row| {
                    Ok(AffinityPrecisionUpdate {
                        affinity_id: row.get(0)?,
                        block_id: row.get(1)?,
                        chunk_id: None,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            candidates.extend(
                chunk_candidates
                    .into_iter()
                    .filter(|candidate| !seen.contains(&candidate.affinity_id)),
            );
        }
        degrade_affinity_precision_in_tx(&tx, candidates)?;
        tx.execute(
            "UPDATE search_session_results
             SET matched_block_id = NULL, matched_chunk_id = NULL
             WHERE matched_block_id = ?1",
            [&block_id],
        )?;
        if !chunk_ids.is_empty() {
            let placeholders = std::iter::repeat_n("?", chunk_ids.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "UPDATE search_session_results SET matched_chunk_id = NULL
                 WHERE matched_chunk_id IN ({placeholders})"
            );
            tx.execute(&sql, rusqlite::params_from_iter(chunk_ids.iter()))?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Clear only the captured stale Chunk IDs, preserving valid Block
    /// precision and immutable feedback receipt history.
    pub fn degrade_chunk_precision(&self, chunk_ids: &[Ulid]) -> Result<(), CoreError> {
        if chunk_ids.is_empty() {
            return Ok(());
        }
        let chunk_ids = chunk_ids
            .iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let placeholders = std::iter::repeat_n("?", chunk_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let candidates = {
            let sql = format!(
                "SELECT id, block_id FROM query_affinities
                 WHERE chunk_id IN ({placeholders}) ORDER BY id"
            );
            let mut stmt = tx.prepare(&sql)?;
            stmt.query_map(rusqlite::params_from_iter(chunk_ids.iter()), |row| {
                Ok(AffinityPrecisionUpdate {
                    affinity_id: row.get(0)?,
                    block_id: row.get(1)?,
                    chunk_id: None,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        degrade_affinity_precision_in_tx(&tx, candidates)?;
        let sql = format!(
            "UPDATE search_session_results SET matched_chunk_id = NULL
             WHERE matched_chunk_id IN ({placeholders})"
        );
        tx.execute(&sql, rusqlite::params_from_iter(chunk_ids.iter()))?;
        tx.commit()?;
        Ok(())
    }

    /// Reconcile local signal references only after a complete content sync
    /// or rebuild has restored the index to a stable state.
    pub fn reconcile_content_references(&self) -> Result<SignalReconcileResult, CoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let entry_stats_deleted = tx.execute(
            "DELETE FROM entry_memory_stats
             WHERE NOT EXISTS (
                 SELECT 1 FROM entries WHERE entries.id = entry_memory_stats.entry_id
             )",
            [],
        )? as u64;
        let feedback_deleted = tx.execute(
            "DELETE FROM search_feedback
             WHERE NOT EXISTS (
                 SELECT 1 FROM entries WHERE entries.id = search_feedback.entry_id
             )",
            [],
        )? as u64;
        let session_results_deleted = tx.execute(
            "DELETE FROM search_session_results
             WHERE NOT EXISTS (
                 SELECT 1 FROM entries
                 WHERE entries.id = search_session_results.entry_id
             )",
            [],
        )? as u64;
        let affinities_deleted = tx.execute(
            "DELETE FROM query_affinities
             WHERE NOT EXISTS (
                 SELECT 1 FROM entries WHERE entries.id = query_affinities.entry_id
             )",
            [],
        )? as u64;

        let invalid_blocks = {
            let mut stmt = tx.prepare(
                "SELECT affinity.id
                 FROM query_affinities affinity
                 WHERE affinity.block_id IS NOT NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM blocks block
                       WHERE block.id = affinity.block_id
                         AND block.entry_id = affinity.entry_id
                   )
                 ORDER BY affinity.id",
            )?;
            stmt.query_map([], |row| {
                Ok(AffinityPrecisionUpdate {
                    affinity_id: row.get(0)?,
                    block_id: None,
                    chunk_id: None,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let (block_affinities_degraded, block_merge_vectors_deleted) =
            degrade_affinity_precision_in_tx(&tx, invalid_blocks)?;

        let invalid_chunks = {
            let mut stmt = tx.prepare(
                "SELECT affinity.id, affinity.block_id
                 FROM query_affinities affinity
                 WHERE affinity.chunk_id IS NOT NULL
                   AND NOT EXISTS (
                       SELECT 1
                       FROM chunks chunk
                       JOIN blocks block ON block.id = chunk.block_id
                       WHERE chunk.id = affinity.chunk_id
                         AND block.entry_id = affinity.entry_id
                         AND (affinity.block_id IS NULL
                              OR affinity.block_id = chunk.block_id)
                   )
                 ORDER BY affinity.id",
            )?;
            stmt.query_map([], |row| {
                Ok(AffinityPrecisionUpdate {
                    affinity_id: row.get(0)?,
                    block_id: row.get(1)?,
                    chunk_id: None,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let (chunk_affinities_degraded, chunk_merge_vectors_deleted) =
            degrade_affinity_precision_in_tx(&tx, invalid_chunks)?;

        let session_blocks_degraded = tx.execute(
            "UPDATE search_session_results
             SET matched_block_id = NULL, matched_chunk_id = NULL
             WHERE matched_block_id IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM blocks block
                   WHERE block.id = search_session_results.matched_block_id
                     AND block.entry_id = search_session_results.entry_id
               )",
            [],
        )? as u64;
        let session_chunks_degraded = tx.execute(
            "UPDATE search_session_results
             SET matched_chunk_id = NULL
             WHERE matched_chunk_id IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1
                   FROM chunks chunk
                   JOIN blocks block ON block.id = chunk.block_id
                   WHERE chunk.id = search_session_results.matched_chunk_id
                     AND block.entry_id = search_session_results.entry_id
                     AND (search_session_results.matched_block_id IS NULL
                          OR search_session_results.matched_block_id = chunk.block_id)
               )",
            [],
        )? as u64;
        let orphan_vectors_deleted = tx.execute(
            "DELETE FROM vec_query_affinities
             WHERE NOT EXISTS (
                 SELECT 1 FROM query_affinities
                 WHERE query_affinities.id = vec_query_affinities.affinity_id
             )",
            [],
        )? as u64;

        let result = SignalReconcileResult {
            entry_stats_deleted,
            affinities_deleted,
            feedback_deleted,
            session_results_deleted,
            affinities_degraded: block_affinities_degraded + chunk_affinities_degraded,
            session_results_degraded: session_blocks_degraded + session_chunks_degraded,
            vectors_deleted: block_merge_vectors_deleted
                + chunk_merge_vectors_deleted
                + orphan_vectors_deleted,
        };
        tx.commit()?;
        Ok(result)
    }

    /// Load dynamic Entry factors in one batched read. Missing signal rows use
    /// the Entry creation time and zero reinforcements.
    pub fn entry_memory_signals(
        &self,
        entry_ids: &[Ulid],
    ) -> Result<HashMap<Ulid, EntryMemorySignal>, CoreError> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids: Vec<String> = entry_ids.iter().map(ToString::to_string).collect();
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT e.id, COALESCE(stats.reinforcement_count, 0),
                    COALESCE(stats.last_reinforced_at, e.created_at)
             FROM entries e
             LEFT JOIN entry_memory_stats stats ON stats.entry_id = e.id
             WHERE e.id IN ({placeholders})"
        );
        let conn = self.conn.lock().unwrap();
        let now = self.clock.now();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(ids.iter()), |row| {
            let id_text: String = row.get(0)?;
            let last_reinforced_text: String = row.get(2)?;
            Ok((
                storage::from_text(0, &id_text, Ulid::from_string)?,
                row.get::<_, u8>(1)?,
                storage::from_text(2, &last_reinforced_text, DateTime::parse_from_rfc3339)?
                    .with_timezone(&Utc),
            ))
        })?;

        let mut signals = HashMap::new();
        for row in rows {
            let (entry_id, reinforcement_count, last_reinforced_at) = row?;
            signals.insert(
                entry_id,
                EntryMemorySignal {
                    entry_id,
                    reinforcement_count,
                    last_reinforced_at,
                    memory_factor: self.policy.entry_memory_factor(
                        reinforcement_count,
                        last_reinforced_at,
                        now,
                    ),
                },
            );
        }
        Ok(signals)
    }

    /// Find learned query associations compatible with the active query.
    ///
    /// sqlite-vec applies KNN before joined-table predicates. The fast path
    /// can safely request every local row only through sqlite-vec's 4,096-row
    /// KNN cap. Larger tables fall back to an exact Rust cosine scan after the
    /// SQL filters, so inactive models and stale references cannot consume
    /// active result slots.
    pub fn affinity_candidates(
        &self,
        query_embedding: &[f32],
        embedding_model: &str,
        limit: usize,
        block_type: Option<&str>,
        tag: Option<&str>,
        include_benchmark: bool,
    ) -> Result<Vec<AffinityHit>, CoreError> {
        if query_embedding.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let query_dim = query_embedding.len() as i64;
        let conn = self.conn.lock().unwrap();
        let now = self.clock.now();
        let vector_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM vec_query_affinities", [], |row| {
                row.get(0)
            })?;
        if vector_count == 0 {
            return Ok(Vec::new());
        }

        let visibility = if include_benchmark {
            "1 = 1".to_string()
        } else {
            format!("NOT {BENCHMARK_ENTRY_PREDICATE}")
        };
        let tag_clause = if tag.is_some() {
            " AND EXISTS (SELECT 1 FROM json_each(e.tags) tag_value WHERE tag_value.value = ?)"
        } else {
            ""
        };
        let block_clause = if block_type.is_some() {
            " AND precise_block.type = ?"
        } else {
            ""
        };

        // A valid stored precision target must still belong to the affinity's
        // Entry. Invalid precision is returned as Entry-only rather than
        // attaching another Entry's Block or Chunk to this hit.
        let joins = "JOIN query_affinities affinity ON affinity.id = vec.affinity_id
                     JOIN entries e ON e.id = affinity.entry_id
                     LEFT JOIN blocks precise_block
                       ON precise_block.id = affinity.block_id
                      AND precise_block.entry_id = affinity.entry_id
                     LEFT JOIN chunks precise_chunk ON precise_chunk.id = affinity.chunk_id
                     LEFT JOIN blocks precise_chunk_block
                       ON precise_chunk_block.id = precise_chunk.block_id
                      AND precise_chunk_block.entry_id = affinity.entry_id";
        let conditions = format!(
            "affinity.embedding_model = ?
             AND affinity.embedding_dim = ?
             AND {visibility}{block_clause}{tag_clause}"
        );
        let filter_binds = || {
            let mut binds = vec![
                rusqlite::types::Value::Text(embedding_model.into()),
                rusqlite::types::Value::Integer(query_dim),
            ];
            if let Some(block_type) = block_type {
                binds.push(rusqlite::types::Value::Text(block_type.into()));
            }
            if let Some(tag) = tag {
                binds.push(rusqlite::types::Value::Text(tag.into()));
            }
            binds
        };
        let selected_columns = "affinity.id, affinity.entry_id,
                                precise_block.id,
                                CASE WHEN precise_chunk_block.id IS NULL
                                     THEN NULL ELSE affinity.chunk_id END,
                                affinity.reinforcement_count,
                                affinity.last_reinforced_at";
        let raws = if vector_count <= 4096 {
            let query_bytes: Vec<u8> = query_embedding
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect();
            let sql = format!(
                "SELECT {selected_columns}, vec.distance
                 FROM vec_query_affinities vec
                 {joins}
                 WHERE vec.embedding MATCH ? AND k = ? AND {conditions}
                 ORDER BY vec.distance"
            );
            let mut binds = vec![
                rusqlite::types::Value::Blob(query_bytes),
                rusqlite::types::Value::Integer(vector_count),
            ];
            binds.extend(filter_binds());
            let mut stmt = conn.prepare(&sql)?;
            stmt.query_map(rusqlite::params_from_iter(binds.iter()), |row| {
                raw_affinity_from_row(row, 1.0 - row.get::<_, f64>(6)?)
            })?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            let sql = format!(
                "SELECT {selected_columns}, vec.embedding
                 FROM vec_query_affinities vec
                 {joins}
                 WHERE {conditions}"
            );
            let binds = filter_binds();
            let mut stmt = conn.prepare(&sql)?;
            stmt.query_map(rusqlite::params_from_iter(binds.iter()), |row| {
                let bytes: Vec<u8> = row.get(6)?;
                let candidate = decode_vec0_f32(&bytes)?;
                let similarity = cosine_similarity(query_embedding, &candidate).unwrap_or(-1.0);
                raw_affinity_from_row(row, similarity)
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        Ok(self.rank_affinities(raws, now, limit))
    }

    /// Find learned query associations while preserving every eligible hit
    /// for a required Entry set. The full eligible set is ranked and
    /// deduplicated before selection, so returned hits retain their original
    /// global `affinity_rank` and `affinity_score`. Required hits do not
    /// consume the allowance for non-required supplemental hits.
    #[allow(clippy::too_many_arguments)]
    pub fn affinity_candidates_with_required_entries(
        &self,
        query_embedding: &[f32],
        embedding_model: &str,
        required_entry_ids: &[Ulid],
        non_required_limit: usize,
        block_type: Option<&str>,
        tag: Option<&str>,
        include_benchmark: bool,
    ) -> Result<Vec<AffinityHit>, CoreError> {
        let required = required_entry_ids.iter().copied().collect::<HashSet<_>>();
        let ranked = self.affinity_candidates(
            query_embedding,
            embedding_model,
            usize::MAX,
            block_type,
            tag,
            include_benchmark,
        )?;
        let mut non_required_selected = 0;
        Ok(ranked
            .into_iter()
            .filter(|hit| {
                if required.contains(&hit.entry_id) {
                    true
                } else if non_required_selected < non_required_limit {
                    non_required_selected += 1;
                    true
                } else {
                    false
                }
            })
            .collect())
    }

    fn rank_affinities(
        &self,
        raws: Vec<RawAffinity>,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Vec<AffinityHit> {
        let mut candidates = raws
            .into_iter()
            .filter_map(|raw| {
                (raw.similarity >= self.policy.affinity_similarity_threshold).then(|| {
                    let confidence = self.policy.normalized_similarity(raw.similarity)
                        * self.policy.decay(raw.last_reinforced_at, now)
                        * self
                            .policy
                            .affinity_reinforcement_factor(raw.reinforcement_count);
                    (raw, confidence)
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|(left, left_confidence), (right, right_confidence)| {
            right_confidence
                .total_cmp(left_confidence)
                .then_with(|| right.similarity.total_cmp(&left.similarity))
                .then_with(|| left.entry_id.cmp(&right.entry_id))
                .then_with(|| left.affinity_id.cmp(&right.affinity_id))
        });

        let mut seen_entries = HashSet::new();
        let mut hits = Vec::new();
        for (raw, confidence) in candidates {
            if !seen_entries.insert(raw.entry_id) {
                continue;
            }
            let affinity_rank = hits.len() as u32 + 1;
            hits.push(AffinityHit {
                entry_id: raw.entry_id,
                block_id: raw.block_id,
                chunk_id: raw.chunk_id,
                similarity: raw.similarity,
                confidence,
                affinity_rank,
                affinity_score: self.policy.affinity_weight * confidence
                    / (60.0 + f64::from(affinity_rank)),
            });
            if hits.len() == limit {
                break;
            }
        }
        hits
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AffinityReembeddingKey {
    normalized_query_hash: String,
    entry_id: String,
    block_id: Option<String>,
    chunk_id: Option<String>,
}

struct AffinityReembeddingRow {
    affinity_id: String,
    key: AffinityReembeddingKey,
    raw_query_text: String,
    effective_query_text: String,
    embedding_model: String,
    embedding_dim: i64,
    reinforcement_count: u8,
    last_reinforced_at: String,
    created_at: String,
    updated_at: String,
}

struct AffinityReembeddingGroup {
    survivor_id: String,
    member_ids: Vec<String>,
    raw_query_text: String,
    effective_query_text: String,
    reinforcement_count: u8,
    last_reinforced_at: String,
    created_at: String,
    updated_at: String,
}

fn load_affinity_reembedding_rows(
    conn: &Connection,
) -> Result<Vec<AffinityReembeddingRow>, CoreError> {
    let mut stmt = conn.prepare(
        "SELECT id, normalized_query_hash, raw_query_text,
                effective_query_text, embedding_model, embedding_dim,
                entry_id, block_id, chunk_id, reinforcement_count,
                last_reinforced_at, created_at, updated_at
         FROM query_affinities ORDER BY id",
    )?;
    Ok(stmt
        .query_map([], |row| {
            Ok(AffinityReembeddingRow {
                affinity_id: row.get(0)?,
                key: AffinityReembeddingKey {
                    normalized_query_hash: row.get(1)?,
                    entry_id: row.get(6)?,
                    block_id: row.get(7)?,
                    chunk_id: row.get(8)?,
                },
                raw_query_text: row.get(2)?,
                effective_query_text: row.get(3)?,
                embedding_model: row.get(4)?,
                embedding_dim: row.get(5)?,
                reinforcement_count: row.get(9)?,
                last_reinforced_at: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

fn fingerprint_text(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

fn fingerprint_optional_text(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            fingerprint_text(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

/// Hash the complete ordinary affinity state in stable ID order. Every field
/// that can affect identity, survivor metadata, strength, or vector meaning is
/// length-delimited so distinct row layouts cannot collide by concatenation.
fn affinity_reembedding_fingerprint(rows: &[AffinityReembeddingRow]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"nomai-query-affinities-v1");
    hasher.update(&(rows.len() as u64).to_le_bytes());
    for row in rows {
        fingerprint_text(&mut hasher, &row.affinity_id);
        fingerprint_text(&mut hasher, &row.key.normalized_query_hash);
        fingerprint_text(&mut hasher, &row.raw_query_text);
        fingerprint_text(&mut hasher, &row.effective_query_text);
        fingerprint_text(&mut hasher, &row.embedding_model);
        hasher.update(&row.embedding_dim.to_le_bytes());
        fingerprint_text(&mut hasher, &row.key.entry_id);
        fingerprint_optional_text(&mut hasher, row.key.block_id.as_deref());
        fingerprint_optional_text(&mut hasher, row.key.chunk_id.as_deref());
        hasher.update(&[row.reinforcement_count]);
        fingerprint_text(&mut hasher, &row.last_reinforced_at);
        fingerprint_text(&mut hasher, &row.created_at);
        fingerprint_text(&mut hasher, &row.updated_at);
    }
    hasher.finalize().to_hex().to_string()
}

/// Compute model-independent affinity groups from one already-fingerprinted
/// row snapshot. The latest reinforced/query example supplies the survivor ID
/// and texts; strength never stacks.
fn affinity_reembedding_groups(
    rows: Vec<AffinityReembeddingRow>,
) -> Result<Vec<AffinityReembeddingGroup>, CoreError> {
    let mut groups = HashMap::<AffinityReembeddingKey, AffinityReembeddingGroup>::new();
    for row in rows {
        // Validate every persisted timestamp even for the first row in a
        // group, so planning cannot silently carry malformed metadata into a
        // replacement transaction.
        let row_last = parse_affinity_timestamp(&row.last_reinforced_at)?;
        let row_created = parse_affinity_timestamp(&row.created_at)?;
        let row_updated = parse_affinity_timestamp(&row.updated_at)?;
        if let Some(group) = groups.get_mut(&row.key) {
            let group_last = parse_affinity_timestamp(&group.last_reinforced_at)?;
            let group_created = parse_affinity_timestamp(&group.created_at)?;
            let group_updated = parse_affinity_timestamp(&group.updated_at)?;
            let row_is_latest = row_last > group_last
                || (row_last == group_last
                    && (row_updated > group_updated
                        || (row_updated == group_updated && row.affinity_id > group.survivor_id)));

            group.member_ids.push(row.affinity_id.clone());
            group.reinforcement_count = group
                .reinforcement_count
                .max(row.reinforcement_count)
                .min(MAX_PERSISTED_REINFORCEMENTS);
            if row_last > group_last {
                group.last_reinforced_at.clone_from(&row.last_reinforced_at);
            }
            if row_created < group_created {
                group.created_at.clone_from(&row.created_at);
            }
            if row_updated > group_updated {
                group.updated_at.clone_from(&row.updated_at);
            }
            if row_is_latest {
                group.survivor_id = row.affinity_id;
                group.raw_query_text = row.raw_query_text;
                group.effective_query_text = row.effective_query_text;
            }
        } else {
            groups.insert(
                row.key,
                AffinityReembeddingGroup {
                    survivor_id: row.affinity_id.clone(),
                    member_ids: vec![row.affinity_id],
                    raw_query_text: row.raw_query_text,
                    effective_query_text: row.effective_query_text,
                    reinforcement_count: row.reinforcement_count.min(MAX_PERSISTED_REINFORCEMENTS),
                    last_reinforced_at: row.last_reinforced_at,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                },
            );
        }
    }

    let mut groups = groups.into_values().collect::<Vec<_>>();
    groups.sort_by(|left, right| left.survivor_id.cmp(&right.survivor_id));
    Ok(groups)
}

struct AffinityPrecisionUpdate {
    affinity_id: String,
    block_id: Option<String>,
    chunk_id: Option<String>,
}

struct AffinityMergeState {
    normalized_query_hash: String,
    raw_query_text: String,
    effective_query_text: String,
    embedding_model: String,
    embedding_dim: i64,
    entry_id: String,
    reinforcement_count: u8,
    last_reinforced_at: String,
    created_at: String,
    updated_at: String,
}

struct AffinityCollisionState {
    affinity_id: String,
    raw_query_text: String,
    effective_query_text: String,
    embedding_dim: i64,
    reinforcement_count: u8,
    last_reinforced_at: String,
    created_at: String,
    updated_at: String,
}

/// Apply nullable precision changes without violating the expression-backed
/// exact-target uniqueness key. When two rows collapse to the same target,
/// keep one example and preserve the latest query/vector metadata, maximum
/// bounded strength, latest reinforcement time, and earliest creation time.
fn degrade_affinity_precision_in_tx(
    tx: &Transaction<'_>,
    updates: Vec<AffinityPrecisionUpdate>,
) -> Result<(u64, u64), CoreError> {
    let mut rows_degraded = 0;
    let mut vectors_deleted = 0;
    for update in updates {
        let state = tx
            .query_row(
                "SELECT normalized_query_hash, raw_query_text, effective_query_text,
                        embedding_model, embedding_dim, entry_id, reinforcement_count,
                        last_reinforced_at, created_at, updated_at
                 FROM query_affinities WHERE id = ?1",
                [&update.affinity_id],
                |row| {
                    Ok(AffinityMergeState {
                        normalized_query_hash: row.get(0)?,
                        raw_query_text: row.get(1)?,
                        effective_query_text: row.get(2)?,
                        embedding_model: row.get(3)?,
                        embedding_dim: row.get(4)?,
                        entry_id: row.get(5)?,
                        reinforcement_count: row.get(6)?,
                        last_reinforced_at: row.get(7)?,
                        created_at: row.get(8)?,
                        updated_at: row.get(9)?,
                    })
                },
            )
            .optional()?;
        let Some(state) = state else {
            continue;
        };
        let collision = tx
            .query_row(
                "SELECT id, raw_query_text, effective_query_text, embedding_dim,
                        reinforcement_count, last_reinforced_at, created_at, updated_at
                 FROM query_affinities
                 WHERE id != ?1
                   AND normalized_query_hash = ?2
                   AND embedding_model = ?3
                   AND entry_id = ?4
                   AND COALESCE(block_id, '') = COALESCE(?5, '')
                   AND COALESCE(chunk_id, '') = COALESCE(?6, '')
                 LIMIT 1",
                params![
                    &update.affinity_id,
                    &state.normalized_query_hash,
                    &state.embedding_model,
                    &state.entry_id,
                    update.block_id.as_deref(),
                    update.chunk_id.as_deref(),
                ],
                |row| {
                    Ok(AffinityCollisionState {
                        affinity_id: row.get(0)?,
                        raw_query_text: row.get(1)?,
                        effective_query_text: row.get(2)?,
                        embedding_dim: row.get(3)?,
                        reinforcement_count: row.get(4)?,
                        last_reinforced_at: row.get(5)?,
                        created_at: row.get(6)?,
                        updated_at: row.get(7)?,
                    })
                },
            )
            .optional()?;

        if let Some(survivor) = collision {
            let survivor_last = parse_affinity_timestamp(&survivor.last_reinforced_at)?;
            let degraded_last = parse_affinity_timestamp(&state.last_reinforced_at)?;
            let survivor_updated = parse_affinity_timestamp(&survivor.updated_at)?;
            let degraded_updated = parse_affinity_timestamp(&state.updated_at)?;
            let degraded_is_latest = degraded_last > survivor_last
                || (degraded_last == survivor_last
                    && (degraded_updated > survivor_updated
                        || (degraded_updated == survivor_updated
                            && update.affinity_id > survivor.affinity_id)));
            let created_at = if parse_affinity_timestamp(&state.created_at)?
                < parse_affinity_timestamp(&survivor.created_at)?
            {
                &state.created_at
            } else {
                &survivor.created_at
            };
            let last_reinforced_at = if degraded_last > survivor_last {
                &state.last_reinforced_at
            } else {
                &survivor.last_reinforced_at
            };
            let updated_at = if degraded_updated > survivor_updated {
                &state.updated_at
            } else {
                &survivor.updated_at
            };
            let (raw_query_text, effective_query_text, embedding_dim) = if degraded_is_latest {
                (
                    &state.raw_query_text,
                    &state.effective_query_text,
                    state.embedding_dim,
                )
            } else {
                (
                    &survivor.raw_query_text,
                    &survivor.effective_query_text,
                    survivor.embedding_dim,
                )
            };
            let survivor_vector = tx
                .query_row(
                    "SELECT embedding FROM vec_query_affinities WHERE affinity_id = ?1",
                    [&survivor.affinity_id],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            let degraded_vector = tx
                .query_row(
                    "SELECT embedding FROM vec_query_affinities WHERE affinity_id = ?1",
                    [&update.affinity_id],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            let merged_vector = if degraded_is_latest {
                degraded_vector
                    .as_ref()
                    .or(survivor_vector.as_ref())
                    .cloned()
            } else {
                survivor_vector
                    .as_ref()
                    .or(degraded_vector.as_ref())
                    .cloned()
            };
            let vectors_before =
                u64::from(survivor_vector.is_some()) + u64::from(degraded_vector.is_some());
            tx.execute(
                "UPDATE query_affinities
                 SET raw_query_text = ?2,
                     effective_query_text = ?3,
                     embedding_dim = ?4,
                     reinforcement_count = ?5,
                     last_reinforced_at = ?6,
                     created_at = ?7,
                     updated_at = ?8
                 WHERE id = ?1",
                params![
                    &survivor.affinity_id,
                    raw_query_text,
                    effective_query_text,
                    embedding_dim,
                    state.reinforcement_count.max(survivor.reinforcement_count),
                    last_reinforced_at,
                    created_at,
                    updated_at,
                ],
            )?;
            tx.execute(
                "DELETE FROM vec_query_affinities WHERE affinity_id = ?1",
                [&survivor.affinity_id],
            )?;
            tx.execute(
                "DELETE FROM vec_query_affinities WHERE affinity_id = ?1",
                [&update.affinity_id],
            )?;
            tx.execute(
                "DELETE FROM query_affinities WHERE id = ?1",
                [&update.affinity_id],
            )?;
            if let Some(embedding) = merged_vector {
                tx.execute(
                    "INSERT INTO vec_query_affinities (affinity_id, embedding)
                     VALUES (?1, ?2)",
                    params![&survivor.affinity_id, embedding],
                )?;
            }
            vectors_deleted += vectors_before.saturating_sub(1);
        } else {
            tx.execute(
                "UPDATE query_affinities SET block_id = ?2, chunk_id = ?3
                 WHERE id = ?1",
                params![
                    &update.affinity_id,
                    update.block_id.as_deref(),
                    update.chunk_id.as_deref(),
                ],
            )?;
        }
        rows_degraded += 1;
    }
    Ok((rows_degraded, vectors_deleted))
}

fn parse_affinity_timestamp(value: &str) -> Result<DateTime<Utc>, CoreError> {
    Ok(storage::from_text(0, value, DateTime::parse_from_rfc3339)?.with_timezone(&Utc))
}

struct FeedbackSession {
    raw_query_text: String,
    effective_query_text: String,
    query_embedding: Vec<u8>,
    embedding_model: String,
    embedding_dim: i64,
    expires_at: DateTime<Utc>,
}

struct ValidatedFeedbackTarget {
    entry_id: Ulid,
    block_id: Option<Ulid>,
    chunk_id: Option<Ulid>,
}

fn validate_feedback_target(
    tx: &Transaction<'_>,
    search_id: &str,
    target: &FeedbackTarget,
) -> Result<ValidatedFeedbackTarget, CoreError> {
    let entry_id = target.entry_id.to_string();
    let recorded = tx
        .query_row(
            "SELECT matched_block_id, matched_chunk_id
             FROM search_session_results
             WHERE search_id = ?1 AND entry_id = ?2",
            params![search_id, &entry_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )
        .optional()?;
    let Some((recorded_block_id, recorded_chunk_id)) = recorded else {
        return Err(CoreError::Validation(format!(
            "entry was not returned by search session: {}",
            target.entry_id
        )));
    };

    let entry_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM entries WHERE id = ?1)",
        [&entry_id],
        |row| row.get(0),
    )?;
    if !entry_exists {
        return Err(CoreError::ResourceNotFound {
            resource: "entry",
            id: target.entry_id,
        });
    }

    let supplied_block_id = target.block_id.map(|id| id.to_string());
    if let Some(block_id) = supplied_block_id.as_deref() {
        if recorded_block_id.as_deref() != Some(block_id) {
            return Err(CoreError::Validation(format!(
                "feedback block does not match search result: {}",
                target.entry_id
            )));
        }
        let block_is_owned: bool = tx.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM blocks WHERE id = ?1 AND entry_id = ?2
             )",
            params![block_id, &entry_id],
            |row| row.get(0),
        )?;
        if !block_is_owned {
            return Err(CoreError::Validation(format!(
                "feedback block does not belong to entry: {}",
                target.entry_id
            )));
        }
    }

    let supplied_chunk_id = target.chunk_id.map(|id| id.to_string());
    let canonical_block_id = if let Some(chunk_id) = supplied_chunk_id.as_deref() {
        if recorded_chunk_id.as_deref() != Some(chunk_id) {
            return Err(CoreError::Validation(format!(
                "feedback chunk does not match search result: {}",
                target.entry_id
            )));
        }
        let current_parent = tx
            .query_row(
                "SELECT chunk.block_id
                 FROM chunks chunk
                 JOIN blocks block ON block.id = chunk.block_id
                 WHERE chunk.id = ?1 AND block.entry_id = ?2",
                params![chunk_id, &entry_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(current_parent) = current_parent else {
            return Err(CoreError::Validation(format!(
                "feedback chunk does not belong to entry block: {}",
                target.entry_id
            )));
        };
        if supplied_block_id
            .as_deref()
            .is_some_and(|block_id| block_id != current_parent)
        {
            return Err(CoreError::Validation(format!(
                "feedback chunk does not belong to matching block: {}",
                target.entry_id
            )));
        }
        Some(storage::from_text(0, &current_parent, Ulid::from_string)?)
    } else {
        target.block_id
    };

    Ok(ValidatedFeedbackTarget {
        entry_id: target.entry_id,
        block_id: canonical_block_id,
        chunk_id: target.chunk_id,
    })
}

fn purge_expired_sessions_in_tx(
    tx: &Transaction<'_>,
    now: &str,
    limit: u64,
    excluded_search_id: Option<&str>,
) -> Result<u64, CoreError> {
    if limit == 0 {
        return Ok(0);
    }
    let deleted = tx.execute(
        "DELETE FROM search_sessions
         WHERE id IN (
             SELECT id FROM search_sessions
             WHERE expires_at <= ?1 AND (?2 IS NULL OR id != ?2)
             ORDER BY expires_at ASC, created_at ASC, id ASC
             LIMIT ?3
         )",
        params![now, excluded_search_id, limit],
    )?;
    Ok(deleted as u64)
}

struct RawAffinity {
    affinity_id: String,
    entry_id: Ulid,
    block_id: Option<Ulid>,
    chunk_id: Option<Ulid>,
    reinforcement_count: u8,
    last_reinforced_at: DateTime<Utc>,
    similarity: f64,
}

fn raw_affinity_from_row(
    row: &rusqlite::Row<'_>,
    similarity: f64,
) -> rusqlite::Result<RawAffinity> {
    let affinity_id: String = row.get(0)?;
    let entry_id: String = row.get(1)?;
    let block_id: Option<String> = row.get(2)?;
    let chunk_id: Option<String> = row.get(3)?;
    let last_reinforced_at: String = row.get(5)?;
    Ok(RawAffinity {
        affinity_id,
        entry_id: storage::from_text(1, &entry_id, Ulid::from_string)?,
        block_id: block_id
            .as_deref()
            .map(|id| storage::from_text(2, id, Ulid::from_string))
            .transpose()?,
        chunk_id: chunk_id
            .as_deref()
            .map(|id| storage::from_text(3, id, Ulid::from_string))
            .transpose()?,
        reinforcement_count: row.get(4)?,
        last_reinforced_at: storage::from_text(
            5,
            &last_reinforced_at,
            DateTime::parse_from_rfc3339,
        )?
        .with_timezone(&Utc),
        similarity,
    })
}

fn decode_vec0_f32(bytes: &[u8]) -> rusqlite::Result<Vec<f32>> {
    if !bytes.len().is_multiple_of(std::mem::size_of::<f32>()) {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            6,
            rusqlite::types::Type::Blob,
            "vec0 float32 blob length is not divisible by four".into(),
        ));
    }
    Ok(bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("four-byte chunk")))
        .collect())
}

fn cosine_similarity(query: &[f32], candidate: &[f32]) -> Option<f64> {
    if query.len() != candidate.len() {
        return None;
    }
    let (mut dot, mut query_norm, mut candidate_norm) = (0.0, 0.0, 0.0);
    for (&query_value, &candidate_value) in query.iter().zip(candidate) {
        let query_value = f64::from(query_value);
        let candidate_value = f64::from(candidate_value);
        if !query_value.is_finite() || !candidate_value.is_finite() {
            return None;
        }
        dot += query_value * candidate_value;
        query_norm += query_value * query_value;
        candidate_norm += candidate_value * candidate_value;
    }
    let denominator = query_norm.sqrt() * candidate_norm.sqrt();
    (denominator > 0.0).then(|| (dot / denominator).clamp(-1.0, 1.0))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, TryLockError};

    use chrono::{DateTime, Utc};
    use rusqlite::{Connection, params};
    use ulid::Ulid;

    use super::decode_vec0_f32;
    use crate::{
        BlockInput, Clock, CoreError, CreateEntry, CreateSearchSession, DimReconciliation, Entry,
        EntryService, FeedbackTarget, MemoryPolicy, MemorySignalsService, SearchResultTarget,
    };

    type StoredFeedbackAffinity = (
        String,
        String,
        String,
        String,
        i64,
        String,
        Option<String>,
        Option<String>,
        Vec<u8>,
    );

    #[derive(Clone)]
    struct FakeClock(Arc<Mutex<DateTime<Utc>>>);

    impl Clock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    impl FakeClock {
        fn set(&self, now: DateTime<Utc>) {
            *self.0.lock().unwrap() = now;
        }

        fn advance_days(&self, days: i64) {
            *self.0.lock().unwrap() += chrono::Duration::days(days);
        }

        fn advance_hours(&self, hours: i64) {
            *self.0.lock().unwrap() += chrono::Duration::hours(hours);
        }
    }

    #[derive(Clone)]
    struct LockObservingClock {
        clock: FakeClock,
        conn: Arc<Mutex<Connection>>,
        sampled_with_connection_locked: Arc<AtomicBool>,
    }

    impl Clock for LockObservingClock {
        fn now(&self) -> DateTime<Utc> {
            let connection_is_locked = match self.conn.try_lock() {
                Err(TryLockError::WouldBlock) => true,
                Ok(guard) => {
                    drop(guard);
                    false
                }
                Err(TryLockError::Poisoned(_)) => panic!("connection mutex poisoned"),
            };
            self.sampled_with_connection_locked
                .store(connection_is_locked, Ordering::SeqCst);
            self.clock.now()
        }
    }

    fn test_service() -> (EntryService, MemorySignalsService, FakeClock) {
        test_service_with_policy(MemoryPolicy::default())
    }

    fn test_service_with_policy(
        policy: MemoryPolicy,
    ) -> (EntryService, MemorySignalsService, FakeClock) {
        let entries = EntryService::for_test().unwrap();
        let clock = FakeClock(Arc::new(Mutex::new(Utc::now())));
        let memory =
            MemorySignalsService::new(entries.conn_for_test(), policy, Arc::new(clock.clone()))
                .unwrap();
        memory.ensure_vec_query_affinities(4).unwrap();
        (entries, memory, clock)
    }

    fn policy_with_cap(max_reinforcements: u8) -> MemoryPolicy {
        let mut policy = MemoryPolicy {
            max_reinforcements,
            ..MemoryPolicy::default()
        };
        match max_reinforcements {
            1 => {
                policy.entry_reinforcement_factors = vec![1.0, 1.1];
                policy.affinity_reinforcement_factors = vec![0.0, 0.7];
            }
            2 => {
                policy.entry_reinforcement_factors = vec![1.0, 1.1, 1.17];
                policy.affinity_reinforcement_factors = vec![0.0, 0.7, 0.9];
            }
            _ => {}
        }
        policy
    }

    fn seed_note(entries: &EntryService, title: &str, body: &str, tags: &[&str]) -> Entry {
        entries
            .create(CreateEntry {
                title: title.into(),
                blocks: vec![BlockInput {
                    r#type: "note".into(),
                    text: body.into(),
                    attrs: None,
                }],
                tags: Some(tags.iter().map(|tag| (*tag).into()).collect()),
                attrs: None,
                source: None,
                attachments: None,
            })
            .unwrap()
    }

    fn insert_affinity_fixture(
        conn: &Arc<Mutex<Connection>>,
        entry_id: Ulid,
        block_id: Ulid,
        embedding_model: &str,
        embedding: &[f32],
        now: DateTime<Utc>,
    ) -> Ulid {
        insert_affinity_fixture_with_targets(
            conn,
            entry_id,
            Some(block_id),
            None,
            embedding_model,
            embedding,
            now,
        )
    }

    fn insert_affinity_fixture_with_targets(
        conn: &Arc<Mutex<Connection>>,
        entry_id: Ulid,
        block_id: Option<Ulid>,
        chunk_id: Option<Ulid>,
        embedding_model: &str,
        embedding: &[f32],
        now: DateTime<Utc>,
    ) -> Ulid {
        let affinity_id = Ulid::new();
        let bytes: Vec<u8> = embedding.iter().flat_map(|f| f.to_le_bytes()).collect();
        let c = conn.lock().unwrap();
        c.execute(
            "INSERT INTO query_affinities (
                id, normalized_query_hash, raw_query_text, effective_query_text,
                embedding_model, embedding_dim, entry_id, block_id, chunk_id,
                reinforcement_count, last_reinforced_at, created_at, updated_at
             ) VALUES (?1, ?2, 'raw', 'effective', ?3, ?4, ?5, ?6, ?7, 1, ?8, ?8, ?8)",
            params![
                affinity_id.to_string(),
                format!("fixture-{affinity_id}"),
                embedding_model,
                embedding.len() as i64,
                entry_id.to_string(),
                block_id.map(|id| id.to_string()),
                chunk_id.map(|id| id.to_string()),
                now.to_rfc3339(),
            ],
        )
        .unwrap();
        c.execute(
            "INSERT INTO vec_query_affinities (affinity_id, embedding) VALUES (?1, ?2)",
            params![affinity_id.to_string(), bytes],
        )
        .unwrap();
        affinity_id
    }

    fn insert_many_affinity_fixtures(
        conn: &Arc<Mutex<Connection>>,
        entry_id: Ulid,
        block_id: Ulid,
        embedding_model: &str,
        embedding: &[f32],
        now: DateTime<Utc>,
        count: usize,
    ) {
        let bytes: Vec<u8> = embedding.iter().flat_map(|f| f.to_le_bytes()).collect();
        let mut c = conn.lock().unwrap();
        let tx = c.transaction().unwrap();
        for _ in 0..count {
            let affinity_id = Ulid::new();
            tx.execute(
                "INSERT INTO query_affinities (
                    id, normalized_query_hash, raw_query_text, effective_query_text,
                    embedding_model, embedding_dim, entry_id, block_id, chunk_id,
                    reinforcement_count, last_reinforced_at, created_at, updated_at
                 ) VALUES (?1, ?2, 'raw', 'effective', ?3, ?4, ?5, ?6, NULL, 1, ?7, ?7, ?7)",
                params![
                    affinity_id.to_string(),
                    format!("fixture-{affinity_id}"),
                    embedding_model,
                    embedding.len() as i64,
                    entry_id.to_string(),
                    block_id.to_string(),
                    now.to_rfc3339(),
                ],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO vec_query_affinities (affinity_id, embedding) VALUES (?1, ?2)",
                params![affinity_id.to_string(), bytes],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    struct ReembeddingAffinityFixture<'a> {
        affinity_id: Ulid,
        normalized_query_hash: &'a str,
        raw_query_text: &'a str,
        effective_query_text: &'a str,
        embedding_model: &'a str,
        entry_id: Ulid,
        block_id: Option<Ulid>,
        chunk_id: Option<Ulid>,
        reinforcement_count: u8,
        last_reinforced_at: &'a str,
        created_at: &'a str,
        updated_at: &'a str,
        embedding: &'a [f32],
    }

    #[derive(Debug, PartialEq, Eq)]
    struct StoredReembeddingAffinity {
        id: String,
        normalized_query_hash: String,
        raw_query_text: String,
        effective_query_text: String,
        embedding_model: String,
        embedding_dim: i64,
        entry_id: String,
        block_id: Option<String>,
        chunk_id: Option<String>,
        reinforcement_count: i64,
        last_reinforced_at: String,
        created_at: String,
        updated_at: String,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ReembeddingStorageSnapshot {
        affinities: Vec<StoredReembeddingAffinity>,
        vectors: Vec<(String, Vec<u8>)>,
    }

    fn insert_reembedding_affinity(
        conn: &Arc<Mutex<Connection>>,
        fixture: ReembeddingAffinityFixture<'_>,
    ) {
        let bytes = fixture
            .embedding
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        let conn = conn.lock().unwrap();
        conn.execute(
            "INSERT INTO query_affinities (
                id, normalized_query_hash, raw_query_text, effective_query_text,
                embedding_model, embedding_dim, entry_id, block_id, chunk_id,
                reinforcement_count, last_reinforced_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, 4, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                fixture.affinity_id.to_string(),
                fixture.normalized_query_hash,
                fixture.raw_query_text,
                fixture.effective_query_text,
                fixture.embedding_model,
                fixture.entry_id.to_string(),
                fixture.block_id.map(|id| id.to_string()),
                fixture.chunk_id.map(|id| id.to_string()),
                fixture.reinforcement_count,
                fixture.last_reinforced_at,
                fixture.created_at,
                fixture.updated_at,
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO vec_query_affinities (affinity_id, embedding) VALUES (?1, ?2)",
            params![fixture.affinity_id.to_string(), bytes],
        )
        .unwrap();
    }

    fn reembedding_storage_snapshot(conn: &Arc<Mutex<Connection>>) -> ReembeddingStorageSnapshot {
        let conn = conn.lock().unwrap();
        let affinities = conn
            .prepare(
                "SELECT id, normalized_query_hash, raw_query_text,
                        effective_query_text, embedding_model, embedding_dim,
                        entry_id, block_id, chunk_id, reinforcement_count,
                        last_reinforced_at, created_at, updated_at
                 FROM query_affinities ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok(StoredReembeddingAffinity {
                    id: row.get(0)?,
                    normalized_query_hash: row.get(1)?,
                    raw_query_text: row.get(2)?,
                    effective_query_text: row.get(3)?,
                    embedding_model: row.get(4)?,
                    embedding_dim: row.get(5)?,
                    entry_id: row.get(6)?,
                    block_id: row.get(7)?,
                    chunk_id: row.get(8)?,
                    reinforcement_count: row.get(9)?,
                    last_reinforced_at: row.get(10)?,
                    created_at: row.get(11)?,
                    updated_at: row.get(12)?,
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let vectors = conn
            .prepare("SELECT affinity_id, embedding FROM vec_query_affinities ORDER BY affinity_id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        ReembeddingStorageSnapshot {
            affinities,
            vectors,
        }
    }

    fn first_chunk_id(conn: &Arc<Mutex<Connection>>, block_id: Ulid) -> Ulid {
        let c = conn.lock().unwrap();
        c.query_row(
            "SELECT id FROM chunks WHERE block_id = ?1 ORDER BY ordinal LIMIT 1",
            [block_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .map(|id| Ulid::from_string(&id).unwrap())
        .unwrap()
    }

    struct FeedbackFixture {
        entries: EntryService,
        memory: MemorySignalsService,
        clock: FakeClock,
        ids: Vec<(Ulid, Ulid, Ulid)>,
    }

    impl FeedbackFixture {
        fn new(entry_count: usize) -> Self {
            Self::with_policy(entry_count, MemoryPolicy::default())
        }

        fn with_policy(entry_count: usize, policy: MemoryPolicy) -> Self {
            let (entries, memory, clock) = test_service_with_policy(policy);
            let ids = (0..entry_count)
                .map(|index| {
                    let entry = seed_note(
                        &entries,
                        &format!("target-{index}"),
                        &format!("body-{index}"),
                        &[],
                    );
                    let block_id = entry.blocks[0].id;
                    let chunk_id = first_chunk_id(&entries.conn_for_test(), block_id);
                    (entry.id, block_id, chunk_id)
                })
                .collect();
            Self {
                entries,
                memory,
                clock,
                ids,
            }
        }

        fn entry_id(&self, index: usize) -> Ulid {
            self.ids[index].0
        }

        fn block_id(&self, index: usize) -> Ulid {
            self.ids[index].1
        }

        fn chunk_id(&self, index: usize) -> Ulid {
            self.ids[index].2
        }

        fn target(&self, index: usize) -> FeedbackTarget {
            FeedbackTarget {
                entry_id: self.entry_id(index),
                block_id: Some(self.block_id(index)),
                chunk_id: Some(self.chunk_id(index)),
            }
        }

        fn new_session(&self, returned_entry_indexes: &[usize]) -> Ulid {
            let nonce = Ulid::new();
            self.new_session_with_query(
                returned_entry_indexes,
                &format!("  Raw   Query {nonce}  "),
                "  Effective   Query  ",
                vec![1.0, 0.0, 0.0, 0.0],
            )
        }

        fn new_session_with_query(
            &self,
            returned_entry_indexes: &[usize],
            raw_query_text: &str,
            effective_query_text: &str,
            query_embedding: Vec<f32>,
        ) -> Ulid {
            self.memory
                .create_search_session(CreateSearchSession {
                    raw_query_text: raw_query_text.into(),
                    effective_query_text: effective_query_text.into(),
                    query_embedding,
                    embedding_model: "model-a".into(),
                    results: returned_entry_indexes
                        .iter()
                        .enumerate()
                        .map(|(rank, &index)| SearchResultTarget {
                            entry_id: self.entry_id(index),
                            matched_block_id: Some(self.block_id(index)),
                            matched_chunk_id: Some(self.chunk_id(index)),
                            result_rank: rank as u32 + 1,
                        })
                        .collect(),
                })
                .unwrap()
        }

        fn affinity_row_count(&self) -> i64 {
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM query_affinities", [], |row| {
                    row.get(0)
                })
                .unwrap()
        }

        fn affinity_vector_row_count(&self) -> i64 {
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM vec_query_affinities", [], |row| {
                    row.get(0)
                })
                .unwrap()
        }

        fn feedback_row_count(&self) -> i64 {
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM search_feedback", [], |row| row.get(0))
                .unwrap()
        }

        fn entry_stats_row_count(&self) -> i64 {
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM entry_memory_stats", [], |row| {
                    row.get(0)
                })
                .unwrap()
        }

        fn signal_row_counts(&self, index: usize) -> (i64, i64, i64, i64, i64) {
            let entry_id = self.entry_id(index).to_string();
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row(
                    "SELECT
                        (SELECT COUNT(*) FROM entry_memory_stats WHERE entry_id = ?1),
                        (SELECT COUNT(*) FROM query_affinities WHERE entry_id = ?1),
                        (SELECT COUNT(*) FROM search_feedback WHERE entry_id = ?1),
                        (SELECT COUNT(*) FROM search_session_results WHERE entry_id = ?1),
                        (SELECT COUNT(*)
                           FROM vec_query_affinities vec
                           JOIN query_affinities affinity ON affinity.id = vec.affinity_id
                          WHERE affinity.entry_id = ?1)",
                    [&entry_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .unwrap()
        }

        fn session_row_count(&self) -> i64 {
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM search_sessions", [], |row| row.get(0))
                .unwrap()
        }

        fn session_result_row_count(&self) -> i64 {
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM search_session_results", [], |row| {
                    row.get(0)
                })
                .unwrap()
        }

        fn session_expiry(&self, search_id: Ulid) -> DateTime<Utc> {
            let expires_at: String = self
                .entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row(
                    "SELECT expires_at FROM search_sessions WHERE id = ?1",
                    [search_id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            DateTime::parse_from_rfc3339(&expires_at)
                .unwrap()
                .with_timezone(&Utc)
        }

        fn expired_session_count(&self) -> i64 {
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM search_sessions WHERE expires_at <= ?1",
                    [self.clock.now().to_rfc3339()],
                    |row| row.get(0),
                )
                .unwrap()
        }

        fn seed_expired_sessions(&self, count: usize) -> Vec<Ulid> {
            let now = self.clock.now();
            let entry_id = self.entry_id(0).to_string();
            let shared_conn = self.entries.conn_for_test();
            let mut conn = shared_conn.lock().unwrap();
            let tx = conn.transaction().unwrap();
            let mut ids = Vec::with_capacity(count);
            for index in 0..count {
                let id = Ulid::new();
                let expires_at = now - chrono::Duration::minutes((count - index) as i64);
                let created_at = expires_at - chrono::Duration::hours(1);
                tx.execute(
                    "INSERT INTO search_sessions (
                        id, raw_query_text, effective_query_text, query_embedding,
                        embedding_model, embedding_dim, created_at, expires_at
                     ) VALUES (?1, 'expired raw', 'expired effective', ?2,
                               'model-a', 4, ?3, ?4)",
                    params![
                        id.to_string(),
                        vec![0_u8; 16],
                        created_at.to_rfc3339(),
                        expires_at.to_rfc3339(),
                    ],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO search_session_results (
                        search_id, entry_id, matched_block_id, matched_chunk_id, result_rank
                     ) VALUES (?1, ?2, NULL, NULL, 1)",
                    params![id.to_string(), &entry_id],
                )
                .unwrap();
                ids.push(id);
            }
            tx.commit().unwrap();
            ids
        }

        fn insert_second_chunk(&self, index: usize) -> Ulid {
            let chunk_id = Ulid::new();
            let now = self.clock.now().to_rfc3339();
            self.entries
                .conn_for_test()
                .lock()
                .unwrap()
                .execute(
                    "INSERT INTO chunks (
                        id, block_id, ordinal, text, attrs, created_at, updated_at
                     ) VALUES (?1, ?2, 1, 'second chunk', '{}', ?3, ?3)",
                    params![chunk_id.to_string(), self.block_id(index).to_string(), now],
                )
                .unwrap();
            chunk_id
        }
    }

    #[test]
    fn memory_service_rejects_invalid_memory_policies() {
        let invalid_cap = MemoryPolicy {
            max_reinforcements: 4,
            ..MemoryPolicy::default()
        };
        let mut invalid_length = policy_with_cap(2);
        invalid_length.entry_reinforcement_factors.pop();
        let mut invalid_order = policy_with_cap(2);
        invalid_order.affinity_reinforcement_factors = vec![0.0, 0.9, 0.8];

        for policy in [invalid_cap, invalid_length, invalid_order] {
            let entries = EntryService::for_test().unwrap();
            let clock = FakeClock(Arc::new(Mutex::new(Utc::now())));
            assert!(matches!(
                MemorySignalsService::new(entries.conn_for_test(), policy, Arc::new(clock)),
                Err(CoreError::Validation(_))
            ));
        }
    }

    #[test]
    fn feedback_updates_entry_and_exact_affinity_once() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        let result = f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        assert_eq!(result.applied.len(), 1);
        assert_eq!(result.applied[0].reinforcement_count, 1);
        assert_eq!(result.applied[0].affinity_count, 1);
        assert!(result.already_applied.is_empty());
    }

    #[test]
    fn feedback_session_creation_is_neutral_until_feedback_is_accepted() {
        let f = FeedbackFixture::new(1);
        f.new_session(&[0]);

        assert_eq!(f.feedback_row_count(), 0);
        assert_eq!(f.entry_stats_row_count(), 0);
        assert_eq!(f.affinity_row_count(), 0);
    }

    #[test]
    fn feedback_accepts_entry_only_and_derives_chunk_ownership_without_block() {
        let entry_only = FeedbackFixture::new(1);
        let search_id = entry_only.new_session(&[0]);
        entry_only
            .memory
            .apply_feedback(
                search_id,
                &[FeedbackTarget {
                    entry_id: entry_only.entry_id(0),
                    block_id: None,
                    chunk_id: None,
                }],
            )
            .unwrap();
        let entry_precision: (Option<String>, Option<String>) = entry_only
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(entry_precision, (None, None));

        let block_only = FeedbackFixture::new(1);
        let search_id = block_only.new_session(&[0]);
        block_only
            .memory
            .apply_feedback(
                search_id,
                &[FeedbackTarget {
                    entry_id: block_only.entry_id(0),
                    block_id: Some(block_only.block_id(0)),
                    chunk_id: None,
                }],
            )
            .unwrap();
        let block_precision: (Option<String>, Option<String>) = block_only
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            block_precision,
            (Some(block_only.block_id(0).to_string()), None)
        );

        let chunk_only = FeedbackFixture::new(1);
        let search_id = chunk_only.new_session(&[0]);
        chunk_only
            .memory
            .apply_feedback(
                search_id,
                &[FeedbackTarget {
                    entry_id: chunk_only.entry_id(0),
                    block_id: None,
                    chunk_id: Some(chunk_only.chunk_id(0)),
                }],
            )
            .unwrap();
        let chunk_precision: (Option<String>, Option<String>) = chunk_only
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            chunk_precision,
            (
                Some(chunk_only.block_id(0).to_string()),
                Some(chunk_only.chunk_id(0).to_string())
            )
        );
    }

    #[test]
    fn feedback_normalizes_effective_query_and_persists_session_vector() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session_with_query(
            &[0],
            "  Original Raw Query  ",
            "  EFFECTIVE   Query  ",
            vec![1.0, 0.0, 0.0, 0.0],
        );
        f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        let conn = f.entries.conn_for_test();
        let stored: StoredFeedbackAffinity = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT affinity.normalized_query_hash, affinity.raw_query_text,
                        affinity.effective_query_text, affinity.embedding_model,
                        affinity.embedding_dim, affinity.entry_id, affinity.block_id,
                        affinity.chunk_id, vec.embedding
                 FROM query_affinities affinity
                 JOIN vec_query_affinities vec ON vec.affinity_id = affinity.id",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(stored.0, blake3::hash(b"effective query").to_hex().as_str());
        assert_eq!(stored.1, "  Original Raw Query  ");
        assert_eq!(stored.2, "  EFFECTIVE   Query  ");
        assert_eq!(stored.3, "model-a");
        assert_eq!(stored.4, 4);
        assert_eq!(stored.5, f.entry_id(0).to_string());
        assert_eq!(stored.6, Some(f.block_id(0).to_string()));
        assert_eq!(stored.7, Some(f.chunk_id(0).to_string()));
        assert_eq!(
            stored.8,
            [1.0f32, 0.0, 0.0, 0.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn same_search_and_entry_is_idempotent_without_refreshing_time() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        let first = f.memory.entry_memory_signals(&[f.entry_id(0)]).unwrap();
        let first_materialized: (String, String, i64, Vec<u8>) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT feedback.created_at, affinity.last_reinforced_at,
                        affinity.reinforcement_count, vec.embedding
                 FROM search_feedback feedback
                 JOIN query_affinities affinity ON affinity.entry_id = feedback.entry_id
                 JOIN vec_query_affinities vec ON vec.affinity_id = affinity.id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        f.clock.advance_hours(1);
        let retry = f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        let second = f.memory.entry_memory_signals(&[f.entry_id(0)]).unwrap();
        assert!(retry.applied.is_empty());
        assert_eq!(retry.already_applied, vec![f.entry_id(0)]);
        assert_eq!(
            first[&f.entry_id(0)].last_reinforced_at,
            second[&f.entry_id(0)].last_reinforced_at
        );
        let second_materialized: (String, String, i64, Vec<u8>) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT feedback.created_at, affinity.last_reinforced_at,
                        affinity.reinforcement_count, vec.embedding
                 FROM search_feedback feedback
                 JOIN query_affinities affinity ON affinity.entry_id = feedback.entry_id
                 JOIN vec_query_affinities vec ON vec.affinity_id = affinity.id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(first_materialized, second_materialized);
    }

    #[test]
    fn feedback_retry_after_reindex_precision_reconciliation_is_already_applied() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        let target = f.target(0);
        f.memory
            .apply_feedback(search_id, std::slice::from_ref(&target))
            .unwrap();
        f.entries
            .conn_for_test()
            .lock()
            .unwrap()
            .execute(
                "DELETE FROM blocks WHERE id = ?1",
                [f.block_id(0).to_string()],
            )
            .unwrap();
        f.memory.reconcile_content_references().unwrap();

        let retry = f.memory.apply_feedback(search_id, &[target]).unwrap();

        assert!(retry.applied.is_empty());
        assert_eq!(retry.already_applied, vec![f.entry_id(0)]);
        assert_eq!(f.feedback_row_count(), 1);
    }

    #[test]
    fn feedback_retry_after_block_ownership_mutation_is_already_applied() {
        let f = FeedbackFixture::new(2);
        let search_id = f.new_session(&[0]);
        let target = f.target(0);
        f.memory
            .apply_feedback(search_id, std::slice::from_ref(&target))
            .unwrap();
        let conn = f.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(&format!(
                "UPDATE blocks SET ordinal = 1 WHERE id = '{}';
                 UPDATE blocks SET entry_id = '{}' WHERE id = '{}';",
                f.block_id(1),
                f.entry_id(1),
                f.block_id(0),
            ))
            .unwrap();

        let retry = f.memory.apply_feedback(search_id, &[target]).unwrap();

        assert!(retry.applied.is_empty());
        assert_eq!(retry.already_applied, vec![f.entry_id(0)]);
        assert_eq!(f.feedback_row_count(), 1);
    }

    #[test]
    fn fourth_distinct_feedback_refreshes_time_but_count_stays_three() {
        let f = FeedbackFixture::new(1);
        for _ in 0..3 {
            let search_id = f.new_session(&[0]);
            f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        }
        let before = f.memory.entry_memory_signals(&[f.entry_id(0)]).unwrap();
        f.clock.advance_days(1);
        let search_id = f.new_session(&[0]);
        let fourth = f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        assert_eq!(fourth.applied[0].reinforcement_count, 3);
        assert_eq!(fourth.applied[0].affinity_count, 3);
        assert!(fourth.applied[0].last_reinforced_at > before[&f.entry_id(0)].last_reinforced_at);
        let affinity: (u8, String) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT reinforcement_count, last_reinforced_at FROM query_affinities",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(affinity.0, 3);
        assert_eq!(
            affinity.1,
            fourth.applied[0].last_reinforced_at.to_rfc3339()
        );
    }

    #[test]
    fn fourth_post_cap_feedback_persists_distinct_vector_and_reloads_latest_timestamps() {
        let f = FeedbackFixture::new(1);
        for index in 0..3 {
            let search_id = f.new_session_with_query(
                &[0],
                &format!("raw capped query {index}"),
                "same capped query",
                vec![1.0, 0.0, 0.0, 0.0],
            );
            f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        }
        f.clock.advance_hours(1);
        let fourth_search = f.new_session_with_query(
            &[0],
            "raw capped query fourth",
            "same capped query",
            vec![0.0, 1.0, 0.0, 0.0],
        );
        let fourth = f
            .memory
            .apply_feedback(fourth_search, &[f.target(0)])
            .unwrap();
        let expected_timestamp = fourth.applied[0].last_reinforced_at;
        let conn = f.entries.conn_for_test();

        let reopened = MemorySignalsService::new(
            conn.clone(),
            MemoryPolicy::default(),
            Arc::new(f.clock.clone()),
        )
        .unwrap();
        let entry_signal = reopened.entry_memory_signals(&[f.entry_id(0)]).unwrap();
        assert_eq!(entry_signal[&f.entry_id(0)].reinforcement_count, 3);
        assert_eq!(
            entry_signal[&f.entry_id(0)].last_reinforced_at,
            expected_timestamp
        );
        let stored: (u8, String, Vec<u8>) = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT affinity.reinforcement_count, affinity.last_reinforced_at,
                        vec.embedding
                 FROM query_affinities affinity
                 JOIN vec_query_affinities vec ON vec.affinity_id = affinity.id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored.0, 3);
        assert_eq!(stored.1, expected_timestamp.to_rfc3339());
        assert_eq!(
            decode_vec0_f32(&stored.2).unwrap(),
            vec![0.0, 1.0, 0.0, 0.0]
        );
        assert_eq!(
            reopened
                .affinity_candidates(&[0.0, 1.0, 0.0, 0.0], "model-a", 1, None, None, false,)
                .unwrap()
                .len(),
            1
        );
        assert!(
            reopened
                .affinity_candidates(&[1.0, 0.0, 0.0, 0.0], "model-a", 1, None, None, false,)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn feedback_respects_max_one_policy() {
        let f = FeedbackFixture::with_policy(1, policy_with_cap(1));
        let mut latest = None;
        for _ in 0..3 {
            let search_id = f.new_session(&[0]);
            latest = Some(f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap());
        }

        let latest = latest.unwrap();
        assert_eq!(latest.applied[0].reinforcement_count, 1);
        assert_eq!(latest.applied[0].affinity_count, 1);
        let counts: (u8, u8) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT stats.reinforcement_count, affinity.reinforcement_count
                 FROM entry_memory_stats stats
                 JOIN query_affinities affinity ON affinity.entry_id = stats.entry_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 1));
    }

    #[test]
    fn feedback_respects_max_two_policy() {
        let f = FeedbackFixture::with_policy(1, policy_with_cap(2));
        let mut latest = None;
        for _ in 0..3 {
            let search_id = f.new_session(&[0]);
            latest = Some(f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap());
        }

        let latest = latest.unwrap();
        assert_eq!(latest.applied[0].reinforcement_count, 2);
        assert_eq!(latest.applied[0].affinity_count, 2);
        let counts: (u8, u8) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT stats.reinforcement_count, affinity.reinforcement_count
                 FROM entry_memory_stats stats
                 JOIN query_affinities affinity ON affinity.entry_id = stats.entry_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (2, 2));
    }

    #[test]
    fn feedback_never_decrements_historical_counts_when_policy_cap_is_lowered() {
        let f = FeedbackFixture::new(1);
        for _ in 0..3 {
            let search_id = f.new_session(&[0]);
            f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        }
        let lower_cap_memory = MemorySignalsService::new(
            f.entries.conn_for_test(),
            policy_with_cap(1),
            Arc::new(f.clock.clone()),
        )
        .unwrap();
        lower_cap_memory.ensure_vec_query_affinities(4).unwrap();
        let search_id = f.new_session(&[0]);

        let result = lower_cap_memory
            .apply_feedback(search_id, &[f.target(0)])
            .unwrap();

        assert_eq!(result.applied[0].reinforcement_count, 3);
        assert_eq!(result.applied[0].affinity_count, 3);
        let counts: (u8, u8) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT stats.reinforcement_count, affinity.reinforcement_count
                 FROM entry_memory_stats stats
                 JOIN query_affinities affinity ON affinity.entry_id = stats.entry_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (3, 3));
    }

    #[test]
    fn feedback_refreshes_existing_affinity_from_latest_session() {
        let f = FeedbackFixture::new(1);
        let first = f.new_session_with_query(
            &[0],
            "first raw",
            " Same   Effective Query ",
            vec![1.0, 0.0, 0.0, 0.0],
        );
        f.memory.apply_feedback(first, &[f.target(0)]).unwrap();
        f.clock.advance_hours(1);
        let second = f.new_session_with_query(
            &[0],
            "second raw",
            "same effective query",
            vec![0.0, 1.0, 0.0, 0.0],
        );
        let result = f.memory.apply_feedback(second, &[f.target(0)]).unwrap();

        let stored: (String, u8, String, Vec<u8>) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT affinity.raw_query_text, affinity.reinforcement_count,
                        affinity.last_reinforced_at, vec.embedding
                 FROM query_affinities affinity
                 JOIN vec_query_affinities vec ON vec.affinity_id = affinity.id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(f.affinity_row_count(), 1);
        assert_eq!(stored.0, "second raw");
        assert_eq!(stored.1, 2);
        assert_eq!(stored.2, result.applied[0].last_reinforced_at.to_rfc3339());
        assert_eq!(
            stored.3,
            [0.0f32, 1.0, 0.0, 0.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn feedback_accepts_multiple_correct_entries_atomically() {
        let f = FeedbackFixture::new(2);
        let search_id = f.new_session(&[0, 1]);
        let result = f
            .memory
            .apply_feedback(search_id, &[f.target(0), f.target(1)])
            .unwrap();
        assert_eq!(result.applied.len(), 2);
        assert_eq!(f.feedback_row_count(), 2);
    }

    #[test]
    fn one_invalid_target_rolls_back_every_target() {
        let f = FeedbackFixture::new(2);
        let search_id = f.new_session(&[0]);
        let error = f
            .memory
            .apply_feedback(search_id, &[f.target(0), f.target(1)])
            .unwrap_err();
        assert!(matches!(error, CoreError::Validation(_)));
        assert_eq!(f.feedback_row_count(), 0);
        assert_eq!(f.entry_stats_row_count(), 0);
    }

    #[test]
    fn expired_session_returns_conflict() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.clock.advance_hours(25);
        assert!(matches!(
            f.memory.apply_feedback(search_id, &[f.target(0)]),
            Err(CoreError::Conflict(_))
        ));
    }

    #[test]
    fn feedback_at_exact_expiry_returns_conflict_without_purging_target_session() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.clock.set(f.session_expiry(search_id));

        assert!(matches!(
            f.memory.apply_feedback(search_id, &[f.target(0)]),
            Err(CoreError::Conflict(_))
        ));
        assert_eq!(f.session_row_count(), 1);
        assert_eq!(f.feedback_row_count(), 0);
        assert_eq!(f.entry_stats_row_count(), 0);
    }

    #[test]
    fn feedback_samples_clock_after_waiting_for_the_connection_transaction() {
        let entries = EntryService::for_test().unwrap();
        let conn = entries.conn_for_test();
        let start: DateTime<Utc> = "2026-09-04T00:00:00Z".parse().unwrap();
        let clock = FakeClock(Arc::new(Mutex::new(start)));
        let sampled_with_connection_locked = Arc::new(AtomicBool::new(false));
        let memory = MemorySignalsService::new(
            conn.clone(),
            MemoryPolicy::default(),
            Arc::new(LockObservingClock {
                clock,
                conn: conn.clone(),
                sampled_with_connection_locked: Arc::clone(&sampled_with_connection_locked),
            }),
        )
        .unwrap();
        memory.ensure_vec_query_affinities(4).unwrap();
        let entry = seed_note(&entries, "target", "body", &[]);
        let block_id = entry.blocks[0].id;
        let chunk_id = first_chunk_id(&conn, block_id);
        let search_id = memory
            .create_search_session(CreateSearchSession {
                raw_query_text: "raw".into(),
                effective_query_text: "effective".into(),
                query_embedding: vec![1.0, 0.0, 0.0, 0.0],
                embedding_model: "model-a".into(),
                results: vec![SearchResultTarget {
                    entry_id: entry.id,
                    matched_block_id: Some(block_id),
                    matched_chunk_id: Some(chunk_id),
                    result_rank: 1,
                }],
            })
            .unwrap();
        sampled_with_connection_locked.store(false, Ordering::SeqCst);

        memory
            .apply_feedback(
                search_id,
                &[FeedbackTarget {
                    entry_id: entry.id,
                    block_id: Some(block_id),
                    chunk_id: Some(chunk_id),
                }],
            )
            .unwrap();

        assert!(sampled_with_connection_locked.load(Ordering::SeqCst));
    }

    #[test]
    fn missing_session_returns_resource_not_found() {
        let f = FeedbackFixture::new(1);
        let search_id = Ulid::new();
        assert!(matches!(
            f.memory.apply_feedback(search_id, &[f.target(0)]),
            Err(CoreError::ResourceNotFound {
                resource: "search session",
                id
            }) if id == search_id
        ));
    }

    #[test]
    fn new_feedback_for_a_deleted_returned_entry_is_resource_not_found() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        let entry_id = f.entry_id(0);
        let target = f.target(0);
        f.entries.delete(entry_id).unwrap();

        assert!(matches!(
            f.memory.apply_feedback(search_id, &[target]),
            Err(CoreError::ResourceNotFound {
                resource: "entry",
                id,
            }) if id == entry_id
        ));
        assert_eq!(f.feedback_row_count(), 0);
        assert_eq!(f.entry_stats_row_count(), 0);
    }

    #[test]
    fn target_must_have_been_returned_and_precise_ids_must_match() {
        let f = FeedbackFixture::new(2);
        let search_id = f.new_session(&[0]);
        assert!(matches!(
            f.memory.apply_feedback(search_id, &[f.target(1)]),
            Err(CoreError::Validation(_))
        ));
        let mut wrong_chunk = f.target(0);
        wrong_chunk.chunk_id = Some(f.chunk_id(1));
        assert!(matches!(
            f.memory.apply_feedback(search_id, &[wrong_chunk]),
            Err(CoreError::Validation(_))
        ));
    }

    #[test]
    fn feedback_precise_target_must_still_belong_to_returned_entry() {
        let f = FeedbackFixture::new(2);
        let search_id = f.new_session(&[0]);
        let conn = f.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        conn.execute(
            "UPDATE blocks SET ordinal = 1 WHERE id = ?1",
            [f.block_id(1).to_string()],
        )
        .unwrap();
        conn.execute(
            "UPDATE blocks SET entry_id = ?1 WHERE id = ?2",
            params![f.entry_id(1).to_string(), f.block_id(0).to_string()],
        )
        .unwrap();
        drop(conn);

        assert!(matches!(
            f.memory.apply_feedback(search_id, &[f.target(0)]),
            Err(CoreError::Validation(_))
        ));
        assert_eq!(f.feedback_row_count(), 0);
        assert_eq!(f.entry_stats_row_count(), 0);
    }

    #[test]
    fn feedback_vector_failure_rolls_back_all_signal_writes_and_transaction() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.memory.ensure_vec_query_affinities(8).unwrap();

        assert!(matches!(
            f.memory.apply_feedback(search_id, &[f.target(0)]),
            Err(CoreError::Storage(_))
        ));
        assert_eq!(f.feedback_row_count(), 0);
        assert_eq!(f.entry_stats_row_count(), 0);
        assert_eq!(f.affinity_row_count(), 0);
        assert_eq!(f.affinity_vector_row_count(), 0);
        assert!(f.entries.conn_for_test().lock().unwrap().is_autocommit());
    }

    #[test]
    fn feedback_rejects_empty_and_duplicate_entry_targets() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        assert!(matches!(
            f.memory.apply_feedback(search_id, &[]),
            Err(CoreError::Validation(_))
        ));
        assert!(matches!(
            f.memory
                .apply_feedback(search_id, &[f.target(0), f.target(0)]),
            Err(CoreError::Validation(_))
        ));
        assert_eq!(f.feedback_row_count(), 0);
    }

    #[test]
    fn cleanup_delete_entry_signals_removes_only_the_selected_entry() {
        let f = FeedbackFixture::new(2);
        let search_id = f.new_session(&[0, 1]);
        f.memory
            .apply_feedback(search_id, &[f.target(0), f.target(1)])
            .unwrap();
        assert_eq!(f.signal_row_counts(0), (1, 1, 1, 1, 1));
        assert_eq!(f.signal_row_counts(1), (1, 1, 1, 1, 1));

        f.memory.delete_entry_signals(f.entry_id(0)).unwrap();

        assert_eq!(f.signal_row_counts(0), (0, 0, 0, 0, 0));
        assert_eq!(f.signal_row_counts(1), (1, 1, 1, 1, 1));
        assert_eq!(f.affinity_vector_row_count(), 1);
    }

    #[test]
    fn cleanup_delete_entry_signals_rolls_back_on_late_failure() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        let conn = f.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(&format!(
                "CREATE TEMP TRIGGER fail_signal_cleanup
                 BEFORE DELETE ON search_session_results
                 WHEN OLD.entry_id = '{}'
                 BEGIN
                    SELECT RAISE(ABORT, 'forced signal cleanup failure');
                 END;",
                f.entry_id(0)
            ))
            .unwrap();

        assert!(matches!(
            f.memory.delete_entry_signals(f.entry_id(0)),
            Err(CoreError::Storage(_))
        ));
        assert_eq!(f.signal_row_counts(0), (1, 1, 1, 1, 1));
        assert_eq!(f.affinity_vector_row_count(), 1);
        assert!(conn.lock().unwrap().is_autocommit());
    }

    #[test]
    fn cleanup_degrade_block_clears_materialized_precision_not_receipt_history() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();

        f.memory
            .degrade_block_precision(f.block_id(0), &[f.chunk_id(0)])
            .unwrap();

        let conn = f.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let affinity: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let session_result: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT matched_block_id, matched_chunk_id FROM search_session_results",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let receipt: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM search_feedback",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(affinity, (None, None));
        assert_eq!(session_result, (None, None));
        assert_eq!(
            receipt,
            (
                Some(f.block_id(0).to_string()),
                Some(f.chunk_id(0).to_string())
            )
        );
    }

    #[test]
    fn cleanup_degrade_block_also_clears_semantic_only_session_chunk_precision() {
        let f = FeedbackFixture::new(1);
        let search_id = f
            .memory
            .create_search_session(CreateSearchSession {
                raw_query_text: "semantic-only raw".into(),
                effective_query_text: "semantic-only query".into(),
                query_embedding: vec![1.0, 0.0, 0.0, 0.0],
                embedding_model: "model-a".into(),
                results: vec![SearchResultTarget {
                    entry_id: f.entry_id(0),
                    matched_block_id: None,
                    matched_chunk_id: Some(f.chunk_id(0)),
                    result_rank: 1,
                }],
            })
            .unwrap();
        f.memory
            .apply_feedback(
                search_id,
                &[FeedbackTarget {
                    entry_id: f.entry_id(0),
                    block_id: None,
                    chunk_id: Some(f.chunk_id(0)),
                }],
            )
            .unwrap();

        f.memory
            .degrade_block_precision(f.block_id(0), &[f.chunk_id(0)])
            .unwrap();

        let conn = f.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let affinity = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .unwrap();
        let session_result = conn
            .query_row(
                "SELECT matched_block_id, matched_chunk_id FROM search_session_results",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(affinity, (None, None));
        assert_eq!(session_result, (None, None));
    }

    #[test]
    fn cleanup_degrade_block_rolls_back_affinity_when_session_update_fails() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        let conn = f.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(
                "CREATE TEMP TRIGGER fail_session_precision_update
                 BEFORE UPDATE ON search_session_results
                 BEGIN
                    SELECT RAISE(ABORT, 'forced precision cleanup failure');
                 END;",
            )
            .unwrap();

        assert!(matches!(
            f.memory
                .degrade_block_precision(f.block_id(0), &[f.chunk_id(0)]),
            Err(CoreError::Storage(_))
        ));
        let precision: (Option<String>, Option<String>) = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            precision,
            (
                Some(f.block_id(0).to_string()),
                Some(f.chunk_id(0).to_string())
            )
        );
        assert_eq!(f.affinity_vector_row_count(), 1);
        assert!(conn.lock().unwrap().is_autocommit());
    }

    #[test]
    fn cleanup_degrade_block_coalesces_an_existing_entry_affinity() {
        let f = FeedbackFixture::new(1);
        let entry_only_session = f.new_session_with_query(
            &[0],
            "older raw",
            "  Effective   Query  ",
            vec![1.0, 0.0, 0.0, 0.0],
        );
        f.memory
            .apply_feedback(
                entry_only_session,
                &[FeedbackTarget {
                    entry_id: f.entry_id(0),
                    block_id: None,
                    chunk_id: None,
                }],
            )
            .unwrap();
        f.clock.advance_hours(1);
        let precise_session = f.new_session_with_query(
            &[0],
            "newer raw",
            "effective query",
            vec![0.0, 1.0, 0.0, 0.0],
        );
        let precise_feedback = f
            .memory
            .apply_feedback(precise_session, &[f.target(0)])
            .unwrap();
        assert_eq!(f.affinity_row_count(), 2);

        f.memory
            .degrade_block_precision(f.block_id(0), &[f.chunk_id(0)])
            .unwrap();

        let affinity: (
            Option<String>,
            Option<String>,
            u8,
            String,
            String,
            String,
            Vec<u8>,
        ) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT affinity.block_id, affinity.chunk_id,
                        affinity.reinforcement_count, affinity.raw_query_text,
                        affinity.effective_query_text, affinity.last_reinforced_at,
                        vec.embedding
                 FROM query_affinities affinity
                 JOIN vec_query_affinities vec ON vec.affinity_id = affinity.id",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(affinity.0, None);
        assert_eq!(affinity.1, None);
        assert_eq!(affinity.2, 1);
        assert_eq!(affinity.3, "newer raw");
        assert_eq!(affinity.4, "effective query");
        assert_eq!(
            affinity.5,
            precise_feedback.applied[0].last_reinforced_at.to_rfc3339()
        );
        assert_eq!(
            affinity.6,
            [0.0f32, 1.0, 0.0, 0.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>()
        );
        assert_eq!(f.affinity_row_count(), 1);
        assert_eq!(f.affinity_vector_row_count(), 1);
        assert_eq!(f.feedback_row_count(), 2);
    }

    #[test]
    fn cleanup_degrade_chunk_clears_only_selected_chunks_and_keeps_blocks() {
        let f = FeedbackFixture::new(2);
        let search_id = f.new_session(&[0, 1]);
        f.memory
            .apply_feedback(search_id, &[f.target(0), f.target(1)])
            .unwrap();

        f.memory.degrade_chunk_precision(&[f.chunk_id(0)]).unwrap();

        let conn = f.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let affinity_zero: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities WHERE entry_id = ?1",
                [f.entry_id(0).to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let affinity_one: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities WHERE entry_id = ?1",
                [f.entry_id(1).to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let session_zero: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT matched_block_id, matched_chunk_id
                 FROM search_session_results WHERE entry_id = ?1",
                [f.entry_id(0).to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let receipt_zero: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM search_feedback WHERE entry_id = ?1",
                [f.entry_id(0).to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(affinity_zero, (Some(f.block_id(0).to_string()), None));
        assert_eq!(session_zero, (Some(f.block_id(0).to_string()), None));
        assert_eq!(
            affinity_one,
            (
                Some(f.block_id(1).to_string()),
                Some(f.chunk_id(1).to_string())
            )
        );
        assert_eq!(
            receipt_zero,
            (
                Some(f.block_id(0).to_string()),
                Some(f.chunk_id(0).to_string())
            )
        );
    }

    #[test]
    fn cleanup_degrade_chunks_coalesces_affinities_that_lose_chunk_precision() {
        let f = FeedbackFixture::new(1);
        let second_chunk = f.insert_second_chunk(0);
        let first_session = f.new_session(&[0]);
        f.memory
            .apply_feedback(first_session, &[f.target(0)])
            .unwrap();
        f.clock.advance_hours(1);
        let second_session = f
            .memory
            .create_search_session(CreateSearchSession {
                raw_query_text: "second chunk raw".into(),
                effective_query_text: "effective query".into(),
                query_embedding: vec![0.0, 1.0, 0.0, 0.0],
                embedding_model: "model-a".into(),
                results: vec![SearchResultTarget {
                    entry_id: f.entry_id(0),
                    matched_block_id: Some(f.block_id(0)),
                    matched_chunk_id: Some(second_chunk),
                    result_rank: 1,
                }],
            })
            .unwrap();
        f.memory
            .apply_feedback(
                second_session,
                &[FeedbackTarget {
                    entry_id: f.entry_id(0),
                    block_id: Some(f.block_id(0)),
                    chunk_id: Some(second_chunk),
                }],
            )
            .unwrap();
        assert_eq!(f.affinity_row_count(), 2);

        f.memory
            .degrade_chunk_precision(&[f.chunk_id(0), second_chunk])
            .unwrap();

        let affinity: (Option<String>, Option<String>, u8) = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT block_id, chunk_id, reinforcement_count FROM query_affinities",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(affinity, (Some(f.block_id(0).to_string()), None, 1));
        assert_eq!(f.affinity_row_count(), 1);
        assert_eq!(f.affinity_vector_row_count(), 1);
        assert_eq!(f.feedback_row_count(), 2);
    }

    #[test]
    fn cleanup_reconcile_removes_missing_entries_and_degrades_stale_precision() {
        let f = FeedbackFixture::new(4);
        let search_id = f.new_session(&[0, 1, 2, 3]);
        f.memory
            .apply_feedback(
                search_id,
                &[f.target(0), f.target(1), f.target(2), f.target(3)],
            )
            .unwrap();
        {
            let conn = f.entries.conn_for_test();
            let conn = conn.lock().unwrap();
            conn.execute(
                "DELETE FROM entries WHERE id = ?1",
                [f.entry_id(0).to_string()],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM chunks WHERE id = ?1",
                [f.chunk_id(1).to_string()],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM blocks WHERE id = ?1",
                [f.block_id(2).to_string()],
            )
            .unwrap();
        }

        let result = f.memory.reconcile_content_references().unwrap();

        assert_eq!(result.entry_stats_deleted, 1);
        assert_eq!(result.affinities_deleted, 1);
        assert_eq!(result.feedback_deleted, 1);
        assert_eq!(result.session_results_deleted, 1);
        assert_eq!(result.affinities_degraded, 2);
        assert_eq!(result.session_results_degraded, 2);
        assert_eq!(result.vectors_deleted, 1);
        assert_eq!(f.signal_row_counts(0), (0, 0, 0, 0, 0));
        assert_eq!(f.signal_row_counts(1), (1, 1, 1, 1, 1));
        assert_eq!(f.signal_row_counts(2), (1, 1, 1, 1, 1));
        assert_eq!(f.signal_row_counts(3), (1, 1, 1, 1, 1));
        assert_eq!(f.affinity_vector_row_count(), 3);

        let conn = f.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let chunk_stale: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities WHERE entry_id = ?1",
                [f.entry_id(1).to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let block_stale: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities WHERE entry_id = ?1",
                [f.entry_id(2).to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let still_valid: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities WHERE entry_id = ?1",
                [f.entry_id(3).to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let receipt_history: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM search_feedback
                 WHERE entry_id IN (?1, ?2) AND block_id IS NOT NULL AND chunk_id IS NOT NULL",
                params![f.entry_id(1).to_string(), f.entry_id(2).to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(chunk_stale, (Some(f.block_id(1).to_string()), None));
        assert_eq!(block_stale, (None, None));
        assert_eq!(
            still_valid,
            (
                Some(f.block_id(3).to_string()),
                Some(f.chunk_id(3).to_string())
            )
        );
        assert_eq!(receipt_history, 2);
    }

    #[test]
    fn cleanup_reconcile_degrades_cross_entry_precision_that_still_resolves() {
        let f = FeedbackFixture::new(2);
        let search_id = f.new_session(&[0]);
        f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        {
            let conn = f.entries.conn_for_test();
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE blocks SET ordinal = 1 WHERE id = ?1",
                [f.block_id(1).to_string()],
            )
            .unwrap();
            conn.execute(
                "UPDATE blocks SET entry_id = ?1 WHERE id = ?2",
                params![f.entry_id(1).to_string(), f.block_id(0).to_string()],
            )
            .unwrap();
        }

        let result = f.memory.reconcile_content_references().unwrap();

        assert_eq!(result.affinities_degraded, 1);
        assert_eq!(result.session_results_degraded, 1);
        let conn = f.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let affinity: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let receipt: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT block_id, chunk_id FROM search_feedback",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(affinity, (None, None));
        assert_eq!(
            receipt,
            (
                Some(f.block_id(0).to_string()),
                Some(f.chunk_id(0).to_string())
            )
        );
    }

    #[test]
    fn purge_expired_sessions_is_bounded_oldest_first_and_cascades_results() {
        let f = FeedbackFixture::new(1);
        let ids = f.seed_expired_sessions(102);

        assert_eq!(f.memory.purge_expired_sessions(100).unwrap(), 100);

        let remaining: Vec<String> = f
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .prepare("SELECT id FROM search_sessions ORDER BY expires_at, created_at, id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(remaining, vec![ids[100].to_string(), ids[101].to_string()]);
        assert_eq!(f.session_result_row_count(), 2);
    }

    #[test]
    fn purge_treats_equal_expiry_as_expired() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.clock.set(f.session_expiry(search_id));

        assert_eq!(f.memory.purge_expired_sessions(100).unwrap(), 1);
        assert_eq!(f.session_row_count(), 0);
        assert_eq!(f.session_result_row_count(), 0);
    }

    #[test]
    fn session_creation_purges_at_most_one_hundred_expired_sessions() {
        let f = FeedbackFixture::new(1);
        f.seed_expired_sessions(101);

        f.new_session(&[0]);

        assert_eq!(f.expired_session_count(), 1);
        assert_eq!(f.session_row_count(), 2);
        assert_eq!(f.session_result_row_count(), 2);
        assert_eq!(f.entry_stats_row_count(), 0);
        assert_eq!(f.affinity_row_count(), 0);
    }

    #[test]
    fn feedback_processing_purges_at_most_one_hundred_other_expired_sessions() {
        let f = FeedbackFixture::new(1);
        let valid_search_id = f.new_session(&[0]);
        f.seed_expired_sessions(101);

        f.memory
            .apply_feedback(valid_search_id, &[f.target(0)])
            .unwrap();

        assert_eq!(f.expired_session_count(), 1);
        assert_eq!(f.session_row_count(), 2);
        assert_eq!(f.session_result_row_count(), 2);
        assert_eq!(f.feedback_row_count(), 1);
    }

    #[test]
    fn purge_expired_session_preserves_accepted_feedback_and_materialized_signals() {
        let f = FeedbackFixture::new(1);
        let search_id = f.new_session(&[0]);
        f.memory.apply_feedback(search_id, &[f.target(0)]).unwrap();
        f.clock.advance_hours(25);

        assert_eq!(f.memory.purge_expired_sessions(100).unwrap(), 1);

        assert_eq!(f.session_row_count(), 0);
        assert_eq!(f.session_result_row_count(), 0);
        assert_eq!(f.signal_row_counts(0), (1, 1, 1, 0, 1));
        assert_eq!(f.affinity_vector_row_count(), 1);
    }

    #[test]
    fn session_creation_fails_and_rolls_back_when_bounded_cleanup_fails() {
        let f = FeedbackFixture::new(1);
        f.seed_expired_sessions(1);
        let conn = f.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(
                "CREATE TEMP TRIGGER fail_session_purge
                 BEFORE DELETE ON search_sessions
                 BEGIN
                    SELECT RAISE(ABORT, 'forced session purge failure');
                 END;",
            )
            .unwrap();

        let result = f.memory.create_search_session(CreateSearchSession {
            raw_query_text: "new raw".into(),
            effective_query_text: "new effective".into(),
            query_embedding: vec![1.0, 0.0, 0.0, 0.0],
            embedding_model: "model-a".into(),
            results: vec![SearchResultTarget {
                entry_id: f.entry_id(0),
                matched_block_id: Some(f.block_id(0)),
                matched_chunk_id: Some(f.chunk_id(0)),
                result_rank: 1,
            }],
        });
        assert!(matches!(result, Err(CoreError::Storage(_))));
        assert_eq!(f.session_row_count(), 1);
        assert_eq!(f.session_result_row_count(), 1);
        assert!(conn.lock().unwrap().is_autocommit());
    }

    #[test]
    fn feedback_fails_without_reinforcement_when_bounded_cleanup_fails() {
        let f = FeedbackFixture::new(1);
        let valid_search_id = f.new_session(&[0]);
        f.seed_expired_sessions(1);
        let conn = f.entries.conn_for_test();
        conn.lock()
            .unwrap()
            .execute_batch(
                "CREATE TEMP TRIGGER fail_feedback_purge
                 BEFORE DELETE ON search_sessions
                 BEGIN
                    SELECT RAISE(ABORT, 'forced feedback purge failure');
                 END;",
            )
            .unwrap();

        assert!(matches!(
            f.memory.apply_feedback(valid_search_id, &[f.target(0)]),
            Err(CoreError::Storage(_))
        ));
        assert_eq!(f.feedback_row_count(), 0);
        assert_eq!(f.entry_stats_row_count(), 0);
        assert_eq!(f.affinity_row_count(), 0);
        assert_eq!(f.affinity_vector_row_count(), 0);
        assert!(conn.lock().unwrap().is_autocommit());
    }

    #[test]
    fn session_persists_raw_effective_query_embedding_and_results() {
        let (entries, memory, _clock) = test_service();
        let entry = seed_note(&entries, "target", "body", &["alpha"]);
        let search_id = memory
            .create_search_session(CreateSearchSession {
                raw_query_text: "  Raw Query  ".into(),
                effective_query_text: "expanded query".into(),
                query_embedding: vec![1.0, 0.0, 0.0, 0.0],
                embedding_model: "model-a".into(),
                results: vec![SearchResultTarget {
                    entry_id: entry.id,
                    matched_block_id: Some(entry.blocks[0].id),
                    matched_chunk_id: None,
                    result_rank: 1,
                }],
            })
            .unwrap();
        let conn = entries.conn_for_test();
        let c = conn.lock().unwrap();
        let stored: (String, String, Vec<u8>, i64) = c
            .query_row(
                "SELECT raw_query_text, effective_query_text, query_embedding, embedding_dim
                 FROM search_sessions WHERE id=?1",
                [search_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(stored.0, "  Raw Query  ");
        assert_eq!(stored.1, "expanded query");
        assert_eq!(
            stored.2,
            [1.0f32, 0.0, 0.0, 0.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>()
        );
        assert_eq!(stored.3, 4);
        let result_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM search_session_results WHERE search_id=?1",
                [search_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(result_count, 1);
    }

    #[test]
    fn failed_session_result_insert_rolls_back_and_restores_autocommit() {
        let (entries, memory, _clock) = test_service();
        let entry = seed_note(&entries, "target", "body", &["alpha"]);

        let error = memory.create_search_session(CreateSearchSession {
            raw_query_text: "duplicate target".into(),
            effective_query_text: "duplicate target".into(),
            query_embedding: vec![1.0, 0.0, 0.0, 0.0],
            embedding_model: "model-a".into(),
            results: vec![
                SearchResultTarget {
                    entry_id: entry.id,
                    matched_block_id: Some(entry.blocks[0].id),
                    matched_chunk_id: None,
                    result_rank: 1,
                },
                SearchResultTarget {
                    entry_id: entry.id,
                    matched_block_id: Some(entry.blocks[0].id),
                    matched_chunk_id: None,
                    result_rank: 2,
                },
            ],
        });
        assert!(error.is_err());

        let conn = entries.conn_for_test();
        let c = conn.lock().unwrap();
        let session_count: i64 = c
            .query_row("SELECT COUNT(*) FROM search_sessions", [], |row| row.get(0))
            .unwrap();
        let result_count: i64 = c
            .query_row("SELECT COUNT(*) FROM search_session_results", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(session_count, 0);
        assert_eq!(result_count, 0);
        assert!(c.is_autocommit());
    }

    #[test]
    fn absent_stats_use_entry_created_at() {
        let (entries, memory, clock) = test_service();
        let entry = seed_note(&entries, "target", "body", &[]);
        clock.set(entry.created_at + chrono::Duration::days(30));
        let signals = memory.entry_memory_signals(&[entry.id]).unwrap();
        assert_eq!(signals[&entry.id].reinforcement_count, 0);
        assert!((signals[&entry.id].memory_factor - 0.85).abs() < 1e-9);
    }

    #[test]
    fn future_created_entry_uses_decay_one_and_a_finite_bounded_factor() {
        let (entries, memory, clock) = test_service();
        let entry = seed_note(&entries, "future target", "future body", &[]);
        clock.set(entry.created_at - chrono::Duration::days(30));

        let signal = memory.entry_memory_signals(&[entry.id]).unwrap()[&entry.id].clone();

        assert_eq!(signal.memory_factor, 1.0);
        assert!(signal.memory_factor.is_finite());
        assert!((0.0..=1.22).contains(&signal.memory_factor));
    }

    #[test]
    fn future_reinforced_affinity_uses_decay_one_and_finite_bounded_scores() {
        let (entries, memory, clock) = test_service();
        let entry = seed_note(&entries, "future affinity", "future body", &[]);
        let block_id = entry.blocks[0].id;
        insert_affinity_fixture(
            &entries.conn_for_test(),
            entry.id,
            block_id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now() + chrono::Duration::days(30),
        );

        let hit = memory
            .affinity_candidates(&[1.0, 0.0, 0.0, 0.0], "model-a", 1, None, None, false)
            .unwrap()
            .remove(0);

        assert!((hit.confidence - 0.70).abs() < 1e-9);
        assert!((hit.affinity_score - 0.70 / 61.0).abs() < 1e-9);
        assert!(hit.confidence.is_finite());
        assert!(hit.affinity_score.is_finite());
        assert!((0.0..=1.0).contains(&hit.confidence));
    }

    #[test]
    fn entry_signal_clock_is_sampled_while_connection_snapshot_is_locked() {
        let entries = EntryService::for_test().unwrap();
        let conn = entries.conn_for_test();
        let clock = FakeClock(Arc::new(Mutex::new(Utc::now())));
        let sampled_with_connection_locked = Arc::new(AtomicBool::new(false));
        let memory = MemorySignalsService::new(
            conn.clone(),
            MemoryPolicy::default(),
            Arc::new(LockObservingClock {
                clock,
                conn: conn.clone(),
                sampled_with_connection_locked: Arc::clone(&sampled_with_connection_locked),
            }),
        )
        .unwrap();
        let entry = seed_note(&entries, "snapshot target", "snapshot body", &[]);

        memory.entry_memory_signals(&[entry.id]).unwrap();

        assert!(sampled_with_connection_locked.load(Ordering::SeqCst));
    }

    #[test]
    fn affinity_clock_is_sampled_while_connection_snapshot_is_locked() {
        let (entries, _memory, clock) = test_service();
        let conn = entries.conn_for_test();
        let entry = seed_note(&entries, "snapshot affinity", "snapshot body", &[]);
        insert_affinity_fixture(
            &conn,
            entry.id,
            entry.blocks[0].id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );
        let sampled_with_connection_locked = Arc::new(AtomicBool::new(false));
        let memory = MemorySignalsService::new(
            conn.clone(),
            MemoryPolicy::default(),
            Arc::new(LockObservingClock {
                clock,
                conn,
                sampled_with_connection_locked: Arc::clone(&sampled_with_connection_locked),
            }),
        )
        .unwrap();

        memory
            .affinity_candidates(&[1.0, 0.0, 0.0, 0.0], "model-a", 1, None, None, false)
            .unwrap();

        assert!(sampled_with_connection_locked.load(Ordering::SeqCst));
    }

    #[test]
    fn vec_affinity_table_reconciles_dimension() {
        let (_entries, memory, _clock) = test_service();
        assert!(matches!(
            memory.ensure_vec_query_affinities(4).unwrap(),
            DimReconciliation::Consistent { dim: 4 }
        ));
        assert!(matches!(
            memory.ensure_vec_query_affinities(8).unwrap(),
            DimReconciliation::Recreated { from: 4, to: 8 }
        ));
    }

    #[test]
    fn vec_dimension_recreation_preserves_ordinary_affinity_rows() {
        let (entries, memory, clock) = test_service();
        let entry = seed_note(&entries, "dimension target", "dimension body", &[]);
        let affinity_id = insert_affinity_fixture(
            &entries.conn_for_test(),
            entry.id,
            entry.blocks[0].id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );

        assert!(matches!(
            memory.ensure_vec_query_affinities(8).unwrap(),
            DimReconciliation::Recreated { from: 4, to: 8 }
        ));

        let conn = entries.conn_for_test();
        let counts: (i64, i64) = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM query_affinities WHERE id = ?1),
                    (SELECT COUNT(*) FROM vec_query_affinities)",
                [affinity_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 0));
    }

    #[test]
    fn required_affinity_selection_keeps_lower_global_rank_and_caps_non_required_hits() {
        let (entries, memory, clock) = test_service();
        let strongest = seed_note(&entries, "strongest", "body", &["alpha"]);
        let second = seed_note(&entries, "second", "body", &["alpha"]);
        let third = seed_note(&entries, "third", "body", &["alpha"]);
        let required = seed_note(&entries, "required", "body", &["alpha"]);
        let conn = entries.conn_for_test();
        insert_affinity_fixture(
            &conn,
            strongest.id,
            strongest.blocks[0].id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            second.id,
            second.blocks[0].id,
            "model-a",
            &[0.98, 0.198_997_49, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            third.id,
            third.blocks[0].id,
            "model-a",
            &[0.96, 0.28, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            required.id,
            required.blocks[0].id,
            "model-a",
            &[0.90, 0.435_889_9, 0.0, 0.0],
            clock.now(),
        );

        let hits = memory
            .affinity_candidates_with_required_entries(
                &[1.0, 0.0, 0.0, 0.0],
                "model-a",
                &[required.id],
                2,
                Some("note"),
                Some("alpha"),
                false,
            )
            .unwrap();

        assert_eq!(
            hits.iter().map(|hit| hit.entry_id).collect::<Vec<_>>(),
            vec![strongest.id, second.id, required.id]
        );
        assert_eq!(
            hits.iter().map(|hit| hit.affinity_rank).collect::<Vec<_>>(),
            vec![1, 2, 4]
        );
        assert!(!hits.iter().any(|hit| hit.entry_id == third.id));
        let required_hit = hits.iter().find(|hit| hit.entry_id == required.id).unwrap();
        assert!((required_hit.affinity_score - 0.004_861_111_111_111_112).abs() < 1e-6);
    }

    #[test]
    fn required_affinity_selection_returns_all_required_without_stacking_or_using_allowance() {
        let (entries, memory, clock) = test_service();
        let strongest = seed_note(&entries, "strongest", "body", &["alpha"]);
        let excluded_non_required = seed_note(&entries, "excluded", "body", &["alpha"]);
        let required_a = seed_note(&entries, "required-a", "body", &["alpha"]);
        let required_b = seed_note(&entries, "required-b", "body", &["alpha"]);
        let conn = entries.conn_for_test();
        insert_affinity_fixture(
            &conn,
            strongest.id,
            strongest.blocks[0].id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            excluded_non_required.id,
            excluded_non_required.blocks[0].id,
            "model-a",
            &[0.98, 0.198_997_49, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            required_a.id,
            required_a.blocks[0].id,
            "model-a",
            &[0.94, 0.341_174_45, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            required_b.id,
            required_b.blocks[0].id,
            "model-a",
            &[0.90, 0.435_889_9, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            required_a.id,
            required_a.blocks[0].id,
            "model-a",
            &[0.83, 0.557_763_4, 0.0, 0.0],
            clock.now(),
        );

        let hits = memory
            .affinity_candidates_with_required_entries(
                &[1.0, 0.0, 0.0, 0.0],
                "model-a",
                &[required_a.id, required_b.id],
                1,
                Some("note"),
                Some("alpha"),
                false,
            )
            .unwrap();

        assert_eq!(
            hits.iter().map(|hit| hit.entry_id).collect::<Vec<_>>(),
            vec![strongest.id, required_a.id, required_b.id]
        );
        assert_eq!(
            hits.iter().map(|hit| hit.affinity_rank).collect::<Vec<_>>(),
            vec![1, 3, 4]
        );
        assert_eq!(
            hits.iter()
                .filter(|hit| hit.entry_id == required_a.id)
                .count(),
            1
        );
        assert!(
            !hits
                .iter()
                .any(|hit| hit.entry_id == excluded_non_required.id)
        );
    }

    #[test]
    fn affinity_lookup_filters_model_threshold_tag_and_block_type() {
        let (entries, memory, clock) = test_service();
        let entry = seed_note(&entries, "target", "body", &["alpha"]);
        insert_affinity_fixture(
            &entries.conn_for_test(),
            entry.id,
            entry.blocks[0].id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );
        let hit = memory
            .affinity_candidates(
                &[1.0, 0.0, 0.0, 0.0],
                "model-a",
                10,
                Some("note"),
                Some("alpha"),
                false,
            )
            .unwrap();
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].entry_id, entry.id);
        assert!(
            memory
                .affinity_candidates(
                    &[1.0, 0.0, 0.0, 0.0],
                    "model-b",
                    10,
                    Some("note"),
                    Some("alpha"),
                    false,
                )
                .unwrap()
                .is_empty()
        );
        assert!(
            memory
                .affinity_candidates(
                    &[0.0, 1.0, 0.0, 0.0],
                    "model-a",
                    10,
                    Some("note"),
                    Some("alpha"),
                    false,
                )
                .unwrap()
                .is_empty()
        );
        assert!(
            memory
                .affinity_candidates(
                    &[1.0, 0.0, 0.0, 0.0],
                    "model-a",
                    10,
                    Some("claim"),
                    Some("alpha"),
                    false,
                )
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn stale_model_vectors_do_not_consume_active_model_knn_slots() {
        let (entries, memory, clock) = test_service();
        let stale = seed_note(&entries, "stale", "body", &["alpha"]);
        let active = seed_note(&entries, "active", "body", &["alpha"]);
        insert_affinity_fixture(
            &entries.conn_for_test(),
            stale.id,
            stale.blocks[0].id,
            "model-stale",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &entries.conn_for_test(),
            active.id,
            active.blocks[0].id,
            "model-active",
            &[0.9, 0.1, 0.0, 0.0],
            clock.now(),
        );

        let hits = memory
            .affinity_candidates(
                &[1.0, 0.0, 0.0, 0.0],
                "model-active",
                1,
                Some("note"),
                Some("alpha"),
                false,
            )
            .unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry_id, active.id);
    }

    #[test]
    fn affinity_lookup_falls_back_to_exact_scan_above_vec_knn_limit() {
        let (entries, memory, clock) = test_service();
        let stale = seed_note(&entries, "stale", "body", &["alpha"]);
        let wrong_tag = seed_note(&entries, "wrong-tag", "body", &["beta"]);
        let strongest = seed_note(&entries, "strongest", "body", &["alpha"]);
        let second = seed_note(&entries, "second", "body", &["alpha"]);
        let conn = entries.conn_for_test();

        insert_many_affinity_fixtures(
            &conn,
            stale.id,
            stale.blocks[0].id,
            "model-stale",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
            4095,
        );
        insert_affinity_fixture(
            &conn,
            wrong_tag.id,
            wrong_tag.blocks[0].id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            strongest.id,
            strongest.blocks[0].id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            strongest.id,
            strongest.blocks[0].id,
            "model-a",
            &[0.9, 0.435_889_9, 0.0, 0.0],
            clock.now(),
        );
        insert_affinity_fixture(
            &conn,
            second.id,
            second.blocks[0].id,
            "model-a",
            &[0.95, 0.312_249_9, 0.0, 0.0],
            clock.now(),
        );

        let hits = memory
            .affinity_candidates(
                &[1.0, 0.0, 0.0, 0.0],
                "model-a",
                2,
                Some("note"),
                Some("alpha"),
                false,
            )
            .unwrap();

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].entry_id, strongest.id);
        assert_eq!(hits[1].entry_id, second.id);
        assert_eq!(hits[0].affinity_rank, 1);
        assert_eq!(hits[1].affinity_rank, 2);
    }

    #[test]
    fn vec0_embedding_reads_as_little_endian_f32_blob() {
        let (entries, _memory, clock) = test_service();
        let entry = seed_note(&entries, "target", "body", &["alpha"]);
        let embedding = [1.0f32, 0.25, -0.5, 0.0];
        let affinity_id = insert_affinity_fixture(
            &entries.conn_for_test(),
            entry.id,
            entry.blocks[0].id,
            "model-a",
            &embedding,
            clock.now(),
        );
        let conn = entries.conn_for_test();
        let stored: Vec<u8> = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT embedding FROM vec_query_affinities WHERE affinity_id = ?1",
                [affinity_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            stored,
            embedding
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cross_entry_stale_targets_do_not_match_or_attach() {
        let (entries, memory, clock) = test_service();
        let owner = seed_note(&entries, "owner", "body", &["alpha"]);
        let foreign = seed_note(&entries, "foreign", "body", &["alpha"]);
        insert_affinity_fixture_with_targets(
            &entries.conn_for_test(),
            owner.id,
            Some(foreign.blocks[0].id),
            None,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );

        assert!(
            memory
                .affinity_candidates(
                    &[1.0, 0.0, 0.0, 0.0],
                    "model-a",
                    10,
                    Some("note"),
                    Some("alpha"),
                    false,
                )
                .unwrap()
                .is_empty()
        );

        let (entries, memory, clock) = test_service();
        let owner = seed_note(&entries, "owner", "body", &["alpha"]);
        let foreign = seed_note(&entries, "foreign", "body", &["alpha"]);
        insert_affinity_fixture_with_targets(
            &entries.conn_for_test(),
            owner.id,
            Some(owner.blocks[0].id),
            Some(first_chunk_id(
                &entries.conn_for_test(),
                foreign.blocks[0].id,
            )),
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );

        let hits = memory
            .affinity_candidates(
                &[1.0, 0.0, 0.0, 0.0],
                "model-a",
                10,
                Some("note"),
                Some("alpha"),
                false,
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry_id, owner.id);
        assert_eq!(hits[0].block_id, Some(owner.blocks[0].id));
        assert_eq!(hits[0].chunk_id, None);
    }

    #[test]
    fn affinity_examples_are_deduplicated_by_the_strongest_confidence() {
        let (entries, memory, clock) = test_service();
        let entry = seed_note(&entries, "target", "body", &["alpha"]);
        insert_affinity_fixture(
            &entries.conn_for_test(),
            entry.id,
            entry.blocks[0].id,
            "model-a",
            &[1.0, 0.0, 0.0, 0.0],
            clock.now(),
        );
        let weaker = insert_affinity_fixture(
            &entries.conn_for_test(),
            entry.id,
            entry.blocks[0].id,
            "model-a",
            &[0.9, 0.435_889_9, 0.0, 0.0],
            clock.now(),
        );
        {
            let conn = entries.conn_for_test();
            conn.lock()
                .unwrap()
                .execute(
                    "UPDATE query_affinities SET reinforcement_count = 3 WHERE id = ?1",
                    [weaker.to_string()],
                )
                .unwrap();
        }
        clock.advance_days(30);

        let hits = memory
            .affinity_candidates(
                &[1.0, 0.0, 0.0, 0.0],
                "model-a",
                10,
                Some("note"),
                Some("alpha"),
                false,
            )
            .unwrap();

        assert_eq!(hits.len(), 1);
        assert!((hits[0].confidence - 0.35).abs() < 1e-6);
        assert!((hits[0].affinity_score - 0.35 / 61.0).abs() < 1e-6);
    }

    #[test]
    fn affinity_reembedding_coalesces_model_collisions_with_strongest_and_latest_metadata() {
        let (entries, memory, _clock) = test_service();
        let entry = seed_note(&entries, "target", "body", &[]);
        let block_id = entry.blocks[0].id;
        let chunk_id = first_chunk_id(&entries.conn_for_test(), block_id);
        let older_id = Ulid::new();
        let latest_id = Ulid::new();
        let hash = blake3::hash(b"same query").to_hex().to_string();
        insert_reembedding_affinity(
            &entries.conn_for_test(),
            ReembeddingAffinityFixture {
                affinity_id: older_id,
                normalized_query_hash: &hash,
                raw_query_text: "older raw",
                effective_query_text: " SAME   Query ",
                embedding_model: "old-model-a",
                entry_id: entry.id,
                block_id: Some(block_id),
                chunk_id: Some(chunk_id),
                reinforcement_count: 3,
                last_reinforced_at: "2026-01-10T00:00:00Z",
                created_at: "2025-01-01T00:00:00Z",
                updated_at: "2026-01-11T00:00:00Z",
                embedding: &[1.0, 0.0, 0.0, 0.0],
            },
        );
        insert_reembedding_affinity(
            &entries.conn_for_test(),
            ReembeddingAffinityFixture {
                affinity_id: latest_id,
                normalized_query_hash: &hash,
                raw_query_text: "latest raw",
                effective_query_text: "same query",
                embedding_model: "old-model-b",
                entry_id: entry.id,
                block_id: Some(block_id),
                chunk_id: Some(chunk_id),
                reinforcement_count: 2,
                last_reinforced_at: "2026-02-10T00:00:00Z",
                created_at: "2025-02-01T00:00:00Z",
                updated_at: "2026-02-11T00:00:00Z",
                embedding: &[0.0, 1.0, 0.0, 0.0],
            },
        );

        let plan = memory.list_affinities_for_reembedding().unwrap();

        assert_eq!(plan.inputs.len(), 1);
        assert_eq!(plan.inputs[0].affinity_id, latest_id);
        assert_eq!(plan.inputs[0].effective_query_text, "same query");
        let conn = entries.conn_for_test();
        assert_eq!(
            conn.lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM query_affinities", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2,
            "planning provider work must not mutate ordinary affinity rows"
        );

        memory
            .replace_affinity_vectors(
                "active-model",
                4,
                &plan.fingerprint,
                &[(latest_id, vec![0.0, 1.0, 0.0, 0.0])],
            )
            .unwrap();

        let conn = entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT id, raw_query_text, effective_query_text, embedding_model,
                        embedding_dim, reinforcement_count, last_reinforced_at,
                        created_at, updated_at
                 FROM query_affinities",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, u8>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            row,
            (
                latest_id.to_string(),
                "latest raw".into(),
                "same query".into(),
                "active-model".into(),
                4,
                3,
                "2026-02-10T00:00:00Z".into(),
                "2025-01-01T00:00:00Z".into(),
                "2026-02-11T00:00:00Z".into(),
            )
        );
        let stored = conn
            .query_row(
                "SELECT embedding FROM vec_query_affinities WHERE affinity_id = ?1",
                [latest_id.to_string()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap();
        assert_eq!(decode_vec0_f32(&stored).unwrap(), vec![0.0, 1.0, 0.0, 0.0]);
    }

    #[test]
    fn affinity_reembedding_preserves_schema_max_history_across_a_lower_active_cap() {
        let entries = EntryService::for_test().unwrap();
        let conn = entries.conn_for_test();
        let clock = FakeClock(Arc::new(Mutex::new(
            "2026-02-10T00:00:00Z".parse().unwrap(),
        )));
        let lower_cap_memory =
            MemorySignalsService::new(conn.clone(), policy_with_cap(1), Arc::new(clock.clone()))
                .unwrap();
        lower_cap_memory.ensure_vec_query_affinities(4).unwrap();
        let entry = seed_note(&entries, "historical target", "historical body", &[]);
        let block_id = entry.blocks[0].id;
        let chunk_id = first_chunk_id(&conn, block_id);
        let affinity_id = Ulid::new();
        let hash = blake3::hash(b"historical query").to_hex().to_string();
        insert_reembedding_affinity(
            &conn,
            ReembeddingAffinityFixture {
                affinity_id,
                normalized_query_hash: &hash,
                raw_query_text: "historical raw",
                effective_query_text: "historical query",
                embedding_model: "old-model",
                entry_id: entry.id,
                block_id: Some(block_id),
                chunk_id: Some(chunk_id),
                reinforcement_count: 3,
                last_reinforced_at: "2026-02-10T00:00:00Z",
                created_at: "2026-01-01T00:00:00Z",
                updated_at: "2026-02-10T00:00:00Z",
                embedding: &[0.0, 1.0, 0.0, 0.0],
            },
        );

        let plan = lower_cap_memory.list_affinities_for_reembedding().unwrap();
        lower_cap_memory
            .replace_affinity_vectors(
                "active-model",
                4,
                &plan.fingerprint,
                &[(affinity_id, vec![1.0, 0.0, 0.0, 0.0])],
            )
            .unwrap();

        let stored_count: u8 = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT reinforcement_count FROM query_affinities WHERE id = ?1",
                [affinity_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_count, 3);

        let full_cap_memory =
            MemorySignalsService::new(conn, MemoryPolicy::default(), Arc::new(clock)).unwrap();
        let hits = full_cap_memory
            .affinity_candidates(&[1.0, 0.0, 0.0, 0.0], "active-model", 1, None, None, false)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!((hits[0].confidence - 1.0).abs() < 1e-9);
    }

    #[test]
    fn affinity_vector_replacement_rolls_back_ordinary_rows_and_vectors_on_write_failure() {
        let (entries, memory, _clock) = test_service();
        let entry = seed_note(&entries, "target", "body", &[]);
        let affinity_id = Ulid::new();
        let hash = blake3::hash(b"rollback query").to_hex().to_string();
        insert_reembedding_affinity(
            &entries.conn_for_test(),
            ReembeddingAffinityFixture {
                affinity_id,
                normalized_query_hash: &hash,
                raw_query_text: "rollback raw",
                effective_query_text: "rollback query",
                embedding_model: "old-model",
                entry_id: entry.id,
                block_id: None,
                chunk_id: None,
                reinforcement_count: 1,
                last_reinforced_at: "2026-01-10T00:00:00Z",
                created_at: "2026-01-01T00:00:00Z",
                updated_at: "2026-01-11T00:00:00Z",
                embedding: &[1.0, 0.0, 0.0, 0.0],
            },
        );
        let conn = entries.conn_for_test();
        let plan = memory.list_affinities_for_reembedding().unwrap();
        conn.lock()
            .unwrap()
            .execute_batch(&format!(
                "CREATE TRIGGER fail_affinity_vector_replace
                 BEFORE UPDATE ON query_affinities
                 WHEN OLD.id = '{}' AND NEW.embedding_model = 'active-model'
                 BEGIN SELECT RAISE(FAIL, 'forced vector replacement failure'); END;",
                affinity_id
            ))
            .unwrap();

        assert!(matches!(
            memory.replace_affinity_vectors(
                "active-model",
                4,
                &plan.fingerprint,
                &[(affinity_id, vec![0.0, 1.0, 0.0, 0.0])],
            ),
            Err(CoreError::Storage(_))
        ));

        let conn = conn.lock().unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT embedding_model FROM query_affinities WHERE id = ?1",
                [affinity_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
            "old-model"
        );
        let stored = conn
            .query_row(
                "SELECT embedding FROM vec_query_affinities WHERE affinity_id = ?1",
                [affinity_id.to_string()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap();
        assert_eq!(decode_vec0_f32(&stored).unwrap(), vec![1.0, 0.0, 0.0, 0.0]);
    }

    #[test]
    fn affinity_reembedding_snapshot_rejects_concurrent_insert_update_and_delete() {
        enum Mutation {
            Insert,
            Update,
            Delete,
        }

        for mutation in [Mutation::Insert, Mutation::Update, Mutation::Delete] {
            let (entries, memory, _clock) = test_service();
            let entry = seed_note(&entries, "target", "body", &[]);
            let affinity_id = Ulid::new();
            let hash = blake3::hash(b"snapshot query").to_hex().to_string();
            insert_reembedding_affinity(
                &entries.conn_for_test(),
                ReembeddingAffinityFixture {
                    affinity_id,
                    normalized_query_hash: &hash,
                    raw_query_text: "snapshot raw",
                    effective_query_text: "snapshot query",
                    embedding_model: "old-model",
                    entry_id: entry.id,
                    block_id: None,
                    chunk_id: None,
                    reinforcement_count: 1,
                    last_reinforced_at: "2026-01-10T00:00:00Z",
                    created_at: "2026-01-01T00:00:00Z",
                    updated_at: "2026-01-11T00:00:00Z",
                    embedding: &[1.0, 0.0, 0.0, 0.0],
                },
            );
            let plan = memory.list_affinities_for_reembedding().unwrap();
            let vectors = plan
                .inputs
                .iter()
                .map(|input| (input.affinity_id, vec![0.0, 1.0, 0.0, 0.0]))
                .collect::<Vec<_>>();
            let conn = entries.conn_for_test();
            match mutation {
                Mutation::Insert => {
                    let inserted_id = Ulid::new();
                    let inserted_hash = blake3::hash(b"inserted query").to_hex().to_string();
                    insert_reembedding_affinity(
                        &conn,
                        ReembeddingAffinityFixture {
                            affinity_id: inserted_id,
                            normalized_query_hash: &inserted_hash,
                            raw_query_text: "inserted raw",
                            effective_query_text: "inserted query",
                            embedding_model: "old-model",
                            entry_id: entry.id,
                            block_id: None,
                            chunk_id: None,
                            reinforcement_count: 1,
                            last_reinforced_at: "2026-02-10T00:00:00Z",
                            created_at: "2026-02-01T00:00:00Z",
                            updated_at: "2026-02-11T00:00:00Z",
                            embedding: &[0.0, 1.0, 0.0, 0.0],
                        },
                    );
                }
                Mutation::Update => {
                    conn.lock()
                        .unwrap()
                        .execute(
                            "UPDATE query_affinities
                             SET raw_query_text = 'concurrent raw',
                                 updated_at = '2026-03-11T00:00:00Z'
                             WHERE id = ?1",
                            [affinity_id.to_string()],
                        )
                        .unwrap();
                }
                Mutation::Delete => {
                    let conn = conn.lock().unwrap();
                    conn.execute(
                        "DELETE FROM vec_query_affinities WHERE affinity_id = ?1",
                        [affinity_id.to_string()],
                    )
                    .unwrap();
                    conn.execute(
                        "DELETE FROM query_affinities WHERE id = ?1",
                        [affinity_id.to_string()],
                    )
                    .unwrap();
                }
            }
            let after_concurrent_mutation = reembedding_storage_snapshot(&conn);

            assert!(matches!(
                memory.replace_affinity_vectors(
                    "active-model",
                    4,
                    &plan.fingerprint,
                    &vectors,
                ),
                Err(CoreError::Conflict(message))
                    if message.contains("changed during affinity re-embedding")
            ));
            assert_eq!(
                reembedding_storage_snapshot(&conn),
                after_concurrent_mutation,
                "snapshot mismatch must not mutate ordinary or vector state"
            );
        }
    }
}
