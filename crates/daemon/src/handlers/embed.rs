//! Chunk-embedding orchestration for the daemon layer.
//!
//! `EntryService::create` (core) does NOT embed — it writes entry + blocks +
//! chunks text only. This module is the daemon-side glue that embeds an
//! entry's chunks and writes them to `vec_chunk_embeddings`, so
//! `search.semantic` works for production entries.
//!
//! Called after entry.create/update, block.append/update, and batch commit.
//! Reuses `CachedEmbedder` (emb_cache) so unchanged bodies skip the API call.

use std::collections::HashMap;
use std::sync::Arc;

use ulid::Ulid;

use nomai_core::{ChunkService, CoreError, EntryService};
use nomai_providers::EmbeddingProvider;

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;

/// Embed every chunk of `entry_id`'s blocks and persist the vectors to
/// `vec_chunk_embeddings`.
///
/// Three phases:
/// 1. sync (spawn_blocking): collect `Vec<(chunk_id, text)>` via the matching
///    normal or benchmark-visible block and chunk list methods.
/// 2. async: batch `daemon.cache().embed(&texts)` (CachedEmbedder → emb_cache).
/// 3. sync (spawn_blocking): `chunks.write_embedding(chunk_id, vec)` each.
///
/// Empty entry (no chunks) → no-op Ok. Embed/SQL failure → `CoreError`
/// propagated (caller maps to RPC 1002 / storage error). The entry itself
/// is already committed by the caller; this function only enriches the
/// vector index and does NOT roll back the entry on failure.
pub(crate) async fn embed_entry_chunks(
    daemon: &Daemon,
    entry_id: Ulid,
    include_benchmark: bool,
) -> Result<(), CoreError> {
    let entries: Arc<EntryService> = daemon.entries().clone();
    let chunks: Arc<ChunkService> = daemon.chunks().clone();

    // 1. Collect (chunk_id, text) pairs.
    let chunk_texts: Vec<(Ulid, String)> = {
        let entries = entries.clone();
        let chunks = chunks.clone();
        blocking(move || -> Result<Vec<(Ulid, String)>, CoreError> {
            let blocks = if include_benchmark {
                entries.block_service().list_with_benchmark(entry_id)?
            } else {
                entries.block_service().list(entry_id)?
            };
            let mut out: Vec<(Ulid, String)> = Vec::new();
            for b in blocks.items {
                let chunk_list = if include_benchmark {
                    chunks.list_with_benchmark(b.id)?
                } else {
                    chunks.list(b.id)?
                };
                for c in chunk_list.items {
                    out.push((c.id, c.text));
                }
            }
            Ok(out)
        })
        .await??
    };

    if chunk_texts.is_empty() {
        return Ok(());
    }

    // 2. Batch embed (async — CachedEmbedder hits emb_cache for unchanged).
    let texts: Vec<&str> = chunk_texts.iter().map(|(_, t)| t.as_str()).collect();
    let embeddings = daemon.cache().embed(&texts).await?;

    // 3. Write each embedding.
    blocking(move || -> Result<(), CoreError> {
        for ((id, _), vec) in chunk_texts.iter().zip(embeddings.iter()) {
            chunks.write_embedding(*id, vec)?;
        }
        Ok(())
    })
    .await?
}

/// Re-embed every retained learned-query example with the daemon's active
/// provider, then atomically move ordinary affinities and their vector index
/// to the active model/dimension.
///
/// Provider work happens before any affinity mutation. Identical effective
/// text is embedded once and its vector is fanned back out to each surviving
/// affinity ID. Consequently a provider error leaves all ordinary affinity
/// rows untouched for a later rebuild retry.
pub(crate) async fn reembed_query_affinities(daemon: &Daemon) -> Result<u64, CoreError> {
    let plan = {
        let memory = daemon.memory.clone();
        blocking(move || memory.list_affinities_for_reembedding()).await??
    };
    let inputs = &plan.inputs;

    let mut text_indexes = HashMap::<String, usize>::new();
    let mut unique_texts = Vec::<String>::new();
    let mut input_text_indexes = Vec::with_capacity(inputs.len());
    for input in inputs {
        let text_index = if let Some(index) = text_indexes.get(&input.effective_query_text) {
            *index
        } else {
            let index = unique_texts.len();
            unique_texts.push(input.effective_query_text.clone());
            text_indexes.insert(input.effective_query_text.clone(), index);
            index
        };
        input_text_indexes.push(text_index);
    }

    let embeddings = if unique_texts.is_empty() {
        Vec::new()
    } else {
        let texts = unique_texts.iter().map(String::as_str).collect::<Vec<_>>();
        daemon.cache.embed(&texts).await?
    };
    if embeddings.len() != unique_texts.len() {
        return Err(CoreError::Validation(format!(
            "affinity provider returned {} vectors for {} texts",
            embeddings.len(),
            unique_texts.len()
        )));
    }

    let vectors = inputs
        .iter()
        .zip(input_text_indexes)
        .map(|(input, text_index)| (input.affinity_id, embeddings[text_index].clone()))
        .collect::<Vec<_>>();
    let count = vectors.len() as u64;
    let memory = daemon.memory.clone();
    let embedding_model = daemon.embedding_model.clone();
    let embedding_dim = daemon.embedding_dim;
    let fingerprint = plan.fingerprint;
    blocking(move || {
        memory.replace_affinity_vectors(&embedding_model, embedding_dim, &fingerprint, &vectors)
    })
    .await??;
    Ok(count)
}
