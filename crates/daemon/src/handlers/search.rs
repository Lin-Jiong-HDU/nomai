//! search.* handlers.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use ulid::Ulid;

use nomai_core::CoreError;
use nomai_providers::{
    ChatMessage, CompletionRequest, EmbeddingProvider, LlmProvider, MessageRole,
};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use crate::search_cache::SearchRpc;

fn default_search_limit() -> u32 {
    10
}

/// Normalize an optional tag from wire input: trim whitespace and treat an
/// empty result as `None` (no filter). Shared by `search.fulltext` and
/// `search.semantic` so the cache key never distinguishes `"  "` from `None`.
fn normalize_tag(tag: Option<String>) -> Option<String> {
    tag.and_then(|t| {
        let t = t.trim();
        (!t.is_empty()).then(|| t.to_string())
    })
}

/// Attempt to expand a vague/context-dependent query into a specific,
/// self-contained search query using the configured LLM.
///
/// On success, returns the rewritten query. On any failure (LLM error,
/// empty response, timeout), returns the original query — rewrite is a
/// best-effort optimization, never a hard failure.
async fn expand_query(llm: &Arc<dyn LlmProvider>, query: &str, context: Option<&str>) -> String {
    let system = "Rewrite the given search query to be specific, self-contained, \
                  and suitable for a search engine. Resolve pronouns, expand \
                  abbreviations, and add missing context. Return ONLY the \
                  rewritten query text, no explanation, no markdown."
        .to_string();

    let context_hint = context
        .map(|c| format!("\n\nContext: {c}"))
        .unwrap_or_default();

    let user = format!("Query: {query}{context_hint}");

    let req = CompletionRequest {
        system: Some(system),
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: user,
        }],
        max_tokens: Some(200),
        temperature: Some(0.0),
    };

    match llm.complete(req).await {
        Ok(resp) => {
            let rewritten = resp.content.trim().to_string();
            if rewritten.is_empty() {
                query.to_string()
            } else {
                rewritten
            }
        }
        Err(_) => query.to_string(),
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct FulltextParams {
    query: String,
    #[serde(default = "default_search_limit")]
    #[schemars(default = "default_search_limit")]
    limit: u32,
    /// Optional block-type filter (e.g. `"claim"`, `"note"`). When supplied,
    /// restricts matches to blocks of that type.
    #[serde(default)]
    #[schemars(default)]
    block_type: Option<String>,
    /// Optional tag filter: restricts matches to entries whose `tags` JSON
    /// array contains this exact value (same semantics as `entry.list`'s
    /// `tag`). Whitespace-only / empty values are treated as no filter.
    #[serde(default)]
    #[schemars(default)]
    tag: Option<String>,
    /// Optional query rewriting strategy. Currently only `"expand"` is
    /// supported — resolves pronouns and expands abbreviations using
    /// the configured LLM. Omit to skip rewriting.
    #[serde(default)]
    #[schemars(default)]
    rewrite: Option<String>,
    /// Conversation context for query rewriting. Helps the LLM resolve
    /// pronouns and implicit references. Only used when `rewrite` is set.
    #[serde(default)]
    #[schemars(default)]
    rewrite_context: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SemanticParams {
    query: String,
    #[serde(default = "default_search_limit")]
    #[schemars(default = "default_search_limit")]
    limit: u32,
    /// Optional block-type filter applied via JOIN blocks.
    #[serde(default)]
    #[schemars(default)]
    block_type: Option<String>,
    /// Optional tag filter: restricts matches to entries whose `tags` JSON
    /// array contains this exact value (same semantics as `entry.list`'s
    /// `tag`). Whitespace-only / empty values are treated as no filter.
    #[serde(default)]
    #[schemars(default)]
    tag: Option<String>,
    /// Optional query rewriting strategy. Currently only `"expand"` is
    /// supported — resolves pronouns and expands abbreviations using
    /// the configured LLM. Omit to skip rewriting.
    #[serde(default)]
    #[schemars(default)]
    rewrite: Option<String>,
    /// Conversation context for query rewriting. Helps the LLM resolve
    /// pronouns and implicit references. Only used when `rewrite` is set.
    #[serde(default)]
    #[schemars(default)]
    rewrite_context: Option<String>,
}

/// Map a `FulltextSearchResult` to the wire JSON object. Extracted so the
/// field set is unit-testable without spinning up a Daemon.
fn serialize_fulltext_result(r: &nomai_core::FulltextSearchResult) -> serde_json::Value {
    json!({
        "entry": r.entry,
        "score": r.score,
        "match_count": r.match_count,
        "matched_block_ids": r.matched_block_ids,
        "best_match": {
            "block_id": r.best_match.block_id,
            "block_type": r.best_match.block_type,
            "snippet": r.best_match.snippet,
        }
    })
}

/// Search demotion penalty for transient entries. Hardcoded for
/// now; promote to config only if a real need appears (YAGNI).
const TRANSIENT_PENALTY: f64 = 0.5;

/// Collect the transient subset among `entry_ids` via a single IN query
/// (policy applied live — never a stale snapshot).
async fn transient_set(
    entries: &nomai_core::EntryService,
    entry_ids: Vec<String>,
) -> Result<std::collections::HashSet<String>, CoreError> {
    if entry_ids.is_empty() {
        return Ok(Default::default());
    }
    let entries = entries.clone();
    blocking(move || entries.transient_ids_among(&entry_ids)).await?
}

/// Demote transient hits (score *= TRANSIENT_PENALTY) and re-sort by score
/// desc. `entry_id_of` extracts the owning entry id from each wire item.
fn downrank_inner(
    items: &mut [Value],
    transient: &std::collections::HashSet<String>,
    entry_id_of: impl Fn(&Value) -> Option<String>,
) {
    for it in items.iter_mut() {
        let is_t = entry_id_of(it)
            .map(|id| transient.contains(&id))
            .unwrap_or(false);
        if is_t && let Some(s) = it["score"].as_f64() {
            it["score"] = json!(s * TRANSIENT_PENALTY);
        }
    }
    items.sort_by(|a, b| {
        b["score"]
            .as_f64()
            .partial_cmp(&a["score"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

/// Fulltext wire items expose `entry.id`.
fn downrank_fulltext(
    mut items: Vec<Value>,
    transient: &std::collections::HashSet<String>,
) -> Vec<Value> {
    downrank_inner(&mut items, transient, |v| {
        v["entry"]["id"].as_str().map(String::from)
    });
    items
}

/// Hybrid wire items expose `entry.id` and use `fusion_score` (not `score`).
fn downrank_hybrid(
    mut items: Vec<Value>,
    transient: &std::collections::HashSet<String>,
) -> Vec<Value> {
    for it in items.iter_mut() {
        let is_t = it["entry"]["id"]
            .as_str()
            .map(|id| transient.contains(id))
            .unwrap_or(false);
        if is_t && let Some(s) = it["fusion_score"].as_f64() {
            it["fusion_score"] = json!(s * TRANSIENT_PENALTY);
        }
    }
    items.sort_by(|a, b| {
        let a_score = a["fusion_score"].as_f64().unwrap_or(0.0);
        let b_score = b["fusion_score"].as_f64().unwrap_or(0.0);
        b_score
            .partial_cmp(&a_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    items
}

/// Semantic wire items expose `entry_id`.
fn downrank_semantic(
    mut items: Vec<Value>,
    transient: &std::collections::HashSet<String>,
) -> Vec<Value> {
    downrank_inner(&mut items, transient, |v| {
        v["entry_id"].as_str().map(String::from)
    });
    items
}

pub struct Fulltext;
#[async_trait]
impl RpcHandler for Fulltext {
    fn method(&self) -> &'static str {
        "search.fulltext"
    }
    fn description(&self) -> &'static str {
        "Fulltext search over block text via SQLite FTS5. Returns entries ranked by relevance. Score is relative relevance (not a boolean hit marker): higher means stronger match; near-zero means weak match. Optional `tag` restricts results to entries whose `tags` array contains that value. Each call is cached per (query, limit, block_type, tag)."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(FulltextParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: FulltextParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let entries = daemon.entries.clone();
        // Optional query rewrite (before cache lookup — rewritten query is
        // what gets cached, so the cache key is deterministic per input).
        let query = if p.rewrite.as_deref() == Some("expand") {
            expand_query(&daemon.llm, &p.query, p.rewrite_context.as_deref()).await
        } else {
            p.query
        };
        let limit = p.limit;
        let block_type = p.block_type;
        // Trim-normalize tag: empty / whitespace-only → None (no filter).
        let tag = normalize_tag(p.tag);
        let include_benchmark = daemon
            .benchmark
            .as_ref()
            .is_some_and(|runtime| runtime.is_active());
        // Snapshot the strings we need inside the compute closure. `query`
        // is moved; `block_type`/`tag` are cloned because we need to also
        // pass an `Option<&str>` to lookup_or_compute below.
        let bt_for_compute = block_type.clone();
        let tag_for_compute = tag.clone();
        let cached = daemon
            .search_cache
            .lookup_or_compute(
                SearchRpc::Fulltext,
                &query,
                limit,
                block_type.as_deref(),
                tag.as_deref(),
                None,
                || {
                    let entries = entries.clone();
                    let bt = bt_for_compute.clone();
                    let tg = tag_for_compute.clone();
                    let query_for_closure = query.clone();
                    async move {
                        let items_inner = blocking(move || {
                            if include_benchmark {
                                entries.fulltext_search_with_benchmark(
                                    &query_for_closure,
                                    limit,
                                    bt.as_deref(),
                                    tg.as_deref(),
                                )
                            } else {
                                entries.fulltext_search(
                                    &query_for_closure,
                                    limit,
                                    bt.as_deref(),
                                    tg.as_deref(),
                                )
                            }
                        })
                        .await??;
                        // Map to JSON value items matching the existing wire
                        // format.
                        Ok(items_inner
                            .into_iter()
                            .map(|r| serialize_fulltext_result(&r))
                            .collect::<Vec<_>>())
                    }
                },
            )
            .await?;
        let items: Vec<Value> = cached.as_ref().clone();
        let ids: Vec<String> = items
            .iter()
            .filter_map(|v| v["entry"]["id"].as_str().map(String::from))
            .collect();
        let transient = transient_set(&daemon.entries, ids).await?;
        let items = downrank_fulltext(items, &transient);
        Ok(json!({ "items": items }))
    }
}

pub struct Semantic;
#[async_trait]
impl RpcHandler for Semantic {
    fn method(&self) -> &'static str {
        "search.semantic"
    }
    fn description(&self) -> &'static str {
        "Semantic search over chunk embeddings via sqlite-vec cosine similarity. Queries the embedding provider on each unique query (cached). Returns chunks ranked by similarity. Optional `tag` restricts results to entries whose `tags` array contains that value."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(SemanticParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: SemanticParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        // Optional query rewrite (before cache lookup — rewritten query is
        // what gets cached, so the cache key is deterministic per input).
        let query_str = if p.rewrite.as_deref() == Some("expand") {
            expand_query(&daemon.llm, &p.query, p.rewrite_context.as_deref()).await
        } else {
            p.query
        };
        let limit = p.limit;
        let block_type = p.block_type;
        // Trim-normalize tag: empty / whitespace-only → None (no filter).
        let tag = normalize_tag(p.tag);
        let include_benchmark = daemon
            .benchmark
            .as_ref()
            .is_some_and(|runtime| runtime.is_active());
        let bt_for_compute = block_type.clone();
        let tag_for_compute = tag.clone();

        let cache = daemon.search_cache.clone();
        let embed = daemon.cache.clone();
        let chunks = daemon.chunks.clone();
        let cached = cache
            .lookup_or_compute(
                SearchRpc::Semantic,
                &query_str,
                limit,
                block_type.as_deref(),
                tag.as_deref(),
                None,
                || {
                    let embed = embed.clone();
                    let chunks = chunks.clone();
                    let query_owned = query_str.clone();
                    let bt = bt_for_compute.clone();
                    let tg = tag_for_compute.clone();
                    async move {
                        let embeddings = embed.embed(&[&query_owned]).await?;
                        let qvec = embeddings
                            .into_iter()
                            .next()
                            .ok_or_else(|| CoreError::Config("empty embedding response".into()))?;
                        let items_inner = blocking(move || {
                            if include_benchmark {
                                chunks.semantic_search_with_benchmark(
                                    &qvec,
                                    limit as usize,
                                    bt.as_deref(),
                                    tg.as_deref(),
                                )
                            } else {
                                chunks
                                    .semantic_search(&qvec, limit as usize, bt.as_deref(), tg.as_deref())
                            }
                        })
                        .await??;
                        Ok(items_inner
                            .into_iter()
                            .map(|r| json!({ "chunk": r.chunk, "score": r.score, "entry_id": r.entry_id }))
                            .collect::<Vec<_>>())
                    }
                },
            )
            .await?;
        let items: Vec<Value> = cached.as_ref().clone();
        let ids: Vec<String> = items
            .iter()
            .filter_map(|v| v["entry_id"].as_str().map(String::from))
            .collect();
        let transient = transient_set(&daemon.entries, ids).await?;
        let items = downrank_semantic(items, &transient);
        Ok(json!({ "items": items }))
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct HybridParams {
    query: String,
    #[serde(default = "default_search_limit")]
    #[schemars(default = "default_search_limit")]
    limit: u32,
    #[serde(default)]
    #[schemars(default)]
    block_type: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    tag: Option<String>,
    #[serde(default = "default_one")]
    #[schemars(default = "default_one")]
    fulltext_weight: f32,
    #[serde(default = "default_one")]
    #[schemars(default = "default_one")]
    semantic_weight: f32,
    /// Optional query rewriting strategy. Currently only `"expand"` is
    /// supported — resolves pronouns and expands abbreviations using
    /// the configured LLM. Omit to skip rewriting.
    #[serde(default)]
    #[schemars(default)]
    rewrite: Option<String>,
    /// Conversation context for query rewriting. Helps the LLM resolve
    /// pronouns and implicit references. Only used when `rewrite` is set.
    #[serde(default)]
    #[schemars(default)]
    rewrite_context: Option<String>,
}

fn default_one() -> f32 {
    1.0
}

/// Serialize a single HybridSearchResult to the wire JSON object.
fn serialize_hybrid_result(r: &nomai_core::HybridSearchResult) -> serde_json::Value {
    let mut obj = json!({
        "entry": r.entry,
        "fusion_score": r.fusion_score,
        "fulltext_rank": r.fulltext_rank,
        "fulltext_score": r.fulltext_score,
        "semantic_rank": r.semantic_rank,
        "semantic_score": r.semantic_score,
    });
    if let Some(ref chunk) = r.matched_chunk {
        obj["matched_chunk"] = json!({
            "id": chunk.id,
            "text": chunk.text,
        });
    }
    if let Some(ref block) = r.matched_block {
        obj["matched_block"] = json!({
            "id": block.id,
            "type": block.r#type,
            "snippet": block.snippet,
        });
    }
    obj
}

fn hybrid_entry_id(item: &Value) -> Result<Ulid, CoreError> {
    let id = item["entry"]["id"]
        .as_str()
        .ok_or_else(|| CoreError::Config("hybrid candidate missing entry.id".into()))?;
    id.parse()
        .map_err(|_| CoreError::Config(format!("hybrid candidate has invalid entry.id: {id}")))
}

fn optional_matched_id(item: &Value, field: &str) -> Result<Option<Ulid>, CoreError> {
    let Some(value) = item.get(field) else {
        return Ok(None);
    };
    let id = value["id"]
        .as_str()
        .ok_or_else(|| CoreError::Config(format!("hybrid candidate has invalid {field}.id")))?;
    id.parse()
        .map(Some)
        .map_err(|_| CoreError::Config(format!("hybrid candidate has invalid {field}.id: {id}")))
}

async fn serialize_affinity_candidate(
    entries: Arc<nomai_core::EntryService>,
    chunks: Arc<nomai_core::ChunkService>,
    hit: nomai_core::AffinityHit,
    include_benchmark: bool,
) -> Result<Value, CoreError> {
    blocking(move || {
        let entry = if include_benchmark {
            entries.get_with_benchmark(hit.entry_id)?
        } else {
            entries.get(hit.entry_id)?
        };
        let matched_block = hit
            .block_id
            .map(|block_id| {
                entry
                    .blocks
                    .iter()
                    .find(|block| block.id == block_id)
                    .map(|block| nomai_core::BlockRef {
                        id: block.id,
                        r#type: block.r#type.clone(),
                        snippet: block.text.clone(),
                    })
                    .ok_or(CoreError::NotFound(block_id))
            })
            .transpose()?;
        let matched_chunk = hit
            .chunk_id
            .map(|chunk_id| {
                let chunk = if include_benchmark {
                    chunks.get_with_benchmark(chunk_id)?
                } else {
                    chunks.get(chunk_id)?
                };
                Ok::<_, CoreError>(nomai_core::ChunkRef {
                    id: chunk.id,
                    text: chunk.text,
                })
            })
            .transpose()?;

        Ok(serialize_hybrid_result(&nomai_core::HybridSearchResult {
            entry,
            fusion_score: 0.0,
            fulltext_rank: 0,
            fulltext_score: 0.0,
            semantic_rank: 0,
            semantic_score: 0.0,
            matched_chunk,
            matched_block,
        }))
    })
    .await?
}

pub struct Hybrid;

#[async_trait]
impl RpcHandler for Hybrid {
    fn method(&self) -> &'static str {
        "search.hybrid"
    }
    fn description(&self) -> &'static str {
        "Hybrid search fusing fulltext (FTS5 BM25) and semantic (cosine similarity) results via Reciprocal Rank Fusion (k=60). Returns entry-granularity results with per-retriever scores and ranks. Optional weights bias the fusion toward one retriever. Cached per (query, limit, block_type, tag, weights)."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(HybridParams).to_value())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: HybridParams = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        // Optional query rewrite (before cache lookup — rewritten query is
        // what gets cached, so the cache key is deterministic per input).
        let raw_query = p.query;
        let query = if p.rewrite.as_deref() == Some("expand") {
            expand_query(&daemon.llm, &raw_query, p.rewrite_context.as_deref()).await
        } else {
            raw_query.clone()
        };
        let limit = p.limit;
        let block_type = p.block_type;
        let tag = normalize_tag(p.tag);
        let fw = p.fulltext_weight;
        let sw = p.semantic_weight;
        let memory_enabled = daemon.memory.policy().enabled;
        let include_benchmark = daemon
            .benchmark
            .as_ref()
            .is_some_and(|runtime| runtime.is_active());
        let base_recall = (limit * 2).max(20);

        // Adaptive search needs the same effective-query vector for semantic
        // retrieval, affinity lookup, and session persistence. Resolve it
        // outside the base-result cache so every call gets the vector; the
        // persistent CachedEmbedder absorbs repeated provider work.
        let effective_query_embedding = if memory_enabled {
            let embeddings = daemon.cache.embed(&[&query]).await?;
            Some(
                embeddings
                    .into_iter()
                    .next()
                    .ok_or_else(|| CoreError::Config("empty embedding response".into()))?,
            )
        } else {
            None
        };

        let entries = daemon.entries.clone();
        let chunks = daemon.chunks.clone();
        let embed = daemon.cache.clone();

        // Snapshot for the compute closure.
        let bt_for_compute = block_type.clone();
        let tag_for_compute = tag.clone();
        let query_for_compute = query.clone();
        let query_embedding_for_compute = effective_query_embedding.clone();
        let cache_limit = if memory_enabled { base_recall } else { limit };
        let fusion_limit = cache_limit;

        let weights_hash = Some(crate::search_cache::hash_weights(fw, sw));
        let cached = daemon
            .search_cache
            .lookup_or_compute(
                SearchRpc::Hybrid,
                &query,
                cache_limit,
                block_type.as_deref(),
                tag.as_deref(),
                weights_hash,
                || {
                    let entries = entries.clone();
                    let chunks = chunks.clone();
                    let embed = embed.clone();
                    let bt = bt_for_compute.clone();
                    let tg = tag_for_compute.clone();
                    let q = query_for_compute.clone();
                    let precomputed_query_embedding = query_embedding_for_compute.clone();
                    async move {
                        // Expand recall window to 2*limit for RRF boundary cases.
                        let recall = base_recall;

                        // Parallel fulltext + semantic.
                        let (ft_result, sem_result) = tokio::try_join!(
                            // Fulltext (sync, wrap in blocking)
                            {
                                let entries = entries.clone();
                                let q = q.clone();
                                let bt = bt.clone();
                                let tg = tg.clone();
                                async move {
                                    blocking(move || {
                                        if include_benchmark {
                                            entries.fulltext_search_with_benchmark(
                                                &q,
                                                recall,
                                                bt.as_deref(),
                                                tg.as_deref(),
                                            )
                                        } else {
                                            entries.fulltext_search(
                                                &q,
                                                recall,
                                                bt.as_deref(),
                                                tg.as_deref(),
                                            )
                                        }
                                    })
                                    .await?
                                }
                            },
                            // Semantic (async embed + sync search)
                            {
                                let embed = embed.clone();
                                let chunks = chunks.clone();
                                let q = q.clone();
                                let bt = bt.clone();
                                let tg = tg.clone();
                                let precomputed_query_embedding =
                                    precomputed_query_embedding.clone();
                                async move {
                                    let qvec = if let Some(qvec) = precomputed_query_embedding {
                                        qvec
                                    } else {
                                        let embeddings = embed.embed(&[&q]).await?;
                                        embeddings.into_iter().next().ok_or_else(|| {
                                            CoreError::Config("empty embedding response".into())
                                        })?
                                    };
                                    blocking(move || {
                                        if include_benchmark {
                                            chunks.semantic_search_with_benchmark(
                                                &qvec,
                                                recall as usize,
                                                bt.as_deref(),
                                                tg.as_deref(),
                                            )
                                        } else {
                                            chunks.semantic_search(
                                                &qvec,
                                                recall as usize,
                                                bt.as_deref(),
                                                tg.as_deref(),
                                            )
                                        }
                                    })
                                    .await?
                                }
                            },
                        )?;

                        // Build ranked (entry_id, score) pairs for RRF.
                        // Fulltext: entry-level scores from FulltextSearchResult.
                        let ft_pairs: Vec<(Ulid, f32)> =
                            ft_result.iter().map(|r| (r.entry.id, r.score)).collect();

                        // Semantic: entry dedup — take highest-scoring chunk per entry.
                        let mut sem_map: std::collections::HashMap<Ulid, f32> =
                            std::collections::HashMap::new();
                        for r in &sem_result {
                            let e = sem_map.entry(r.entry_id).or_insert(r.score);
                            if r.score > *e {
                                *e = r.score;
                            }
                        }
                        let mut sem_pairs: Vec<(Ulid, f32)> = sem_map.into_iter().collect();
                        sem_pairs.sort_by(|a, b| {
                            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
                        });

                        // RRF fuse.
                        let fused = nomai_core::rrf_fuse(
                            &ft_pairs,
                            &sem_pairs,
                            [fw, sw],
                            fusion_limit as usize,
                        );

                        // Build HybridSearchResults: fetch entry + best chunk/block per fused id.
                        let ft_by_id: std::collections::HashMap<
                            Ulid,
                            &nomai_core::FulltextSearchResult,
                        > = ft_result.iter().map(|r| (r.entry.id, r)).collect();
                        let sem_by_entry: std::collections::HashMap<
                            Ulid,
                            &nomai_core::ChunkSearchResult,
                        > = {
                            let mut m = std::collections::HashMap::new();
                            for r in &sem_result {
                                m.entry(r.entry_id).or_insert(r);
                            }
                            m
                        };

                        let mut items = Vec::with_capacity(fused.len());
                        for (entry_id, fusion_score) in &fused {
                            // Fetch entry (blocking).
                            let entry = {
                                let entries = entries.clone();
                                let id = *entry_id;
                                blocking(move || {
                                    if include_benchmark {
                                        entries.get_with_benchmark(id)
                                    } else {
                                        entries.get(id)
                                    }
                                })
                                .await??
                            };

                            let ft_rank = ft_pairs
                                .iter()
                                .position(|(id, _)| id == entry_id)
                                .map(|p| p as u32 + 1)
                                .unwrap_or(0);
                            let ft_score = ft_by_id.get(entry_id).map(|r| r.score).unwrap_or(0.0);

                            let sem_rank = sem_pairs
                                .iter()
                                .position(|(id, _)| id == entry_id)
                                .map(|p| p as u32 + 1)
                                .unwrap_or(0);
                            let sem_score =
                                sem_by_entry.get(entry_id).map(|r| r.score).unwrap_or(0.0);

                            let matched_chunk =
                                sem_by_entry.get(entry_id).map(|r| nomai_core::ChunkRef {
                                    id: r.chunk.id,
                                    text: r.chunk.text.clone(),
                                });

                            let matched_block =
                                ft_by_id.get(entry_id).map(|r| nomai_core::BlockRef {
                                    id: r.best_match.block_id,
                                    r#type: r.best_match.block_type.clone(),
                                    snippet: r.best_match.snippet.clone(),
                                });

                            items.push(serialize_hybrid_result(&nomai_core::HybridSearchResult {
                                entry,
                                fusion_score: *fusion_score,
                                fulltext_rank: ft_rank,
                                fulltext_score: ft_score,
                                semantic_rank: sem_rank,
                                semantic_score: sem_score,
                                matched_chunk,
                                matched_block,
                            }));
                        }

                        Ok(items)
                    }
                },
            )
            .await?;

        let base_items: Vec<Value> = cached.as_ref().clone();
        if !memory_enabled {
            let ids: Vec<String> = base_items
                .iter()
                .filter_map(|v| v["entry"]["id"].as_str().map(String::from))
                .collect();
            let transient = transient_set(&daemon.entries, ids).await?;
            // Legacy hybrid behavior mutates fusion_score directly and keeps
            // the historical {items} response shape when memory is disabled.
            let items = downrank_hybrid(base_items, &transient);
            return Ok(json!({ "items": items }));
        }

        let query_embedding = effective_query_embedding
            .ok_or_else(|| CoreError::Config("adaptive query embedding missing".into()))?;
        let mut base_ids = Vec::with_capacity(base_items.len());
        let mut base_id_set = std::collections::HashSet::with_capacity(base_items.len());
        for item in &base_items {
            let id = hybrid_entry_id(item)?;
            if base_id_set.insert(id) {
                base_ids.push(id);
            }
        }

        let supplement_limit = daemon.memory.policy().affinity_candidate_limit;
        let affinities = {
            let memory = daemon.memory.clone();
            let query_embedding = query_embedding.clone();
            let embedding_model = daemon.embedding_model.clone();
            let required_entry_ids = base_ids.clone();
            let block_type = block_type.clone();
            let tag = tag.clone();
            blocking(move || {
                memory.affinity_candidates_with_required_entries(
                    &query_embedding,
                    &embedding_model,
                    &required_entry_ids,
                    supplement_limit,
                    block_type.as_deref(),
                    tag.as_deref(),
                    include_benchmark,
                )
            })
            .await??
        };

        let mut supplemental = Vec::with_capacity(supplement_limit);
        for hit in &affinities {
            if base_id_set.contains(&hit.entry_id) {
                continue;
            }
            if supplemental.len() == supplement_limit {
                break;
            }
            supplemental.push(
                serialize_affinity_candidate(
                    daemon.entries.clone(),
                    daemon.chunks.clone(),
                    hit.clone(),
                    include_benchmark,
                )
                .await?,
            );
        }

        let mut candidate_ids = base_ids;
        for item in &supplemental {
            let id = hybrid_entry_id(item)?;
            if base_id_set.insert(id) {
                candidate_ids.push(id);
            }
        }
        let signals = {
            let memory = daemon.memory.clone();
            let ids = candidate_ids.clone();
            blocking(move || memory.entry_memory_signals(&ids)).await??
        };
        if let Some(missing) = candidate_ids
            .iter()
            .find(|entry_id| !signals.contains_key(entry_id))
        {
            return Err(CoreError::NotFound(*missing));
        }

        let transient_strings = transient_set(
            &daemon.entries,
            candidate_ids.iter().map(ToString::to_string).collect(),
        )
        .await?;
        let transient = transient_strings
            .into_iter()
            .map(|id| {
                id.parse().map_err(|_| {
                    CoreError::Config(format!(
                        "invalid transient entry id returned by storage: {id}"
                    ))
                })
            })
            .collect::<Result<std::collections::HashSet<Ulid>, CoreError>>()?;

        let items = crate::adaptive_search::rank_candidates(
            base_items,
            supplemental,
            affinities,
            signals,
            transient,
            limit as usize,
            supplement_limit,
        );
        let session_results = items
            .iter()
            .enumerate()
            .map(|(rank, item)| {
                Ok(nomai_core::SearchResultTarget {
                    entry_id: hybrid_entry_id(item)?,
                    matched_block_id: optional_matched_id(item, "matched_block")?,
                    matched_chunk_id: optional_matched_id(item, "matched_chunk")?,
                    result_rank: rank as u32 + 1,
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        let session = nomai_core::CreateSearchSession {
            raw_query_text: raw_query,
            effective_query_text: query,
            query_embedding,
            embedding_model: daemon.embedding_model.clone(),
            results: session_results,
        };
        let search_id = {
            let memory = daemon.memory.clone();
            blocking(move || memory.create_search_session(session)).await??
        };

        Ok(json!({ "search_id": search_id.to_string(), "items": items }))
    }
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use nomai_core::{
        BlockInput, CreateEntry, CreateSearchSession, Entry, EntryService, FeedbackTarget,
        MemoryPolicy, SearchResultTarget,
    };
    use nomai_providers::{CompletionResponse, ProviderError};

    use crate::daemon::DaemonBuilder;

    struct RecordingEmbed {
        calls: AtomicUsize,
        texts: Mutex<Vec<String>>,
        vector: Vec<f32>,
    }

    impl RecordingEmbed {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                texts: Mutex::new(Vec::new()),
                vector: vec![1.0, 0.0, 0.0, 0.0],
            }
        }
    }

    #[async_trait]
    impl EmbeddingProvider for RecordingEmbed {
        async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
            self.calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.texts
                .lock()
                .unwrap()
                .extend(texts.iter().map(|text| (*text).to_string()));
            Ok(vec![self.vector.clone(); texts.len()])
        }

        fn dim(&self) -> usize {
            self.vector.len()
        }

        fn name(&self) -> &str {
            "recording-embed"
        }
    }

    struct NullLlm;

    #[async_trait]
    impl LlmProvider for NullLlm {
        async fn complete(
            &self,
            _req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            Err(ProviderError::new(
                nomai_providers::ProviderErrorKind::Unknown,
                "null llm",
                None,
            ))
        }

        fn name(&self) -> &str {
            "null-llm"
        }
    }

    struct RewriteLlm;

    #[async_trait]
    impl LlmProvider for RewriteLlm {
        async fn complete(
            &self,
            _req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            Ok(CompletionResponse {
                content: "expanded retrieval query".into(),
            })
        }

        fn name(&self) -> &str {
            "rewrite-llm"
        }
    }

    fn search_daemon(
        memory_enabled: bool,
        llm: Arc<dyn LlmProvider>,
    ) -> (Daemon, Arc<RecordingEmbed>) {
        let entries = Arc::new(EntryService::for_test().unwrap());
        let embed = Arc::new(RecordingEmbed::new());
        let policy = MemoryPolicy {
            enabled: memory_enabled,
            ..MemoryPolicy::default()
        };
        let daemon = DaemonBuilder::new()
            .conn(entries.conn_for_test())
            .content_store(entries.content_store().clone())
            .embedder(embed.clone())
            .llm(llm)
            .embedding_dim(4)
            .chunk_target_size(1024)
            .cache_model("test-embed")
            .warn_rows(100_000)
            .memory_policy(policy)
            .build()
            .unwrap();
        (daemon, embed)
    }

    fn seed_search_entry(
        daemon: &Daemon,
        title: &str,
        block_type: &str,
        text: &str,
        tags: &[&str],
        attrs: Option<Value>,
    ) -> Entry {
        daemon
            .entries
            .create(CreateEntry {
                title: title.into(),
                blocks: vec![BlockInput {
                    r#type: block_type.into(),
                    text: text.into(),
                    attrs: None,
                }],
                tags: (!tags.is_empty())
                    .then(|| tags.iter().map(|tag| (*tag).to_string()).collect()),
                attrs,
                source: None,
                attachments: None,
            })
            .unwrap()
    }

    fn base_candidate(
        entry: &Entry,
        fusion_score: f64,
        matched_block_id: Option<Ulid>,
        matched_chunk_id: Option<Ulid>,
    ) -> Value {
        let mut item = json!({
            "entry": entry,
            "fusion_score": fusion_score,
            "fulltext_rank": 1,
            "fulltext_score": 1.0,
            "semantic_rank": 1,
            "semantic_score": 1.0,
        });
        if let Some(block_id) = matched_block_id {
            item["matched_block"] = json!({
                "id": block_id,
                "type": entry.blocks[0].r#type,
                "snippet": entry.blocks[0].text,
            });
        }
        if let Some(chunk_id) = matched_chunk_id {
            item["matched_chunk"] = json!({
                "id": chunk_id,
                "text": entry.blocks[0].text,
            });
        }
        item
    }

    async fn prime_hybrid_cache(daemon: &Daemon, query: &str, cache_limit: u32, items: Vec<Value>) {
        daemon
            .search_cache
            .lookup_or_compute(
                SearchRpc::Hybrid,
                query,
                cache_limit,
                None,
                None,
                Some(crate::search_cache::hash_weights(1.0, 1.0)),
                || async move { Ok(items) },
            )
            .await
            .unwrap();
    }

    fn learn_precise_affinity(daemon: &Daemon, entry: &Entry, query: &str) -> (Ulid, Ulid) {
        learn_precise_affinity_with_embedding(daemon, entry, query, &[1.0, 0.0, 0.0, 0.0])
    }

    fn learn_precise_affinity_with_embedding(
        daemon: &Daemon,
        entry: &Entry,
        query: &str,
        query_embedding: &[f32],
    ) -> (Ulid, Ulid) {
        let block_id = entry.blocks[0].id;
        let chunk_id = daemon.chunks.list_with_benchmark(block_id).unwrap().items[0].id;
        let search_id = daemon
            .memory
            .create_search_session(CreateSearchSession {
                raw_query_text: query.into(),
                effective_query_text: query.into(),
                query_embedding: query_embedding.to_vec(),
                embedding_model: "test-embed".into(),
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
        (block_id, chunk_id)
    }

    fn table_count(daemon: &Daemon, table: &str) -> i64 {
        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    #[tokio::test]
    async fn disabled_memory_preserves_legacy_hybrid_shape_order_and_transient_penalty() {
        let (daemon, _) = search_daemon(false, Arc::new(NullLlm));
        let durable = seed_search_entry(&daemon, "durable", "note", "durable body", &[], None);
        let transient = seed_search_entry(
            &daemon,
            "transient",
            "note",
            "transient body",
            &[],
            Some(json!({"transient": true})),
        );
        prime_hybrid_cache(
            &daemon,
            "legacy-query",
            2,
            vec![
                base_candidate(&transient, 0.03, None, None),
                base_candidate(&durable, 0.02, None, None),
            ],
        )
        .await;

        let response = Hybrid
            .call(&daemon, json!({"query": "legacy-query", "limit": 2}))
            .await
            .unwrap();

        assert_eq!(response.as_object().unwrap().len(), 1);
        assert!(response.get("search_id").is_none());
        assert_eq!(response["items"][0]["entry"]["id"], durable.id.to_string());
        assert_eq!(
            response["items"][1]["entry"]["id"],
            transient.id.to_string()
        );
        assert!((response["items"][1]["fusion_score"].as_f64().unwrap() - 0.015).abs() < 1e-9);
        assert!(response["items"][0].get("score").is_none());
        assert_eq!(table_count(&daemon, "search_sessions"), 0);
    }

    #[tokio::test]
    async fn enabled_identical_hybrid_calls_reuse_base_and_embedding_but_create_fresh_sessions() {
        let (daemon, embed) = search_daemon(true, Arc::new(NullLlm));
        let entry = seed_search_entry(
            &daemon,
            "cache target",
            "note",
            "session cache needle",
            &[],
            None,
        );
        let block_id = entry.blocks[0].id;
        let chunk_id = daemon.chunks.list(block_id).unwrap().items[0].id;
        daemon
            .chunks
            .write_embedding(chunk_id, &[1.0, 0.0, 0.0, 0.0])
            .unwrap();

        let params = json!({"query": "session cache needle", "limit": 1});
        let first = Hybrid.call(&daemon, params.clone()).await.unwrap();
        let second = Hybrid.call(&daemon, params).await.unwrap();
        let first_id = first["search_id"].as_str().unwrap();
        let second_id = second["search_id"].as_str().unwrap();

        assert_ne!(first_id, second_id);
        assert_eq!(first["items"][0]["entry"]["id"], entry.id.to_string());
        assert!(first["items"][0]["score"].as_f64().is_some());
        assert!(first["items"][0]["fusion_score"].as_f64().is_some());
        assert!(first["items"][0]["memory_factor"].as_f64().is_some());
        assert_eq!(first["items"][0]["signals"]["affinity_matched"], false);

        let search_stats = daemon.search_cache.stats();
        assert_eq!(search_stats.hybrid_misses, 1);
        assert_eq!(search_stats.hybrid_hits, 1);
        let embedding_stats = daemon.cache.stats().unwrap();
        assert_eq!(embedding_stats.misses, 1);
        assert_eq!(embedding_stats.hits, 1);
        assert_eq!(embed.calls.load(AtomicOrdering::Relaxed), 1);

        assert_eq!(table_count(&daemon, "search_sessions"), 2);
        assert_eq!(table_count(&daemon, "search_session_results"), 2);
        assert_eq!(table_count(&daemon, "entry_memory_stats"), 0);
        assert_eq!(table_count(&daemon, "query_affinities"), 0);
        assert_eq!(table_count(&daemon, "search_feedback"), 0);

        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let persisted: (String, Option<String>, Option<String>, i64) = conn
            .query_row(
                "SELECT entry_id, matched_block_id, matched_chunk_id, result_rank
                 FROM search_session_results WHERE search_id = ?1",
                [first_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(persisted.0, entry.id.to_string());
        assert_eq!(persisted.1, Some(block_id.to_string()));
        assert_eq!(persisted.2, Some(chunk_id.to_string()));
        assert_eq!(persisted.3, 1);
    }

    #[tokio::test]
    async fn chunk_only_feedback_uses_semantic_chunks_actual_parent_in_a_multi_block_result() {
        let (daemon, _) = search_daemon(true, Arc::new(NullLlm));
        let entry = daemon
            .entries
            .create(CreateEntry {
                title: "split precision target".into(),
                blocks: vec![
                    BlockInput {
                        r#type: "note".into(),
                        text: "lexical precision needle".into(),
                        attrs: None,
                    },
                    BlockInput {
                        r#type: "evidence".into(),
                        text: "semantic-only supporting material".into(),
                        attrs: None,
                    },
                ],
                tags: None,
                attrs: None,
                source: None,
                attachments: None,
            })
            .unwrap();
        let fulltext_block_id = entry.blocks[0].id;
        let semantic_block_id = entry.blocks[1].id;
        let fulltext_chunk_id = daemon.chunks.list(fulltext_block_id).unwrap().items[0].id;
        let semantic_chunk_id = daemon.chunks.list(semantic_block_id).unwrap().items[0].id;
        daemon
            .chunks
            .write_embedding(fulltext_chunk_id, &[0.0, 1.0, 0.0, 0.0])
            .unwrap();
        daemon
            .chunks
            .write_embedding(semantic_chunk_id, &[1.0, 0.0, 0.0, 0.0])
            .unwrap();

        let search = Hybrid
            .call(
                &daemon,
                json!({"query": "lexical precision needle", "limit": 1}),
            )
            .await
            .unwrap();
        assert_eq!(
            search["items"][0]["matched_block"]["id"],
            fulltext_block_id.to_string()
        );
        assert_eq!(
            search["items"][0]["matched_chunk"]["id"],
            semantic_chunk_id.to_string()
        );

        let feedback = crate::handlers::feedback::Feedback
            .call(
                &daemon,
                json!({
                    "search_id": search["search_id"],
                    "targets": [{
                        "entry_id": entry.id,
                        "chunk_id": semantic_chunk_id,
                    }],
                }),
            )
            .await
            .unwrap();

        assert_eq!(feedback["applied"][0]["entry_id"], entry.id.to_string());
        let stored: (Option<String>, Option<String>) = daemon
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT block_id, chunk_id FROM query_affinities WHERE entry_id = ?1",
                [entry.id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            stored,
            (
                Some(semantic_block_id.to_string()),
                Some(semantic_chunk_id.to_string()),
            )
        );
    }

    #[tokio::test]
    async fn enabled_hybrid_ranks_wide_cached_base_before_caller_limit() {
        let (daemon, _) = search_daemon(true, Arc::new(NullLlm));
        let stale = seed_search_entry(&daemon, "stale", "note", "stale body", &[], None);
        let fresh = seed_search_entry(&daemon, "fresh", "note", "fresh body", &[], None);
        {
            let conn = daemon.entries.conn_for_test();
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE entries SET created_at = '2000-01-01T00:00:00Z' WHERE id = ?1",
                [stale.id.to_string()],
            )
            .unwrap();
        }
        prime_hybrid_cache(
            &daemon,
            "wide-base-query",
            20,
            vec![
                base_candidate(&stale, 0.020, None, None),
                base_candidate(&fresh, 0.019, None, None),
            ],
        )
        .await;

        let response = Hybrid
            .call(&daemon, json!({"query": "wide-base-query", "limit": 1}))
            .await
            .unwrap();

        assert_eq!(response["items"].as_array().unwrap().len(), 1);
        assert_eq!(response["items"][0]["entry"]["id"], fresh.id.to_string());
        assert!((response["items"][0]["fusion_score"].as_f64().unwrap() - 0.019).abs() < 1e-9);
        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let persisted_entry: String = conn
            .query_row(
                "SELECT entry_id FROM search_session_results WHERE search_id = ?1",
                [response["search_id"].as_str().unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(persisted_entry, fresh.id.to_string());
    }

    #[tokio::test]
    async fn enabled_hybrid_rewrites_before_embedding_and_persists_both_queries() {
        let (daemon, embed) = search_daemon(true, Arc::new(RewriteLlm));
        let entry = seed_search_entry(&daemon, "rewrite target", "note", "body", &[], None);
        prime_hybrid_cache(
            &daemon,
            "expanded retrieval query",
            20,
            vec![base_candidate(&entry, 0.02, None, None)],
        )
        .await;

        let response = Hybrid
            .call(
                &daemon,
                json!({
                    "query": "what about that",
                    "limit": 1,
                    "rewrite": "expand",
                    "rewrite_context": "Nomai retrieval"
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            embed.texts.lock().unwrap().as_slice(),
            &["expanded retrieval query".to_string()]
        );
        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let queries: (String, String) = conn
            .query_row(
                "SELECT raw_query_text, effective_query_text FROM search_sessions WHERE id = ?1",
                [response["search_id"].as_str().unwrap()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(queries.0, "what about that");
        assert_eq!(queries.1, "expanded retrieval query");
    }

    #[tokio::test]
    async fn affinity_supplements_respect_tag_block_type_and_benchmark_visibility() {
        let (daemon, _) = search_daemon(true, Arc::new(NullLlm));
        let allowed = seed_search_entry(
            &daemon,
            "allowed",
            "note",
            "ordinary allowed body",
            &["wanted"],
            None,
        );
        let wrong_tag = seed_search_entry(
            &daemon,
            "wrong tag",
            "note",
            "ordinary wrong tag body",
            &["other"],
            None,
        );
        let wrong_type = seed_search_entry(
            &daemon,
            "wrong type",
            "claim",
            "ordinary wrong type body",
            &["wanted"],
            None,
        );
        let benchmark = seed_search_entry(
            &daemon,
            "benchmark",
            "note",
            "benchmark hidden body",
            &["wanted"],
            Some(json!({
                "benchmark_run_id": "run-1",
                "benchmark_case_id": "case-1"
            })),
        );
        let (allowed_block, allowed_chunk) =
            learn_precise_affinity(&daemon, &allowed, "learned source query");
        learn_precise_affinity(&daemon, &wrong_tag, "learned source query");
        learn_precise_affinity(&daemon, &wrong_type, "learned source query");
        learn_precise_affinity(&daemon, &benchmark, "learned source query");

        let response = Hybrid
            .call(
                &daemon,
                json!({
                    "query": "zzzxxyy",
                    "limit": 10,
                    "tag": "wanted",
                    "block_type": "note"
                }),
            )
            .await
            .unwrap();

        let items = response["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["entry"]["id"], allowed.id.to_string());
        assert_eq!(items[0]["matched_block"]["id"], allowed_block.to_string());
        assert_eq!(items[0]["matched_chunk"]["id"], allowed_chunk.to_string());
        assert_eq!(items[0]["fusion_score"], 0.0);
        assert_eq!(items[0]["signals"]["affinity_matched"], true);

        let conn = daemon.entries.conn_for_test();
        let conn = conn.lock().unwrap();
        let persisted: (String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT entry_id, matched_block_id, matched_chunk_id
                 FROM search_session_results WHERE search_id = ?1",
                [response["search_id"].as_str().unwrap()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(persisted.0, allowed.id.to_string());
        assert_eq!(persisted.1, Some(allowed_block.to_string()));
        assert_eq!(persisted.2, Some(allowed_chunk.to_string()));
    }

    #[tokio::test]
    async fn stronger_supplements_do_not_crowd_out_base_entry_affinity_score() {
        let (daemon, _) = search_daemon(true, Arc::new(NullLlm));
        let base = seed_search_entry(&daemon, "base", "note", "base body", &[], None);
        let strongest =
            seed_search_entry(&daemon, "strongest", "note", "strongest body", &[], None);
        let second = seed_search_entry(&daemon, "second", "note", "second body", &[], None);
        let third = seed_search_entry(&daemon, "third", "note", "third body", &[], None);
        learn_precise_affinity_with_embedding(
            &daemon,
            &strongest,
            "strongest source",
            &[1.0, 0.0, 0.0, 0.0],
        );
        learn_precise_affinity_with_embedding(
            &daemon,
            &second,
            "second source",
            &[0.98, 0.198_997_49, 0.0, 0.0],
        );
        learn_precise_affinity_with_embedding(
            &daemon,
            &third,
            "third source",
            &[0.96, 0.28, 0.0, 0.0],
        );
        learn_precise_affinity_with_embedding(
            &daemon,
            &base,
            "base source",
            &[0.90, 0.435_889_9, 0.0, 0.0],
        );
        prime_hybrid_cache(
            &daemon,
            "crowded-affinity-query",
            20,
            vec![base_candidate(&base, 0.02, None, None)],
        )
        .await;

        let response = Hybrid
            .call(
                &daemon,
                json!({"query": "crowded-affinity-query", "limit": 10}),
            )
            .await
            .unwrap();

        let items = response["items"].as_array().unwrap();
        assert_eq!(items.len(), 3, "one base plus two learned supplements");
        let base_item = items
            .iter()
            .find(|item| item["entry"]["id"] == base.id.to_string())
            .unwrap();
        assert!(base_item["affinity_score"].as_f64().unwrap() > 0.004);
        assert!(
            items
                .iter()
                .any(|item| item["entry"]["id"] == strongest.id.to_string())
        );
        assert!(
            items
                .iter()
                .any(|item| item["entry"]["id"] == second.id.to_string())
        );
        assert!(
            !items
                .iter()
                .any(|item| item["entry"]["id"] == third.id.to_string())
        );
    }

    #[tokio::test]
    async fn fulltext_and_semantic_shapes_remain_non_adaptive() {
        let (daemon, _) = search_daemon(true, Arc::new(NullLlm));
        let entry = seed_search_entry(&daemon, "shape target", "note", "shape sentinel", &[], None);
        let chunk_id = daemon.chunks.list(entry.blocks[0].id).unwrap().items[0].id;
        daemon
            .chunks
            .write_embedding(chunk_id, &[1.0, 0.0, 0.0, 0.0])
            .unwrap();

        let fulltext = Fulltext
            .call(&daemon, json!({"query": "shape sentinel", "limit": 1}))
            .await
            .unwrap();
        let semantic = Semantic
            .call(&daemon, json!({"query": "shape sentinel", "limit": 1}))
            .await
            .unwrap();

        assert_eq!(fulltext.as_object().unwrap().len(), 1);
        assert_eq!(semantic.as_object().unwrap().len(), 1);
        assert!(fulltext.get("search_id").is_none());
        assert!(semantic.get("search_id").is_none());
        assert!(fulltext["items"][0].get("memory_factor").is_none());
        assert!(semantic["items"][0].get("memory_factor").is_none());
        assert_eq!(table_count(&daemon, "search_sessions"), 0);
    }

    #[tokio::test]
    async fn failed_search_session_write_rolls_back_without_orphan() {
        let (daemon, _) = search_daemon(true, Arc::new(NullLlm));
        let entry = seed_search_entry(&daemon, "rollback target", "note", "body", &[], None);
        prime_hybrid_cache(
            &daemon,
            "session-write-failure",
            20,
            vec![base_candidate(&entry, 0.02, None, None)],
        )
        .await;
        {
            let conn = daemon.entries.conn_for_test();
            let conn = conn.lock().unwrap();
            conn.execute_batch("DROP TABLE search_session_results")
                .unwrap();
        }

        let result = Hybrid
            .call(
                &daemon,
                json!({"query": "session-write-failure", "limit": 1}),
            )
            .await;

        assert!(matches!(result, Err(CoreError::Storage(_))));
        assert_eq!(table_count(&daemon, "search_sessions"), 0);
    }

    fn downrank_transient_fulltext_for_test(
        items: Vec<Value>,
        svc: &nomai_core::EntryService,
    ) -> Result<Vec<Value>, CoreError> {
        let ids: Vec<String> = items
            .iter()
            .filter_map(|v| v["entry"]["id"].as_str().map(String::from))
            .collect();
        let set = svc.transient_ids_among(&ids)?;
        Ok(downrank_fulltext(items, &set))
    }

    #[test]
    fn downrank_demotes_transient_and_resorts() {
        let svc = nomai_core::EntryService::for_test().unwrap();
        svc.create(nomai_core::CreateEntry {
            title: "long".into(),
            blocks: vec![nomai_core::BlockInput {
                r#type: "note".into(),
                text: "rust ownership borrow".into(),
                attrs: None,
            }],
            tags: None,
            attrs: None,
            source: None,
            attachments: None,
        })
        .unwrap();
        svc.create(nomai_core::CreateEntry {
            title: "short".into(),
            blocks: vec![nomai_core::BlockInput {
                r#type: "note".into(),
                text: "rust ownership borrow".into(),
                attrs: None,
            }],
            tags: None,
            attrs: Some(json!({"transient": true})),
            source: None,
            attachments: None,
        })
        .unwrap();
        let hits = svc.fulltext_search("rust", 10, None, None).unwrap();
        let items: Vec<Value> = hits.iter().map(serialize_fulltext_result).collect();
        let demoted = downrank_transient_fulltext_for_test(items, &svc).unwrap();
        // 长期必须排到第一(其 score 未变,短期 score *= 0.5 沉后)
        assert_eq!(demoted[0]["entry"]["title"].as_str(), Some("long"));
        // 短期 score 被乘 0.5
        let short_item = demoted
            .iter()
            .find(|v| v["entry"]["title"].as_str() == Some("short"))
            .unwrap();
        let demoted_score = short_item["score"].as_f64().unwrap();
        let orig = hits
            .iter()
            .find(|h| h.entry.title == "short")
            .unwrap()
            .score as f64;
        assert!((demoted_score - orig * 0.5).abs() < 1e-5);
    }

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params)
            .map_err(|errs| errs.map(|e| format!("{e}")).collect::<Vec<_>>())
    }

    #[test]
    fn hybrid_schema_accepts_minimal_params() {
        let schema = Hybrid.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "hello"})).is_ok());
    }

    #[test]
    fn hybrid_schema_accepts_weights() {
        let schema = Hybrid.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "hello", "fulltext_weight": 2.0})).is_ok());
    }

    #[test]
    fn hybrid_schema_rejects_missing_query() {
        let schema = Hybrid.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn fulltext_schema_accepts_query() {
        let schema = Fulltext.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "hello"})).is_ok());
    }

    #[test]
    fn fulltext_schema_rejects_missing_query() {
        let schema = Fulltext.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn semantic_schema_accepts_query() {
        let schema = Semantic.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "hello"})).is_ok());
    }

    #[test]
    fn semantic_schema_rejects_missing_query() {
        let schema = Semantic.input_schema().unwrap();
        assert!(validate(&schema, &json!({})).is_err());
    }

    #[test]
    fn fulltext_schema_accepts_tag() {
        let schema = Fulltext.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "hello", "tag": "nomai"})).is_ok());
    }

    #[test]
    fn semantic_schema_accepts_tag() {
        let schema = Semantic.input_schema().unwrap();
        assert!(validate(&schema, &json!({"query": "hello", "tag": "nomai"})).is_ok());
    }

    #[test]
    fn normalize_tag_trims_and_drops_empty() {
        assert_eq!(normalize_tag(None), None);
        assert_eq!(normalize_tag(Some(String::new())), None);
        assert_eq!(normalize_tag(Some("   ".into())), None);
        assert_eq!(
            normalize_tag(Some("\t nomai \n".into())),
            Some("nomai".into())
        );
        assert_eq!(normalize_tag(Some("nomai".into())), Some("nomai".into()));
    }

    #[test]
    fn fulltext_wire_serializes_new_fields() {
        // Drive a real fulltext_search result so we don't hand-construct
        // Entry/Ulid/DateTime — only the wire mapping is under test here.
        let svc = nomai_core::EntryService::for_test().unwrap();
        svc.create(nomai_core::CreateEntry {
            title: "t".into(),
            blocks: vec![nomai_core::BlockInput {
                r#type: "note".into(),
                text: "the setsid call".into(),
                attrs: None,
            }],
            tags: None,
            attrs: None,
            source: None,
            attachments: None,
        })
        .unwrap();
        let results = svc.fulltext_search("setsid", 10, None, None).unwrap();
        let v = serialize_fulltext_result(&results[0]);
        assert!(v.get("entry").is_some());
        assert!(v["match_count"].as_u64().is_some());
        assert!(v["matched_block_ids"].is_array());
        assert_eq!(v["best_match"]["block_type"], "note");
        assert!(
            v["best_match"]["snippet"]
                .as_str()
                .unwrap()
                .contains("**setsid**")
        );
    }

    // --- hybrid transient demotion ---

    fn downrank_transient_hybrid_for_test(
        items: Vec<Value>,
        svc: &nomai_core::EntryService,
    ) -> Result<Vec<Value>, CoreError> {
        let ids: Vec<String> = items
            .iter()
            .filter_map(|v| v["entry"]["id"].as_str().map(String::from))
            .collect();
        let set = svc.transient_ids_among(&ids)?;
        Ok(downrank_hybrid(items, &set))
    }

    #[test]
    fn downrank_demotes_transient_hybrid_and_resorts() {
        let svc = nomai_core::EntryService::for_test().unwrap();
        svc.create(nomai_core::CreateEntry {
            title: "long".into(),
            blocks: vec![nomai_core::BlockInput {
                r#type: "note".into(),
                text: "rust ownership borrow".into(),
                attrs: None,
            }],
            tags: None,
            attrs: None,
            source: None,
            attachments: None,
        })
        .unwrap();
        svc.create(nomai_core::CreateEntry {
            title: "short".into(),
            blocks: vec![nomai_core::BlockInput {
                r#type: "note".into(),
                text: "rust ownership borrow".into(),
                attrs: None,
            }],
            tags: None,
            attrs: Some(json!({"transient": true})),
            source: None,
            attachments: None,
        })
        .unwrap();
        // Fulltext search yields entry-level scores; rebadge as fusion_score
        // since that's what downrank_hybrid reads.
        let hits = svc.fulltext_search("rust", 10, None, None).unwrap();
        let mut items: Vec<Value> = hits.iter().map(serialize_fulltext_result).collect();
        for item in &mut items {
            let score = item["score"].as_f64().unwrap_or(0.0);
            item["fusion_score"] = json!(score);
        }
        let demoted = downrank_transient_hybrid_for_test(items, &svc).unwrap();
        // Non-transient must sort to top.
        assert_eq!(demoted[0]["entry"]["title"].as_str(), Some("long"));
        // Transient entry's fusion_score must be halved.
        let short_item = demoted
            .iter()
            .find(|v| v["entry"]["title"].as_str() == Some("short"))
            .unwrap();
        let demoted_score = short_item["fusion_score"].as_f64().unwrap();
        let orig: f64 = hits
            .iter()
            .find(|h| h.entry.title == "short")
            .map(|h| h.score as f64)
            .unwrap_or(0.0);
        assert!((demoted_score - orig * 0.5).abs() < 1e-5);
    }

    // --- cache key isolation by weights ---

    #[tokio::test]
    async fn cache_key_isolation_by_weights() {
        use crate::search_cache::hash_weights;

        let cache = crate::search_cache::SearchCache::new();
        let _ = cache
            .lookup_or_compute(
                crate::search_cache::SearchRpc::Hybrid,
                "test",
                10,
                None,
                None,
                Some(hash_weights(1.0, 1.0)),
                || async { Ok(vec![json!({"fusion_score": 0.5})]) },
            )
            .await
            .unwrap();
        let _ = cache
            .lookup_or_compute(
                crate::search_cache::SearchRpc::Hybrid,
                "test",
                10,
                None,
                None,
                Some(hash_weights(2.0, 1.0)),
                || async { Ok(vec![json!({"fusion_score": 0.3})]) },
            )
            .await
            .unwrap();
        let stats = cache.stats();
        assert_eq!(
            stats.hybrid_misses, 2,
            "different weights -> 2 distinct keys -> 2 misses"
        );
        assert_eq!(stats.hybrid_hits, 0, "no hits when weights differ");
    }

    // --- query rewrite ---

    #[tokio::test]
    async fn expand_query_returns_original_on_llm_failure() {
        struct FailingLlm;
        #[async_trait]
        impl LlmProvider for FailingLlm {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<nomai_providers::CompletionResponse, nomai_providers::ProviderError>
            {
                Err(nomai_providers::ProviderError::new(
                    nomai_providers::ProviderErrorKind::Network,
                    "down",
                    None,
                ))
            }
            fn name(&self) -> &str {
                "failing"
            }
        }
        let llm: Arc<dyn LlmProvider> = Arc::new(FailingLlm);
        let result = expand_query(&llm, "那玩意怎么用", Some("讨论 nomai chunking")).await;
        assert_eq!(result, "那玩意怎么用");
    }
}
