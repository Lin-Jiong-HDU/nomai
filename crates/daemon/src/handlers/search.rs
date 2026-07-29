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
        if is_t {
            if let Some(s) = it["score"].as_f64() {
                it["score"] = json!(s * TRANSIENT_PENALTY);
            }
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
        if is_t {
            if let Some(s) = it["fusion_score"].as_f64() {
                it["fusion_score"] = json!(s * TRANSIENT_PENALTY);
            }
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
        let query = if p.rewrite.as_deref() == Some("expand") {
            expand_query(&daemon.llm, &p.query, p.rewrite_context.as_deref()).await
        } else {
            p.query
        };
        let limit = p.limit;
        let block_type = p.block_type;
        let tag = normalize_tag(p.tag);
        let fw = p.fulltext_weight;
        let sw = p.semantic_weight;
        let include_benchmark = daemon
            .benchmark
            .as_ref()
            .is_some_and(|runtime| runtime.is_active());

        let entries = daemon.entries.clone();
        let chunks = daemon.chunks.clone();
        let embed = daemon.cache.clone();

        // Snapshot for the compute closure.
        let bt_for_compute = block_type.clone();
        let tag_for_compute = tag.clone();
        let query_for_compute = query.clone();

        let weights_hash = Some(crate::search_cache::hash_weights(fw, sw));
        let cached = daemon
            .search_cache
            .lookup_or_compute(
                SearchRpc::Hybrid,
                &query,
                limit,
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
                    async move {
                        // Expand recall window to 2*limit for RRF boundary cases.
                        let recall = (limit * 2).max(20) as u32;

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
                                async move {
                                    let embeddings = embed.embed(&[&q]).await?;
                                    let qvec = embeddings.into_iter().next().ok_or_else(|| {
                                        CoreError::Config("empty embedding response".into())
                                    })?;
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
                        let fused =
                            nomai_core::rrf_fuse(&ft_pairs, &sem_pairs, [fw, sw], limit as usize);

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

        let items: Vec<Value> = cached.as_ref().clone();
        let ids: Vec<String> = items
            .iter()
            .filter_map(|v| v["entry"]["id"].as_str().map(String::from))
            .collect();
        let transient = transient_set(&daemon.entries, ids).await?;
        // Hybrid items use fusion_score (not score), so call downrank_hybrid.
        let items = downrank_hybrid(items, &transient);
        Ok(json!({ "items": items }))
    }
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

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
