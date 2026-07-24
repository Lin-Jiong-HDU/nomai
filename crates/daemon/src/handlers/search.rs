//! search.* handlers.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::CoreError;
use nomai_providers::EmbeddingProvider;

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

/// Search demotion penalty for transient entries (Spec §5). Hardcoded for
/// now; promote to config only if a real need appears (YAGNI).
const TRANSIENT_PENALTY: f64 = 0.5;

/// Collect the transient subset among `entry_ids` via a single IN query
/// (policy applied live — never a stale snapshot, Spec §5.2).
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
        let query = p.query;
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

        let query_str = p.query;
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
}
