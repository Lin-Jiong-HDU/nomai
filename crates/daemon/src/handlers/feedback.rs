//! search.feedback handler.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use ulid::Ulid;

use nomai_core::{CoreError, FeedbackTarget};
use nomai_protocol::method::search::FEEDBACK;

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::handlers::params::ulid_field_schema;
use crate::rpc::RpcHandler;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FeedbackParams {
    #[schemars(schema_with = "ulid_field_schema")]
    pub search_id: Ulid,
    #[schemars(length(min = 1))]
    pub targets: Vec<FeedbackTargetParams>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FeedbackTargetParams {
    #[schemars(schema_with = "ulid_field_schema")]
    pub entry_id: Ulid,
    #[schemars(with = "Option<String>", regex(pattern = "^[0-9A-HJKMNP-TV-Z]{26}$"))]
    pub block_id: Option<Ulid>,
    #[schemars(with = "Option<String>", regex(pattern = "^[0-9A-HJKMNP-TV-Z]{26}$"))]
    pub chunk_id: Option<Ulid>,
}

/// Record positive feedback for one or more targets returned by a search.
pub struct Feedback;

#[async_trait]
impl RpcHandler for Feedback {
    fn method(&self) -> &'static str {
        FEEDBACK
    }

    fn description(&self) -> &'static str {
        "Record positive feedback for results returned by a prior search. Idempotent per search and entry; returns applied reinforcement records and entries already applied by a retry."
    }

    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(FeedbackParams).to_value())
    }

    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let params: FeedbackParams = serde_json::from_value(params)
            .map_err(|error| CoreError::Validation(format!("invalid params: {error}")))?;

        if !daemon.memory.policy().enabled {
            return Err(CoreError::Config("adaptive memory is disabled".into()));
        }

        let targets = params
            .targets
            .into_iter()
            .map(|target| FeedbackTarget {
                entry_id: target.entry_id,
                block_id: target.block_id,
                chunk_id: target.chunk_id,
            })
            .collect::<Vec<_>>();
        let memory = daemon.memory.clone();
        let embedding_model = daemon.embedding_model.clone();
        let embedding_dim = daemon.embedding_dim;
        let feedback = blocking(move || {
            memory.apply_feedback_for_embedding(
                params.search_id,
                &targets,
                &embedding_model,
                embedding_dim,
            )
        })
        .await??;

        let applied = feedback
            .applied
            .into_iter()
            .map(|record| {
                json!({
                    "entry_id": record.entry_id.to_string(),
                    "reinforcement_count": record.reinforcement_count,
                    "affinity_count": record.affinity_count,
                    "last_reinforced_at": record.last_reinforced_at.to_rfc3339(),
                })
            })
            .collect::<Vec<_>>();
        let already_applied = feedback
            .already_applied
            .into_iter()
            .map(|entry_id| entry_id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({ "applied": applied, "already_applied": already_applied }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use async_trait::async_trait;
    use nomai_core::{
        BlockInput, CreateEntry, CreateSearchSession, Entry, EntryService, MemoryPolicy,
        SearchResultTarget,
    };
    use nomai_protocol::{Id, JSONRPC_VERSION, Request};
    use nomai_providers::{CompletionRequest, CompletionResponse, EmbeddingProvider, LlmProvider};
    use serde_json::{Value, json};
    use ulid::Ulid;

    use crate::daemon::{Daemon, DaemonBuilder};

    struct NullEmbed;

    #[async_trait]
    impl EmbeddingProvider for NullEmbed {
        async fn embed(
            &self,
            _texts: &[&str],
        ) -> Result<Vec<Vec<f32>>, nomai_protocol::ProviderError> {
            Ok(vec![])
        }

        fn dim(&self) -> usize {
            8
        }

        fn name(&self) -> &str {
            "null-embed"
        }
    }

    struct NullLlm;

    #[async_trait]
    impl LlmProvider for NullLlm {
        async fn complete(
            &self,
            _req: CompletionRequest,
        ) -> Result<CompletionResponse, nomai_protocol::ProviderError> {
            Err(nomai_protocol::ProviderError::new(
                nomai_protocol::ProviderErrorKind::Unknown,
                "null llm",
                None,
            ))
        }

        fn name(&self) -> &str {
            "null-llm"
        }
    }

    fn daemon(memory_enabled: bool) -> Daemon {
        let entries = Arc::new(EntryService::for_test().unwrap());
        let memory_policy = MemoryPolicy {
            enabled: memory_enabled,
            ..MemoryPolicy::default()
        };

        DaemonBuilder::new()
            .conn(entries.conn_for_test())
            .content_store(entries.content_store().clone())
            .embedder(Arc::new(NullEmbed))
            .llm(Arc::new(NullLlm))
            .embedding_dim(8)
            .chunk_target_size(1024)
            .cache_model("test-embed")
            .warn_rows(100_000)
            .memory_policy(memory_policy)
            .build()
            .unwrap()
    }

    fn seed_entry(daemon: &Daemon, title: &str) -> Entry {
        daemon
            .entries
            .create(CreateEntry {
                title: title.into(),
                blocks: vec![BlockInput {
                    r#type: "note".into(),
                    text: format!("{title} body"),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
                attachments: None,
            })
            .unwrap()
    }

    fn record_session(daemon: &Daemon, entry: &Entry, precise: bool) -> Ulid {
        record_session_with_embedding(daemon, entry, precise, "test-embed", vec![1.0; 8])
    }

    fn record_session_with_embedding(
        daemon: &Daemon,
        entry: &Entry,
        precise: bool,
        embedding_model: &str,
        query_embedding: Vec<f32>,
    ) -> Ulid {
        let (matched_block_id, matched_chunk_id) = if precise {
            let block_id = entry.blocks[0].id;
            let chunk_id = daemon.chunks.list(block_id).unwrap().items[0].id;
            (Some(block_id), Some(chunk_id))
        } else {
            (None, None)
        };

        daemon
            .memory
            .create_search_session(CreateSearchSession {
                raw_query_text: "original query".into(),
                effective_query_text: "effective query".into(),
                query_embedding,
                embedding_model: embedding_model.into(),
                results: vec![SearchResultTarget {
                    entry_id: entry.id,
                    matched_block_id,
                    matched_chunk_id,
                    result_rank: 1,
                }],
            })
            .unwrap()
    }

    fn signal_counts(daemon: &Daemon) -> (i64, i64, i64, i64) {
        daemon
            .entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM search_feedback),
                    (SELECT COUNT(*) FROM entry_memory_stats),
                    (SELECT COUNT(*) FROM query_affinities),
                    (SELECT COUNT(*) FROM vec_query_affinities)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap()
    }

    fn request(params: Value) -> Request {
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: Some(Id::Number(1)),
            method: "search.feedback".into(),
            params: Some(params),
        }
    }

    #[tokio::test]
    async fn feedback_schema_requires_non_empty_targets_and_allows_entry_or_precise_target() {
        let daemon = daemon(true);
        let tools = daemon
            .dispatch(Request {
                jsonrpc: JSONRPC_VERSION.into(),
                id: Some(Id::Number(1)),
                method: "tools/list".into(),
                params: Some(json!({})),
            })
            .await
            .result
            .unwrap();
        let tool = tools["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "search.feedback")
            .expect("search.feedback must be advertised through MCP");
        let schema = &tool["inputSchema"];

        assert_eq!(schema["required"], json!(["search_id", "targets"]));
        assert_eq!(
            schema["properties"]["search_id"]["pattern"],
            "^[0-9A-HJKMNP-TV-Z]{26}$"
        );
        assert_eq!(schema["properties"]["targets"]["minItems"], 1);
        assert_eq!(
            schema["properties"]["targets"]["items"]["$ref"],
            "#/$defs/FeedbackTargetParams"
        );
        let target = &schema["$defs"]["FeedbackTargetParams"];
        assert_eq!(target["required"], json!(["entry_id"]));
        assert_eq!(
            target["properties"]["entry_id"]["pattern"],
            "^[0-9A-HJKMNP-TV-Z]{26}$"
        );
        assert_eq!(
            target["properties"]["block_id"]["pattern"],
            "^[0-9A-HJKMNP-TV-Z]{26}$"
        );
        assert_eq!(
            target["properties"]["chunk_id"]["pattern"],
            "^[0-9A-HJKMNP-TV-Z]{26}$"
        );
    }

    #[tokio::test]
    async fn feedback_applies_entry_only_and_precise_targets_then_reports_retry_receipt() {
        let daemon = daemon(true);
        let entry_only = seed_entry(&daemon, "entry-only");
        let precise = seed_entry(&daemon, "precise");
        let entry_session = record_session(&daemon, &entry_only, false);
        let precise_session = record_session(&daemon, &precise, true);
        let precise_block_id = precise.blocks[0].id;
        let precise_chunk_id = daemon.chunks.list(precise_block_id).unwrap().items[0].id;

        let entry_response = daemon
            .dispatch(request(json!({
                "search_id": entry_session,
                "targets": [{"entry_id": entry_only.id}],
            })))
            .await;
        let entry_result = entry_response.result.expect("entry-only feedback succeeds");
        assert_eq!(
            entry_result["applied"][0]["entry_id"],
            entry_only.id.to_string()
        );
        assert_eq!(entry_result["applied"][0]["reinforcement_count"], 1);
        assert_eq!(entry_result["applied"][0]["affinity_count"], 1);
        assert!(
            entry_result["applied"][0]["last_reinforced_at"]
                .as_str()
                .is_some()
        );
        assert_eq!(entry_result["already_applied"], json!([]));

        let precise_response = daemon
            .dispatch(request(json!({
                "search_id": precise_session,
                "targets": [{
                    "entry_id": precise.id,
                    "block_id": precise_block_id,
                    "chunk_id": precise_chunk_id,
                }],
            })))
            .await;
        assert!(
            precise_response.error.is_none(),
            "{:?}",
            precise_response.error
        );

        let retry = daemon
            .dispatch(request(json!({
                "search_id": entry_session,
                "targets": [{"entry_id": entry_only.id}],
            })))
            .await
            .result
            .expect("feedback retry succeeds");
        assert_eq!(retry["applied"], json!([]));
        assert_eq!(retry["already_applied"], json!([entry_only.id.to_string()]));
    }

    #[tokio::test]
    async fn feedback_rejects_empty_targets_and_malformed_ulids() {
        let daemon = daemon(true);
        let entry = seed_entry(&daemon, "target");
        let search_id = record_session(&daemon, &entry, false);

        let empty = daemon
            .dispatch(request(json!({"search_id": search_id, "targets": []})))
            .await
            .error
            .expect("empty targets are rejected");
        assert_eq!(empty.code, 1003);
        assert_eq!(empty.message, "feedback targets must not be empty");

        let malformed = daemon
            .dispatch(request(json!({
                "search_id": "not-a-ulid",
                "targets": [{"entry_id": entry.id}],
            })))
            .await
            .error
            .expect("malformed ULIDs are rejected");
        assert_eq!(malformed.code, 1003);
        assert!(malformed.message.starts_with("invalid params:"));
    }

    #[tokio::test]
    async fn feedback_rejects_target_outside_recorded_search_result() {
        let daemon = daemon(true);
        let returned = seed_entry(&daemon, "returned");
        let other = seed_entry(&daemon, "other");
        let search_id = record_session(&daemon, &returned, false);

        let error = daemon
            .dispatch(request(json!({
                "search_id": search_id,
                "targets": [{"entry_id": other.id}],
            })))
            .await
            .error
            .expect("unrecorded target is rejected");
        assert_eq!(error.code, 1003);
        assert!(error.message.contains("not returned by search session"));
    }

    #[tokio::test]
    async fn feedback_rejects_old_dimension_session_before_any_signal_write() {
        let daemon = daemon(true);
        let entry = seed_entry(&daemon, "old dimension");
        let search_id =
            record_session_with_embedding(&daemon, &entry, false, "test-embed", vec![1.0; 4]);

        let error = daemon
            .dispatch(request(json!({
                "search_id": search_id,
                "targets": [{"entry_id": entry.id}],
            })))
            .await
            .error
            .expect("old-dimension session is rejected");

        assert_eq!(error.code, 1008);
        assert!(error.message.contains("dimension"));
        assert_eq!(signal_counts(&daemon), (0, 0, 0, 0));
    }

    #[tokio::test]
    async fn feedback_rejects_old_model_session_before_any_signal_write() {
        let daemon = daemon(true);
        let entry = seed_entry(&daemon, "old model");
        let search_id = record_session_with_embedding(
            &daemon,
            &entry,
            false,
            "retired-embed-model",
            vec![1.0; 8],
        );

        let error = daemon
            .dispatch(request(json!({
                "search_id": search_id,
                "targets": [{"entry_id": entry.id}],
            })))
            .await
            .error
            .expect("old-model session is rejected");

        assert_eq!(error.code, 1008);
        assert!(error.message.contains("model"));
        assert_eq!(signal_counts(&daemon), (0, 0, 0, 0));
    }

    #[tokio::test]
    async fn feedback_returns_config_error_when_memory_is_disabled() {
        let daemon = daemon(false);

        let error = daemon
            .dispatch(request(json!({
                "search_id": Ulid::new(),
                "targets": [{"entry_id": Ulid::new()}],
            })))
            .await
            .error
            .expect("disabled memory rejects feedback");
        assert_eq!(error.code, 1004);
        assert!(error.message.contains("memory"));
        assert!(error.message.contains("disabled"));
    }

    #[test]
    fn feedback_does_not_take_the_filesystem_sync_lock() {
        use crate::rpc::RpcHandler;

        assert!(!Feedback.is_mutating());
    }
}
