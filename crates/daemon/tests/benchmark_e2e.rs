use std::sync::Arc;

use async_trait::async_trait;
use nomai_core::EntryService;
use nomai_daemon::config::DevelopmentConfig;
use nomai_daemon::daemon::Daemon;
use nomai_protocol::{Id, JSONRPC_VERSION, Request};
use nomai_providers::{CompletionRequest, CompletionResponse, EmbeddingProvider, LlmProvider};
use serde_json::{Value, json};
use tempfile::TempDir;

const EMBEDDING_DIM: usize = 1536;

struct NullEmbed;

#[async_trait]
impl EmbeddingProvider for NullEmbed {
    async fn embed(&self, _texts: &[&str]) -> Result<Vec<Vec<f32>>, nomai_protocol::ProviderError> {
        Ok(vec![])
    }

    fn dim(&self) -> usize {
        EMBEDDING_DIM
    }

    fn name(&self) -> &str {
        "null"
    }
}

struct NullLlm;

#[async_trait]
impl LlmProvider for NullLlm {
    async fn complete(
        &self,
        _request: CompletionRequest,
    ) -> Result<CompletionResponse, nomai_protocol::ProviderError> {
        Err(nomai_protocol::ProviderError::new(
            nomai_protocol::ProviderErrorKind::Unknown,
            "null",
            None,
        ))
    }

    fn name(&self) -> &str {
        "null"
    }
}

fn request(id: i64, method: &str, params: Value) -> Request {
    Request {
        jsonrpc: JSONRPC_VERSION.into(),
        id: Some(Id::Number(id)),
        method: method.into(),
        params: Some(params),
    }
}

fn assets() -> (TempDir, DevelopmentConfig) {
    let root = tempfile::tempdir().unwrap();
    let cases = root.path().join("cases");
    let suites = root.path().join("suites");
    let baselines = root.path().join("baselines");
    std::fs::create_dir_all(&cases).unwrap();
    std::fs::create_dir_all(&suites).unwrap();
    std::fs::create_dir_all(&baselines).unwrap();
    std::fs::write(
        cases.join("case.toml"),
        r#"
id = "case-1"
question = "Which evidence should be retrieved?"

[[fixtures]]
id = "01J0K3H6Y0F9Q7V8X1A2B3C4D5"
title = "Benchmark evidence"

  [[fixtures.blocks]]
  id = "01J0K3H6Y1F9Q7V8X1A2B3C4D6"
  type = "note"
  text = "The benchmark evidence says to inspect the retrieved note."

[retrieval]
required_tools = ["search.fulltext", "entry.get"]
relevant_entry_ids = ["01J0K3H6Y0F9Q7V8X1A2B3C4D5"]
relevant_block_ids = ["01J0K3H6Y1F9Q7V8X1A2B3C4D6"]
k = 5

[answer]
reference = "Inspect the retrieved note."
judge = false
"#,
    )
    .unwrap();
    std::fs::write(
        suites.join("suite.toml"),
        "id = \"suite-1\"\ncases = [\"case-1\"]\n",
    )
    .unwrap();
    std::fs::write(
        baselines.join("baseline.json"),
        r#"
{
  "schema_version": 1,
  "suite_id": "suite-1",
  "case_ids": ["case-1"],
  "provider": {"name": "test", "base_url": ""},
  "embedding_model": "",
  "llm_model": "",
  "metrics": {
    "hit_at_k": 0.0,
    "recall_at_k": 0.0,
    "mrr": 0.0,
    "ndcg": 0.0,
    "required_tools_success": 0.0,
    "evidence_entry_hit": 0.0,
    "search_call_count": 0.0,
    "latency_ms_total": 0.0,
    "latency_ms_average": 0.0
  },
  "thresholds": {
    "hit_at_k": {"minimum": 0.0},
    "recall_at_k": {"minimum": 0.0},
    "mrr": {"minimum": 0.0},
    "ndcg": {"minimum": 0.0},
    "required_tools_success": {"minimum": 0.0},
    "evidence_entry_hit": {"minimum": 0.0},
    "search_call_count": {"maximum": 10.0},
    "latency_ms_total": {"maximum": 100000.0},
    "latency_ms_average": {"maximum": 100000.0}
  }
}
"#,
    )
    .unwrap();
    (
        root,
        DevelopmentConfig {
            enabled: true,
            benchmark_cases_dir: cases,
            benchmark_suites_dir: suites,
            benchmark_baselines_dir: baselines,
        },
    )
}

fn build_daemon(development: DevelopmentConfig) -> Daemon {
    let entries = Arc::new(EntryService::for_test().unwrap());
    Daemon::from_services_with_development(
        entries.conn_for_test(),
        entries.content_store().clone(),
        Arc::new(NullEmbed),
        Arc::new(NullLlm),
        EMBEDDING_DIM,
        1024,
        "",
        100_000,
        1024 * 1024,
        development,
    )
    .unwrap()
}

#[tokio::test]
async fn enabled_daemon_runs_model_like_benchmark_workflow_end_to_end() {
    let (_root, development) = assets();
    let daemon = build_daemon(development);

    let tools = daemon
        .dispatch(request(1, "tools/list", json!({})))
        .await
        .result
        .unwrap();
    let names: Vec<&str> = tools["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    assert!(names.contains(&"benchmark.start"));
    assert!(names.contains(&"benchmark.next_case"));
    assert!(names.contains(&"benchmark.finish"));

    let started = daemon
        .dispatch(request(
            2,
            "benchmark.start",
            json!({"suite_id": "suite-1"}),
        ))
        .await
        .result
        .unwrap();
    let run_id = started["run_id"].as_str().unwrap().to_owned();
    let next = daemon
        .dispatch(request(3, "benchmark.next_case", json!({"run_id": run_id})))
        .await
        .result
        .unwrap();
    assert_eq!(next["question"], "Which evidence should be retrieved?");
    assert!(next.get("reference").is_none());
    assert!(next.get("relevant_entry_ids").is_none());

    let search = daemon
        .dispatch(request(
            4,
            "search.fulltext",
            json!({"query": "evidence", "limit": 5}),
        ))
        .await
        .result
        .unwrap();
    let entry_id = search["items"][0]["entry"]["id"].as_str().unwrap();
    let _evidence = daemon
        .dispatch(request(5, "entry.get", json!({"id": entry_id})))
        .await
        .result
        .unwrap();

    let answer = daemon
        .dispatch(request(
            6,
            "benchmark.record_answer",
            json!({
                "run_id": run_id,
                "case_id": "case-1",
                "answer": "Inspect the retrieved note."
            }),
        ))
        .await
        .result
        .unwrap();
    assert_eq!(answer["metrics"]["hit_at_k"], 1.0);
    assert_eq!(answer["metrics"]["evidence_entry_hit"], true);

    let report = daemon
        .dispatch(request(7, "benchmark.finish", json!({"run_id": run_id})))
        .await
        .result
        .unwrap();
    assert_eq!(report["summary"]["required_tools_success"], 1.0);
    assert_eq!(report["baseline_comparison"]["compatible"], true);
    assert_eq!(report["cases"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn disabled_daemon_hides_benchmark_tools_and_rejects_direct_calls() {
    let (_root, mut development) = assets();
    development.enabled = false;
    let daemon = build_daemon(development);

    let tools = daemon
        .dispatch(request(1, "tools/list", json!({})))
        .await
        .result
        .unwrap();
    let names: Vec<&str> = tools["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    assert!(!names.iter().any(|name| name.starts_with("benchmark.")));

    let response = daemon
        .dispatch(request(
            2,
            "benchmark.start",
            json!({"suite_id": "suite-1"}),
        ))
        .await;
    let error = response.error.expect("disabled benchmark must be rejected");
    assert_eq!(error.code, nomai_protocol::error::METHOD_NOT_FOUND);
}
