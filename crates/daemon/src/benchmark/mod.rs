#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nomai_core::{BlockInput, CoreError, CreateEntry, EntryService, MemorySignalsService};
use nomai_providers::{ChatMessage, CompletionRequest, LlmProvider, MessageRole};
use serde::Serialize;
use serde_json::Value;
use ulid::Ulid;

pub(crate) mod baseline;
pub(crate) mod cases;
pub(crate) mod metrics;

use cases::{BenchmarkCatalog, CaseSpec};
use metrics::{CaseReport, CaseTrace, ResolvedFixtureIds, RunMetadata, RunReport, ToolTrace};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StartResult {
    pub run_id: String,
    pub suite_id: String,
    pub case_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NextCaseResult {
    pub run_id: String,
    pub case_id: String,
    pub question: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AbortResult {
    pub run_id: String,
    pub deleted_ids: Vec<Ulid>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StatusResult {
    pub active: bool,
    pub run_id: Option<String>,
    pub suite_id: Option<String>,
    pub case_id: Option<String>,
    pub next_case_index: Option<usize>,
}

struct ActiveRun {
    run_id: String,
    suite_id: String,
    case_ids: Vec<String>,
    next_case_index: usize,
    resolved: HashMap<String, ResolvedFixtureIds>,
    traces: HashMap<String, CaseTrace>,
}

pub(crate) struct BenchmarkRuntime {
    catalog: BenchmarkCatalog,
    baselines: Vec<baseline::BaselineFile>,
    entries: Arc<EntryService>,
    memory: Arc<MemorySignalsService>,
    state: Mutex<Option<ActiveRun>>,
    provider: Mutex<ProviderMetadata>,
}

#[derive(Default)]
struct ProviderMetadata {
    name: String,
    base_url: String,
    embedding_model: String,
    llm_model: String,
}

impl BenchmarkRuntime {
    pub(crate) fn new(
        config: crate::config::DevelopmentConfig,
        entries: Arc<EntryService>,
        memory: Arc<MemorySignalsService>,
    ) -> Result<Self, CoreError> {
        let catalog = BenchmarkCatalog::load(&config)?;
        let baselines = baseline::load_baselines(&config, &catalog)?;
        Ok(Self {
            catalog,
            baselines,
            entries,
            memory,
            state: Mutex::new(None),
            provider: Mutex::new(ProviderMetadata::default()),
        })
    }

    pub(crate) fn set_provider_metadata(
        &self,
        name: impl Into<String>,
        base_url: impl Into<String>,
        embedding_model: impl Into<String>,
        llm_model: impl Into<String>,
    ) {
        let mut provider = self.provider.lock().unwrap();
        provider.name = name.into();
        provider.base_url = base_url.into();
        provider.embedding_model = embedding_model.into();
        provider.llm_model = llm_model.into();
    }

    pub(crate) fn recover_stale_entries(&self) -> Result<Vec<Ulid>, CoreError> {
        self.purge_fixture_entries_and_signals()
    }

    fn purge_fixture_entries_and_signals(&self) -> Result<Vec<Ulid>, CoreError> {
        let deleted_ids = self.entries.purge_benchmark_entries()?;
        // purge_benchmark_entries completes every Entry deletion before this
        // reconciliation, so transient fixture IDs cannot survive in local
        // adaptive-memory rows and temporary reindex states are never seen.
        self.memory.reconcile_content_references()?;
        Ok(deleted_ids)
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.lock().unwrap().is_some()
    }

    pub(crate) fn active_fixture_entry_ids(&self, run_id: &str) -> Result<Vec<Ulid>, CoreError> {
        let state = self.state.lock().unwrap();
        let active = state
            .as_ref()
            .ok_or_else(|| CoreError::Validation("no benchmark run is active".into()))?;
        if active.run_id != run_id {
            return Err(CoreError::Validation("invalid benchmark run id".into()));
        }

        let mut ids: Vec<Ulid> = active
            .resolved
            .values()
            .flat_map(|resolved| resolved.entries.values().copied())
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    pub(crate) fn start(&self, suite_id: &str) -> Result<StartResult, CoreError> {
        let suite = self.catalog.suite(suite_id)?.clone();
        let mut state = self.state.lock().unwrap();
        if state.is_some() {
            return Err(CoreError::Validation(
                "a benchmark run is already active".into(),
            ));
        }

        let run_id = Ulid::new().to_string();
        let mut resolved = HashMap::new();
        let mut created_ids = Vec::new();
        let create_result = (|| -> Result<(), CoreError> {
            for case_id in &suite.cases {
                let case = self.catalog.case(case_id)?;
                let ids = self.create_fixtures(case, &run_id)?;
                created_ids.extend(ids.entries.values().copied());
                resolved.insert(case_id.clone(), ids);
            }
            Ok(())
        })();
        if let Err(error) = create_result {
            for id in created_ids {
                let _ = self.entries.delete(id);
            }
            return Err(error);
        }

        let case_ids = suite.cases;
        *state = Some(ActiveRun {
            run_id: run_id.clone(),
            suite_id: suite.id.clone(),
            case_ids: case_ids.clone(),
            next_case_index: 0,
            resolved,
            traces: HashMap::new(),
        });
        Ok(StartResult {
            run_id,
            suite_id: suite.id,
            case_ids,
        })
    }

    pub(crate) fn next_case(&self, run_id: &str) -> Result<NextCaseResult, CoreError> {
        let mut state = self.state.lock().unwrap();
        let active = active_run_mut(&mut state, run_id)?;
        let case_id = active
            .case_ids
            .get(active.next_case_index)
            .cloned()
            .ok_or_else(|| CoreError::Validation("benchmark suite is exhausted".into()))?;
        active.next_case_index += 1;
        let question = self.catalog.case(&case_id)?.question.clone();
        active.traces.entry(case_id.clone()).or_default();
        Ok(NextCaseResult {
            run_id: run_id.into(),
            case_id,
            question,
        })
    }

    pub(crate) fn record_rpc(
        &self,
        method: &str,
        _params: &Value,
        result: &Result<Value, CoreError>,
        latency: Duration,
    ) {
        if !matches!(
            method,
            "search.semantic" | "search.fulltext" | "entry.get" | "block.get"
        ) {
            return;
        }
        let mut state = self.state.lock().unwrap();
        let Some(active) = state.as_mut() else {
            return;
        };
        let Some(case_id) = active
            .case_ids
            .get(active.next_case_index.saturating_sub(1))
            .cloned()
        else {
            return;
        };
        active
            .traces
            .entry(case_id)
            .or_default()
            .tool_traces
            .push(ToolTrace {
                method: method.into(),
                ok: result.is_ok(),
                latency_ms: latency.as_millis() as u64,
                results: result
                    .as_ref()
                    .map(|value| extract_tool_results(method, value))
                    .unwrap_or_default(),
                error: result.as_ref().err().map(ToString::to_string),
            });
    }

    pub(crate) async fn record_answer(
        &self,
        run_id: &str,
        case_id: &str,
        answer: String,
        llm: &dyn LlmProvider,
    ) -> Result<CaseReport, CoreError> {
        let case = {
            let mut state = self.state.lock().unwrap();
            let active = active_run_mut(&mut state, run_id)?;
            if !active.case_ids.iter().any(|id| id == case_id) {
                return Err(CoreError::Validation(format!(
                    "case is not part of benchmark run: {case_id}"
                )));
            }
            self.catalog.case(case_id)?.clone()
        };

        let (judge_score, judge_error) = if case.answer.judge {
            match run_judge(&case, &answer, llm).await {
                Ok(score) => (Some(score), None),
                Err(error) => (None, Some(error)),
            }
        } else {
            (None, None)
        };

        let mut state = self.state.lock().unwrap();
        let active = active_run_mut(&mut state, run_id)?;
        let trace = active.traces.entry(case_id.into()).or_default();
        trace.answer = Some(answer);
        trace.judge_score = judge_score;
        trace.judge_error = judge_error;
        let resolved = active
            .resolved
            .get(case_id)
            .ok_or_else(|| CoreError::Config("missing fixture mapping".into()))?;
        Ok(CaseReport {
            case_id: case_id.into(),
            metrics: metrics::score_case(&case, trace, resolved),
        })
    }

    pub(crate) fn finish(&self, run_id: &str) -> Result<RunReport, CoreError> {
        let mut state = self.state.lock().unwrap();
        let active = state
            .take()
            .ok_or_else(|| CoreError::Validation("no benchmark run is active".into()))?;
        if active.run_id != run_id {
            *state = Some(active);
            return Err(CoreError::Validation("invalid benchmark run id".into()));
        }

        let mut reports = Vec::with_capacity(active.case_ids.len());
        for case_id in &active.case_ids {
            let case = self.catalog.case(case_id)?;
            let trace = active.traces.get(case_id).cloned().unwrap_or_default();
            let resolved = active
                .resolved
                .get(case_id)
                .ok_or_else(|| CoreError::Config("missing fixture mapping".into()))?;
            reports.push(CaseReport {
                case_id: case_id.clone(),
                metrics: metrics::score_case(case, &trace, resolved),
            });
        }
        if let Err(error) = self.purge_fixture_entries_and_signals() {
            *state = Some(active);
            return Err(error);
        }
        let provider = self.provider.lock().unwrap();
        let mut report = RunReport {
            metadata: RunMetadata {
                schema_version: 2,
                suite_id: active.suite_id,
                case_ids: active.case_ids,
                provider_name: provider.name.clone(),
                provider_base_url: provider.base_url.clone(),
                embedding_model: provider.embedding_model.clone(),
                llm_model: provider.llm_model.clone(),
            },
            summary: metrics::summarize(&reports),
            cases: reports,
            baseline_comparison: None,
        };
        if let Some(baseline) = self
            .baselines
            .iter()
            .find(|baseline| baseline.suite_id == report.metadata.suite_id)
        {
            report.baseline_comparison = Some(baseline::compare_baseline(&report, baseline));
        }
        Ok(report)
    }

    pub(crate) fn abort(&self, run_id: &str) -> Result<AbortResult, CoreError> {
        let mut state = self.state.lock().unwrap();
        let active = state
            .take()
            .ok_or_else(|| CoreError::Validation("no benchmark run is active".into()))?;
        if active.run_id != run_id {
            *state = Some(active);
            return Err(CoreError::Validation("invalid benchmark run id".into()));
        }
        let deleted_ids = match self.purge_fixture_entries_and_signals() {
            Ok(ids) => ids,
            Err(error) => {
                *state = Some(active);
                return Err(error);
            }
        };
        Ok(AbortResult {
            run_id: run_id.into(),
            deleted_ids,
        })
    }

    pub(crate) fn status(&self) -> StatusResult {
        let state = self.state.lock().unwrap();
        match state.as_ref() {
            Some(active) => StatusResult {
                active: true,
                run_id: Some(active.run_id.clone()),
                suite_id: Some(active.suite_id.clone()),
                case_id: active
                    .next_case_index
                    .checked_sub(1)
                    .and_then(|index| active.case_ids.get(index).cloned()),
                next_case_index: Some(active.next_case_index),
            },
            None => StatusResult {
                active: false,
                run_id: None,
                suite_id: None,
                case_id: None,
                next_case_index: None,
            },
        }
    }

    fn create_fixtures(
        &self,
        case: &CaseSpec,
        run_id: &str,
    ) -> Result<ResolvedFixtureIds, CoreError> {
        let mut resolved = ResolvedFixtureIds::default();
        let mut created_ids = Vec::new();
        for fixture in &case.fixtures {
            let mut attrs = if fixture.attrs.is_null() {
                Value::Object(Default::default())
            } else {
                fixture.attrs.clone()
            };
            let object = attrs.as_object_mut().ok_or_else(|| {
                CoreError::Config(format!("fixture {} attrs must be an object", fixture.id))
            })?;
            object.insert("transient".into(), Value::Bool(true));
            object.insert("benchmark_run_id".into(), Value::String(run_id.into()));
            object.insert("benchmark_case_id".into(), Value::String(case.id.clone()));
            let entry = match self.entries.create(CreateEntry {
                title: fixture.title.clone(),
                blocks: fixture
                    .blocks
                    .iter()
                    .map(|block| BlockInput {
                        r#type: block.block_type.clone(),
                        text: block.text.clone(),
                        attrs: (!block.attrs.is_null()).then(|| block.attrs.clone()),
                    })
                    .collect(),
                tags: Some(fixture.tags.clone()),
                attrs: Some(attrs),
                source: Some("benchmark".into()),
                attachments: None,
            }) {
                Ok(entry) => entry,
                Err(error) => {
                    for id in created_ids {
                        let _ = self.entries.delete(id);
                    }
                    return Err(error);
                }
            };
            created_ids.push(entry.id);
            resolved.entries.insert(fixture.id, entry.id);
            for (source_block, stored_block) in fixture.blocks.iter().zip(entry.blocks.iter()) {
                resolved.blocks.insert(source_block.id, stored_block.id);
            }
        }
        Ok(resolved)
    }
}

fn extract_tool_results(method: &str, value: &Value) -> Vec<metrics::RetrievedResult> {
    match method {
        "search.fulltext" => value["items"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|item| metrics::RetrievedResult {
                entry_id: item["entry"]["id"].as_str().map(String::from),
                block_id: item["best_match"]["block_id"].as_str().map(String::from),
                score: item["score"].as_f64(),
            })
            .collect(),
        "search.semantic" => value["items"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|item| metrics::RetrievedResult {
                entry_id: item["entry_id"].as_str().map(String::from),
                block_id: item["chunk"]["block_id"].as_str().map(String::from),
                score: item["score"].as_f64(),
            })
            .collect(),
        "entry.get" => value["id"]
            .as_str()
            .map(|entry_id| metrics::RetrievedResult {
                entry_id: Some(entry_id.into()),
                ..Default::default()
            })
            .into_iter()
            .collect(),
        "block.get" => value["id"]
            .as_str()
            .map(|block_id| metrics::RetrievedResult {
                entry_id: value["entry_id"].as_str().map(String::from),
                block_id: Some(block_id.into()),
                ..Default::default()
            })
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

async fn run_judge(case: &CaseSpec, answer: &str, llm: &dyn LlmProvider) -> Result<f64, String> {
    let response = llm
        .complete(CompletionRequest {
            system: Some(
                "You are a benchmark judge. Compare the model answer to the reference answer. Return only a number from 0 to 1, where 1 is fully correct and 0 is incorrect.".into(),
            ),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: format!(
                    "Question:\n{}\n\nReference answer:\n{}\n\nModel answer:\n{}",
                    case.question, case.answer.reference, answer
                ),
            }],
            max_tokens: Some(16),
            temperature: Some(0.0),
        })
        .await
        .map_err(|error| error.to_string())?;

    let score = response
        .content
        .split_whitespace()
        .find_map(|token| {
            token
                .trim_matches(|ch: char| !ch.is_ascii_digit() && ch != '.')
                .parse::<f64>()
                .ok()
        })
        .ok_or_else(|| "judge response did not contain a numeric score".to_string())?;
    if !score.is_finite() || !(0.0..=1.0).contains(&score) {
        return Err(format!("judge score out of range: {score}"));
    }
    Ok(score)
}

fn active_run_mut<'a>(
    state: &'a mut Option<ActiveRun>,
    run_id: &str,
) -> Result<&'a mut ActiveRun, CoreError> {
    let active = state
        .as_mut()
        .ok_or_else(|| CoreError::Validation("no benchmark run is active".into()))?;
    if active.run_id != run_id {
        return Err(CoreError::Validation("invalid benchmark run id".into()));
    }
    Ok(active)
}

fn config_error(path: &Path, message: impl Display) -> CoreError {
    CoreError::Config(format!("{}: {message}", path.display()))
}

fn read_to_string(path: &Path) -> Result<String, CoreError> {
    std::fs::read_to_string(path).map_err(|err| config_error(path, format!("read failed: {err}")))
}

fn sorted_files(dir: &Path, extension: &str) -> Result<Vec<PathBuf>, CoreError> {
    let mut paths = std::fs::read_dir(dir)
        .map_err(|err| config_error(dir, format!("read_dir failed: {err}")))?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|err| config_error(dir, format!("read_dir entry failed: {err}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    paths.retain(|path| path.is_file() && path.extension().is_some_and(|ext| ext == extension));
    paths.sort();
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use nomai_core::{
        CreateSearchSession, FeedbackTarget, MemoryPolicy, MemorySignalsService,
        SearchResultTarget, SystemClock,
    };
    use nomai_providers::{
        CompletionRequest, CompletionResponse, ProviderError, ProviderErrorKind,
    };
    use serde_json::json;
    use tempfile::TempDir;

    struct NullLlm;

    #[async_trait]
    impl LlmProvider for NullLlm {
        async fn complete(
            &self,
            _req: CompletionRequest,
        ) -> Result<CompletionResponse, ProviderError> {
            Err(ProviderError::new(ProviderErrorKind::Unknown, "null", None))
        }

        fn name(&self) -> &str {
            "null"
        }
    }

    fn test_runtime() -> (
        TempDir,
        Arc<EntryService>,
        BenchmarkRuntime,
        crate::config::DevelopmentConfig,
    ) {
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
question = "Which fixture should the model retrieve?"

[[fixtures]]
id = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
title = "Benchmark fixture"

  [[fixtures.blocks]]
  id = "01J0K3H6Y1F9Q7V8X1A2B3C4D6"
  type = "note"
  text = "Fixture evidence"

[retrieval]
required_tools = ["search.fulltext"]
relevant_entry_ids = ["01ARZ3NDEKTSV4RRFFQ69G5FAV"]
relevant_block_ids = ["01J0K3H6Y1F9Q7V8X1A2B3C4D6"]
k = 5

[answer]
reference = "fixture"
judge = false
"#,
        )
        .unwrap();
        std::fs::write(
            suites.join("suite.toml"),
            "id = \"suite-1\"\ncases = [\"case-1\"]\n",
        )
        .unwrap();
        let config = crate::config::DevelopmentConfig {
            enabled: true,
            benchmark_cases_dir: cases,
            benchmark_suites_dir: suites,
            benchmark_baselines_dir: baselines,
        };
        let entries = Arc::new(EntryService::for_test().unwrap());
        let memory = Arc::new(
            MemorySignalsService::new(
                entries.conn_for_test(),
                MemoryPolicy::default(),
                Arc::new(SystemClock),
            )
            .unwrap(),
        );
        memory.ensure_vec_query_affinities(4).unwrap();
        let runtime = BenchmarkRuntime::new(config.clone(), entries.clone(), memory).unwrap();
        (root, entries, runtime, config)
    }

    #[tokio::test]
    async fn runtime_loads_fixture_next_case_and_cleans_on_abort() {
        let (_root, entries, runtime, _config) = test_runtime();
        let start = runtime.start("suite-1").unwrap();
        assert_eq!(start.case_ids, vec!["case-1"]);
        assert_eq!(entries.list(Default::default()).unwrap().total, 0);
        let next = runtime.next_case(&start.run_id).unwrap();
        assert_eq!(next.question, "Which fixture should the model retrieve?");
        let next_json = serde_json::to_value(&next).unwrap();
        for key in [
            "reference",
            "relevant_entry_ids",
            "relevant_block_ids",
            "judge",
            "baseline",
            "fixtures",
        ] {
            assert!(next_json.get(key).is_none(), "gold field leaked: {key}");
        }
        assert!(runtime.next_case(&start.run_id).is_err());
        assert!(runtime.start("suite-1").is_err());

        runtime.record_rpc(
            "search.fulltext",
            &json!({"query": "fixture"}),
            &Ok(json!({"items": []})),
            Duration::from_millis(3),
        );
        let report = runtime
            .record_answer(&start.run_id, "case-1", "fixture".into(), &NullLlm)
            .await
            .unwrap();
        assert!(report.metrics.required_tools_success);
        let aborted = runtime.abort(&start.run_id).unwrap();
        assert_eq!(aborted.deleted_ids.len(), 1);
        assert_eq!(entries.list(Default::default()).unwrap().total, 0);
        assert!(!runtime.status().active);
    }

    #[test]
    fn benchmark_abort_reconciles_signals_for_deleted_fixtures() {
        let (_root, entries, runtime, _config) = test_runtime();
        let memory = runtime.memory.clone();
        let start = runtime.start("suite-1").unwrap();
        let entry_id = runtime.active_fixture_entry_ids(&start.run_id).unwrap()[0];
        let entry = entries.get_with_benchmark(entry_id).unwrap();
        let block_id = entry.blocks[0].id;
        let chunk_id = entries
            .conn_for_test()
            .lock()
            .unwrap()
            .query_row(
                "SELECT id FROM chunks WHERE block_id = ?1",
                [block_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            .parse()
            .unwrap();
        let search_id = memory
            .create_search_session(CreateSearchSession {
                raw_query_text: "benchmark raw".into(),
                effective_query_text: "benchmark effective".into(),
                query_embedding: vec![1.0, 0.0, 0.0, 0.0],
                embedding_model: "active-model".into(),
                results: vec![SearchResultTarget {
                    entry_id,
                    matched_block_id: Some(block_id),
                    matched_chunk_id: Some(chunk_id),
                    result_rank: 1,
                }],
            })
            .unwrap();
        memory
            .apply_feedback(
                search_id,
                &[FeedbackTarget {
                    entry_id,
                    block_id: Some(block_id),
                    chunk_id: Some(chunk_id),
                }],
            )
            .unwrap();

        runtime.abort(&start.run_id).unwrap();

        let conn = entries.conn_for_test();
        let counts = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM entry_memory_stats WHERE entry_id = ?1),
                    (SELECT COUNT(*) FROM query_affinities WHERE entry_id = ?1),
                    (SELECT COUNT(*) FROM search_feedback WHERE entry_id = ?1),
                    (SELECT COUNT(*) FROM search_session_results WHERE entry_id = ?1),
                    (SELECT COUNT(*) FROM vec_query_affinities)",
                [entry_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(counts, (0, 0, 0, 0, 0));
    }

    #[test]
    fn fresh_runtime_recovers_stale_benchmark_entries() {
        let (_root, entries, runtime, config) = test_runtime();
        let start = runtime.start("suite-1").unwrap();
        assert_eq!(entries.list(Default::default()).unwrap().total, 0);

        let recovered =
            BenchmarkRuntime::new(config, entries.clone(), runtime.memory.clone()).unwrap();
        let deleted = recovered.recover_stale_entries().unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(entries.list(Default::default()).unwrap().total, 0);
        assert!(!start.run_id.is_empty());
    }

    #[test]
    fn extracts_ordered_search_and_evidence_ids_from_tool_results() {
        let fulltext = extract_tool_results(
            "search.fulltext",
            &json!({
                "items": [{
                    "entry": {"id": "entry-1"},
                    "score": 0.9,
                    "best_match": {"block_id": "block-1"}
                }]
            }),
        );
        assert_eq!(fulltext[0].entry_id.as_deref(), Some("entry-1"));
        assert_eq!(fulltext[0].block_id.as_deref(), Some("block-1"));

        let semantic = extract_tool_results(
            "search.semantic",
            &json!({
                "items": [{
                    "entry_id": "entry-2",
                    "chunk": {"block_id": "block-2"},
                    "score": 0.8
                }]
            }),
        );
        assert_eq!(semantic[0].entry_id.as_deref(), Some("entry-2"));
        assert_eq!(semantic[0].block_id.as_deref(), Some("block-2"));
        assert!(extract_tool_results("provider.list", &json!({})).is_empty());
    }
}
