#![allow(dead_code)]

use std::collections::HashSet;
use std::path::Path;

use nomai_core::CoreError;
use serde::Deserialize;

use crate::benchmark::cases::BenchmarkCatalog;
use crate::benchmark::{config_error, read_to_string, sorted_files};
use crate::config::DevelopmentConfig;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BaselineFile {
    pub schema_version: u32,
    pub suite_id: String,
    pub case_ids: Vec<String>,
    pub embedding_provider: String,
    pub embedding_model: String,
    pub llm_provider: String,
    pub llm_model: String,
    pub metrics: Vec<BaselineCaseMetrics>,
    pub thresholds: BaselineThresholds,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BaselineCaseMetrics {
    pub case_id: String,
    pub hit_at_k: f64,
    pub recall_at_k: f64,
    pub mrr: f64,
    pub ndcg: f64,
    pub required_tools_success: bool,
    pub evidence_entry_hit: bool,
    pub search_call_count: u32,
    pub latency_ms_total: u64,
    pub latency_ms_average: u64,
    #[serde(default)]
    pub judge_score: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct MetricThreshold {
    #[serde(default)]
    pub minimum: Option<f64>,
    #[serde(default)]
    pub maximum: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BaselineThresholds {
    pub hit_at_k: MetricThreshold,
    pub recall_at_k: MetricThreshold,
    pub mrr: MetricThreshold,
    pub ndcg: MetricThreshold,
    #[serde(default)]
    pub judge_score: MetricThreshold,
    pub search_call_count: MetricThreshold,
    pub latency_ms_total: MetricThreshold,
    pub latency_ms_average: MetricThreshold,
}

pub(crate) fn load_baselines(
    config: &DevelopmentConfig,
    catalog: &BenchmarkCatalog,
) -> Result<Vec<BaselineFile>, CoreError> {
    let mut baselines = Vec::new();

    for path in sorted_files(&config.benchmark_baselines_dir, "json")? {
        let content = read_to_string(&path)?;
        let baseline: BaselineFile = serde_json::from_str(&content)
            .map_err(|err| config_error(&path, format!("parse failed: {err}")))?;
        validate_baseline(&path, &baseline, catalog)?;
        baselines.push(baseline);
    }

    Ok(baselines)
}

fn validate_baseline(
    path: &Path,
    baseline: &BaselineFile,
    catalog: &BenchmarkCatalog,
) -> Result<(), CoreError> {
    let suite = catalog
        .suite(&baseline.suite_id)
        .map_err(|_| config_error(path, format!("unknown suite: {}", baseline.suite_id)))?;

    let suite_case_ids: HashSet<_> = suite.cases.iter().collect();
    for case_id in &baseline.case_ids {
        if !suite_case_ids.contains(case_id) {
            return Err(config_error(
                path,
                format!("unknown case in baseline {}: {case_id}", baseline.suite_id),
            ));
        }
        catalog.case(case_id)?;
    }

    if baseline.case_ids != suite.cases {
        return Err(config_error(
            path,
            format!(
                "baseline case_ids do not match suite {} order",
                baseline.suite_id
            ),
        ));
    }

    for metric in &baseline.metrics {
        if !baseline
            .case_ids
            .iter()
            .any(|case_id| case_id == &metric.case_id)
        {
            return Err(config_error(
                path,
                format!("metric references unknown case: {}", metric.case_id),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::load_baselines;
    use crate::benchmark::cases::BenchmarkCatalog;
    use crate::config::DevelopmentConfig;
    use tempfile::TempDir;
    use ulid::Ulid;

    const FIXTURE_ENTRY_ID: Ulid = Ulid::from_bytes([
        0x01, 0x92, 0xC1, 0xD5, 0xA1, 0x10, 0xC0, 0xDE, 0xF0, 0x0D, 0xBA, 0x5E, 0xCA, 0xFE, 0x00,
        0x01,
    ]);
    const FIXTURE_BLOCK_ID: Ulid = Ulid::from_bytes([
        0x01, 0x92, 0xC1, 0xD5, 0xA1, 0x10, 0xC0, 0xDE, 0xF0, 0x0D, 0xBA, 0x5E, 0xCA, 0xFE, 0x00,
        0x02,
    ]);

    fn valid_case_toml() -> String {
        format!(
            r#"
id = "search-rust-errors-001"
question = "How do I inspect Rust compiler errors?"

[[fixtures]]
id = "{fixture_entry_id}"
title = "Rust error guide"

[[fixtures.blocks]]
id = "{fixture_block_id}"
type = "note"
text = "Rust compiler errors usually point at the exact file and line."

[retrieval]
required_tools = ["search.fulltext", "entry.get"]
relevant_entry_ids = ["{fixture_entry_id}"]
relevant_block_ids = ["{fixture_block_id}"]
k = 5

[answer]
reference = "Inspect the compiler output and fetch the relevant evidence entry."
judge = "Mentions compiler output plus opening the relevant entry."
"#,
            fixture_entry_id = FIXTURE_ENTRY_ID,
            fixture_block_id = FIXTURE_BLOCK_ID,
        )
    }

    fn valid_suite_toml() -> &'static str {
        r#"
id = "search-regression"
cases = ["search-rust-errors-001"]
"#
    }

    fn valid_baseline_json() -> &'static str {
        r#"{
  "schema_version": 1,
  "suite_id": "search-regression",
  "case_ids": ["search-rust-errors-001"],
  "embedding_provider": "openai-compatible",
  "embedding_model": "text-embedding-3-small",
  "llm_provider": "openai-compatible",
  "llm_model": "gpt-4o-mini",
  "metrics": [
    {
      "case_id": "search-rust-errors-001",
      "hit_at_k": 1.0,
      "recall_at_k": 1.0,
      "mrr": 1.0,
      "ndcg": 1.0,
      "required_tools_success": true,
      "evidence_entry_hit": true,
      "search_call_count": 1,
      "latency_ms_total": 250,
      "latency_ms_average": 250,
      "judge_score": 0.9
    }
  ],
  "thresholds": {
    "hit_at_k": { "minimum": 1.0 },
    "recall_at_k": { "minimum": 1.0 },
    "mrr": { "minimum": 1.0 },
    "ndcg": { "minimum": 1.0 },
    "search_call_count": { "maximum": 2.0 },
    "latency_ms_total": { "maximum": 500.0 },
    "latency_ms_average": { "maximum": 500.0 }
  }
}"#
    }

    fn dirs_with_baseline(baseline_json: &str) -> (TempDir, DevelopmentConfig) {
        let tmp = tempfile::tempdir().unwrap();
        let cases_dir = tmp.path().join("cases");
        let suites_dir = tmp.path().join("suites");
        let baselines_dir = tmp.path().join("baselines");
        std::fs::create_dir_all(&cases_dir).unwrap();
        std::fs::create_dir_all(&suites_dir).unwrap();
        std::fs::create_dir_all(&baselines_dir).unwrap();

        std::fs::write(
            cases_dir.join("search-rust-errors-001.toml"),
            valid_case_toml(),
        )
        .unwrap();
        std::fs::write(
            suites_dir.join("search-regression.toml"),
            valid_suite_toml(),
        )
        .unwrap();
        std::fs::write(baselines_dir.join("search-regression.json"), baseline_json).unwrap();

        let dirs = DevelopmentConfig {
            enabled: true,
            benchmark_cases_dir: cases_dir,
            benchmark_suites_dir: suites_dir,
            benchmark_baselines_dir: baselines_dir,
        };
        (tmp, dirs)
    }

    #[test]
    fn loads_baseline_metadata_metrics_and_thresholds() {
        let (_tmp, dirs) = dirs_with_baseline(valid_baseline_json());
        let catalog = BenchmarkCatalog::load(&dirs).unwrap();

        let baselines = load_baselines(&dirs, &catalog).unwrap();
        assert_eq!(baselines.len(), 1);

        let baseline = &baselines[0];
        assert_eq!(baseline.schema_version, 1);
        assert_eq!(baseline.suite_id, "search-regression");
        assert_eq!(baseline.case_ids, vec!["search-rust-errors-001"]);
        assert_eq!(baseline.embedding_model, "text-embedding-3-small");
        assert_eq!(baseline.llm_model, "gpt-4o-mini");
        assert_eq!(baseline.metrics[0].case_id, "search-rust-errors-001");
        assert_eq!(baseline.thresholds.hit_at_k.minimum, Some(1.0));
        assert_eq!(baseline.thresholds.search_call_count.maximum, Some(2.0));
    }

    #[test]
    fn rejects_baseline_with_unknown_suite_case_ids() {
        let invalid = valid_baseline_json().replace(
            "\"case_ids\": [\"search-rust-errors-001\"]",
            "\"case_ids\": [\"missing-case\"]",
        );
        let (_tmp, dirs) = dirs_with_baseline(&invalid);
        let catalog = BenchmarkCatalog::load(&dirs).unwrap();

        let err = load_baselines(&dirs, &catalog).unwrap_err();
        assert!(err.to_string().contains("unknown case"));
    }
}
