#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use nomai_core::CoreError;
use serde::Deserialize;

use crate::benchmark::cases::BenchmarkCatalog;
use crate::benchmark::metrics::{BaselineComparison, RunReport};
use crate::benchmark::{config_error, read_to_string, sorted_files};
use crate::config::DevelopmentConfig;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BaselineFile {
    pub schema_version: u32,
    pub suite_id: String,
    pub case_ids: Vec<String>,
    pub provider: BaselineProvider,
    pub embedding_model: String,
    pub llm_model: String,
    pub metrics: BaselineMetrics,
    pub thresholds: BaselineThresholds,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BaselineProvider {
    pub name: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BaselineMetrics {
    pub hit_at_k: f64,
    pub recall_at_k: f64,
    pub mrr: f64,
    pub ndcg: f64,
    pub required_tools_success: f64,
    pub evidence_entry_hit: f64,
    pub search_call_count: f64,
    pub latency_ms_total: f64,
    pub latency_ms_average: f64,
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
    #[serde(default)]
    pub required_tools_success: MetricThreshold,
    #[serde(default)]
    pub evidence_entry_hit: MetricThreshold,
    pub search_call_count: MetricThreshold,
    pub latency_ms_total: MetricThreshold,
    pub latency_ms_average: MetricThreshold,
}

pub(crate) fn compare_baseline(current: &RunReport, baseline: &BaselineFile) -> BaselineComparison {
    let mut compatible = true;
    let mut violations = Vec::new();
    let metadata = &current.metadata;

    if baseline.schema_version != metadata.schema_version {
        incompatible(
            &mut compatible,
            &mut violations,
            format!(
                "schema version mismatch: current {}, baseline {}",
                metadata.schema_version, baseline.schema_version
            ),
        );
    }
    if baseline.suite_id != metadata.suite_id {
        incompatible(
            &mut compatible,
            &mut violations,
            format!(
                "suite mismatch: current {}, baseline {}",
                metadata.suite_id, baseline.suite_id
            ),
        );
    }
    if baseline.case_ids != metadata.case_ids {
        incompatible(
            &mut compatible,
            &mut violations,
            "case order or membership mismatch".into(),
        );
    }
    if baseline.provider.name != metadata.provider_name {
        incompatible(
            &mut compatible,
            &mut violations,
            format!(
                "provider mismatch: current {}, baseline {}",
                metadata.provider_name, baseline.provider.name
            ),
        );
    }
    if baseline.provider.base_url != metadata.provider_base_url {
        incompatible(
            &mut compatible,
            &mut violations,
            "provider base URL mismatch".into(),
        );
    }
    if baseline.embedding_model != metadata.embedding_model {
        incompatible(
            &mut compatible,
            &mut violations,
            format!(
                "embedding model mismatch: current {}, baseline {}",
                metadata.embedding_model, baseline.embedding_model
            ),
        );
    }
    if baseline.llm_model != metadata.llm_model {
        incompatible(
            &mut compatible,
            &mut violations,
            format!(
                "llm model mismatch: current {}, baseline {}",
                metadata.llm_model, baseline.llm_model
            ),
        );
    }

    let current_values = summary_values(&current.summary);
    let baseline_values = baseline_values(&baseline.metrics);
    let mut deltas = BTreeMap::new();
    for (name, current_value) in &current_values {
        if let Some(baseline_value) = baseline_values.get(name) {
            deltas.insert(name.clone(), current_value - baseline_value);
        }
    }

    for (name, value) in current_values {
        if let Some(threshold) = threshold_for(&baseline.thresholds, &name) {
            check_threshold(&name, value, threshold, &mut violations);
        }
    }

    BaselineComparison {
        compatible,
        deltas,
        violations,
    }
}

fn incompatible(compatible: &mut bool, violations: &mut Vec<String>, message: String) {
    *compatible = false;
    violations.push(message);
}

fn summary_values(summary: &crate::benchmark::metrics::SummaryMetrics) -> BTreeMap<String, f64> {
    let mut values = BTreeMap::from([
        ("hit_at_k".into(), summary.hit_at_k),
        ("recall_at_k".into(), summary.recall_at_k),
        ("mrr".into(), summary.mrr),
        ("ndcg".into(), summary.ndcg),
        (
            "required_tools_success".into(),
            summary.required_tools_success,
        ),
        ("evidence_entry_hit".into(), summary.evidence_entry_hit),
        ("search_call_count".into(), summary.search_call_count),
        ("latency_ms_total".into(), summary.latency_ms_total),
        ("latency_ms_average".into(), summary.latency_ms_average),
    ]);
    if let Some(judge_score) = summary.judge_score {
        values.insert("judge_score".into(), judge_score);
    }
    values
}

fn baseline_values(metrics: &BaselineMetrics) -> BTreeMap<String, f64> {
    let mut values = BTreeMap::from([
        ("hit_at_k".into(), metrics.hit_at_k),
        ("recall_at_k".into(), metrics.recall_at_k),
        ("mrr".into(), metrics.mrr),
        ("ndcg".into(), metrics.ndcg),
        (
            "required_tools_success".into(),
            metrics.required_tools_success,
        ),
        ("evidence_entry_hit".into(), metrics.evidence_entry_hit),
        ("search_call_count".into(), metrics.search_call_count),
        ("latency_ms_total".into(), metrics.latency_ms_total),
        ("latency_ms_average".into(), metrics.latency_ms_average),
    ]);
    if let Some(judge_score) = metrics.judge_score {
        values.insert("judge_score".into(), judge_score);
    }
    values
}

fn threshold_for<'a>(
    thresholds: &'a BaselineThresholds,
    name: &str,
) -> Option<&'a MetricThreshold> {
    Some(match name {
        "hit_at_k" => &thresholds.hit_at_k,
        "recall_at_k" => &thresholds.recall_at_k,
        "mrr" => &thresholds.mrr,
        "ndcg" => &thresholds.ndcg,
        "judge_score" => &thresholds.judge_score,
        "required_tools_success" => &thresholds.required_tools_success,
        "evidence_entry_hit" => &thresholds.evidence_entry_hit,
        "search_call_count" => &thresholds.search_call_count,
        "latency_ms_total" => &thresholds.latency_ms_total,
        "latency_ms_average" => &thresholds.latency_ms_average,
        _ => return None,
    })
}

fn check_threshold(
    name: &str,
    value: f64,
    threshold: &MetricThreshold,
    violations: &mut Vec<String>,
) {
    if let Some(minimum) = threshold.minimum
        && value < minimum
    {
        violations.push(format!("{name}={value} is below minimum {minimum}"));
    }
    if let Some(maximum) = threshold.maximum
        && value > maximum
    {
        violations.push(format!("{name}={value} is above maximum {maximum}"));
    }
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::load_baselines;
    use crate::benchmark::cases::BenchmarkCatalog;
    use crate::benchmark::metrics::RunReport;
    use crate::config::DevelopmentConfig;
    use std::path::PathBuf;
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
judge = false
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
  "provider": {
    "name": "openai-compatible",
    "base_url": "https://api.openai.com/v1"
  },
  "embedding_model": "text-embedding-3-small",
  "llm_model": "gpt-4o-mini",
  "metrics": {
    "hit_at_k": 1.0,
    "recall_at_k": 1.0,
    "mrr": 1.0,
    "ndcg": 1.0,
    "required_tools_success": 1.0,
    "evidence_entry_hit": 1.0,
    "search_call_count": 1.0,
    "latency_ms_total": 250.0,
    "latency_ms_average": 250.0,
    "judge_score": 0.9
  },
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
        assert_eq!(baseline.provider.name, "openai-compatible");
        assert_eq!(baseline.embedding_model, "text-embedding-3-small");
        assert_eq!(baseline.llm_model, "gpt-4o-mini");
        assert_eq!(baseline.metrics.hit_at_k, 1.0);
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

    #[test]
    fn incompatible_baseline_is_not_a_pass_and_reports_deltas() {
        let baseline: super::BaselineFile = serde_json::from_str(valid_baseline_json()).unwrap();
        let current = RunReport {
            metadata: crate::benchmark::metrics::RunMetadata {
                schema_version: 1,
                suite_id: "search-regression".into(),
                case_ids: vec!["search-rust-errors-001".into()],
                provider_name: "different-provider".into(),
                provider_base_url: "https://api.openai.com/v1".into(),
                embedding_model: "text-embedding-3-small".into(),
                llm_model: "gpt-4o-mini".into(),
            },
            cases: vec![],
            summary: crate::benchmark::metrics::SummaryMetrics {
                hit_at_k: 0.5,
                recall_at_k: 0.5,
                mrr: 0.5,
                ndcg: 0.5,
                required_tools_success: 1.0,
                evidence_entry_hit: 1.0,
                search_call_count: 3.0,
                latency_ms_total: 600.0,
                latency_ms_average: 600.0,
                judge_score: None,
            },
            baseline_comparison: None,
        };

        let comparison = super::compare_baseline(&current, &baseline);
        assert!(!comparison.compatible);
        assert!(comparison.violations.iter().any(|v| v.contains("provider")));
        assert_eq!(comparison.deltas["hit_at_k"], -0.5);
        assert!(comparison.violations.iter().any(|v| v.contains("hit_at_k")));
    }

    #[test]
    fn configured_nomai_kb_assets_load() {
        let Some(root) = std::env::var_os("NOMAI_KB_BENCHMARK_DIR") else {
            return;
        };
        let root = PathBuf::from(root);
        let dirs = DevelopmentConfig {
            enabled: true,
            benchmark_cases_dir: root.join("cases"),
            benchmark_suites_dir: root.join("suites"),
            benchmark_baselines_dir: root.join("baselines"),
        };

        let catalog = BenchmarkCatalog::load(&dirs).unwrap();
        let baseline = load_baselines(&dirs, &catalog).unwrap();
        assert_eq!(catalog.suite("search-regression").unwrap().cases.len(), 2);
        assert_eq!(baseline.len(), 1);
        assert_eq!(
            baseline[0].case_ids,
            catalog.suite("search-regression").unwrap().cases
        );
    }
}
