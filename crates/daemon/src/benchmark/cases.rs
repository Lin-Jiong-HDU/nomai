#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use nomai_core::{BlockType, CoreError};
use serde::Deserialize;
use ulid::Ulid;

use crate::benchmark::{config_error, read_to_string, sorted_files};
use crate::config::DevelopmentConfig;

#[derive(Debug, Clone)]
pub(crate) struct BenchmarkCatalog {
    suites: BTreeMap<String, SuiteSpec>,
    cases: BTreeMap<String, CaseSpec>,
}

impl BenchmarkCatalog {
    pub(crate) fn load(config: &DevelopmentConfig) -> Result<Self, CoreError> {
        let cases = load_cases(&config.benchmark_cases_dir)?;
        let suites = load_suites(&config.benchmark_suites_dir, &cases)?;
        Ok(Self { suites, cases })
    }

    pub(crate) fn suite(&self, id: &str) -> Result<&SuiteSpec, CoreError> {
        self.suites
            .get(id)
            .ok_or_else(|| CoreError::Config(format!("unknown benchmark suite: {id}")))
    }

    pub(crate) fn case(&self, id: &str) -> Result<&CaseSpec, CoreError> {
        self.cases
            .get(id)
            .ok_or_else(|| CoreError::Config(format!("unknown benchmark case: {id}")))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SuiteSpec {
    pub id: String,
    pub cases: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CaseSpec {
    pub id: String,
    pub question: String,
    #[serde(default)]
    pub fixtures: Vec<FixtureEntrySpec>,
    pub retrieval: RetrievalSpec,
    pub answer: AnswerSpec,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FixtureEntrySpec {
    pub id: Ulid,
    pub title: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub attrs: serde_json::Value,
    pub blocks: Vec<FixtureBlockSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FixtureBlockSpec {
    pub id: Ulid,
    #[serde(rename = "type")]
    pub block_type: String,
    pub text: String,
    #[serde(default)]
    pub attrs: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RetrievalSpec {
    pub required_tools: Vec<String>,
    pub relevant_entry_ids: Vec<Ulid>,
    #[serde(default)]
    pub relevant_block_ids: Vec<Ulid>,
    pub k: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AnswerSpec {
    pub reference: String,
    pub judge: String,
}

fn load_cases(dir: &Path) -> Result<BTreeMap<String, CaseSpec>, CoreError> {
    let mut cases = BTreeMap::new();

    for path in sorted_files(dir, "toml")? {
        let content = read_to_string(&path)?;
        let case: CaseSpec = toml::from_str(&content)
            .map_err(|err| config_error(&path, format!("parse failed: {err}")))?;
        validate_case(&path, &case)?;

        if cases.insert(case.id.clone(), case).is_some() {
            return Err(config_error(&path, "duplicate case id"));
        }
    }

    Ok(cases)
}

fn load_suites(
    dir: &Path,
    cases: &BTreeMap<String, CaseSpec>,
) -> Result<BTreeMap<String, SuiteSpec>, CoreError> {
    let mut suites = BTreeMap::new();

    for path in sorted_files(dir, "toml")? {
        let content = read_to_string(&path)?;
        let suite: SuiteSpec = toml::from_str(&content)
            .map_err(|err| config_error(&path, format!("parse failed: {err}")))?;

        for case_id in &suite.cases {
            if !cases.contains_key(case_id) {
                return Err(config_error(
                    &path,
                    format!("unknown case in suite {}: {case_id}", suite.id),
                ));
            }
        }

        if suites.insert(suite.id.clone(), suite).is_some() {
            return Err(config_error(&path, "duplicate suite id"));
        }
    }

    Ok(suites)
}

fn validate_case(path: &Path, case: &CaseSpec) -> Result<(), CoreError> {
    if case.retrieval.k == 0 {
        return Err(config_error(path, "k must be greater than 0"));
    }

    let mut fixture_entry_ids = HashSet::new();
    let mut fixture_block_ids = HashSet::new();

    for entry in &case.fixtures {
        fixture_entry_ids.insert(entry.id);
        for block in &entry.blocks {
            BlockType::from_str(&block.block_type).ok_or_else(|| {
                config_error(path, format!("unknown block type: {}", block.block_type))
            })?;
            fixture_block_ids.insert(block.id);
        }
    }

    for entry_id in &case.retrieval.relevant_entry_ids {
        if !fixture_entry_ids.contains(entry_id) {
            return Err(config_error(
                path,
                format!("missing relevant fixture entry id: {entry_id}"),
            ));
        }
    }

    for block_id in &case.retrieval.relevant_block_ids {
        if !fixture_block_ids.contains(block_id) {
            return Err(config_error(
                path,
                format!("missing relevant fixture block id: {block_id}"),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::BenchmarkCatalog;
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
tags = ["rust", "errors"]

[fixtures.attrs]
kind = "fixture"

[[fixtures.blocks]]
id = "{fixture_block_id}"
type = "note"
text = "Rust compiler errors usually point at the exact file and line."

[fixtures.blocks.attrs]
source = "guide"

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

    fn write_file(path: PathBuf, contents: &str) {
        std::fs::write(path, contents).unwrap();
    }

    fn valid_dirs() -> (TempDir, DevelopmentConfig) {
        let tmp = tempfile::tempdir().unwrap();
        let cases_dir = tmp.path().join("cases");
        let suites_dir = tmp.path().join("suites");
        let baselines_dir = tmp.path().join("baselines");
        std::fs::create_dir_all(&cases_dir).unwrap();
        std::fs::create_dir_all(&suites_dir).unwrap();
        std::fs::create_dir_all(&baselines_dir).unwrap();

        write_file(
            cases_dir.join("search-rust-errors-001.toml"),
            &valid_case_toml(),
        );
        write_file(
            suites_dir.join("search-regression.toml"),
            valid_suite_toml(),
        );

        let dirs = DevelopmentConfig {
            enabled: true,
            benchmark_cases_dir: cases_dir,
            benchmark_suites_dir: suites_dir,
            benchmark_baselines_dir: baselines_dir,
        };
        (tmp, dirs)
    }

    fn dirs_with_case_files(case_files: &[(&str, String)]) -> (TempDir, DevelopmentConfig) {
        let tmp = tempfile::tempdir().unwrap();
        let cases_dir = tmp.path().join("cases");
        let suites_dir = tmp.path().join("suites");
        let baselines_dir = tmp.path().join("baselines");
        std::fs::create_dir_all(&cases_dir).unwrap();
        std::fs::create_dir_all(&suites_dir).unwrap();
        std::fs::create_dir_all(&baselines_dir).unwrap();

        for (name, contents) in case_files {
            write_file(cases_dir.join(name), contents);
        }
        write_file(
            suites_dir.join("search-regression.toml"),
            valid_suite_toml(),
        );

        let dirs = DevelopmentConfig {
            enabled: true,
            benchmark_cases_dir: cases_dir,
            benchmark_suites_dir: suites_dir,
            benchmark_baselines_dir: baselines_dir,
        };
        (tmp, dirs)
    }

    fn dirs_with_suite_case(case_id: &str) -> (TempDir, DevelopmentConfig) {
        let tmp = tempfile::tempdir().unwrap();
        let cases_dir = tmp.path().join("cases");
        let suites_dir = tmp.path().join("suites");
        let baselines_dir = tmp.path().join("baselines");
        std::fs::create_dir_all(&cases_dir).unwrap();
        std::fs::create_dir_all(&suites_dir).unwrap();
        std::fs::create_dir_all(&baselines_dir).unwrap();

        write_file(
            cases_dir.join("search-rust-errors-001.toml"),
            &valid_case_toml(),
        );
        write_file(
            suites_dir.join("search-regression.toml"),
            &format!(
                r#"
id = "search-regression"
cases = ["{case_id}"]
"#
            ),
        );

        let dirs = DevelopmentConfig {
            enabled: true,
            benchmark_cases_dir: cases_dir,
            benchmark_suites_dir: suites_dir,
            benchmark_baselines_dir: baselines_dir,
        };
        (tmp, dirs)
    }

    #[test]
    fn loads_case_and_resolves_fixture_ids() {
        let (_tmp, dirs) = valid_dirs();
        let catalog = BenchmarkCatalog::load(&dirs).unwrap();

        let suite = catalog.suite("search-regression").unwrap();
        assert_eq!(suite.cases, vec!["search-rust-errors-001"]);

        let case = catalog.case("search-rust-errors-001").unwrap();
        assert_eq!(case.retrieval.k, 5);
        assert_eq!(case.question, "How do I inspect Rust compiler errors?");
        assert_eq!(
            case.answer.reference,
            "Inspect the compiler output and fetch the relevant evidence entry."
        );
        assert!(
            case.fixtures
                .iter()
                .any(|entry| entry.id == FIXTURE_ENTRY_ID)
        );
        assert!(
            case.fixtures
                .iter()
                .flat_map(|entry| entry.blocks.iter())
                .any(|block| block.id == FIXTURE_BLOCK_ID)
        );
    }

    #[test]
    fn rejects_duplicate_case_ids() {
        let (_tmp, dirs) =
            dirs_with_case_files(&[("a.toml", valid_case_toml()), ("b.toml", valid_case_toml())]);

        let err = BenchmarkCatalog::load(&dirs).unwrap_err();
        assert!(err.to_string().contains("duplicate case id"));
    }

    #[test]
    fn rejects_suite_with_unknown_case() {
        let (_tmp, dirs) = dirs_with_suite_case("missing");
        let err = BenchmarkCatalog::load(&dirs).unwrap_err();
        assert!(err.to_string().contains("unknown case"));
    }

    #[test]
    fn rejects_invalid_block_type() {
        let invalid = valid_case_toml().replace("type = \"note\"", "type = \"not-a-block\"");
        let (_tmp, dirs) = dirs_with_case_files(&[("search-rust-errors-001.toml", invalid)]);

        let err = BenchmarkCatalog::load(&dirs).unwrap_err();
        assert!(err.to_string().contains("unknown block type"));
    }

    #[test]
    fn rejects_zero_retrieval_k() {
        let invalid = valid_case_toml().replace("k = 5", "k = 0");
        let (_tmp, dirs) = dirs_with_case_files(&[("search-rust-errors-001.toml", invalid)]);

        let err = BenchmarkCatalog::load(&dirs).unwrap_err();
        assert!(err.to_string().contains("k must be greater than 0"));
    }

    #[test]
    fn rejects_missing_relevant_fixture_ids() {
        let invalid = valid_case_toml().replace(
            &format!("relevant_entry_ids = [\"{FIXTURE_ENTRY_ID}\"]"),
            "relevant_entry_ids = [\"01ARZ3NDEKTSV4RRFFQ69G5FAV\"]",
        );
        let (_tmp, dirs) = dirs_with_case_files(&[("search-rust-errors-001.toml", invalid)]);

        let err = BenchmarkCatalog::load(&dirs).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing relevant fixture entry id")
        );
    }
}
