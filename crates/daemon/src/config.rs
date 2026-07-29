//! TOML config loading and validation.

use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub data: DataConfig,
    pub embedding: EmbeddingConfig,
    pub llm: LlmConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub serve: ServeConfig,
    #[serde(default)]
    pub chunking: ChunkingConfig,
    #[serde(default)]
    pub development: DevelopmentConfig,
    #[serde(default)]
    pub reranking: RerankingConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DevelopmentConfig {
    pub enabled: bool,
    pub benchmark_cases_dir: PathBuf,
    pub benchmark_suites_dir: PathBuf,
    pub benchmark_baselines_dir: PathBuf,
}

impl Default for DevelopmentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            benchmark_cases_dir: PathBuf::from("benchmark/cases"),
            benchmark_suites_dir: PathBuf::from("benchmark/suites"),
            benchmark_baselines_dir: PathBuf::from("benchmark/baselines"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,
    /// Root directory for FS-backed content storage (.nomai files). Defaults
    /// to `<data_dir>/store/` (sibling of the default db_path) when None.
    /// Tilde (`~`) prefixes are expanded at read time in `Daemon::new`.
    #[serde(default)]
    pub knowledge_root: Option<PathBuf>,
    /// Soft per-file cap on attachment size (bytes). `entry.create` /
    /// `block.append` / `block.update` reject attachments whose decoded
    /// bytes exceed this with `Validation("attachment too large: ...")`.
    /// Default 10 MiB — the practical ceiling for base64-in-JSON-RPC.
    #[serde(default = "default_attachment_max_bytes")]
    pub attachment_max_bytes: usize,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            db_path: default_db_path(),
            knowledge_root: None,
            attachment_max_bytes: default_attachment_max_bytes(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmbeddingConfig {
    pub base_url: String,
    pub api_key_env: String,
    pub model: String,
    pub dim: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmConfig {
    pub base_url: String,
    pub api_key_env: String,
    pub model: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// Soft capacity threshold. When `emb_cache` row count for the configured
    /// model exceeds this, `cache.stats` returns `warning: true`. The cache
    /// is never auto-evicted — `warn_rows` only flags that the user may want
    /// to run `cache.clear`.
    #[serde(default = "default_warn_rows")]
    pub warn_rows: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            warn_rows: default_warn_rows(),
        }
    }
}

fn default_warn_rows() -> u64 {
    100_000
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ChunkingConfig {
    /// Target chunk size in **characters** (not tokens). Block text is split
    /// by `chunking::chunk_text`: paragraph → sentence → hard cut. See
    /// `crates/core/src/chunking.rs`. Default 1024 keeps the pre-config
    /// behavior; raise it (e.g. 2048) when using a larger embedding model
    /// with a higher token budget, lower it for finer retrieval granularity.
    pub target_size: usize,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            target_size: default_chunk_target_size(),
        }
    }
}

fn default_chunk_target_size() -> usize {
    1024
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServeConfig {
    /// Seconds the resident daemon stays alive after the last client
    /// disconnects before exiting (debounce). Default 30.
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            idle_timeout_secs: default_idle_timeout_secs(),
        }
    }
}

fn default_idle_timeout_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RerankingConfig {
    /// When true, the LLM reranker is active. When false (default), the
    /// NoopReranker is used regardless of other fields in this section.
    #[serde(default)]
    pub enabled: bool,
    /// LLM model for reranking. Defaults to the value of `[llm].model`
    /// when empty.
    #[serde(default)]
    pub model: String,
    /// Maximum candidates sent to the LLM per rerank call. Excess
    /// candidates are truncated by original score before the LLM call.
    #[serde(default = "default_max_candidates")]
    pub max_candidates: usize,
}

impl Default for RerankingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            model: String::new(),
            max_candidates: default_max_candidates(),
        }
    }
}

fn default_max_candidates() -> usize {
    20
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config at {0}: {1}")]
    Read(PathBuf, std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("environment variable {0} (referenced by api_key_env) is not set")]
    MissingEnv(String),
    #[error("invalid config: {0}")]
    Invalid(String),
}

fn default_db_path() -> PathBuf {
    ProjectDirs::from("dev", "nomai", "nomai")
        .map(|d| d.data_dir().join("db.sqlite"))
        .unwrap_or_else(|| PathBuf::from("nomai.sqlite"))
}

fn default_attachment_max_bytes() -> usize {
    10 * 1024 * 1024
}

/// Resolve a `knowledge_root` config value to a concrete path. If the user
/// supplied a value, it's used as-is. Otherwise, default to `<data_dir>/store/`
/// (sibling of the default `db_path`). Mirrors how `default_db_path` uses
/// `ProjectDirs` so both storage roots share the same base directory.
pub fn default_knowledge_root() -> PathBuf {
    ProjectDirs::from("dev", "nomai", "nomai")
        .map(|d| d.data_dir().join("store"))
        .unwrap_or_else(|| PathBuf::from("store"))
}

pub fn default_config_path() -> PathBuf {
    ProjectDirs::from("dev", "nomai", "nomai")
        .map(|d| d.config_dir().join("config.toml"))
        .unwrap_or_else(|| PathBuf::from("config.toml"))
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from(&default_config_path())
    }

    pub fn load_from(path: &Path) -> Result<Self, ConfigError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| ConfigError::Read(path.to_path_buf(), e))?;
        let config: Config = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if std::env::var(&self.embedding.api_key_env).is_err() {
            return Err(ConfigError::MissingEnv(self.embedding.api_key_env.clone()));
        }
        if std::env::var(&self.llm.api_key_env).is_err() {
            return Err(ConfigError::MissingEnv(self.llm.api_key_env.clone()));
        }
        if self.development.enabled {
            self.validate_development_dirs()?;
        }
        Ok(())
    }

    fn validate_development_dirs(&self) -> Result<(), ConfigError> {
        let dirs = [
            ("benchmark_cases_dir", &self.development.benchmark_cases_dir),
            (
                "benchmark_suites_dir",
                &self.development.benchmark_suites_dir,
            ),
            (
                "benchmark_baselines_dir",
                &self.development.benchmark_baselines_dir,
            ),
        ];

        for (name, path) in dirs {
            if !path.is_dir() {
                return Err(ConfigError::Invalid(format!(
                    "development.{name} must exist and be a directory: {}",
                    path.display()
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    static CONFIG_TEST_LOCK: Mutex<()> = Mutex::new(());

    /// Acquire the global lock for config tests so env var mutations don't race
    /// under `cargo test` parallelism (process-global env vars are not thread-safe).
    fn lock() -> MutexGuard<'static, ()> {
        CONFIG_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn unset(name: &str) -> Option<String> {
        std::env::var(name).ok().inspect(|_| {
            // SAFETY: tests are single-threaded within this module.
            unsafe { std::env::remove_var(name) };
        })
    }

    fn restore(name: &str, value: Option<String>) {
        if let Some(v) = value {
            // SAFETY: tests are single-threaded within this module.
            unsafe { std::env::set_var(name, v) };
        }
    }

    fn parse_test_config_without_development() -> Config {
        let old_emb = unset("TEST_OPENAI_KEY");
        let old_llm = unset("TEST_OPENAI_KEY");
        // SAFETY: tests are single-threaded within this module.
        unsafe { std::env::set_var("TEST_OPENAI_KEY", "sk-test") };

        let toml_text = r#"
[data]
db_path = "/tmp/test.sqlite"

[embedding]
base_url = "https://api.openai.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "text-embedding-3-small"
dim = 1536

[llm]
base_url = "https://api.openai.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "gpt-4o-mini"
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_text).unwrap();
        let config = Config::load_from(tmp.path()).unwrap();

        restore("TEST_OPENAI_KEY", old_emb);
        restore("TEST_OPENAI_KEY", old_llm);
        config
    }

    fn config_with_development(enabled: bool, cases_dir: PathBuf) -> Config {
        let mut config = parse_test_config_without_development();
        config.development = DevelopmentConfig {
            enabled,
            benchmark_cases_dir: cases_dir,
            benchmark_suites_dir: PathBuf::from("benchmark/suites"),
            benchmark_baselines_dir: PathBuf::from("benchmark/baselines"),
        };
        config
    }

    #[test]
    fn parses_minimal_config() {
        let _guard = lock();
        let config = parse_test_config_without_development();
        assert_eq!(config.embedding.dim, 1536);
        assert_eq!(config.llm.model, "gpt-4o-mini");
        assert_eq!(config.data.db_path, PathBuf::from("/tmp/test.sqlite"));
        // [chunking] section absent → default 1024 (backward compat).
        assert_eq!(config.chunking.target_size, 1024);
    }

    #[test]
    fn rejects_missing_env_var() {
        let _guard = lock();
        let old = unset("TEST_DEFINITELY_MISSING_KEY");
        let toml_text = r#"
[embedding]
base_url = "https://example.com/v1"
api_key_env = "TEST_DEFINITELY_MISSING_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.com/v1"
api_key_env = "TEST_DEFINITELY_MISSING_KEY"
model = "x"
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_text).unwrap();
        let err = Config::load_from(tmp.path()).unwrap_err();
        assert!(matches!(err, ConfigError::MissingEnv(_)));
        restore("TEST_DEFINITELY_MISSING_KEY", old);
    }

    #[test]
    fn data_section_is_optional_with_default_db_path() {
        let _guard = lock();
        let old = unset("TEST_OPENAI_KEY");
        // SAFETY: tests are single-threaded within this module.
        unsafe { std::env::set_var("TEST_OPENAI_KEY", "sk") };
        let toml_text = r#"
[embedding]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_text).unwrap();
        let config = Config::load_from(tmp.path()).unwrap();
        // Default db_path ends with "db.sqlite".
        assert!(config.data.db_path.to_string_lossy().ends_with("db.sqlite"));
        restore("TEST_OPENAI_KEY", old);
    }

    #[test]
    fn chunking_section_accepts_custom_target_size() {
        let _guard = lock();
        let old = unset("TEST_OPENAI_KEY");
        // SAFETY: tests are single-threaded within this module.
        unsafe { std::env::set_var("TEST_OPENAI_KEY", "sk") };
        let toml_text = r#"
[embedding]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"

[chunking]
target_size = 2048
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_text).unwrap();
        let config = Config::load_from(tmp.path()).unwrap();
        assert_eq!(config.chunking.target_size, 2048);
        restore("TEST_OPENAI_KEY", old);
    }

    #[test]
    fn data_section_defaults_attachment_max_bytes_to_10mib() {
        let _guard = lock();
        let old = unset("TEST_OPENAI_KEY");
        // SAFETY: tests are single-threaded within this module.
        unsafe { std::env::set_var("TEST_OPENAI_KEY", "sk") };
        let toml_text = r#"
[embedding]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_text).unwrap();
        let config = Config::load_from(tmp.path()).unwrap();
        // Default is 10 MiB.
        assert_eq!(config.data.attachment_max_bytes, 10 * 1024 * 1024);
        restore("TEST_OPENAI_KEY", old);
    }

    #[test]
    fn data_section_accepts_custom_attachment_max_bytes() {
        let _guard = lock();
        let old = unset("TEST_OPENAI_KEY");
        // SAFETY: tests are single-threaded within this module.
        unsafe { std::env::set_var("TEST_OPENAI_KEY", "sk") };
        let toml_text = r#"
[data]
attachment_max_bytes = 2048

[embedding]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_text).unwrap();
        let config = Config::load_from(tmp.path()).unwrap();
        assert_eq!(config.data.attachment_max_bytes, 2048);
        restore("TEST_OPENAI_KEY", old);
    }

    #[test]
    fn development_defaults_to_disabled_and_relative_dirs() {
        let _guard = lock();
        let config = parse_test_config_without_development();
        assert!(!config.development.enabled);
        assert_eq!(
            config.development.benchmark_cases_dir,
            PathBuf::from("benchmark/cases")
        );
        assert_eq!(
            config.development.benchmark_suites_dir,
            PathBuf::from("benchmark/suites")
        );
        assert_eq!(
            config.development.benchmark_baselines_dir,
            PathBuf::from("benchmark/baselines")
        );
    }

    #[test]
    fn enabled_development_requires_existing_benchmark_dirs() {
        let _guard = lock();
        let config = config_with_development(true, PathBuf::from("/missing/cases"));

        assert!(matches!(config.validate(), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn serve_section_defaults_to_30s() {
        let _guard = lock();
        let old = unset("TEST_OPENAI_KEY");
        // SAFETY: tests are single-threaded within this module.
        unsafe { std::env::set_var("TEST_OPENAI_KEY", "sk") };
        let toml_text = r#"
[embedding]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_text).unwrap();
        let config = Config::load_from(tmp.path()).unwrap();
        assert_eq!(config.serve.idle_timeout_secs, 30);
        restore("TEST_OPENAI_KEY", old);
    }

    #[test]
    fn serve_section_accepts_custom_idle_timeout() {
        let _guard = lock();
        let old = unset("TEST_OPENAI_KEY");
        // SAFETY: tests are single-threaded within this module.
        unsafe { std::env::set_var("TEST_OPENAI_KEY", "sk") };
        let toml_text = r#"
[embedding]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"
dim = 8

[llm]
base_url = "https://example.com/v1"
api_key_env = "TEST_OPENAI_KEY"
model = "x"

[serve]
idle_timeout_secs = 5
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_text).unwrap();
        let config = Config::load_from(tmp.path()).unwrap();
        assert_eq!(config.serve.idle_timeout_secs, 5);
        restore("TEST_OPENAI_KEY", old);
    }
}
