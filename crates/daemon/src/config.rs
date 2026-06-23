//! TOML config loading and validation.

use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub data: DataConfig,
    pub embedding: EmbeddingConfig,
    pub llm: LlmConfig,
    #[serde(default)]
    pub cache: CacheConfig,
}

#[derive(Debug, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,
    /// Root directory for FS-backed content storage (.nomai files). Defaults
    /// to `<data_dir>/store/` (sibling of the default db_path) when None.
    /// Tilde (`~`) prefixes are expanded at read time in `Daemon::new`.
    #[serde(default)]
    pub knowledge_root: Option<PathBuf>,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            db_path: default_db_path(),
            knowledge_root: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct EmbeddingConfig {
    pub base_url: String,
    pub api_key_env: String,
    pub model: String,
    pub dim: usize,
}

#[derive(Debug, Deserialize)]
pub struct LlmConfig {
    pub base_url: String,
    pub api_key_env: String,
    pub model: String,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config at {0}: {1}")]
    Read(PathBuf, std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("environment variable {0} (referenced by api_key_env) is not set")]
    MissingEnv(String),
}

fn default_db_path() -> PathBuf {
    ProjectDirs::from("dev", "nomai", "nomai")
        .map(|d| d.data_dir().join("db.sqlite"))
        .unwrap_or_else(|| PathBuf::from("nomai.sqlite"))
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

    #[test]
    fn parses_minimal_config() {
        let _guard = lock();
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
        assert_eq!(config.embedding.dim, 1536);
        assert_eq!(config.llm.model, "gpt-4o-mini");
        assert_eq!(config.data.db_path, PathBuf::from("/tmp/test.sqlite"));

        restore("TEST_OPENAI_KEY", old_emb);
        restore("TEST_OPENAI_KEY", old_llm);
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
}
