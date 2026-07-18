//! system.* handlers. Plan 6 Task 3 introduces `system.export_to_fs`, a Spec
//! §12 migration utility that walks every entry row and renders the `.nomai`
//! file for any that lack one. Post-Plan-3 entries created via `entry.create`
//! already have `.nomai` and are skipped; this is for rows created via direct
//! DB manipulation (e.g. an import path that bypasses the service layer).
//!
//! `system.export_to_fs` runs the full pass and returns
//! `{ exported, skipped, errors }`.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use nomai_core::{CoreError, EntryService, ExportResult};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;
use crate::rpc::RpcHandler;
use nomai_protocol::method::system::EXPORT_TO_FS as SYSTEM_EXPORT_TO_FS;
use nomai_protocol::method::system::RESTART as SYSTEM_RESTART;

pub struct ExportToFs;

#[async_trait]
impl RpcHandler for ExportToFs {
    fn method(&self) -> &'static str {
        SYSTEM_EXPORT_TO_FS
    }
    fn description(&self) -> &'static str {
        "Walk every entry row and render a .nomai file for any that lack one. Skips entries that already have a .nomai on disk. Returns {exported, skipped, errors}."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    fn is_mutating(&self) -> bool {
        // Renders .nomai files into knowledge_root; must serialize against
        // sync.run's rebase (same race class as entry/block writes, spec §8).
        true
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Clone the Arc before spawning so the closure is 'static. The
        // export pass takes per-entry locks internally during get/write.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let result: ExportResult = { blocking(move || entries.export_to_fs()).await?? };
        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

/// `system.restart`: rebuild the resident daemon's internal state
/// (SQLite connection, embedder, LLM, caches) in-process from the live
/// `Config`, then atomically swap the new `Daemon` into the slot. In-flight
/// RPCs already hold the old `Arc<Daemon>` and finish against it; new RPCs
/// read the fresh one. `emb_cache` (a SQLite table) survives the reopen, so
/// unchanged bodies don't re-hit the provider. Returns `{ ok: true }`.
///
/// Emits NO events (matches the `daemon.rs:680` no-event convention for
/// daemon-level ops). `is_mutating()` is `false`: this rebuilds in-memory
/// state and reopens SQLite — it is not a knowledge_root mutation, and
/// taking `sync_lock` would deadlock against an in-flight `sync.run`.
pub struct Restart;

#[async_trait]
impl RpcHandler for Restart {
    fn method(&self) -> &'static str {
        SYSTEM_RESTART
    }
    fn description(&self) -> &'static str {
        "Rebuild the resident daemon's internal state (sqlite/embedder/llm/cache) \
         in-process without dropping client connections. Use when embedding calls \
         (search.semantic / ingest) start failing due to long-uptime state decay. \
         Returns {ok: true}. No params."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    fn is_mutating(&self) -> bool {
        // Rebuilds in-memory + reopens sqlite; not a knowledge_root mutation,
        // and must NOT take sync_lock (would deadlock if a sync.run is in
        // flight — the old daemon's in-flight requests finish on the old
        // sqlite connection, the new one opens fresh).
        false
    }
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        let slot = daemon.restart_slot().ok_or_else(|| {
            CoreError::Config("system.restart unavailable: daemon not running in a slot".into())
        })?;
        // Rebuild from the same config. Task 1 made `config: Option<Arc<Config>>`
        // (lib-mode from_services/builder daemons are None — they have no
        // Config to rebuild from). Reject those: restart only applies to a
        // config-backed daemon.
        let cfg_arc = daemon.config.as_ref().ok_or_else(|| {
            CoreError::Config("system.restart requires a config-backed daemon".into())
        })?.clone();
        // emb_cache (sqlite table) survives the reopen, so unchanged bodies
        // don't re-hit the provider.
        let new_daemon = Daemon::from_arc(cfg_arc).await?;
        new_daemon.set_restart_slot(std::sync::Arc::downgrade(&slot));
        // Atomic swap. In-flight RPCs already hold the old Arc<Daemon> and
        // finish against it; new RPCs read the new one.
        *slot.write().unwrap() = Arc::new(new_daemon);
        Ok(serde_json::json!({ "ok": true }))
    }
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params)
            .map_err(|errs| errs.map(|e| format!("{e}")).collect::<Vec<_>>())
    }

    #[test]
    fn export_to_fs_schema_accepts_empty_object() {
        let schema = ExportToFs.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({})).is_ok());
    }

    #[test]
    fn export_to_fs_schema_rejects_extra_props() {
        let schema = ExportToFs.input_schema().unwrap();
        assert!(validate(&schema, &serde_json::json!({"foo": 1})).is_err());
    }
}

#[cfg(test)]
mod restart_tests {
    use super::*;
    use crate::daemon::{Daemon, DaemonSlot};
    use std::sync::{Arc, RwLock};

    use crate::config::{
        CacheConfig, ChunkingConfig, Config, DataConfig, EmbeddingConfig, LlmConfig, ServeConfig,
    };

    /// Minimal config for restart tests. `base_url` is unreachable on purpose:
    /// `Daemon::from_arc` on an empty DB constructs the providers but does NOT
    /// call them, so rebuild succeeds without a live embedding server.
    /// (Task 5 overrides `embedding.base_url` to a wiremock URI.) Sets
    /// NOMAI_TEST_KEY so `api_key_env` resolves.
    fn restart_test_config(tmp: &tempfile::TempDir) -> Config {
        // SAFETY: tests are single-threaded within this module; this env var
        // name is unique to the restart test path (NOMAI_TEST_KEY), and the
        // caller serializes against other env-touching tests in the daemon
        // suite via the global FROM_ARC_LOCK-style pattern if needed.
        unsafe {
            std::env::set_var("NOMAI_TEST_KEY", "sk-test");
        }
        Config {
            data: DataConfig {
                db_path: tmp.path().join("t.sqlite"),
                knowledge_root: None,
                attachment_max_bytes: 10 * 1024 * 1024,
            },
            embedding: EmbeddingConfig {
                base_url: "http://127.0.0.1:1".into(),
                api_key_env: "NOMAI_TEST_KEY".into(),
                model: "m".into(),
                dim: 8,
            },
            llm: LlmConfig {
                base_url: "http://127.0.0.1:1".into(),
                api_key_env: "NOMAI_TEST_KEY".into(),
                model: "m".into(),
            },
            cache: CacheConfig::default(),
            serve: ServeConfig::default(),
            chunking: ChunkingConfig::default(),
        }
    }

    #[tokio::test]
    async fn restart_swaps_slot_and_returns_ok() {
        // Daemon::from_arc opens its own Connection; like the production
        // binary and the from_arc_retains_config_for_restart test, we must
        // register the sqlite-vec auto-extension before that open so the
        // V9 vec0 migration succeeds.
        nomai_core::storage::init_sqlite_extensions();

        let tmp = tempfile::tempdir().unwrap();
        let d = Daemon::new(restart_test_config(&tmp)).await.unwrap();
        let slot: DaemonSlot = Arc::new(RwLock::new(Arc::new(d)));
        slot.read()
            .unwrap()
            .clone()
            .set_restart_slot(Arc::downgrade(&slot));

        let cfg_arc = slot.read().unwrap().config.clone();
        let before: *const Daemon = Arc::as_ptr(&slot.read().unwrap().clone());
        let restart = super::Restart;
        // Clone the Daemon Arc out of the slot and drop the read guard BEFORE
        // calling restart: Restart::call takes a write lock on the same slot,
        // so holding a read guard across the `.await` would self-deadlock.
        let daemon_snap = slot.read().unwrap().clone();
        let out = restart.call(&daemon_snap, serde_json::json!({})).await.unwrap();
        assert_eq!(out, serde_json::json!({ "ok": true }));
        let after: *const Daemon = Arc::as_ptr(&slot.read().unwrap().clone());
        assert_ne!(before, after, "restart must install a new Daemon");
        let _ = cfg_arc;
    }
}
