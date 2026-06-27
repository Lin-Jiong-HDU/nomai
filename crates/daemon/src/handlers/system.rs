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
    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        // Clone the Arc before spawning so the closure is 'static. The
        // export pass takes per-entry locks internally during get/write.
        let entries: Arc<EntryService> = daemon.entries.clone();
        let result: ExportResult = { blocking(move || entries.export_to_fs()).await?? };
        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

#[cfg(test)]
mod descriptor_tests {
    use super::*;

    fn validate(schema: &Value, params: &Value) -> Result<(), Vec<String>> {
        let v = jsonschema::validator_for(schema).unwrap();
        v.validate(params).map_err(|errs| {
            errs.map(|e| format!("{e}")).collect::<Vec<_>>()
        })
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
