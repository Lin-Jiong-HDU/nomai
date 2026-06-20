//! provider.* handlers.

use serde_json::{Value, json};

use nomai_core::CoreError;

use crate::daemon::Daemon;

pub async fn list(daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
    Ok(json!({
        "embedding": {
            "name": daemon.embedder.name(),
            "model": daemon.embedding_model,
        },
        "llm": {
            "name": daemon.llm.name(),
            "model": daemon.llm_model,
        }
    }))
}
