//! Custom RPC example: implement `RpcHandler`, register it, and dispatch.
//!
//! This example demonstrates the lib-mode extension point:
//!   1. Build a Daemon via `from_services` (no config.toml needed)
//!   2. Implement `RpcHandler` for a custom struct
//!   3. Call `register_handler` to add it to the registry
//!   4. Dispatch a request to the custom RPC
//!
//! Run: `cargo run --example custom_rpc`

use std::sync::Arc;

use async_trait::async_trait;
use rusqlite::Connection;
use serde_json::{Value, json};

use nomai_core::{CoreError, EntryListQuery, EntryService};
use nomai_daemon::daemon::Daemon;
use nomai_daemon::rpc::RpcHandler;

/// A custom RPC that returns entry count.
struct Stats;

#[async_trait]
impl RpcHandler for Stats {
    fn method(&self) -> &'static str {
        "stats"
    }

    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        let entries = daemon.entries().clone();
        let entry_total = tokio::task::spawn_blocking(move || {
            entries.list(EntryListQuery::default()).map(|r| r.total)
        })
        .await
        .map_err(|e| CoreError::Config(format!("join error: {e}")))??;

        Ok(json!({
            "entries": entry_total,
            "description": "Database statistics via custom RPC"
        }))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize SQLite extensions (sqlite-vec)
    nomai_core::storage::init_sqlite_extensions();

    // 2. Open in-memory database + run migrations
    let conn = Arc::new(std::sync::Mutex::new(Connection::open_in_memory()?));
    let entries = Arc::new(EntryService::new(conn.clone())?);

    // 3. Seed some data
    entries.create(nomai_core::CreateEntry {
        title: "Hello".into(),
        body: "World".into(),
        tags: None,
        attrs: None,
        source: None,
    })?;
    entries.create(nomai_core::CreateEntry {
        title: "Second".into(),
        body: "Entry".into(),
        tags: None,
        attrs: None,
        source: None,
    })?;

    // 4. Build Daemon via from_services (no config.toml needed)
    //    Using a dummy embedder/llm since Stats doesn't need them.
    let embedder: Arc<dyn nomai_providers::EmbeddingProvider> = Arc::new(
        nomai_providers::OpenAiCompatibleEmbed::new("http://localhost", "dummy", "dummy", 8),
    );
    let llm: Arc<dyn nomai_providers::LlmProvider> = Arc::new(
        nomai_providers::OpenAiCompatibleLlm::new("http://localhost", "dummy", "dummy"),
    );

    let mut daemon = Daemon::from_services(conn, embedder, llm, 8, "example-model", 100_000)?;

    // 5. Register custom RPC
    daemon.register_handler(Arc::new(Stats));

    // 6. Dispatch the custom RPC
    let req = nomai_protocol::Request {
        jsonrpc: nomai_protocol::JSONRPC_VERSION.into(),
        id: Some(nomai_protocol::Id::Number(1)),
        method: "stats".into(),
        params: None,
    };
    let resp = daemon.dispatch(req).await;
    println!("Response: {}", serde_json::to_string_pretty(&resp)?);

    // 7. Verify custom RPC appears in MCP tools/list
    let tools_req = nomai_protocol::Request {
        jsonrpc: nomai_protocol::JSONRPC_VERSION.into(),
        id: Some(nomai_protocol::Id::Number(2)),
        method: "tools/list".into(),
        params: None,
    };
    let tools_resp = daemon.dispatch(tools_req).await;
    if let Some(result) = tools_resp.result {
        let tools = result["tools"].as_array().unwrap();
        let has_stats = tools.iter().any(|t| t["name"] == "stats");
        println!("\n'stats' in tools/list: {}", has_stats);
    }

    Ok(())
}
