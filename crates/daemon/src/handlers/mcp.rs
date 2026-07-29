//! MCP (Model Context Protocol) lifecycle handlers.
//!
//! These three handlers make nomai an MCP-compatible server. External MCP
//! clients (e.g. Claude Desktop) connect via stdio and call:
//!
//! - `initialize`     — handshake, returns server capabilities
//! - `tools/list`     — enumerate available tools (built from the handler registry)
//! - `tools/call`     — invoke a tool by name with arguments
//!
//! Because nomai's wire format is already NDJSON JSON-RPC 2.0 (since Phase 0),
//! any MCP client can transparently invoke nomai's existing RPCs as tools.
//!
//! See <https://spec.modelcontextprotocol.io> for the protocol spec.

use async_trait::async_trait;
use serde_json::{Value, json};

use nomai_core::CoreError;

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

/// Protocol version reported to MCP clients during `initialize`.
///
/// Pinned to `"2024-11-05"` — the canonical version string MCP clients
/// expect from servers implementing the 2024 spec.
pub const PROTOCOL_VERSION: &str = "2024-11-05";

/// MCP protocol method names. Also used to filter the tool list returned
/// by `tools/list` (we don't advertise the MCP meta-methods themselves).
pub const INITIALIZE: &str = "initialize";
pub const TOOLS_LIST: &str = "tools/list";
pub const TOOLS_CALL: &str = "tools/call";

/// `initialize` — MCP handshake.
///
/// Returns the protocol version, advertised capabilities, and server info.
pub struct Initialize;

#[async_trait]
impl RpcHandler for Initialize {
    fn method(&self) -> &'static str {
        INITIALIZE
    }

    async fn call(&self, _daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": "nomai",
                "version": env!("CARGO_PKG_VERSION"),
            }
        }))
    }
}

/// `tools/list` — enumerate available MCP tools.
///
/// Each non-MCP handler registered in `daemon.handlers` is surfaced as a
/// tool. The descriptor's `description` and `inputSchema` are sourced from
/// the handler's `description()` and `input_schema()` trait methods
/// (default `""` and `None` preserve pre-spec behavior for plugins).
pub struct ToolsList;

#[async_trait]
impl RpcHandler for ToolsList {
    fn method(&self) -> &'static str {
        TOOLS_LIST
    }

    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        let mut tools: Vec<Value> = Vec::new();
        for &name in daemon.handlers.keys() {
            if is_mcp_method(name) {
                continue;
            }
            // Look up the handler to fetch its description + input_schema.
            // We just verified `name` is non-MCP, so the entry exists.
            let handler = &daemon.handlers[name];
            tools.push(tool_descriptor(
                name,
                handler.description(),
                handler.input_schema(),
            ));
        }
        // Stable ordering: sort alphabetically by tool name so list output
        // is deterministic across runs (helps testing + diffs).
        tools.sort_by(|a, b| {
            a["name"]
                .as_str()
                .unwrap_or("")
                .cmp(b["name"].as_str().unwrap_or(""))
        });
        Ok(json!({ "tools": tools }))
    }
}

/// `tools/call` — dispatch to a registered handler by name.
///
/// Params: `{"name": "<method>", "arguments": {...}}`. The underlying
/// handler's result Value is wrapped as MCP text content:
///
/// ```jsonc
/// {
///   "content": [{ "type": "text", "text": "<JSON of underlying result>" }]
/// }
/// ```
///
/// If the requested tool name is unknown, returns a `CoreError::Validation`
/// (which surfaces as a JSON-RPC error with code 1003 at the daemon layer).
pub struct ToolsCall;

#[async_trait]
impl RpcHandler for ToolsCall {
    fn method(&self) -> &'static str {
        TOOLS_CALL
    }

    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(serde::Deserialize)]
        struct Params {
            name: String,
            #[serde(default)]
            arguments: Value,
        }

        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

        let handler = daemon
            .handlers
            .get(p.name.as_str())
            .ok_or_else(|| CoreError::Validation(format!("unknown tool: {}", p.name)))?;

        // MCP handlers are not themselves callable as tools.
        if is_mcp_method(p.name.as_str()) {
            return Err(CoreError::Validation(format!("not a tool: {}", p.name)));
        }

        // Chokepoint: `mcp.tools/call` routes to a nested handler via
        // `handler.call(...)` DIRECTLY, bypassing `Daemon::dispatch` (which
        // would otherwise hold `sync_lock` for mutating calls). To keep the
        // lock invariant uniform across both JSON-RPC entry points, acquire
        // `sync_lock` here when the resolved nested handler is mutating.
        // Without this, an MCP client could race `sync.run`'s rebase via a
        // `tools/call { name: "entry.create" }` while the dispatcher-bound
        // `entry.create` path is correctly serialized.
        let _lock = if handler.is_mutating() {
            Some(daemon.sync_lock.lock().await)
        } else {
            None
        };

        let result = handler.call(daemon, p.arguments).await?;
        let text = serde_json::to_string(&result)
            .map_err(|e| CoreError::Config(format!("serialize tool result: {e}")))?;

        Ok(json!({
            "content": [{ "type": "text", "text": text }]
        }))
    }
}

/// True if `name` is one of the MCP meta-methods (not a callable tool).
fn is_mcp_method(name: &str) -> bool {
    matches!(name, INITIALIZE | TOOLS_LIST | TOOLS_CALL)
}

/// Build a single MCP tool descriptor.
///
/// - `desc` empty → omit the `description` field entirely (clients fall
///   back to the method name; matches pre-spec behavior).
/// - `schema` None → emit `{"type": "object"}` (current behavior).
/// - `schema` Some(v) → emit `v`.
fn tool_descriptor(name: &str, desc: &str, schema: Option<Value>) -> Value {
    let input_schema = schema.unwrap_or_else(|| json!({"type": "object"}));
    let mut d = json!({
        "name": name,
        "inputSchema": input_schema,
    });
    if !desc.is_empty() {
        d["description"] = json!(desc);
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn protocol_version_is_2024_spec() {
        assert_eq!(PROTOCOL_VERSION, "2024-11-05");
    }

    #[test]
    fn is_mcp_method_recognizes_all_three() {
        assert!(is_mcp_method("initialize"));
        assert!(is_mcp_method("tools/list"));
        assert!(is_mcp_method("tools/call"));
        assert!(!is_mcp_method("entry.create"));
        assert!(!is_mcp_method(""));
    }

    #[test]
    fn tool_descriptor_omits_description_when_empty() {
        let d = tool_descriptor("entry.create", "", None);
        assert_eq!(d["name"], "entry.create");
        assert_eq!(d["inputSchema"]["type"], "object");
        assert!(d.get("description").is_none());
    }

    #[test]
    fn tool_descriptor_includes_description_when_nonempty() {
        let d = tool_descriptor(
            "entry.create",
            "Create a new entry.",
            Some(json!({"type": "object", "properties": {}})),
        );
        assert_eq!(d["description"], "Create a new entry.");
        assert_eq!(d["inputSchema"]["properties"], json!({}));
    }

    #[test]
    fn tool_descriptor_falls_back_to_object_schema_when_none() {
        let d = tool_descriptor("noop", "does nothing", None);
        assert_eq!(d["inputSchema"]["type"], "object");
    }

    #[tokio::test]
    async fn initialize_returns_expected_shape() {
        let daemon = build_test_daemon().await;
        let resp = Initialize
            .call(&daemon, json!({}))
            .await
            .expect("initialize never errors");
        assert_eq!(resp["protocolVersion"], "2024-11-05");
        assert!(resp["capabilities"]["tools"].is_object());
        assert_eq!(resp["serverInfo"]["name"], "nomai");
        // version comes from CARGO_PKG_VERSION; just sanity-check it's a non-empty string.
        assert!(!resp["serverInfo"]["version"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn tools_list_excludes_mcp_meta_methods() {
        let daemon = build_test_daemon().await;
        let resp = ToolsList
            .call(&daemon, json!({}))
            .await
            .expect("tools/list never errors");
        let tools = resp["tools"].as_array().expect("tools is array");
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(!names.contains(&"initialize"));
        assert!(!names.contains(&"tools/list"));
        assert!(!names.contains(&"tools/call"));
        // And core RPCs should be present.
        assert!(names.contains(&"entry.create"));
        assert!(names.contains(&"provider.list"));
    }

    #[tokio::test]
    async fn tools_call_unknown_tool_returns_validation_error() {
        let daemon = build_test_daemon().await;
        let err = ToolsCall
            .call(&daemon, json!({"name": "nope.noop", "arguments": {}}))
            .await
            .expect_err("unknown tool must error");
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[tokio::test]
    async fn tools_call_rejects_mcp_meta_method_as_tool() {
        let daemon = build_test_daemon().await;
        let err = ToolsCall
            .call(&daemon, json!({"name": "initialize", "arguments": {}}))
            .await
            .expect_err("MCP meta-methods are not tools");
        assert!(matches!(err, CoreError::Validation(_)));
    }

    /// Build a Daemon populated with the default handler registry so the
    /// MCP handlers can be exercised without standing up the full storage
    /// stack. `EntryService::for_test` loads the sqlite-vec extension
    /// globally so `vec0` virtual tables work; providers/embedder are not
    /// called by MCP-only paths.
    async fn build_test_daemon() -> Daemon {
        use std::sync::Arc;

        let entries = Arc::new(nomai_core::EntryService::for_test().unwrap());
        // Entry-level embeddings retired; only chunk-level vec0 table
        // remains. Daemon::for_test creates the ChunkService; ensure the
        // virtual table exists before any test path touches it.

        struct NullEmbed;
        #[async_trait::async_trait]
        impl nomai_providers::EmbeddingProvider for NullEmbed {
            async fn embed(
                &self,
                _texts: &[&str],
            ) -> Result<Vec<Vec<f32>>, nomai_protocol::ProviderError> {
                Ok(vec![])
            }
            fn dim(&self) -> usize {
                8
            }
            fn name(&self) -> &str {
                "null-embed"
            }
        }
        struct NullLlm;
        #[async_trait::async_trait]
        impl nomai_providers::LlmProvider for NullLlm {
            async fn complete(
                &self,
                _req: nomai_providers::CompletionRequest,
            ) -> Result<nomai_providers::CompletionResponse, nomai_protocol::ProviderError>
            {
                Err(nomai_protocol::ProviderError::new(
                    nomai_protocol::ProviderErrorKind::Unknown,
                    "null llm",
                    None,
                ))
            }
            fn name(&self) -> &str {
                "null-llm"
            }
        }

        let daemon = Daemon::for_test(
            entries,
            Arc::new(NullEmbed),
            Arc::new(NullLlm),
            "test-embed".into(),
            "test-llm".into(),
            8,
            1024,
        );
        daemon.chunks.ensure_vec_chunk_embeddings(8).unwrap();
        daemon
    }
}
