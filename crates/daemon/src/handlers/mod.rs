//! JSON-RPC method dispatch registry.
//!
//! Each method is a zero-sized struct implementing `RpcHandler`. The daemon
//! looks up handlers by method name in a `HashMap` populated by `registry()`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::rpc::RpcHandler;

pub mod batch;
pub mod block;
pub mod cache;
pub mod chunk;
pub mod entry;
pub mod events;
pub mod index;
pub mod link;
pub mod mcp;
pub mod provider;
pub mod search;
pub mod system;

/// Build the default method → handler registry for the daemon.
///
/// Returns the full set of built-in JSON-RPC handlers. Plugins may add
/// more via `Daemon::register_handler`.
pub fn registry() -> HashMap<&'static str, Arc<dyn RpcHandler>> {
    let mut m: HashMap<&'static str, Arc<dyn RpcHandler>> = HashMap::new();

    // entry.*
    let h = entry::Create;
    m.insert(h.method(), Arc::new(h));
    let h = entry::Get;
    m.insert(h.method(), Arc::new(h));
    let h = entry::Update;
    m.insert(h.method(), Arc::new(h));
    let h = entry::Delete;
    m.insert(h.method(), Arc::new(h));
    let h = entry::List;
    m.insert(h.method(), Arc::new(h));

    // link.*
    let h = link::Create;
    m.insert(h.method(), Arc::new(h));
    let h = link::Get;
    m.insert(h.method(), Arc::new(h));
    let h = link::Delete;
    m.insert(h.method(), Arc::new(h));
    let h = link::List;
    m.insert(h.method(), Arc::new(h));
    let h = link::Neighbors;
    m.insert(h.method(), Arc::new(h));

    // chunk.* (Plan 4: only Get + List; Create/Update/Delete removed because
    // chunks are auto-derived from blocks.)
    let h = chunk::Get;
    m.insert(h.method(), Arc::new(h));
    let h = chunk::List;
    m.insert(h.method(), Arc::new(h));

    // block.* (Plan 5: block-level RPCs on top of Plan 3 blocks storage.)
    let h = block::Append;
    m.insert(h.method(), Arc::new(h));
    let h = block::Update;
    m.insert(h.method(), Arc::new(h));
    let h = block::Delete;
    m.insert(h.method(), Arc::new(h));

    // index.* (Plan 5: FS↔SQLite reconciliation. Plan 6: read-only verify.)
    let h = index::Sync;
    m.insert(h.method(), Arc::new(h));
    let h = index::Rebuild;
    m.insert(h.method(), Arc::new(h));
    let h = index::Verify;
    m.insert(h.method(), Arc::new(h));

    // system.* (Plan 6: Spec §12 migration utilities.)
    let h = system::ExportToFs;
    m.insert(h.method(), Arc::new(h));

    // events.*
    let h = events::List;
    m.insert(h.method(), Arc::new(h));
    let h = events::Get;
    m.insert(h.method(), Arc::new(h));
    let h = events::Purge;
    m.insert(h.method(), Arc::new(h));

    // search.*
    let h = search::Fulltext;
    m.insert(h.method(), Arc::new(h));
    let h = search::Semantic;
    m.insert(h.method(), Arc::new(h));

    // provider.*
    let h = provider::List;
    m.insert(h.method(), Arc::new(h));

    // cache.* (embedding cache introspection + management)
    let h = cache::Stats;
    m.insert(h.method(), Arc::new(h));
    let h = cache::Clear;
    m.insert(h.method(), Arc::new(h));

    // mcp.* (lifecycle: initialize / tools/list / tools/call)
    let h = mcp::Initialize;
    m.insert(h.method(), Arc::new(h));
    let h = mcp::ToolsList;
    m.insert(h.method(), Arc::new(h));
    let h = mcp::ToolsCall;
    m.insert(h.method(), Arc::new(h));

    // batch (multi-op atomic transaction)
    let h = batch::Batch;
    m.insert(h.method(), Arc::new(h));

    m
}

#[cfg(test)]
mod tests {
    use crate::daemon::Daemon;
    use nomai_core::EntryService;
    use nomai_protocol::{Id, JSONRPC_VERSION, Request};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const DIM: usize = 1536;

    async fn setup_daemon(server: &MockServer) -> Daemon {
        let entries = Arc::new(EntryService::for_test().unwrap());
        // Plan 4: entry-level embeddings retired; only chunk-level vec0
        // table remains, created via ensure_vec_chunk_embeddings below.
        let embedder: Arc<dyn nomai_providers::EmbeddingProvider> =
            Arc::new(nomai_providers::OpenAiCompatibleEmbed::new(
                server.uri(),
                "test-key",
                "test-model",
                DIM,
            ));
        let llm: Arc<dyn nomai_providers::LlmProvider> = Arc::new(
            nomai_providers::OpenAiCompatibleLlm::new(server.uri(), "test-key", "test-model"),
        );
        let daemon = Daemon::for_test(
            entries,
            embedder,
            llm,
            "test-model".into(),
            "test-model".into(),
            DIM,
            1024,
        );
        // Ensure vec_chunk_embeddings table exists for chunk semantic search
        // (Daemon::for_test does not auto-create it; the production constructor
        // does this via config.embedding.dim).
        daemon.chunks.ensure_vec_chunk_embeddings(DIM).unwrap();
        daemon
    }

    fn req(method: &str, params: Value) -> Request {
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: Some(Id::Number(1)),
            method: method.into(),
            params: Some(params),
        }
    }

    /// Build a 1536-dim embedding (V9 daemon default) from a short prefix,
    /// zero-padding the rest. Used by similarity tests that want unit vectors
    /// along specific axes.
    fn vec_1536(prefix: &[f32]) -> Vec<f32> {
        let mut v = vec![0.0_f32; DIM];
        for (i, x) in prefix.iter().enumerate() {
            v[i] = *x;
        }
        v
    }

    #[tokio::test]
    async fn entry_create_round_trips_via_get() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec![0.0_f32; DIM]}]
            })))
            .mount(&server)
            .await;

        let daemon = setup_daemon(&server).await;
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({
                    "title": "Note",
                    "blocks":[{"type":"note","text":"Hello world"}],
                }),
            ))
            .await;
        assert!(
            create_resp.error.is_none(),
            "create failed: {:?}",
            create_resp.error
        );
        let entry: Value = create_resp.result.unwrap();
        let id = entry["id"].as_str().unwrap().to_string();

        let get_resp = daemon.dispatch(req("entry.get", json!({ "id": id }))).await;
        assert!(get_resp.error.is_none());
        assert_eq!(get_resp.result.unwrap()["title"], "Note");
    }

    #[tokio::test]
    async fn entry_create_does_not_trigger_embedding_call() {
        // Plan 4: entry.create no longer triggers entry-level embedding work.
        // A separate background chunk embedder (Plan 5) will handle chunks.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec![0.0_f32; DIM]}]
            })))
            .expect(0) // ← zero embedding calls expected
            .mount(&server)
            .await;

        let daemon = setup_daemon(&server).await;
        let _ = daemon
            .dispatch(req(
                "entry.create",
                json!({
                    "title": "X",
                    "blocks":[{"type":"note","text":"Y"}],
                }),
            ))
            .await;
        // Mock's expect(0) verifies on drop that no embedding call was made.
    }

    #[tokio::test]
    async fn search_semantic_ranks_by_similarity() {
        let server = MockServer::start().await;

        // Seed two entries with known chunk embeddings, then answer the query
        // embedding deterministically. Plan 4: semantic search runs over
        // chunk embeddings (auto-derived from block text).
        let daemon = setup_daemon(&server).await;

        // Create entries — the mock returns a zero vector each time; we then
        // overwrite the chunk embedding directly via ChunkService for
        // deterministic ranking.
        let entries = daemon.entries.clone();
        let a = entries
            .create(nomai_core::CreateEntry {
                title: "a".into(),
                blocks: vec![nomai_core::BlockInput {
                    r#type: "note".into(),
                    text: "near query".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let b = entries
            .create(nomai_core::CreateEntry {
                title: "b".into(),
                blocks: vec![nomai_core::BlockInput {
                    r#type: "note".into(),
                    text: "far".into(),
                    attrs: None,
                }],
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();

        // Each block auto-derives one chunk (text < 1024 chars). Find it
        // via ChunkService::list and overwrite its embedding.
        let chunks = daemon.chunks.clone();
        let a_block_id = a.blocks[0].id;
        let b_block_id = b.blocks[0].id;
        let a_chunk_id = chunks.list(a_block_id).unwrap().items[0].id;
        let b_chunk_id = chunks.list(b_block_id).unwrap().items[0].id;
        chunks
            .write_embedding(a_chunk_id, &vec_1536(&[1.0]))
            .unwrap();
        chunks
            .write_embedding(
                b_chunk_id,
                &vec_1536(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]),
            )
            .unwrap();

        // search.semantic will issue an embedding request for the query.
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec_1536(&[1.0])}]
            })))
            .mount(&server)
            .await;

        let resp = daemon
            .dispatch(req(
                "search.semantic",
                json!({
                    "query": "anything",
                    "limit": 10,
                }),
            ))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let items = resp.result.unwrap()["items"].as_array().unwrap().clone();
        assert_eq!(items.len(), 2);
        // Chunk "a" (cos=1.0) ranks above chunk "b" (cos=0.0). Resolve
        // entry title via JOIN for the assertion.
        let top_chunk_block_id = items[0]["chunk"]["block_id"].as_str().unwrap();
        let top_title: String = {
            let conn = daemon.entries.conn_for_test();
            conn.lock().unwrap().query_row(
                "SELECT e.title FROM blocks b JOIN entries e ON e.id = b.entry_id WHERE b.id = ?1",
                rusqlite::params![top_chunk_block_id],
                |row| row.get::<_, String>(0),
            ).unwrap()
        };
        assert_eq!(top_title, "a");
    }

    #[tokio::test]
    async fn search_hybrid_returns_method_not_found() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;
        let resp = daemon.dispatch(req("search.hybrid", json!({}))).await;
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32601);
    }

    #[tokio::test]
    async fn unknown_method_returns_method_not_found() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;
        let resp = daemon.dispatch(req("nope.noop", json!({}))).await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32601);
        assert_eq!(err.data.unwrap()["method"], "nope.noop");
    }

    #[tokio::test]
    async fn provider_list_returns_active_provider_info() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;
        let resp = daemon.dispatch(req("provider.list", json!({}))).await;
        let result = resp.result.unwrap();
        assert_eq!(result["embedding"]["name"], "openai-compatible");
        assert_eq!(result["embedding"]["model"], "test-model");
        assert_eq!(result["llm"]["model"], "test-model");
    }

    // ----- link.* e2e tests (Plan 3 Task 3) -----

    async fn mount_embedding_mock(server: &MockServer) {
        // Non-zero embeddings: cosine similarity in sqlite-vec treats the
        // zero vector as invalid; tests that rely on semantic search need
        // non-zero vectors to get any hits.
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec![1.0_f32; DIM]}]
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn link_create_round_trips_via_get() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Seed two entries (embedding mock mounted by mount_embedding_mock above).
        let a_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"a","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        let a_id = a_resp.result.unwrap()["id"].as_str().unwrap().to_string();
        let b_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"b","blocks":[{"type":"note","text":"y"}]}),
            ))
            .await;
        let b_id = b_resp.result.unwrap()["id"].as_str().unwrap().to_string();

        let create_resp = daemon
            .dispatch(req(
                "link.create",
                json!({
                    "source_id": a_id,
                    "target_id": b_id,
                    "relation": "references",
                }),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);
        let link = create_resp.result.unwrap();
        let link_id = link["id"].as_str().unwrap().to_string();

        let get_resp = daemon
            .dispatch(req("link.get", json!({"id": link_id})))
            .await;
        assert!(get_resp.error.is_none(), "{:?}", get_resp.error);
        assert_eq!(get_resp.result.unwrap()["relation"], "references");
    }

    #[tokio::test]
    async fn link_create_returns_validation_for_missing_entry() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let b_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"b","blocks":[{"type":"note","text":"y"}]}),
            ))
            .await;
        let b_id = b_resp.result.unwrap()["id"].as_str().unwrap().to_string();
        let phantom = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

        let resp = daemon
            .dispatch(req(
                "link.create",
                json!({
                    "source_id": phantom,
                    "target_id": b_id,
                    "relation": "r",
                }),
            ))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003); // Validation per spec §5
    }

    #[tokio::test]
    async fn link_list_returns_outgoing_links() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let a = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"a","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        let a_id = a.result.unwrap()["id"].as_str().unwrap().to_string();
        let b = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"b","blocks":[{"type":"note","text":"y"}]}),
            ))
            .await;
        let b_id = b.result.unwrap()["id"].as_str().unwrap().to_string();
        let c = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"c","blocks":[{"type":"note","text":"z"}]}),
            ))
            .await;
        let c_id = c.result.unwrap()["id"].as_str().unwrap().to_string();

        daemon
            .dispatch(req(
                "link.create",
                json!({"source_id": a_id.clone(), "target_id": b_id, "relation": "r"}),
            ))
            .await;
        daemon
            .dispatch(req(
                "link.create",
                json!({"source_id": a_id.clone(), "target_id": c_id, "relation": "r"}),
            ))
            .await;

        let resp = daemon
            .dispatch(req("link.list", json!({"from": a_id, "limit": 50})))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["total"], 2);
        assert_eq!(result["items"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn link_neighbors_returns_entries_and_links() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let a = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"a","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        let a_id = a.result.unwrap()["id"].as_str().unwrap().to_string();
        let b = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"b","blocks":[{"type":"note","text":"y"}]}),
            ))
            .await;
        let b_id = b.result.unwrap()["id"].as_str().unwrap().to_string();

        daemon
            .dispatch(req(
                "link.create",
                json!({"source_id": a_id.clone(), "target_id": b_id, "relation": "r"}),
            ))
            .await;

        let resp = daemon
            .dispatch(req(
                "link.neighbors",
                json!({"id": a_id, "direction": "out"}),
            ))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["entries"].as_array().unwrap().len(), 1);
        assert_eq!(result["entries"][0]["title"], "b");
        assert_eq!(result["links"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn link_delete_round_trip() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let a = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"a","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        let a_id = a.result.unwrap()["id"].as_str().unwrap().to_string();
        let b = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"b","blocks":[{"type":"note","text":"y"}]}),
            ))
            .await;
        let b_id = b.result.unwrap()["id"].as_str().unwrap().to_string();

        let create = daemon
            .dispatch(req(
                "link.create",
                json!({"source_id": a_id, "target_id": b_id, "relation": "r"}),
            ))
            .await;
        let link_id = create.result.unwrap()["id"].as_str().unwrap().to_string();

        let del = daemon
            .dispatch(req("link.delete", json!({"id": link_id.clone()})))
            .await;
        assert!(del.error.is_none());
        assert_eq!(del.result.unwrap()["deleted"], true);

        let get = daemon
            .dispatch(req("link.get", json!({"id": link_id})))
            .await;
        assert_eq!(get.error.unwrap().code, 1001); // NotFound
    }

    #[tokio::test]
    async fn link_traverse_returns_method_not_found() {
        // Phase 2 deferred per spec §5.
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;
        let resp = daemon
            .dispatch(req("link.traverse", json!({"root":"x","max_depth":2})))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32601);
        assert_eq!(err.data.unwrap()["method"], "link.traverse");
    }

    #[tokio::test]
    async fn link_list_returns_validation_when_neither_from_nor_to() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;
        let resp = daemon.dispatch(req("link.list", json!({}))).await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003);
    }

    // ----- events.* e2e tests (Plan 2 Task 3) -----

    #[tokio::test]
    async fn events_list_returns_entry_created_after_create() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"Note","blocks":[{"type":"note","text":"Hello"}]}),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);

        // entry.create also emits block.created (one per block). Filter to
        // entry.created so this test stays focused on the entry event.
        let list_resp = daemon
            .dispatch(req("events.list", json!({"type": "entry.created"})))
            .await;
        assert!(list_resp.error.is_none(), "{:?}", list_resp.error);
        let result = list_resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["type"], "entry.created");
        assert_eq!(items[0]["target_type"], "entry");
        assert_eq!(items[0]["payload"]["title"], "Note");
    }

    #[tokio::test]
    async fn events_list_filters_by_type() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create + update → emits entry.created + entry.updated
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"orig","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        let id = create_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        daemon
            .dispatch(req("entry.update", json!({"id": id, "title": "new"})))
            .await;

        let list_resp = daemon
            .dispatch(req("events.list", json!({"type": "entry.updated"})))
            .await;
        let result = list_resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["type"], "entry.updated");
        assert_eq!(items[0]["payload"]["title"], "new");
    }

    #[tokio::test]
    async fn events_get_returns_event_by_id() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let _create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"X","blocks":[{"type":"note","text":"y"}]}),
            ))
            .await;
        // Get the entry.created event id (filter by type so block.created
        // — also emitted by entry.create — doesn't get picked up first).
        let list_resp = daemon
            .dispatch(req("events.list", json!({"type": "entry.created"})))
            .await;
        let event_id = list_resp.result.unwrap()["items"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();

        let get_resp = daemon
            .dispatch(req("events.get", json!({"id": event_id})))
            .await;
        assert!(get_resp.error.is_none());
        assert_eq!(get_resp.result.unwrap()["type"], "entry.created");
    }

    #[tokio::test]
    async fn events_get_returns_not_found_for_unknown_id() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let phantom = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let resp = daemon
            .dispatch(req("events.get", json!({"id": phantom})))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1001);
    }

    #[tokio::test]
    async fn events_purge_deletes_old_events() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create 3 entries → 3 entry.created events total. After P3-M7,
        // entry.create suppresses block.created emission (the entry.created
        // payload already embeds the full blocks vector). Plan 6 Task 5:
        // daemon startup emits `index.synced` only when the boot scan
        // changes something; setup_daemon's empty FS means no boot event,
        // so the total is 3 before any entry.create call.
        daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"a","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"b","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        let _last_create = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"c","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;

        // Get all events; the last event_id is the boundary.
        let list_resp = daemon.dispatch(req("events.list", json!({}))).await;
        let result = list_resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        let last_event_id = items[2]["id"].as_str().unwrap().to_string();

        // Purge events with id < last_event_id (exclusive).
        let purge_resp = daemon
            .dispatch(req("events.purge", json!({"before": last_event_id})))
            .await;
        assert_eq!(purge_resp.result.unwrap()["deleted"], 2);

        // Verify only 1 event remains.
        let list_resp2 = daemon.dispatch(req("events.list", json!({}))).await;
        let result2 = list_resp2.result.unwrap();
        let items2 = result2["items"].as_array().unwrap();
        assert_eq!(items2.len(), 1);
    }

    #[tokio::test]
    async fn events_list_paginates_with_since_cursor() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create 3 entries → 3 entry events total. After P3-M7, entry.create
        // suppresses block.created emission. Plan 6 Task 5: daemon startup
        // emits `index.synced` only when the boot scan changes something;
        // setup_daemon's empty FS means no boot event, so total = 3.
        for i in 0..3 {
            daemon
                .dispatch(req(
                    "entry.create",
                    json!({"title": format!("e{i}"), "blocks":[{"type":"note","text":"x"}]}),
                ))
                .await;
        }

        // Page 1: limit=2
        let p1 = daemon
            .dispatch(req("events.list", json!({"limit": 2})))
            .await;
        let p1_result = p1.result.unwrap();
        assert_eq!(p1_result["items"].as_array().unwrap().len(), 2);
        assert_eq!(p1_result["has_more"], true);
        let last_id = p1_result["items"][1]["id"].as_str().unwrap().to_string();

        // Page 2: since = last_id from page 1 → 1 remaining event.
        let p2 = daemon
            .dispatch(req("events.list", json!({"limit": 2, "since": last_id})))
            .await;
        let p2_result = p2.result.unwrap();
        assert_eq!(p2_result["items"].as_array().unwrap().len(), 1);
        assert_eq!(p2_result["has_more"], false);
    }

    #[tokio::test]
    async fn link_create_emits_link_created_event() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let a = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"a","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        let a_id = a.result.unwrap()["id"].as_str().unwrap().to_string();
        let b = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"b","blocks":[{"type":"note","text":"y"}]}),
            ))
            .await;
        let b_id = b.result.unwrap()["id"].as_str().unwrap().to_string();

        daemon
            .dispatch(req(
                "link.create",
                json!({"source_id": a_id, "target_id": b_id, "relation": "r"}),
            ))
            .await;

        let list_resp = daemon
            .dispatch(req("events.list", json!({"type": "link.created"})))
            .await;
        let result = list_resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["target_type"], "link");
        assert_eq!(items[0]["payload"]["relation"], "r");
    }

    // ----- chunk.* + search.semantic + entry.delete cleanup e2e (Plan 4) -----
    //
    // Plan 4 changes:
    //   - chunk.create / chunk.delete RPCs removed (return -32601); chunks
    //     are auto-derived from block text via BlockService::create_in_tx.
    //   - chunk.list takes `block_id` (was `entry_id`); chunk.get unchanged.
    //   - search.semantic no longer accepts `granularity`; it always searches
    //     chunk embeddings (entry-level embeddings retired).
    //   - chunk.created events are gone (no chunk.create RPC).

    #[tokio::test]
    async fn chunk_create_returns_method_not_found() {
        // Plan 4: chunk.create is gone — chunks are auto-derived from blocks.
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "chunk.create",
                json!({"entry_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "ordinal": 0, "text": "x"}),
            ))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32601);
    }

    #[tokio::test]
    async fn chunk_delete_returns_method_not_found() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "chunk.delete",
                json!({"id": "01ARZ3NDEKTSV4RRFFQ69G5FAV"}),
            ))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32601);
    }

    #[tokio::test]
    async fn chunk_list_returns_auto_derived_chunks_for_block() {
        // Plan 4: chunks are auto-derived from block text on entry.create.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"d","blocks":[{"type":"note","text":"hello world"}]}),
            ))
            .await;
        let entry_json = entry_resp.result.unwrap();
        let block_id = entry_json["blocks"][0]["id"].as_str().unwrap().to_string();

        let list_resp = daemon
            .dispatch(req("chunk.list", json!({"block_id": block_id})))
            .await;
        assert!(list_resp.error.is_none(), "{:?}", list_resp.error);
        let result = list_resp.result.unwrap();
        // short text → exactly one chunk
        assert_eq!(result["total"], 1);
        let items = result["items"].as_array().unwrap();
        assert_eq!(items[0]["text"], "hello world");
        assert_eq!(items[0]["block_id"], block_id);
        assert_eq!(items[0]["attrs"]["parent_block_type"], "note");
    }

    #[tokio::test]
    async fn chunk_get_retrieves_auto_derived_chunk() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"d","blocks":[{"type":"note","text":"body"}]}),
            ))
            .await;
        let entry_json = entry_resp.result.unwrap();
        let block_id = entry_json["blocks"][0]["id"].as_str().unwrap().to_string();

        // Resolve chunk id via chunk.list, then chunk.get.
        let list = daemon
            .dispatch(req("chunk.list", json!({"block_id": block_id})))
            .await;
        let chunk_id = list.result.unwrap()["items"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();

        let get_resp = daemon
            .dispatch(req("chunk.get", json!({"id": chunk_id})))
            .await;
        assert!(get_resp.error.is_none());
        assert_eq!(get_resp.result.unwrap()["text"], "body");
    }

    #[tokio::test]
    async fn search_semantic_returns_chunks() {
        // Plan 4: search.semantic always returns chunks (entry-level retired).
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"d","blocks":[{"type":"note","text":"chunk content"}]}),
            ))
            .await;
        let entry_json = entry_resp.result.unwrap();
        let block_id = entry_json["blocks"][0]["id"].as_str().unwrap().to_string();

        // Write a chunk embedding so semantic search has a hit (Plan 4: no
        // background embedder yet; tests must populate embeddings directly).
        let chunks = daemon.chunks.clone();
        let chunk_id = chunks
            .list(ulid::Ulid::from_string(&block_id).unwrap())
            .unwrap()
            .items[0]
            .id;
        chunks.write_embedding(chunk_id, &[1.0_f32; DIM]).unwrap();

        let resp = daemon
            .dispatch(req(
                "search.semantic",
                json!({"query":"anything","limit":10}),
            ))
            .await;
        let result = resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert!(!items.is_empty());
        assert!(items.iter().all(|i| i["chunk"].is_object()));
        assert_eq!(items[0]["chunk"]["block_id"], block_id);
    }

    #[tokio::test]
    async fn entry_delete_cleans_chunk_embeddings_via_trigger() {
        // Plan 5 Task 4: the V9 chunks_ad trigger now cleans
        // vec_chunk_embeddings when chunks are CASCADE-deleted; the entry.delete
        // handler no longer walks chunks and calls delete_embedding.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create entry with 2 blocks → 2 auto-derived chunks.
        let entry_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"d","blocks":[
                    {"type":"note","text":"first block"},
                    {"type":"note","text":"second block"}
                ]}),
            ))
            .await;
        let entry_json = entry_resp.result.unwrap();
        let entry_id = entry_json["id"].as_str().unwrap().to_string();

        // Write chunk embeddings so semantic search finds them.
        let chunks = daemon.chunks.clone();
        for block in entry_json["blocks"].as_array().unwrap() {
            let block_id: ulid::Ulid = block["id"].as_str().unwrap().parse().unwrap();
            for chunk in chunks.list(block_id).unwrap().items {
                chunks.write_embedding(chunk.id, &[1.0_f32; DIM]).unwrap();
            }
        }

        // Precondition: chunk search finds 2; vec_chunk_embeddings has 2 rows.
        let pre = daemon
            .dispatch(req("search.semantic", json!({"query":"x","limit":10})))
            .await;
        assert_eq!(pre.result.unwrap()["items"].as_array().unwrap().len(), 2);
        {
            let conn = daemon.entries.conn_for_test();
            let n: i64 = conn
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM vec_chunk_embeddings", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(n, 2);
        }

        // Delete the entry — CASCADE removes blocks → chunks; chunks_ad trigger
        // cleans vec_chunk_embeddings.
        let del_resp = daemon
            .dispatch(req("entry.delete", json!({"id": entry_id})))
            .await;
        // F-entry-1: entry.delete ack carries the id (mirrors block.delete).
        let del_result = del_resp.result.unwrap();
        assert_eq!(del_result["deleted"], true);
        assert_eq!(del_result["id"], entry_id);

        // After: chunk search returns 0 because the trigger cleaned up
        // vec_chunk_embeddings (no handler-side walk).
        let post = daemon
            .dispatch(req("search.semantic", json!({"query":"x","limit":10})))
            .await;
        assert_eq!(post.result.unwrap()["items"].as_array().unwrap().len(), 0);
        {
            let conn = daemon.entries.conn_for_test();
            let n: i64 = conn
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM vec_chunk_embeddings", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(n, 0, "chunks_ad trigger should have cleaned embeddings");
        }

        // entry.list should not return the deleted entry.
        let list = daemon
            .dispatch(req("entry.list", json!({"limit":100})))
            .await;
        let result = list.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert!(items.iter().all(|e| e["id"].as_str().unwrap() != entry_id));
    }

    #[tokio::test]
    async fn search_semantic_with_block_type_filter() {
        // Plan 4: block_type filter is the successor to the old granularity.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Two entries: one note, one claim.
        let note_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"n","blocks":[{"type":"note","text":"note body"}]}),
            ))
            .await;
        let claim_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"c","blocks":[{"type":"claim","text":"claim body"}]}),
            ))
            .await;

        // Write embeddings for both chunks.
        let chunks = daemon.chunks.clone();
        for resp in [note_resp, claim_resp] {
            let block_id: ulid::Ulid = resp.result.unwrap()["blocks"][0]["id"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            for chunk in chunks.list(block_id).unwrap().items {
                chunks.write_embedding(chunk.id, &[1.0_f32; DIM]).unwrap();
            }
        }

        // Filter to claims only.
        let resp = daemon
            .dispatch(req(
                "search.semantic",
                json!({"query":"anything","limit":10,"block_type":"claim"}),
            ))
            .await;
        let items = resp.result.unwrap()["items"].as_array().unwrap().clone();
        assert_eq!(items.len(), 1);
        // All hits should be from a claim block.
        assert_eq!(items[0]["chunk"]["attrs"]["parent_block_type"], "claim");
    }

    // ----- MCP lifecycle + plugin registration e2e (Plan KS1-T5) -----

    #[tokio::test]
    async fn mcp_initialize_returns_capabilities() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon.dispatch(req("initialize", json!({}))).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["protocolVersion"], "2024-11-05");
        assert!(result["capabilities"]["tools"].is_object());
        assert_eq!(result["serverInfo"]["name"], "nomai");
        // version comes from daemon's CARGO_PKG_VERSION at compile time.
        assert_eq!(
            result["serverInfo"]["version"].as_str().unwrap(),
            env!("CARGO_PKG_VERSION")
        );
    }

    #[tokio::test]
    async fn mcp_tools_list_returns_all_registered_methods() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon.dispatch(req("tools/list", json!({}))).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        let tools = result["tools"].as_array().expect("tools is array");
        // 28 built-in non-MCP handlers (entry:5, link:5, chunk:2, block:3,
        // events:3, search:2, provider:1, cache:2, batch:1, index:3,
        // system:1).
        assert_eq!(tools.len(), 28);
        for tool in tools {
            assert!(tool["name"].is_string());
            assert!(tool["inputSchema"].is_object());
        }
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        // All core RPC categories present.
        assert!(names.contains(&"entry.create"));
        assert!(names.contains(&"link.neighbors"));
        assert!(names.contains(&"events.list"));
        // Plan 4: chunk.create / chunk.delete are gone.
        assert!(!names.contains(&"chunk.create"));
        assert!(!names.contains(&"chunk.delete"));
        assert!(names.contains(&"chunk.get"));
        assert!(names.contains(&"search.semantic"));
        assert!(names.contains(&"provider.list"));
        assert!(names.contains(&"index.sync"));
        assert!(names.contains(&"index.rebuild"));
        assert!(names.contains(&"index.verify"));
        assert!(names.contains(&"system.export_to_fs"));
        // MCP meta-methods are not callable tools.
        assert!(!names.contains(&"initialize"));
        assert!(!names.contains(&"tools/list"));
        assert!(!names.contains(&"tools/call"));
    }

    #[tokio::test]
    async fn mcp_tools_call_dispatches_to_handler() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Seed one entry via the core RPC path.
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"hi","blocks":[{"type":"note","text":"world"}]}),
            ))
            .await;
        assert!(create_resp.error.is_none());
        let created_id = create_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Dispatch via MCP tools/call → entry.list.
        let resp = daemon
            .dispatch(req(
                "tools/call",
                json!({"name": "entry.list", "arguments": {"limit": 10}}),
            ))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        let content = result["content"].as_array().expect("content is array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        let text = content[0]["text"].as_str().expect("text is string");
        let parsed: Value = serde_json::from_str(text).expect("content text is JSON");
        let items = parsed["items"].as_array().expect("items is array");
        let found = items
            .iter()
            .any(|e| e["id"].as_str() == Some(created_id.as_str()));
        assert!(found, "created entry missing from tools/call output");
    }

    #[tokio::test]
    async fn mcp_tools_call_unknown_returns_error() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "tools/call",
                json!({"name": "nonexistent.method", "arguments": {}}),
            ))
            .await;
        let err = resp.error.expect("expected validation error");
        assert_eq!(err.code, 1003); // Validation per spec §5
    }

    #[tokio::test]
    async fn mcp_tools_call_rejects_mcp_meta_method() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        // MCP meta-methods (initialize/tools.list/tools.call) cannot be
        // invoked through tools/call — they are lifecycle, not tools.
        let resp = daemon
            .dispatch(req(
                "tools/call",
                json!({"name": "initialize", "arguments": {}}),
            ))
            .await;
        let err = resp.error.expect("expected validation error");
        assert_eq!(err.code, 1003);
    }

    #[tokio::test]
    async fn daemon_register_handler_adds_custom_rpc() {
        use crate::rpc::RpcHandler;
        use async_trait::async_trait;
        use nomai_core::CoreError;

        struct CustomHandler;
        #[async_trait]
        impl RpcHandler for CustomHandler {
            fn method(&self) -> &'static str {
                "custom.echo"
            }
            async fn call(&self, _: &Daemon, params: Value) -> Result<Value, CoreError> {
                Ok(params)
            }
        }

        let server = MockServer::start().await;
        let mut daemon = setup_daemon(&server).await;

        // register_handler requires &mut self; dispatch takes &self after.
        daemon.register_handler(Arc::new(CustomHandler));

        let resp = daemon
            .dispatch(req("custom.echo", json!({"hello":"world"})))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        assert_eq!(resp.result.unwrap(), json!({"hello":"world"}));

        // Registered handler also surfaces in tools/list.
        let list = daemon.dispatch(req("tools/list", json!({}))).await;
        let tools = list.result.unwrap()["tools"].as_array().unwrap().clone();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"custom.echo"));
        assert_eq!(tools.len(), 29); // 28 built-in + custom.echo
    }

    // ----- batch RPC e2e (Plan 2 Task 3) -----

    #[tokio::test]
    async fn batch_all_success_creates_entries_and_link() {
        // Plan 4: chunk.create is gone from batch; entries auto-derive chunks.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "batch",
                json!({
                    "ops": [
                        {"id": "e1", "method": "entry.create", "params": {"title": "doc", "blocks":[{"type":"note","text":"body text"}]}},
                        {"id": "e2", "method": "entry.create", "params": {"title": "target", "blocks":[{"type":"note","text":"target body"}]}},
                        {"method": "link.create", "params": {
                            "source_id": {"$ref": "e1.id"},
                            "target_id": {"$ref": "e2.id"},
                            "relation": "references"
                        }}
                    ]
                }),
            ))
            .await;

        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["rolled_back"], false);
        let results = result["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r["ok"] == true));

        // Verify $ref resolved correctly
        let entry_id = results[0]["result"]["id"].as_str().unwrap();
        let target_id = results[1]["result"]["id"].as_str().unwrap();
        let link_source_id = results[2]["result"]["source_id"].as_str().unwrap();
        let link_target_id = results[2]["result"]["target_id"].as_str().unwrap();
        assert_eq!(link_source_id, entry_id, "link source_id should match $ref");
        assert_eq!(
            link_target_id, target_id,
            "link target_id should match $ref"
        );

        // Plan 4 bonus: chunks auto-derived for each entry.
        let e1_block: ulid::Ulid = results[0]["result"]["blocks"][0]["id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            daemon.chunks.list(e1_block).unwrap().items.len(),
            1,
            "entry.create should auto-derive one chunk for the block"
        );
    }

    #[tokio::test]
    async fn batch_atomic_rolls_back_on_failure() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "batch",
                json!({
                    "ops": [
                        {"id": "e1", "method": "entry.create", "params": {"title": "will rollback", "blocks":[{"type":"note","text":"x"}]}},
                        // link.create with phantom source → FK violation
                        {"method": "link.create", "params": {
                            "source_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                            "target_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
                            "relation": "references"
                        }}
                    ]
                }),
            ))
            .await;

        // Should fail (op[1] FK violation)
        assert!(resp.error.is_some());
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003);

        // Verify op[0] was rolled back (entry not persisted)
        let list_resp = daemon
            .dispatch(req("entry.list", json!({"limit": 100})))
            .await;
        let list_result = list_resp.result.unwrap();
        let items = list_result["items"].as_array().unwrap();
        assert!(
            items
                .iter()
                .all(|e| e["title"].as_str().unwrap() != "will rollback"),
            "entry should have been rolled back"
        );
    }

    #[tokio::test]
    async fn batch_rejects_read_method() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "batch",
                json!({
                    "ops": [
                        {"method": "entry.list", "params": {"limit": 3}}
                    ]
                }),
            ))
            .await;

        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003);
    }

    #[tokio::test]
    async fn batch_rejects_empty_ops() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon.dispatch(req("batch", json!({"ops": []}))).await;

        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003);
    }

    #[tokio::test]
    async fn batch_rejects_chunk_create() {
        // Plan 4: chunk.create is no longer an allowed batch method.
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "batch",
                json!({
                    "ops": [
                        {"method": "chunk.create", "params": {
                            "entry_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                            "ordinal": 0,
                            "text": "x"
                        }}
                    ]
                }),
            ))
            .await;

        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003);
    }

    #[tokio::test]
    async fn batch_ref_unknown_op_id_fails() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "batch",
                json!({
                    "ops": [
                        // link.create with $ref to nonexistent op id
                        {"method": "link.create", "params": {
                            "source_id": {"$ref": "nonexistent.id"},
                            "target_id": {"$ref": "nonexistent.id"},
                            "relation": "r"
                        }}
                    ]
                }),
            ))
            .await;

        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003);
    }

    #[tokio::test]
    async fn batch_does_not_call_embedder() {
        // Plan 4: entry.create no longer triggers embedding work (Plan 5
        // will add a separate background chunk embedder). batch of N
        // entry.create ops must therefore issue zero embedding calls.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec![1.0_f32; DIM]}]
            })))
            .expect(0) // ← zero embedding calls
            .mount(&server)
            .await;

        let daemon = setup_daemon(&server).await;

        daemon
            .dispatch(req(
                "batch",
                json!({
                    "ops": [
                        {"method": "entry.create", "params": {"title": "a", "blocks":[{"type":"note","text":"text a"}]}},
                        {"method": "entry.create", "params": {"title": "b", "blocks":[{"type":"note","text":"text b"}]}},
                        {"method": "entry.create", "params": {"title": "c", "blocks":[{"type":"note","text":"text c"}]}}
                    ]
                }),
            ))
            .await;

        // Mock's expect(0) verifies on drop that no embedding call was made.
    }

    #[tokio::test]
    async fn batch_nested_ref_field_access() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon
            .dispatch(req(
                "batch",
                json!({
                    "ops": [
                        {"id": "e1", "method": "entry.create", "params": {"title": "doc", "blocks":[{"type":"note","text":"x"}]}},
                        // Self-link: source_id and target_id both reference e1.
                        // Exercises both top-level ($ref) and field-only (.id) access.
                        {"method": "link.create", "params": {
                            "source_id": {"$ref": "e1.id"},
                            "target_id": {"$ref": "e1.id"},
                            "relation": "self"
                        }}
                    ]
                }),
            ))
            .await;

        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        let results = result["results"].as_array().unwrap();

        // Verify nested ref: link target_id == entry_id == source_id
        let entry_id = results[0]["result"]["id"].as_str().unwrap();
        let link_source = results[1]["result"]["source_id"].as_str().unwrap();
        let link_target = results[1]["result"]["target_id"].as_str().unwrap();
        assert_eq!(link_source, entry_id);
        assert_eq!(link_target, entry_id);
    }

    #[tokio::test]
    async fn batch_visible_via_mcp_tools_list() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon.dispatch(req("tools/list", json!({}))).await;
        let tools = resp.result.unwrap();
        let tools = tools["tools"].as_array().unwrap();
        assert!(
            tools.iter().any(|t| t["name"] == "batch"),
            "batch should appear in MCP tools/list"
        );
    }

    // ----- cache.* e2e tests (Spec 5 enhancement) -----

    #[tokio::test]
    async fn cache_stats_returns_initial_state_with_warn_fields() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let resp = daemon.dispatch(req("cache.stats", json!({}))).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        let emb = &result["embeddings"];
        assert_eq!(emb["model"], "test-model");
        assert_eq!(emb["dim"], DIM);
        assert_eq!(emb["rows"], 0);
        assert_eq!(emb["hits"], 0);
        assert_eq!(emb["misses"], 0);
        assert!(emb["warn_rows"].as_u64().is_some());
        assert_eq!(emb["warning"], false);
        // Spec 7: searches namespace present.
        let s = &result["searches"];
        assert_eq!(s["generation"], 0);
        assert_eq!(s["entries"], 0);
        assert_eq!(s["hits"], 0);
        assert_eq!(s["misses"], 0);
    }

    #[tokio::test]
    async fn cache_clear_returns_by_model_breakdown() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        // Plan 4: entry.create no longer auto-populates emb_cache; inject
        // rows directly. Two distinct body hashes under "test-model" plus
        // one under "other-model" so by_model has two keys.
        {
            let conn = daemon.entries.conn_for_test();
            let c = conn.lock().unwrap();
            c.execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "test-model",
                    vec![1u8; 32],
                    DIM,
                    vec![0u8; DIM * 4],
                    "2026-01-01T00:00:00Z"
                ],
            )
            .unwrap();
            c.execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "test-model",
                    vec![2u8; 32],
                    DIM,
                    vec![0u8; DIM * 4],
                    "2026-01-02T00:00:00Z"
                ],
            )
            .unwrap();
            c.execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "other-model",
                    vec![9u8; 32],
                    DIM,
                    vec![0u8; DIM * 4],
                    "2026-01-01T00:00:00Z"
                ],
            )
            .unwrap();
        }

        // Default namespace = "embeddings" → only emb_cache cleared.
        let resp = daemon.dispatch(req("cache.clear", json!({}))).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert!(result["embeddings"]["cleared"].as_u64().unwrap() >= 2);
        let by_model = result["embeddings"]["by_model"].as_object().unwrap();
        assert!(by_model.contains_key("test-model"));
        assert!(by_model.contains_key("other-model"));
        // Searches namespace null (not touched by default clear).
        assert!(result["searches"].is_null());
    }

    #[tokio::test]
    async fn cache_clear_with_before_filter() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        // Insert two rows directly with different created_at.
        {
            let conn = daemon.entries.conn_for_test();
            let c = conn.lock().unwrap();
            c.execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "test-model",
                    vec![1u8; 32],
                    DIM,
                    vec![0u8; DIM * 4],
                    "2026-01-01T00:00:00Z"
                ],
            )
            .unwrap();
            c.execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "test-model",
                    vec![2u8; 32],
                    DIM,
                    vec![0u8; DIM * 4],
                    "2026-03-01T00:00:00Z"
                ],
            )
            .unwrap();
        }

        // Clear everything before 2026-02-01.
        let resp = daemon
            .dispatch(req(
                "cache.clear",
                json!({
                    "before": "2026-02-01T00:00:00Z"
                }),
            ))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let cleared = resp.result.unwrap()["embeddings"]["cleared"]
            .as_u64()
            .unwrap();
        assert_eq!(cleared, 1, "only the 2026-01-01 row should be cleared");

        // Verify the remaining row is the newer one.
        let stats = daemon.dispatch(req("cache.stats", json!({}))).await;
        assert_eq!(stats.result.unwrap()["embeddings"]["rows"], 1);
    }

    #[tokio::test]
    async fn cache_clear_with_keep_recent_filter() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        {
            let conn = daemon.entries.conn_for_test();
            let c = conn.lock().unwrap();
            for i in 1..=4u8 {
                c.execute(
                    "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        "test-model",
                        vec![i; 32],
                        DIM,
                        vec![0u8; DIM * 4],
                        format!("2026-0{i}-01T00:00:00Z"),
                    ],
                )
                .unwrap();
            }
        }

        // Keep newest 1 row.
        let resp = daemon
            .dispatch(req("cache.clear", json!({"keep_recent": 1})))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let cleared = resp.result.unwrap()["embeddings"]["cleared"]
            .as_u64()
            .unwrap();
        assert_eq!(cleared, 3);

        let stats = daemon.dispatch(req("cache.stats", json!({}))).await;
        assert_eq!(stats.result.unwrap()["embeddings"]["rows"], 1);
    }

    #[tokio::test]
    async fn cache_clear_with_namespace_searches_only() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;
        let _ = seed_one_entry(&daemon, "warm").await;

        // Warm search cache + drop one row into emb_cache.
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"warm","limit":10})))
            .await;
        {
            let conn = daemon.entries.conn_for_test();
            conn.lock().unwrap().execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params!["test-model", vec![1u8; 32], DIM, vec![0u8; DIM * 4], "2026-01-01T00:00:00Z"],
            ).unwrap();
        }

        let resp = daemon
            .dispatch(req("cache.clear", json!({"namespace": "searches"})))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert!(result["embeddings"].is_null(), "emb_cache untouched");
        assert_eq!(result["searches"]["cleared"].as_u64().unwrap(), 1);

        // Verify emb_cache still has its row.
        let stats = daemon.dispatch(req("cache.stats", json!({}))).await;
        assert_eq!(stats.result.unwrap()["embeddings"]["rows"], 1);
    }

    #[tokio::test]
    async fn cache_clear_with_namespace_all_clears_both() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;
        let _ = seed_one_entry(&daemon, "warm").await;

        // Warm both caches.
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"warm","limit":10})))
            .await;
        {
            let conn = daemon.entries.conn_for_test();
            conn.lock().unwrap().execute(
                "INSERT INTO emb_cache (model, body_hash, dim, embedding, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params!["test-model", vec![1u8; 32], DIM, vec![0u8; DIM * 4], "2026-01-01T00:00:00Z"],
            ).unwrap();
        }

        let resp = daemon
            .dispatch(req("cache.clear", json!({"namespace": "all"})))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert!(result["embeddings"]["cleared"].as_u64().unwrap() >= 1);
        assert!(result["searches"]["cleared"].as_u64().unwrap() >= 1);
    }

    #[tokio::test]
    async fn cache_clear_rejects_unknown_namespace() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;
        let resp = daemon
            .dispatch(req("cache.clear", json!({"namespace": "bogus"})))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003); // Validation
    }

    // ----- block.append e2e test (Plan 5 Task 2) -----

    #[tokio::test]
    async fn block_append_adds_block_and_rerenders_nomai() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create entry with one initial block at ordinal 0.
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"T","blocks":[{"type":"note","text":"first"}]}),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);
        let entry_json = create_resp.result.unwrap();
        let entry_id = entry_json["id"].as_str().unwrap().to_string();
        let entry_ulid: ulid::Ulid = entry_id.parse().unwrap();

        // Append a second block of a different type.
        let append_resp = daemon
            .dispatch(req(
                "block.append",
                json!({
                    "entry_id": entry_id,
                    "type": "question",
                    "text": "Why?",
                    "attrs": {"priority": "high"},
                }),
            ))
            .await;
        assert!(append_resp.error.is_none(), "{:?}", append_resp.error);
        let appended = append_resp.result.unwrap();
        assert_eq!(appended["entry_id"], entry_id);
        assert_eq!(appended["ordinal"], 1, "append should pick max ordinal + 1");
        assert_eq!(appended["type"], "question");
        assert_eq!(appended["text"], "Why?");
        assert_eq!(appended["attrs"]["priority"], "high");

        // entry.get reflects 2 blocks.
        let get_resp = daemon
            .dispatch(req("entry.get", json!({"id": entry_id})))
            .await;
        let blocks = get_resp.result.unwrap()["blocks"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[1]["type"], "question");

        // The .nomai file on disk was rewritten and round-trips.
        let doc = daemon
            .entries
            .content_store()
            .read_entry(entry_ulid)
            .expect("rerendered .nomai must be readable");
        assert_eq!(doc.title, "T");
        assert_eq!(doc.blocks.len(), 2);
        assert_eq!(doc.blocks[1].r#type.as_str(), "question");
    }

    #[tokio::test]
    async fn block_append_updates_fs_mtime_so_sync_skips_reindex() {
        // Plan 5 final review (I1): rerender_entry_nomai must refresh
        // entries.fs_mtime so the next sync_from_fs treats the entry as
        // unchanged. Without the refresh, sync sees a stale mtime and
        // triggers a full reindex on every boot.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create entry with one block.
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"T","blocks":[{"type":"note","text":"first"}]}),
            ))
            .await;
        let entry_id = create_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        let entry_ulid: ulid::Ulid = entry_id.parse().unwrap();

        // Capture fs_mtime right after create.
        let original_mtime: Option<String> = {
            let conn = daemon.entries.conn_for_test();
            let guard = conn.lock().unwrap();
            guard
                .query_row(
                    "SELECT fs_mtime FROM entries WHERE id = ?1",
                    rusqlite::params![entry_id],
                    |row| row.get(0),
                )
                .unwrap()
        };
        // entry.create writes the .nomai file via EntryService::create, which
        // sets fs_mtime. Sanity check it's present.
        assert!(original_mtime.is_some(), "create should set fs_mtime");

        // Sleep so the file mtime can differ (filesystem mtime resolution).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Append a block — this rewrites the .nomai file via rerender.
        let append_resp = daemon
            .dispatch(req(
                "block.append",
                json!({"entry_id": entry_id, "type": "note", "text": "second"}),
            ))
            .await;
        assert!(append_resp.error.is_none(), "{:?}", append_resp.error);

        // fs_mtime should have been refreshed to the new file's mtime.
        let new_mtime: Option<String> = {
            let conn = daemon.entries.conn_for_test();
            let guard = conn.lock().unwrap();
            guard
                .query_row(
                    "SELECT fs_mtime FROM entries WHERE id = ?1",
                    rusqlite::params![entry_id],
                    |row| row.get(0),
                )
                .unwrap()
        };
        assert!(
            new_mtime.is_some(),
            "fs_mtime should remain set after append"
        );
        assert_ne!(
            original_mtime, new_mtime,
            "fs_mtime should refresh after block.append rewrites the .nomai file"
        );

        // The on-disk mtime must match what we stored — that's the whole
        // point of the refresh.
        let on_disk = daemon
            .entries
            .content_store()
            .entry_mtime(entry_ulid)
            .unwrap()
            .to_rfc3339();
        assert_eq!(
            new_mtime.unwrap(),
            on_disk,
            "stored fs_mtime must match on-disk mtime after rerender"
        );

        // sync_from_fs should treat this entry as unchanged (no reindex).
        let result = daemon.entries.sync_from_fs().unwrap();
        assert_eq!(
            result.updated, 0,
            "sync should not reindex; fs_mtime is fresh"
        );
        assert_eq!(
            result.unchanged, 1,
            "sync should report the entry as unchanged"
        );
        assert_eq!(result.added, 0);
        assert_eq!(result.removed, 0);
    }

    #[tokio::test]
    async fn block_append_to_unknown_entry_returns_not_found() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let phantom = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let resp = daemon
            .dispatch(req(
                "block.append",
                json!({"entry_id": phantom, "type": "note", "text": "x"}),
            ))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1001); // NotFound
    }

    // ----- block.update e2e tests (Plan 5 Task 3) -----

    #[tokio::test]
    async fn block_update_changes_text_and_rerenders_nomai() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create entry with a single block.
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"T","blocks":[{"type":"note","text":"original"}]}),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);
        let entry_json = create_resp.result.unwrap();
        let entry_id = entry_json["id"].as_str().unwrap().to_string();
        let entry_ulid: ulid::Ulid = entry_id.parse().unwrap();
        let block_id = entry_json["blocks"][0]["id"].as_str().unwrap().to_string();

        // Update the block: new text + type, leaving attrs untouched.
        let update_resp = daemon
            .dispatch(req(
                "block.update",
                json!({
                    "id": block_id,
                    "type": "claim",
                    "text": "rewritten",
                }),
            ))
            .await;
        assert!(update_resp.error.is_none(), "{:?}", update_resp.error);
        let updated = update_resp.result.unwrap();
        assert_eq!(updated["text"], "rewritten");
        assert_eq!(updated["type"], "claim");

        // entry.get reflects the mutation.
        let get_resp = daemon
            .dispatch(req("entry.get", json!({"id": entry_id})))
            .await;
        let blocks = get_resp.result.unwrap()["blocks"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "claim");
        assert_eq!(blocks[0]["text"], "rewritten");

        // The .nomai file on disk was rewritten and round-trips.
        let doc = daemon
            .entries
            .content_store()
            .read_entry(entry_ulid)
            .expect("rerendered .nomai must be readable");
        assert_eq!(doc.blocks.len(), 1);
        assert_eq!(doc.blocks[0].r#type.as_str(), "claim");
        assert_eq!(doc.blocks[0].text.trim(), "rewritten");
    }

    #[tokio::test]
    async fn block_update_re_chunks_on_text_change() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create entry whose block has text spanning multiple chunks.
        let long_text = "para.\n\n".repeat(200);
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"T","blocks":[{"type":"note","text":long_text}]}),
            ))
            .await;
        let entry_json = create_resp.result.unwrap();
        let block_id: ulid::Ulid = entry_json["blocks"][0]["id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();

        let before = daemon.chunks.list(block_id).unwrap().total;
        assert!(before > 1);

        // Shrink the text → re-chunk should produce exactly one chunk.
        daemon
            .dispatch(req(
                "block.update",
                json!({"id": block_id.to_string(), "text": "short"}),
            ))
            .await;
        let after = daemon.chunks.list(block_id).unwrap().total;
        assert_eq!(after, 1);
    }

    #[tokio::test]
    async fn block_update_to_unknown_block_returns_not_found() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let phantom = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let resp = daemon
            .dispatch(req("block.update", json!({"id": phantom, "text": "x"})))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1001); // NotFound
    }

    // ----- block.delete e2e tests (Plan 5 Task 4) -----

    #[tokio::test]
    async fn block_delete_removes_block_and_rerenders_nomai() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create entry with two blocks.
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"T","blocks":[
                    {"type":"note","text":"first"},
                    {"type":"claim","text":"second"}
                ]}),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);
        let entry_json = create_resp.result.unwrap();
        let entry_id = entry_json["id"].as_str().unwrap().to_string();
        let entry_ulid: ulid::Ulid = entry_id.parse().unwrap();
        let block0_id = entry_json["blocks"][0]["id"].as_str().unwrap().to_string();
        let block1_id = entry_json["blocks"][1]["id"].as_str().unwrap().to_string();

        // Delete the first block.
        let del_resp = daemon
            .dispatch(req("block.delete", json!({"id": block0_id})))
            .await;
        assert!(del_resp.error.is_none(), "{:?}", del_resp.error);
        let del_result = del_resp.result.unwrap();
        assert_eq!(del_result["deleted"], true);
        assert_eq!(del_result["id"], block0_id);

        // entry.get now shows only the surviving block.
        let get_resp = daemon
            .dispatch(req("entry.get", json!({"id": entry_id})))
            .await;
        let blocks = get_resp.result.unwrap()["blocks"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["id"], block1_id);
        assert_eq!(blocks[0]["type"], "claim");

        // The .nomai file on disk was rewritten and round-trips.
        let doc = daemon
            .entries
            .content_store()
            .read_entry(entry_ulid)
            .expect("rerendered .nomai must be readable");
        assert_eq!(doc.blocks.len(), 1);
        assert_eq!(doc.blocks[0].r#type.as_str(), "claim");
    }

    #[tokio::test]
    async fn block_delete_cleans_chunk_embeddings_via_trigger() {
        // Plan 5 Task 4: the V9 chunks_ad trigger must clean
        // vec_chunk_embeddings when block.delete CASCADE-removes chunks.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"T","blocks":[{"type":"note","text":"body"}]}),
            ))
            .await;
        let entry_json = create_resp.result.unwrap();
        let block_id: ulid::Ulid = entry_json["blocks"][0]["id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();

        // Write a chunk embedding directly.
        let chunks = daemon.chunks.clone();
        let chunk_id = chunks.list(block_id).unwrap().items[0].id;
        chunks.write_embedding(chunk_id, &[1.0_f32; DIM]).unwrap();

        // Precondition: the vec_chunk_embeddings row exists.
        {
            let conn = daemon.entries.conn_for_test();
            let n: i64 = conn
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM vec_chunk_embeddings", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(n, 1);
        }

        // Delete the block — CASCADE removes chunks; chunks_ad trigger cleans
        // vec_chunk_embeddings.
        daemon
            .dispatch(req("block.delete", json!({"id": block_id.to_string()})))
            .await;

        // The trigger should have removed the embedding row.
        let n: i64 = {
            let conn = daemon.entries.conn_for_test();
            conn.lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM vec_chunk_embeddings", [], |row| {
                    row.get(0)
                })
                .unwrap()
        };
        assert_eq!(n, 0, "chunks_ad trigger should clean vec_chunk_embeddings");
    }

    #[tokio::test]
    async fn block_delete_to_unknown_block_returns_not_found() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        let phantom = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let resp = daemon
            .dispatch(req("block.delete", json!({"id": phantom})))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1001); // NotFound
    }

    // ----- system.export_to_fs e2e test (Plan 6 Task 3) -----

    #[tokio::test]
    async fn system_export_to_fs_rpc_generates_missing_nomai() {
        // Spec §12: walk every entry row and render .nomai for any that lack
        // one. Entries created via entry.create already have .nomai; orphan
        // rows (created via direct DB manipulation) do not.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create one entry normally (writes .nomai).
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"Has File","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);
        let indexed_id = create_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .parse::<ulid::Ulid>()
            .unwrap();

        // Manually insert an orphan entry directly in the DB, bypassing the
        // service so no .nomai file is written. Daemon startup has already
        // run its sync pass, so this row will not be cleaned up on boot.
        let orphan_id = ulid::Ulid::new();
        {
            let conn = daemon.entries.conn_for_test();
            let guard = conn.lock().unwrap();
            guard
                .execute(
                    "INSERT INTO entries (id, title, tags, attrs, source, fs_path, fs_mtime, created_at, updated_at)
                     VALUES (?1, 'orphan', '[]', '{}', NULL, NULL, NULL, '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
                    rusqlite::params![orphan_id.to_string()],
                )
                .unwrap();
            let block_id = ulid::Ulid::new().to_string();
            guard
                .execute(
                    "INSERT INTO blocks (id, entry_id, ordinal, type, text, attrs, created_at, updated_at)
                     VALUES (?1, ?2, 0, 'note', 'orphan content', '{}', '2026-06-23T10:00:00Z', '2026-06-23T10:00:00Z')",
                    rusqlite::params![block_id, orphan_id.to_string()],
                )
                .unwrap();
        }

        // Precondition: orphan has no .nomai file; indexed entry does.
        let cs = daemon.entries.content_store().clone();
        assert!(!cs.entry_file(orphan_id).exists());
        assert!(cs.entry_file(indexed_id).exists());

        // Call system.export_to_fs.
        let resp = daemon.dispatch(req("system.export_to_fs", json!({}))).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["exported"], 1, "orphan should be exported");
        assert_eq!(result["skipped"], 1, "indexed entry should be skipped");
        assert!(result["errors"].as_array().unwrap().is_empty());

        // Verify orphan now has a .nomai file on disk and fs_path populated.
        assert!(cs.entry_file(orphan_id).exists());
        let fs_path: String = {
            let conn = daemon.entries.conn_for_test();
            let guard = conn.lock().unwrap();
            guard
                .query_row(
                    "SELECT fs_path FROM entries WHERE id = ?1",
                    rusqlite::params![orphan_id.to_string()],
                    |row| row.get(0),
                )
                .unwrap()
        };
        assert!(!fs_path.is_empty());

        // The orphan's .nomai round-trips via the content store.
        let doc = cs.read_entry(orphan_id).unwrap();
        assert_eq!(doc.title, "orphan");
        assert_eq!(doc.blocks[0].text, "orphan content\n");
    }

    // ----- index.verify e2e test (Plan 6 Task 4) -----

    #[tokio::test]
    async fn index_verify_reports_drift_without_mutating() {
        // Read-only drift report. Drop an orphan .nomai file and verify
        // index.verify reports fs_only=1 without indexing it.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create one entry via the service so the index starts non-empty.
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"Indexed","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);

        // Drop an orphan .nomai file directly via the content store.
        let external_id = ulid::Ulid::new();
        let now: chrono::DateTime<chrono::Utc> = "2026-06-23T10:00:00Z".parse().unwrap();
        let doc = nomai_core::NomaiDoc {
            format_version: 1,
            id: external_id,
            title: "External".into(),
            tags: vec![],
            attrs: Default::default(),
            source: None,
            created_at: now,
            updated_at: now,
            blocks: vec![nomai_core::nomai_format::Block {
                r#type: nomai_core::nomai_format::BlockType::Note,
                text: "from fs\n".into(),
                attrs: Default::default(),
            }],
        };
        daemon
            .entries
            .content_store()
            .write_entry(external_id, &doc)
            .unwrap();

        // Call index.verify.
        let verify_resp = daemon.dispatch(req("index.verify", json!({}))).await;
        assert!(verify_resp.error.is_none(), "{:?}", verify_resp.error);
        let result = verify_resp.result.unwrap();
        assert_eq!(result["fs_only"], 1, "orphan FS file");
        assert_eq!(result["consistent"], 1, "indexed entry with matching mtime");
        assert_eq!(result["db_only"], 0);
        assert_eq!(result["stale_mtime"], 0);

        // Read-only: the orphan must NOT be indexed.
        let get_resp = daemon
            .dispatch(req("entry.get", json!({"id": external_id.to_string()})))
            .await;
        assert!(get_resp.error.is_some(), "verify must not index the orphan");
    }

    // ----- index.sync e2e test (Plan 5 Task 6) -----
    #[tokio::test]
    async fn index_sync_picks_up_external_file_and_reports_counts() {
        // Spec §7.1: FS is source-of-truth; index.sync reconciles. Drop a
        // .nomai file directly into the content store, then call index.sync
        // and verify the new entry appears + counts are reported.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create one entry via the service so the index starts non-empty.
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"Indexed","blocks":[{"type":"note","text":"x"}]}),
            ))
            .await;
        let indexed_id = create_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .parse::<ulid::Ulid>()
            .unwrap();

        // Drop a second .nomai file directly via the content store (no INSERT).
        let external_id = ulid::Ulid::new();
        let now: chrono::DateTime<chrono::Utc> = "2026-06-23T10:00:00Z".parse().unwrap();
        let doc = nomai_core::NomaiDoc {
            format_version: 1,
            id: external_id,
            title: "External".into(),
            tags: vec![],
            attrs: Default::default(),
            source: None,
            created_at: now,
            updated_at: now,
            blocks: vec![nomai_core::nomai_format::Block {
                r#type: nomai_core::nomai_format::BlockType::Note,
                text: "from fs\n".into(),
                attrs: Default::default(),
            }],
        };
        daemon
            .entries
            .content_store()
            .write_entry(external_id, &doc)
            .unwrap();

        // Sync.
        let sync_resp = daemon.dispatch(req("index.sync", json!({}))).await;
        assert!(sync_resp.error.is_none(), "{:?}", sync_resp.error);
        let result = sync_resp.result.unwrap();
        assert_eq!(result["added"], 1);
        assert_eq!(result["unchanged"], 1);
        assert_eq!(result["updated"], 0);
        assert_eq!(result["removed"], 0);

        // The external entry is now retrievable via entry.get.
        let get_resp = daemon
            .dispatch(req("entry.get", json!({"id": external_id.to_string()})))
            .await;
        assert!(get_resp.error.is_none(), "{:?}", get_resp.error);
        assert_eq!(get_resp.result.unwrap()["title"], "External");

        // The originally-indexed entry is untouched (still present, same id).
        let _ = indexed_id;
    }

    // ----- index.rebuild e2e test (Plan 5 Task 7) -----

    #[tokio::test]
    async fn index_rebuild_restores_blocks_and_preserves_events() {
        // Spec §7.1: rebuild wipes derived tables + reindexes every FS
        // entry. events (audit history) and emb_cache (deterministic) are
        // untouched.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create two entries via the service so the index + FS are seeded.
        let c1 = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"first","blocks":[{"type":"note","text":"a"}]}),
            ))
            .await;
        let c2 = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"second","blocks":[{"type":"note","text":"b"}]}),
            ))
            .await;
        let id1 = c1.result.unwrap()["id"].as_str().unwrap().to_string();
        let id2 = c2.result.unwrap()["id"].as_str().unwrap().to_string();

        // Corrupt the index: drop id1's blocks row directly. (The chunks_ad
        // trigger also removes chunk embeddings when we drop chunks; that's
        // fine for the test, we only care that rebuild re-creates them.)
        {
            let conn = daemon.entries.conn_for_test();
            let guard = conn.lock().unwrap();
            guard
                .execute(
                    "DELETE FROM blocks WHERE entry_id = ?1",
                    rusqlite::params![id1],
                )
                .unwrap();
        }
        // Precondition: id1 has no blocks.
        let pre = daemon
            .dispatch(req("entry.get", json!({"id": id1.clone()})))
            .await;
        assert!(pre.result.unwrap()["blocks"].as_array().unwrap().is_empty());

        // Snapshot event count before rebuild (2 entry.created + 2 block.created
        // = 4 events). These should survive the rebuild (reindex may append
        // new block.created events during re-population, but must not wipe
        // pre-existing ones).
        let events_before: i64 = {
            let conn = daemon.entries.conn_for_test();
            conn.lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
                .unwrap()
        };

        // Rebuild.
        let rebuild_resp = daemon.dispatch(req("index.rebuild", json!({}))).await;
        assert!(rebuild_resp.error.is_none(), "{:?}", rebuild_resp.error);
        let result = rebuild_resp.result.unwrap();
        assert_eq!(result["reindexed"], 2);
        assert!(result["errors"].as_array().unwrap().is_empty());

        // id1's blocks are restored from the .nomai file.
        let post = daemon
            .dispatch(req("entry.get", json!({"id": id1.clone()})))
            .await;
        let blocks = post.result.unwrap()["blocks"].as_array().unwrap().clone();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["text"], "a");

        // id2 also re-indexed.
        let post2 = daemon
            .dispatch(req("entry.get", json!({"id": id2.clone()})))
            .await;
        assert_eq!(post2.result.unwrap()["blocks"][0]["text"], "b");

        // events table not wiped — reindex may append new block.created
        // events, but pre-existing events (the original entry.created +
        // block.created) must still be present.
        let events_after: i64 = {
            let conn = daemon.entries.conn_for_test();
            conn.lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
                .unwrap()
        };
        assert!(
            events_after >= events_before,
            "events table wiped by rebuild: before={events_before}, after={events_after}"
        );
    }

    // ----- daemon startup scan e2e test (Plan 5 Task 8) -----

    #[tokio::test]
    async fn daemon_startup_syncs_pre_populated_fs_to_index() {
        // Spec §9.1: daemon startup runs `EntryService::sync_from_fs` once
        // before serving RPCs. Pre-populate the FS with a .nomai file (via
        // ContentStore directly, bypassing EntryService so the index starts
        // empty), then construct a daemon and verify the entry is indexed.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;

        // Build a content store in a temp dir, then construct EntryService
        // against it (instead of for_test, which makes its own anonymous dir).
        let tmp = tempfile::tempdir().unwrap();
        let content_store = Arc::new(nomai_core::ContentStore::new(tmp.path().to_path_buf()));
        nomai_core::storage::init_sqlite_extensions();
        let conn = Arc::new(std::sync::Mutex::new(
            rusqlite::Connection::open_in_memory().unwrap(),
        ));
        let entries = Arc::new(EntryService::new(conn, content_store.clone(), 1024).unwrap());

        // Drop a .nomai file directly via the content store (no INSERT, no
        // EntryService::create) so the index is empty but the FS has one entry.
        let external_id = ulid::Ulid::new();
        let now: chrono::DateTime<chrono::Utc> = "2026-06-23T10:00:00Z".parse().unwrap();
        let doc = nomai_core::NomaiDoc {
            format_version: 1,
            id: external_id,
            title: "Pre-existing".into(),
            tags: vec![],
            attrs: Default::default(),
            source: None,
            created_at: now,
            updated_at: now,
            blocks: vec![nomai_core::nomai_format::Block {
                r#type: nomai_core::nomai_format::BlockType::Note,
                text: "dropped before boot\n".into(),
                attrs: Default::default(),
            }],
        };
        content_store.write_entry(external_id, &doc).unwrap();

        // Precondition: index has no rows yet.
        {
            let conn = entries.conn_for_test();
            let n: i64 = conn
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
                .unwrap();
            assert_eq!(n, 0);
        }

        // Construct the daemon. Daemon::for_test runs run_startup_sync at
        // the end of construction, which must pick up the external file.
        let embedder: Arc<dyn nomai_providers::EmbeddingProvider> =
            Arc::new(nomai_providers::OpenAiCompatibleEmbed::new(
                server.uri(),
                "test-key",
                "test-model",
                DIM,
            ));
        let llm: Arc<dyn nomai_providers::LlmProvider> = Arc::new(
            nomai_providers::OpenAiCompatibleLlm::new(server.uri(), "test-key", "test-model"),
        );
        let daemon = Daemon::for_test(
            entries,
            embedder,
            llm,
            "test-model".into(),
            "test-model".into(),
            DIM,
            1024,
        );

        // The external entry is now retrievable via entry.get without any
        // explicit index.sync call — startup scan indexed it.
        let get_resp = daemon
            .dispatch(req("entry.get", json!({"id": external_id.to_string()})))
            .await;
        assert!(get_resp.error.is_none(), "{:?}", get_resp.error);
        let result = get_resp.result.unwrap();
        assert_eq!(result["title"], "Pre-existing");
        assert_eq!(result["blocks"][0]["text"], "dropped before boot");

        // The startup scan emitted an `index.synced` event into the audit log.
        let events: Vec<Value> = {
            let conn = daemon.entries.conn_for_test();
            let guard = conn.lock().unwrap();
            let mut stmt = guard
                .prepare("SELECT type, payload FROM events ORDER BY id")
                .unwrap();
            let rows = stmt
                .query_map([], |row| {
                    let t: String = row.get(0)?;
                    let p: String = row.get(1)?;
                    Ok(json!({"type": t, "payload": p}))
                })
                .unwrap();
            rows.map(|r| r.unwrap()).collect()
        };
        let synced: Vec<&Value> = events
            .iter()
            .filter(|e| e["type"] == "index.synced")
            .collect();
        assert_eq!(
            synced.len(),
            1,
            "exactly one index.synced event: {events:?}"
        );
        let payload: Value = serde_json::from_str(synced[0]["payload"].as_str().unwrap()).unwrap();
        assert_eq!(payload["added"], 1);
        assert_eq!(payload["updated"], 0);
        assert_eq!(payload["removed"], 0);
        assert_eq!(payload["unchanged"], 0);
    }

    // ----- Plan 6 Task 5: quiet boot emits no index.synced event -----

    // ----- Plan 7 Task 3: full content storage lifecycle e2e regression -----

    #[tokio::test]
    async fn content_storage_full_lifecycle_e2e() {
        // One walk through create → append → update → search(block_type) →
        // verify → drop FS → verify drift → export_to_fs → verify clean.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // 1. Create entry with one @claim block.
        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({
                    "title": "Test claim",
                    "blocks": [{"type": "claim", "text": "Earth orbits the sun."}]
                }),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);
        let entry_id = create_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        let entry_ulid: ulid::Ulid = entry_id.parse().unwrap();

        // 2. Append an @evidence block.
        let append_resp = daemon
            .dispatch(req(
                "block.append",
                json!({
                    "entry_id": entry_id,
                    "type": "evidence",
                    "text": "Kepler observed elliptical orbits."
                }),
            ))
            .await;
        assert!(append_resp.error.is_none(), "{:?}", append_resp.error);
        let evidence_id = append_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // 3. Update the @evidence text.
        let update_resp = daemon
            .dispatch(req(
                "block.update",
                json!({
                    "id": evidence_id,
                    "text": "Kepler's laws describe elliptical orbits with the sun at one focus."
                }),
            ))
            .await;
        assert!(update_resp.error.is_none(), "{:?}", update_resp.error);

        // 4. entry.get reflects 2 blocks (claim + updated evidence).
        let get_resp = daemon
            .dispatch(req("entry.get", json!({"id": entry_id})))
            .await;
        assert!(get_resp.error.is_none(), "{:?}", get_resp.error);
        let blocks = get_resp.result.unwrap()["blocks"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "claim");
        assert_eq!(blocks[1]["type"], "evidence");
        assert!(blocks[1]["text"].as_str().unwrap().contains("one focus"));

        // 5. search.fulltext with block_type=claim matches "Earth".
        let claim_search = daemon
            .dispatch(req(
                "search.fulltext",
                json!({"query": "Earth", "block_type": "claim"}),
            ))
            .await;
        assert!(claim_search.error.is_none(), "{:?}", claim_search.error);
        let claim_hits = claim_search.result.unwrap()["items"]
            .as_array()
            .unwrap()
            .clone();
        assert!(!claim_hits.is_empty(), "claim should match 'Earth'");

        // Same query with block_type=evidence should NOT match the claim
        // (the evidence block contains "Kepler", not "Earth").
        let evidence_search = daemon
            .dispatch(req(
                "search.fulltext",
                json!({"query": "Earth", "block_type": "evidence"}),
            ))
            .await;
        assert!(
            evidence_search.error.is_none(),
            "{:?}",
            evidence_search.error
        );
        let evidence_hits = evidence_search.result.unwrap()["items"]
            .as_array()
            .unwrap()
            .clone();
        assert!(evidence_hits.is_empty(), "evidence doesn't contain 'Earth'");

        // 6. index.verify should report consistent=1 (entry's .nomai matches
        // the index with a fresh mtime).
        let verify_resp = daemon.dispatch(req("index.verify", json!({}))).await;
        assert!(verify_resp.error.is_none(), "{:?}", verify_resp.error);
        assert_eq!(verify_resp.result.unwrap()["consistent"], 1);

        // 7. Drop the .nomai file from FS to simulate corruption.
        let content_store = daemon.entries.content_store().clone();
        std::fs::remove_file(content_store.entry_file(entry_ulid)).unwrap();

        // 8. index.verify should now report drift — the entry is no longer
        // consistent because its .nomai is missing on disk.
        let verify_after = daemon.dispatch(req("index.verify", json!({}))).await;
        assert!(verify_after.error.is_none(), "{:?}", verify_after.error);
        let consistent_after: u64 =
            serde_json::from_value(verify_after.result.unwrap()["consistent"].clone()).unwrap();
        assert_eq!(
            consistent_after, 0,
            "entry should no longer be consistent after FS drop"
        );

        // 9. system.export_to_fs regenerates the .nomai from DB state.
        let export_resp = daemon.dispatch(req("system.export_to_fs", json!({}))).await;
        assert!(export_resp.error.is_none(), "{:?}", export_resp.error);
        let export_result = export_resp.result.unwrap();
        let exported: u64 = serde_json::from_value(export_result["exported"].clone()).unwrap();
        assert_eq!(exported, 1, "the dropped entry should be re-exported");
        assert!(
            content_store.entry_file(entry_ulid).exists(),
            ".nomai should be regenerated"
        );

        // 10. index.verify should now be consistent again.
        let verify_final = daemon.dispatch(req("index.verify", json!({}))).await;
        assert!(verify_final.error.is_none(), "{:?}", verify_final.error);
        assert_eq!(verify_final.result.unwrap()["consistent"], 1);
    }

    #[tokio::test]
    async fn quiet_boot_emits_no_index_synced_event() {
        // setup_daemon uses EntryService::for_test — empty FS + empty index.
        // Boot scan sees zero added/updated/removed and must skip the event
        // so the audit log stays quiet across restarts with no FS changes.
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let n: i64 = {
            let conn = daemon.entries.conn_for_test();
            let guard = conn.lock().unwrap();
            guard
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE type = 'index.synced'",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
        };
        assert_eq!(
            n, 0,
            "empty boot should not emit index.synced event (got {n})"
        );
    }

    // ----- Spec 7: search cache e2e -----

    #[tokio::test]
    async fn search_fulltext_caches_on_repeat() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"Rust","blocks":[{"type":"note","text":"rust programming language"}]}),
            ))
            .await;

        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"rust","limit":10})))
            .await;
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"rust","limit":10})))
            .await;

        let stats = daemon.search_cache.stats();
        assert_eq!(stats.fulltext_hits, 1, "second fulltext call should hit");
        assert_eq!(stats.fulltext_misses, 1);
    }

    #[tokio::test]
    async fn search_semantic_caches_on_repeat() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec![1.0_f32; DIM]}]
            })))
            .mount(&server)
            .await;
        let daemon = setup_daemon(&server).await;

        daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"X","blocks":[{"type":"note","text":"some body"}]}),
            ))
            .await;

        let _ = daemon
            .dispatch(req("search.semantic", json!({"query":"q","limit":10})))
            .await;
        let _ = daemon
            .dispatch(req("search.semantic", json!({"query":"q","limit":10})))
            .await;

        let stats = daemon.search_cache.stats();
        assert_eq!(stats.semantic_hits, 1, "second semantic call should hit");
        assert_eq!(stats.semantic_misses, 1);
    }

    async fn seed_one_entry(daemon: &Daemon, title: &str) -> String {
        let resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":title,"blocks":[{"type":"note","text":title}]}),
            ))
            .await;
        resp.result.unwrap()["id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn entry_create_invalidates_search_cache() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // First search (empty result): miss.
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"foo","limit":10})))
            .await;
        // Second search: hit (cache populated).
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"foo","limit":10})))
            .await;
        let stats_before = daemon.search_cache.stats();
        assert_eq!(stats_before.fulltext_hits, 1);

        // Create an entry → bump generation.
        let _id = seed_one_entry(&daemon, "foo entry").await;

        // Third search: should miss again (generation bumped).
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"foo","limit":10})))
            .await;
        let stats_after = daemon.search_cache.stats();
        assert_eq!(
            stats_after.generation,
            stats_before.generation + 1,
            "create should bump generation"
        );
        assert!(
            stats_after.fulltext_misses > stats_before.fulltext_misses,
            "post-bump search should miss"
        );
    }

    #[tokio::test]
    async fn entry_update_invalidates_search_cache() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;
        let id = seed_one_entry(&daemon, "orig").await;

        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"orig","limit":10})))
            .await;
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"orig","limit":10})))
            .await;
        let gen_before = daemon.search_cache.generation();

        daemon
            .dispatch(req("entry.update", json!({"id": id, "title": "renamed"})))
            .await;

        assert_eq!(
            daemon.search_cache.generation(),
            gen_before + 1,
            "entry.update should bump"
        );
    }

    #[tokio::test]
    async fn entry_delete_invalidates_search_cache() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;
        let id = seed_one_entry(&daemon, "x").await;

        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"x","limit":10})))
            .await;
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"x","limit":10})))
            .await;
        let gen_before = daemon.search_cache.generation();

        daemon
            .dispatch(req("entry.delete", json!({"id": id})))
            .await;

        assert_eq!(daemon.search_cache.generation(), gen_before + 1);
    }

    #[tokio::test]
    async fn block_append_invalidates_search_cache() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;
        let entry_id = seed_one_entry(&daemon, "host").await;

        // Warm the cache.
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"host","limit":10})))
            .await;
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"host","limit":10})))
            .await;
        let gen_before = daemon.search_cache.generation();

        daemon
            .dispatch(req(
                "block.append",
                json!({"entry_id": entry_id, "type": "note", "text": "appended"}),
            ))
            .await;

        assert_eq!(daemon.search_cache.generation(), gen_before + 1);
    }

    #[tokio::test]
    async fn block_update_invalidates_search_cache() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"t","blocks":[{"type":"note","text":"original"}]}),
            ))
            .await;
        let block_id = create_resp.result.unwrap()["blocks"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();

        let _ = daemon
            .dispatch(req(
                "search.fulltext",
                json!({"query":"original","limit":10}),
            ))
            .await;
        let _ = daemon
            .dispatch(req(
                "search.fulltext",
                json!({"query":"original","limit":10}),
            ))
            .await;
        let gen_before = daemon.search_cache.generation();

        daemon
            .dispatch(req(
                "block.update",
                json!({"id": block_id, "text": "rewritten"}),
            ))
            .await;

        assert_eq!(daemon.search_cache.generation(), gen_before + 1);
    }

    #[tokio::test]
    async fn block_delete_invalidates_search_cache() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let create_resp = daemon
            .dispatch(req(
                "entry.create",
                json!({"title":"t","blocks":[
                    {"type":"note","text":"first"},
                    {"type":"note","text":"second"}
                ]}),
            ))
            .await;
        let entry_json = create_resp.result.unwrap();
        let block0 = entry_json["blocks"][0]["id"].as_str().unwrap().to_string();

        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"first","limit":10})))
            .await;
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"first","limit":10})))
            .await;
        let gen_before = daemon.search_cache.generation();

        daemon
            .dispatch(req("block.delete", json!({"id": block0})))
            .await;

        assert_eq!(daemon.search_cache.generation(), gen_before + 1);
    }

    #[tokio::test]
    async fn index_sync_no_change_does_not_invalidate() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;
        let _ = seed_one_entry(&daemon, "x").await;

        // Warm cache.
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"x","limit":10})))
            .await;
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"x","limit":10})))
            .await;
        let gen_before = daemon.search_cache.generation();

        // Sync with no FS drift → must NOT bump.
        let resp = daemon.dispatch(req("index.sync", json!({}))).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);

        assert_eq!(
            daemon.search_cache.generation(),
            gen_before,
            "no-op sync should not bump generation"
        );
    }

    #[tokio::test]
    async fn index_rebuild_always_invalidates() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;
        let _ = seed_one_entry(&daemon, "x").await;

        // Warm cache.
        let _ = daemon
            .dispatch(req("search.fulltext", json!({"query":"x","limit":10})))
            .await;
        let gen_before = daemon.search_cache.generation();

        // Rebuild — even with no changes, must bump (safe default).
        let resp = daemon.dispatch(req("index.rebuild", json!({}))).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);

        assert_eq!(
            daemon.search_cache.generation(),
            gen_before + 1,
            "rebuild should always bump"
        );
    }
}
