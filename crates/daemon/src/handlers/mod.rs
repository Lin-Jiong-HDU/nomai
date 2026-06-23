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
pub mod link;
pub mod mcp;
pub mod provider;
pub mod search;

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

    const DIM: usize = 2048;

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

    /// Build a 2048-dim embedding (V9 daemon default) from a short prefix,
    /// zero-padding the rest. Used by similarity tests that want unit vectors
    /// along specific axes.
    fn vec_2048(prefix: &[f32]) -> Vec<f32> {
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
            .write_embedding(a_chunk_id, &vec_2048(&[1.0]))
            .unwrap();
        chunks
            .write_embedding(
                b_chunk_id,
                &vec_2048(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]),
            )
            .unwrap();

        // search.semantic will issue an embedding request for the query.
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec_2048(&[1.0])}]
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

        // Create 3 entries → 3 entry.created + 3 block.created = 6 events.
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
        assert_eq!(items.len(), 6);
        let last_event_id = items[5]["id"].as_str().unwrap().to_string();

        // Purge events with id < last_event_id (exclusive).
        let purge_resp = daemon
            .dispatch(req("events.purge", json!({"before": last_event_id})))
            .await;
        assert_eq!(purge_resp.result.unwrap()["deleted"], 5);

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

        // Create 3 entries → 6 events total (each entry.create emits
        // entry.created + block.created).
        for i in 0..3 {
            daemon
                .dispatch(req(
                    "entry.create",
                    json!({"title": format!("e{i}"), "blocks":[{"type":"note","text":"x"}]}),
                ))
                .await;
        }

        // Page 1: limit=4
        let p1 = daemon
            .dispatch(req("events.list", json!({"limit": 4})))
            .await;
        let p1_result = p1.result.unwrap();
        assert_eq!(p1_result["items"].as_array().unwrap().len(), 4);
        assert_eq!(p1_result["has_more"], true);
        let last_id = p1_result["items"][3]["id"].as_str().unwrap().to_string();

        // Page 2: since = last_id from page 1
        let p2 = daemon
            .dispatch(req("events.list", json!({"limit": 4, "since": last_id})))
            .await;
        let p2_result = p2.result.unwrap();
        assert_eq!(p2_result["items"].as_array().unwrap().len(), 2);
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
    async fn entry_delete_cascades_to_chunks_and_cleanups_embeddings() {
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

        // Precondition: chunk search finds 2.
        let pre = daemon
            .dispatch(req("search.semantic", json!({"query":"x","limit":10})))
            .await;
        assert_eq!(pre.result.unwrap()["items"].as_array().unwrap().len(), 2);

        // Delete the entry.
        daemon
            .dispatch(req("entry.delete", json!({"id": entry_id})))
            .await;

        // After: chunk search returns 0 (CASCADE removed chunks; entry.delete
        // handler cleaned up vec_chunk_embeddings rows).
        let post = daemon
            .dispatch(req("search.semantic", json!({"query":"x","limit":10})))
            .await;
        assert_eq!(post.result.unwrap()["items"].as_array().unwrap().len(), 0);

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
        // 23 built-in non-MCP handlers (entry:5, link:5, chunk:2, block:2,
        // events:3, search:2, provider:1, cache:2, batch:1).
        assert_eq!(tools.len(), 23);
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
        assert_eq!(tools.len(), 24); // 23 built-in + custom.echo
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
        let emb = &resp.result.unwrap()["embeddings"];
        assert_eq!(emb["model"], "test-model");
        assert_eq!(emb["dim"], DIM);
        assert_eq!(emb["rows"], 0);
        assert_eq!(emb["hits"], 0);
        assert_eq!(emb["misses"], 0);
        assert!(emb["warn_rows"].as_u64().is_some());
        assert_eq!(emb["warning"], false);
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

        let resp = daemon.dispatch(req("cache.clear", json!({}))).await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert!(result["cleared"].as_u64().unwrap() >= 2);
        let by_model = result["by_model"].as_object().unwrap();
        assert!(by_model.contains_key("test-model"));
        assert!(by_model.contains_key("other-model"));
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
        let cleared = resp.result.unwrap()["cleared"].as_u64().unwrap();
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
        let cleared = resp.result.unwrap()["cleared"].as_u64().unwrap();
        assert_eq!(cleared, 3);

        let stats = daemon.dispatch(req("cache.stats", json!({}))).await;
        assert_eq!(stats.result.unwrap()["embeddings"]["rows"], 1);
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
}
