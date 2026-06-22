//! JSON-RPC method dispatch registry.
//!
//! Each method is a zero-sized struct implementing `RpcHandler`. The daemon
//! looks up handlers by method name in a `HashMap` populated by `registry()`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::rpc::RpcHandler;

pub mod chunk;
pub mod entry;
pub mod events;
pub mod link;
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

    // chunk.*
    let h = chunk::Create;
    m.insert(h.method(), Arc::new(h));
    let h = chunk::Get;
    m.insert(h.method(), Arc::new(h));
    let h = chunk::Delete;
    m.insert(h.method(), Arc::new(h));
    let h = chunk::List;
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

    const DIM: usize = 8;

    async fn setup_daemon(server: &MockServer) -> Daemon {
        let entries = Arc::new(EntryService::for_test().unwrap());
        entries.ensure_vec_embeddings(DIM).unwrap();
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
                    "body": "Hello world",
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
    async fn entry_create_triggers_embedding_http_call() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec![0.0_f32; DIM]}]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let daemon = setup_daemon(&server).await;
        let _ = daemon
            .dispatch(req(
                "entry.create",
                json!({
                    "title": "X",
                    "body": "Y",
                }),
            ))
            .await;
        // Mock's expect(1) verifies on drop that the embedding call was made.
    }

    #[tokio::test]
    async fn search_semantic_ranks_by_similarity() {
        let server = MockServer::start().await;

        // Seed two entries with known embeddings, then answer the query
        // embedding deterministically.
        let daemon = setup_daemon(&server).await;

        // Create entries — the mock returns a zero vector each time; we then
        // overwrite the embedding directly via EntryService for deterministic ranking.
        let entries = daemon.entries.clone();
        let a = entries
            .create(nomai_core::CreateEntry {
                title: "a".into(),
                body: "near query".into(),
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        let b = entries
            .create(nomai_core::CreateEntry {
                title: "b".into(),
                body: "far".into(),
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        entries
            .write_embedding(a.id, &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            .unwrap();
        entries
            .write_embedding(b.id, &[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
            .unwrap();

        // search.semantic will issue an embedding request for the query.
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec![1.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}]
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
        assert_eq!(items[0]["entry"]["title"], "a");
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
            .dispatch(req("entry.create", json!({"title":"a","body":"x"})))
            .await;
        let a_id = a_resp.result.unwrap()["id"].as_str().unwrap().to_string();
        let b_resp = daemon
            .dispatch(req("entry.create", json!({"title":"b","body":"y"})))
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
            .dispatch(req("entry.create", json!({"title":"b","body":"y"})))
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
            .dispatch(req("entry.create", json!({"title":"a","body":"x"})))
            .await;
        let a_id = a.result.unwrap()["id"].as_str().unwrap().to_string();
        let b = daemon
            .dispatch(req("entry.create", json!({"title":"b","body":"y"})))
            .await;
        let b_id = b.result.unwrap()["id"].as_str().unwrap().to_string();
        let c = daemon
            .dispatch(req("entry.create", json!({"title":"c","body":"z"})))
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
            .dispatch(req("entry.create", json!({"title":"a","body":"x"})))
            .await;
        let a_id = a.result.unwrap()["id"].as_str().unwrap().to_string();
        let b = daemon
            .dispatch(req("entry.create", json!({"title":"b","body":"y"})))
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
            .dispatch(req("entry.create", json!({"title":"a","body":"x"})))
            .await;
        let a_id = a.result.unwrap()["id"].as_str().unwrap().to_string();
        let b = daemon
            .dispatch(req("entry.create", json!({"title":"b","body":"y"})))
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
            .dispatch(req("entry.create", json!({"title":"Note","body":"Hello"})))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);

        let list_resp = daemon.dispatch(req("events.list", json!({}))).await;
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
            .dispatch(req("entry.create", json!({"title":"orig","body":"x"})))
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
            .dispatch(req("entry.create", json!({"title":"X","body":"y"})))
            .await;
        // Get the event id from events.list
        let list_resp = daemon.dispatch(req("events.list", json!({}))).await;
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

        // Create 3 entries → 3 events
        daemon
            .dispatch(req("entry.create", json!({"title":"a","body":"x"})))
            .await;
        daemon
            .dispatch(req("entry.create", json!({"title":"b","body":"x"})))
            .await;
        let _last_create = daemon
            .dispatch(req("entry.create", json!({"title":"c","body":"x"})))
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

        // Create 3 entries
        for i in 0..3 {
            daemon
                .dispatch(req(
                    "entry.create",
                    json!({"title": format!("e{i}"), "body": "x"}),
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

        // Page 2: since = last_id from page 1
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
            .dispatch(req("entry.create", json!({"title":"a","body":"x"})))
            .await;
        let a_id = a.result.unwrap()["id"].as_str().unwrap().to_string();
        let b = daemon
            .dispatch(req("entry.create", json!({"title":"b","body":"y"})))
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

    // ----- chunk.* + search.semantic granularity + entry.delete cleanup e2e (Plan 2 Task 4) -----

    #[tokio::test]
    async fn chunk_create_round_trips_via_get() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        // Create an entry first.
        let entry_resp = daemon
            .dispatch(req("entry.create", json!({"title":"doc","body":"x"})))
            .await;
        let entry_id = entry_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Create a chunk (will fire embedding HTTP call for chunk text).
        let create_resp = daemon
            .dispatch(req(
                "chunk.create",
                json!({"entry_id": entry_id, "ordinal": 0, "text":"first chunk"}),
            ))
            .await;
        assert!(create_resp.error.is_none(), "{:?}", create_resp.error);
        let chunk = create_resp.result.unwrap();
        let chunk_id = chunk["id"].as_str().unwrap().to_string();
        assert_eq!(chunk["ordinal"], 0);

        let get_resp = daemon
            .dispatch(req("chunk.get", json!({"id": chunk_id})))
            .await;
        assert!(get_resp.error.is_none());
        assert_eq!(get_resp.result.unwrap()["text"], "first chunk");
    }

    #[tokio::test]
    async fn chunk_create_returns_validation_for_missing_entry() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;
        let phantom = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

        let resp = daemon
            .dispatch(req(
                "chunk.create",
                json!({"entry_id": phantom, "ordinal": 0, "text":"x"}),
            ))
            .await;
        let err = resp.error.unwrap();
        assert_eq!(err.code, 1003);
    }

    #[tokio::test]
    async fn chunk_list_returns_chunks_sorted_by_ordinal() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req("entry.create", json!({"title":"d","body":"x"})))
            .await;
        let entry_id = entry_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Create chunks out of order.
        for ord in [2, 0, 1] {
            daemon
                .dispatch(req(
                    "chunk.create",
                    json!({"entry_id": entry_id, "ordinal": ord, "text": format!("c{ord}")}),
                ))
                .await;
        }

        let list_resp = daemon
            .dispatch(req("chunk.list", json!({"entry_id": entry_id})))
            .await;
        let result = list_resp.result.unwrap();
        assert_eq!(result["total"], 3);
        let items = result["items"].as_array().unwrap();
        assert_eq!(items[0]["ordinal"], 0);
        assert_eq!(items[1]["ordinal"], 1);
        assert_eq!(items[2]["ordinal"], 2);
    }

    #[tokio::test]
    async fn chunk_delete_removes_chunk_and_embedding() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req("entry.create", json!({"title":"d","body":"x"})))
            .await;
        let entry_id = entry_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        let create_resp = daemon
            .dispatch(req(
                "chunk.create",
                json!({"entry_id": entry_id, "ordinal": 0, "text":"x"}),
            ))
            .await;
        let chunk_id = create_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Verify semantic search finds the chunk.
        let search_resp = daemon
            .dispatch(req(
                "search.semantic",
                json!({"query":"anything","granularity":"chunk","limit":10}),
            ))
            .await;
        let result = search_resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);

        // Delete the chunk.
        let del_resp = daemon
            .dispatch(req("chunk.delete", json!({"id": chunk_id})))
            .await;
        assert_eq!(del_resp.result.unwrap()["deleted"], true);

        // Semantic search should now return empty.
        let search_resp2 = daemon
            .dispatch(req(
                "search.semantic",
                json!({"query":"anything","granularity":"chunk","limit":10}),
            ))
            .await;
        let result2 = search_resp2.result.unwrap();
        let items2 = result2["items"].as_array().unwrap();
        assert!(items2.is_empty());
    }

    #[tokio::test]
    async fn search_semantic_granularity_defaults_to_entry() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req("entry.create", json!({"title":"d","body":"x"})))
            .await;
        let entry_id = entry_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Create a chunk.
        daemon
            .dispatch(req(
                "chunk.create",
                json!({"entry_id": entry_id, "ordinal": 0, "text":"chunk text"}),
            ))
            .await;

        // Default granularity should be "entry" — items have "entry" field, not "chunk".
        let resp = daemon
            .dispatch(req(
                "search.semantic",
                json!({"query":"anything","limit":10}),
            ))
            .await;
        let result = resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        // Should have entry-level results (entry body was embedded), not chunk-level.
        assert!(items.iter().all(|i| i["entry"].is_object()));
    }

    #[tokio::test]
    async fn search_semantic_granularity_chunk_returns_chunks() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req("entry.create", json!({"title":"d","body":"x"})))
            .await;
        let entry_id = entry_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        daemon
            .dispatch(req(
                "chunk.create",
                json!({"entry_id": entry_id, "ordinal": 0, "text":"chunk content"}),
            ))
            .await;

        let resp = daemon
            .dispatch(req(
                "search.semantic",
                json!({"query":"anything","granularity":"chunk","limit":10}),
            ))
            .await;
        let result = resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert!(!items.is_empty());
        assert!(items.iter().all(|i| i["chunk"].is_object()));
        assert_eq!(items[0]["chunk"]["entry_id"], entry_id);
    }

    #[tokio::test]
    async fn entry_delete_cascades_to_chunks_and_cleanups_embeddings() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req("entry.create", json!({"title":"d","body":"x"})))
            .await;
        let entry_id = entry_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Create 2 chunks.
        for ord in 0..2 {
            daemon
                .dispatch(req(
                    "chunk.create",
                    json!({"entry_id": entry_id, "ordinal": ord, "text": format!("c{ord}")}),
                ))
                .await;
        }

        // Precondition: chunk search finds 2.
        let pre = daemon
            .dispatch(req(
                "search.semantic",
                json!({"query":"x","granularity":"chunk","limit":10}),
            ))
            .await;
        assert_eq!(pre.result.unwrap()["items"].as_array().unwrap().len(), 2);

        // Delete the entry.
        daemon
            .dispatch(req("entry.delete", json!({"id": entry_id})))
            .await;

        // After: chunk search returns 0 (CASCADE removed chunks, cleanup removed embeddings).
        let post = daemon
            .dispatch(req(
                "search.semantic",
                json!({"query":"x","granularity":"chunk","limit":10}),
            ))
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
    async fn chunk_create_emits_event_visible_via_events_list() {
        let server = MockServer::start().await;
        mount_embedding_mock(&server).await;
        let daemon = setup_daemon(&server).await;

        let entry_resp = daemon
            .dispatch(req("entry.create", json!({"title":"d","body":"x"})))
            .await;
        let entry_id = entry_resp.result.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        daemon
            .dispatch(req(
                "chunk.create",
                json!({"entry_id": entry_id, "ordinal": 0, "text":"x"}),
            ))
            .await;

        let list_resp = daemon
            .dispatch(req("events.list", json!({"type":"chunk.created"})))
            .await;
        let result = list_resp.result.unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["payload"]["entry_id"], entry_id);
    }
}
