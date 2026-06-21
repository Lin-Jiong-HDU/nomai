//! JSON-RPC method dispatch table.

use serde_json::Value;

use nomai_protocol::Request;

use crate::daemon::Daemon;
use crate::rpc::DispatchError;

pub mod entry;
pub mod provider;
pub mod qa;
pub mod search;

pub async fn route(daemon: &Daemon, req: Request) -> Result<Value, DispatchError> {
    let params = req.params.unwrap_or(Value::Null);
    let result: Result<Value, nomai_core::CoreError> = match req.method.as_str() {
        "entry.create" => entry::create(daemon, params).await,
        "entry.get" => entry::get(daemon, params).await,
        "entry.update" => entry::update(daemon, params).await,
        "entry.delete" => entry::delete(daemon, params).await,
        "entry.list" => entry::list(daemon, params).await,
        "search.fulltext" => search::fulltext(daemon, params).await,
        "search.semantic" => search::semantic(daemon, params).await,
        "qa.ask" => qa::ask(daemon, params).await,
        "provider.list" => provider::list(daemon, params).await,
        // Reserved method names per spec §6: return -32601.
        "search.hybrid" | "provider.set" => {
            return Err(DispatchError::MethodNotFound(req.method.clone()));
        }
        _ => return Err(DispatchError::MethodNotFound(req.method.clone())),
    };
    result.map_err(DispatchError::Core)
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
        Daemon::for_test(
            entries,
            embedder,
            llm,
            "test-model".into(),
            "test-model".into(),
            DIM,
        )
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
    async fn qa_ask_returns_answer_and_citations() {
        let server = MockServer::start().await;
        let daemon = setup_daemon(&server).await;

        // Seed an entry + embedding.
        let entries = daemon.entries.clone();
        let e = entries
            .create(nomai_core::CreateEntry {
                title: "Rust".into(),
                body: "Rust is a systems programming language.".into(),
                tags: None,
                attrs: None,
                source: None,
            })
            .unwrap();
        entries
            .write_embedding(e.id, &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            .unwrap();

        // Embedding call for the question.
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"index": 0, "embedding": vec![1.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}]
            })))
            .mount(&server)
            .await;
        // LLM call.
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "Rust is a systems language."}
                }]
            })))
            .mount(&server)
            .await;

        let resp = daemon
            .dispatch(req(
                "qa.ask",
                json!({
                    "question": "what is rust",
                    "top_k": 3,
                }),
            ))
            .await;
        assert!(resp.error.is_none(), "{:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["answer"], "Rust is a systems language.");
        let citations = result["citations"].as_array().unwrap();
        assert_eq!(citations.len(), 1);
        assert_eq!(citations[0].as_str().unwrap(), e.id.to_string());
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
}
