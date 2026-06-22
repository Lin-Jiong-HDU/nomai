//! Reference Naive RAG implementation.
//!
//! Demonstrates that `nomai-core` (EntryService) + `nomai-providers`
//! (EmbeddingProvider + LlmProvider) are sufficient to build a RAG flow
//! *without* any daemon-level RAG support: no qa.ask RPC, no subprocess.
//!
//! Usage:
//!     cargo run --example rag -- "your question here"
//!
//! Requires the same env vars + config.toml as the daemon
//! (see `nomai_daemon::config::Config`).

use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use nomai_core::EntryService;
use nomai_core::storage;
use nomai_daemon::config::Config;
use nomai_providers::{
    ChatMessage, CompletionRequest, EmbeddingProvider, LlmProvider, MessageRole,
    OpenAiCompatibleEmbed, OpenAiCompatibleLlm,
};

const SYSTEM_PROMPT: &str =
    "Answer based on the following context. If insufficient, say so explicitly.";
const TOP_K: u32 = 5;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let question = std::env::args().nth(1).ok_or("usage: rag <question>")?;

    // 1. Load config + open the same SQLite store the daemon uses.
    let config = Config::load()?;
    if let Some(parent) = config.data.db_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    storage::init_sqlite_extensions();
    let conn = Arc::new(Mutex::new(Connection::open(&config.data.db_path)?));
    let entries = EntryService::new(conn)?;
    entries.ensure_vec_embeddings(config.embedding.dim)?;

    // 2. Read API keys + construct providers (config.validate checked env).
    let embed_key = std::env::var(&config.embedding.api_key_env)?;
    let llm_key = std::env::var(&config.llm.api_key_env)?;
    let embedder: Arc<dyn EmbeddingProvider> = Arc::new(OpenAiCompatibleEmbed::new(
        &config.embedding.base_url,
        &embed_key,
        &config.embedding.model,
        config.embedding.dim,
    ));
    let llm: Arc<dyn LlmProvider> = Arc::new(OpenAiCompatibleLlm::new(
        &config.llm.base_url,
        &llm_key,
        &config.llm.model,
    ));

    // 3. Embed the question.
    let qvec = embedder
        .embed(&[&question])
        .await?
        .into_iter()
        .next()
        .ok_or("empty embedding response")?;

    // 4. KNN top-K semantic search over entries.
    let hits = entries.semantic_search(&qvec, TOP_K)?;

    // 5. Build context from title + body of each hit.
    let context = hits
        .iter()
        .map(|h| format!("## {}\n\n{}", h.entry.title, h.entry.body))
        .collect::<Vec<_>>()
        .join("\n---\n\n");
    let user = if context.is_empty() {
        format!("Question: {question}\n\n(No relevant materials were found.)")
    } else {
        format!("Context:\n\n{context}\n\nQuestion: {question}")
    };

    // 6. LLM call.
    let resp = llm
        .complete(CompletionRequest {
            system: Some(SYSTEM_PROMPT.into()),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: user,
            }],
            max_tokens: None,
            temperature: Some(0.0),
        })
        .await?;

    // 7. Answer + citations.
    println!("{}", resp.content);
    let citations: Vec<String> = hits.iter().map(|h| h.entry.id.to_string()).collect();
    eprintln!("citations: {}", citations.join(", "));
    Ok(())
}
