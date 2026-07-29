//! Reference Naive RAG implementation.
//!
//! Demonstrates that `nomai-core` (EntryService + ChunkService) +
//! `nomai-providers` (EmbeddingProvider + LlmProvider) are sufficient to
//! build a RAG flow *without* any daemon-level RAG support: no qa.ask RPC,
//! no subprocess.
//!
//! Semantic search runs over chunks (block-derived); each hit's
//! parent block/entry is reachable via JOIN.
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
use nomai_daemon::config::default_knowledge_root;
use nomai_providers::{
    ChatMessage, CompletionRequest, EmbeddingProvider, LlmProvider, MessageRole,
    OpenAiCompatibleEmbed, OpenAiCompatibleLlm,
};

const SYSTEM_PROMPT: &str =
    "Answer based on the following context. If insufficient, say so explicitly.";
const TOP_K: usize = 5;

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
    let knowledge_root = config
        .data
        .knowledge_root
        .clone()
        .unwrap_or_else(default_knowledge_root);
    std::fs::create_dir_all(&knowledge_root)?;
    let content_store = Arc::new(nomai_core::ContentStore::new(knowledge_root));
    // EntryService is constructed to run migrations + share the connection;
    // not called directly in this example (chunks drive search).
    let _entries = EntryService::new(conn.clone(), content_store, 1024)?;
    let chunks = nomai_core::ChunkService::new(conn.clone())?;
    chunks.ensure_vec_chunk_embeddings(config.embedding.dim)?;

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

    // 4. KNN top-K semantic search over chunks. Each hit includes
    //    the chunk text; we resolve its parent block/entry via the daemon's
    //    block→entry JOIN when needed.
    let chunk_hits = chunks.semantic_search(&qvec, TOP_K, None, None)?;

    // 5. Build context from chunk text + look up parent entry for headings.
    let mut context_parts: Vec<String> = Vec::new();
    for hit in &chunk_hits {
        // Look up block → entry to title the citation. Lazy JOIN via
        // direct query (BlockService::get would also work).
        let block_id = hit.chunk.block_id;
        let entry_title: Option<String> = {
            let conn = conn.lock().unwrap();
            conn.query_row(
                "SELECT e.title FROM blocks b JOIN entries e ON e.id = b.entry_id WHERE b.id = ?1",
                rusqlite::params![block_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .ok()
        };
        let heading = entry_title.unwrap_or_else(|| "(unknown entry)".into());
        context_parts.push(format!("## {heading}\n\n{}", hit.chunk.text));
    }
    let context = context_parts.join("\n---\n\n");
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

    // 7. Answer + citations (entry ids of matched chunks).
    println!("{}", resp.content);
    let citations: Vec<String> = chunk_hits
        .iter()
        .map(|h| h.chunk.block_id.to_string())
        .collect();
    eprintln!("citations (block ids): {}", citations.join(", "));
    Ok(())
}
