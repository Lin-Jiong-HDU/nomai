//! qa.ask RAG handler.

use serde::Deserialize;
use serde_json::{json, Value};

use nomai_core::CoreError;
use nomai_providers::{ChatMessage, CompletionRequest, MessageRole};

use crate::daemon::Daemon;
use crate::handlers::entry::blocking;

const SYSTEM_PROMPT: &str = "You are a helpful assistant answering questions strictly based on the provided materials. If the materials are insufficient to answer, say so explicitly and do not fabricate.";

fn default_top_k() -> u32 {
    5
}

#[derive(Deserialize)]
struct AskParams {
    question: String,
    #[serde(default = "default_top_k")]
    top_k: u32,
    #[serde(default)]
    max_tokens: Option<u32>,
}

pub async fn ask(daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
    let p: AskParams = serde_json::from_value(params)
        .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;

    // 1. Embed question.
    let q = p.question;
    let embeddings = daemon.embedder.embed(&[&q]).await?;
    let qvec = embeddings
        .into_iter()
        .next()
        .ok_or_else(|| CoreError::Config("empty embedding response".into()))?;

    // 2. KNN top-K.
    let entries = daemon.entries.clone();
    let top_k = p.top_k;
    let hits = blocking(move || entries.semantic_search(&qvec, top_k)).await??;

    // 3. Build context.
    let context = hits
        .iter()
        .map(|h| format!("## {}\n\n{}\n", h.entry.title, h.entry.body))
        .collect::<Vec<_>>()
        .join("\n---\n\n");

    // 4. Construct prompt + LLM call.
    let user = if context.is_empty() {
        format!("Question: {q}\n\n(No relevant materials were found.)")
    } else {
        format!("Materials:\n\n{context}\n\nQuestion: {q}")
    };

    let resp = daemon
        .llm
        .complete(CompletionRequest {
            system: Some(SYSTEM_PROMPT.into()),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: user,
            }],
            max_tokens: p.max_tokens,
            temperature: Some(0.0),
        })
        .await?;

    // 5. Citations = ids of contributing entries, in rank order.
    let citations: Vec<String> = hits.iter().map(|h| h.entry.id.to_string()).collect();

    Ok(json!({
        "answer": resp.content,
        "citations": citations,
    }))
}
