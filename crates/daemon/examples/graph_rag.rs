//! Graph-aware RAG: search → expand via links → LLM synthesis.
//! Demonstrates: search.semantic + link.neighbors + LLM composition.
//! Proves GraphRAG is pure application-layer composition over nomai primitives.
//!
//! Run: cargo run --example graph_rag -- "your question"

use std::collections::HashSet;

use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let question = std::env::args()
        .nth(1)
        .ok_or("Usage: graph_rag <question>")?;

    let config = nomai_daemon::config::Config::load()?;
    let daemon = nomai_daemon::daemon::Daemon::new(config).await?;

    // Step 1: Seed search (entry-level semantic, top 3)
    let seed_resp = daemon
        .dispatch(nomai_protocol::Request {
            jsonrpc: nomai_protocol::JSONRPC_VERSION.into(),
            id: Some(nomai_protocol::Id::Number(1)),
            method: "search.semantic".into(),
            params: Some(json!({ "query": &question, "limit": 3 })),
        })
        .await;

    let seed_items = seed_resp.result.unwrap()["items"]
        .as_array()
        .unwrap()
        .clone();

    // Step 2: Expand via link.neighbors (graph traversal)
    let mut entries: Vec<serde_json::Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for item in &seed_items {
        let entry = &item["entry"];
        let id = entry["id"].as_str().unwrap();
        if seen.insert(id.to_string()) {
            entries.push(entry.clone());
        }

        let nb_resp = daemon
            .dispatch(nomai_protocol::Request {
                jsonrpc: nomai_protocol::JSONRPC_VERSION.into(),
                id: Some(nomai_protocol::Id::Number(2)),
                method: "link.neighbors".into(),
                params: Some(json!({ "id": id, "direction": "both", "limit": 5 })),
            })
            .await;

        if let Some(result) = nb_resp.result {
            for neighbor in result["entries"].as_array().unwrap() {
                let nid = neighbor["id"].as_str().unwrap();
                if seen.insert(nid.to_string()) {
                    entries.push(neighbor.clone());
                }
            }
        }
    }

    let seeds = seed_items.len();
    let neighbors = entries.len() - seeds;
    println!(
        "Retrieved {} entries ({} seeds + {} neighbors via link.neighbors)",
        entries.len(),
        seeds,
        neighbors
    );

    // Step 3: Build context + call LLM (pure application-layer composition).
    //         Entries store content as blocks; re-join the block texts with
    //         "\n\n" to reconstruct the body for the LLM.
    let context: Vec<String> = entries
        .iter()
        .map(|e| {
            let body = e["blocks"]
                .as_array()
                .map(|blocks| {
                    blocks
                        .iter()
                        .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                        .collect::<Vec<_>>()
                        .join("\n\n")
                })
                .unwrap_or_default();
            format!(
                "## {}\n{}",
                e["title"].as_str().unwrap_or("(untitled)"),
                body
            )
        })
        .collect();
    let context_str = context.join("\n\n---\n\n");

    let llm = daemon.llm().clone();
    let completion = nomai_providers::LlmProvider::complete(
        &*llm,
        nomai_providers::CompletionRequest {
            system: Some("You are a knowledge assistant. Answer based on the context. If the context is insufficient, say so.".into()),
            messages: vec![nomai_providers::ChatMessage {
                role: nomai_providers::MessageRole::User,
                content: format!("Context:\n\n{context_str}\n\nQuestion: {question}"),
            }],
            max_tokens: Some(500),
            temperature: None,
        },
    )
    .await?;

    println!("\n--- Answer ---\n{}\n", completion.content);
    println!("--- Citations ---");
    for e in &entries {
        println!(
            "  • {} ({})",
            e["title"].as_str().unwrap_or("?"),
            e["id"].as_str().unwrap_or("?")
        );
    }
    Ok(())
}
