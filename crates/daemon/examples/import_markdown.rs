//! Batch import a markdown file into nomai.
//! Demonstrates: batch RPC + $ref + chunking (all application-layer composition).
//!
//! Run: cargo run --example import_markdown -- path/to/file.md

use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let md_path = std::env::args()
        .nth(1)
        .ok_or("Usage: import_markdown <file.md>")?;
    let content = std::fs::read_to_string(&md_path)?;
    let title = std::path::Path::new(&md_path)
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .to_string();

    // Simple chunking: split by double-newline (paragraphs).
    // Plan 3: entries are blocks-based. Put every paragraph in a single
    // entry as one block per paragraph; skip separate chunk creation.
    let paragraphs: Vec<&str> = content
        .split("\n\n")
        .filter(|p| !p.trim().is_empty())
        .collect();

    let blocks: Vec<serde_json::Value> = paragraphs
        .iter()
        .map(|p| json!({ "type": "note", "text": p }))
        .collect();

    let mut ops = vec![json!({
        "id": "e1",
        "method": "entry.create",
        "params": { "title": title, "blocks": blocks }
    })];

    // Extra paragraphs were previously emitted as chunks; that role is now
    // filled by multiple blocks within the same entry. We keep the rest of
    // the batch flow unchanged for demonstration purposes — no chunks here.
    for (ordinal, text) in paragraphs.iter().skip(1).enumerate() {
        ops.push(json!({
            "id": format!("c{ordinal}"),
            "method": "chunk.create",
            "params": {
                "entry_id": { "$ref": "e1.id" },
                "ordinal": ordinal,
                "text": *text
            }
        }));
    }

    let config = nomai_daemon::config::Config::load()?;
    let daemon = nomai_daemon::daemon::Daemon::new(config).await?;

    let req = nomai_protocol::Request {
        jsonrpc: nomai_protocol::JSONRPC_VERSION.into(),
        id: Some(nomai_protocol::Id::Number(1)),
        method: "batch".into(),
        params: Some(json!({ "ops": ops })),
    };
    let resp = daemon.dispatch(req).await;

    if let Some(err) = &resp.error {
        eprintln!("Batch failed: {} (code {})", err.message, err.code);
        std::process::exit(1);
    }

    let result = resp.result.unwrap();
    let results = result["results"].as_array().unwrap();
    let rolled_back = result["rolled_back"].as_bool().unwrap();
    let entry_id = results[0]["result"]["id"].as_str().unwrap();
    let chunk_count = results.len() - 1;

    println!("✓ Imported '{}' as entry {}", title, entry_id);
    println!(
        "  {} chunks created, atomic={}, rolled_back={}",
        chunk_count, !rolled_back, rolled_back
    );
    println!(
        "  Embedding: 1 batch API call for {} texts",
        1 + chunk_count
    );
    Ok(())
}
