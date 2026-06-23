//! Batch import a markdown file into nomai.
//! Demonstrates: batch RPC + blocks-based entry.create (paragraphs → blocks).
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
    // Plan 3: entries are blocks-based. Each paragraph becomes one block in
    // a single entry.create — no separate chunk.create ops needed.
    let blocks: Vec<serde_json::Value> = content
        .split("\n\n")
        .filter(|p| !p.trim().is_empty())
        .map(|p| json!({ "type": "note", "text": p }))
        .collect();

    let ops = vec![json!({
        "id": "e1",
        "method": "entry.create",
        "params": { "title": title, "blocks": blocks }
    })];

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
    let block_count = blocks.len();

    println!("✓ Imported '{}' as entry {}", title, entry_id);
    println!(
        "  {} blocks created, atomic={}, rolled_back={}",
        block_count, !rolled_back, rolled_back
    );
    println!("  Embedding: 1 batch API call for {block_count} texts");
    Ok(())
}
