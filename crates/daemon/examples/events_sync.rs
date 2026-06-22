//! Incremental sync: poll events → export entries to markdown files.
//! Demonstrates: events.list + client cursor + entry payload from event snapshot.
//! Proves Events primitive enables sync patterns without core-level reconciler.
//!
//! No API keys needed (read-only, uses dummy providers).
//!
//! Run: cargo run --example events_sync -- /path/to/output_dir [db_path]

use std::sync::Arc;

use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = std::env::args()
        .nth(1)
        .ok_or("Usage: events_sync <output_dir> [db_path]")?;
    let db_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "~/.local/share/nomai/db.sqlite".into());

    // Expand ~ in db_path
    let db_path = if let Some(rest) = db_path.strip_prefix('~') {
        let home = std::env::var("HOME")?;
        format!("{home}{rest}")
    } else {
        db_path
    };

    let cursor_file = format!("{output_dir}/.nomai_cursor");

    // Read last cursor (ULID of last synced event)
    let since: Option<String> = std::fs::read_to_string(&cursor_file)
        .ok()
        .and_then(|s| s.trim().parse::<ulid::Ulid>().ok())
        .map(|u| u.to_string());

    // Build daemon with dummy providers (read-only, no API calls)
    nomai_core::storage::init_sqlite_extensions();
    let conn = Arc::new(std::sync::Mutex::new(rusqlite::Connection::open(&db_path)?));
    let embedder: Arc<dyn nomai_providers::EmbeddingProvider> = Arc::new(
        nomai_providers::OpenAiCompatibleEmbed::new("http://localhost", "x", "x", 8),
    );
    let llm: Arc<dyn nomai_providers::LlmProvider> = Arc::new(
        nomai_providers::OpenAiCompatibleLlm::new("http://localhost", "x", "x"),
    );
    let daemon = nomai_daemon::daemon::Daemon::from_services(
        conn,
        embedder,
        llm,
        8,
        "example-model",
        100_000,
    )?;

    // Poll events since cursor
    let mut params = json!({ "limit": 100, "order": "asc" });
    if let Some(ref s) = since {
        params["since"] = json!(s);
    }

    let resp = daemon
        .dispatch(nomai_protocol::Request {
            jsonrpc: nomai_protocol::JSONRPC_VERSION.into(),
            id: Some(nomai_protocol::Id::Number(1)),
            method: "events.list".into(),
            params: Some(params),
        })
        .await;

    let result = resp.result.unwrap();
    let events = result["items"].as_array().unwrap();
    let has_more = result["has_more"].as_bool().unwrap_or(false);

    std::fs::create_dir_all(&output_dir)?;

    let mut synced = 0usize;
    let mut last_id: Option<String> = None;

    for event in events {
        let event_type = event["type"].as_str().unwrap_or("");
        let payload = &event["payload"];
        let id = payload["id"].as_str().unwrap_or("unknown");
        let title = payload["title"].as_str().unwrap_or("untitled");

        match event_type {
            "entry.created" | "entry.updated" => {
                let body = payload["body"].as_str().unwrap_or("");
                let file_path = format!("{output_dir}/{id}.md");
                std::fs::write(&file_path, format!("# {title}\n\n{body}"))?;
                synced += 1;
            }
            "entry.deleted" => {
                let file_path = format!("{output_dir}/{id}.md");
                let _ = std::fs::remove_file(&file_path);
                synced += 1;
            }
            _ => {} // skip link/chunk events for this demo
        }
        last_id = event["id"].as_str().map(|s| s.to_string());
    }

    // Save cursor
    if let Some(ref id) = last_id {
        std::fs::write(&cursor_file, id)?;
    }

    println!("Synced {synced} events (has_more={has_more})");
    match &last_id {
        Some(id) => println!("Cursor updated to {id}"),
        None => println!("No new events"),
    }
    Ok(())
}
