//! Block lifecycle demo: create entry -> append blocks -> update one -> delete one.
//!
//! Shows the typed-blocks primitive from Spec 6:
//! - Each entry holds ordered blocks (claim / evidence / question / source / note / connection)
//! - `BlockService::append` adds at the end (auto-assigns ordinal)
//! - `BlockService::update` changes text (auto re-chunks; chunks_ad cleans embeddings)
//! - `BlockService::delete` removes (cascade cleans chunks + embeddings)
//!
//! `BlockService` is the storage-layer primitive: it mutates the SQLite row +
//! chunks/embeddings but does NOT touch the `.nomai` file. The production
//! `block.*` RPC handlers (see `crates/daemon/src/handlers/block.rs`) wrap each
//! primitive with a `rerender_entry_nomai` step that re-renders the file from
//! the post-mutation entry state, keeping the FS (Spec §7.1 source-of-truth)
//! in sync with the index. This example inlines that same re-render so the
//! final `.nomai` dump reflects every mutation shown above it.
//!
//! Usage:
//!     cargo run --example block_lifecycle
//!
//! No network required. Uses an in-memory DB + a temp dir for the content store
//! (cleaned up at process exit by the OS).

use std::sync::Arc;

use rusqlite::Connection;

use nomai_core::{
    BlockInput, ContentStore, CreateEntry, EntryService, NomaiDoc,
    nomai_format_util::storage_block_to_parser_block,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize SQLite extensions (sqlite-vec) required by migrations.
    nomai_core::storage::init_sqlite_extensions();

    // Use a per-run temp dir rather than default_knowledge_root() so we never
    // pollute the user's real knowledge store by running this example.
    let knowledge_root =
        std::env::temp_dir().join(format!("nomai-block-lifecycle-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&knowledge_root)?;
    let content_store = Arc::new(ContentStore::new(knowledge_root));

    let conn = Arc::new(std::sync::Mutex::new(Connection::open_in_memory()?));
    let entries = EntryService::new(conn.clone(), content_store.clone(), 1024)?;

    println!("=== Create entry with one @claim block ===\n");
    let entry = entries.create(CreateEntry {
        title: "Heliocentrism".into(),
        blocks: vec![BlockInput {
            r#type: "claim".into(),
            text: "Earth orbits the sun.".into(),
            attrs: None,
        }],
        tags: Some(vec!["astronomy".into()]),
        attrs: None,
        source: None,
    })?;
    println!(
        "Created entry {} with {} block(s):",
        entry.id,
        entry.blocks.len()
    );
    for b in &entry.blocks {
        println!(
            "  [{}] {}: {}",
            b.ordinal,
            b.r#type,
            b.text.chars().take(60).collect::<String>()
        );
    }

    let blocks = entries.block_service().clone();

    println!("\n=== block.append: add @evidence + @question ===\n");
    let evidence = blocks.append(
        entry.id,
        "evidence".into(),
        "Kepler's laws show planetary orbits are ellipses with the sun at one focus.".into(),
        None,
    )?;
    println!(
        "Appended block ordinal={} type={}",
        evidence.ordinal, evidence.r#type
    );

    let question = blocks.append(
        entry.id,
        "question".into(),
        "Why did Ptolemy's geocentric model persist for 1400 years?".into(),
        None,
    )?;
    println!(
        "Appended block ordinal={} type={}",
        question.ordinal, question.r#type
    );
    rerender(&entries, &content_store, entry.id)?;

    println!("\n=== Entry state after appends ===");
    let after_append = entries.get(entry.id)?;
    println!("  {} blocks total", after_append.blocks.len());
    for b in &after_append.blocks {
        println!(
            "  [{}] {}: {}",
            b.ordinal,
            b.r#type,
            b.text.chars().take(60).collect::<String>()
        );
    }

    println!("\n=== block.update: refine the @claim text ===\n");
    let updated_claim = blocks.update(
        entry.blocks[0].id,
        None,
        Some("Earth orbits the sun in an elliptical path.".into()),
        None,
    )?;
    println!(
        "Updated block ordinal={} text={:?}",
        updated_claim.ordinal, updated_claim.text
    );
    rerender(&entries, &content_store, entry.id)?;

    println!("\n=== block.delete: remove the @question ===\n");
    let deleted = blocks.delete(question.id)?;
    println!("Deleted block id={} type={}", deleted.id, deleted.r#type);
    rerender(&entries, &content_store, entry.id)?;

    println!("\n=== Final entry state ===");
    let final_entry = entries.get(entry.id)?;
    println!("  {} blocks remaining", final_entry.blocks.len());
    for b in &final_entry.blocks {
        println!(
            "  [{}] {}: {}",
            b.ordinal,
            b.r#type,
            b.text.chars().take(60).collect::<String>()
        );
    }

    println!("\n=== .nomai file content ===\n");
    let doc = content_store.read_entry(entry.id)?;
    println!("{}", nomai_core::render_nomai(&doc));

    Ok(())
}

/// Re-render the entry's `.nomai` file from its current DB state.
///
/// Mirrors `rerender_entry_nomai` in `crates/daemon/src/handlers/block.rs`:
/// fetch the entry + its blocks, project to parser blocks, write the file.
/// `BlockService` itself is just the storage primitive and does not write the
/// file, so callers that want FS/index consistency must re-render after each
/// mutation (the production `block.*` handlers do this automatically).
fn rerender(
    entries: &EntryService,
    content_store: &ContentStore,
    entry_id: ulid::Ulid,
) -> Result<(), Box<dyn std::error::Error>> {
    let entry = entries.get(entry_id)?;
    let parser_blocks: Vec<_> = entry
        .blocks
        .iter()
        .map(storage_block_to_parser_block)
        .collect();
    let doc = NomaiDoc {
        format_version: 1,
        id: entry.id,
        title: entry.title,
        tags: entry.tags,
        attrs: entry.attrs.as_object().cloned().unwrap_or_default(),
        source: entry.source,
        created_at: entry.created_at,
        updated_at: entry.updated_at,
        blocks: parser_blocks,
    };
    content_store.write_entry(entry_id, &doc)?;
    Ok(())
}
