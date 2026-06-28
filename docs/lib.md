# nomai lib mode

How to embed nomai directly as a Rust library — either the storage primitives (`nomai-core`) or the full daemon with RPC dispatch + MCP + batch (`nomai-daemon`). For the JSON-RPC API surface, see the [reference](reference.md). For concepts, see the [developer guide](guide.md).

## Table of contents

- [Option 1: nomai-core directly (storage primitives only)](#option-1-nomai-core-directly-storage-primitives-only)
- [Option 2: nomai-daemon (full Daemon with RPC dispatch + MCP + batch)](#option-2-nomai-daemon-full-daemon-with-rpc-dispatch--mcp--batch)
- [`DaemonBuilder`](#daemonbuilder)
- [Embedding in lib mode](#embedding-in-lib-mode)
- [Custom RPCs](#custom-rpcs)
  - [How it works](#how-it-works)
  - [Accessing services](#accessing-services)
  - [Lib-mode construction](#lib-mode-construction)
  - [Example](#example)
  - [Batch composition](#batch-composition)

---

## Option 1: nomai-core directly (storage primitives only)

```toml
[dependencies]
nomai-core = { path = "..." }
```

```rust
use nomai_core::{EntryService, LinkService, EventService, ChunkService, BlockInput};

let conn = std::sync::Arc::new(std::sync::Mutex::new(
    rusqlite::Connection::open("db.sqlite")?
));
let entries = std::sync::Arc::new(EntryService::new(conn.clone())?);
let links = std::sync::Arc::new(LinkService::new(conn.clone())?);
let events = std::sync::Arc::new(EventService::new(conn.clone())?);
let chunks = std::sync::Arc::new(ChunkService::new(conn.clone())?);

let entry = entries.create(nomai_core::CreateEntry {
    title: "Hello".into(),
    blocks: vec![BlockInput {
        r#type: "note".into(),
        text: "world".into(),
        attrs: None,
    }],
    tags: None,
    attrs: None,
    source: None,
})?;
```

**Re-exported types** (Spec 8 Plan 2):

- `EntryListOrder` (replaces the old `ListOrder`) — order enum for `entry.list`: `CreatedDesc` (default) / `CreatedAsc` / `UpdatedDesc` / `UpdatedAsc`.
- `EventListOrder` (replaces the old `ListOrder`) — order enum for `events.list`: `Asc` (default) / `Desc`.
- `NomaiBlock` — alias for the parser's `Block` type (Spec 8 Plan 2 / F-lib-1), re-exported from `nomai_core` so lib-mode callers don't have to depend on the parser crate directly. Use it when reading or constructing blocks outside the service layer.

---

## Option 2: nomai-daemon (full Daemon with RPC dispatch + MCP + batch)

```toml
[dependencies]
nomai-daemon = { path = "..." }
nomai-providers = { path = "..." }
```

```rust
use nomai_daemon::daemon::Daemon;

// Construct without config.toml — pass services directly. Full signature:
// from_services(conn, content_store, embedder, llm,
//               embedding_dim, chunk_target_size, cache_model, warn_rows)
let mut daemon = Daemon::from_services(
    conn,
    content_store,
    embedder,
    llm,
    1024,    // embedding_dim
    1024,    // chunk_target_size (chars)
    "your-embedding-model",
    100_000,
)?;

// Register custom RPCs (see Custom RPCs section below)
daemon.register_handler(std::sync::Arc::new(MyHandler));

// Dispatch any RPC (batch, search, CRUD, MCP tools/call, etc.)
let resp = daemon.dispatch(req).await;
```

All four services share the same `Arc<Mutex<Connection>>`. Emission (events) still happens automatically inside each mutation method.

---

## `DaemonBuilder`

(Spec 8 Plan 2 / F-lib-2) — fluent alternative to `from_services`'s 8 positional arguments:

```rust
use nomai_daemon::DaemonBuilder;

let mut daemon = DaemonBuilder::new()
    .conn(conn)
    .content_store(content_store)
    .embedder(embedder)
    .llm(llm)
    .embedding_dim(1024)
    .chunk_target_size(1024)
    .cache_model("your-embedding-model")
    .warn_rows(100_000)
    .build()?;
```

All eight fields are required; `build()` returns `Err(CoreError::Config)` if any field is unset, with a field-specific message (e.g. `"DaemonBuilder: conn required"`). `Daemon::from_services` is kept for backward compatibility.

**RPC method constants** (Spec 8 Plan 2 / F-cache-1): `nomai_protocol::method` exposes named constants for every RPC method string, including `cache::STATS` and `cache::CLEAR`. Use them in dispatch match-arms instead of string literals to avoid typos.

See `crates/daemon/examples/` for complete working examples:

- `rag.rs` — Naive RAG via lib API
- `custom_rpc.rs` — Register a custom `stats` RPC
- `import_markdown.rs` — Batch import with $ref + chunking
- `graph_rag.rs` — GraphRAG via search + link.neighbors + LLM
- `events_sync.rs` — Incremental sync via events.list + cursor
- `block_lifecycle.rs` — Block primitive: append + update + delete
- `index_management.rs` — FS drift detection + index.sync / rebuild

---

## Embedding in lib mode

`EntryService::create` writes the entry + emits the event, but does **not** call the embedding provider (that's daemon-layer orchestration). You must call `entries.write_embedding(id, &vec)` yourself after creating. Same for `chunks.write_embedding`. The `nomai-providers` crate provides the `EmbeddingProvider` trait and an OpenAI-compatible implementation.

To get the same transparent caching behavior as the daemon, wrap your embedder in `CachedEmbedder` (also from `nomai-providers`) — see [Embedding cache](reference.md#embedding-cache) in the reference.

---

## Custom RPCs

nomai's daemon uses a plugin registry (`RpcHandler` trait + `HashMap`). You can add custom RPCs without forking the codebase.

### How it works

1. **Implement `RpcHandler`** — a trait with `method()` (returns the RPC name) and `call()` (async, receives `&Daemon` + params).
2. **Register** — call `daemon.register_handler(Arc::new(YourHandler))`.
3. **That's it** — your custom RPC appears in `tools/list` and is callable by any MCP client.

### Adding MCP descriptors

MCP clients (Claude Desktop, Cursor) read `description` and `inputSchema`
from `tools/list` to display tools and guide argument construction.
Override the default trait methods to populate them:

```rust
impl RpcHandler for YourHandler {
    fn method(&self) -> &'static str { "your.rpc" }
    fn description(&self) -> &'static str {
        "What it does. When to use it. Notable behavior."
    }
    fn input_schema(&self) -> Option<Value> {
        // Use schemars::schema_for!(YourParams).to_value() for derived
        // schemas, or hand-write JSON for full control.
        Some(schemars::schema_for!(YourParams).to_value())
    }
    async fn call(&self, ...) { ... }
}
```

Shared helpers (`ulid_param_schema()`, `empty_param_schema()`) live in
`nomai_daemon::handlers::params`.

### Accessing services

Custom handlers access the four core services via `&Daemon` accessors:

```rust
daemon.entries()  // &Arc<EntryService>
daemon.links()    // &Arc<LinkService>
daemon.events()   // &Arc<EventService>
daemon.chunks()   // &Arc<ChunkService>
daemon.cache()    // &Arc<CachedEmbedder>  (EmbeddingProvider trait + cache.stats/clear hooks)
daemon.llm()      // &Arc<dyn LlmProvider>
```

### Lib-mode construction

Use `DaemonBuilder` (preferred, see [above](#daemonbuilder)) or `Daemon::from_services(conn, content_store, embedder, llm, embedding_dim, chunk_target_size, cache_model, warn_rows)` to build a Daemon without a `config.toml` file. This is the entry point for embedding nomai into your own binary.

### Example

See `crates/daemon/examples/custom_rpc.rs` for a complete working example that:

- Builds a Daemon via `from_services` (in-memory DB, no config)
- Implements a `Stats` RPC that queries entry counts
- Registers it via `register_handler`
- Dispatches the custom RPC
- Verifies it appears in MCP `tools/list`

Run: `cargo run --example custom_rpc`

### Batch composition

The `batch` RPC lets you compose multiple mutations atomically (see [batch in the reference](reference.md#batch)). Custom handlers participate in the ecosystem but are not callable from within `batch` ops (only built-in mutations are batch-eligible).
