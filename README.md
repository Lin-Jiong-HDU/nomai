# nomai

**A Nomai-inspired personal knowledge core.** Storage, indexing, and retrieval primitives over JSON-RPC on stdio — bring your own UI.

> Named after the Nomai from _Outer Wilds_ — an alien race who wove a network of knowledge across their star system. nomai the project aims to be the substrate on which your knowledge tools are built, not the tool itself.

## What it is

nomai is a single-binary daemon that stores knowledge entries on the file system (one directory per entry, holding a typed-blocks `.nomai` file plus optional source attachments) and exposes five **primitives** — Entry, Block, Links, Events, Chunks — through a JSON-RPC 2.0 interface over NDJSON/stdio. Clients (TUI, web UI, CLI tools, sync agents) connect by piping JSON-RPC requests to the daemon's stdin and reading responses from stdout.

The core is deliberately mechanism, not policy: it stores, indexes, and emits events. It does not impose a specific RAG strategy, sync target, or schema. You compose those on top.

## Status

Early alpha. API surface is stabilizing but may change before 1.0. Currently single-user, single-process, single SQLite file.

## Key features

- **5 primitives**: Entry / Block / Links / Events / Chunks — composable building blocks
- **Blocks primitive**: each entry is composed of typed blocks (`claim` / `evidence` / `question` / `source` / `note` / `connection`) — see `.nomai` format
- **File-system as source of truth**: every entry has a `.nomai` file on disk; the SQLite index is derived and can be rebuilt via `index.rebuild`
- **Batch RPC**: `$ref` inter-op references + atomic transactions — compose multi-step workflows in one request
- **MCP server**: native Model Context Protocol compatibility — Claude Desktop / Cursor / any MCP client can connect directly
- **Plugin registry**: `RpcHandler` trait + `register_handler` — add custom RPCs without forking
- **Embedding cache**: transparent `CachedEmbedder` wrapper persists `(model, blake3(body)) → embedding` in SQLite — repeated bodies skip the network API call entirely (zero invalidate logic; embeddings are deterministic)
- **Search results cache**: transparent in-memory cache for `search.semantic` / `search.fulltext` results — same query twice hits the cache, skips the FTS5/KNN work. Generation-based invalidation: every `entry.*` / `block.*` mutation bumps the cache generation atomically, so cached results are always consistent with current state.
- **lib + daemon dual mode**: embed `nomai-core` directly, or run `nomai-daemon` as a stdio service

## The five primitives

| Primitive  | What it does                                   | Typical use                                 |
| ---------- | ---------------------------------------------- | ------------------------------------------- |
| **Entry**  | Markdown knowledge note split into typed blocks | The atomic unit of knowledge                |
| **Block**  | Typed semantic block within an entry           | Structured notes — claim, evidence, source  |
| **Links**  | Directed edges between entries                 | GraphRAG, backlinks, knowledge graph        |
| **Events** | Append-only mutation log                       | Async embedding retry, external sync, audit |
| **Chunks** | Block-derived pieces for embedding             | Long-document RAG, fine-grained retrieval   |

## Quick start

```bash
# Build
cargo build --release --bin nomai-daemon

# Configure (one-time)
mkdir -p ~/.config/nomai ~/.local/share/nomai
cat > ~/.config/nomai/config.toml <<EOF
[embedding]
base_url    = "https://open.bigmodel.cn/api/paas/v4"
api_key_env = "GLM_API_KEY"
model       = "embedding-3"
dim         = 2048

[llm]
base_url    = "http://your-llm-endpoint/v1"
api_key_env = "NOMAI_LLM_API_KEY"
model       = "your-model"
EOF

# Set API keys
# fish:   set -Ux GLM_API_KEY "..."
# bash/zsh:
export GLM_API_KEY="..."
export NOMAI_LLM_API_KEY="..."

# First RPC
echo '{"jsonrpc":"2.0","id":1,"method":"entry.create","params":{"title":"Hello","blocks":[{"type":"note","text":"world"}]}}' \
  | ./target/release/nomai-daemon
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Your app (TUI / Web / CLI / sync agent / ...)  │
└────────────────────┬────────────────────────────┘
                     │ JSON-RPC 2.0 over NDJSON/stdio
┌────────────────────▼────────────────────────────┐
│              nomai-daemon (this repo)            │
│  ┌──────────────────────────────────────────┐   │
│  │ RPC Handlers (entry/block/link/.../*)    │   │
│  ├──────────────────────────────────────────┤   │
│  │ Service Layer                            │   │
│  │   EntryService / BlockService /          │   │
│  │   LinkService / EventService /           │   │
│  │   ChunkService                           │   │
│  ├──────────────────────────────────────────┤   │
│  │ Provider Layer (traits)                  │   │
│  │   EmbeddingProvider / LlmProvider        │   │
│  ├──────────────────────────────────────────┤   │
│  │ Storage: FS (source of truth) +          │   │
│  │         SQLite (derived index)           │   │
│  └──────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────┘
                     │
            ┌────────┴─────────┐
            ▼                  ▼
   ┌─────────────────┐  ┌──────────────────┐
   │   File System   │  │  SQLite + FTS5   │
   │                 │  │  + sqlite-vec    │
   │  knowledge_root │  │                  │
   │  └── entries/   │  │  (derived,       │
   │      └── <id>/  │  │   droppable,     │
   │          ├── *.nomai  rebuildable)    │
   │          └── *.pdf │                  │
   └─────────────────┘  └──────────────────┘
```

The file system is the source of truth — every entry has a `.nomai` file (typed blocks + metadata) on disk, plus any source attachments in the same directory. SQLite holds the derived index (FTS5 full-text, sqlite-vec chunk embeddings) and can be dropped and rebuilt via `index.rebuild` if it drifts.

**Lib mode**: `nomai-core` is a plain Rust crate. If you don't want the daemon overhead, depend on it directly and call `EntryService` / `BlockService` / `LinkService` / etc. from your own binary. Or use `nomai-daemon`'s `Daemon::from_services()` constructor to build a full daemon (with RPC dispatch + MCP + batch) in-process, and `register_handler()` to add custom RPCs.

## Examples

```
crates/daemon/examples/
├── rag.rs                # Naive RAG via lib API (search + LLM)
├── custom_rpc.rs         # Register a custom "stats" RPC
├── import_markdown.rs    # Batch import: paragraphs → entry blocks
├── graph_rag.rs          # GraphRAG: search → link.neighbors → LLM
├── events_sync.rs        # Incremental sync: events.list → .md files
├── block_lifecycle.rs    # Block primitive: append + update + delete
└── index_management.rs   # FS drift detection + index.sync / rebuild
```

Run any example: `cargo run --example <name>`

## Migrations (Spec 6 — breaking)

> **If you have a database from before 2026-06-23, the daemon will run migrations automatically on next boot. These migrations are destructive and irreversible.**

- **V7**: drops `entries.body` (entries no longer have a single body field — they have typed blocks instead)
- **V8**: drops `vec_embeddings` (per-entry embeddings retired), `fts_entries` (per-entry FTS retired, replaced by per-block `fts_blocks`), and `chunks.entry_id` (chunks are now block-addressed)

After these migrations run, existing entries without typed blocks will have empty `.nomai` files. To regenerate from current state:

1. Stop the daemon
2. Back up `db.sqlite` (in case you need to roll back)
3. Start the daemon — migrations V6→V9 run automatically
4. Call `system.export_to_fs` to generate `.nomai` files for entries missing them
5. Call `index.rebuild` to re-derive chunks + FTS + embeddings from the `.nomai` files

If your `config.embedding.dim` differs from 1536 (the daemon default), the daemon reconciles `vec_chunk_embeddings` automatically at boot — existing chunk embeddings are lost but re-populate from `emb_cache` on the next `search.semantic` (zero API calls for unchanged bodies).

## Documentation

- **[docs/guide.md](docs/guide.md)** — Concepts, full RPC reference, error codes, configuration, and usage patterns.

## Project layout

```
nomai/
├── crates/
│   ├── protocol/    # JSON-RPC types (no logic)
│   ├── core/        # Services + storage (pure lib)
│   ├── providers/   # EmbeddingProvider / LlmProvider trait + OpenAI impl
│   └── daemon/      # Binary: stdio loop + RPC dispatch
├── hooks/           # Git hooks (version-controlled)
│   └── pre-commit   # Runs `cargo fmt --check` on staged .rs files
└── docs/
    └── guide.md     # Developer guide
```

## Development

**After clone**, enable the pre-commit hook (one-time):

```fish
git config core.hooksPath hooks
```

The hook runs `cargo fmt --check` whenever `.rs` files are staged, blocking the commit if code is not rustfmt-clean. Bypass once with `git commit --no-verify`.

Before pushing: `cargo fmt && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`.

## License

[Apache-2.0](LICENSE) — see `LICENSE` for the full text.
