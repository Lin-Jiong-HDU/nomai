# nomai

**A Nomai-inspired personal knowledge core.** Storage, indexing, and retrieval primitives over JSON-RPC on stdio — bring your own UI.

> Named after the Nomai from *Outer Wilds* — an alien race who wove a network of knowledge across their star system. nomai the project aims to be the substrate on which your knowledge tools are built, not the tool itself.

## What it is

nomai is a single-binary daemon that stores knowledge entries and exposes four **primitives** — Entry, Links, Events, Chunks — through a JSON-RPC 2.0 interface over NDJSON/stdio. Clients (TUI, web UI, CLI tools, sync agents) connect by piping JSON-RPC requests to the daemon's stdin and reading responses from stdout.

The core is deliberately mechanism, not policy: it stores, indexes, and emits events. It does not impose a specific RAG strategy, sync target, or schema. You compose those on top.

## Status

Early alpha. API surface is stabilizing but may change before 1.0. Currently single-user, single-process, single SQLite file.

## The four primitives

| Primitive | What it does | Typical use |
|---|---|---|
| **Entry** | Markdown note with tags + free-form attrs | The atomic unit of knowledge |
| **Links** | Directed edges between entries | GraphRAG, backlinks, knowledge graph |
| **Events** | Append-only mutation log | Async embedding retry, external sync, audit |
| **Chunks** | Entry split into N embeddable pieces | Long-document RAG, fine-grained retrieval |

## Quick start

```fish
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

# Set API keys (fish example)
set -Ux GLM_API_KEY "..."
set -Ux NOMAI_LLM_API_KEY "..."

# First RPC
echo '{"jsonrpc":"2.0","id":1,"method":"entry.create","params":{"title":"Hello","body":"world"}}' \
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
│  │ Service Layer                            │   │
│  │   EntryService / LinkService /           │   │
│  │   EventService / ChunkService            │   │
│  ├──────────────────────────────────────────┤   │
│  │ Provider Layer (traits)                  │   │
│  │   EmbeddingProvider / LlmProvider        │   │
│  ├──────────────────────────────────────────┤   │
│  │ Storage: SQLite + FTS5 + sqlite-vec      │   │
│  └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

**Lib mode**: `nomai-core` is a plain Rust crate. If you don't want the daemon overhead, depend on it directly and call `EntryService` / `LinkService` / etc. from your own binary.

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
└── docs/
    └── guide.md     # Developer guide
```

## License

TBD (early stage — pick before public release).
