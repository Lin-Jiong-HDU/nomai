# nomai

**A substrate for personal knowledge.** Typed blocks + links + events + chunks, exposed as JSON-RPC primitives over stdio — build your RAG, LLM wiki, or agent memory on top.

> Named after the Nomai from _Outer Wilds_ — an alien race who wove a network of knowledge across their star system. nomai aims to be the substrate on which your knowledge tools are built, not the tool itself.

## What it is

nomai is a single-binary daemon that stores knowledge entries on the file system (one directory per entry, holding a typed-blocks `.nomai` file plus optional attachments). It exposes six **primitives** — Entry, Block, Links, Events, Chunks, Conversation — through a JSON-RPC 2.0 interface over NDJSON/stdio. Clients (TUI, web UI, CLI tools, sync agents) connect by piping JSON-RPC requests to the daemon's stdin and reading responses from stdout.

The core is deliberately mechanism, not policy: it stores, indexes, and emits events. It does not impose a specific RAG strategy, sync target, or schema. You compose those on top.

## Status

Early alpha. API surface is stabilizing but may change before 1.0. Currently single-user, single-process, single SQLite file.

## Download a release

Prebuilt `nomai-daemon` binaries for Linux, macOS, and Windows are attached to the [latest GitHub Release](https://github.com/Lin-Jiong-HDU/nomai/releases/latest). Choose the archive matching your operating system and CPU architecture. Asset names include the Rust target triple, for example `x86_64-unknown-linux-gnu` or `aarch64-apple-darwin`.

On Linux or macOS, extract the archive and put the binary somewhere on your `PATH`:

```bash
VERSION="v0.4.5"
TARGET="x86_64-unknown-linux-gnu" # or aarch64-apple-darwin, etc.
curl -LO "https://github.com/Lin-Jiong-HDU/nomai/releases/download/${VERSION}/nomai-daemon-${VERSION}-${TARGET}.tar.gz"
tar -xzf "nomai-daemon-${VERSION}-${TARGET}.tar.gz"
install -Dm755 "nomai-daemon-${VERSION}-${TARGET}" "$HOME/.local/bin/nomai-daemon"
```

On Windows, download the `x86_64-pc-windows-msvc.zip` archive, extract `nomai-daemon.exe`, and add its directory to `PATH`.

## The six primitives

| Primitive        | What it does                                    | Typical use                                 |
| ---------------- | ----------------------------------------------- | ------------------------------------------- |
| **Entry**        | Markdown knowledge note split into typed blocks | The atomic unit of knowledge                |
| **Block**        | Typed semantic block within an entry            | Structured notes — claim, evidence, source  |
| **Links**        | Directed edges between entries                  | GraphRAG, backlinks, knowledge graph        |
| **Events**       | Append-only mutation log                        | Async embedding retry, external sync, audit |
| **Chunks**       | Block-derived pieces for embedding              | Long-document RAG, fine-grained retrieval   |
| **Conversation** | Turn-by-turn agent dialogue storage             | Agent session history, chat logs, memory    |

**Retrieval:** `search.hybrid` fuses FTS5 BM25 and vector cosine similarity via Reciprocal Rank Fusion. `search.fulltext` and `search.semantic` are also available individually. An optional `rewrite: "expand"` parameter resolves pronouns before search. `rerank.rerank` provides LLM-based post-retrieval relevance scoring.

## Quick start

```bash
# Build
cargo build --release --bin nomai-daemon

# Configure (one-time) — see docs/reference.md for all options
mkdir -p ~/.config/nomai
cat > ~/.config/nomai/config.toml <<EOF
[embedding]
base_url    = "https://your-embedding-endpoint/v1"
api_key_env = "NOMAI_EMBEDDING_API_KEY"
model       = "your-embedding-model"
dim         = 1024
EOF

export NOMAI_EMBEDDING_API_KEY="..."

# First RPC
echo '{"jsonrpc":"2.0","id":1,"method":"entry.create","params":{"title":"Hello","blocks":[{"type":"note","text":"world"}]}}' \
  | ./target/release/nomai-daemon
```

## Documentation

- **[docs/guide.md](docs/guide.md)** — Concepts: transport, the five primitives, storage model, and the development benchmark workflow.
- **[docs/reference.md](docs/reference.md)** — Full RPC reference, benchmark methods, error codes, and configuration.
- **[docs/lib.md](docs/lib.md)** — Embedding nomai as a Rust library (lib mode, custom RPCs).

Examples live in `crates/daemon/examples/` — RAG, GraphRAG, custom RPCs, incremental sync, and more. Run with `cargo run --example <name>`.

## Project layout

```
nomai/
├── crates/
│   ├── protocol/    # JSON-RPC types (no logic)
│   ├── core/        # Services + storage (pure lib)
│   ├── providers/   # EmbeddingProvider / LlmProvider / Reranker trait + OpenAI impl
│   └── daemon/      # Binary: stdio loop + RPC dispatch
├── hooks/           # Git hooks (version-controlled)
│   └── pre-commit   # Runs `cargo fmt --check` on staged .rs files
└── docs/
    ├── guide.md       # Concepts: transport, primitives, storage model
    ├── reference.md   # Full RPC + error + config reference
    └── lib.md         # Lib mode + custom RPCs
```

## Development

**After clone**, enable the pre-commit hook (one-time):

```fish
git config core.hooksPath hooks
```

The hook runs `cargo fmt --check` whenever `.rs` files are staged.

Before pushing: `cargo fmt && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`.

## License

[Apache-2.0](LICENSE)
