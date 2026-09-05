# nomai

A local-first memory substrate for agents and knowledge tools, exposed as JSON-RPC over stdio.

## Install

Download a prebuilt `nomai-daemon` binary for Linux, macOS, or Windows from the [latest release](https://github.com/Lin-Jiong-HDU/nomai/releases/latest).

To build from source, use Rust 1.88 or newer:

```bash
cargo build --release --bin nomai-daemon
```

## Quick start

Create `~/.config/nomai/config.toml`:

```toml
[embedding]
base_url = "https://your-embedding-endpoint/v1"
api_key_env = "NOMAI_EMBEDDING_API_KEY"
model = "your-embedding-model"
dim = 1024
```

Set the API key and send a request:

```bash
export NOMAI_EMBEDDING_API_KEY="..."

echo '{"jsonrpc":"2.0","id":1,"method":"entry.create","params":{"title":"Hello","blocks":[{"type":"note","text":"world"}]}}' \
  | nomai-daemon
```

## Documentation

- [Guide](docs/guide.md) — concepts, storage, retrieval, and sync
- [Reference](docs/reference.md) — configuration, RPC methods, and errors
- [Library usage](docs/lib.md) — embedding nomai in Rust
- [Examples](crates/daemon/examples) — RAG, GraphRAG, custom RPCs, and sync

## Development

```bash
cargo fmt --all -- --check
cargo +1.88.0 clippy --workspace --all-targets -- -D warnings -A clippy::uninlined-format-args
cargo test --workspace --all-targets
```

## License

[Apache-2.0](LICENSE)
