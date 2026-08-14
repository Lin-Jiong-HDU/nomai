.PHONY: all build debug test fmt check lint clean doc run install help

CARGO := cargo

# --- Build ---

all: build

build: ## Release build
	$(CARGO) build --release

debug: ## Debug build (fast, unoptimized)
	$(CARGO) build

# --- Test ---

test: ## Run all tests
	$(CARGO) test

test-release: ## Run tests in release mode
	$(CARGO) test --release

# --- Format & Lint ---

fmt: ## Format all Rust sources
	$(CARGO) fmt

check: ## CI gate: fmt-check + clippy (deny warnings)
	$(CARGO) fmt --check
	$(CARGO) clippy -- -D warnings

lint: ## Run clippy with warnings
	$(CARGO) clippy

# --- Docs ---

doc: ## Build and open API docs
	$(CARGO) doc --no-deps --open

# --- Run ---

run: ## Run the daemon (release)
	$(CARGO) run --release

dev: ## Run the daemon (debug)
	$(CARGO) run

# --- Install ---

install: build ## Install nomai-daemon to ~/.cargo/bin
	$(CARGO) install --path crates/daemon

# --- Clean ---

clean: ## Remove build artifacts
	$(CARGO) clean

# --- Help ---

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
