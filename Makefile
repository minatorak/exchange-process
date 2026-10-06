CONFIG ?= config.toml
ENV_FILE ?= .env.local

.DEFAULT_GOAL := help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

fmt: ## Format all sources
	cargo fmt --all

fmt-check: ## Verify formatting
	cargo fmt --all -- --check

check: ## Type-check all targets
	cargo check --all-targets --locked

lint: ## Clippy with warnings as errors
	cargo clippy --all-targets --locked -- -D warnings

clippy: lint ## Alias of lint

test: ## Run all tests
	cargo test --locked

build: ## Release build
	cargo build --release --locked

run: ## Run locally (requires CONFIG and ENV_FILE; loads .env.local)
	@[ -f $(CONFIG) ] || { echo "$(CONFIG) not found" >&2; exit 1; }
	@[ -f $(ENV_FILE) ] || { echo "$(ENV_FILE) not found - copy .env.example to $(ENV_FILE), then fill it" >&2; exit 1; }
	set -a && . ./$(ENV_FILE) && set +a && EXCHANGE_PROCESS_CONFIG=$(CONFIG) cargo run --locked

image: ## Build localhost/exchange-process:dev
	@ENGINE="$(ENGINE)" ENV="$(ENV)" bash scripts/build-image.sh
