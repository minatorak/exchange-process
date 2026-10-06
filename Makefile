APP_PACKAGE := exchange-process-app
ENGINE ?=
ENV ?= dev

.DEFAULT_GOAL := help

.PHONY: help run fmt fmt-check lint check test clippy build image

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

run: ## Run the service with config.toml + .env.local
	cargo run -p $(APP_PACKAGE)

fmt: ## Format all crates
	cargo fmt --all

fmt-check: ## Fail if any crate is unformatted
	cargo fmt --all -- --check

check: ## Type-check the workspace
	cargo check --workspace --all-targets --locked

test: ## Run all tests
	cargo test --workspace --locked

lint: ## Lint the workspace (clippy, warnings are errors)
	cargo clippy --workspace --all-targets --locked -- -D warnings

clippy: lint ## Alias of lint

build: ## Build the workspace
	cargo build --workspace --locked

image: ## Build localhost/exchange-process:dev (override ENGINE and ENV)
	@ENGINE="$(ENGINE)" ENV="$(ENV)" bash scripts/build-image.sh
