-include .env
export

.PHONY: bootstrap fmt fmt-check lint test check build release vitest examples clean

bootstrap:
	node crates/runtime/build.mjs

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace --all-features

check: bootstrap fmt-check lint test examples vitest

build:
	cargo build --workspace --all-features

release:
	cargo build --release --locked -p peren


vitest:
	npm --prefix packaging/vitest run typecheck
	npm --prefix packaging/vitest test
	cd packaging/vitest && npm pack --dry-run

examples: build
	@set -e; \
	export OPENAI_API_KEY="$${OPENAI_API_KEY:-peren-example-local-secret}"; \
	export ANTHROPIC_API_KEY="$${ANTHROPIC_API_KEY:-peren-example-local-secret}"; \
	export GEMINI_API_KEY="$${GEMINI_API_KEY:-peren-example-local-secret}"; \
	export CLOUDFLARE_ACCOUNT_ID="$${CLOUDFLARE_ACCOUNT_ID:-peren-example-account}"; \
	export CLOUDFLARE_API_TOKEN="$${CLOUDFLARE_API_TOKEN:-peren-example-local-secret}"; \
	export QDRANT_API_KEY="$${QDRANT_API_KEY:-peren-example-local-secret}"; \
	export PINECONE_API_KEY="$${PINECONE_API_KEY:-peren-example-local-secret}"; \
	export WEAVIATE_API_KEY="$${WEAVIATE_API_KEY:-peren-example-local-secret}"; \
	export VECTOR_API_TOKEN="$${VECTOR_API_TOKEN:-peren-example-local-secret}"; \
	export TURSO_DATABASE_URL="$${TURSO_DATABASE_URL:-libsql://example.turso.io}"; \
	export TURSO_AUTH_TOKEN="$${TURSO_AUTH_TOKEN:-peren-example-local-secret}"; \
	export REDIS_URL="$${REDIS_URL:-redis://localhost:6379}"; \
	export KV_ACCESS_KEY_ID="$${KV_ACCESS_KEY_ID:-peren-example-local-secret}"; \
	export KV_SECRET_ACCESS_KEY="$${KV_SECRET_ACCESS_KEY:-peren-example-local-secret}"; \
	export CACHE_ACCESS_KEY_ID="$${CACHE_ACCESS_KEY_ID:-peren-example-local-secret}"; \
	export CACHE_SECRET_ACCESS_KEY="$${CACHE_SECRET_ACCESS_KEY:-peren-example-local-secret}"; \
	export R2_ACCESS_KEY_ID="$${R2_ACCESS_KEY_ID:-peren-example-local-secret}"; \
	export R2_SECRET_ACCESS_KEY="$${R2_SECRET_ACCESS_KEY:-peren-example-local-secret}"; \
	for config in examples/javascript/*/*.toml; do \
		target/debug/peren diagnose --storage-test --read-only "$$config" >/dev/null; \
		echo "ok $$config"; \
	done

clean:
	cargo clean
	node -e 'for (const path of ["dist", "node_modules", "packaging/vitest/node_modules"]) require("node:fs").rmSync(path, { recursive: true, force: true })'
