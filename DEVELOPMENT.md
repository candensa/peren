# Developing Peren

This guide is for contributors working in the Peren source tree. Product installation and fleet-operation instructions belong in the public documentation.

## Prerequisites

Install:

- the Rust toolchain pinned by `rust-toolchain.toml`;
- Node.js and npm for the Vitest integration package;
- Docker for container and clean-install smoke checks;

The pinned Rust toolchain includes `rustfmt` and Clippy. Run commands from the repository root unless a section says otherwise.

Provider tests may require external credentials and may write, replace or fence provider state. Use disposable resources created for testing. Never point development or conformance checks at a production bucket, database, queue, tenant or fleet.

## Repository setup

Confirm the toolchain and fetch dependencies:

```sh
rustc --version
cargo --version
cargo fetch --locked
npm --prefix packaging/vitest ci
```

The Rust version should match `rust-toolchain.toml`. Do not update the pinned toolchain as incidental cleanup in an unrelated change.

## Build

Build the complete workspace with all features:

```sh
make build
```

The equivalent Cargo command is:

```sh
cargo build --workspace --all-features
```

Use a package-scoped build while iterating when the change does not cross crate boundaries:

```sh
cargo build -p peren-runtime
```

## Run locally from source

Inspect the CLI:

```sh
cargo run -p peren -- --help
```

The repository includes focused configurations under `examples/javascript`. That directory covers a hello Worker, quickstart vars, KV, D1, R2, cache, queues, AI, vector search, containers, assets, cron handlers, a durable counter, Wasm, outbound fetch, client mTLS, AWS SigV4, service RPC, secrets, analytics, rate limiting, workflows, images, Hyperdrive metadata, alarm dispatch, and cell storage. For example:

```sh
cargo run -p peren -- devcert certs
cargo run -p peren -- serve examples/javascript/quickstart/config.toml
```

In another terminal, call the configured public listener:

```sh
curl http://127.0.0.1:8102/
```

`devcert` creates the local certificate authority and leaf certificate expected by the example configuration. These certificates are for development only. The quickstart configuration uses memory-backed storage and does not demonstrate production durability.

## Format and lint

Format source files:

```sh
make fmt
```

Check formatting without changing files:

```sh
make fmt-check
```

Run Clippy across all targets and features with warnings denied:

```sh
make lint
```

Do not suppress a warning without explaining why the stricter behavior is inappropriate at that boundary.

## Test

Run the complete Rust workspace suite:

```sh
make test
```

Run a package while iterating:

```sh
cargo test -p peren-runtime
```

Run one integration-test target:

```sh
cargo test -p peren-runtime --test isolate
```

Run one named test only when narrowing a failure:

```sh
cargo test -p peren-node process::tests::public_listener_passes_native_d1_binding_to_worker -- --exact
```

Named internal tests are contributor tools. Do not copy them into public product documentation.

Build and type-check the Vitest integration package:

```sh
make npm-plugin
```

## Required checks before review

For ordinary code changes, run:

```sh
make check
```

`make check` verifies formatting, runs Clippy, executes the Rust workspace tests and builds and type-checks the Vitest package.

For changes to runtime behavior, persistence, replication, providers, deployment, tenancy, protocol compatibility, packaging or installation, run the relevant maintained workflow target from the current `Makefile` and record any provider-specific evidence in the pull request.

Only document targets that exist in the current Makefile. If a change requires provider-specific evidence for which the repository has no stable target, record the exact procedure in the pull request and add a maintained target before presenting that procedure as a standard workflow.

## External providers

Tests requiring live services must be opt-in and must name every required environment variable. Use a dedicated test account or disposable resource with the narrowest practical credentials.

Before running an external-provider test:

1. Read the test and identify every write, delete, lease and fencing operation.
2. Confirm the endpoint and resource name belong to a disposable environment.
3. Confirm logs and failure output do not print secret values.
4. Record the provider, region or deployment shape needed to reproduce the result.

## State, compatibility and failure behavior

Changes involving state require explicit answers to these questions:

- Which component owns the state?
- Is the state durable, replicated, reconstructable or process-local?
- What happens if a write is interrupted?
- How is a stale owner prevented from committing?
- Can an older binary read the new format?
- Can the change be rolled back safely?
- Does recovery require an operator action or migration?

Changes to configuration, persisted formats, peer protocol, CLI output or runtime APIs must identify compatibility consequences in the pull request.

## Error handling

Errors should name the failed boundary and preserve an actionable cause. Configuration and provider construction should fail before listeners accept traffic. Diagnostics may name a missing secret reference, provider, binding or configuration field, but must not print credential values.

Runtime compatibility failures should return explicit refusal errors. Do not silently fall back to memory, local files or partial behavior that appears successful.

Use panics only in tests or unreachable internal assertions where continuing would violate an invariant. Product paths should return typed errors.

## Documentation

Public product documentation lives under `docs/public/en-GB`. Update it whenever a public command, option, configuration field, API, binding, provider, default, limit, error or operational contract changes.

Public documentation must:

- use installed-product commands rather than source-tree commands;
- state prerequisites, side effects, expected output and verification;
- describe important limits and failure behavior;
- distinguish local, single-node and fleet behavior;
- contain no credentials, private endpoints, local absolute paths or planning references;

Maintainer commands and internal evidence belong in this file, pull requests or other contributor material.

## Release artifacts

Build local release artifacts with:

```sh
make release
```

The target builds the optimized binary for the current host and writes the target-specific binary, `checksums.txt`, `manifest.json` and `target.txt` under `dist`.

This local target does not by itself publish a release, generate every artifact described by the public verification guide or prove every supported target. Publishing requires the repository's release automation and a separate compatibility review.

## Pull request evidence

Describe:

- the concrete problem;
- the resulting behavior;
- compatibility, persistence and security consequences;
- failure and rollback behavior;
- the checks you ran;
- skipped checks and why they were not applicable;
- any limitation that remains.

Keep formatting churn, speculative abstractions and unrelated cleanup in separate changes.
