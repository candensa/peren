<p align="center">
  <img src="docs/public/assets/images/banner.png" alt="Peren" width="100%" />
</p>

# Peren

Run Workers and Durable Objects on infrastructure you control.

Peren is an open-source, self-hosted runtime for stateful Worker applications. It runs JavaScript and WebAssembly in V8 isolates, gives each stateful cell SQLite-backed storage, and provides the fleet operations needed to deploy and recover services across a VPS, bare-metal servers or Kubernetes.

Peren is built for platform engineers, infrastructure engineers, backend engineers and technical teams that want the Workers programming model while retaining control of their infrastructure, providers, release process and data boundaries.

## What Peren provides

### Worker runtime

- JavaScript, CommonJS and WebAssembly module loading in V8 isolates.
- Tested web platform APIs and selected Worker-safe Node compatibility modules.
- Explicit runtime limits for CPU time, wall-clock time, heap use, request size, subrequests and isolate admission.
- Named services, entrypoints, service bindings and request routing.

### Durable state

- Durable Object-style cells with one current owner and SQLite-backed state.
- Ownership epochs, signed leases and fencing that refuse writes from stale owners.
- Snapshot and write-ahead-log records used to recover state after node loss.
- Alarms, workflows, scheduled work and point-in-time recovery capabilities with documented limits.

### Bindings and providers

- KV, D1-compatible SQL, R2-style object storage and queues.
- Service bindings, dispatch namespaces, secrets and controlled outbound network access.
- Cache, AI, vector indexes, Hyperdrive metadata, analytics, images, module loading and containers.
- Native, local and provider-backed implementations selected through fleet configuration.

Provider choice changes durability, consistency, topology and failure behavior. Peren documents those boundaries instead of treating every provider as equivalent.

### Fleet operations

- Node join, health, drain, removal and recovery workflows.
- Verified service deployment records, gradual rollout controls and rollback.
- Backup, restore, diagnostics, storage qualification and upgrade planning.
- Tenant lifecycle, scoped credentials, secret rotation and authenticated peer identity.
- Prometheus metrics, logs, tracing and export integrations for operating a fleet.

## Compatibility is a contract

Peren does not claim complete Cloudflare Workers compatibility. Supported runtime APIs, bindings and provider behavior are recorded explicitly. Behavior that cannot be represented safely is refused instead of being silently approximated.

Before migrating an existing application, review:

- [runtime and provider compatibility](docs/public/en-GB/deployment/compatibility.mdx);
- [web platform compatibility](docs/public/en-GB/concepts/web.mdx);
- [Cloudflare migration guidance](docs/public/en-GB/deployment/cloudflare.mdx);
- [degraded operation](docs/public/en-GB/operations/degraded.mdx).

## Install

The installer supports Linux and macOS on x86-64 and ARM64:

```sh
curl -fsSL https://peren.dev/install | sh
peren --version
```

The installer places the `peren` binary in `/usr/local/bin` by default. Review the [installer source](packaging/install.sh) before running it, or download a release artifact and verify it using [the release verification guide](packaging/verify.md).

For containers, system packages and Kubernetes, follow the [installation and deployment documentation](docs/public/en-GB/start/quickstart.mdx).

## Start with the documentation

Peren has separate paths for application development and fleet operation:

- [Start Peren locally](docs/public/en-GB/start/quickstart.mdx)
- [Build a stateful Worker](docs/public/en-GB/tutorial.mdx)
- [Understand the architecture](docs/public/en-GB/start/overview.mdx)
- [Configure a fleet](docs/public/en-GB/deployment/config.mdx)
- [Explore bindings](docs/public/en-GB/bindings/overview.mdx)
- [Understand security boundaries](docs/public/en-GB/security/overview.mdx)
- [Operate degraded systems](docs/public/en-GB/operations/degraded.mdx)
- [CLI reference](docs/public/en-GB/reference/cli.mdx)

The documentation describes released product behavior. Repository build commands and maintainer checks live in [DEVELOPMENT.md](DEVELOPMENT.md).

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request. Source setup, architecture rules and repository verification commands are documented in [DEVELOPMENT.md](DEVELOPMENT.md).

## Licence

Peren is licensed under the [Apache License 2.0](LICENSE).
