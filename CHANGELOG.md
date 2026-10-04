# Changelog

## 0.3.0 - 2026-10-05

### Added

- Added first-class Prometheus coverage for Worker dispatches, cell lifecycle activity, storage failures, and WebSocket events. [#20](https://github.com/candensa/peren/pull/20)
- Added seconds-based Prometheus duration histograms while keeping the existing millisecond metrics available for current dashboards. [#20](https://github.com/candensa/peren/pull/20)

### Changed

- Updated the Prometheus and Grafana guidance around `/metrics`, low-cardinality labels, and listener placement. [#20](https://github.com/candensa/peren/pull/20)
- Refined the Grafana dashboard to use seconds for latency panels and keep rate panels on operation units. [#20](https://github.com/candensa/peren/pull/20)

## 0.2.0 - 2026-10-04

### Added

- Cell lifecycle tracing and shared dispatch lifecycle proofing.
- Wrangler-compatible local profile support and deeper development diagnostics.
- Release preparation workflow and release metadata.

### Changed

- Hardened replica manifests, retention, pruning, and explicit cell storage erasure.
- Improved runtime parity for bindings, worker loading, service targets, WebSockets, and Durable Object startup props.
- Strengthened fleet isolation, residency placement, peer control mutations, and provider credential handling.
- Made release preparation manual so releases are cut intentionally.

### Fixed

- Bound node identity to the data directory and made first-start identity initialization atomic.
- Preserved legacy object cells and shutdown readiness through lifecycle hardening.
- Stabilized example validation by isolating per-example data directories.
