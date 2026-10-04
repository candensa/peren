# Changelog

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
