# Release

Peren releases are produced by `.github/workflows/release.yml` from a tagged commit.

Before tagging, run:

```sh
make check
```

`make check` verifies formatting, Clippy, the Rust workspace tests, examples, and the Vitest package checks.

For a local optimized binary build, run:

```sh
make release
```

The published release workflow builds every supported target, assembles release assets, writes checksums and a manifest, publishes the container image, signs artifacts, attaches provenance, and creates the GitHub release.

Do not describe ad hoc local scripts as the release process. If a release check is required, keep it as a maintained Makefile target or as an explicit step in `.github/workflows/release.yml`.
