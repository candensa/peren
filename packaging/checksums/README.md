# Release checksums

The GitHub release workflow builds each supported target, writes per-target checksum files, assembles `checksums.txt`, writes `manifest.json`, attaches provenance, and publishes the release assets.

Local maintainers can run:

```sh
make release
```

to build the release binary for the current host. The published release artifact set is produced by `.github/workflows/release.yml`.
