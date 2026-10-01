# Verify a release

A Peren release publishes the runtime binary, a checksum manifest, a release manifest, an SPDX SBOM, a Sigstore bundle and a signed container image.

Verify the local artifacts before installing them:

```sh
sha256sum --check checksums.txt
cosign verify-blob \
  --bundle peren.bundle \
  --certificate-identity-regexp 'https://github.com/candensa/peren/.github/workflows/release.yml@refs/tags/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  peren
```

Verify the container image before running it:

```sh
cosign verify \
  --certificate-identity-regexp 'https://github.com/candensa/peren/.github/workflows/release.yml@refs/tags/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/candensa/peren:<tag>
```

Run `peren config validate /etc/peren/config.toml` after installation and before enabling the service.
