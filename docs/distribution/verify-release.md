# Verifying a GlyphaStore release

These commands describe the future verified release layout. They do not imply that a complete
release currently exists.

Download all assets into one directory, then verify the sorted checksum manifest:

```sh
sha256sum -c SHA256SUMS
```

Verify the transitive file seal with the version-matched repository tool:

```sh
python3 engineering/tools/release_bundle.py verify-seal \
  --directory . --seal verified-seal.json
python3 engineering/tools/release_bundle.py validate-manifest --directory .
python3 engineering/tools/release_bundle.py validate-release-policy --directory .
python3 engineering/tools/release_bundle.py validate-sbom ./*.spdx.json
```

Verify provenance online while constraining repository and signer:

```sh
gh attestation verify verified-seal.json \
  --repo gpicchiarelli/GlyphaStore \
  --bundle verified-seal.sigstore.json \
  --signer-workflow https://github.com/gpicchiarelli/GlyphaStore/.github/workflows/release.yml
```

For offline verification, first obtain `trusted_root.jsonl` through a separately trusted channel,
then add `--custom-trusted-root trusted_root.jsonl`. The bundle proves the seal; the seal proves the
exact digest and size of every manifest/checksum/payload file. Inspect `build-metadata.json` for the
tag, commit, toolchain, target, TLS backend, build options, ABI, wire, and persistence versions.
Each SPDX document must describe exactly one GlyphaStore root package whose SHA-256 matches the
adjacent artifact; it is not sufficient for the JSON to parse or merely enumerate dependencies.

Do not trust an asset name alone. Do not install if any checksum, seal, policy, identity, SBOM, or
attestation check fails.
