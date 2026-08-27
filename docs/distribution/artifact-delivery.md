# Artifact delivery architecture

Status: normative for future tagged releases
Claim ceiling: architectural prototype until every linked release gate has retained evidence

GlyphaStore releases follow one unidirectional path:

```text
annotated vX.Y.Z tag
        |
        v
Candidate: build once -> identify -> SPDX SBOM -> candidate seal
        |
        v
Verify: exact admission -> test installed bytes -> import evidence/packages
        |          -> verify imported closed set -> compatibility policy
        |          -> release manifest -> SHA256SUMS -> verified seal
        |          -> OIDC/Sigstore attestation of the verified seal
        v
Publish: download -> verify exact seal/checksums/manifest/attestation
        |          -> refuse an existing release
        v
GitHub prerelease: publish the same files, without rebuilding
```

`.github/workflows/release-candidate.yml` is the only workflow allowed to compile bytes that can
enter the candidate. `.github/workflows/release.yml` orchestrates evidence, Verify and Publish. Its
isolated reproducibility job may compile a second comparison tree, but those bytes can only enter a
diagnostic CI artifact and are never imported into the candidate. Verify may compile an external
consumer against an extracted installed prefix; it must never compile or relink the engine.
Publish must not invoke CMake, a compiler, a linker, a package builder, or an archive builder.

## Source identity

`engineering/tools/release_identity.py` requires all of the following:

- strict `VERSION` and `ABI_VERSION` authorities;
- an annotated `vX.Y.Z` tag matching `VERSION` exactly;
- checkout `HEAD` equal to the tag target;
- no tracked working-tree changes;
- a positive commit timestamp used as `SOURCE_DATE_EPOCH`.

`scripts/package-source-release.py` feeds that exact tag to `git archive`, normalizes ownership,
orders members, compresses deterministically, and rejects generated/unsafe/missing members. It
never archives the working tree. `scripts/package-install-prefix.py` packages a completed install
tree with normalized metadata and refuses a missing versioned C ABI.

Syft discovers packaged components, but its output is not trusted as the artifact identity.
`release_bundle.py bind-sbom` adds one authoritative GlyphaStore SPDX package whose version,
BSD-3-Clause license and SHA-256 identify the adjacent artifact, then normalizes creation time from
`SOURCE_DATE_EPOCH`. Validation rejects a missing subject, a digest/version mismatch, duplicate
package identities, a missing document-to-package relationship, or any discovered package for
which neither declared nor concluded license is resolved. Thus a syntactically valid but empty or
unbound SPDX document cannot pass the candidate gate.

`build-metadata.json` is validated before SBOM generation against a closed v1 object model. Unknown
fields, incomplete nested authorities, tag/version disagreement, invalid commit IDs, empty
toolchain/target/TLS identities, and duplicate or malformed build options fail the candidate. The
JSON Schema under `engineering/schemas/` mirrors that contract for independent tooling.

## Seals and manifests

`candidate-seal.json` is a sorted SHA-256 inventory of every candidate file. Verify rejects an
added, missing, replaced, resized, symlinked, or renamed file before executing candidate bytes and
writes `candidate-admission.json`, which binds that exact-set decision to the candidate-seal digest.
The builder also exports the seal's SHA-256 as a workflow output. Every same-run consumer checks
that external anchor before trusting the seal; the reproducibility job repeats the check after its
untrusted comparison build and immediately before reading reference bytes.
Post-build evidence and native packages may be merged only after admission. Subsequent verification
rechecks every admitted member and the admission receipt while allowing those additions; it never
redefines the admitted set. `evidence-import.json` inventories the only permitted additions. Import
rejects protected control names, symlinks, nested/non-regular inputs, duplicate names, collisions
with candidate members and empty sources. Any unrecorded addition or imported-byte drift stops
verification.
`release-manifest.json` records product/ABI/wire/persistence identities and the digest and role of
every artifact/evidence file. `SHA256SUMS` covers that manifest. `verified-seal.json` then covers the
complete augmented verification set, including the candidate seal and admission receipt. Thus the
candidate remains immutable while evidence can be added without creating a seal/evidence cycle.

The Sigstore/in-toto attestation has `verified-seal.json` as its subject. This is a transitive
commitment to every published payload digest without creating a manifest/signature cycle. The
offline bundle is `verified-seal.sigstore.json`; verification also pins repository, signer workflow,
source ref, source digest, and OIDC issuer.

## Fail-closed release policy

Publication is prohibited unless the exact candidate contains one source archive, Linux installed
archive, compiled ABI-v1 consumer, compiled wire-v2 client, FreeBSD package, and OpenBSD package,
with an SPDX 2.3 SBOM for each. It also requires
passing, same-commit machine evidence for:

- old/new C ABI compatibility;
- persistence compatibility/recovery;
- wire compatibility;
- installed SDK interoperability;
- the required security matrix;
- native FreeBSD and OpenBSD package/service lifecycles;
- an independent reproducibility comparison.

Each record follows the artifact-bound contract in
[`release-evidence.md`](release-evidence.md): required check IDs, commands and retained non-empty
logs are validated in addition to commit/version/subject digests. `validate-release-policy`
enforces these names and bindings. Installed-SDK, persistence, C-ABI, wire, security-matrix,
FreeBSD-package, OpenBSD-package, and reproducibility evidence now have same-run
producers. Persistence requires a complete Store created by a strictly older tagged release. ABI
requires the complete attested prior release plus its sealed compiled consumer and proves both
dynamic-library directions. Wire requires that same prior release plus its sealed compiled wire-v2
client and proves new/new plus both N−1 client/server directions. Neither cross-release producer
accepts a same-build substitute. Reproducibility rebuilds all four deterministic archives on a
distinct runner and requires an exact size and SHA-256 match; rebuilt bytes cannot replace candidate
bytes. The security producer executes the complete Linux sanitizer/static/CodeQL/supply-chain
matrix and inspects the SBOMs and distributed ELF from the externally anchored candidate. Both BSD
producers use the native ports/pkg stacks and are intentionally blocked until service accounts are
registered. Prior baselines and retained native packages remain absent, so the release workflow is
expected to stop before attestation/publication. See
[`wave5-l7-residuals.md`](wave5-l7-residuals.md).
Removing the gate to cut an early release is forbidden; a deliberate policy change needs an ADR,
requirements, hazards, tests, and residual-risk review.

## Immutability and permissions

Candidate and Verify have `contents: read`; only Verify receives short-lived OIDC and attestation
permissions. Publish alone receives `contents: write` and is protected by the `release` Environment.
It aborts if the GitHub Release already exists, never rebuilds candidate bytes, and never uses asset
clobbering (`--clobber`). Different bytes under the same tag name require a new version; a bad
release is withdrawn and replaced, never republished in place.

The reusable workflow is a trust-boundary-compatible design, but this repository does not claim
SLSA Build Level 3: achieving that level also requires hosting/pinning the reusable builder in an
appropriately protected trusted repository and retaining successful provenance evidence.
