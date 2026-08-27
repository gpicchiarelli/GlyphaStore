# Wave 5 (L7) — release, portability, supply-chain residuals

Status: living honesty ledger (not a gate closure)
Applies to: C ABI cross-release, wire/disk N↔N−1, BSD packages, signing/SLSA/SBOM, sealed publish
Owner: L7 / release maintainers
Last reviewed: 2026-08-27

Claim ceiling remains **architectural prototype**. Producers and validators may exist without
retained tagged evidence; absence of evidence is a hard residual, not a waiver.

## C ABI v1 — permanent fixture scaffolding

| Layer | Status |
| --- | --- |
| Spec + ADR 0038 + `ABI_VERSION` | Present |
| Symbol allowlist / layout probes / installed pure-C consumer | Present (`ci.yml` `install-consumer`) |
| Deterministic consumer packager + fixture schema/validator | Present |
| Same-run `abi-compatibility-evidence` producer | Present; fail-closed without prior attested release |
| Permanent old-binary × new-library row from a real tag | **Blocked** — no complete official ABI-1 release asset retained yet |

First-tag runbook (do not invent digests):

1. Cut annotated `vX.Y.Z` only when other release prerequisites are intentional residuals or closed.
2. Candidate packages `glyphastore-abi-v1-consumer-…tar.xz` via `scripts/package-abi-consumer.py`.
3. Retain that asset on the GitHub Release; optionally record a pointer under
   `engineering/evidence/release/` (never hand-written SHA substitutes).
4. The **next** tag's `abi-compatibility-evidence` job selects that prior release and runs both
   library directions through `scripts/test-installed-abi-compat.sh`.

Until step 4 succeeds with retained logs, matrix row `ABI-C1-N-MINUS-1` stays `not_promised`.

## Wire N↔N−1 and disk fixtures

| Kind | Present | Residual |
| --- | --- | --- |
| In-tree golden codecs | `tests/fixtures/*.hex` | Not cross-release proof |
| Released codec harness | `tests/fixtures/released/self-v1/` + `release-compat.yml` | Permanent `<semver>/` trees absent |
| Complete Store drops | `tests/fixtures/released-stores/` (README only) | No `<semver>/` baseline |
| Wire sealed client producer | `package-wire-client.py` + release job | No prior attested client/server bytes |

Fail-closed producers refuse same-build substitutes. Documented in
[`n-n1-matrix.yaml`](../../engineering/compatibility/n-n1-matrix.yaml) and
[`release-evidence.md`](release-evidence.md).

## FreeBSD / OpenBSD

| Signal | What it proves | What it does **not** prove |
| --- | --- | --- |
| `freebsd.yml` / `openbsd-libressl.yml` | Native build + tests (+ ABI install smoke) | Ports/pkg lifecycle |
| `packaging/{freebsd,openbsd}/` | Structural reference ports | Official ports acceptance or package bytes |
| `scripts/test-freebsd-package-lifecycle.sh` | Same-run producer (blocked on account markers) | Retained tagged package evidence |
| `scripts/test-openbsd-package-lifecycle.sh` | Same-run producer (blocked on account markers) | Retained tagged package evidence |

`PORTS_ACCOUNT_REGISTERED` markers are deliberately absent until upstream ports allocation exists.
Do not commit circular `distinfo` into the source archive.

## Supply chain and sealed publish

- Actions: 40-char SHA pins (`validate_actions_pins.py`) including Dependabot SHA-discipline check
  against `.github/dependabot.yml`.
- Publish (`release.yml`): re-verifies seal/checksums/attestation, **refuses an existing release
  tag**, never rebuilds, never `--clobber`s assets — different bytes require a new version.
- Least privilege: candidate/verify default `contents: read`; publish alone gets `contents: write`
  behind the protected `release` Environment; signing jobs elevate `id-token` / `attestations` only
  when needed.
- Overlapping PR vs tag scans (Trivy/gitleaks in `supply-chain-scan.yml` and tag
  `security-supply-chain`) are intentional: PR regression gate ≠ tag-retained matrix evidence.
  Do not prune the tag path while `security-matrix-evidence` requires those logs.

## Signing / SLSA / SBOM

Implemented paths (Cosign keyless, SPDX bind-sbom, optional GitHub attestations) are **not**
retained tagged evidence until a successful tag deposits artifacts and pointers under
[`engineering/evidence/release/`](../../engineering/evidence/release/README.md). Project GPG and
full SLSA Build L3 remain explicit residuals.

## Correctness gates vs performance gates

Correctness / portability / distribution gates (`CI`, `Assurance`, FreeBSD/OpenBSD, install-consumer,
release sealing) stay independent of absolute performance budgets and hard-pinned scaling
([evidence-taxonomy.md](../assurance/evidence-taxonomy.md)). Wave 6 hardware/`ACCETTATA` work does
not reopen Wave 5 sealing residuals, and Wave 5 must not absorb absolute perf claims from macOS
`local` benches.

## Related

- [Release checklist](../assurance/release-checklist.md)
- [Artifact delivery](artifact-delivery.md)
- [BSD packaging](bsd-packaging.md)
- [Debt remediation lanes](../assurance/debt-remediation-lanes.md)
