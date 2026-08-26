Status: descriptive release process; does not raise claim ceiling
Applies to: tagged releases and claim packaging for GlyphaStore 0.1.x
Owner: release maintainers
Last reviewed: 2026-08-26

# Release checklist

Use this list when cutting a version tag. Completing it does **not** by itself promote the claim
ceiling above *architectural prototype*. Authority for gates:
[`engineering/gates/quality-gates.yaml`](../../engineering/gates/quality-gates.yaml).

## Pre-tag

- [ ] `LICENSE` / `NOTICE` / `THIRD_PARTY_NOTICES.md` still accurate for any new dependency
- [ ] Local: `./scripts/ci-license-check.sh`
- [ ] `CHANGELOG.md` Unreleased section reviewed and version section drafted
- [ ] Persistence / wire / API changes have matching fixtures and matrix rows
  (`engineering/compatibility/n-n1-matrix.yaml` if N↔N-1 policy changed)
- [ ] Local: `python3 engineering/tools/validate_assurance.py`
- [ ] Local: structure / Actions / compat / claims / perf budget validators (see `engineering/README.md`)
- [ ] Local: `ctest` / `glyphastore_tests` green on at least one supported host
- [ ] Sanitizer / crash suites considered for the change set (or deferred with residual note)
- [ ] No new absolute performance claims without `glyphastore-linux-perf` `pass-candidate` evidence

## Tag packaging

- [ ] Create annotated tag `vX.Y.Z`; `VERSION == X.Y.Z` and clean exact-tag checkout are mandatory
- [ ] FreeBSD/OpenBSD `distinfo`, account registration and native package evidence exist
- [ ] All eight artifact-bound release evidence JSON files and referenced logs pass
      `engineering/tools/release_evidence.py --require-ci`
- [ ] Candidate contains the deterministic compiled ABI consumer archive and its bound SPDX SBOM
- [ ] Candidate contains the deterministic compiled wire-v2 client archive and its bound SPDX SBOM
- [ ] `scripts/package-release-compatibility-artifacts.sh X.Y.Z`
- [ ] `scripts/package-release-claim.sh X.Y.Z <40-char-sha>`
- [ ] Decide whether to **commit** `tests/fixtures/released/X.Y.Z/` as a permanent N−1 drop
- [ ] Package and commit `tests/fixtures/released-stores/X.Y.Z/` from the stopped installed tagged
      daemon Store so the next version has a complete prior persistence baseline
- [ ] Candidate workflow builds once and produces `candidate-seal.json`
- [ ] Verify creates `candidate-admission.json` before executing or importing any post-build input
- [ ] Same-run certification artifacts are imported only through `import-evidence`; the closed
      `evidence-import.json` inventory validates before release-wide policy evaluation
- [ ] Persistence evidence selected a strictly older tagged Store and ran only candidate daemon/tools;
      absence of a prior baseline remains a hard failure, not a waiver
- [ ] ABI evidence revalidated and attested the complete prior release, proved loader resolution,
      and ran both old-consumer/new-library and new-consumer/old-library directions
- [ ] Wire evidence revalidated and attested the complete prior release, then ran new/new,
      old-client/new-server, and new-client/old-server using only retained compiled clients
- [ ] Security evidence ran full ASan+UBSan/TSan suites, four-language CodeQL, strict/static and
      supply-chain checks, then validated candidate SBOMs and the exact distributed Linux ELF
- [ ] FreeBSD evidence generated `distinfo` from the sealed source, built and installed the native
      package, exercised rc.subr and durable recovery, then proved config/data preservation
- [ ] Reproducibility evidence rebuilt the closed four-archive set on a distinct runner, matched
      every size and SHA-256, and kept rebuilt bytes outside candidate import and promotion
- [ ] Verify consumes those exact bytes, tests the installed archive, emits manifest/checksums and
      attests `verified-seal.json`
- [ ] Protected `release` Environment approves promotion only after technical gates pass
- [ ] Publish re-verifies the offline Sigstore bundle and uploads without rebuild or clobber
- [ ] Upload / retain CI artifacts under `engineering/evidence/` pointers in the claim YAML

## Honesty gates (do not skip)

- [ ] Claim YAML `claim_massimo` stays honest (`architectural-prototype` until alpha criteria met)
- [ ] Durability campaigns still show `e3_certified=no` unless a promoted campaign record exists
- [ ] Hosted CI green only after Actions billing allows runs; local green ≠ hosted green
- [ ] Docs snapshot: version-lifecycle, compatibility manual, secure-profile residuals still accurate

## Post-tag

- [ ] Retain `candidate-admission.json` and `evidence-import.json` with the verified bundle
- [ ] Verify `release-compat.yml` / `supply-chain.yml` artifacts for the tag
- [ ] Update support notes if a prior release becomes N−1 for reopen matrices
- [ ] File residuals that remain open (E3/E4, hardware perf, hot zero-fence backup, …)

## Related

- [Compatibility and migration manual](../operations/compatibility-and-migration.md)
- [GitHub branch-protection checklist](github-branch-protection.md)
- [Final engineering report](final-engineering-report.md)
- [Production readiness (generated)](../production-readiness.md)
