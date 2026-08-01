Status: descriptive release process; does not raise claim ceiling
Applies to: tagged releases and claim packaging for GlyphaStore 0.1.x
Owner: release maintainers
Last reviewed: 2026-08-01

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

- [ ] Create annotated tag `vX.Y.Z` (or project tag policy)
- [ ] `scripts/package-release-compatibility-artifacts.sh X.Y.Z`
- [ ] `scripts/package-release-claim.sh X.Y.Z <40-char-sha>`
- [ ] Decide whether to **commit** `tests/fixtures/released/X.Y.Z/` as a permanent N−1 drop
- [ ] Supply-chain workflow on tag: SBOM / checksums / Cosign / attestations as configured
- [ ] Release workflow attaches SDK artifacts to the GitHub Release (prerelease wording)
- [ ] Upload / retain CI artifacts under `engineering/evidence/` pointers in the claim YAML

## Honesty gates (do not skip)

- [ ] Claim YAML `claim_massimo` stays honest (`architectural-prototype` until alpha criteria met)
- [ ] Durability campaigns still show `e3_certified=no` unless a promoted campaign record exists
- [ ] Hosted CI green only after Actions billing allows runs; local green ≠ hosted green
- [ ] Docs snapshot: version-lifecycle, compatibility manual, secure-profile residuals still accurate

## Post-tag

- [ ] Verify `release-compat.yml` / `supply-chain.yml` artifacts for the tag
- [ ] Update support notes if a prior release becomes N−1 for reopen matrices
- [ ] File residuals that remain open (E3/E4, hardware perf, hot zero-fence backup, …)

## Related

- [Compatibility and migration manual](../operations/compatibility-and-migration.md)
- [GitHub branch-protection checklist](github-branch-protection.md)
- [Final engineering report](final-engineering-report.md)
- [Production readiness (generated)](../production-readiness.md)
