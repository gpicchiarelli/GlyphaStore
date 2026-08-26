Status: maintained repository settings checklist
Applies to: GitHub branch protection, security products, required checks
Owner: maintainers
Last reviewed: 2026-08-26

# GitHub branch protection and security settings

Workflow YAML cannot enable org/repo settings. In repository settings,
apply this checklist on `main` so CI evidence is enforceable.

Claim ceiling remains **architectural prototype**. Green required checks do not imply E3/E4
certification, absolute performance budgets, or production readiness.

## Required status checks (suggest)

Require these workflows/jobs to pass before merge (adjust names to match the Actions UI):

| Workflow | Why |
| --- | --- |
| `CI` (`strict-iso-hardening`, `build-test`, `sdk-clients`, `install-consumer`, `release-optimized`) | Core correctness / install |
| `Assurance` | Catalog + Actions SHA pins |
| `Sanitizers` | ASan/UBSan + TSan (+ fuzz on schedule) |
| `Static analysis` | clang-tidy / format |
| `Supply chain scan` | gitleaks + Trivy |
| `Dependency review` | PR dependency gate |
| `Actionlint` | Workflow YAML + pin validator (when workflows change) |
| `CodeQL` (`analyze-cpp` at minimum) | SAST |
| `OpenSSF Scorecard` | Supply-chain posture |
| `FreeBSD` / `OpenBSD LibreSSL` | Supported Unix portability |
| `Released artifact compatibility` | Fixture / self-artifact harness |

Optional (path-filtered or scheduled — require only if always run on the PR):

- `Docs links`, `License check`, `Formal ShardPair (TLC)`
- `release-lto-smoke` (heavier; keep required once stable)

Do **not** require: `Coverage` (diagnostic), `Extended soak`, `Benchmarks` absolute
thresholds, `Paired Linux hard-pinned A/B` (self-hosted), physical E3 jobs.

## Security products (Settings → Security)

1. **Secret scanning** + **push protection** (public repos: enable both).
2. **Private vulnerability reporting** (aligns with `SECURITY.md`).
3. **Code scanning** enabled so Scorecard / CodeQL / Trivy SARIF can publish when
   repository variable `GLYPHASTORE_UPLOAD_SARIF=true`.
4. **Dependabot alerts** + security updates (ecosystems configured in
   [`.github/dependabot.yml`](../../.github/dependabot.yml)).

## Merge queue

After required checks are stable for several weeks, enable a merge queue on `main`
with the same required checks. Prefer squash merges for linear history.

## Actions SHA discipline

Third-party Actions stay on 40-character commit SHAs
([actions-pinning](../security/actions-pinning.md)). Dependabot PRs that only bump a
floating tag must be rewritten to a SHA before merge.

## Honest residuals

- A queued, skipped, cancelled, or unavailable hosted run is not green evidence; local green ≠ CI green.
- Physical E3/E4, absolute hardware perf, and tagged N−1 fixture drops remain outside
  this checklist.
