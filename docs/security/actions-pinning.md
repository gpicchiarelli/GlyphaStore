Status: normative for CI workflow composition
Applies to: `.github/workflows/*.yml` third-party Actions
Owner: maintainers
Last reviewed: 2026-08-01

# GitHub Actions commit pinning

Third-party Actions must be pinned to a **40-character commit SHA**. Optional trailing comments
may record the human tag (for example `# v7`). Mutable refs (`@v1`, `@main`, short SHAs) are
forbidden.

Authority:

- Validator: `python3 engineering/tools/validate_actions_pins.py`
- Requirement: `GS-SUPPLY-ACTIONS-001`
- Gate: `GATE-THREAT-SUPPLY`

When updating an Action, change the SHA and the comment together. Dependabot PRs that only bump
a floating tag must be rewritten to a SHA before merge. Dependabot also watches `pip`
(`/sdk/python`) and `gomod` (`/sdk/go`); GitHub Actions updates remain SHA-pinned.

Additional supply-chain CI: OpenSSF Scorecard (`.github/workflows/scorecard.yml`),
dependency-review on PRs, and actionlint + `validate_actions_pins.py` on workflow changes.
Repository settings checklist: [github-branch-protection](../assurance/github-branch-protection.md).
