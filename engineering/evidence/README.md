# Retained evidence drops for tagged claims (Phase D+).
# Populate from CI artifacts / campaign records; do not invent E3/E4.

- [`sdk-installed-secure-profile.md`](sdk-installed-secure-profile.md) defines the temporary CI
  evidence bundle for the installed six-SDK secure-profile matrix. It is not a tagged claim drop.
- [`adr0036-slot-token-local-2026-08-27.md`](adr0036-slot-token-local-2026-08-27.md) records a local,
  prototype-only Release/sanitizer campaign. It is neither CI nor release evidence.
- [`paired-generation-admission-local-2026-08-27.md`](paired-generation-admission-local-2026-08-27.md)
  records the local Alternative A retire-bound campaign. It is neither CI nor release evidence.
- [`adr0036-fixed-shell-local-2026-08-27.md`](adr0036-fixed-shell-local-2026-08-27.md) records the
  local fixed-shell, direct-object and two-thread publication candidate campaigns. It is neither CI
  nor release evidence.

Tagged release promotion uses the machine-readable contract in
[`docs/distribution/release-evidence.md`](../../docs/distribution/release-evidence.md). Evidence JSON
and every referenced log are carried as sealed release candidate files; this directory stores only
durable pointers or reviewed campaign records, never hand-written replacements for missing CI runs.
