# Retained evidence drops for tagged claims (Phase D+).
# Populate from CI artifacts / campaign records; do not invent E3/E4.

- [`sdk-installed-secure-profile.md`](sdk-installed-secure-profile.md) defines the temporary CI
  evidence bundle for the installed six-SDK secure-profile matrix. It is not a tagged claim drop.

Tagged release promotion uses the machine-readable contract in
[`docs/distribution/release-evidence.md`](../../docs/distribution/release-evidence.md). Evidence JSON
and every referenced log are carried as sealed release candidate files; this directory stores only
durable pointers or reviewed campaign records, never hand-written replacements for missing CI runs.
