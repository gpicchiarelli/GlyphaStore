# Released compatibility artifacts

Status: descriptive
Applies to: persistence v1 and wire v2 golden fixtures across release labels
Owner: persistence maintainers
Last reviewed: 2026-07-23

Cross-release evidence is **not** implied by the in-tree `tests/fixtures/*.hex` codecs alone.
Release packaging drops (or CI downloads) labeled trees here:

```text
tests/fixtures/released/<label>/
  manifest_v1.hex
  segment_header_v1.hex
  record_v1.hex
  ...
  METADATA.txt          # optional: git tag, commit, package date
```

The unit target `released_artifact_compat_tests` decodes every `*.hex` under each subdirectory
using the current tree's codecs. An empty `released/` tree (only this README) is a successful
no-op: the harness exists, but tagged artifact drops remain an alpha gate.

Package the current tree's fixtures for a label:

```bash
scripts/package-release-compatibility-artifacts.sh 0.1.0-alpha.1
```

Policy: [version lifecycle](../../docs/architecture/version-lifecycle.md).
