# Released compatibility artifacts

Status: descriptive
Applies to: persistence v1 and wire v2 golden fixtures across release labels
Owner: persistence maintainers
Last reviewed: 2026-08-27

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
no-op: the harness exists. In-tree `self-v1/` is the local self-check; CI job
`released-artifact-compat` (`.github/workflows/release-compat.yml`) also packages a per-SHA self
artifact on every push/PR and, on version tags, packages + uploads `released/<label>/`.

Package the current tree's fixtures for a label:

```bash
scripts/package-release-compatibility-artifacts.sh 0.1.0-alpha.1
```

## Wave 5 (L7) residuals

| Surface | Present | Still open |
| --- | --- | --- |
| Codec harness / `self-v1` | Yes | Permanent `<semver>/` N−1 trees |
| Store N−1 drops | README only under `released-stores/` | First tagged complete Store |
| ABI old×new binary row | Same-run producer + install-consumer scaffold | Prior attested ABI-1 consumer asset |
| Wire N−1 clients | Same-run producer | Prior attested wire-v2 client/server |
| FreeBSD/OpenBSD packages | Fail-closed producers | `PORTS_ACCOUNT_REGISTERED` + tagged evidence |

Authority: [wave5-l7-residuals.md](../../../docs/distribution/wave5-l7-residuals.md).

Policy: [version lifecycle](../../../docs/architecture/version-lifecycle.md).
