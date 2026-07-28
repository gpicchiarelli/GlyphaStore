# E3 pinned-row campaign (operator runbook)

Status: descriptive  
Applies to: `scripts/run-e3-campaign.sh`, `scripts/run-e3-block-reset.sh`,
`scripts/collect-durability-evidence.sh`  
Owner: release and storage maintainers  
Last reviewed: 2026-07-25

Operator procedure to produce a **retained E3 campaign-prep artifact** on a declared hardware/VM
and filesystem pin. A coding agent cannot certify E3 on real NVMe. This runbook exists so a
maintainer can run, review, and — only when gates pass — promote a row.

**Hard rule:** every automated artifact keeps `e3_certified=no` until a human completes the
sign-off checklist and updates the evidence matrix / release notes. A green campaign exit code is
not certification.

Normative claim rules:
[platform durability evidence matrix](../architecture/platform-durability-evidence.md).

## What this campaign establishes

| If the pin is… | A passing campaign may support… | It does **not** establish… |
|---|---|---|
| Hosted CI / unknown backing store | Harness regression only | Any production storage claim |
| Loopback / APFS diskimage (even on NVMe host) | E3-harness evidence for that **image** row | NVMe firmware / controller-cache certification |
| VM dedicated virtio/NVMe disk with Store data on that disk | E3 row claim for that exact guest disk + FS after review | Other kernels, FS options, or hosts |
| Bare-metal NVMe/SATA partition + physical or confirmed below-process reset | Strongest in-repo path toward E3 after review | Untested firmware revisions or mount changes |

The current in-repo E3 harness (`run-e3-block-reset.sh`) still provisions a **disposable** ext4
loopback or APFS sparsebundle. Label `guest_host_boundary` honestly. Certifying “production NVMe”
requires Store data on that device (dedicated disk/partition), not merely a sparse file sitting on
it, plus a reset that is confirmed below the process boundary (and ideally a physical power cut).

## Prerequisites

1. Clean git worktree at the commit under test.
2. Built tree containing at least:
   - `glyphastore_tests`
   - `glyphastore_allocation_fault_tests`
   - `glyphastore_crash_persistence`
   - daemon crash binaries required by the E2 collector
3. Linux: root/`sudo` for loop, mount, fsck, optional `dm-flakey` (`e2fsprogs`, `lvm2`).
4. macOS: ability to run `hdiutil` attach/detach for APFS sparsebundles.
5. Disposable target: no unrelated data on the image/device under reset.

Suggested builds:

```bash
cmake --preset unix-debug    # Linux
cmake --build --preset unix-debug --target \
  glyphastore_tests glyphastore_allocation_fault_tests \
  glyphastore_crash_persistence glyphastore_crash_daemon glyphastored

cmake --preset macos-debug   # macOS
cmake --build --preset macos-debug --target \
  glyphastore_tests glyphastore_allocation_fault_tests \
  glyphastore_crash_persistence glyphastore_crash_daemon glyphastored
```

## Pin the row (before any E3 arming)

Record a stable **redacted** identity. Do not store serials, MACs, or credentials.

| Field | Examples | Notes |
|---|---|---|
| `pin_label` | `lab-nvme0-ext4-a`, `mac-mini-apfs-1` | Stable name for release notes |
| `hardware_class` | `hosted-ci`, `diskimage-rehearsal`, `vm-dedicated-disk`, `bare-metal-nvme`, `bare-metal-other` | Drives promotion eligibility hint |
| `fs_pin` | `ext4` or `apfs` | Must match harness platform |
| `guest_host_boundary` | `loopback-image-on-host-nvme`, `hdiutil-sparsebundle-apfs`, `vm-virtio-blk-ext4-on-nvme`, `physical-nvme-partition-ext4` | Honesty gate |
| Mount / mkfs | `defaults`, `barrier=1`, APFS volume role | Attach redacted `findmnt` / `diskutil` under `stages/pin-attachments/` |
| Cache / barrier | virtio `cache=none` / `writeback`; controller write cache on/off | Required for promotion review |
| Reset mechanism | `abrupt-detach`, `dm-flakey`, physical PDU cut | “Command issued” ≠ “power removed” |

## Sequence: E0 → E1 → E2 → E3

The orchestrator runs stages in order and stops further heavy stages after a hard FAIL:

| Stage | Mechanism | Default command inside orchestrator |
|---|---|---|
| E0 | Unit / codec / reopen | `ctest -R '^glyphastore_tests$'` |
| E1 | Allocation fault injection | `ctest -R '^glyphastore_allocation_fault_tests$'` |
| E2 | Process-kill collector | `collect-durability-evidence.sh --run process-kill` |
| E3 | Block-reset harness | `run-e3-block-reset.sh --profile campaign` |

Do not skip stages on a promotion candidate. `--skip-e*` is diagnostic only and forces
`promotion_candidate=no`.

## Repetition policy

| Layer | Minimum for campaign-prep tarball | Minimum before maintainer may consider E3 promotion |
|---|---|---|
| E2 process-kill suite | `--e2-repeat 3` (orchestrator default) | ≥ 3 clean passes on the pinned probe path |
| E3 per checkpoint | `--repeat 10` (orchestrator default) | ≥ 10 with zero unexplained outcomes for the claimed scope |
| E3 checkpoint set | harness `campaign` profile (put smoke + bootstrap/rotate) | Narrow put-scope may use smoke set only if the claim text says so; general durable claim needs full matrix scope |

Raise `--repeat` when timing jitter or flaky arming appears. Never discard FAIL/INCONCLUSIVE rows.

## PASS / FAIL / INCONCLUSIVE

Per E3 case (checkpoint × repetition), use the matrix definitions:

| Outcome | Meaning |
|---|---|
| **PASS** | Reset confirmed below process boundary; remount + non-repairing fsck; recovery oracle accepts |
| **FAIL** | Reset confirmed, but oracle disagrees or committed state that must survive is lost/corrupt |
| **INCONCLUSIVE** | Checkpoint not reached, reset not confirmed, host/harness crash, lost console, fsck/remount infra failure |

Campaign-level result:

| Result | When |
|---|---|
| `passed` | E0–E3 all ran and passed (E3 harness_result=passed) |
| `failed` | Any hard FAIL |
| `inconclusive` | No FAIL, but E3 reported inconclusive |
| `incomplete` | Stages skipped |

`passed` still leaves `e3_certified=no`.

## How to run the campaign

### Linux ext4 (loopback rehearsal on a lab host)

```bash
sudo -v   # harness needs privileged loop/mount/fsck
./scripts/run-e3-campaign.sh \
  --output "$HOME/glypha-evidence/e3-campaign-$(date -u +%Y%m%dT%H%M%SZ)" \
  --build-dir build/unix-debug \
  --pin-label lab-host-ext4-loop-a \
  --hardware-class diskimage-rehearsal \
  --fs-pin ext4 \
  --guest-host-boundary loopback-image-on-host-filesystem \
  --platform linux-ext4 \
  --reset-mechanism auto \
  --probe-path "$PWD" \
  --repeat 10 \
  --e2-repeat 3
```

### macOS APFS (diskimage rehearsal)

```bash
./scripts/run-e3-campaign.sh \
  --output "$HOME/glypha-evidence/e3-campaign-$(date -u +%Y%m%dT%H%M%SZ)" \
  --build-dir build/macos-debug \
  --pin-label lab-mac-apfs-image-a \
  --hardware-class diskimage-rehearsal \
  --fs-pin apfs \
  --guest-host-boundary hdiutil-sparsebundle-apfs \
  --platform macos-apfs \
  --reset-mechanism abrupt-detach \
  --probe-path "$PWD" \
  --repeat 10 \
  --e2-repeat 3
```

### Pin notes for a future dedicated-disk campaign

When Store data and reset target are a dedicated VM disk or bare-metal partition (not only a
loop file), set for example:

```bash
  --hardware-class vm-dedicated-disk \
  --guest-host-boundary vm-virtio-blk-ext4-on-nvme \
  --mount-options-note 'ext4 defaults,barrier=1; mkfs.ext4 -F' \
  --cache-barrier-note 'virtio cache=none; guest writeback off'
```

If the harness still uses a loop image *on* that disk, keep `loopback-…` in the boundary string;
do not claim bare-partition certification from a loop row.

## Artifact layout

```text
<output>/
  campaign-provenance.txt      # always e3_certified=no
  campaign-pin.txt
  campaign-summary.md
  promotion-checklist.md       # all boxes unchecked at creation
  stage-results.tsv
  commands.txt
  manifest.sha256
  e2-orchestrator.log
  e3-orchestrator.log
  stages/
    pin-attachments/           # operator drops redacted findmnt/diskutil here
    e0/                        # ctest log + result
    e1/
    e2/                        # collect-durability-evidence artifact
    e3/                        # run-e3-block-reset artifact (provenance, results.tsv, …)
<output>.tar.gz
<output>.tar.gz.sha256
```

Retain the tarball and its `.sha256` beside release evidence. Re-verify with
`sha256sum -c` / `shasum -a 256 -c` before review.

## Maintainer sign-off checklist

Copied into every artifact as `promotion-checklist.md`. Promotion is **human-only**:

- [ ] Clean source commit matches `campaign-provenance.txt`
- [ ] Pin identity complete (hardware/VM, FS, mkfs/mount, cache/barrier, boundary, reset)
- [ ] E0, E1, and E2 stages passed on this same pin (not skipped)
- [ ] E3 campaign profile covered required checkpoints; repeat ≥ policy minimum
- [ ] Zero FAIL and zero unexplained INCONCLUSIVE outcomes
- [ ] Every retained PASS case has `reset_confirmed=yes`
- [ ] SHA-256 manifest verifies; tarball retained
- [ ] Row is not hosted-CI / disposable-image-only if claiming production NVMe/SATA
- [ ] Release maintainer recorded artifact reference in release notes
- [ ] Matrix row in `platform-durability-evidence.md` updated only after the above

## How to promote (gate)

1. Campaign tarball result is `passed`, `source_dirty=no`, stages complete.
2. `hardware_class` and `guest_host_boundary` match the claim you intend to publish.
3. Every checklist item above is checked by a release/storage maintainer.
4. Edit `docs/architecture/platform-durability-evidence.md` Current evidence table for that row only.
5. Reference the tarball SHA-256 and pin label in release notes.
6. Do **not** patch harness scripts to emit `e3_certified=yes` as a substitute for steps 4–5.

Until steps 1–5 succeed, leave the matrix at **not E3 certified** and keep artifacts labeled
`e3_certified=no`.

## Related

- [Platform durability evidence](../architecture/platform-durability-evidence.md)
- [Test strategy](../development/test-strategy.md)
- [Operations index](README.md)
- CI rehearsal only: `.github/workflows/durability-evidence.yml` (not a promotion path)
