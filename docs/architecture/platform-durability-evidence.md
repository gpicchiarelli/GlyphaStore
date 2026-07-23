# Platform durability evidence matrix

Status: roadmap
Applies to: persistence v1 and every storage mode that claims restart durability
Owner: release and storage maintainers
Last reviewed: 2026-07-23

This document records what each kind of test can establish and what GlyphaStore has established on
each platform/filesystem row. A hosted runner whose filesystem, mount options, cache path, and
reset mechanism are unknown is useful regression evidence, but it is not storage certification.

The normative byte layouts and restart decisions remain in
[persistence v1](../spec/persistence-v1.md) and the
[recovery state-transition matrix v1](../spec/recovery-state-matrix-v1.md). This matrix governs
evidence claims, not recovery behavior.

## Evidence levels

| Level | Mechanism | What it establishes | What it does not establish |
|---|---|---|---|
| E0 | Codec, unit, model, and ordinary reopen tests | Local invariants, canonical bytes, validation, deterministic recovery logic | OS write ordering, process death, device persistence |
| E1 | Deterministic filesystem/allocation fault injection | Exact pre/post syscall outcome, retry, fail-closed behavior, recovery oracle | Kernel/filesystem/device behavior under abrupt loss |
| E2 | `SIGKILL` of the Store harness or real daemon, followed by reopen | Software ordering survives immediate process termination at named checkpoints | Dirty kernel page-cache loss, controller-cache loss, torn sectors, physical power loss |
| E3 | Abrupt VM/block-device reset or controlled physical power cut on a pinned storage row | Recovery under loss below the process/kernel boundary for that exact row | Other kernels, filesystems, mount options, devices, firmware, or cache policies |
| E4 | Repeated release campaign with retained, checksummed provenance and zero unexplained outcomes | Release evidence for the exact supported row and source artifact | A general promise for untested storage stacks |

Levels are cumulative for a certified row: E4 requires the relevant E0–E3 evidence. Passing a higher
number once does not erase missing lower-level coverage. Emulation may prove codec portability, but
it cannot promote storage evidence beyond the layer whose persistence behavior is actually under
test.

## Current evidence

No platform/filesystem row is E3 or E4 certified. The repository has extensive E1 coverage and E2
crash harnesses. Retained evidence from pinned native power-loss campaigns does not exist yet.

| Platform/filesystem row | Automated evidence available | Highest defensible current claim | Missing before certification |
|---|---|---|---|
| macOS / APFS | Native build/test workflow; local deterministic faults; embedded and daemon `SIGKILL`/reopen suites | E2 when a collector artifact records an actual passing native run; otherwise only “workflow configured” | Pinned Apple hardware/storage, APFS and mount metadata, controlled abrupt-loss campaign, repetitions and retained artifacts |
| Linux / ext4 | Hosted Linux workflow and the same E1/E2 suites; hosted backing storage is not pinned | E2 only for a separately recorded run on a declared ext4 mount | Dedicated ext4 block device or VM image, mount/cache/barrier metadata, reset campaign |
| Linux / XFS | Code and suites are portable to the row; no pinned row in current CI | E1 implementation coverage; E2/E3 row evidence not retained | Native XFS job, E2 run, reset campaign and artifacts |
| Linux / btrfs | Code and suites are portable to the row; no pinned row in current CI | E1 implementation coverage; E2/E3 row evidence not retained | Native btrfs job, E2 run, reset campaign and artifacts |
| FreeBSD / UFS | Native FreeBSD VM workflow builds and runs the general suite (`.github/workflows/freebsd.yml`); guest storage row is not pinned | Portability/regression signal, not UFS certification | Pin UFS mount metadata, E2 collector artifact, then E3/E4 campaign |
| FreeBSD / ZFS | No maintained native storage row; FreeBSD CI does not select or report ZFS | Architectural target, not a supported durability claim | Explicit ZFS contract, native ZFS job, E2, then pinned E3/E4 campaign |
| OpenBSD / FFS | OpenBSD/LibreSSL VM workflow runs the general suite, but does not pin or report the guest storage row as durability evidence | Portability/regression signal, not FFS certification | Native or controlled FFS row, filesystem metadata, E2 and E3/E4 artifacts |
| NFS, SMB, FUSE, overlay, remote or user-space storage | Deliberately outside the local-filesystem contract | Unsupported | Not eligible for certification under persistence v1 |

“Hosted workflow” never means that its provider’s current backing filesystem has been inferred or
certified. A row advances only from an artifact that names the tested stack.

## Evidence artifact contract

Every retained E2–E4 artifact must contain:

- schema version, UTC start/end, exact source commit, clean/dirty source status, and build profile;
- OS release and kernel, architecture and hardware/virtualization class;
- filesystem type, mount point and options, backing device/image class, and relevant cache/barrier
  policy;
- compiler, CMake/CTest, test executable identity, exact command, repetition count, and exit status;
- checkpoint or reset schedule, recovery oracle, and all unexpected outcomes;
- SHA-256 manifest for the artifact contents.

E3/E4 additionally require the reset mechanism, guest/host boundary, controller or virtual-device
cache policy, boot/filesystem health result, and confirmation that the reset was not a graceful
shutdown. Physical device identifiers and credentials must not be stored; stable redacted labels
are sufficient.

An artifact from a dirty worktree is diagnostic only. An artifact with missing filesystem or reset
metadata cannot be promoted by manually relabeling it. Evidence is attached to the tested commit and
row; it does not automatically transfer to later source, OS, kernel, firmware, or mount changes.

## Safe native collector

The repository collector records provenance and optionally runs all six embedded/daemon
process-kill tests:

```sh
scripts/collect-durability-evidence.sh \
  --output /path/to/new/evidence-directory \
  --build-dir build/macos-debug \
  --probe-path . \
  --run process-kill \
  --repeat 3
```

The collector refuses to overwrite an existing directory. It records only E0 metadata or E2
process-kill evidence and always emits `power_loss_exercised=no`. It never resets a machine, detaches
a block device, changes a mount, or claims E3/E4. During an E2 run it sets `TMPDIR` to the writable
probe directory so the crash harness creates its temporary data on the filesystem being described.

For a quick inventory without running tests:

```sh
scripts/collect-durability-evidence.sh \
  --output /path/to/new/metadata-directory \
  --probe-path . \
  --metadata-only
```

## E3 campaign protocol

Power-loss work must run only in a disposable, controlled environment with no unrelated data on the
target device or image. Automation is platform-specific, but every campaign follows the same
sequence:

1. Provision a fresh named filesystem row and record the image/device, filesystem creation options,
   mount options, cache mode, host/guest boundary, and software versions.
2. Build a clean, immutable source commit and first pass E0, E1, and E2 on that exact row.
3. For every persistent checkpoint in the normative recovery matrix, arm an external reset trigger.
   The trigger must act below the process boundary and must not allow an orderly shutdown.
4. Restart, capture filesystem health/fsck output without silently repairing away evidence, open the
   Store, and evaluate the checkpoint’s required/optional/forbidden visibility oracle.
5. Repeat each checkpoint enough times to cover timing variability. Record every attempt, including
   harness failures and inconclusive resets; do not discard failed rows.
6. Checksum and retain the complete artifact. Promote to E4 only through the release checklist and
   only for the exact tested row.

The campaign controller must distinguish “reset command issued” from “power was actually removed.”
A timeout, host crash, lost console, or test-harness error is inconclusive, not a passing recovery.

## Promotion and regression rules

A storage row is supported only when its exact scope and E4 artifact are published with the release.
Any change to write ordering, synchronization primitives, manifest/intent publication, commit-slot
handling, recovery authority, filesystem policy, or power-loss harness invalidates the affected
campaign. Kernel/filesystem/device changes require review and usually a new campaign.

CI may archive E2 collector output for every change. E3 should run on dedicated scheduled
infrastructure, and E4 is a reviewed release campaign. Until those gates exist, GlyphaStore remains
experimental for durable deployment even when every process-kill test passes.
