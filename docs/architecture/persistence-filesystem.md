# Persistence filesystem layer

This document specifies the low-level filesystem primitives behind public
`Store::open(durable_sync)` creation, recovery, and mutation. Production certification still
requires native-platform and process-kill evidence.

## Security and namespace policy

`DataDirectory` opens an existing directory with `O_DIRECTORY`, `O_CLOEXEC`, and `O_NOFOLLOW`, then
anchors every engine-owned name to that descriptor with `openat`, `unlinkat`, and `renameat`.
Changing the process working directory cannot redirect an operation. The directory must be owned by
the effective user and must not be writable by group or other users.

The lock, temporary manifest, and authoritative manifest must be private regular files. Reads reject
symbolic links, multiple hard links, foreign ownership, and group/other write permission. Creation
uses `O_EXCL`, `O_NOFOLLOW`, close-on-exec, and mode `0600`. These checks reduce pathname substitution
and special-file blocking attacks; they do not make advisory locks enforceable against a process
that deliberately ignores the protocol.

The engine-owned names are:

| Name | Purpose |
|---|---|
| `.glyphastore.lock` | lifetime of the exclusive process lock |
| `.manifest.glypha.tmp` | replaceable, recognizable publication temporary |
| `manifest.glypha` | authoritative complete manifest |
| `.glyphastore.bootstrap.tmp` | temporary bootstrap-intent publication |
| `.glyphastore.bootstrap` | bootstrap intent encoded as a complete v1 Manifest |
| `.glyphastore.compaction.tmp` | temporary compaction-intent publication |
| `.glyphastore.compaction` | checksummed compaction intent with old and replacement manifests |
| `.segment-<id>-<generation>.glypha.tmp` | recognizable unpublished Segment temporary |
| `segment-<id>-<generation>.glypha` | immutable-identity Segment generation |

## Platform synchronization strategy

| Platform | Data synchronization | Metadata/publication synchronization |
|---|---|---|
| Linux | `fdatasync` when only retrievable file data is required | `fsync` for manifests and directory entries |
| macOS | `F_BARRIERFSYNC` for Record-before-slot ordering; `F_FULLFSYNC` for the durable commit point | `fcntl(F_FULLFSYNC)` for both manifest and directory descriptors |
| FreeBSD | `fdatasync` for data-only requests | `fsync` for manifests and directories |
| OpenBSD | `fdatasync` API, currently implemented as `fsync` by the OS | `fsync` for manifests and directories |

Linux documents that synchronizing a file does not necessarily synchronize its directory entry, so
publication explicitly synchronizes the directory. It also documents that `fdatasync` includes file
size metadata needed to retrieve written data. See the current Linux
[`fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html) and
[`rename(2)`](https://man7.org/linux/man-pages/man2/rename.2.html) manuals.

Apple documents that `F_BARRIERFSYNC` orders I/O synchronized before the barrier ahead of I/O issued
after it, while `F_FULLFSYNC` drains the device queue and makes earlier synchronized data persistent.
GlyphaStore uses that two-phase protocol for Segment Record and commit-slot publication on supported
HFS/APFS storage. The final commit point, manifest publication, and directory publication still use
`F_FULLFSYNC`; there is no downgrade to ordinary `fsync`. An unsupported barrier or full flush is an
I/O failure. See Apple's [`fsync(2)`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)
and [`fcntl(2)`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html)
manuals.

FreeBSD and OpenBSD use their native `fdatasync`/`fsync` contracts. OpenBSD explicitly warns that an
`EIO` synchronization failure may leave partially written data and continues reporting failure until
references close. See the [FreeBSD manual service](https://man.freebsd.org/cgi/man.cgi?query=fsync&sektion=2)
and OpenBSD [`fsync(2)`](https://man.openbsd.org/fsync.2).

## Complete positional I/O

`FileDescriptor` is move-only and closes exactly once. Positional reads and writes:

- reject extents that cannot be represented by `off_t`;
- split requests at `SSIZE_MAX`;
- retry only operations interrupted by `EINTR`;
- continue after short `pread`/`pwrite` results;
- fail on a zero-progress write or premature EOF.

This keeps file position shared by no caller and is safe for future concurrent access to distinct
Segment offsets. `close` is intentionally not retried because several supported systems may already
have released the descriptor after an interrupted close; durability errors are collected by the
explicit synchronization call before RAII destruction.

Each test `DataDirectory` may carry instance-local raw file-I/O callbacks with the same result and
`errno` contract as `pread`, `pwrite`, and synchronization syscalls. Production leaves them null and
takes the direct native call. The deterministic suite forces repeated short transfers, a first-call
`EINTR`, delayed synchronization `EIO`, `ENOSPC`, `EDQUOT`, and `EROFS`. Hooks move with their file
descriptor and contain no process-global state, so independent fault matrices can execute safely in
parallel.

## Locking

The process lock uses non-blocking exclusive `flock` and is held by an open descriptor for the
`DataDirectory` lifetime. A second cooperating process or independently opened instance fails before
reading or mutating storage. `flock` is advisory on every target; OpenBSD and Apple document those
semantics explicitly in their [`flock(2)`](https://man.openbsd.org/flock.2) and
[`flock(2)`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/flock.2.html)
manuals.

Network and user-space filesystems are unsupported. Linux specifically notes that a failed NFS
rename may already have happened after RPC replay. The implementation therefore treats every actual
`renameat` failure conservatively as indeterminate rather than claiming that the old name won.
Do not deploy durable mode on NFS, SMB, FUSE, overlay, or another remote/user-space filesystem.

| Platform/filesystem | Deterministic faults and process kill | Real power cut | Durable status |
|---|---|---|---|
| macOS/APFS | exercised locally and in hosted CI, whose exact storage stack is not pinned | pending | experimental, not certified |
| Linux/ext4, XFS, btrfs | hosted Linux CI exists but does not pin and certify each filesystem/mount row | pending | experimental, not certified |
| FreeBSD/UFS, ZFS | native runner pending | pending | architectural target only |
| OpenBSD/FFS | native runner pending | pending | architectural target only |
| NFS, SMB, FUSE, overlay, other network/user-space storage | deliberately outside the local-filesystem contract | not accepted | unsupported |

Certification requires a pinned OS, filesystem and mount configuration, repeated device/VM power
cuts at every checkpoint, post-boot filesystem health inspection, and v1 recovery verification.

## Manifest publication state machine

```text
encode
  -> validate the existing manifest and require a greater generation
  -> remove recognized stale temporary
  -> create private temporary with O_EXCL
  -> complete positional write
  -> full file synchronization
  -> atomic renameat over manifest.glypha
  -> full directory synchronization
```

The result distinguishes three outcomes:

| Outcome | Meaning | Required caller action |
|---|---|---|
| `durable` | rename and directory synchronization completed | publication may become recovery authority |
| `not_published` | failure is known to precede the rename syscall | old manifest remains authoritative; retry may be allowed |
| `indeterminate` | rename was attempted, or a later directory sync failed | poison the instance, stop service, close, and recover |

`renameat` replacement is atomic on the supported local filesystem model: readers never observe a
missing final name between old and new. Atomic visibility is not the same as durable visibility, which
is why the directory synchronization is a separate mandatory step.

The exclusive lock serializes the read-before-publish generation check. Equal or decreasing manifest
generations fail before the temporary is touched. A malformed existing authoritative manifest also
fails closed instead of being overwritten as an implicit repair.

The test-only seams inject failures before write, file sync, rename, and directory sync and inside
positional I/O/synchronization retry loops. Tests prove that every manifest/bootstrap/Segment
pre-rename failure preserves the prior authority or a pristine namespace, while post-rename failure
poisons the current instance and requires reopening. Real block-device disk-full, controller-cache,
torn-sector, filesystem, and power-loss matrices remain mandatory before durable certification.

The same primitives now back exact-size preallocated Segment creation and alternate-slot mutation.
Platform allocation choices, Segment-specific outcome boundaries, scan complexity, and tests are
specified in [durable Segment files](segment-filesystem.md).

Recovery also uses a fresh descriptor-relative directory stream to classify every entry before
opening catalog Segments. Its strict filename grammar, no-follow metadata checks, deterministic
reporting, bounds, and crash-temporary policy are specified in the
[data-directory namespace audit](namespace-policy.md).
