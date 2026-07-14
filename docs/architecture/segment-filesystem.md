# Durable Segment files

This document describes the Segment-file layer backing end-to-end `durable_sync` creation,
recovery, and mutation. Process-kill evidence remains a certification gate.

## Names and creation

A Segment identity maps to one canonical lowercase filename:

```text
segment-<16 hexadecimal Segment-ID digits>-<8 hexadecimal generation digits>.glypha
```

Creation uses the recognizable temporary `.<final-name>.tmp`, private mode `0600`, `O_EXCL`,
`O_NOFOLLOW`, and directory-relative operations. The filename is only a namespace key: opening also
decodes the immutable header and requires the complete expected Store ID, Segment ID, generation,
and owner Worker.

The creation state machine is:

```text
encode initial header (slot 0 generation 1, empty active extent)
  -> reject an existing final identity
  -> create private temporary
  -> physically reserve exactly 64 MiB
  -> write the 4 KiB header
  -> synchronize the complete file
  -> rename to the final name
  -> synchronize the data directory
```

An injected or real failure before `renameat` is `not_published` and removes the temporary name. A
failure after attempting `renameat`, or during the following directory synchronization, is
`indeterminate` and poisons the `DataDirectory` instance. This matches manifest publication: atomic
visibility is not evidence that a name survived a machine failure.

## Platform allocation strategy

The file is not accepted after sparse `ftruncate` alone. Allocation is chosen at compile time:

| Platform | Allocation path | Reason |
|---|---|---|
| Linux | direct `fallocate(fd, 0, 0, 64 MiB)` syscall | native optimal allocation without the unsafe userspace-emulation fallback described for glibc `posix_fallocate` |
| macOS | `F_PREALLOCATE` with contiguous/all-or-nothing first, then non-contiguous/all-or-nothing; exact `ftruncate` afterward | reserves backing store while preferring low fragmentation; preallocation alone does not set logical EOF |
| FreeBSD | native `posix_fallocate` syscall | the OS guarantees the requested range is allocated or returns an error number |
| OpenBSD | eager sequential zero writes in bounded 256 KiB chunks | OpenBSD exposes no documented reservation syscall; this avoids approving a merely sparse file |

Linux documents that successful default `fallocate` prevents later writes in the range from failing
for lack of disk space. FreeBSD documents the same guarantee for `posix_fallocate`. Apple documents
`F_PREALLOCATE` as reserving backing store and recommends the contiguous/non-contiguous fallback
pattern. OpenBSD's eager path is intentionally described separately and still requires native
filesystem crash and disk-full certification.

## Append and commit protocol

`append` accepts only a complete canonical Record that passes structural and CRC32C validation. Its
sequence must be non-zero and strictly greater than the last committed sequence. Checked arithmetic
rejects generation/count overflow and capacity exhaustion before I/O.

```text
pwrite complete Record at selected committed_end
  -> data synchronization
  -> encode the next generation into the other 128-byte slot
  -> pwrite the complete slot
  -> data synchronization
  -> update the in-memory selected slot
```

No header read-modify-write occurs. Slot writes are fixed-size and alternate between offsets 128 and
256, preserving the previous valid recovery boundary. `seal` uses the same alternate-slot protocol
without changing the Record extent.

The result classification is deliberately asymmetric:

| Failure boundary | Outcome | Meaning |
|---|---|---|
| argument, Record write, or Record-data sync | `not_committed` | the selected slot still defines the old extent; any tail is ignored |
| before attempting the slot write (fault seam) | `not_committed` | Record bytes may be durable, but remain outside recovery authority |
| slot write attempted or following sync failed | `indeterminate` | either old or new valid slot may win after restart; every handle sharing the data directory is poisoned |
| slot sync completed | `committed` | the new boundary is recovery authority |

After `indeterminate`, the shared `DataDirectory` health state fails closed across Segment and
manifest operations. Callers must stop mutation, close, and recover; they must not infer success from
bytes visible through the page cache.

The shared state is also poisoned before a `DataDirectory` releases or replaces its process lock.
Consequently, a Segment handle cannot outlive the lock owner and continue mutating storage.

## Opening, scan, and reads

Open rejects non-private files, symbolic links, extra hard links, incorrect ownership, any size other
than 64 MiB, a mismatched immutable identity, and headers without an unambiguous valid commit slot.

Recovery scanning reads exactly `[4096, committed_end)` with one bounded contiguous positional-read
operation. Memory is therefore `O(committed bytes)` with a hard 64 MiB Segment bound, while syscall
count is independent of Record count. The decoder advances by each canonical encoded extent and
validates checksum, alignment, strict sequence ordering, Record count, and first/last commit
sequences. It never reads or interprets a crash tail. The returned `RecordRef` list is `O(record
count)` and can feed deterministic Index rebuild.

The recovery orchestrator uses the lower-level visitor form of this scan: each already decoded
`RecordView` is consumed before the bounded buffer is released, avoiding an additional read and CRC
pass per Record. See [durable recovery implementation](recovery-implementation.md).

Positional Record reads require a matching Segment ID/generation, an aligned fully committed bounded
extent, and the expected sequence, then revalidate the Record checksum. Stale generations and
malformed extents cannot escape the selected recovery boundary.

Runtime reads open Segment handles read-only; the active owner upgrades its one cached handle to
read-write for mutation. Read-only handles reject append and seal. The single-Record visitor performs
one positional read and one canonical CRC-verified decode, then exposes a view only for the callback
lifetime. The bounded cache, commit-snapshot check, mutation ordering, and fail-closed behavior are specified in the
[durable runtime catalog](durable-runtime-catalog.md).

## Current evidence and limits

Unit tests cover exact size and identity, duplicate creation, durable reopen, alternating slots,
sealing, uncommitted tail recovery, indeterminate slot poisoning, committed-region corruption, and
directory-lifetime fail-closed behavior, read-only mutation rejection, plus allocation/write/sync
fault boundaries. The macOS
implementation is exercised locally. Linux,
FreeBSD, and OpenBSD paths still require native CI plus real disk-full, short-I/O, process-kill, and
power-loss testing before any platform is certified for durable service.
