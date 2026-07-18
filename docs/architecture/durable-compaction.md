# Crash-safe durable compaction

This document defines the v1 compaction invariants. The current tree implements the deterministic
planner, its resource gates, a checksummed intent codec embedding both exact manifest authorities,
descriptor-relative intent publication/removal primitives, and restart recovery of an interrupted
transaction. The durable builder now prepares the replacement Index, installs the intent, copies
and revalidates exact visible Records, and supports zero-output retirement. The internal durable
runtime now installs the prepared manifest, commit catalog, and Worker Index atomically, retires the
old sources, and fails closed whenever restart must complete recovery. Store-level exposure,
scheduling, and the complete crash matrix remain implementation work; durable compaction is
therefore not yet available through `Store`.

## Why the complete sealed history is one unit

Compacting one sealed Segment independently is unsafe. If it discards a tombstone while an older
put remains in another Segment, recovery can resurrect the deleted value. The same issue applies to
expired or superseded puts. One compaction transaction consequently selects every sealed Segment
owned by one Worker. The active Segment is never rewritten by that transaction.

The transaction copies only currently visible, non-expired puts whose `RecordRef` points into the
selected sealed set. Because every obsolete sealed Record for the Worker is retired together, no
discarded tombstone is needed to suppress a retained older value. Records are copied in increasing
sequence order and retain their v1 sequence, opcode, value type, flags, key hash, expiration, key,
and value.

## Identity and manifest plan

Outputs reuse the earliest source Segment IDs, increment their generations, remain sealed, and keep
their Worker owner. Surplus source entries disappear from the next manifest. The active entry,
`next_segment_id`, routing metadata, and every other Worker entry remain unchanged. This preserves
global Segment-ID ordering and keeps compacted sequence ranges before the Worker's active Segment
without changing Manifest, Segment, commit-slot, or Record v1 formats.

The planner rejects:

- a Worker without a sealed set or an output count larger than that set;
- exhausted manifest or selected Segment generations;
- plans that reclaim no physical Segment;
- temporary allocation, peak Store size, or physical write amplification outside
  `DurableResourceLimits`;
- a next manifest that does not pass the ordinary v1 encoder validation.

Physical write amplification is conservatively bounded as preallocated output Segment bytes divided
by Segment bytes reclaimed. Empty output is valid when the complete sealed history is obsolete.
Output sizing uses the exact sequence-ordered Record extents. Dividing aggregate live bytes by
Segment payload is only a lower bound because Records cannot span Segment boundaries; the builder
uses an allocation-free next-fit layout that also determines every future `RecordRef` before intent
publication. Peak Store accounting includes the output Segments, both external manifest files, and
both manifest payloads embedded in the intent.

## Publication and retirement protocol

The implementation must use this order:

```text
lock target Worker and freeze its Index
  -> collect and verify visible source Records
  -> prepare replacement Index publication without post-commit allocation
  -> publish and directory-sync an exact compaction intent
  -> create, fill, seal, and re-open validate every replacement
  -> atomically publish and directory-sync the next v1 Manifest
  -> publish prepared in-memory Index/catalog state
  -> close cached source descriptors
  -> unlink every old source name
  -> directory sync
  -> remove compaction intent
  -> directory sync
```

The intent is protocol metadata, not a second data format. It must bind the Store ID, Worker,
previous and next manifest generations, and exact source/replacement identities with a checksum.
It is durable before any unlisted replacement name can appear.

Before publishing that intent, the builder verifies the manifest is still authoritative, validates
every frozen Index reference against its catalog identity and routed source Record, drops expired
source puts at the supplied Store time, and constructs the complete replacement Index. Source
entries are ordered by their preserved sequence; one reusable Record buffer avoids per-read
allocation. After publication, encoded v1 Record bytes are copied exactly, batched into the planned
Segment boundaries, sealed, reopened, checksum-compared to their sources, and checked against exact
record-count, extent, and sequence metadata. Thus success requires no Index allocation after the
future manifest commit. A post-intent failure is explicitly `recovery_required` and restart rolls
the partial outputs back under the old authority.

The implemented intent codec stores the complete old and next v1 manifests under a fixed header
containing Store ID, Worker, both generations, exact payload lengths, format version, reserved-zero
space, and CRC32C. Decode validates both embedded manifests and reruns the canonical whole-Worker
transition validator; a header/payload disagreement, unknown version, trailing data, or a transition
that changes routing, active ownership, or next-ID state is rejected.

Filesystem publication writes a private exclusive temporary, synchronizes the complete intent,
renames it to the canonical name, and synchronizes the directory. Failures known to precede the
rename are `not_published` and clean the temporary; a rename attempt or later directory-sync failure
is `indeterminate` and poisons that `DataDirectory`. Reads validate a private singly linked regular
file, enforce the configured manifest-derived byte bound before allocating, read the exact stable
extent, and decode it. Intent removal distinguishes a fault before `unlinkat` (`not_removed`) from an
unlink attempt or subsequent directory-sync failure (`indeterminate`).

On restart, the runtime decodes and validates the intent before ordinary recovery, accepts only an
authoritative manifest exactly equal to its old or next manifest, and performs a complete recovery
scan of that authority before deleting anything. Namespace recovery admits only the canonical
intent and the exact non-authoritative Segment identities implied by the validated transition;
unrelated, malformed, linked, or missing authoritative entries still fail closed.

Recovery with an intent has only two valid authorities:

- if the old manifest is authoritative, all old sources must still exist; exact replacements named
  by the intent are rollback material and may be removed before the intent;
- if the next manifest is authoritative, every replacement must validate against it; remaining old
  sources are retirement material and may be removed before the intent;
- any other manifest generation, identity, missing authoritative file, or unrelated namespace entry
  fails closed without adoption.

Retirement opens every still-present obsolete name against its expected immutable identity before
unlinking. Missing obsolete names are accepted so recovery can resume after a partial cleanup. A
failure before the first unlink is `not_removed`; any failure after removal begins, including the
mandatory directory sync, is `indeterminate` and poisons that directory instance. A fresh reopen
repeats the same authoritative recovery, finishes the idempotent retirement batch, removes the
intent, and performs a final ordinary namespace audit.

No old source is unlinked before the new manifest and its directory entry are durable. `unlinkat`
is descriptor-relative. An already open file remains usable until its last descriptor closes on the
POSIX platforms, but GlyphaStore does not depend on that grace period: the target Worker lock drains
owning reads, and other Workers cannot reference its routed Segments. The directory is synchronized
after the retirement batch so restart observes either names still awaiting cleanup or their durable
removal.

## Cooperative scheduling policy

Compaction must not run in a mutation or acknowledgement critical path. The implemented internal
`compact_worker` transaction freezes only its target Worker. Other Workers continue reads and
ordinary mutations during copy and validation. A Store-wide publication mutex serializes rotation
and compaction authorities until intent removal; the short manifest and in-memory catalog switch
uses the catalog-exclusive lock. Cached Segment descriptors are keyed by immutable file identity,
not a manifest vector position, so removal of source entries cannot stale another Worker's cache.

The public `Store::compact()` scheduler is explicit and creates no background thread. It examines
eligible Workers in round-robin order, skips exact layouts that would reclaim no physical Segment,
and completes at most one transaction per call under the configured temporary-space and
amplification limits. A Store-local try-lock rejects a concurrent maintenance request instead of
queuing it. This bounds shutdown: `close()` either sees no compaction or waits only for the one
already admitted transaction to finish.

## Required evidence

Before P0-08 can close, tests must kill the process before and after intent publication, every
replacement write/sync/seal/validation boundary, manifest rename and directory sync, each unlink,
retirement directory sync, intent removal, and final directory sync. Reopen must recover exactly one
catalog and prove that concurrent owning readers never observe missing backing storage. Tests must
also cover tombstone non-resurrection, TTL reclamation, zero-output compaction, generation/resource
limits, allocation failure, and repeated compaction.
