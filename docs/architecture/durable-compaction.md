# Crash-safe durable compaction

This document defines the v1 compaction invariants. The current tree implements the deterministic
planner, its resource gates, a checksummed intent codec embedding both exact manifest authorities,
descriptor-relative intent publication/removal primitives, and restart recovery of an interrupted
transaction. The durable builder now prepares the replacement Index, installs the intent, copies
and revalidates exact visible Records, and supports zero-output retirement. The internal durable
runtime now installs the prepared manifest, commit catalog, and Worker Index atomically, retires the
old sources, and fails closed whenever restart must complete recovery. Explicit Store-level
scheduling is available through `Store::compact()`. An optional Store-owned
[MaintenanceController](maintenance-controller.md) (ADR 0023) may observe and schedule that primitive
under explicit modes and budgets. The single-output online kill/fault matrix is complete.
Multi-output recovery now covers partial replacement rollback and partial source retirement; the
differential online SIGKILL matrix covers every persistence transition unique to a second
replacement. An opt-in exhaustive profile covers every individual Record-copy occurrence.
An additional fixed-seed profile exercises randomized multi-output PUT/overwrite/ERASE/TTL
histories across old- and next-authority checkpoints. Native power-loss campaigns remain
certification work.

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
briefly lock target Worker + catalog
  -> copy owning Index entries and pin exact sealed generations
  -> release both locks
  -> reserve the exact old/next authority pair under the publication serializer
  -> release the serializer; rotations now fail fast while the logical lease exists
  -> collect and verify visible source Records
  -> prepare replacement Index publication without post-commit allocation
  -> publish and directory-sync an exact compaction intent
  -> create, fill, seal, and re-open validate every replacement
  -> try-lock target Worker and lock catalog
  -> reject if sequence, batch, manifest, source identity, or pin changed
  -> prepare the complete non-allocating in-memory publication
  -> mark the target Worker commit-gated and release both locks
  -> atomically publish and directory-sync the next v1 Manifest with no mutex held
  -> re-lock, merge retained commit metadata, and publish prepared in-memory state
  -> clear the commit gate and release both locks
  -> unlink every old source name
  -> directory sync
  -> remove compaction intent
  -> directory sync
```

The intent is protocol metadata, not a second data format. It must bind the Store ID, Worker,
previous and next manifest generations, and exact source/replacement identities with a checksum.
It is durable before any unlisted replacement name can appear.

Before publishing that intent, the builder verifies the manifest is still authoritative, validates
every snapshot Index reference against its catalog identity and routed source Record, drops expired
sealed source puts at the supplied Store time (counted in `expired_records_dropped`), and constructs
the complete replacement Index. Active-Segment Index entries are preserved by identity without a TTL
visit; those expired keys remain until lazy GET reclaim or the next recovery rebuild. Source
entries are ordered by their preserved sequence; one reusable Record buffer avoids per-read
allocation. After publication, encoded v1 Record bytes are copied exactly, batched into the planned
Segment boundaries, sealed, reopened, checksum-compared to their sources, and checked against exact
record-count, extent, and sequence metadata. Thus success requires no Index allocation after the
future manifest commit. A post-intent failure is explicitly `recovery_required` and restart rolls
the partial outputs back under the old authority.

TTL reclaim has three cooperating paths: recovery omits expired latest puts from the rebuilt Index;
validated GET lazily removes Index/hot entries without a durable tombstone; sealed compaction drops
Index-resident expired puts from the replacement catalog and physically omits them from replacement
Segments. `Store::compact()` / `MaintenanceSnapshot` expose `expired_records_dropped` so operators
can measure sealed TTL reclaim independently of bytes/records copied.

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
is descriptor-relative. A cold reader carries shared ownership of the exact immutable generation
and its already-open descriptor; source retirement can remove the pathname after publication without
invalidating that reader. Its final locked validation observes the replacement `RecordRef`/pin and
retries before returning. The directory is synchronized after the retirement batch so restart
observes either names still awaiting cleanup or their durable removal.

## Cooperative scheduling policy

Compaction must not run in a mutation or acknowledgement critical path. The implemented internal
`compact_worker` takes a brief owning snapshot, then performs source reads, CRC validation,
replacement writes, sealing, and reopen validation without the target Worker or catalog lock. Reads
and ordinary mutations continue on that Worker. A mutation wins publication: sequence/batch drift
causes a finite `sequence_conflict`, durable removal of replacement files and intent under the still
authoritative old manifest, and no fail-close unless rollback itself becomes uncertain. Publication
uses a non-waiting Worker try-lock and the catalog-exclusive lock only to validate and prepare
already-reserved vectors. It then installs a Worker-local logical commit gate and releases every
physical mutex before manifest write, sync, rename, and directory sync. Reads continue through the
old Index and pinned sources. Mutations on that Worker receive a finite `sequence_conflict`; flush
paths wait on a condition variable that releases the Worker mutex. After durable publication, the
runtime reacquires the locks, refreshes retained commit metadata that other Workers may have
advanced, performs the allocation-free in-memory switch, and clears the gate. Source retirement and
final audit also run unlocked.

Persistence v1 admits exactly the old and next manifest authorities. A Store-wide logical
publication lease preserves that invariant without holding the publication mutex through intent
installation, replacement creation, manifest I/O, or retirement. Rotation and a second compaction
take the physical serializer only long enough to observe the lease. A second compaction returns a
finite `sequence_conflict`; rotation waits on a condition variable, then builds from the newly
authoritative Manifest and commits. The wait avoids rejected writes but can extend the latency of
the owning Worker; it does not permit a third manifest authority. Ordinary non-rotating operations
on other Workers continue. This keeps persistence v1 unchanged.

The public `Store::compact()` scheduler is cooperative and creates no background thread by itself.
It examines eligible Workers in round-robin order, skips exact layouts that would reclaim no physical
Segment, and completes at most one transaction per call under the configured temporary-space and
amplification limits. A Store-local try-lock rejects a concurrent maintenance request instead of
queuing it. This bounds shutdown: `close()` either sees no compaction or waits only for the one
already admitted transaction to finish.

The first public-path benefit/cost measurement is the
[2026-07-23 durable compaction benchmark](../benchmarks/durable-compaction-2026-07-23.md). It
validates high/medium/low reclaim, copy-heavy, TTL, and no-gain layouts through close/reopen and
full model checks. It is exploratory evidence from a dirty macOS/APFS build; foreground tail
latency, controlled cross-platform runs, and automatic-policy tuning remain release work.

When `StoreConfig::maintenance.mode` is `background`, a Store-owned
[MaintenanceController](maintenance-controller.md) may run one evaluation thread and invoke
`Store::compact()` under Phase 1 normal-policy budgets. The controller shares the same non-queuing
maintenance gate.

## Required evidence

The process-kill matrix now addresses intent publication, each of two replacement writes, the batched
Record sync, both commit-slot publications (data boundary and seal), replacement rename, manifest
publication, both source unlinks, intent removal, and all five directory-sync occurrences. Every case
reopens through the ordinary recovery entry point, preserves both values, and verifies the rebuilt
Index. A complementary pre-operation I/O-fault matrix covers the same 25 boundaries and checks both
runtime outcome/health and clean reopen. Allocator interposition enumerates every allocation observed
by the online transaction and reopens after each failure. Existing tests also cover tombstone
non-resurrection, TTL reclamation, zero-output compaction, generation/resource limits, and repeated
compaction. An online end-to-end case compacts 64 maximum-size live Records from three sealed
sources into exactly two replacements, verifies every rebuilt reference, and reopens every value.
Four independent fixed-seed histories apply 608 total PUT/ERASE/expiring-PUT operations across 32
keys, three sealed sources, and the active Segment; each sequential model must agree before
compaction, after in-memory installation, and after ordinary reopen. A failure reports the exact
reproduction seed. Five additional seeds combine another 760 operations with occurrence-specific
I/O failure before intent write, during the third Record copy, at Manifest synchronization, during
the second source unlink, and before intent removal. Each case verifies the returned outcome,
healthy/fail-closed state, selected old/next authority, clean namespace, and the same model after
reopen.
Two-output recovery additionally injects a failure and SIGKILL after each partial replacement
rollback and partial source retirement position, then proves idempotent cleanup of every remaining
identity. Rollback removes both the canonical and partially created temporary name for each obsolete
replacement identity, including a crash during creation of the second output. The single-output
online SIGKILL scenario now seeds a fixed 30-operation
PUT/ERASE/expiring-PUT history across two sealed sources and the active Segment; the same eight-key
model must survive every one of its 25 persistence checkpoints. A complementary 15-checkpoint
online 3-to-2 scenario covers the transitions not present in that single-output transaction:
second-replacement preallocation/header/sync/rename, its directory sync, the final of 64 maximum-size
Record writes, its distinct data and seal commits, the shifted Manifest/retirement/intent directory
syncs, and the third source unlink. The separate `copy-matrix` kills after each of the other 63
Record writes, so the two profiles cover every one of the 64 copies and 154 distinct checkpoints in
total. The source seed batches pending Records and makes them durable at seal so exhaustive evidence
does not add a sync per seed Record. The `random-matrix` adds four reproducible 96-operation
histories, each combining PUT, overwrite, ERASE, expired PUT, and restoration while retaining
exactly 64 maximum-size live values. It SIGKILL-tests nine checkpoint classes per seed: durable
intent, early/middle/final copy, second-output creation and seal, Manifest authority change,
partial source retirement, and final cleanup. Native power-loss evidence remains before P0-08 can
be certified across all supported platforms.
