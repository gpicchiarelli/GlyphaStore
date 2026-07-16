# Durable runtime catalog and mutation state machine

This layer materializes a recovered durable Store into per-Worker Indexes and a bounded file-backed
runtime. `Store::open(durable_sync)` owns it through the public PImpl.

## Initial bootstrap

Creation publishes a CRC-protected bootstrap intent containing the complete canonical initial
manifest using temporary-file, file-sync, rename, and directory-sync ordering. The manifest is then
published, one pristine active Segment is created per Worker, and finally the intent is removed and
the directory synchronized. A restart may complete missing initial Segments only while this exact
intent exists and only after verifying every present Segment is pristine. Without the intent, a
missing catalog Segment is corruption rather than an initialization state.

## Open and ownership

`DurableRuntimeCatalog::open_existing` locks the data directory, completes only an exactly validated
interrupted rotation, performs full manifest/namespace/Segment/Record recovery, moves each recovered
Index into its runtime Worker, and retains the lock for its lifetime. Cached Segment handles are
destroyed before the directory owner.

Rotation completion is the sole mutating open transition. All other invalid namespaces and recovery
failures leave files untouched and release the lock.

## Bounded descriptors and verified reads

Each Worker owns one mutex and at most one cached Segment descriptor. Reads and ordinary mutations
hold a shared catalog lock; different Workers therefore proceed concurrently. Rotation alone takes
the exclusive catalog lock because it appends the globally ordered manifest and commit-state vectors.

A read-only cache miss reopens with no-follow and private-file checks, validates the complete identity
and exact recovered commit snapshot, and then verifies the Record extent, CRC32C, sequence, key hash,
full binary key, opcode, and expiration. Only owning bytes escape in `OwnedValue`. Normal misses and
expiration do not poison the runtime; corruption or I/O disagreement makes later operations return
`unavailable`.

The steady-state Segment-descriptor bound is the Worker count. Catalog lookup is binary search over
the strictly ordered manifest, avoiding a second potentially million-entry map.

## Durable mutation order

Put performs these transitions while holding its Worker lock:

1. Prepare Swiss-table capacity and long-key arena storage, then encode the complete Record.
2. Write and synchronize Record bytes.
3. Write the alternate commit slot and synchronize the Segment.
4. Publish the `RecordRef` in the prepared Index and advance the Worker sequence.

No allocation is required after the persistent commit. Erase appends a tombstone and uses a
deliberately non-compacting, non-allocating Index removal after commit; volatile erase retains safe
arena reclamation. Results distinguish `committed`, `not_committed`, and `indeterminate`. Any
post-commit publication error reports the committed boundary and makes the runtime fail closed.

Strict group commit with one v1 Worker separates admission from commit execution. Producers stage
bounded pending mutations and wait for completion; the durability coordinator owns Record ordering,
commit-slot synchronization, whole-batch Index publication, and waiter wakeup. A threshold closes
admission until that batch completes. The first Record schedules an absolute batch deadline, and
explicit `flush()` is dispatched to and completed by the same coordinator thread. A multi-Worker
Store retains independent Worker-local batches and commit domains.

## Observable shutdown

`Store::close()` first changes the public admission state from open to closing. Calls that already
acquired admission finish; later reads, mutations, verification, and flush calls return
`unavailable`. Before waiting, close posts a non-blocking force-all request so a strict-group
producer cannot deadlock waiting for a long batch deadline. Once all active-call counters reach
zero, the runtime performs one final force-all flush on the coordinator, stops and joins the
executor, and releases the runtime and anchored data-directory lock. Admission accounting is
cache-line-isolated per Worker, plus one control shard, so independent Workers do not contend on a
single lifecycle counter during normal operation.

Concurrent close callers observe the same cached result. A final flush or background callback
failure is sticky and makes the runtime fail closed. Destruction invokes close and discards its
status; applications that must distinguish clean shutdown from possible durability failure call
`close()` explicitly.

## Crash-safe rotation

Rotation first seals the old manifest-active Segment. That durable lifecycle change is its intent
marker. It then creates the replacement using exactly the manifest next ID/generation and publishes a
new manifest marking the old Segment sealed and the replacement active.

At restart, an unlisted Segment is adoptable only if there is exactly one sealed-active marker and
the file has the exact next identity, same owner, correct Store ID, exact size, and pristine initial
active commit (generation 1, zero Records, offset 4096). No arbitrary orphan is adopted. If the crash
preceded creation, open creates the replacement. If manifest rename may already have happened, normal
manifest and namespace recovery selects and validates the completed state.

## Evidence and remaining gates

Tests cover binary reads, expiration, concurrent readers, lock lifetime, sticky corruption handling,
preflighted long-key publication, puts/replacements/tombstones across restart, sealed-active
completion, and exact prepared-replacement adoption. A separate allocator-interposition executable
fails every allocation observed in native put, update, erase, owning-read, strict-group, and rotation
paths. It reopens every pre-write/interrupted-rotation failure, requires uncertain paths to fail
closed, releases background waiters after `bad_alloc`, and forbids any allocation after the ordinary
Record write boundary through coherent runtime publication.

Still required before durability can be certified:

- native-platform process-kill and power-loss evidence at every mutation and rotation boundary;
- retirement/vacuum manifest publication;
- disk-full and native Linux/FreeBSD/OpenBSD evidence.
