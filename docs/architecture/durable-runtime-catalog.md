# Durable runtime catalog and mutation state machine

Status: descriptive of the implemented durable backend
Applies to: persistence v1 under all durable Store policies
Owner: persistence maintainers
Last reviewed: 2026-08-26

This layer materializes a recovered durable Store into per-Worker Indexes and a bounded file-backed
runtime. `Store::open` owns it through the public PImpl for `durable_sync`, `durable_group`, and
`durable_periodic`.

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
Index into its runtime Worker, and retains the lock for its lifetime. It then opens one immutable
read handle per exact catalog Segment generation. Generation pins and mutable Segment handles are
destroyed before the directory owner.

Rotation completion is the sole mutating open transition. All other invalid namespaces and recovery
failures leave files untouched and release the lock.

## Bounded descriptors and verified reads

Each Worker owns one mutex and at most one mutable active-Segment descriptor. In default paired mode,
the per-shard execution token/dedicated Writer is the sole mutation owner; the mutex remains for
maintenance, legacy mode, and durable group/periodic state shared with the flush coordinator. The
`durable_sync` exclusive Writer hot path may elide it under the quiescence protocol. The catalog owns
one immutable shared generation pin, including a read-only descriptor, per published Segment.
Ordinary mutations use only the required shared catalog/publication boundary; different Workers
therefore proceed concurrently. Rotation takes the exclusive catalog lock because it appends the
globally ordered Manifest and commit-state vectors.
Online compaction briefly snapshots one target Worker, owns copied Index entries and exact sealed
generation pins, then performs replacement I/O without the Worker or catalog lock. Publication
try-locks the Worker, takes the catalog lock, and succeeds only if sequence, batch, manifest, source,
and pin tokens still match; otherwise the prepared old-authority transaction is durably rolled back.
A logical publication lease makes a second compaction fail fast and makes rotation wait without
retaining the physical serializer during replacement I/O. The waiting rotation releases the
publication mutex, then rebuilds its transition from the newly published authority. Final manifest
I/O also holds no Worker, catalog, or publication mutex: a Worker-local commit gate rejects target
mutations, lets reads continue, and makes flush wait with its mutex released. After durable
publication, the allocation-free in-memory switch refreshes commit metadata for other Workers
before clearing that gate. No third manifest authority can appear while the lease exists.

In paired mode a cold read obtains its `RecordRef` and exact Segment-generation pin from the
immutable published `ReadGeneration`; completion validates that published authority before use. The
legacy/internal mutable-Index path captures the same pair under Worker/catalog synchronization and
revalidates it afterward. In both cases positional I/O, decoding, CRC32C, sequence, key-hash,
full-binary-key, opcode, expiration validation, and owning value copy happen without a Worker,
catalog, or file-cache mutex. A Manifest publication may retire the pathname while an already pinned
descriptor remains valid: a `RecordRef` never crosses a synchronization boundary without generation
ownership and an explicit linearization check. Normal misses and expiration do not poison the
runtime; corruption or I/O disagreement on a still-current pin makes later mutations fail closed.

After a validated expiration (generation/cache hit or cold Record visit), the runtime returns `not_found`
immediately and never serves the expired value. Physical Index removal is queued on a bounded
per-Worker deferred-TTL backlog and drained by existing Worker paths (`prepare_get`, `mutate`) with
exact `RecordRef` verification so a concurrent reinsert is never erased. Matching hot-cache rows are
dropped immediately. Compaction remains the durable TTL cleanup path (`expired_records_dropped`).
A zero backlog limit forces synchronous Index reclaim. Repeated GETs after a successful drain are
Index misses and perform no Segment I/O.

The legacy/internal per-Worker active-Record hot cache is a Swiss-style flat open-addressed table
(power-of-two capacity, maximum load 0.75, geometric growth, 8-slot control groups, H2
fingerprints, SIMD/scalar group matching shared with the Index via
`swiss_control_group.hpp`, values inlined up to 48 bytes).
Bucket arrays are allocated on first successful admission, so disabled and never-used Worker caches
have zero table storage. Each resident slot uses its `RecordRef` as the only sequence identity,
derives inline/heap representation from value size, and does not duplicate its recalculable byte
charge. It is bounded by deterministic shares of a global byte budget, a per-Worker
byte cap, a staging-byte cap, and an entry cap. Admission prepares the immutable value before
persistent writes; exhaustion bypasses the cache and never rejects an otherwise valid mutation.
Hash is never identity — full key bytes are compared on every fingerprint candidate. Accounting
charges fixed bucket arrays once, external resident key/value payload separately, and complete
temporary publication state while staged. Accounted resident/staged/bucket bytes, limits, hits,
misses, stale hits, evictions, size rejects, expired-TTL GETs, and admission bypasses are observable
without a global cache lock. Default paired opens disable this duplicate read authority and use only
the immutable generation; zero-cache operation therefore remains correct. Durable GET path timing
(mutex wait, prepare/complete hold, Index/hot/pin lookup, cold read, CRC/value copy,
relinearization retries) is published through
`get_path_stats()` with relaxed atomics so the Worker critical section stays short; fine-grained
clock sampling compiles out of Release unless `GLYPHASTORE_GET_PATH_TIMING` is set. The mutable
`prepare_get` compatibility path holds only the Worker mutex (the catalog shared lock is taken only
on cold-miss pin acquisition). A cache hit snapshots shared immutable ownership under the Worker
mutex and performs the value-sized owning copy after unlocking. On an active-generation miss, the
pinned runtime reader may
exceed its handle's opening boundary only for the exact authoritative `RecordRef`; the mandatory
post-I/O Index and pin revalidation above is its linearization point. Rotation removes all entries
charged to the retired active generation.

The steady-state Segment-descriptor bound is the catalog Segment count plus the Worker count. Cold
read concurrency reuses immutable pins and therefore does not increase that bound. Catalog lookup on
the GET path resolves `SegmentId` → pin slot in O(1) via a dense side table rebuilt on recovery,
rotation, and compaction publication; identity, generation, owner, and pin-object checks remain
mandatory. Other catalog walks may still binary-search the ordered manifest.

Configured descriptor policy must cover generation pins, Worker mutable handles, directory, lock,
enumeration, and transient publication descriptors and must fit the process `RLIMIT_NOFILE`.
New-key admission uses a deterministic Worker share of the global live-key budget. Rotation validates
Segment, manifest, peak-byte, and currently available-space budgets before sealing the old active
Segment; a budget failure is therefore `not_committed` and leaves the existing rotation state
unchanged.

## Durable mutation order

The owning paired mutation executor (or the legacy Worker lock) serializes these transitions:

1. Prepare Swiss-table capacity, long-key arena storage, bounded hot-cache ownership, and the
   complete encoded Record.
2. Append complete Record bytes.
3. Write the alternate commit slot and apply the synchronization required by the selected policy.
4. Publish the `RecordRef`, advance the Worker sequence, and publish the coherent read generation.

Strict sync/group acknowledgement crosses the required Segment synchronization before success;
periodic acknowledgement may precede the later forced boundary exactly as specified by persistence
v1. No allocation is required after the persistent commit. Erase appends a tombstone and uses a
deliberately non-compacting, non-allocating Index removal after commit; volatile erase retains safe
arena reclamation. Results distinguish `committed`, `not_committed`, and `indeterminate`. Any
post-commit publication error reports the committed boundary and makes the runtime fail closed.
Mutations and mutable Index GETs reject while unhealthy; immutable published-generation reads
(`prepare_published_get` / pin-backed complete) remain servable so ACK-after-publish RAW holds.

Strict group commit with one v1 Worker separates admission from commit execution. Producers stage
bounded pending mutations and wait for completion; the durability coordinator owns Record ordering,
commit-slot synchronization, whole-batch Index publication, and waiter wakeup. An adaptive record
target bounded by configured minimum and maximum values closes admission until that batch completes.
A deadline contracts the next target to observed occupancy; admitted producer pressure grows it.
The first Record still schedules one absolute batch deadline, and explicit `flush()` is dispatched
to and completed by the same coordinator thread. A multi-Worker Store retains independent
Worker-local batches and commit domains.

Batch observability is maintained in a cache-line-aligned Worker-local block and read without taking
the Worker or catalog mutex. It reports pending records/bytes, current adaptive target, flush
attempts/failures, committed occupancy and maxima, close reasons, and total/maximum
`flush_pending_commit` duration. In strict group mode that duration covers the synchronized v1 batch
commit boundary. In periodic/deferred mode it measures deferred commit publication only; the later
whole-store dirty flush is a distinct operation and must not be interpreted as included sync time.

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

## Tests

Tests cover binary reads, expiration, concurrent readers, a blocked cold `pread` that does not
block a same-Worker mutation, blocked compaction build and manifest-sync concurrency, waiting and
restart-durable unrelated rotation, close/rollback, lock lifetime, sticky corruption handling,
preflighted long-key publication, plus put, replace, and tombstone reopen across restart,
sealed-active completion, and exact prepared-replacement adoption. A separate
allocator-interposition executable
fails every allocation observed in native put, update, erase, owning-read, strict-group, and rotation
paths. It reopens every pre-write/interrupted-rotation failure, requires uncertain paths to fail
closed, releases background waiters after `bad_alloc`, and forbids any allocation after the ordinary
Record write boundary through coherent runtime publication.

Related release gates: native-platform process-kill and power-loss evidence; online compaction
kill/fault matrix completeness; disk-full and native Linux/FreeBSD/OpenBSD rows. See
[production readiness](../production-readiness.md).
