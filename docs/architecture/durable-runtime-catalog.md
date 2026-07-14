# Durable runtime catalog and mutation state machine

This internal layer materializes a recovered durable Store into per-Worker Indexes and a bounded
file-backed runtime. Public `durable_sync` remains disabled while public Store construction and
creation of a new durable directory are still separate.

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
completion, and exact prepared-replacement adoption.

Still required before public durable mode is enabled:

- public Store integration and new-directory creation;
- fault injection and process-kill tests at every mutation and rotation boundary;
- retirement/vacuum manifest publication;
- disk-full and native Linux/FreeBSD/OpenBSD evidence.
