# Durability and recovery contract

This document defines the persistence v1 engine contract implemented by the embedded Store. The
project remains an architectural prototype: process-termination evidence is present, but no
filesystem/device row is E3/E4 certified. Release criteria are in the
[persistence v1 production roadmap](../v1-production-roadmap.md). Normative terms such as **must**,
**must not**, and **may** describe required persistence-v1 behavior, not achieved release status.

The consolidated normative mapping from every durable intermediate state to its exact restart
outcome is the [recovery state-transition matrix v1](../spec/recovery-state-matrix-v1.md).

## Storage modes

`Store` creation selects exactly one storage mode:

- **volatile:** Segments and the Index live in memory. A successful mutation means the new state is
  visible to subsequent operations in the same process. No state is promised after process exit.
- **durable-sync:** a data directory is mandatory. A successful mutation means the mutation crossed
  the durable commit point and must be recovered after process or machine restart, subject to the
  documented filesystem and hardware guarantees.
- **durable-periodic:** a data directory is mandatory. A successful mutation means the mutation was
  published in memory and is visible to subsequent operations in the same process. The mutation is
  not promised after restart until a background or explicit flush has synchronized its Segment
  commit slot. The maximum loss window is `sync_interval_ms` plus any in-flight unflushed writes.
  See [ADR 0010](../adr/0010-durable-periodic-policy.md).
- **durable-group:** a data directory is mandatory. A successful mutation means the batch
  containing the mutation completed commit-slot publication and platform synchronization. Clients
  may block until the batch closes. There is no durability loss window. See
  [ADR 0011](../adr/0011-durable-group-commit.md).

Group commit batching may also be enabled for `durable-periodic` to reduce per-record commit-slot
writes while preserving the periodic visibility contract in [ADR 0010](../adr/0010-durable-periodic-policy.md).

## Persistent identities and versions

Disk, manifest, Record, and wire versions are independent. The manifest records at least:

- Store identity generated at creation;
- manifest format version;
- Segment and Record format versions;
- routing algorithm identifier and seed (FNV-1a by default; persisted SipHash-2-4 for keyed Stores);
- persisted Worker count and routing epoch;
- the complete Segment catalog and active Segment for every Worker;
- the next Segment ID and generation information needed to prevent stale references.

Worker auto-sizing runs only when a durable Store is created. Reopen uses the persisted Worker
count, even when current machine topology differs. Supplying an incompatible Worker count, routing
algorithm, or routing epoch must fail before any file is mutated. Online resizing and routing
migration are outside the 0.1.x scope.

Unknown required format versions fail closed. Optional forward-compatible fields must be length
delimited and safely skippable. No decoder may allocate directly from an unchecked persisted size.
The exact implemented Segment header v1 layout is specified in
[`segment-format.md`](segment-format.md); a correctly checksummed unknown commit-slot version is an
incompatibility and cannot be treated as a damaged slot eligible for fallback.
The exact implemented manifest v1 layout and catalog invariants are specified in
[`manifest-format.md`](manifest-format.md).

## Data directory

A durable Store owns one exclusive process lock for its data directory. The directory contains a
single authoritative manifest and a Segment namespace. Temporary publication files and orphaned
Segments are distinguishable from committed files by name and manifest membership.

The implementation must not use `/tmp` as a default. It must reject symlinks or other filesystem
objects that violate the documented data-directory policy. Filenames are implementation details;
Store identity, Segment identity, generation, and ownership come from validated encoded metadata,
not from parsing a filename alone.

Unknown files are reported and quarantined or rejected according to an explicit operator policy;
they are never silently added to the Store. A manifest-listed file that is absent, duplicated, or
inconsistent is corruption and prevents read-write open.

## Resource preflight

Durable resource policy is supplied at open and is deliberately absent from v1 metadata. Reopen
first limits manifest bytes before allocating its decode buffer, then checks Segment count, steady
logical Store bytes, descriptor policy and `RLIMIT_NOFILE`, recovery memory, and Worker-partitioned
live-key capacity. A smaller policy fails read-only with a stable resource category.

Creation computes its peak as all initial 64 MiB Segments plus the simultaneously present bootstrap
intent and manifest. Rotation computes the replacement Segment plus current and replacement
manifest generations. It samples space available to the unprivileged process and preserves
`reserved_free_bytes`; failure occurs before intent publication or active-Segment sealing. This
sample cannot reserve against unrelated processes, so native full-file preallocation remains
mandatory and its `ENOSPC`/quota result is authoritative.

Recovery scans one Worker at a time and charges a conservative estimate for catalog arrays, Worker
state, temporary latest-key nodes, duplicated key bytes, and the resulting Index. It returns
`resource_exhausted` before exceeding the configured estimate. This is a safety ceiling rather than
an allocator-exact telemetry value.

## Segment header and commit slots

Every Segment remains exactly 64 MiB. Its first 4 KiB contain an explicitly little-endian header
with immutable identity fields and two independently checksummed commit slots. No field is decoded
through C++ object layout.

Immutable header fields include at least Segment magic and format version, Store identity, Segment
ID, generation, owner Worker, Record format version, and header size. Each commit slot contains at
least:

- slot format version and CRC32C;
- monotonic commit generation;
- committed end offset;
- committed Record count;
- first and last committed sequence where present;
- lifecycle state required for active/sealed recovery.

The committed end starts at byte 4096, is 8-byte aligned, never exceeds Segment capacity, and never
moves backwards. Writers alternate slots and never overwrite the only known-valid newest slot.
Recovery validates both slots and selects the valid slot with the greatest commit generation. Two
different valid slots with the same generation are corruption unless their complete decoded
contents are identical.

Bytes after the selected committed end have no logical meaning and are never scanned. Corruption,
checksum failure, or an invalid Record inside the committed extent is fatal corruption; it is not
treated as a crash tail. This distinction is the reason commit slots are required.

## Mutation ordering and acknowledgement

Durable-sync mutation processing follows this order:

1. Validate arguments, limits, ownership, sequence availability, and all fallible publication
   capacity needed by the in-memory Index.
2. Encode the complete immutable Record outside the committed extent.
3. Write all Record bytes and establish the platform Record-before-slot ordering boundary.
4. Encode the alternate commit slot with the new extent and sequence, write it, and synchronize
   the Segment again.
5. Treat successful synchronization of that commit slot as the durable commit point.
6. Publish the `RecordRef`, liveness changes, and visibility in memory.
7. Return success or encode the successful network response.

Failures before step 5 return an error and must not become visible after recovery. Once step 5 has
succeeded, publication failure is a fail-closed engine event because recovery will observe the
mutation. The process must not continue serving a state that disagrees with its durable log.

An acknowledged mutation must survive restart. A client that loses its connection after the
commit point but before receiving the response has an indeterminate outcome and must read the key
before deciding whether to retry. Idempotency keys and cross-key transactions are not part of the
current contract.

Volatile mode retains the current append-then-publish behavior but must still fail closed if a
rollback cannot restore a coherent Index/liveness state.

### Durable-periodic mutation ordering

Durable-periodic mutation processing follows this order:

1. Validate arguments, limits, ownership, sequence availability, and all fallible publication
   capacity needed by the in-memory Index.
2. Encode the complete immutable Record outside the committed extent.
3. Write all Record bytes without synchronizing the Segment data or publishing a commit slot.
4. Publish the `RecordRef`, liveness changes, and visibility in memory.
5. Return success or encode the successful network response.
6. When the batch closes, establish the platform Record-before-slot ordering boundary, publish one
   alternate commit slot, and synchronize that slot. `Store::flush()` and orderly shutdown force the
   same sequence.

Failures before step 4 return an error and must not become visible in the running process. A
successful response does not imply restart durability until step 6 completes. An unflushed
mutation may or may not survive a crash, but Record-before-slot ordering ensures that a surviving
valid slot never authorizes an unsynchronized Record extent.

`Store::flush()` and `Store::close()` synchronize all dirty Segment files before releasing the
data-directory lock. Close returns the sticky final-flush result; destruction uses the same path but
cannot expose its status. Flush failure is fail-closed.

### Durable-group mutation ordering

Durable-group mutation processing follows this order:

1. Validate arguments, limits, ownership, sequence availability, and publication capacity.
2. Encode the complete immutable Record outside the committed extent.
3. Write all Record bytes without publishing a commit slot.
4. When the Worker batch closes by its adaptive record target, byte threshold, or `max_wait_ms`,
   establish the platform Record-before-slot ordering boundary, publish one commit slot, and
   synchronize the Segment again. The record target remains within the configured `min_records`
   and `max_records` bounds.
5. Publish the `RecordRef`, liveness changes, and visibility in memory for each mutation in the
   closed batch while the commit executor owns the Worker lock.
6. Return success or encode the successful network response.

Failures before step 4 return an error and must not become visible after recovery. Once step 4 has
succeeded for a batch, every mutation in that batch must survive restart. The commit executor
completes all in-memory publication before waking the batch waiters; a flush or publication failure
wakes all waiters with the runtime fail-closed. A full Segment first closes any staged batch before
rotation. The one-Worker runtime delegates the commit phases to its durability coordinator and
bounds admission when the adaptive record target or `max_bytes` closes the batch. Deadline-limited
occupancy contracts the next target; already-admitted producer pressure grows it, without changing
the absolute deadline or acknowledgement semantics. A multi-Worker Store keeps independent
Worker-local producer-closed batches and commit domains.

### Durable-periodic batching

When batching is enabled for `durable-periodic`, step 3 writes Record bytes without publishing a
commit slot. Steps 4 and 5 occur immediately after each Record write. A foreground threshold or
background deadline then performs step 6 for the accumulated extent; `Store::flush()` and orderly
shutdown force it synchronously.

## Segment creation, rotation, and manifest publication

Before the first committed Record enters a new Segment, durable-sync mode must:

1. create and size a temporary Segment file;
2. write and synchronize its initial header and commit slots;
3. publish its final filename and synchronize the Segment directory;
4. publish a new manifest using temporary file, file synchronization, atomic rename, and parent
   directory synchronization;
5. only then make the Segment writable by its owner.

Rotation seals and synchronizes the old Segment first, creating an unambiguous intent marker. It then
creates and synchronizes the exact next-identity replacement and publishes the manifest that makes
the replacement active. Runtime rotation copies the old Segment identity before replacing the
in-memory manifest, so cache retirement never retains an iterator into the replaced catalog. A crash
may therefore leave the selected manifest naming a sealed active
Segment, with or without one pristine prepared replacement. Runtime open validates or creates only
that exact replacement and publishes the completed rotation before serving writes. It never reopens
the sealed Segment for append; other lifecycle mismatches are corruption.

A created Segment absent from the selected manifest is an orphan, not part of the Store. Engine
temporary files and otherwise valid orphan Segments may be quarantined without adoption; malformed
or unexpected objects follow the configured reject/quarantine policy. Recovery must advance or
safely reserve Segment IDs so an orphan can never collide with a newly created Segment.

Manifest generations are monotonic. Manifest publication never modifies the currently selected
manifest in place. If the platform cannot provide the required atomic replacement and directory
synchronization semantics, durable-sync open must fail as unsupported.

The implemented low-level ordering, platform flush choices, namespace protections, and publication
outcomes are specified in the [persistence filesystem layer](persistence-filesystem.md). Exact-size
creation, alternate-slot commit/seal, and bounded recovery scan are implemented in the
[durable Segment-file layer](segment-filesystem.md). Manifest-driven Segment validation, partitioned
Index rebuild, sequence restoration, and interrupted-rotation detection are implemented in the
[durable recovery orchestrator](recovery-implementation.md). Descriptor-relative namespace audit and
strict normal-recovery policy are implemented in the [namespace policy](namespace-policy.md).
Bounded verified reads, durable mutation ordering, and exact-intent rotation completion are
implemented in the [durable runtime catalog](durable-runtime-catalog.md) and owned by the public
Store PImpl. Offline inspection and fail-closed repair tools exist; live/hot backup and in-place
destructive rewrite remain out of the ordinary open path.

## Recovery

Recovery executes before network listeners start and before the Store accepts operations:

1. acquire the exclusive data-directory lock;
2. select and validate the newest complete manifest publication;
3. validate Store identity, versions, routing metadata, and catalog invariants;
4. open every manifest-listed Segment and validate its identity against the catalog;
5. select the newest valid commit slot for each Segment;
6. scan exactly each committed extent and validate every Record checksum and bound;
7. require every non-empty Segment's first sequence to exceed the preceding committed range for
   that Worker;
8. rebuild visibility by full key and highest sequence, with tombstones and expiration suppressing
   older values;
9. partition rebuilt entries using the persisted routing configuration;
10. restore each Worker's next sequence using checked `maximum + 1` arithmetic;
11. validate the complete rebuilt state before publishing the Store as ready.

Filesystem enumeration order must not affect the result; canonical Manifest order is authoritative.
Equal, overlapping, or reversed per-Worker Segment ranges fail recovery. Equal highest sequences for
the same key are a conflict. Sequence exhaustion fails read-write open. A future read-only salvage
mode must be a separate, explicit operator action and must never silently weaken normal open.

Expiration uses absolute Unix-epoch nanoseconds. Recovery retains the newest Record decision even
when that Record is already expired; it must not resurrect an older value. Clock rollback behavior
and test clock injection belong to the public time contract.

## Failure classification

- An uncommitted trailing write is ignored because it lies beyond the selected committed extent.
- A malformed or checksummed Record inside a committed extent is corruption.
- A missing or inconsistent manifest-listed Segment is corruption.
- An unknown required version is incompatibility, not generic corruption.
- Disk-full and I/O failures before the commit point fail the operation without acknowledgement.
- An I/O or publication failure after the commit point makes the runtime unhealthy and stops
  service until recovery.

Errors must preserve these categories for CLI diagnostics and automation. Recovery must never
repair, truncate, delete, or rewrite source files during an ordinary read-write open. Repair and
salvage tools operate on a copy or require an explicit destructive flag added under a separate
contract.

## Required evidence

Persistence-v1 release evidence requires CI coverage of:

- golden fixtures for manifest, Segment header, commit slots, and Records;
- restart tests across every mutation and rotation transition;
- process-kill and injected short-read/write, `EINTR`, writeback `EIO`, rename, disk/quota/read-only,
  and allocation failures;
- active-tail tolerance and committed-region corruption rejection;
- manifest rollback and explicit orphan quarantine/identity reservation;
- routing and Worker-count mismatch rejection;
- recovery independence from Segment enumeration order;
- compatibility reads using artifacts emitted by every supported released format version.

Current development coverage is recorded in the
[format compatibility matrix](format-compatibility.md). Cross-release compatibility and pinned
filesystem power-loss campaigns are release gates (see
[platform durability evidence](platform-durability-evidence.md)).

Codec unit tests alone are not durability evidence.
