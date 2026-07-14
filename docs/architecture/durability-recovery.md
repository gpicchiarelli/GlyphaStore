# Durability and recovery contract

This document defines the target contract for the alpha persistent engine. The current `0.1.x`
prototype remains volatile until the implementation and automated evidence described here land.
Normative terms such as **must**, **must not**, and **may** describe required alpha behavior.

## Storage modes

`Store` creation selects exactly one storage mode:

- **volatile:** Segments and the Index live in memory. A successful mutation means the new state is
  visible to subsequent operations in the same process. No state is promised after process exit.
- **durable-sync:** a data directory is mandatory. A successful mutation means the mutation crossed
  the durable commit point and must be recovered after process or machine restart, subject to the
  documented filesystem and hardware guarantees.

There is no alpha mode named `async`, `buffered`, or `eventually durable`. Group commit may be added
later, but it must preserve the same acknowledgement contract and receive a separate ADR.

## Persistent identities and versions

Disk, manifest, Record, and wire versions are independent. The manifest records at least:

- Store identity generated at creation;
- manifest format version;
- Segment and Record format versions;
- routing algorithm identifier (`fnv1a64-v1` for the bootstrap);
- persisted Worker count and routing epoch;
- the complete Segment catalog and active Segment for every Worker;
- the next Segment ID and generation information needed to prevent stale references.

Worker auto-sizing runs only when a durable Store is created. Reopen uses the persisted Worker
count, even when current machine topology differs. Supplying an incompatible Worker count, routing
algorithm, or routing epoch must fail before any file is mutated. Online resizing and routing
migration are outside the alpha scope.

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
3. Write all Record bytes and synchronize the Segment data.
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
alpha contract.

Volatile mode retains the current append-then-publish behavior but must still fail closed if a
rollback cannot restore a coherent Index/liveness state.

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
the replacement active. A crash may therefore leave the selected manifest naming a sealed active
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
implemented in the [durable runtime catalog](durable-runtime-catalog.md). Public Store integration,
explicit repair for arbitrary orphans, retirement, and crash evidence remain pending.

## Recovery

Recovery executes before network listeners start and before the Store accepts operations:

1. acquire the exclusive data-directory lock;
2. select and validate the newest complete manifest publication;
3. validate Store identity, versions, routing metadata, and catalog invariants;
4. open every manifest-listed Segment and validate its identity against the catalog;
5. select the newest valid commit slot for each Segment;
6. scan exactly each committed extent and validate every Record checksum and bound;
7. rebuild visibility by full key and highest sequence, with tombstones and expiration suppressing
   older values;
8. partition rebuilt entries using the persisted routing configuration;
9. restore each Worker's next sequence using checked `maximum + 1` arithmetic;
10. validate the complete rebuilt state before publishing the Store as ready.

Segment scan order must not affect the result. Equal highest sequences for the same key are a
conflict and fail recovery. Sequence exhaustion fails read-write open. A future read-only salvage
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

The persistent implementation is incomplete until CI exercises:

- golden fixtures for manifest, Segment header, commit slots, and Records (manifest, Segment header,
  both commit slots, and Record now have canonical v1 fixtures);
- restart tests across every mutation and rotation transition;
- process-kill and injected short-write, rename, disk-full, and real allocation failures (unit seams
  now cover allocation, write, file-sync, slot-sync, and directory-sync boundaries);
- active-tail tolerance and committed-region corruption rejection (covered at Segment-file unit
  level; restart/process-kill coverage remains pending);
- manifest rollback and explicit orphan quarantine/identity reservation;
- routing and Worker-count mismatch rejection;
- recovery independence from Segment enumeration order;
- compatibility reads using artifacts emitted by every supported alpha format version.

The current development reader/writer coverage is recorded in the
[format compatibility matrix](format-compatibility.md). Cross-release compatibility and durable
filesystem behavior remain unproven until exercised using released artifacts.

Passing unit tests for codecs alone is not evidence of durability.
