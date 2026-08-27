# Public C++ API contract

Status: normative for the current installed C++ API
Applies to: `glyphastore::Store` 0.1.x
Owner: API maintainers
Last reviewed: 2026-08-26

This document defines the current supported C++ surface while the product remains below alpha. The
Store header uses PImpl, owning reads, and a deliberate installed header set. All three durable
policies use the same public key operations as volatile mode. Store-owned clock injection, positional
`put_batch`, and fenced online backup are implemented; a dedicated incompatible-format error and
pinned zero-copy reads remain future additive work.

Durable mode requires `data_directory`. `create_new` rejects an existing leaf, `open_existing`
requires durable metadata, and `open_or_create` initializes only a missing or pristine directory.
Worker auto-sizing applies only to creation. An explicit Worker count on reopen must match the
persisted routing metadata.

## Supported surface

The supported embedded API contains Store creation/open/close, byte-key `get`, `get_copy`, `put`,
`put_batch`, and `erase`, owning result values, configuration value types, typed error categories,
explicit flush/fenced backup/compaction, maintenance snapshots, and Index verification. A pinned
read handle is a reserved future extension and is not part of the current public API.

Workers, Index partitions, Segments, Record codecs, routing hashes, pollers, reactors, and vacuum
builders are implementation mechanisms. They are not supported merely because a header is present
under `include/`. Current CMake packaging installs a deliberate public header set and keeps internal
headers out of exported target usage requirements; additions require an explicit API decision.

The native daemon and wire protocol have their own compatibility contract. Internal server paths
may call owner-checked Store operations without exposing those operations to embedded consumers.
The separately installed C ABI v1 is a smaller synchronous facade; it does not make C++ layouts or
the full Store surface ABI-stable.

## Keys, values, and limits

Keys and values are uninterpreted byte strings; no text encoding or normalization is performed.
Empty keys are valid. Public operations validate that the encoded Record, including headers and
alignment, does not exceed the normal 1 MiB Record limit.

`put` stores byte values and an optional absolute expiration timestamp expressed as Unix-epoch
nanoseconds. Zero means no expiration. A value is expired when the Store clock is greater than or
equal to its nonzero expiration. Production calls use the Store clock; deterministic tests inject
a thread-safe, non-throwing `StoreClock` through construction rather than passing arbitrary time
into each public read. With no injected clock, the Store reads `system_clock` and clamps values
before the Unix epoch to zero and values beyond the encoded range to `uint64_t` maximum.

Each Store instance maintains an atomic high-water mark, so a backward wall-clock adjustment cannot
make time move backward during that instance's lifetime. Recovery samples the same clock once and
uses that snapshot for its complete scan; subsequent reads advance from that value. A process
restart cannot preserve the high-water mark without adding persistent state, so operators requiring
strict TTL behavior across large clock corrections must keep the host Unix clock synchronized and
nondecreasing. Expiration removes visibility but durable byte reclamation remains compaction work.

Callers do not provide a precomputed routing hash. The Store hashes the exact key bytes once and
uses that result consistently for routing, Record metadata, and Index lookup. This prevents a
caller-supplied key/hash mismatch from violating Worker ownership.

`put_batch` accepts a borrowed positional list, groups it by owner, preserves FIFO order within each
owner and restores caller order in its result vector. Owners publish independently and individual
failures do not roll back successful siblings; it is not a cross-key transaction. Every successful
item is visible before return. Within one owner, a dedicated paired Writer processes at most 32
batch items per scheduling turn. Concurrently admitted async mutations may linearize between those
groups, while the batch's own items retain FIFO order.

## Read ownership

`get` and the compatibility spelling `get_copy` return an owning value object. `get_copy` does not
select a distinct slow or zero-copy path. Returned bytes and metadata remain valid after later
Store calls, mutation of the same key, vacuum, and Store destruction. This is the safe default and
the source-compatible API on which ordinary consumers should rely.

A zero-copy API may return a move-only `PinnedValue`. Its immutable key/value views remain valid
until that handle is destroyed. A pin holds the exact Segment generation resident and prevents
unmap, deletion, reuse, or destructive repair. Replacing or erasing the key does not change the
bytes observed through an existing pin; the handle is a historical read snapshot, not a promise
that the key remains current.

`PinnedValue` may be moved between threads and destroyed on a different thread, but concurrent
mutation of the handle object itself is not allowed. Store close stops new operations but does not
invalidate an existing pin. Reclamation must account for pins independently of the Store object's
lifetime.

Non-owning `RecordView` results are restricted to internal code whose lexical execution scope pins
the Segment or otherwise proves that reclamation cannot occur. They must not cross an asynchronous
boundary or be stored in connection state.

## Concurrency

Public key operations are safe to call concurrently. The default open mode is paired
([ADR 0032](../adr/0032-paired-concurrency-embedded-store.md)): ordinary GET adopts a published
immutable generation; same-shard `put`/`erase` serialize on the Writer lane. The API does not
promise lock freedom. A completed mutation happens before a later operation that observes its
result. There is no atomicity across multiple keys in the current API.

`StoreConfig::concurrency = StoreConcurrencyMode::legacy_mutex` restores Worker-mutex serialization
and is deprecated in 0.1.x (removed in 0.2). Mixing legacy and paired mutators on one Store is
refused at open.

Configuration is immutable after open. Diagnostics return owning snapshots rather than references
to live Index, Worker, or Segment containers. Public callers cannot acquire engine mutexes, mutate
Segment bytes, publish `RecordRef` values, or invoke unchecked owner-specific paths.

`Store::close()` linearizes when it changes admission from open to closing. Operations admitted
before that point complete; later reads, mutations, verification, and flush calls return
`unavailable`. Close forces partial strict groups, waits for admitted calls, drains paired Writer
lanes when present, performs the final durability barrier, joins internal executors, releases Store
resources and the data-directory lock, and returns a sticky idempotent status. Concurrent close
callers receive the same outcome. The destructor performs close as a non-throwing fallback but
cannot expose its result. Owned values remain valid after close and destruction.

## Durable resource policy

`StoreConfig::durable_limits` is immutable runtime policy, not persistent metadata. It bounds total
and peak Store bytes, free-space reserve, Segment count, manifest bytes, Store-owned descriptors,
recovery memory, live keys, temporary compaction bytes, and write amplification. The same v1 Store
may therefore be opened under different policy, but a policy smaller than its existing catalog or
recovery state rejects open without editing the Store.

The policy also retains bounds for the derived durable hot cache globally and per Worker, with
separate entry and pre-publication staging limits. Default paired opens disable that duplicate read
authority and use immutable generations; the limits remain relevant only to the deprecated
`legacy_mutex` path and 0.1.x configuration compatibility. Zero-cache operation never changes
durable data or mutation success.

The live-key limit is partitioned deterministically across persisted Workers and the partition sizes
sum exactly to the configured total. This preserves Worker-local admission and avoids a shared
counter on the mutation path. A new key returns `resource_exhausted` when its owner partition is
full; replacement remains permitted, and a committed erase returns its capacity.

Bootstrap and rotation check configured peak namespace bytes and currently available filesystem
space before publishing an intent or sealing an active Segment. The free-space sample is advisory in
the presence of other writers and thin provisioning; fixed-size native Segment preallocation remains
the authoritative allocation boundary.

## Errors and mutation outcomes

Public errors have typed categories and may carry diagnostic text that is not intended for machine
parsing. The current categories distinguish invalid arguments, checked arithmetic overflow, Record
and Segment constraints, invalid/corrupted/checksum state, reference and sequence conflicts,
not-found, resource/storage exhaustion, file-size and descriptor limits, read-only filesystem,
I/O, unavailable state, and internal failure. There is not yet a dedicated
`incompatible_format` category; unsupported required versions use an existing validation or
corruption category until the planned additive error-model work lands.

- Invalid input and not-found failures do not mutate state.
- A durable-sync failure before the durable commit point must not reappear after recovery.
- After the durable commit point, failure to publish coherent runtime state is fatal to that Store
  instance rather than a recoverable per-operation error.
- Loss of a network response after commit is an indeterminate client outcome, not evidence that the
  mutation failed.

Exceptions do not cross the supported API boundary for expected operational failures. Allocation
and unexpected implementation exceptions are translated to a stable error or place the Store in a
fail-closed state as required by the commit status. Deterministic allocator-interposition tests
enumerate the actual allocation sites emitted by each native STL build instead of assuming a fixed
implementation-specific allocation count.

## Online backup

`Store::backup_to` is available only for durable Stores. It briefly fences admissions, drains and
flushes admitted work, copies a structurally verified catalog under the exclusive catalog boundary,
writes the destination Manifest last, resumes admissions, and then optionally performs the complete
destination CRC scan. The destination must be new and empty. This is the online **fenced** contract,
not a zero-fence hot snapshot; any failed destination remains unfit for service until an independent
verification succeeds. Concurrent backup callers retain independent counted fences, so completion
of one copy cannot reopen admissions underneath another copy waiting for the catalog boundary. See
[Backup and restore v1](../spec/backup-restore-v1.md).

## Explicit maintenance

`Store::compact()` is supported for volatile (selective vacuum) and durable (whole-Worker sealed
history) Stores. Concurrent compaction calls are not queued; mutations never invoke compaction
implicitly; Store shutdown waits only for the one maintenance transaction already admitted.

Selection for a single `compact()` call is round-robin across Workers. A successful result reports
either one completed transaction and its copy statistics, or no current physical gain.

Optional automatic scheduling is governed by `StoreConfig::maintenance` and ADR 0023. The embedded
default is `cooperative` (no maintenance thread). `background` starts one Store-owned evaluation
thread that may call `compact()` under Phase 3 normal/pressure/emergency budgets. Under emergency,
`put`/`erase` return `storage_exhausted` until capacity recovers (embedded Store). On the TCP path the
Reactor maps that to `OVERLOADED`; official clients report `retryability=never`. `glyphastored`
defaults to
`background`. Normal background maintenance preflights the selected durable Worker's exact
Index-referenced live bytes against `max_copy_bytes_per_cycle` (128 MiB default, inclusive; zero
means unlimited); pressure and emergency bypass the limit. See
[maintenance controller](maintenance-controller.md).

## Compatibility policy

Before `1.0`, minor releases may make source-breaking changes announced in the changelog and
migration notes. Patch releases within one minor line preserve documented source behavior and do
not deliberately break a supported disk or wire reader. No C++ ABI stability is promised before
`1.0`; consumers rebuild against each release.

Disk, manifest, and wire compatibility are governed by their encoded versions, not by the library
version alone. A release must not claim alpha status until golden fixtures and a compatibility
matrix demonstrate every supported reader/writer pair. Current development coverage is listed in
the [format compatibility matrix](format-compatibility.md). Operator-facing upgrade, downgrade,
Worker-count, and release-artifact rules are normative in
[version lifecycle](version-lifecycle.md) ([ADR 0024](../adr/0024-offline-worker-migration.md)).
Worker-count changes use offline [store migration](store-migration.md); reopen never reshards.

## Prototype migration

The migration checklist is:

1. [x] Introduce owning public key/value and diagnostic types.
2. [x] Replace public `RecordView` reads with the owning result.
3. [ ] Add the pinned handle only together with reader accounting and reclamation tests.
4. [x] Remove `Worker`, `Index`, `Segment`, `GlobalSegmentManager`, `HashedKey`, and mutable byte
   access from the supported Store interface.
5. [x] Move corruption and owner-checked hooks into a build-tree-only internal bridge.
6. [x] Export and install only the supported header set.
7. [x] Update the external consumer test to use only the installed owning API.
8. [x] Make ordinary reads and durable recovery use one Store-owned injectable clock.

Temporary compatibility adapters may exist during development, but they must be visibly marked as
deprecated prototype surface and must not survive the alpha release boundary.
