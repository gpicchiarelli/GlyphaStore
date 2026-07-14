# Public C++ API contract

This document defines the supported C++ surface being prepared for the alpha release. The Store
header now uses PImpl, owning reads, and a deliberate installed header set. An internal durable
runtime can now serve bounded verified owning reads, but public persistence, stable error evolution,
injected clock configuration, and pinned zero-copy reads remain future work.

## Supported surface

The supported embedded API contains Store creation/open/close, byte-key `get`, `put`, and `erase`,
owned result values, an optional pinned read handle, configuration value types, stable error
categories, and immutable diagnostic snapshots.

Workers, Index partitions, Segments, Record codecs, routing hashes, pollers, reactors, and vacuum
builders are implementation mechanisms. They are not supported merely because a header is present
under `include/` or installed by CMake. Alpha packaging must install a deliberate public header set
and keep internal headers out of exported target usage requirements.

The native daemon and wire protocol have their own compatibility contract. Internal server paths
may call owner-checked Store operations without exposing those operations to embedded consumers.

## Keys, values, and limits

Keys and values are uninterpreted byte strings; no text encoding or normalization is performed.
Empty keys are valid. Public operations validate that the encoded Record, including headers and
alignment, does not exceed the normal 1 MiB Record limit.

`put` stores byte values and an optional absolute expiration timestamp expressed as Unix-epoch
nanoseconds. Zero means no expiration. A value is expired when the Store clock is greater than or
equal to its nonzero expiration. Production calls use the Store clock; deterministic tests inject
a clock through construction rather than passing arbitrary time into each public read.

Callers do not provide a precomputed routing hash. The Store hashes the exact key bytes once and
uses that result consistently for routing, Record metadata, and Index lookup. This prevents a
caller-supplied key/hash mismatch from violating Worker ownership.

## Read ownership

The default `get` returns an owning value object. Its bytes and metadata remain valid after later
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

Public key operations are safe to call concurrently. Operations on the same Worker may serialize;
the API does not promise lock freedom. A completed mutation happens before a later operation that
observes its result. There is no atomicity across multiple keys in alpha.

Configuration is immutable after open. Diagnostics return owning snapshots rather than references
to live Index, Worker, or Segment containers. Public callers cannot acquire engine mutexes, mutate
Segment bytes, publish `RecordRef` values, or invoke unchecked owner-specific paths.

Closing a Store prevents new operations and waits for internal executors. Owned values remain
valid. Pinned values remain valid and delay only the resources they reference; the close contract
must not require application-wide destruction ordering that is invisible in the type system.

## Errors and mutation outcomes

Public errors have stable categories and may carry diagnostic text that is not intended for
machine parsing. At minimum the API distinguishes invalid input, not found/expired, resource
limit, incompatible format, corruption, I/O failure, unavailable/overloaded, and internal
fail-closed state.

- Invalid input and not-found failures do not mutate state.
- A durable-sync failure before the durable commit point must not reappear after recovery.
- After the durable commit point, failure to publish coherent runtime state is fatal to that Store
  instance rather than a recoverable per-operation error.
- Loss of a network response after commit is an indeterminate client outcome, not evidence that the
  mutation failed.

Exceptions do not cross the supported API boundary for expected operational failures. Allocation
and unexpected implementation exceptions are translated to a stable error or place the Store in a
fail-closed state as required by the commit status.

## Compatibility policy

Before `1.0`, minor releases may make source-breaking changes announced in the changelog and
migration notes. Patch releases within one minor line preserve documented source behavior and do
not deliberately break a supported disk or wire reader. No C++ ABI stability is promised before
`1.0`; consumers rebuild against each release.

Disk, manifest, and wire compatibility are governed by their encoded versions, not by the library
version alone. A release must not claim alpha status until golden fixtures and a compatibility
matrix demonstrate every supported reader/writer pair. Current development coverage is listed in
the [format compatibility matrix](format-compatibility.md).

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

Temporary compatibility adapters may exist during development, but they must be visibly marked as
deprecated prototype surface and must not survive the alpha release boundary.
