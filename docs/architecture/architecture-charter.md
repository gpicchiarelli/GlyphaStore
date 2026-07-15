# GlyphaStore architecture charter

## Purpose

GlyphaStore is a native-binary, many-core, memory-first key-value store. Its fast path pays only
for exact-key operations and must remain measurable, bounded, and independent from compatibility
with Redis or any text protocol.

## Fixed decisions

- One logical Store and one logical key-space.
- Workers are selected automatically from usable physical CPU and memory topology; override is
  explicit and stable for the process lifetime.
- The public model uses Store, Workers, Index, Segments, Records, and Vacuum. It does not expose a
  shard abstraction.
- Every physical Segment is exactly 64 MiB (67,108,864 bytes).
- Records are variable-size, immutable after publication, append-only, 8-byte aligned, and never
  span normal Segments.
- The Index maps a full logical key to a `RecordRef`; its optimized destination is a benchmark-
  selected flat open-addressing table, not `std::unordered_map` as a final engine.
- A `RecordRef` is positional and contains stable IDs, offsets, sizes, sequence, and generation; it
  never persists a process address.
- The Index is derived acceleration state. Records in Segments are sufficient to rebuild it; for
  the same full key, the highest valid sequence defines visibility.
- Durable alpha mode uses a versioned manifest plus alternating CRC-protected Segment commit slots;
  bytes beyond the newest valid committed extent are never recovery input.
- Durable Store creation persists the routing algorithm, Worker count, and routing epoch. Reopen
  uses that configuration until an explicit migration changes it.
- Vacuum uses copy-build-validate-publish-retire and never rewrites published records in place.
- The Index and active Segments are RAM-resident. Sealed Segments may be persisted, mapped, or
  evicted by explicit policy.
- Linux, macOS, FreeBSD, and OpenBSD are architectural targets since `0.1.0`; macOS is the primary
  development environment. Only Linux and macOS currently have continuous build-and-test evidence.
- Memory safety, integer safety, data-race freedom, corruption detection, and bounded resource
  use are release requirements, not optional hardening.

## Non-goals

- Redis/RESP compatibility, Lua, or historical Redis command semantics.
- SQL, joins, generic secondary indexes, and unbounded ordered scans.
- Claims that a design alone outperforms another product.
- GPU/NPU dependencies in the key-value fast path.
- A stable on-disk or wire format before explicit versioning and compatibility tests exist.

The embedded Store now implements persistence v1 and has automated process-termination evidence,
but this does not make the project production durable. Filesystem/power-loss certification,
resource bounds, durable compaction, operational tooling, and the remaining gates in the
[persistence v1 production roadmap](../v1-production-roadmap.md) are still required.

## Performance contract

Exact-key operations target constant expected work relative to total key count. The project
optimizes cache misses, allocations, copies, syscalls, probe count, and tail latency rather than
relying on asymptotic notation alone. Background work is budgeted and controlled rejection is
preferred to unbounded queuing.
