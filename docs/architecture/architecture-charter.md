# GlyphaStore architecture charter

Status: normative architectural constraints
Applies to: GlyphaStore 0.1.x
Owner: project maintainers
Last reviewed: 2026-08-26

## Purpose

GlyphaStore is a native-binary, many-core, memory-first key-value store. Its fast path pays only
for exact-key operations and must remain measurable and resource-bounded.

## Fixed decisions

- One logical Store and one logical key-space.
- Workers/shard pairs are selected automatically from the best platform topology/memory signal
  currently available; explicit override is stable for the process lifetime and is required when
  exact affinity-aware sizing matters.
- The internal architecture vocabulary uses Store, paired Worker ownership, Index, Segments,
  Records, and compaction. The supported API does not expose Workers, the Index, Segments, or a
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
- Durable persistence v1 uses a versioned Manifest plus alternating CRC-protected Segment commit
  slots; bytes beyond the newest valid committed extent are never recovery input.
- Durable Store creation persists the routing algorithm, Worker count, and routing epoch. Reopen
  uses that configuration until an explicit migration changes it.
- Paired Reader–Writer concurrency is the embedded default and the only daemon runtime. Each owner
  publishes immutable read generations and has exactly one mutation executor at a time; the
  `legacy_mutex` embedded escape hatch is deprecated for 0.1.x and removed in 0.2.
- Vacuum uses copy-build-validate-publish-retire and never rewrites published records in place.
- The Index and immutable read-generation metadata are RAM-resident. Volatile Segments are
  RAM-resident; durable Segments are file-backed and accessed through bounded exact-generation
  pins. The deprecated legacy hot cache is not a paired-read authority.
- Linux, macOS, FreeBSD, and OpenBSD are architectural targets since `0.1.0`; macOS is the primary
  development environment. All four have build/test workflows; hosted and BSD VM rows are
  portability/regression evidence, not filesystem/device durability certification.
- Memory safety, integer safety, data-race freedom, corruption detection, and bounded resource
  use are release requirements, not optional hardening.

## Non-goals

- Text-protocol or command-emulation compatibility layers.
- SQL, joins, generic secondary indexes, and unbounded ordered scans.
- Performance claims without reproducible benchmarks and hardware description.
- GPU/NPU dependencies in the key-value fast path.
- Unversioned on-disk/wire changes or serialization through native object layout. Persistence v1
  and wire v2 evolve only through explicit codecs, compatibility rules, and fixtures.

The embedded Store implements persistence v1 with automated process-termination coverage. Release
certification criteria are listed in
[production readiness](../production-readiness.md) and the
[persistence v1 production roadmap](../v1-production-roadmap.md).

## Performance contract

Exact-key operations target constant expected work relative to total key count. The project
optimizes cache misses, allocations, copies, syscalls, probe count, and tail latency rather than
relying on asymptotic notation alone. Background work is budgeted and controlled rejection is
preferred to unbounded queuing.
