# GlyphaStore Code Tour

Status: maintained onboarding guide
Applies to: repository version `0.1.x`
Owner: project maintainers
Last reviewed: 2026-07-19

## 1. Reading order

Start with [Architecture](../spec/architecture.md), [Glossary](../glossary.md), and the public `Store` declaration. Then follow one volatile operation, one durable operation, recovery, and finally the TCP adapter. Read exact binary format documents before changing any codec.

## 2. Repository map

| Area | Public headers | Implementation | Responsibility |
|---|---|---|---|
| Core | `include/glyphastore/core` | mostly header-only | result/error types, checked arithmetic, key hash |
| Store | `include/glyphastore/store` | `src/store` | public facade, configuration, lifecycle, routing, runtime selection |
| Worker | `include/glyphastore/worker` | `src/worker` | volatile ownership and per-Worker serialized operations |
| Index | `include/glyphastore/index` | `src/index` | Swiss-style map and long-key arena |
| Segment/record | `include/glyphastore/segment` | `src/segment` | record codec, CRC32C, in-memory Segment and manager |
| Persistence | `include/glyphastore/persistence` | `src/persistence` | manifest, file Segment, recovery, namespace, flush, compaction |
| Networking | `include/glyphastore/server` | `src/server` | sockets, pollers, reactor, framing, executor handoff |
| Vacuum | `include/glyphastore/vacuum` | `src/vacuum` | volatile maintenance/reclamation policy |
| CLI/daemon | — | `src/main.cpp`, `src/cli`, `src/server/daemon_main.cpp` | executable entry points |
| Benchmarks | `benchmarks` | `benchmarks` | isolated engine and loopback TCP measurement |
| Tests | `tests` | `tests` | unit, integration, property, crash, quality, consumer coverage |

## 3. Follow a volatile put

1. `Store::put` admits the operation and computes the owning Worker.
2. The volatile runtime delegates to that Worker.
3. The Worker locks its mutex, prepares Index capacity, appends an encoded Record to its active Segment, and publishes the returned `RecordRef` in the Index.
4. Rotation goes through `GlobalSegmentManager` when the active Segment is full.
5. The admission guard is released on every exit path.

The critical invariant is append-complete-before-Index-publication.

## 4. Follow a durable put

The same facade selects the internal durable runtime. The owning durable Worker prepares its Index, encodes into the active file Segment, and follows the selected sync/periodic/group commit order. `DurableRuntimeCatalog` resolves Segment identity and file access; the manifest-publication mutex is entered only for namespace transitions such as rotation.

Do not assume that the volatile `Worker` class is also the durable runtime Worker. They intentionally implement the same ownership model with different storage authorities.

## 5. Follow recovery

Read in this order:

1. filesystem and namespace audit;
2. bootstrap and compaction intent resolution;
3. manifest decoding and catalog validation;
4. Segment-file header/commit-slot validation;
5. committed Record scan;
6. latest-key rebuild and per-Worker Index creation;
7. runtime catalog publication.

Recovery code must be reviewed against [Persistence v1](../spec/persistence-v1.md), never only against the happy-path writer.

## 6. Follow a TCP request

The daemon configures a `Store` and `Server`. The acceptor assigns a new connection to an executor. The Reactor decodes protocol frames. `INIT` reports capabilities; `BIND_WORKER` may hand the connection object through a bounded MPSC queue to the target executor. Thereafter that executor owns the connection and invokes the Store. Wrong-owner operations receive routing metadata rather than remote execution.

Platform pollers are separate implementations: kqueue on macOS/FreeBSD and epoll on Linux. Their observable contract is the Reactor interface, not platform event struct layout.

## 7. Safe places to change behavior

- Public semantics: update the public header, C++ API reference, and tests together.
- Index algorithm: update `index-v1.md` if semantics/invariants change; pure SIMD equivalence need not version it.
- Persistent bytes: update format version, fixture, compatibility matrix, recovery, and ADR before merging.
- Wire bytes: update protocol version/specification and golden tests.
- Locks/atomics: update concurrency ownership and lock order.
- Benchmarks: preserve sample validation and document any timed-region change.

## 8. Common traps

- Confusing a `RecordRef` with lifetime ownership.
- Returning a reference to a container after releasing its mutex.
- Treating a filename, cache, or Index as durable authority.
- Hashing a key differently for routing and then searching other Workers on miss.
- Publishing a commit slot before Record bytes are ordered.
- Holding the flush coordinator mutex while running its callback.
- Moving a connection without relinquishing the source executor's access.
- Comparing owner-bound internal benchmarks with public Store benchmarks.
- Changing a reserved field from zero without a compatibility decision.

## 9. Before submitting a change

Run the focused tests, full test suite, formatter, and appropriate sanitizer. Persistent/network changes require fixture and malformed-input coverage. Performance changes require the standard benchmark matrix and raw results. Update the normative document whose contract changed; do not bury the decision only in a code comment.
