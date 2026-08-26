# Worker model

Workers are an internal execution and ownership mechanism, not separate user-visible stores.

At startup, `WorkerTopology` detects logical CPUs, usable CPUs, physical cores where available,
and memory. `WorkerCountPolicy` applies explicit override, reserved cores, maximum Worker count,
and minimum memory per Worker. The result is always at least one and remains stable during the
process lifetime. For a durable Store, automatic selection occurs only at creation; reopen uses the
persisted Worker count and routing configuration.

Each Worker owns:

- one physical Index partition;
- one active Segment at a time;
- a monotonic sequence counter;
- references to Segments it produced;
- Worker-local accounting and maintenance state.

The volatile `Worker` owns in-memory Segments through `GlobalSegmentManager`. Durable mode uses a
separate internal runtime Worker that owns a file-backed active Segment and bounded hot-record state
through `DurableRuntimeCatalog`. They share the routing/ownership contract, not one concrete class.

Routing is deterministic from the persisted routing algorithm/seed and routes directly by Worker
count. Online Worker resizing would require a stable routing transition and migration protocol and
is intentionally outside 0.1.x. Changing the persisted Worker count is supported only via
offline [store migration](store-migration.md) ([ADR 0024](../adr/0024-offline-worker-migration.md)).

## Concurrency

The public `Store` API (`get`, `put`, `erase`) is thread-safe. The product default is **paired**
concurrency ([ADR 0032](../adr/0032-paired-concurrency-embedded-store.md)): each key routes to one
owner shard; ordinary GET adopts that shard's immutable `ReadGeneration` without taking the Index
mutex; `put`/`erase` hand off to the shard's serial Writer. Independent keys on different shards
execute in parallel. Same-shard mutations serialize on the Writer lane.

Deprecated `StoreConcurrencyMode::legacy_mutex` restores the historical path that serializes callers
on that Worker's mutex. Mixing legacy mutex mutators with a paired Writer on the same Store is
refused at open.

Routing ownership is authoritative: a miss in the routed Worker/generation is a Store miss, not a
request to search peer Workers. Future rebalancing must transfer ownership through an explicit
routing-table state transition; it must not introduce peer-to-peer fallback searches in the normal
data path.

Each Worker resolves records through its local catalog of owned Segments, so the hot data path does
not consult the global catalog. `GlobalSegmentManager` is the control plane for Segment allocation,
rotation, retirement, and global snapshots.

Lock ordering for maintenance: `verify_index()` acquires every Worker mutex in ascending index order
before scanning Segments or comparing Index partitions. Direct use of `Worker::index()` bypasses
this synchronization and is intended for single-threaded tests and tools only. Compaction, backup,
and durable catalog refresh retain their existing locks under paired mode.

The complete lock order, atomic publication rules, operation admission, and shutdown model are
normative in the [concurrency and memory model](../spec/concurrency-memory-model.md).

The supported public API returns an owning value copy. Internal server execution may use a
`RecordView` only through the build-tree-only Store access bridge and must encode the response
before its execution lifetime ends. An explicitly pinned public zero-copy handle remains future
work. See the [public API contract](public-api-contract.md).

On macOS, physical core and memory detection use `sysctlbyname`. FreeBSD uses `kern.smp.cores` and
`hw.physmem`; OpenBSD uses `_SC_NPROCESSORS_ONLN` for available CPU count. Linux currently uses the
online logical CPU count and `sysinfo` free-memory sample; it does not yet distinguish physical cores
or reduce that count to the process affinity mask. Every platform retains the portable
`std::thread::hardware_concurrency` fallback. Explicit Worker/shard-pair count is therefore the
deterministic choice for controlled affinity or benchmark runs.
