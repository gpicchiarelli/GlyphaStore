# Worker model

Workers are an internal execution and ownership mechanism, not separate user-visible stores.

At startup, `WorkerTopology` detects logical CPUs, usable CPUs, physical cores where available,
and memory. `WorkerCountPolicy` applies explicit override, reserved cores, maximum Worker count,
and minimum memory per Worker. The result is always at least one and remains stable during the
process lifetime.

Each Worker owns:

- one physical Index partition;
- one active Segment at a time;
- a monotonic sequence counter;
- references to Segments it produced;
- future bounded command queues and local metrics.

Routing is deterministic from a key hash. The first implementation may route directly by Worker
count; online Worker resizing would require a stable routing-slot table and migration protocol and
is intentionally outside this bootstrap.

## Concurrency

The public `Store` API (`get`, `put`, `erase`) is thread-safe by routing each key to one Worker
and serializing callers on that Worker's mutex. Independent keys on different Workers execute in
parallel. `GlobalSegmentManager` uses its own mutex for catalog mutations and lookups.

Lock ordering for maintenance: `verify_index()` acquires every Worker mutex in ascending index order
before scanning Segments or comparing Index partitions. Direct use of `Worker::index()` bypasses
this synchronization and is intended for single-threaded tests and tools only.

`RecordView` spans returned from `get()` remain valid in memory for the Store lifetime but may
become semantically stale if another thread updates the same key; copy bytes when a stable value is
required across threads.

On macOS, physical core detection uses `sysctlbyname`. Linux must respect the process CPU affinity
mask when the platform backend is completed. FreeBSD uses native topology APIs with a portable
fallback.
