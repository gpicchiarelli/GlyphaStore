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

On macOS, physical core detection uses `sysctlbyname`. Linux must respect the process CPU affinity
mask when the platform backend is completed. FreeBSD uses native topology APIs with a portable
fallback.
