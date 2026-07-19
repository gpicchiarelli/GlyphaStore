# ADR 0018: bounded Worker-affine durable mutation lanes

- Status: accepted
- Date: 2026-07-19
- Preserves: ADR 0008, ADR 0011, ADR 0012, persistence v1, wire protocol v2

## Context

The daemon previously invoked a durable `PUT` or `ERASE` on its owner Reactor. Record writes and
the selected durability barrier could therefore stop every connection owned by that Reactor. A
process-wide mutation pool would remove the syscall from the event loop, but a shared request lock
or a blocked generic worker could recreate cross-Worker head-of-line blocking. An unbounded queue
would convert slow storage into unbounded key/value duplication.

## Decision

Durable servers create exactly one FIFO execution lane per Store Worker. Volatile servers retain
the direct synchronous path and create no mutation threads. Each lane owns a fixed-cell request
ring, a short admission mutex, and one execution thread. The mutex is held only while moving an
owning task into or out of the ring; it is released before the Store is called, so no filesystem I/O
runs under the lane mutex. Lanes share neither a request queue nor an admission mutex.

Each Reactor has one fixed-capacity MPSC completion ring. Admission is bounded twice per Worker:
by outstanding request count and by the dynamic capacity owned by copied key/value bytes plus the
fixed task object. Exhaustion returns `overloaded` before the request enters the lane. The daemon
exposes both limits as `--durable-mutation-queue-capacity` and
`--durable-mutation-queue-bytes`.

The successful enqueue under the lane mutex is the daemon admission point and fixes FIFO execution
order for that Worker. It is not the storage visibility or durable linearization point. Those remain
the persistence-v1 points defined by the selected Store mode. The completion contains only the
connection `(slot, generation)`, request id, byte-accounting charge, and result; no `RecordRef`,
file handle, Segment, or borrowed protocol span crosses the asynchronous boundary. Key and value
bytes are owned by the task.

At most one asynchronous Store request is active per connection. Later frames remain buffered in
wire order until completion, avoiding a response reorder buffer and preventing one pipelined client
from monopolizing a lane. A success response is encoded only after the Store call reports that the
selected acknowledgement contract has completed. A disconnect never cancels an admitted mutation;
its stale completion is discarded by connection generation and the client outcome is indeterminate.

Shutdown first stops Reactor loops, then closes lane admission and drains every admitted mutation,
then closes the Store. Outstanding-count admission guarantees that all drain completions still fit
their Reactor rings even when Reactors no longer consume them.

## Consequences

- A slow durable Worker no longer stops socket I/O on its Reactor or admission/execution on another
  Worker lane.
- Per-Worker mutation order and the existing persistence contract are preserved.
- Memory amplification from queued payload copies has a configured, deterministic upper bound.
- Durable mode adds one storage thread per Worker; thread count and NUMA placement require measured
  platform tuning before very high Worker counts are certified.
- This decision does not change locking inside the durable runtime. Catalog publication,
  maintenance fairness, and shorter internal critical sections remain separate kernel work.

## Evidence

Integration tests suspend the real file-sync hook while proving that the owner Reactor still serves
non-storage traffic, a second mutation can enter the FIFO without waiting on the lane mutex, excess
admission returns `overloaded`, and shutdown waits for and recovers an already admitted mutation.
