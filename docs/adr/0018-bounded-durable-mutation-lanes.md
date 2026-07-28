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

Durable servers create exactly one FIFO dispatch lane per Store Worker. Volatile servers retain the
direct synchronous path and create no mutation threads. Each lane owns a fixed-cell request ring and
a short admission mutex. Sync and periodic modes use one execution thread per lane. Strict-group
mode uses a bounded configurable producer set, capped by `max_records`, because a producer waits for
its batch acknowledgement and at least two concurrent Store calls are required to form a batch.
The mutex is held only while moving an owning task into or out of the ring; it is released before the
Store is called, so no filesystem I/O runs under the lane mutex. Lanes share neither a request queue
nor an admission mutex.

Each Reactor has one fixed-capacity MPSC completion ring. Admission is bounded twice per Worker:
by outstanding request count and by the dynamic capacity owned by copied key/value bytes plus the
fixed task object. Exhaustion returns `overloaded` before the request enters the lane. The daemon
exposes both limits as `--durable-mutation-queue-capacity` and
`--durable-mutation-queue-bytes`.

An optional server-side queue-wait deadline is evaluated by a producer only after dequeue and before
calling Store. Expiry therefore has the exact `not_committed` meaning: no storage operation began.
Once Store execution starts, timeout and disconnect never cancel the mutation. The default deadline
is one second; `--durable-mutation-queue-wait-ms=0` disables it.

The daemon may replay an owned mutation at most once, and only when the persistence kernel returns
the exact pair `not_committed + sequence_conflict`. This covers a rotation whose generation snapshot
became stale while it waited without the Worker mutex for an active compaction publication lease.
The retry is internal to the same admitted task and does not advance that connection's pipeline.
`committed`, `indeterminate`, every other error, and a second conflict are returned without replay.
This rule consumes the kernel's rich outcome directly; inferring retry safety from `ErrorCode` alone
is forbidden.

The successful enqueue under the lane mutex is the daemon admission point and fixes FIFO dequeue
order for that Worker. With multiple strict-group producers, cross-connection Store acquisition may
reorder after dequeue; the durable runtime defines that linearization, while one-in-flight per
connection preserves each connection's order. Admission is not the storage visibility or durable
linearization point. Those remain the persistence-v1 points defined by the selected Store mode. The
completion contains only the
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
- Per-Worker snapshots expose current/peak queue records and bytes, admitted/rejected/expired/completed
  counts, bounded conflict retries and retry commits, and total/maximum queue-wait and Store-service
  nanoseconds. Snapshotting locks only one lane queue at a time and never waits on its storage call.
- The server also exposes lock-free durable-kernel batch snapshots: real committed occupancy,
  adaptive close reason, failures, and `flush_pending_commit` duration. This separates queueing,
  complete Store service, and the batch persistence boundary.
- Sync/periodic modes add one storage thread per Worker. Group mode adds a bounded producer set per
  Worker, with a hard process-wide thread limit; concurrency and NUMA placement require measured
  platform tuning before very high Worker counts are certified.
- This decision does not change locking inside the durable runtime. Catalog publication,
  maintenance fairness, and shorter internal critical sections remain separate kernel work.

## Evidence

Integration tests suspend the real file-sync hook while proving that the owner Reactor still serves
non-storage traffic, a second mutation can enter the FIFO without waiting on the lane mutex, excess
admission returns `overloaded`, and shutdown waits for and recovers an already admitted mutation.
