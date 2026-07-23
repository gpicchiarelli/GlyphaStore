# GlyphaStore Concurrency and Memory Model

Status: normative for the current implementation
Applies to: repository version `0.1.x`
Owner: project maintainers
Last reviewed: 2026-07-19

## 1. Scope

This specification defines thread ownership, synchronization domains, lock order, operation admission, linearization, and shutdown. It does not standardize implementation-private class names, but every shared mutable object must map to one of the domains below.

## 2. Threads and executors

An embedded Store has caller threads and, in durable mode, one flush-coordinator thread. The TCP daemon additionally has an acceptor and one reactor/executor thread per Worker. An executor owns all mutable state of each connection assigned to it.

Store callers may invoke public operations concurrently unless a method explicitly says otherwise. Thread safety does not create multi-key atomicity.

## 3. Ownership table

| State | Owner / synchronization | Notes |
|---|---|---|
| Volatile Worker Index and segments | that Worker's mutex | ordinary key operations acquire exactly one Worker mutex |
| Durable Worker Index, active file, hot records, group state | that durable Worker's mutex | never accessed concurrently without the mutex |
| Volatile global segment namespace | `GlobalSegmentManager` mutex | snapshots are copied while locked |
| Durable segment catalog | catalog shared mutex | readers take shared; namespace mutation takes exclusive |
| Manifest publication | manifest-publication mutex + condition variable | serializes generations; rotation waits for an active compaction lease |
| Public compaction | Store compaction mutex | only one compaction attempt runs at once |
| Store lifecycle admission | sharded atomic counters | one shard per Worker plus one control shard |
| Flush scheduling state | coordinator mutex and condition variables | callback executes after releasing this mutex |
| TCP connection | exactly one executor | ownership may transfer once after bind |
| Executor handoff queue | bounded MPSC protocol | many producers, one owning consumer |
| Directory health | atomic state plus protected error payload | acquire/release publishes terminal health |

No reference, iterator, or span into a protected container may escape after its lock is released unless the referenced object has independent shared ownership and immutable lifetime guarantees. Snapshot APIs therefore return copies such as `std::vector<SegmentPtr>` and `std::vector<SegmentId>`.

## 4. Worker routing

The complete key is hashed before acquiring a Worker. The selected Worker is stable for the duration of the operation because worker count and routing epoch are immutable for an open Store. A Store operation must not lock a second Worker to recover from a miss. A miss in the owning Index is authoritative for the current in-memory state.

## 5. Lock order

When more than one lock is necessary, acquire locks only in these orders:

1. public compaction mutex;
2. Worker mutex;
3. manifest-publication mutex;
4. durable catalog mutex.

For the volatile runtime, the allowed nested order is Worker mutex, then global segment-manager mutex. Code that holds the segment-manager mutex must not acquire a Worker mutex.

Verification that must observe all Workers locks Worker mutexes in ascending Worker identifier order. No ordinary key operation locks all Workers.

The flush coordinator's internal mutex is not part of this storage lock order: it protects request/generation state only. Its callback is invoked with that mutex released, preventing condition-state waits from enclosing filesystem or Worker work.

A rotation that reaches a compaction publication lease waits on the publication condition variable
while retaining only its owning Worker mutex. The wait releases the publication mutex. An
unrelated-Worker compaction can therefore finish; a same-Worker compaction uses its final try-lock
to roll back before waking the rotation. The rotation then constructs its transition from the
newly authoritative Manifest. It must not publish a third authority while the dual-Manifest
compaction intent is active.

New nested locking requires updating this document and adding a test or static invariant that makes the order reviewable.

## 6. Operation admission and shutdown

Admission uses one cache-line-separated atomic counter per Worker plus a control counter. The high bit means admission closed; remaining bits count admitted operations.

An operation:

1. increments its shard;
2. detects the closed bit;
3. if closed, immediately decrements and fails;
4. otherwise completes all access to Store-owned runtime state;
5. decrements the shard and notifies a closer when it was the last admitted operation.

Closing atomically sets the closed bit on every shard, then waits with acquire observations until all counts reach zero. Consequently, destruction of runtime objects happens after every previously admitted operation has released them. The lifecycle transition is one-way. Repeated `close()` calls observe the first close result.

No operation may retain a raw pointer into the Store after releasing its admission token.

## 7. Linearization points

| Operation | Linearization point |
|---|---|
| Volatile `get` | read and validation of the current Index mapping while holding the owner mutex |
| Durable `get` | selection and validation of the record identified by the owner Index while holding the owner mutex |
| `put` | publication of the new `RecordRef` in the owner Index, after the complete record exists |
| `erase` | removal/tombstone publication in the owner Index, after any required durable record exists |
| `flush` | completion of the requested flush generation or synchronous persistence boundary |
| `compact` | publication of the accepted replacement namespace; cleanup may follow |
| `close` | successful transition that closes admission; completion additionally waits for drain and teardown |

A successful `put` may be visible before it is crash-durable in relaxed or group-commit modes. The exact acknowledgement point is defined by the durability policy, not by mutex release alone.

## 8. Atomics and memory ordering

Memory orders are chosen for a specific publication relation:

- admission count arithmetic may be relaxed, while close/drain uses acquire/release operations to order teardown after admitted work;
- terminal health and lifecycle publication use release stores and acquire loads so observers see the associated state;
- queue cell sequence numbers use release publication and acquire consumption; enqueue/dequeue positions may use relaxed arithmetic because cell sequence is the handoff barrier;
- counters used only as statistics, high-water marks, or scheduling hints may be relaxed and must never be used to publish object contents.

Changing an atomic's role from telemetry to correctness requires revisiting its memory order and this specification. `volatile` is not a synchronization primitive.

## 9. MPSC connection handoff

The server's bounded queue has many producer executors and one consumer executor. A producer constructs a complete handoff object, claims a cell, and publishes the cell with release semantics. The sole consumer observes the cell with acquire semantics before taking ownership.

After successful publication, the producer must not read or mutate the connection. Before successful publication, the consumer cannot observe it. If the queue is full, the connection is closed; the server does not create an unbounded backlog. Wakeups are hints to run the consumer, not ownership transfer by themselves.

This queue is for one-time connection transfer. It is not a general per-request remote execution mesh.

## 10. Durable flush coordination

The coordinator accepts ordinary, all-worker, and deadline-based flush requests. Its mutex protects flags, deadlines, generations, and terminal background error. `flush_all_blocking()` serializes callers, records a generation, and waits until that generation completes or the coordinator stops.

The callback may acquire storage locks and perform I/O, so it always runs without the coordinator state mutex. A callback failure is recorded as terminal background error, wakes waiters, and stops the coordinator. `stop()` is serialized, requests thread stop, wakes all waiters, and joins the thread.

## 11. Data-race obligations

For every mutable field, at least one must be true:

- it is thread-confined;
- every access is protected by the same mutex;
- it is atomic with a documented publication role;
- it is immutable after publication and its lifetime is independently owned.

Tests under ThreadSanitizer are evidence, not a substitute for these invariants. A passing race detector does not prove absence of deadlocks or incorrect relaxed ordering.

## 12. Failure and cancellation

Exceptions must not cross background-thread boundaries. Allocation failure becomes `resource_exhausted`; unexpected failure becomes `internal_error`. Network disconnect cancels only that connection's future requests; it does not roll back already linearized Store operations.

The current API has no general operation cancellation token. Shutdown waits for admitted operations and filesystem work; it does not forcibly invalidate objects still in use by those operations.

## 13. Review checklist

Any concurrency change must answer:

1. Which thread or lock owns each new mutable field?
2. Does it introduce a new nested lock acquisition?
3. What is the linearization point?
4. What publishes initialized data to another thread?
5. What happens if the queue is full, allocation fails, or shutdown begins?
6. Can a reference outlive the lock or owner that protects it?
7. Which deterministic and sanitizer tests cover the change?
