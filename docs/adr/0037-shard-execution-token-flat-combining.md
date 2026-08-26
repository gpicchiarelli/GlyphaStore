# ADR 0037: Shard execution token and flat combining

- Status: accepted
- Date: 2026-08-26
- Deciders: storage, concurrency, and performance maintainers
- Applies to: paired `ShardPairRuntime` mutation ownership (embedded Store and
  `glyphastored`); persistence format v1 and wire protocol v2 unchanged
- Amends: [ADR 0031](paired-reader-writer-shards.md) (Writer identity),
  [ADR 0032](0032-paired-concurrency-embedded-store.md) (embedded handoff model)
- Supersedes: none
- Depends on: ADR 0031, ADR 0032
- Related: [ADR 0036](0036-generation-slot-pool-publish.md) (proposed; orthogonal),
  [mutation-lifecycle.md](../spec/mutation-lifecycle.md),
  [concurrency-memory-model.md](../spec/concurrency-memory-model.md)

## Context

Paired reads (immutable `ReadGeneration`) already scale. Sync embedded PUT still treats the
Writer as a remote actor: caller enqueues, parks, and waits for a dedicated per-shard
`std::thread` to mutate, publish, and complete. Lab attribution
(`benchmarks/results/local-macos-2026-08-26-head-94f1307/`) shows roughly two-thirds of
single-op PUT time in rendezvous/ACK versus a few percent in Index work.

ADR 0031/0032 required a **dedicated Writer thread** as the sole mutator. The correctness
property that matters is weaker and stronger at once:

- weaker: ownership need not be a permanent thread;
- stronger: **exactly one executor may mutate a shard at a time**, represented by an
  execution token.

Wait-to-fill batching on empty queues remains rejected (ADR 0031 micro-batch rule; lab
summary). Combining may drain only work already queued (cap ≤32), matching today’s sync
coalesce spirit without artificial delay.

## Decision drivers

- Preserve RAW: success ACK only after required visibility (and durable_sync persistence).
- Preserve fail-closed polarity (OVERLOADED ⇔ known-not-committed; post-commit → unavailable).
- Remove uncontended embedded handoff/park without changing the read plane.
- Keep daemon Reactor free of Store mutate (no Writer-on-Reactor).
- Shared-nothing multi-shard scaling: no new globally mutable hot-write structures.
- No wire/format/routing change; ADR 0036 stays proposed.

## Alternatives considered

### Keep dedicated Writer thread; optimize wake/spin only

Rejected as the primary lever: measured cost is structural rendezvous, not a missing pause
instruction. Incremental wake tuning does not unlock million-class uncontended PUT/s.

### Sleep/yield to fill publication batches on single-op PUT

Rejected (already measured): hurts latency; forbidden by ADR 0031 empty-queue rule.

### Reactor executes mutations (daemon)

Rejected (ADR 0031): head-of-line blocking of the event loop.

### Separate TCP read and write ports

Rejected: separation is internal (read plane vs mutation plane), not addressing.

### Early ACK before publication

Rejected: breaks RAW ([hot-path-performance](../architecture/hot-path-performance-2026-08-01.md)).

## Decision

1. **Writer = execution-token holder.** Per shard, an atomic `IDLE | EXECUTING` token grants
   sole rights to mutate Index/Segment/delta and to publish generations for that shard.
2. **Embedded (`async_lane_capacity == 0`):** no mandatory Writer `std::thread`. A caller that
   successfully `IDLE → EXECUTING` becomes the **combiner** for that turn. Losers enqueue on a
   bounded sync structure and wait for their completion bit.
3. **Flat combining:** while holding the token, drain up to 32 already-queued sync mutations
   (FIFO after LIFO admission), apply Store mutates in order, perform **one** generation
   publication for the chunk when the path requires it, complete waiters, then repeat until the
   queue is empty or a turn budget is exhausted. **Never** sleep/yield to wait for more work.
4. **Lost-wakeup-safe release:** after `EXECUTING → IDLE`, if the queue is non-empty, attempt
   `IDLE → EXECUTING` again (or the next producer will); no work may sit forever without an
   executor.
5. **Daemon (`async_lane_capacity > 0`):** retains a dedicated per-shard executor thread (Reactor
   must not block in Store). The executor holds the same token model. Future mutation windows
   (PUT coalesce until GET barrier) submit grouped work to that executor; one TCP port remains.
6. **GET barrier (daemon, phased):** a pipelined GET after mutations must not adopt a generation
   older than the epoch required by prior mutations on that connection; visibility waits on
   publish completion / `visible_epoch`, not on enqueue alone.
7. **Grouped publication ≠ multi-key transaction.** Linearization remains per mutation FIFO;
   Readers simply observe one generation containing the combined effects.
8. **Quiesce / exclusive Index:** `ExclusiveIndexQuiesce` / `hot_path_depth` treat the token
   holder (caller or dedicated executor) as the Writer for the duration of the turn.

## Phased landing

| Phase | Scope | Status target |
| --- | --- | --- |
| A | Embedded volatile combining (no Writer thread when `!async && !durable`) | landed |
| B | Embedded `durable_sync` combining (`!async`) | landed |
| C | Daemon mutation windows + GET visibility barrier | landed |

Gates and litmus must stay green each phase; no production-readiness claim.

## Consequences

### Positive

- Uncontended embedded PUT avoids cross-thread park/wake.
- Natural throughput combining under contention without wait-to-fill.
- Clarifies single-writer as ownership, enabling multi-shard scale-out.

### Negative / residual

- Combiner turns can extend caller latency under heavy same-shard contention (by design).
- Without a dedicated thread, durable catalog refresh deferred until a combiner turn (embedded).
- Larger critical section on the caller stack for uncontended durable_sync I/O.

### Deferred

- ADR 0036 slot-pool publish.
- Flusher-backed `durable_group` / `durable_periodic` token sharing redesign.
- Removing dedicated daemon executor threads entirely.

## Compatibility and migration

- Public `Store::put` / `erase` / `put_batch` signatures unchanged; completion polarity unchanged.
- Wire protocol v2 and persistence v1 unchanged.
- Config: embedded default `async_lane_capacity == 0` selects combining; daemon keeps async lanes.
- `legacy_mutex` escape hatch unchanged (deprecated).

## Verification

- Unit: token CAS lost-wakeup, combine FIFO same-key, uncontended path does not require a second
  thread (`tests/unit/shard_combining_executor_tests.cpp`).
- Integration: existing `paired` litmus + durable ACK / fail-closed polarity.
- Daemon: half-close, BIND handoff, OVERLOADED polarity; Phase C adds window/GET barrier tests.
- Benchmarks: interlaced vs `local-macos-2026-08-26-head-94f1307` (≥9 samples); classify
  improvement / neutral / regression. Architectural aim (not a promise): uncontended PUT no
  longer dominated by handoff.

## References

- [ADR 0031](paired-reader-writer-shards.md), [ADR 0032](0032-paired-concurrency-embedded-store.md)
- [concurrency-memory-model.md](../spec/concurrency-memory-model.md)
- [mutation-lifecycle.md](../spec/mutation-lifecycle.md)
- [error-taxonomy-v1.md](../spec/error-taxonomy-v1.md)
- Requirement `GS-CONCUR-PAIR-001`, `GS-CONCUR-COMBINE-001`
- Lab: `benchmarks/results/local-macos-2026-08-26-head-94f1307/`
