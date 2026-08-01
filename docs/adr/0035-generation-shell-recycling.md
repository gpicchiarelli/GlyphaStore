# ADR 0035: PairReadGeneration shell recycling under existing publish protocol

- Status: rejected
- Date: 2026-08-02
- Deciders: storage and performance maintainers
- Applies to: paired `PairReadGeneration` publication path (`shared_ptr` + raw published
  pointer + QSBR/lease reclaim)
- Amends: none (rejected; does not amend ADR 0031)
- Supersedes: none

## Context

Every sync `publish_incremental` allocates a fresh `PairReadGeneration` object managed by
`shared_ptr`. Readers observe generations only through a raw published pointer plus
`ReadLease`; they never own `shared_ptr`. Writers retire previous generations into a
bounded vector (`kMaximumRetiredReadGenerations = 64`) and drop those `shared_ptr`s on the
Writer thread during reclaim.

Hot-path attribution showed single-op PUT dominated by Writer apply+publish cost. A
prototype generation *slot pool* in `src/experimental/paired_shard.cpp` replaces the
publish protocol itself and remains out of scope without a dedicated ADR and proofs.

This ADR evaluated a narrower idea: recycle only the `PairReadGeneration` *shell* while
keeping the existing publish/lease protocol.

## Decision drivers

- Correctness: RAW, lease fencing, and reclaim bounds must stay identical.
- No silent format, hash, routing, or publish-protocol change.
- Measured latency of sync single-op PUT, batched PUT, and parallel PUT must not regress.
- Bounded memory if a freelist were used.

## Alternatives considered

### A. Full generation slot pool (experimental prototype)

Fixed slots with QSBR epoch ownership, no `shared_ptr` on the publish path.

- Advantage: removes control-block traffic.
- Disadvantage: new lifetime protocol; needs epoch/lease proofs.
- Deferred; not selected here.

### B. Thread-local DeltaPage / Block / Chunk freelist

Already rejected after measured regression on uniform parallel PUT (Apple Silicon lab).

### C. Bounded TLS shell freelist with clear-before-park (evaluated)

Acquire/rebind recycled shells; custom `shared_ptr` deleter clears pins then returns the
shell to a bounded per-thread freelist (capacity 64).

### D. Keep plain `shared_ptr` allocation (selected after measurement)

## Decision

**Rejected.** Do not recycle `PairReadGeneration` shells via a TLS freelist and custom
`shared_ptr` deleter under the current publish protocol.

Same-machine A/B on Apple M4 (`macos-release`, 200k ops, warmup 3, repeats 11):

| Cell | Shell freelist | Plain `shared_ptr` | Δ |
| --- | ---: | ---: | ---: |
| `store_put` 1t | 359 k | 344 k | +4.4% |
| `store_put_batch` 1t | 540 k | 515 k | +4.9% |
| uniform PUT 2t | 131 k | 129 k | ~neutral |
| worker-affine PUT 2t | 422 k | 503 k | **−16%** |

Modest single-thread gains do not justify a clear affine parallel regression. Residual
publish cost remains dominated by `shared_ptr` control blocks, DeltaState, and COW
spine allocations — not shell object malloc alone.

## Consequences

- Production path keeps `make_shared` co-allocation of `PairReadGeneration` + embedded
  `DeltaState` with the default `shared_ptr` deleter (no TLS shell freelist).
- Full generation slot-pool publish protocol is tracked by
  [ADR 0036](0036-generation-slot-pool-publish.md) (proposed; production unchanged until
  its verification matrix closes).
- Lesson matches the rejected Delta COW freelist: custom deleters + TLS reuse can tax
  the Writer hot path enough to erase allocator savings under parallel load.

## Compatibility and migration

None — no production change shipped.

## Verification

- A/B benches recorded under `benchmarks/results/local-macos-2026-08-02-gen-shell/`
  (candidate) and `...-gen-shell-ab-baseline/` (plain path).
- Paired generation/store tests were green on the candidate before revert.

## References

- [ADR 0031](paired-reader-writer-shards.md)
- [hot-path-performance-2026-08-01.md](../architecture/hot-path-performance-2026-08-01.md)
- [hot-path-rejected-optimizations.md](../architecture/hot-path-rejected-optimizations.md)
- `src/experimental/paired_shard.cpp` — lab slot-pool prototype (not production)
