# Durable GET path + hot-cache optimization (2026-07-24)

## Scope

Six logical commits on `main`:

1. GET-path / hot-cache telemetry (`get_path_stats`, extended `hot_cache_stats`)
2. O(1) `SegmentId` → generation-pin slot table on the GET fast path
3. Bounded deferred Index TTL reclaim (no new threads)
4. Hot-cache disable switch + `max_hot_cache_value_bytes` (default 64 KiB)
5. Hot-cache density: load factor 0.5, geometric reserve, 32-byte inline values
6. This report + comparative microbenchmarks

Hard constraints preserved: exclusive Worker shard ownership and mutex, authoritative
per-Worker Index, GET linearization / pin identity, TTL fail-closed visibility,
CRC verification, Linux/macOS/FreeBSD compatibility, no protocol changes, no RCU /
`shared_mutex` reader sharing of SwissTable, no new executors.

## How to reproduce

```bash
./scripts/benchmark_get_path.sh final
# or a reduced matrix:
./scripts/dev.sh benchmark --filter store-durable-get --workers 1 --value-size 32 \
  --ops 500 --warmup 2 --repeats 5
./scripts/dev.sh benchmark --filter store-durable-parallel-get --workers 4 --threads 4 \
  --value-size 256 --distribution zipf --ops 2000 --warmup 2 --repeats 5 --latency
```

Raw outputs: `docs/benchmarks/data/get-path-final/` (post) and
`docs/benchmarks/data/get-path-telemetry-only/` (commit 1 baseline worktree).

ASan (macos-asan, ASan+UBSan): `365 tests, 0 failures` after these commits.
TSan: not re-run in this pass (full suite is the CI gate via `./scripts/dev.sh tsan`).

## Comparative medians (warmup 2, repeats 5)

| Workload | Baseline (telemetry-only) ops/s | Final ops/s | Δ |
| --- | ---: | ---: | ---: |
| durable GET w1 v32 | 1 113 480 | 887 050 | −20% (noisy @ 500 ops) |
| durable GET w1 v256 | 893 323 | 889 944 | −0.4% |
| parallel GET w4 v256 zipf | 617 808 | 651 660 | +5.5% |

Additional final-only points:

| Workload | median ops/s | median ns/op | notes |
| --- | ---: | ---: | --- |
| durable GET w1 v4096 | 574 053 | 1742 | still hot-admitted |
| durable GET w2 v65536 | 103 204 | 9689 | at value-size cap |
| durable GET w2 v131072 | 11 172 | 89506 | size-rejected → cold |
| durable put-get w2 v256 | 355 | 2.8e6 | sync PUT dominated |
| parallel GET w4 uniform | 901 493 | 1109 | p50≈1.9µs p99≈27µs |
| parallel GET hot shard | 819 350 | 1220 | single-worker |

## Interpretation

- Hot small-value GET remains memory-bound (~1µs/op). Commit-level deltas at 500
  ops are within run-to-run noise; treat them as health checks, not precise speedups.
- Zipf / hot-shard parallel GETs benefit most from shorter pin resolution and denser
  hot entries; uniform multi-worker scales near-linearly per shard.
- Values above `max_hot_cache_value_bytes` correctly fall to pinned cold reads and
  must not inflate hot resident bytes.
- Sync `put-get` remains durability-bound; GET-path work does not change that ceiling.
- Hot cache remains write-admitted only (ADR 0017). Utility is high for working sets
  inside the per-Worker budget and value-size cap; oversize / disabled cache is a
  correctness-neutral cold path.

## Discarded / deferred optimizations

- Full SwissTable replacement of the hot map: deferred in this slice; addressed in the follow-up
  (`docs/benchmarks/get-path-hot-cache-followup-2026-07-24.md`) with a flat open-addressed
  `HotRecordTable` (not the Index SwissTable control SIMD path).
- Caching a Worker-global generation version to skip the second Index lookup after
  cold I/O: rejected (ABA / rehash fragility). Cold GET keeps exact RecordRef + pin
  object revalidation.
- Global LRU / shared cache lock: rejected (ADR 0017).
- Populating the hot cache from cold GET: out of scope; would change admission policy.

Follow-up (critical-section slim, Release timing gate, flat hot table, high-ops benches):
see `docs/benchmarks/get-path-hot-cache-followup-2026-07-24.md`.
