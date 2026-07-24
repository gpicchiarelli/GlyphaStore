# Durable GET path follow-up optimizations (2026-07-24)

## Scope

Follow-up to `get-path-hot-cache-2026-07-24.md` (commits `99bceac`…`3e5501b`). This pass
finishes remaining original-prompt items without redoing O(1) pin, deferred TTL policy, or
admission limits (except bugfixes / accounting updates for the new table).

Hard constraints preserved: exclusive Worker shard ownership and mutex, authoritative
per-Worker Index, GET linearization / pin identity, TTL fail-closed visibility, CRC
verification, Linux/macOS/FreeBSD compatibility, no protocol changes, no RCU /
`shared_mutex` reader sharing of the Index SwissTable, no new executors.

## What changed

1. **`prepare_get` critical section** — Ordinary hot path under mutex is: optional deferred-TTL
   drain only when backlog non-empty (before Index find), Index lookup, hot match + snapshot,
   unlock. Pin acquire and catalog identity checks run only on cold miss. Value materialization
   (`owned_value_from_hot`) happens after unlock. No I/O/CRC under the lock.
2. **Stats / bookkeeping** — Hot hit/miss/stale/eviction/admission/size-rejected/expired counters
   moved to `alignas(64)` relaxed atomics on `GetPathMetrics`, published outside or at the edge of
   the critical section (local deltas in `prepare_get`, then `fetch_add`).
3. **GET-path timing gated off in Release** — Fine-grained `steady_clock` sampling compiles out
   when `NDEBUG` and `GLYPHASTORE_GET_PATH_TIMING` is unset (Release / benchmark builds). Debug /
   ASan / UBSan keep timing for telemetry tests. Counters remain available via `get_path_stats()`.
4. **Hot cache structure** — Replaced `std::unordered_map` with a flat open-addressed
   `HotRecordTable` (power-of-two capacity, load 0.5, linear probe, tombstones, FNV-1a hash from
   `HashedKey`). Inline values raised to 48 bytes. Staging map removed: `PreparedHotRecord` owns
   the staged entry in-place. Hash is never identity; full key compare remains mandatory.
5. **Cold GET revalidation** — Kept the second Index lookup + pin-object identity check. A
   Worker-global generation version was re-evaluated and still rejected (ABA / rehash fragility).
   At hot-path µs scale the revalidation hold is a small fraction of cold I/O+CRC; see telemetry
   fields `complete_revalidate_hold_ns` vs `cold_read_ns` / `crc_value_copy_ns` on cold workloads.
6. **Correctness tests** — Concurrent GET+PUT/DELETE linearization, stale-hot after admission
   bypass, flat-table probe/collision full-key identity, plus existing TTL/disable/limit coverage.
7. **Bench script** — Default ops raised (`5000` / parallel `8000`); 500-ops remains available for
   smoke but is not treated as a precise speed signal.

## How to reproduce

```bash
./scripts/benchmark_get_path.sh followup
# focused high-ops matrix used for this report:
./scripts/dev.sh benchmark --filter store-durable-get --workers 1 --value-size 32 \
  --ops 20000 --warmup 3 --repeats 7
./scripts/dev.sh benchmark --filter store-durable-get --workers 1 --value-size 256 \
  --ops 5000 --warmup 2 --repeats 5
./scripts/dev.sh benchmark --filter store-durable-parallel-get --workers 4 --threads 4 \
  --value-size 256 --distribution zipf --ops 8000 --warmup 2 --repeats 5 --latency
```

Raw outputs: `docs/benchmarks/data/get-path-followup/`.

Sanitizers: ASan+UBSan `glyphastore_tests` Passed (368 internal cases). TSan via
`./scripts/dev.sh tsan`. `glyphastore_allocation_fault_tests` still aborts on intentional
`std::bad_alloc` under ASan (fault-injection harness), unchanged from prior policy.

## Comparative medians

Prior slice (telemetry-only → final densify) used **500** ops and was noisy for v32. This pass
uses higher ops; a 500-ops v32 rerun is included only to close the regression question.

| Workload | Prior final (500 ops) | Follow-up | Notes |
| --- | ---: | ---: | --- |
| durable GET w1 v32 | 887 050 | **2 358 140** (20 000 ops, w3 r7) | −20% regression closed |
| durable GET w1 v32 @500 | 887 050 | **1 348 770** | same ops; still above telemetry 1.11M |
| durable GET w1 v256 | 889 944 @500 | **1 248 540** @5000 | +40% vs prior final |
| parallel GET w4 v256 zipf | 651 660 @2000 | **1 002 210** @8000 | +54% |
| parallel GET w4 v256 uniform | 901 493 @2000 | **1 277 440** @8000 | +42% |
| durable GET w2 v131072 | 11 172 @50 | **14 222** @200 | size-rejected → cold |

## Interpretation

- The prior −20% v32 result was dominated by Release-path timing clocks inside `prepare_get` plus
  node-based hot map overhead, amplified by 500-op noise. Gating timing in Release and switching
  to a flat hot table recovers and exceeds the telemetry-only baseline.
- Hot small-value GET is still memory-bound (~0.4–0.7 µs/op at high ops counts).
- Zipf / multi-worker parallel GETs benefit from shorter critical sections and denser open
  addressing (FNV hash reuse, no `std::unordered_map` node chase).
- Oversized values remain correctness-neutral cold reads under the size cap.

## Still deferred / rejected

- Full SwissTable control-byte SIMD for the hot cache: the new flat table already matches repo
  open-addressing primitives; SIMD control matching would be a separate micro-opt.
- Caching a Worker-global generation version to skip cold revalidation: still rejected (ABA).
- Populating the hot cache from cold GET: still out of scope (ADR 0017 write-admit policy).
- Global LRU / shared cache lock: rejected.
