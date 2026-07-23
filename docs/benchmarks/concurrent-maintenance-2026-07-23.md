# Concurrent maintenance exploratory benchmark — 2026-07-23

Status: exploratory local measurement, not a release baseline
Applies to: disabled, cooperative, and background durable maintenance
Owner: performance and persistence maintainers
Last reviewed: 2026-07-23

## Question

What foreground cost appears while one useful durable compaction overlaps a mixed GET/PUT workload,
and does the Store-owned background controller add material overhead beyond the compaction itself?

This benchmark compares policies inside GlyphaStore; it is not a competitive database benchmark.
Reported throughput is instrumented by a per-operation steady-clock latency measurement.

## Environment and method

| Item | Value |
| --- | --- |
| Source | clean `1ccd379` |
| Build | CMake `Release`, Apple clang 21.0.0, C++23, `-O3 -DNDEBUG`, ARM64, LTO off |
| Host | Apple M4, 10 physical / 10 logical cores, 16 GiB RAM |
| OS | macOS 26.5.2 (25F84), Darwin 25.5.0 |
| Storage | internal Apple SSD, APFS, about 96.8 GB free at capture |
| Thermal / power policy | not controlled or instrumented; no CPU affinity |
| Sampling | three warmups, seven measured repeats per mode, 500 ms cooldown, rotated mode order |
| Command | `./scripts/dev.sh benchmark-maintenance --warmup 3 --repeats 7 --cooldown-ms 500` |
| Raw result | [`data/concurrent-maintenance-2026-07-23.csv`](data/concurrent-maintenance-2026-07-23.csv) |

Each sample creates a fresh two-Worker `durable-periodic` Store. Worker 0 receives 1,024 updates
across 128 keys with 256 KiB values, producing a high-reclaim candidate. Worker 1 receives 128
preloaded keys and then 15,360 operations from four threads: 14,615 GETs and 745 PUTs (5%), with
64 KiB values. Seed, flush, close, reopen, final `verify_index()`, and full key verification are
outside the timed foreground region.

The disabled mode performs no maintenance. Cooperative mode starts one public `Store::compact()`
with the foreground latch. Background mode uses the Store-owned controller at a 10 ms evaluation
interval; a benchmark-only filesystem hook holds its first evaluation until the same latch. This
ensures that planning and compaction, rather than only a later copy phase, overlap foreground work.

## Results

Medians are primary; min–max ranges show the substantial run-to-run variance.

| Mode | Foreground ops/s median (min–max) | p50 µs | p95 µs | p99 µs (min–max) | max µs median (min–max) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disabled | 101,252 (72,189–106,969) | 9.75 | 178.21 | 373.96 (356.96–696.54) | 5,306 (2,732–7,597) |
| Cooperative | 82,836 (46,209–86,152) | 10.46 | 250.25 | 588.67 (487.33–1,459.38) | 5,120 (3,219–8,487) |
| Background | 82,719 (47,047–86,361) | 10.29 | 257.17 | 576.79 (503.92–1,500.54) | 6,757 (4,095–17,137) |

| Comparison of medians | Throughput | p50 | p95 | p99 | GET p99 | PUT p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cooperative vs disabled | -18.2% | +7.3% | +40.4% | +57.4% | +49.4% | +38.3% |
| Background vs disabled | -18.3% | +5.6% | +44.3% | +54.2% | +43.7% | +45.6% |
| Background vs cooperative | -0.1% | -1.6% | +2.8% | -2.0% | -3.8% | +5.3% |

Every cooperative and background sample attempted and completed exactly one useful compaction,
reported zero conflicts, copied 32,515,776 bytes (31.01 MiB), and reduced the post-close Segment
count from six to three. Every final reopen, Index verification, and key/value check passed.

## Interpretation

1. **Useful reclaim has a visible foreground cost.** While this 31 MiB copy overlaps foreground
   access, median throughput falls about 18% and aggregate p99 rises about 54–57%. Median p50 moves
   only 6–7%, so the cost is concentrated in the tail.
2. **The controller is not the dominant cost in this workload.** Cooperative and background
   medians are effectively equal for throughput and p99. The shared scan/copy/publication work
   dominates the policy mechanism.
3. **Maximum latency is not stable enough for a ranking.** Background's median maximum is 32%
   above cooperative, but their broad 3.2–17.1 ms ranges overlap. This local matrix cannot support
   a claim that background scheduling worsens the maximum.
4. **Efficacy is deterministic in the measured case.** Both maintenance policies reclaim the same
   three Segments with identical copied bytes and no conflict in all seven samples.
5. **These are diagnostic, not release thresholds.** The host's thermal and power state were not
   controlled, several modes show a slow cluster around repeats 4–5, and per-operation timing
   lowers absolute capacity. The ratios are useful for direction; absolute values are not a
   production SLO or a cross-host claim.

## Concurrency finding from calibration

A deliberately larger calibration put enough foreground data on Worker 1 to force rotation while
Worker 0 was publishing compaction. Only 127 of 401 foreground PUTs succeeded because the
Store-wide Manifest publication lease makes an unrelated-Worker rotation fail fast with
`sequence_conflict`. This behavior is already explicit in the implementation and covered by a
deterministic test, but the benchmark shows its operational consequence: a maintenance transaction
can reject unrelated durable writes at a rotation boundary.

That diagnostic matrix is not mixed into the latency table because failed operations would make
the modes incomparable. It identifies the next engineering target rather than a performance
number to optimize away.

This finding is historical for source `1ccd379`. The follow-up implementation replaces fail-fast
rotation with condition-based serialization, and the clean
[forced-rotation, idle, and 1 GiB churn matrix](maintenance-rotation-idle-churn-2026-07-23.md)
records zero foreground failures across all forced overlaps.

## Decision and next gates

The concurrent foreground measurement gate is complete. Forced rotation, idle overhead, and
sustained churn are covered by the linked follow-up. Remaining:

1. expose rejected/no-gain planning work in compaction and maintenance telemetry;
2. decide whether unread TTL needs a bounded normal-mode probe independent of pressure;
3. instrument rotation phase and Manifest-wait durations separately;
4. rerun longer controlled matrices on macOS/APFS and Linux ext4/XFS, then define separate
   throughput and tail-latency regression gates.
