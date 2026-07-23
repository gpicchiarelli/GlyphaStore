# Maintenance rotation, idle, and churn follow-up — 2026-07-23

Status: exploratory local measurement, not a release baseline
Applies to: durable background maintenance and independent-Worker rotation
Owner: performance and persistence maintainers
Last reviewed: 2026-07-23

## Questions

1. Does an unrelated Worker rotation still reject foreground writes while compaction owns the
   Manifest publication lease?
2. What CPU overhead does the background controller add when the Store is idle?
3. During sustained rotation churn, does maintenance keep reclaiming without foreground failures,
   starvation, or unbounded Segment growth?

This follow-up is an internal policy comparison, not a competitive database benchmark. All
throughput numbers include one steady-clock latency sample per operation.

## Environment and method

| Item | Value |
| --- | --- |
| Source | clean `573a741` |
| Build | CMake `Release`, Apple clang 21.0.0, C++23, `-O3 -DNDEBUG`, ARM64, LTO off |
| Host | Apple M4, 10 physical / 10 logical cores, 16 GiB RAM |
| OS | macOS 26.5.2 (25F84), Darwin 25.5.0 |
| Storage | internal Apple SSD, APFS |
| Thermal / power policy | not controlled or instrumented; no CPU affinity |
| Sampling | seven measured repeats per mode; one warmup for forced rotation; rotated mode order |

The executable closes and reopens every durable Store, runs `verify_index()`, checks the complete
key/value model, and fails the sample on any foreground error.

Raw results:

- [`data/maintenance-rotation-2026-07-23.csv`](data/maintenance-rotation-2026-07-23.csv)
- [`data/maintenance-idle-default-2026-07-23.csv`](data/maintenance-idle-default-2026-07-23.csv)
- [`data/maintenance-idle-10ms-2026-07-23.csv`](data/maintenance-idle-10ms-2026-07-23.csv)
- [`data/maintenance-churn-2026-07-23.csv`](data/maintenance-churn-2026-07-23.csv)

## Forced independent-Worker rotation

Worker 0 is seeded with a 31.01 MiB useful compaction candidate. Worker 1 is filled to the exact
active-Segment Record boundary. A deterministic filesystem hook releases one Worker 1 PUT only
after Worker 0 has published its compaction intent and owns the Manifest publication lease. The
disabled mode forces the same rotation without compaction.

| Mode | Rotation median ms (min–max) | Useful compactions | Sequence conflicts | Segments after reopen |
| --- | ---: | ---: | ---: | ---: |
| Disabled | 76.52 (39.71–109.88) | 0 | 0 | 7 |
| Cooperative | 185.04 (181.34–343.30) | 1 | 0 | 4 |
| Background | 193.81 (179.68–353.89) | 2 | 0 | 3 |

All 14 rotations that overlapped cooperative or background maintenance waited, committed, and
survived reopen. No foreground PUT returned `sequence_conflict` or any other error. Relative to
disabled, median rotation latency is 2.42x in cooperative mode and 2.53x in background mode.

This is serialization, not simultaneous Manifest publication. Persistence v1 permits the exact
old/new authority pair in one compaction intent; it has no durable representation for a third
authority. Rotation therefore releases its Manifest mutex while waiting for the compaction lease,
then rebuilds its transition from the newly published Manifest. This removes the availability
failure while preserving the crash-recovery invariant, at the cost of one compaction-length wait
at the forced boundary.

Background reports two useful compactions because it subsequently reclaims Worker 1's newly sealed
history during benchmark settling. That second compaction happens after the timed forced rotation.

## Idle controller overhead

Idle measures process CPU time with no foreground operations and no data. Product defaults use a
1,000 ms minimum and 60,000 ms maximum evaluation interval; the 10 ms matrix deliberately stresses
the scheduler and is not the shipping policy.

| Policy | Window | CPU ms median (min–max) | CPU duty median | Evaluations median | Last evaluation µs median |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disabled | 3 s | 0.028 (0.021–0.042) | 0.00093% | 0 | 0 |
| Background, defaults | 3 s | 0.054 (0.048–0.061) | 0.00179% | 1 | 19.17 |
| Disabled | 1 s | 0.020 (0.015–0.021) | 0.00198% | 0 | 0 |
| Background, forced 10 ms | 1 s | 3.881 (2.419–5.227) | 0.38425% | 70 | 37.83 |

At product defaults the first no-candidate evaluation adds roughly 0.026 ms of median process CPU
over three seconds, after which the controller backs off toward the 60-second interval. Even an
artificial 10 ms interval remains below 0.52% process CPU in every sample, but it wakes only about
69–71 times per second rather than 100 because evaluation and timed-wait scheduling are not a
hard-periodic real-time loop.

## Sustained churn

Each sample starts with the same 31.01 MiB reclaim candidate on Worker 0, then four threads issue
32,768 successful 32 KiB PUTs to Worker 1: 1 GiB of logical foreground mutation and repeated
active-Segment rotations. Background evaluation is deliberately aggressive at 10 ms. Seed, close,
reopen, and verification are outside the timed region.

| Mode | Ops/s median (min–max) | p50 µs | p95 µs | p99 µs | Max ms median | Useful compactions | Segments after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Disabled | 5,636 (5,419–9,016) | 50.58 | 232.38 | 1,438.00 | 3,341.87 | 0 | 22 |
| Background | 5,473 (5,038–8,841) | 51.38 | 237.46 | 1,452.04 | 3,306.43 | 2 | 4 |

Background versus disabled changes median throughput by -2.9%, p50 by +1.6%, p95 by +2.2%, and
p99 by +1.0%. The ranges are bimodal and overlap substantially, so this local run cannot support
sub-percent regression thresholds. It does show that maintenance did not introduce a new latency
class: multi-second maxima occur in both modes and are dominated by the synchronous rotation /
flush path of this `durable-periodic` stress shape.

Every background sample completed exactly two useful compactions, copied 31.51–31.76 MiB, and
finished with four Segments instead of 22. It recorded one to four internal sequence conflicts.
Those are maintenance attempts invalidated by concurrent Worker rotation, not foreground failures;
the controller retries and reaches the same final reclaim result in all seven samples.

An earlier 256 MiB calibration exposed a policy-fairness bug: advancing the round-robin cursor only
after a compaction attempt allowed a below-threshold Worker to pin observation and starve a
reclaimable peer. The clean source measured here advances the cursor after every observed
candidate. The seven 1 GiB samples all reclaim both eligible histories, providing direct evidence
that the starvation is fixed.

## Decision

- The independent-Worker availability conflict is closed: forced rotation waits and commits
  instead of rejecting the write.
- The remaining contention is explicit and bounded by the in-flight compaction publication:
  rotation latency rises about 2.5x in the forced overlap, but ordinary mutations that do not
  rotate remain independent.
- Idle overhead is negligible at product defaults on this host. The 10 ms stress policy is still
  cheap in absolute CPU terms, but is unnecessary for production.
- Sustained churn makes deterministic reclaim progress with no foreground errors and bounded
  Segment count. Internal maintenance conflicts are observable and recover through retry.

The next performance work should instrument rotation phases and wait duration separately, then run
the same matrix on controlled macOS/APFS and Linux ext4/XFS runners. A persistence-format redesign
for truly concurrent Manifest authorities is justified only if the measured forced-boundary wait
violates a product latency objective; the current v1 serialization is the simpler safe contract.
