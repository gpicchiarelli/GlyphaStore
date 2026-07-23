# Durable rotation deep-phase benchmark on macOS — 2026-07-23

Status: exploratory local measurement, not a release baseline
Applies to: durable-periodic rotation and background maintenance
Owner: performance and persistence maintainers
Last reviewed: 2026-07-23

## Question

After separating publication wait from aggregate rotation execution, which durable step dominates
that execution: sealing the old Segment, creating the replacement Segment, publishing the Manifest,
or committing the Record that triggered rotation? Does the same attribution hold through prolonged
churn?

This is an internal policy measurement, not a competitive database benchmark.

## Environment and method

| Item | Value |
| --- | --- |
| Source | clean `d98b766` |
| Build | CMake `Release`, Apple clang 21.0.0, C++23, `-O3 -DNDEBUG`, ARM64, LTO off |
| Host | Apple M4, 10 physical / 10 logical cores, 16 GiB RAM |
| OS | macOS 26.5.2 (25F84), Darwin 25.5.0 |
| Storage | internal Apple SSD, APFS |
| Thermal / power policy | not controlled or instrumented; no CPU affinity |
| Sampling | seven measured repeats per mode; one warmup for forced rotation; rotated mode order |

Raw results:

- [`data/maintenance-rotation-deep-phases-macos-2026-07-23.csv`](data/maintenance-rotation-deep-phases-macos-2026-07-23.csv)
- [`data/maintenance-churn-deep-phases-macos-2026-07-23.csv`](data/maintenance-churn-deep-phases-macos-2026-07-23.csv)

Runtime phase definitions:

- `publication_wait`: entry into `rotate_active()` through publication authority and any active
  compaction-lease wait;
- `seal`: state validation and durable seal of the old active Segment;
- `create`: durable creation of the replacement Segment;
- `manifest_publication`: durable publication of the next Manifest;
- `other_execution`: planning, reader setup, pin construction, and in-memory catalog installation,
  derived from aggregate execution minus the three named execution phases;
- `final_record_commit`: retry after rotation through durable append/commit and Index publication
  of the Record that triggered rotation;
- `measurement_residual`: external PUT latency minus complete rotation and final-Record time.

Completed multi-field statistics are published through a short atomic writer gate and versioned
snapshot. It does not enter the storage lock order or change persistence v1.

## Forced overlap

Worker 0 owns a 31.01 MiB useful compaction candidate. Worker 1 is filled exactly to its active
Segment Record boundary. A filesystem hook releases the Worker 1 PUT only after Worker 0 publishes
its compaction intent, guaranteeing intersection with the active publication lease.

| Mode | PUT/rotation ms median (min–max) | Publication wait ms | Seal ms | Create ms | Manifest ms | Other execution ms | Final Record ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Disabled | 54.38 (40.75–106.31) | 0.00017 | 4.32 | 38.93 | 8.98 | 0.177 | 0.309 |
| Cooperative | 200.09 (186.94–379.45) | 155.61 | 3.82 | 30.99 | 9.84 | 0.189 | 0.314 |
| Background | 189.77 (184.21–402.89) | 141.80 | 3.83 | 29.21 | 10.81 | 0.193 | 0.302 |

Every sample reports one rotation attempt, one committed rotation, one attempted and committed
final Record, and—outside disabled mode—one compaction wait. The median wait is 77.8% of cooperative
PUT latency and 74.7% of background PUT latency.

Within rotation execution, replacement Segment creation is the dominant named phase: 65–72% of
the mode medians. Seal is 8–9%, Manifest publication is 17–24%, and residual in-memory work is below
0.3 ms in every sample. The final Record commit is about 0.3 ms median and never exceeds 0.53 ms.
The wide slow cluster also localizes to Segment creation: create reaches 90–114 ms while seal and
Manifest publication remain nearly flat.

## Sustained churn

Each sample issues 32,768 successful 32 KiB PUTs to Worker 1—1 GiB logical foreground mutation and
16 rotations—while Worker 0 begins with the same compaction candidate. Background maintenance uses
a deliberately aggressive 10 ms interval.

Settlement now requires a complete two-Worker evaluation sweep with no reclaimable candidate,
active compaction, or new sequence conflict. An earlier preliminary run exposed the weaker
single-observation condition by ending three samples with 19 Segments; those samples were discarded,
the harness was corrected, and every measured background sample below ends with four Segments.

| Mode | Ops/s median (min–max) | p50 µs | p95 µs | p99 µs | Rotation waits | Execution ms / 16 | Create ms / 16 | Final Record ms / 16 | Segments |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Disabled | 5,130 (4,893–8,198) | 57.08 | 238.46 | 1,464.13 | 0 | 2,627.99 | 1,600.34 | 3.48 | 22 |
| Background | 4,399 (4,135–7,443) | 58.00 | 362.21 | 1,496.33 | 2 (1–5) | 2,797.33 | 1,741.14 | 3.40 | 4 |

All 224 rotations and all 224 final Records commit. Background performs two useful compactions in
every sample and retains four instead of 22 Segments. Against disabled mode, background changes the
median by -14.3% throughput, +1.6% p50, +51.9% p95, and +2.2% p99. The bimodal local throughput and
p99 ranges are broad, so these deltas are diagnostic rather than release thresholds.

The churn attribution agrees with forced rotation: median replacement creation is 61–62% of total
rotation execution, Manifest publication is about 6%, residual in-memory work is about 4 ms total,
and all 16 final Record commits total only 3.4 ms median. Natural compaction overlap causes a median
two rotation waits and 33.02 ms cumulative wait, versus approximately 0.003 ms without maintenance.

## Decision

- Keep the four deep phases in `MaintenanceSnapshot`, daemon `STATS`, and benchmark CSV. They close
  the previous unattributed execution bucket without adding a storage lock.
- Do not optimize final Record commit or residual in-memory installation; both are negligible here.
- Treat replacement Segment creation as the first rotation-execution optimization target. Its
  variance should be correlated with APFS allocation and sync behavior before changing protocol.
- The forced tail is still dominated by the compaction publication lease, not rotation execution.
  The next decision should split that lease into intent publication, replacement build/copy,
  authoritative Manifest commit, and retirement. If replacement build/copy dominates, prototype
  building compaction replacements before publication authority, then revalidate/rebase at commit.
- Do not widen persistence v1 or shorten the lease yet. The pre-publication prototype needs explicit
  generation-conflict, cleanup, crash-recovery, space-preflight, and allocation-failure evidence.
