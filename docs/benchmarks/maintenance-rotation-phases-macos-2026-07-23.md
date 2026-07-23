# Durable rotation phase benchmark on macOS — 2026-07-23

Status: exploratory local measurement, not a release baseline
Applies to: durable-periodic rotation and background maintenance
Owner: performance and persistence maintainers
Last reviewed: 2026-07-23

## Question

When compaction and an unrelated Worker rotation overlap, how much time is spent waiting for
Manifest publication authority, how much is rotation execution, and how much remains in the
foreground PUT outside `rotate_active()`?

The same runtime telemetry is then exercised through sustained churn to determine how often the
forced worst case occurs naturally. This is an internal policy measurement, not a competitive
database benchmark.

## Environment and method

| Item | Value |
| --- | --- |
| Source | clean `4908752` |
| Build | CMake `Release`, Apple clang 21.0.0, C++23, `-O3 -DNDEBUG`, ARM64, LTO off |
| Host | Apple M4, 10 physical / 10 logical cores, 16 GiB RAM |
| OS | macOS 26.5.2 (25F84), Darwin 25.5.0 |
| Storage | internal Apple SSD, APFS |
| Thermal / power policy | not controlled or instrumented; no CPU affinity |
| Sampling | seven measured repeats per mode; one warmup for forced rotation; rotated mode order |

Raw results:

- [`data/maintenance-rotation-phases-macos-2026-07-23.csv`](data/maintenance-rotation-phases-macos-2026-07-23.csv)
- [`data/maintenance-churn-phases-macos-2026-07-23.csv`](data/maintenance-churn-phases-macos-2026-07-23.csv)

Runtime phase definitions:

- `publication_wait`: entry into `rotate_active()` through acquisition of the Manifest serializer
  and completion of any active-compaction condition wait;
- `execution`: validated rotation planning, Segment seal/create, Manifest publication, and in-memory
  installation after publication authority is available;
- `accounted_total`: publication wait plus execution;
- `residual_put`: externally measured foreground PUT latency minus `accounted_total`; it includes
  the final Record append/commit after rotation and measurement-call overhead.

The telemetry is runtime-local and uses atomics. A short versioned publication prevents a reader
from mixing completed duration aggregates from different rotations. It adds no storage lock and
does not change the persistent format.

## Forced overlap

Worker 0 owns a 31.01 MiB useful compaction candidate. Worker 1 is filled exactly to its active
Segment Record boundary. A filesystem hook releases the Worker 1 PUT only after Worker 0 publishes
its compaction intent, guaranteeing that the rotation reaches the active publication lease.

| Mode | PUT/rotation ms median (min–max) | Publication wait ms | Execution ms | Residual PUT ms | Wait share |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disabled | 55.77 (42.32–130.35) | 0.00008 | 55.36 | 0.413 | ~0.0% |
| Cooperative | 216.39 (185.32–385.10) | 154.68 | 60.89 | 0.442 | 71.5% |
| Background | 189.32 (181.44–366.47) | 142.92 | 45.98 | 0.474 | 75.5% |

Every sample reports one attempted and committed rotation. Every cooperative/background sample
reports exactly one compaction wait and zero foreground or maintenance errors. Internal accounting
plus residual PUT time matches the externally measured latency.

The important result is causal: 71–75% of forced-overlap latency is the intentional wait before
rotation execution. The execution medians and ranges overlap across policies, while residual PUT
time remains below 0.8 ms in all 21 samples. The Manifest mutex is not contended during rotation
I/O; the rotation waits on the condition variable first, then owns publication authority while it
performs the existing durable transition.

The broad slow cluster remains visible in all modes. This uncontrolled local matrix cannot assign
the 42–130 ms baseline execution variance to code rather than APFS, storage flush, thermal, or
scheduler effects.

## Sustained churn

Each sample issues 32,768 successful 32 KiB PUTs to Worker 1—1 GiB of logical foreground mutation
and 16 rotations—while Worker 0 starts with the same compaction candidate. Background maintenance
uses a deliberately aggressive 10 ms interval. Settling now requires both at least one useful
compaction and no observed candidate at or above the 50% dead-byte threshold, preventing a
sequence-conflict sample from closing with reclaimable history still present.

| Mode | Ops/s median (min–max) | p50 µs | p95 µs | p99 µs | Rotations | Rotation waits | Segments |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Disabled | 5,213 (5,129–5,501) | 54.29 | 233.29 | 1,445.88 | 16/16 | 0 | 22 |
| Background | 5,083 (4,830–5,397) | 54.71 | 240.29 | 1,455.17 | 16/16 | 1 | 4 |

Background versus disabled changes median throughput by -2.5%, p50 by +0.8%, p95 by +3.0%, and
p99 by +0.6%. Every background sample completes exactly two useful compactions and finishes with
four Segments. Maintenance sequence conflicts range from zero to three and are retried internally;
all foreground PUTs and reopen/model validations succeed.

Only one of 16 rotations waits for compaction in every background sample. Its cumulative
publication wait is 22.69 ms median (12.87–41.78 ms), compared with 2.6 microseconds across all 16
disabled rotations. Total rotation execution is essentially the same: 2,778.94 ms background
versus 2,797.16 ms disabled. The forced benchmark is therefore a real worst-case boundary, but not
the typical cost of every rotation during this churn shape.

## Decision

- Keep condition-based v1 serialization on macOS. It removes write rejection, and natural churn
  intersects the lease on only one of 16 rotations in this matrix.
- Treat the forced-overlap wait as a tail-latency risk, not general lock contention. The remaining
  mutex-protected rotation execution is comparable with the no-maintenance path.
- Keep the new phase counters in `MaintenanceSnapshot` and daemon `STATS`; they make wait frequency
  and duration observable in production without persistent-format changes.
- Do not justify a persistence-v2 third-authority protocol from this evidence. First establish a
  rotation latency objective and obtain controlled APFS measurements.

The requested split into Segment seal, replacement creation, Manifest publication, residual
execution, and final Record commit is completed in the
[deep-phase macOS follow-up](maintenance-rotation-deep-phases-macos-2026-07-23.md). That evidence
localizes rotation execution mainly to replacement creation and leaves shortening the compaction
publication lease as a separately instrumented next decision.
