# Performance analysis

## Net judgment

GlyphaStore has a strong shard-local in-memory path and a TCP engine that benefits substantially from
parallel owner-bound execution and pipelining. It is not yet possible to call the system performance
profile production-complete. The largest measured weakness is cross-Worker access; the largest
unmeasured risks are component-level memory amplification, cold durable reads during concurrent
mutation/compaction, and foreground latency during maintenance.

The public synchronous C++ client is correct and useful as a reference implementation, but it is not a
high-throughput client. Synchronous durability is storage-latency-bound as expected; periodic and group
policies change throughput by orders of magnitude and therefore must never be compared without naming
their durability semantics.

## Representative measurements

| Area | Workload | Median result |
| --- | --- | ---: |
| Index | 16-byte key, find hit, Release | 13.30 M ops/s |
| Store | PUT, 16-byte key / 64-byte value, LTO | 4.00 M ops/s |
| Store | read-after-write, 16/64, LTO | 4.40 M ops/s |
| Parallel Store | 8 Workers, worker-affine read-after-write | 21.80 M ops/s |
| Parallel Store | 10 Workers, uniform read-after-write | 4.96 M ops/s |
| Raw TCP | 4 Workers, pipeline 128, 16/64 | 5.42 M ops/s; 867 MB/s duplex |
| Raw TCP latency | 2 Workers, pipeline 32 | p50 67.25 us; p99 144.50 us; p99.9 1.03 ms |
| Raw TCP latency | 4 Workers, pipeline 1 | p50 36.04 us; p99 89.88 us; p99.9 254.13 us |
| C++ client latency | 4 Workers, synchronous API | p50 40.17 us; p99 86.00 us; p99.9 154.38 us |
| Durable sync PUT | one Worker | 196 ops/s; 5.10 ms/op |
| Durable periodic PUT | one Worker | 115.35 k ops/s; 8.67 us/op |
| Durable group PUT | 32 threads | 6.21 k ops/s; p50 4.99 ms; p99 6.86 ms |
| Recovery open + reads | 256 records | 250.71 k records/s; 3.99 us/record |
| TCP large value | 65,536 bytes, 4 Workers, pipeline 32 | 62.44 k ops/s; 4.11 GB/s duplex |

All rates count the operation semantics printed in the raw artifact. TCP and composed PUT/GET results
count both operations. Bandwidth is protocol-frame traffic, not Ethernet traffic.

## Highest-priority findings

### P0 — Cross-Worker ownership is the dominant scalability boundary

At 10 threads, worker-affine read-after-write reaches 19.06 M ops/s while uniform routing reaches
4.96 M ops/s, only 26% of the affine result. At 8 threads the corresponding results are 21.80 M and
4.44 M ops/s. This is consistent with Worker mutex/cache-line migration and arbitrary callers crossing
ownership boundaries; it is not evidence of a single global catalog lock.

The next design experiment should retain independent Worker linearization and generation pins while
moving cross-owner work through bounded per-Worker queues or batches. The acceptance benchmark must
show same-Worker writes and reads progressing while another Worker is stalled, and must report queue
wait separately from execution. Replacing the Worker mutex with a catalog/cache mutex would not solve
the measured problem.

### P0 — Memory amplification cannot yet be measured correctly

RSS values include benchmark key/value material, prepared TCP batches, allocator state, server and
client threads, connection buffers, and engine storage. The normative benchmark standard explicitly
forbids dividing this RSS by operation count and calling the result record size. Consequently the
current suite cannot confirm or reject the concern about 80-byte slots, fixed 64 MiB Segments, and the
duplicated durable hot cache.

Add allocator-independent counters for Index control/slot capacity, inline and arena key bytes, dead
arena bytes, live/allocated Segment bytes, hot-cache payload and hash-node bytes, pinned generations,
connection buffer capacity, and maintenance scratch space. A steady-state fill/churn/compact benchmark
must sample those counters before and after maintenance and after allocator purge where supported.

### P0 — Maintenance and cold-I/O interference remain unprofiled

There is no benchmark that runs foreground traffic while `compact()`, rotation, reclaim, or a cold
durable GET is active. Therefore this run cannot validate the two critical architectural claims: no I/O
under the Worker mutex, and no equivalent head-of-line blocking through the runtime catalog or file
cache.

The missing benchmark must force page-cache misses, hold exact generation pins, run concurrent
same-Worker and other-Worker mutations, trigger source retirement, and report queue delay plus
p50/p95/p99/p99.9/max latency. Maintenance must be phase-budgeted so build, publication, fsync, and
retirement are distinguishable.

### P0 — A paired regression candidate remains on composed PUT/GET

The old checked-in report suggested a 20–28% LTO Store regression, but it compared a dirty historical
tree with a different run and is not sufficient evidence. A clean interlaced A/B between `7f54681` and
`28a12ae` gives:

| Isolated workload | Old round 1 | New round 1 | Old round 2 | New round 2 | Mean delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| `store_put_get_copy` | 2.79 M | 2.45 M | 2.87 M | 2.54 M | -11.83% |
| `store_read_after_write_copy` | 3.24 M | 3.32 M | 3.24 M | 3.03 M | -2.07% |

The composed PUT/GET result is repeatable enough to require bisection across the Store/catalog changes.
Read-after-write changes sign between rounds and is inconclusive. Absolute A/B rates drifted from the
earlier suite, reinforcing that Low Power Mode and macOS scheduling prevent release-grade claims.

### P1 — The synchronous C++ client pays roughly twice the raw pipeline-1 cost

At four Workers the public C++ API reaches 79.64 k ops/s versus 170.46 k for the raw-wire harness at
pipeline 1: 46.7% of the raw rate. Latency instrumentation gives similar p50/p99 call latency, so part
of the throughput gap is client-side serialization, polling, frame allocation/copying, and benchmark
scheduling rather than server execution alone.

Preserve the synchronous client as the correctness reference. Optimize reusable frame buffers,
scatter/gather writes, receive-buffer compaction, and poll/syscall avoidance before treating it as the
production client. A pipelined/asynchronous client requires an explicit request-lifetime and
backpressure contract; it should not weaken retry indeterminacy or Worker binding.

### P1 — Pipelining is essential and exposes bounded-watermark behavior

Raw TCP at four Workers rises from 170.46 k ops/s at pipeline 1 to 5.42 M at pipeline 128, a 31.8x
increase. One to four Workers at pipeline 128 scales from 2.27 M to 5.42 M, or 2.39x. This shows that
round trips and syscall/event-loop interaction dominate the shallow pipeline, while the deeper pipeline
approaches engine and memory-copy limits.

A 65,536-byte value fails at pipeline 128 because outstanding frames exceed the 4 MiB per-connection
input/output watermarks; pipeline 32 succeeds. Clients should derive or negotiate a byte budget rather
than using an operation-count pipeline independent of frame size.

### P1 — The 24/25-byte inline-key boundary is expensive

Index find-hit throughput falls from 12.84 M ops/s at 16-byte keys to 7.29 M at 32 bytes, a 43% drop as
keys move beyond the 24-byte inline representation into the arena. It then falls to 5.55 M at 64 bytes
and 1.56 M at 256 bytes. Profile arena locality, hash cost, and comparison bytes separately. Candidate
experiments are cached prefixes/fingerprints, length-specialized comparison, and controlled prefetch;
each must preserve collision correctness.

### P1 — Apple heterogeneous-core topology needs an explicit policy

Worker-affine read-after-write scales 5.18x from one to eight Workers, then declines 12.6% at ten.
This Apple M4 has four performance and six efficiency cores, while affinity is only advisory and Low
Power Mode was enabled. A default of `hardware_concurrency()` is therefore not automatically optimal.
Topology selection should understand performance levels, and benchmarks should separate server and
load-generator CPUs before changing the default Worker count.

### P2 — LTO helps Store code, but native CPU flags do not win consistently

Within the initial run, LTO versus ordinary Release improved Store PUT by 28.9%, GET by 9.9%, and
read-after-write by 8.7%; Index results were mixed. The native build did not consistently beat LTO.
Because runs were not interleaved and variance was material, this supports keeping LTO in the
production benchmark matrix, not claiming a guaranteed gain.

## CPU sample interpretation

The five-second macOS wall-clock sample is dominated by waiting: `recv`, `kevent`, condition waits,
and `send`. Among executable hot symbols, the leading groups are SwissTable insert probing and rehash,
frame-size validation/decoding, response queuing/encoding, record CRC, and memory movement. No catalog
mutex dominates this sample, but absence in a sampled hot in-memory workload is not proof that cold
reads or publication avoid catalog head-of-line blocking.

The profile also catches rehash in a growing workload. A second steady-capacity profile should preload
and reserve the target cardinality so startup growth is not mistaken for steady-state cost.

## Durability interpretation

Synchronous PUT is approximately 5.10 ms per operation and is governed by storage durability latency.
Periodic mode improves write throughput by roughly three orders of magnitude but exposes a different
loss window. Group mode reaches 6.21 k ops/s with 32 writers while retaining roughly 5–7 ms request
tail latency. The current harness does not report filesystem bytes written or write amplification, so
no storage-efficiency claim is justified.

Recovery at 256 records is only a correctness-scale latency point. Production readiness still needs
recovery curves over record count, Segment count, hot-cache budget, corruption checks, and cold-cache
conditions.

## Limits and next benchmark block

- Low Power Mode was active; rerun the release baseline with it disabled and stable AC power.
- macOS affinity is advisory; clients and server shared CPUs and caches.
- The CPU sample is wall-clock sampling, not hardware-counter evidence for IPC, cache, TLB, or branch
  misses.
- No Redis, DragonflyDB, or Memcached comparison was made. A competitive claim requires adapters,
  exact versions, equivalent durability semantics, separate clients, and identical datasets.
- No NUMA or Linux/Windows result can be inferred from this Apple Silicon run.

The next benchmark implementation block should be, in order: component memory telemetry; concurrent
cold-read/compaction latency with generation pins; maintenance phase timing; a separate-process client;
then Linux `perf`/NUMA and Windows ETW/IOCP profiles on native machines.
