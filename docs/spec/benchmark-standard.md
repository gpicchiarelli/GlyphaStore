# GlyphaStore Benchmark Standard

Status: normative measurement methodology
Applies to: performance claims and regression reports
Owner: performance maintainers
Last reviewed: 2026-08-26

## 1. Purpose

Benchmarks answer narrowly defined questions; they do not prove production capacity by themselves. Any published performance comparison must be reproducible, validate results, expose variance, and distinguish engine work from client, transport, storage, and scheduler costs.

## 2. Required environment record

Every report must include:

- full Git commit and whether the tree is dirty;
- compiler identity/version and relevant optimization flags, including LTO or PGO;
- operating system and kernel version;
- CPU model, logical/physical core count, architecture, and affinity policy;
- RAM capacity and relevant memory-pressure state;
- power source/mode and thermal-throttling note;
- storage device and filesystem for durable tests;
- Store mode, Worker count, client/thread count, queue/pipeline settings;
- operations, warmups, measured repeats, key/value sizes, and access distribution.

Results without this metadata are exploratory, not baseline evidence.

## 3. Sampling

Default microbenchmark policy is one warmup and seven independently measured repeats. Each repeat creates a fresh Index or Store. Preload/setup is outside the timed region unless the workload explicitly measures it. `--filter all` must run each benchmark with isolated state.

Report at least sample count, median, minimum, and maximum for elapsed seconds, nanoseconds per operation, and operations per second. Median is the comparison statistic. Minimum indicates attainable steady execution; maximum exposes interference. For service benchmarks also report p50, p95, p99, and maximum request latency when available.

Every sample validates operation counts, hits/misses, response IDs, status, and payload bytes. A fast sample with invalid results is a failed benchmark.

Automated retained reports must fail closed when an input suite is empty, required metadata is
missing, a result identity is duplicated, counts disagree with the run metadata, values are
non-finite/non-positive, or min/median/max ordering is invalid. Exploratory parsing may be more
permissive only when it is explicitly excluded from CI evidence.
When CI promises a fixed matrix, a machine-readable source contract must enumerate the complete
expected file set and per-source warmup/repeat policy. Missing and unexpected suite files or weaker
sampling both invalidate the retained report.

## 4. Canonical workloads

| Family | Question | Timed region |
|---|---|---|
| `index_insert_find` and Index subfilters | cost of the in-memory mapping | named Index operations only |
| `index_churn_miss` | miss stability after deleting 75% of a populated table | maintenance outside timed region; misses only |
| `store_put` | mutation path | puts; construction excluded |
| `store_get` | visible read path | preload excluded; gets only |
| `store_put_get` | phase-composed Store work | documented put phase plus get phase |
| `store_read_after_write` | representative immediate reuse | alternating put and get per key |
| `store-parallel-*` | public Store scaling | all client operations and synchronization |
| `store-durable-*` | persistence-mode path | explicitly named durability policy |
| durable compaction | physical reclaim benefit/cost | public `Store::compact()` only; seed/flush/reopen/verification excluded |
| concurrent maintenance | foreground cost, reclaim progress, and forced rotation phases for disabled/cooperative/background policy | mixed public GET/PUT calls only; preload/flush/close/reopen/verification excluded; forced rotation reports publication wait, Segment seal, replacement creation, Manifest publication, residual execution, complete rotation, final Record commit, and measurement residual separately |
| server TCP | parser, loopback transport, executor, Store, response | validated request/response pipeline |

Sequential/random describe key visitation order, not Worker distribution. Parallel distribution must be named:

- **worker-affine**: each client primarily targets keys owned by its Worker;
- **uniform**: every client distributes operations across the complete routed key set.

Do not compare an internal owner-bound function with the public Store path as though they were the same workload.

## 5. Standard matrix

Single-thread reports should cover key sizes 8, 16, 32, 64, and 256 bytes and value sizes 0, 64, and 256 bytes where meaningful. Scaling reports should include 1, 2, and 4 Workers/clients, then additional physical-core counts supported by the machine. Both affine and uniform distributions are required.

For a fixed TCP pipeline depth, report Worker scaling as:

```text
speedup(W, p)    = median_ops_per_second(W, p) / median_ops_per_second(1, p)
efficiency(W, p) = speedup(W, p) / W
```

The highest observed median per Worker count is a descriptive matrix summary, not an automatic
optimum: min/max overlap, latency, bandwidth, and resource use still govern interpretation.

TCP reports must state connection count, pipeline depth, request mix, payload sizes, executor affinity, and whether server/client share a process and CPUs. A same-process loopback result must be labeled as such because load generation competes for CPU and caches.
Canonical TCP matrix filenames, metadata, and result coordinates must agree on Worker/client count
and pipeline depth; transport mode, routing distribution, storage mode, and latency instrumentation
are part of the validated workload identity.

Cross-SDK reports must validate the exact expected language/runtime/execution/Worker/pipeline grid.
A run that omits an SDK is exploratory and must name every omission; it is not a complete SDK
comparison. The result contract must reject missing or extra cells, SDK version drift, wrong
operation/sample counts, non-finite values, and inconsistent min/median/max ordering.

## 6. RAM measurement

Report baseline, peak, and final resident set size (RSS), plus peak delta over baseline. State the scope: current server process, client process, or whole benchmark process. On platforms where RSS includes file-backed mapped pages, say so.

For engine memory analysis also report, when instrumented:

- Index control/slot capacity;
- inline versus arena key bytes, including dead arena bytes;
- hot-record cache bytes;
- live and allocated Segment bytes;
- connection input/output capacity and queued handoffs.

`--latency` is available for durable parallel PUT and GET workloads. It adds one steady-clock sample
around each operation and is therefore a tail-latency diagnostic, not a throughput baseline; report
its instrumentation mode and do not compare its throughput with an uninstrumented run.

RSS is not allocator-exact ownership. It must not be divided by operation count and called record size without component evidence.

## 7. Bandwidth

Server results must report both logical application payload and wire traffic:

```text
logical ingress B/s  = sum(request key + value bytes) / measured seconds
logical egress B/s   = sum(successful response value bytes) / measured seconds
wire ingress B/s     = sum(complete request frame bytes) / measured seconds
wire egress B/s      = sum(complete response frame bytes) / measured seconds
aggregate wire B/s   = wire ingress + wire egress
```

Report bytes and operations together. Empty-value workloads can show high operations/s but negligible payload bandwidth; large values can show lower operations/s and higher served bandwidth. TCP/IP/Ethernet framing overhead is outside protocol-wire metrics unless separately measured by the network stack.

Durable benchmarks should additionally report logical Record bytes written, filesystem bytes written when observable, and write amplification as a ratio with a precisely named numerator/denominator.
Shared mutation-completion counters do not by themselves identify a durable workload. A report may
label queue/service/commit data as a durable pipeline profile only when the recorded storage mode is
explicitly durable and the durable completion counter is non-zero.

## 8. Isolation and affinity

Linux `--pin-cpu` may pin eligible single-thread benchmarks to CPU 0; output must say whether affinity was requested and successfully applied. On macOS, affinity is advisory or unavailable and must not be presented as hard pinning.

For scaling tests, reserve CPUs for load generation or run clients in a separate process/host. If this is not done, explicitly state that the result measures combined server/client scheduling. Avoid unrelated foreground load; repeat after cooldown when max/min spread suggests thermal or scheduler interference.

## 9. Comparing revisions

Use identical hardware, build type, CLI, data placement, and environmental policy. Automated
revision reports must suppress deltas when their recorded runner OS/architecture, image version,
kernel, CPU model/count, compiler, build preset, or benchmark-contract digest differ or are absent.
Result matching must include operations, warmups, and measured repeats. Interleave old/new runs when
practical to reduce drift. Report absolute medians and ratios, not only percentages.

A change is a plausible regression only when it repeats across runs and exceeds normal spread.
Automated reports must classify overlapping current/baseline min/max throughput ranges as
inconclusive; only disjoint ranges may be labeled improvement or regression candidates. Stronger
paired/statistical evidence may supersede this conservative classification. Optimize a named
bottleneck only after decomposing relevant time (hashing, Index, record codec, I/O, queue, wakeup,
parsing, response).

## 10. Canonical commands

```sh
./scripts/dev.sh benchmark --filter store-put-get --ops 200000 --warmup 1 --repeats 7
./scripts/dev.sh benchmark --filter store-parallel-all --workers 4 --threads 4 --distribution uniform --warmup 1 --repeats 7
./scripts/dev.sh benchmark-durable --ops 20000 --warmup 1 --repeats 7
./scripts/dev.sh benchmark-compaction --warmup 1 --repeats 7
./scripts/dev.sh benchmark-maintenance --warmup 1 --repeats 7
./scripts/dev.sh benchmark-maintenance --scenario forced-rotation --warmup 1 --repeats 7
./scripts/dev.sh benchmark-maintenance --scenario idle --warmup 0 --repeats 7
./scripts/dev.sh benchmark-maintenance --scenario churn --warmup 0 --repeats 7
./scripts/dev.sh benchmark-server --ops 100000 --workers 4 --clients 4 --pipeline 32 --executor-affinity --warmup 1 --repeats 7
```

Preserve raw machine-readable output as a CI artifact. Tables in prose are summaries and must identify their source artifact.

## 11. Claim language

- **measurement:** exact result for the documented environment;
- **comparison:** controlled result against a named revision;
- **inference:** likely cause supported by counters or decomposition;
- **target:** desired future threshold, not current performance;
- **competitive claim:** requires comparable external systems, versions, durability semantics, and hardware.

Never label a number “excellent” in a specification. State the workload, bottleneck, scaling efficiency, bandwidth, latency, memory, and comparison set.
