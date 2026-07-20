# SDK 0.1.0 benchmark analysis

## Purpose of this suite

This suite is a **public SDK 0.1.0 baseline**. It does not claim absolute GlyphaStore server
capacity. It measures, reproducibly, the end-to-end overhead of the official clients on same-host
loopback against a volatile `glyphastored`.

In other words it answers:

> How much throughput do I get when I use the official SDKs?

That is a different question from the raw TCP / in-process engine benchmarks, which show higher
orders of magnitude when the client is not the limiter.

## Why the methodology is strong

- Complete matrix: Workers 1/2/4, pipeline 1/8/32/128, Python sync, Python async, Perl
- Warmup + 7 measured repeats; median is the comparison statistic
- Environment, daemon build, and SDK version pinned (`0.1.0`)
- Raw result files plus `results.json` for charts and CI regression
- Every sample validates response count, success, and GET payload bytes

Scores for this publication (methodological):

| Dimension | Score |
| --- | ---: |
| Reproducibility | 10/10 |
| Configuration coverage | 9.5/10 |
| Documentation | 9.5/10 |
| CI regression utility | 10/10 |
| Public SDK benchmark usefulness | 9/10 |

## Observed rates (median ops/s)

### Pipeline depth effect

| Client | p=1 | p=128 | Gain |
| --- | ---: | ---: | ---: |
| Python sync concurrent | ~37 k | ~108–113 k | ~2.9× |
| Python async | ~26 k | ~98–100 k | ~3.7× |
| Perl sequential | ~21 k | ~45 k | ~2.2× |

Pipeline depth is the dominant lever, as expected for an ordered non-atomic batch API.

### Sync vs async at deep pipeline

At pipeline 128 / 4 Workers:

- Python sync concurrent ≈ **113 k** ops/s
- Python async ≈ **100 k** ops/s

The gap is small. At that operating point the bottleneck is no longer the sync/async scheduling
model; it is more likely Python framing cost, serialization, syscalls, the TCP stack, and protocol
overhead—not the server’s shard path.

### Worker scaling under this client load

From ~107.8 k (1 Worker, p=128, Python sync) to ~113.0 k (4 Workers) the increase is small. That is
useful evidence for this suite: the client is already near saturation, so adding server Workers does
not unlock more SDK throughput. The benchmark is measuring the client, not discovering server
capacity.

### Fair cross-language compare

Perl is single-process. The 0.1.0 published Perl rows were largely **sequential** across Workers.
Compare those to Python **sequential**, not to Python concurrent/async. On that machine Python
sequential remained roughly 2–2.5× faster than Perl at deep pipelines.

The harness now defaults to `execute_worker_pipelines` when `workers>1`. A dedicated re-bench
([Perl 2026-07-20](../../benchmark-results-perl-0.1.0-20260720-170225/analysis.md)) shows concurrent
≈ sequential at deep pipelines (CPU-bound framing), with clearer wins on shallow pipelines and
better stability when sequential multi-Worker runs jitter. Pure-Perl framing remains saturated;
further product wins are process-level parallelism and pipeline overlap, not more generic
micro-tuning (optional XS on the codec is the only large remaining SDK leap). See
[where performance matters](../../docs/architecture/where-performance-matters.md).

## What this suite does not prove

It does not prove that GlyphaStore tops out near 113 k ops/s. Server-side and raw-TCP suites already
show much higher rates when the load generator is not the Python/Perl SDK path.

## Planned strengthenings for SDK 0.2

- Client-side p50 / p95 / p99 latency
- MB/s and bytes-per-request accounting
- CPU% and read/write syscall counts
- Zipf and mixed (e.g. 90/10 read/write) workloads
- Working set larger than LLC
- Non-loopback / real network path

## Bottom line

Publish raw results and the summary together so SDK performance can be tracked release-to-release.
This 0.1.0 folder is the first such baseline.

For how these numbers (and the much higher engine rates) translate to real services—cache, session,
gateway, chat, gaming, AI—see
[Where GlyphaStore performance matters](../../docs/architecture/where-performance-matters.md).
The short version: the win is more concurrent requests per CPU when the hot path is many small
lookups, not a magically faster page when PostgreSQL or an LLM dominates the budget.
