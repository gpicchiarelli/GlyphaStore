Status: descriptive
Applies to: product positioning and benchmark interpretation (not a wire or disk contract)
Owner: maintainer
Last reviewed: 2026-08-01

# Where GlyphaStore performance matters

A faster key-value path does not accelerate every request equally. Value depends on how much of the
request is spent on small, frequent lookups versus business logic or remote calls.

## Typical request shape

```text
Browser
  → reverse proxy
  → application
  → GlyphaStore   (session, cache, flags, quotas, …)
```

One HTTP request may touch GlyphaStore for authentication material, session state, cached profiles,
permissions, rate limits, configuration, tokens, and similar. When those operations are cheap:

- mean latency falls;
- the same hardware serves more concurrent users;
- fewer app servers are needed for the same load.

Capacity and latency under lookup-heavy load are the operator-relevant outcomes—not a single
microbenchmark number.

## Workloads where the advantage is large

| Pattern | Why many small ops dominate |
| --- | --- |
| Distributed cache | Often 3–10 lookups per HTTP request |
| Session store | Every request: session → user → permissions → CSRF → expiry |
| API gateway | Token checks, rate limits, quotas, config |
| Chat / presence | Online?, last position?, typing?, last message? |
| Gaming | Leaderboards, inventory, matchmaking, presence |
| AI serving | Embedding/prompt cache, feature flags, session, metadata |

These paths issue many tiny operations. Engine and SDK cost compound across the site.

## Workloads where it changes little

```text
HTTP request
  → 20 ms business logic
  → 300 ms relational query
  → 100 ms external API
```

If GlyphaStore spends 0.15 ms instead of 0.30 ms, the end-user latency is unchanged. Use GlyphaStore
where the hot path is lookup-heavy; it does not hide slow dependencies.

## Engine vs SDK

Engine, raw-TCP and public-SDK suites measure different work. The public SDK path includes API
validation, routing, request encoding, owned results and outcome construction; a raw-wire harness
may pre-encode frames. A slower SDK number therefore does not by itself cap server capacity.

Track client and engine regressions separately. Multi-language clients should not force continuous
core rewrites for framing costs.

Use `./scripts/benchmark_sdk_clients.sh` for the current comparison matrix and interpret it under
the [benchmark standard](../spec/benchmark-standard.md). Local `benchmark-results*` directories are
gitignored and must not be cited as durable repository documentation.

## Perl SDK: where further speed comes from

The Perl client already aggregates a Worker pipeline, buffers reads and overlaps Worker sockets in
one `IO::Select` loop. It still creates public request/result hashes, assembles encoded frames per
call and materializes owned GET scalars. Those costs are candidates for profiling, not presumed
single-digit details.

| Priority | Action | Effect |
| --- | --- | --- |
| Use the API correctly | Pipelines + `execute_worker_pipelines` / `execute_batch` | Amortize syscalls and overlap independent Workers; select depth from throughput and tail latency |
| Production scale | One client per Hypnotoad/prefork worker process; no shared ithreads | Parallelism = N processes |
| Profile first | Allocation/copy counts, parser/buffer work, `IO::Select`, syscalls | Locate CPU and waiting time before choosing a technique |
| Pure Perl | Flatten internal metadata/results, avoid eager hashes/copies, reuse buffers where ownership permits | Reduce interpreter and allocator work without packaging cost |
| Optional native kernel | Narrow XS codec/routing path, only if the profile is dominated there | Trade packaging complexity for measured CPU reduction |
| Concurrency candidates | Event-loop adapter or `connections_per_worker`, after workload evidence | Improve application or hot-Worker concurrency; not automatically single-pipeline speed |

The performance mindset is empirical: preserve semantics, change one cost centre, and rerun the same
validated workload. XS is one possible outcome of that process, not the starting assumption.

## Lab hot-path evidence

Same-machine macOS Apple Silicon measurements and cost maps for Store GET/PUT and TCP are recorded
in [hot-path-performance-2026-08-01.md](hot-path-performance-2026-08-01.md). Those figures are
architectural-prototype lab evidence, not production capacity claims. Daemon clients should keep
one connection per Worker (`BIND_WORKER` / owner-bound); embedded uniform multi-thread PUT still
crosses shard Writers and is not equivalent to that model.

## Product claim

Useful claim: the same application logic can handle more concurrent requests with less CPU and fewer
servers when the hot path is many small key-value operations.

Release and platform requirements beyond the embedded engine are tracked in
[production readiness](../production-readiness.md), the
[v1 roadmap](../v1-production-roadmap.md), and the [SDK roadmap](sdk-roadmap.md).
