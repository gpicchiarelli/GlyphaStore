Status: descriptive
Applies to: product positioning and benchmark interpretation (not a wire or disk contract)
Owner: maintainer
Last reviewed: 2026-07-23

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

Published engine and raw-TCP suites reach far higher operations per second than the official
language SDKs on the same host. The SDK 0.1.0 baseline (~10⁵ ops/s class on loopback) measures
client overhead; it does not cap GlyphaStore capacity.

Track client and engine regressions separately. Multi-language clients should not force continuous
core rewrites for framing costs.

See [SDK 0.1.0 analysis](../../benchmark-results-sdk-0.1.0-20260719-161956/analysis.md) and the
[benchmark standard](../spec/benchmark-standard.md).

## Perl SDK: where further speed comes from

The pure-Perl hot path is already tight (`pack 'Q<'`, in-place `sysread`, reused `IO::Select`,
`encode_request_hot`, multi-Worker overlap). Closing the remaining gap versus Python sequential at
deep pipelines needs structural changes, not generic micro-tuning.

| Priority | Action | Effect |
| --- | --- | --- |
| Use the API correctly | Deep pipelines + `execute_worker_pipelines` / `execute_batch` | Overlap Workers inside one process; concurrent ≈ sequential on loopback at deep pipelines, more stable under multi-Worker load ([Perl re-bench](../../benchmark-results-perl-0.1.0-20260720-170225/analysis.md)) |
| Production scale | One client per Hypnotoad/prefork worker process; no shared ithreads | Parallelism = N processes |
| Later | Mojolicious / `IO::Async` / AnyEvent adapter | Raises web-app throughput by not blocking the reactor |
| Optional | XS on encode/decode/FNV | Largest remaining SDK-side leap; FFI around the C++ client is out of design |
| Secondary | Fewer response hashrefs, less FNV/`substr` copying | Typically single-digit % |
| Like other SDKs | `connections_per_worker` only after measure; p50/p95 in suite 0.2 | Locate time before changing connection shape |

Production scale: processes plus pipelines/overlap, then an event loop. Microbench gap: XS on
framing.

## Product claim

Useful claim: the same application logic can handle more concurrent requests with less CPU and fewer
servers when the hot path is many small key-value operations.

Release and platform requirements beyond the embedded engine are tracked in
[production readiness](../production-readiness.md), the
[v1 roadmap](../v1-production-roadmap.md), and the [SDK roadmap](sdk-roadmap.md).
