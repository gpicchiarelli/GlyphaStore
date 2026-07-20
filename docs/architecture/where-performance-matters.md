Status: descriptive
Applies to: product positioning and benchmark interpretation (not a wire or disk contract)
Owner: maintainer
Last reviewed: 2026-07-19

# Where GlyphaStore performance matters

A faster key-value path does **not** make every page load “magically twice as fast.” The value
depends on what fraction of the request is spent on small, frequent lookups versus business logic,
SQL, or remote AI calls.

## Typical request shape

```text
Browser
  → Nginx
  → FastAPI / Mojolicious / Go / …
  → GlyphaStore   (session, cache, flags, quotas, …)
```

One HTTP request may touch GlyphaStore for authentication material, session state, cached profiles,
permissions, rate limits, configuration, tokens, and similar. When those operations are cheap:

- mean latency falls;
- the same hardware serves more concurrent users;
- fewer app servers are needed for the same load.

That capacity effect is the real advantage for operators—not a headline microbenchmark alone.

## Workloads where the advantage is large

| Pattern | Why many small ops dominate |
| --- | --- |
| Distributed cache | Often 3–10 lookups per HTTP request |
| Session store | Every request: session → user → permissions → CSRF → expiry |
| API gateway | JWT/API-key checks, rate limits, quotas, config |
| Chat / presence | Online?, last position?, typing?, last message? |
| Gaming | Leaderboards, inventory, matchmaking, presence |
| AI serving | Embedding/prompt cache, feature flags, session, metadata |

These are millions of tiny operations. Engine and SDK cost compound across the whole site.

## Workloads where it changes little

```text
HTTP request
  → 20 ms business logic
  → 300 ms PostgreSQL
  → 100 ms OpenAI (or similar)
```

If GlyphaStore spends 0.15 ms instead of 0.30 ms, the user does not notice. The bottleneck is
elsewhere. Use GlyphaStore where the hot path is lookup-heavy; do not expect it to hide slow
dependencies.

## Engine vs SDK: the server is not the bottleneck

Published engine and raw-TCP suites reach **orders of magnitude** more operations per second than
the official language SDKs on the same host. The SDK 0.1.0 baseline (~10⁵ ops/s class on loopback)
measures client overhead; it does **not** cap GlyphaStore capacity.

That split is intentional:

- the core can stay fast while more language clients are added;
- client regressions are tracked separately from engine regressions;
- multi-language adopters do not force continuous core rewrites for framing costs.

See [SDK 0.1.0 analysis](../../benchmark-results-sdk-0.1.0-20260719-161956/analysis.md) and the
[benchmark standard](../spec/benchmark-standard.md).

## Perl SDK: where further speed comes from

The pure-Perl hot path is already squeezed (`pack 'Q<'`, in-place `sysread`, reused `IO::Select`,
`encode_request_hot`, multi-Worker overlap). A remaining ~2–2.5× gap versus Python **sequential** at
deep pipelines will not close with generic micro-tuning.

| Priority | Action | Effect |
| --- | --- | --- |
| Use the API correctly | Deep pipelines + `execute_worker_pipelines` / `execute_batch` | Overlap Workers inside one process; the 0.1.0 baseline was mostly sequential—re-read numbers with concurrent defaults when `workers>1` |
| Production scale | One client per Hypnotoad/prefork worker process; no shared ithreads | Real win: parallelism = N processes (documented contract) |
| Later | Mojolicious / `IO::Async` / AnyEvent adapter | Does not raise sync microbench ops/s; raises web-app throughput by not blocking the reactor |
| Optional big jump | XS only on encode/decode/FNV | Only large remaining SDK-side leap; FFI around the C++ client stays out of design |
| Secondary polish | Fewer response hashrefs, less FNV/`substr` copying | Typically single-digit %, not 2× |
| Like other SDKs | `connections_per_worker` only after measure; p50/p95 in suite 0.2 | Know *where* time goes before changing connection shape |

**Product performance:** processes + pipelines/overlap, then event-loop. **Closing the microbench
gap:** XS on framing. Everything else is polish.

## The real quid

The claim is not “faster than Redis” as a product slogan (Redis/RESP compatibility is a non-goal;
see [ADR 0001](../adr/0001-project-scope.md)).

The useful claim is:

> **The same application logic can handle more concurrent requests with less CPU and fewer servers
> when the hot path is many small key-value operations.**

Performance is one reason to choose the engine. A complete platform also needs replication,
reliable persistence, efficient snapshots, observability, authentication/ACL, and clustering. Those
remain roadmap work; see [production readiness](../production-readiness.md), the
[v1 roadmap](../v1-production-roadmap.md), and the [SDK roadmap](sdk-roadmap.md).
