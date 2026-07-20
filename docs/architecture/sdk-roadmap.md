Status: roadmap
Applies to: native SDKs (C++, Python, Perl, Go; Ruby planned) and shared wire contract
Owner: maintainer
Last reviewed: 2026-07-20

# SDK and client roadmap

The official clients already share the correct nucleus: wire protocol v2, Worker routing, ordered
pipelines, timeouts, controlled reconnect, and `committed` / `rejected` / `indeterminate` mutation
outcomes. The remaining work is not “rebuild the SDKs.” It splits into **completing the shared
contract** and **making GlyphaStore usable in production**.

Related: [production readiness](../production-readiness.md),
[v1 production roadmap](../v1-production-roadmap.md),
[documentation roadmap](../documentation-roadmap.md),
[C++ client API / cross-language contract](../reference/cpp-client-api.md),
[where performance matters](where-performance-matters.md),
[Ruby SDK roadmap](ruby-sdk-roadmap.md).

## Verdict (2026-07-20)

| Client | Assessment |
| --- | --- |
| C++ | Effectively complete for alpha |
| Python | Effectively complete, including async |
| Perl | Complete as a synchronous client; performance path is process-scale + optional XS |
| Go | Complete as a synchronous client (protocol + client + interop + CI) |
| Ruby | **Planned** — isomorphic sync client first; see [Ruby SDK roadmap](ruby-sdk-roadmap.md) |

Do not add languages without an isomorphism plan and Phase-1 correctness gates. New SDKs must meet
[client semantics v1](../spec/client-semantics-v1.md) and the interop matrix before they count as
official. The server must still be treated as loopback / tightly controlled private network /
sidecar / development only until authentication and TLS exist.

## Immediate client priorities

### 1. Cross-SDK interoperability suite (highest SDK gap) — **done for alpha matrix**

`scripts/test-sdk-interop.sh` starts a volatile `glyphastored` and proves PUT→GET across
C++ / Python / Perl / Go (and same-SDK) for binary keys, empty values, per-SDK pipelines, and short
TTL expiry on Workers 1 / 2 / 4. Wire golden fixtures are verified and compared to vendored SDK
copies in the same script and in CI.

Still desirable later: 8 Workers, explicit limit/error matrices, and released-artifact cross-version
compat.

### 2. Normative wire and client semantics — **client errors/retry/timeouts done**

Official TCP client behavior is frozen in [client semantics v1](../spec/client-semantics-v1.md)
([ADR 0019](../adr/0019-client-error-retry-timeout.md)): portable categories, wire→outcome tables,
automatic retry limits, monotonic deadlines, and late-response/connection-reset rules.

Still open on the wire/server side (see [production readiness](../production-readiness.md)): fuller
normative treatment of daemon cancellation after admission, configuration precedence, and some
limit/concurrency guarantees beyond the client contract.

### 3. Optional mutation idempotency key (post-alpha candidate)

Outcomes are classified correctly, but applications cannot auto-heal `indeterminate` without their
own policy. A stable `client_id + operation_id` retained briefly on the server would allow safe
`PUT`/`ERASE` retries without double-apply. Not required for the first release; it turns
indeterminate from “application problem” into “protocol-managed.”

### 4. Multi-Worker batch API — **done**

`execute_batch` (C++ / Python sync+async / Perl) groups requests by Worker, runs one pipeline per
Worker (overlapping when multiple Workers are involved), and restores caller order. Not a
transaction: Workers succeed or fail independently after admission. Perl still exposes
`execute_worker_pipelines` for explicit per-Worker vectors.

### 5. Configurable connections per Worker (measure first)

Today: one connection per Worker. Concurrent same-Worker traffic serializes. A future
`connections_per_worker = 1|2|4|…` helps Zipf / hot Worker / slow reads / many threads—but only after
benchmarks prove the connection is the bottleneck. Suite 0.2 should add p50/p95 so that decision is
data-driven.

### 5b. Perl performance path (guidance)

Pure-Perl hot path is saturated for generic tuning. Prefer: deep pipelines + Worker overlap, one
client per prefork process, later event-loop adapter for web throughput. Optional XS on framing is
the only large remaining microbench lever; C++ FFI remains out of design. Re-measure with
`./scripts/benchmark_perl_client.sh` (sequential + concurrent). Details:
[where performance matters](where-performance-matters.md#perl-sdk-where-further-speed-comes-from).

### 6. Per-request deadlines — **done**

Optional per-call request timeout overrides the configured default (same absolute deadline across
automatic retries). See [client semantics §6.5](../spec/client-semantics-v1.md). Surfaces:
`RequestOptions` (C++), `timeout=` (Python), `timeout =>` (Perl), `CallOptions` (Go).

### 7. Structured errors (uniform across SDKs) — **done**

Official clients expose client-semantics §2.1 fields: category, message, wire status, request id,
Worker, routing epoch, bytes sent, retryability, operation, mutation outcome. Surfaces: enriched
`glyphastore::Error` (C++), `GlyphaError` attributes (Python), `GlyphaStore::Error` accessors (Perl),
`client.Error` (Go).

### 8. Perl monotonic clock — **done**

Deadlines use `clock_gettime(CLOCK_MONOTONIC)` via `Time::HiRes`, not civil `time`.

### 9. Perl thread / fork contract — **documented**

README and Client POD state: not shareable across ithreads; do not reuse pre-`fork` sockets; one
client per process. Prefer `execute_worker_pipelines` for multi-Worker overlap.

### 10. Go client — **done for alpha**

`sdk/go` provides a production-oriented synchronous client (`protocol` + `client`), golden fixture
tests, `ExecutePipeline` / `ExecuteBatch`, interop CLI (`cmd/glyphastore-interop`), and CI coverage
via `scripts/test-go-client.sh` and the cross-SDK matrix.

### 11. Ruby client — **planned (isomorphic)**

Full native Ruby SDK roadmap: [ruby-sdk-roadmap.md](ruby-sdk-roadmap.md). Priorities:
**correctness → security posture → performance**. Sync client + interop first; AsyncClient and
optional C framing extension after measurement; TLS/auth on the shared server security train. No
FFI wrap of the C++ client as the default design.

## Product blockers (not “more languages”)

These gate “ready for real applications” more than additional SDKs:

| Area | Still open |
| --- | --- |
| Security | Authn/authz, TLS, rate limits, audit, credential handling |
| Observability | Structured logs, metrics, readiness, admin diagnostics, build info, connection/Worker stats |
| Operations | Backup/restore/verify, drain, corruption, disk-full, upgrade/downgrade runbooks |
| Release compatibility | ABI/API policy, wire compatibility, signed artifacts, checksums, SBOM, reproducible builds, deprecation/support lifetime |
| Stress / fault | Exhaustive socket/thread fault injection, continuous fuzz, soak, reconnect/shutdown stress, memory stability, power-loss/filesystem matrices |

Minimum useful ops metrics for a web app: `connections_active`, `requests_total`, `errors_total`,
`overloaded_total`, latency, `queue_depth`, `bytes_in`/`bytes_out`, `worker_utilization`,
`reconnects`, `indeterminate_mutations`.

## Recommended order

1. Shared wire golden vectors (C++ / Python / Perl). **(CI verify + vendored cmp)**
2. Cross-SDK interoperability tests. **(`scripts/test-sdk-interop.sh`, Workers 1/2/4)**
3. Perl monotonic clock + thread/fork contract docs. **(done)**
4. Normative errors, retry, and timeout specification. **([client semantics v1](../spec/client-semantics-v1.md), [ADR 0019](../adr/0019-client-error-retry-timeout.md))**
5. Multi-Worker `batch` API on every official client. **(done)**
6. Go client. **(`sdk/go`, interop + CI)**
7. Structured errors + per-call timeouts on every official client. **(done)**
8. Ruby client Phase 1 (sync, isomorphic). **([Ruby SDK roadmap](ruby-sdk-roadmap.md))**
9. Authentication and TLS (all SDKs in the same train, including Ruby Phase 3).
10. Metrics, health, and diagnostics.
11. Backup/restore and operational procedures.
12. Cross-release compatibility and artifact signing.

The leap is that **every official client is demonstrably equivalent, compatible across releases,
and usable safely**—including any new language admitted under an isomorphism roadmap.
