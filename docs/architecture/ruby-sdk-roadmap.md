Status: roadmap
Applies to: planned official Ruby SDK (`sdk/ruby`) isomorphic to C++ / Python / Perl / Go
Owner: client maintainers
Last reviewed: 2026-07-20

# Ruby SDK roadmap

Build a **complete, native** Ruby client for GlyphaStore wire protocol v2 that is
**isomorphic in every observable aspect** to the existing official SDKs. Language sugar may be
idiomatic Ruby; wire bytes, mutation/pipeline outcomes, retries, deadlines, structured errors, and
interop matrices must not diverge.

**Priority order (non-negotiable):** correctness → security posture → performance.

Related: [SDK roadmap](sdk-roadmap.md), [client semantics v1](../spec/client-semantics-v1.md),
[ADR 0019](../adr/0019-client-error-retry-timeout.md),
[C++ client API / cross-language contract](../reference/cpp-client-api.md),
[wire protocol v2](../spec/wire-protocol-v2.md),
[where performance matters](where-performance-matters.md),
[production readiness](../production-readiness.md).

## 1. Design principles

| Principle | Rule |
| --- | --- |
| Native, not FFI | Implement the 40-byte wire codec in Ruby (optionally a tiny C extension later for framing only). Do **not** wrap the C++ client by default. |
| Isomorphic semantics | Same categories, outcomes, retry counts, deadline rules, unhealthy rules as [client semantics v1](../spec/client-semantics-v1.md). |
| Fail closed | Malformed frames, owner/epoch mismatch, non-empty mutation `OK` → protocol/unhealthy/`indeterminate` as specified; never “best effort” success. |
| Binary-safe | Keys and values are opaque byte strings (`String` with `Encoding::BINARY` / `ASCII-8BIT`). No implicit UTF-8, no string encoding conversions on the hot path. |
| Secure by posture | Until server TLS/auth exist, document private-network/sidecar-only. When TLS lands, Ruby gets it in the **same release train** as other SDKs—no silent cleartext fallback. |
| Measure before cleverness | Ship a correct pure-Ruby hot path first; C extension / Fiber async / `connections_per_worker` only after benchmarks prove the bottleneck. |

## 2. Isomorphism checklist (definition of “complete”)

The Ruby SDK is complete for alpha when **all** of the following match peers:

### 2.1 Wire and routing

- [ ] Protocol v2 request/response encode/decode (little-endian, reserved fields, frame limits)
- [ ] Canonical FNV-1a 64-bit over the **complete** binary key; `worker_for(key, worker_count)`
- [ ] Vendored copies of `wire_requests_v2.hex` / `wire_responses_v2.hex` + bit-identical golden tests
- [ ] Malformed / truncated / non-canonical frame rejection vectors (same as Python/Go)

### 2.2 Session and transport

- [ ] `connect`: TCP per Worker, `TCP_NODELAY`, `INIT` then `BIND_WORKER` for every Worker
- [ ] One connection per Worker (v1 shape); mutex (or equivalent) serializes that connection
- [ ] Lazy reconnect of a single Worker after transient failure; accept only original
      `worker_count` + `routing_epoch`
- [ ] `close` idempotent; concurrent close vs in-flight call defined like C++/Go/Python
- [ ] Connect timeout vs request timeout separation ([§1](../spec/client-semantics-v1.md), §6.4)

### 2.3 API surface (functional parity)

| Capability | Ruby surface (proposed) | Peers |
| --- | --- | --- |
| Config | `GlyphaStore::ClientConfig` | C++/Python/Go/Perl config |
| Connect | `GlyphaStore::Client.connect(...)` | all |
| CRUD | `get` / `put` / `erase` / `ping` | all |
| Pipelines | `execute_pipeline` (single Worker) | all |
| Multi-Worker | `execute_batch` (group, overlap, restore order) | all |
| Explicit multi-Worker waves | optional `execute_worker_pipelines` (Perl parity) | Perl |
| Per-call timeout | `timeout:` kwarg / options object | §6.5 |
| Mutation result | `committed?` / `rejected?` / `indeterminate?` + error | all |
| Pipeline outcomes | `succeeded` / `failed` / `indeterminate` | all |
| Structured errors | category, message, wire_status, bytes_sent, request_id, worker, routing_epoch, retryability, operation, mutation_outcome | §2.1 |
| Routing helpers | `worker_count`, `routing_epoch`, `worker_for`, `healthy?` | all |
| Async | `GlyphaStore::AsyncClient` (Fiber/`async`) — **phase 2**, isomorphic to Python `AsyncClient` | Python |

Idiomatic naming (`CamelCase` classes, `snake_case` methods, `?` predicates) is encouraged.
Silent semantic drift is not.

### 2.4 Client semantics v1

- [ ] Portable categories (§2)
- [ ] Wire status → category / mutation outcome (§3)
- [ ] Retryability classes (§4)
- [ ] At-most-one automatic retry rules (§5) — **exact**, including no pipeline/batch auto-retry
- [ ] Monotonic request deadlines; connection reset on expiry; late frame never satisfies next call (§6)
- [ ] Per-call timeout override (§6.5); batch shares one absolute deadline
- [ ] Async cancellation poisons the Worker connection (§6.3) when AsyncClient ships
- [ ] Unhealthy client rules (§8)

### 2.5 Interop, packaging, ops evidence

- [ ] `scripts/test-ruby-client.sh` (unit + fake-server integration)
- [ ] Interop CLI + row in `scripts/test-sdk-interop.sh` (C++/Python/Perl/Go/**Ruby**, Workers 1/2/4)
- [ ] CI job (or extension of `sdk-clients`) on supported Rubies
- [ ] Gem packaging (`glyphastore` or `glypha_store` — name frozen before first publish)
- [ ] README: fork/thread contract, private-network warning, performance guidance
- [ ] `scripts/benchmark_ruby_client.sh` + published results folder (same matrix as Go/Perl)

## 3. Phased delivery

### Phase 0 — Spec lock (no code yet, or stubs only)

1. Add Ruby to the cross-language table in [cpp-client-api.md](../reference/cpp-client-api.md).
2. Freeze gem name, module namespace (`GlyphaStore`), minimum Ruby version (**3.2+** recommended:
   Fiber scheduler, better Ractor/Timeout primitives; drop EOL Rubies).
3. Decide sync-first (like Go/Perl) vs sync+async day-one (like Python). **Recommendation: sync
   complete in Phase 1; AsyncClient in Phase 2** so correctness gates are not diluted.
4. Threat/posture note in README skeleton: cleartext TCP, no auth — loopback / private / sidecar only.

**Exit:** maintainers agree API sketch + gem name; this roadmap linked from [sdk-roadmap.md](sdk-roadmap.md).

### Phase 1 — Correctness MVP (sync client) — **done for alpha**

Order is intentional: codec → errors → session → CRUD → pipeline → batch → interop.

| Step | Deliverable | Status |
| ---: | --- | --- |
| 1.1 | `sdk/ruby` gem layout, LICENSE BSD-3-Clause, CI | done |
| 1.2 | `GlyphaStore::Protocol` + golden fixtures | done |
| 1.3 | `GlyphaStore::Error` + category/retryability helpers | done |
| 1.4 | `Client.connect` / bootstrap / bind / health | done |
| 1.5 | `get`/`put`/`erase`/`ping` + mutation outcomes | done |
| 1.6 | Monotonic deadlines + per-call `timeout:` | done |
| 1.7 | Automatic retry §5 only | done (aligned with Go; expand fault matrix in 0.2) |
| 1.8 | `execute_pipeline` positional outcomes | done |
| 1.9 | `execute_batch` | done |
| 1.10 | Thread / fork contract documented | done (README) |
| 1.11 | Interop CLI + matrix | done (Workers 1/2/4) |
| 1.12 | Fake-server fault tests | done (core cases); broaden in 0.2 |

**Exit (Phase 1):** green `test-ruby-client.sh`, green interop row including Ruby, semantics coverage
for CRUD / pipeline / batch / structured errors / deadlines.

### Phase 2 — Performance hardening + async

| Step | Deliverable | Notes |
| ---: | --- | --- |
| 2.1 | Hot-path encode into pre-sized `String` buffers; reuse read buffer | Mirror Go/Python techniques |
| 2.2 | `execute_batch` overlap without excess allocations | Prefer one wait set / thread pool policy documented in README |
| 2.3 | Published benchmark suite (`benchmark_ruby_client.sh`) | Same OPS/pipeline/Workers matrix; sequential + concurrent |
| 2.4 | Analysis vs Python/Go/Perl on same host | Document MRI ceiling; prefer deep pipelines |
| 2.5 | `AsyncClient` (Fiber scheduler / `async` gem as needed) | §6.3 cancel → poison connection; isomorphic ops |
| 2.6 | Optional C extension **only** for FNV + frame pack/unpack | Gate: measured ≥15–20% on deep pipeline median; keep pure-Ruby fallback |
| 2.7 | Puma/Unicorn/Falcon guidance | One client per worker process; no sharing across `fork` |

**Exit:** published bench folder; AsyncClient passes cancel/poison tests; optional C ext behind
feature flag or separate gem variant if packaging requires it.

### Phase 3 — Security alignment (tracks server)

Ruby must not lag other SDKs when product security lands.

| Step | Deliverable | Notes |
| ---: | --- | --- |
| 3.1 | TLS client options (`ssl_context` / CA bundle / hostname verify) | Fail closed; no “TLS optional default on” surprise in secure profiles |
| 3.2 | Authn hooks (whatever wire/session auth the server grows) | Same credential model as Python/Go |
| 3.3 | No secrets in exceptions/logs by default | Keys/values redacted unless explicit debug |
| 3.4 | Gem provenance | Checksums/SBOM when release policy requires; pinned CI Rubies |
| 3.5 | Hostile-input fuzz on codec | Feed malformed lengths/status; assert no crash / no desync |

Until 3.1–3.2 exist on the server, Ruby README keeps the same **private network** banner as Go/Python.

### Phase 4 — Parity with shared SDK roadmap leftovers

| Item | Ruby action |
| --- | --- |
| `connections_per_worker` | Same config knob, only after suite 0.2 p50/p95 shows connection saturation |
| Mutation idempotency key | Wire field when server supports it; Ruby day-one of that protocol bump |
| Metrics hooks | Optional callbacks/counters consistent with other SDKs when ops metrics exist |
| Cross-release compat | Interop against N−1 gem + daemon |

## 4. Correctness priorities (detail)

1. **Golden wire first** — treat fixture mismatch as release blocker.
2. **Outcome tables** — every disconnect/partial-send/`INTERNAL_ERROR`/non-empty mutation OK case
   has an automated test; copy matrices from Python/Go tests, do not re-invent.
3. **Retry budget** — property-style or counter tests: at most one automatic retry; pipelines never.
4. **Deadline isolation** — after timeout, a late server frame must not complete a later `get`.
5. **Health fail-closed** — wrong owner / epoch change → `unavailable` thereafter.
6. **Fork safety** — after `fork`, sockets are undefined; document mandatory new `connect` in child
   (Puma clustered, Unicorn).
7. **Encoding** — force `BINARY` on all key/value boundaries; reject or ignore external encoding flags
   so UTF-8 tags cannot corrupt length prefixes.

## 5. Security priorities (detail)

Client-side security before server TLS is mostly **hygiene and honesty**:

| Area | Requirement |
| --- | --- |
| Deployment honesty | README + connect-time warning path: cleartext, no auth |
| Limits | Enforce `maximum_frame_bytes` / pipeline caps **before** send (DoS amplification / memory) |
| Parser hardness | No integer wrap on frame sizes; cap allocations to declared limits |
| Timeouts | Non-zero defaults; reject non-positive per-call overrides (§6.5) |
| Dependency surface | Minimal runtime deps for sync client (stdlib sockets); AsyncClient may add one pinned async stack |
| Supply chain | Prefer vendoring fixtures; CI on locked Ruby; no `eval` of server data |
| Future TLS | Hostname verification on; custom verify callbacks documented as escape hatches, not defaults |

Ruby-specific: do not put password/token material into `inspect` / `Exception#message` for auth
objects when those arrive.

## 6. Performance priorities (detail)

Expected shape (hypothesis until measured):

- MRI single-process ceiling between Perl and Python for pure Ruby, depending on pack/`String`
  discipline.
- Deep pipelines + `execute_batch` are the primary levers (same as every SDK).
- Process parallelism (Puma workers) is the production scale-out knob under the GIL.
- Threads help **overlap Workers** inside one process (like Go goroutines / Python threads), not
  CPU-bound framing throughput under MRI.

| Priority | Action |
| ---: | --- |
| 1 | Correct pack/unpack hot path; pre-size buffers; one `write` per exchange/pipeline |
| 2 | Overlap multi-Worker I/O in `execute_batch` |
| 3 | Publish benches; compare fairly (wire-ops vs pairs—match current harness units) |
| 4 | AsyncClient for Falcon/async apps (app throughput, not microbench miracles) |
| 5 | Optional C ext for FNV/framing if Phase 2.3 shows clear win |
| 6 | `connections_per_worker` only after latency evidence |

**Non-goals for performance:** FFI to `libglyphastore` C++ client; rewriting the daemon in Ruby;
claiming Redis-class ops/s from the MRI SDK path.

## 7. Proposed layout

```text
sdk/ruby/
  glyphastore.gemspec          # name TBD in Phase 0
  README.md
  CHANGELOG.md
  LICENSE
  Rakefile                     # test + rubocop
  lib/glypha_store.rb          # or glyphastore.rb — frozen with gem name
  lib/glypha_store/version.rb
  lib/glypha_store/protocol.rb
  lib/glypha_store/error.rb
  lib/glypha_store/client.rb
  lib/glypha_store/async_client.rb   # Phase 2
  test/                        # Minitest or RSpec — prefer Minitest for lean deps
  test/fixtures/wire_*.hex
  benchmarks/client_benchmark.rb
  exe/glyphastore-interop      # or cmd-style under exe/
  ext/glyphastore/             # optional Phase 2.6
```

Scripts (repo root, mirror peers):

- `scripts/test-ruby-client.sh`
- `scripts/benchmark_ruby_client.sh`
- `scripts/package-ruby-client.sh`
- extend `scripts/test-sdk-interop.sh` and `scripts/benchmark_sdk_clients.sh`

## 8. Concurrency and deployment contract

| Environment | Rule |
| --- | --- |
| MRI threads | One `Client` may be shared if each Worker connection is mutex-protected (Go/C++/Python sync model). |
| `fork` (Puma clustered, Unicorn) | **New client in the child** after fork; never reuse parent sockets. |
| Ractors | Out of scope for v0.1 unless a clean message-passing design appears; do not claim Ractor-safe. |
| Async / Fiber | Only via `AsyncClient`; do not mix blocking sync `Client` calls inside a scheduler without offload. |
| JRuby / TruffleRuby | Best-effort later; Phase 1 CI is MRI. Document if byte/`pack` assumptions need adjustment. |

## 9. Definition of done (0.1.0)

Ruby `0.1.0` may be published when:

1. Phase 1 exit criteria are green in CI.
2. Interop matrix includes Ruby alongside C++/Python/Perl/Go for Workers 1/2/4.
3. README states private-network posture, fork/thread contract, and pipeline guidance.
4. Client semantics conformance §9 is explicitly checked (fixtures, outcomes, monotonic deadlines,
   retry limits).
5. A baseline benchmark folder exists (even if numbers trail Go)—methodology matches peers.
6. No known silent divergence from [client semantics v1](../spec/client-semantics-v1.md).

AsyncClient, C extension, and TLS may follow in `0.2.x` without blocking `0.1.0` **only if** documented
as such; they remain on this roadmap until checked off.

## 10. Recommended build order (summary)

1. Phase 0 freeze (name, Ruby ≥ 3.2, sync-first)
2. Protocol + goldens
3. Errors + outcomes
4. Sync client CRUD + deadlines + retries
5. Pipeline + batch
6. Interop + CI
7. Benchmarks + hot-path pass
8. AsyncClient
9. Optional C ext (data-gated)
10. TLS/auth with the server security train

## 11. Explicit non-goals

- Wrapping the C++ client via FFI/Rice as the primary artifact
- Redis/RESP compatibility
- Guaranteeing C++/Go microbench parity on MRI
- Shipping Ruby before Phase 1 correctness gates
- Inventing Ruby-only outcome names or extra automatic retries “for convenience”
