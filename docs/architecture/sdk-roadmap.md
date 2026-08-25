Status: roadmap
Applies to: native SDKs (C++, Python, Perl, Go, Erlang, Ruby) and shared wire contract
Owner: client maintainers
Last reviewed: 2026-07-28

# SDK and client roadmap

The repository contains six native clients. Their common contract is
[wire protocol v2](../spec/wire-protocol-v2.md) plus
[client semantics v1](../spec/client-semantics-v1.md): binary-safe keys, owner-bound connections,
monotonic deadlines, bounded ordered pipelines, controlled reconnect, structured errors and
`committed` / `rejected` / `indeterminate` mutation outcomes.

“Implemented” below describes source-tree behavior and tests. It does not mean that every package
has been published to its language registry or that the server is production-certified.

## Current matrix

| Client | Public surface | Concurrency model | TLS 1.3 | Worker routing |
| --- | --- | --- | --- | --- |
| C++ | sync CRUD, pipeline, batch | thread-safe connection per Worker | yes | FNV + keyed SipHash `INIT` extension |
| Python | sync + `asyncio`, pipeline, batch | mutexed sync / native async | yes | FNV + keyed SipHash `INIT` extension |
| Perl | sync CRUD, pipeline, batch, Worker vectors | one select loop across Workers; one client per process/thread | optional `IO::Socket::SSL` | FNV + keyed SipHash `INIT` extension |
| Go | sync CRUD, pipeline, batch | mutex per Worker; goroutine batch fan-out | yes | FNV + keyed SipHash `INIT` extension |
| Erlang | sync OTP API, pipeline, batch, Worker vectors | shareable coordinator + per-Worker `gen_server` | yes | FNV + keyed SipHash `INIT` extension |
| Ruby | sync + optional `async`, pipeline, batch | MRI-thread-safe sync / Fiber async | yes | FNV + keyed SipHash `INIT` extension |

All six use golden wire fixtures and participate in the default-routing interoperability harness.
Every official client decodes plain `GlyphaStore/2` (FNV) and the extended SipHash INIT identity.

## Highest-priority gaps

### 1. Keyed routing across the SDK train — done

ADR 0030 INIT decode + SipHash-2-4 routing landed in C++ / Python / Perl / Go / Erlang / Ruby.
Unit tests cover paper vectors, plain/extended INIT, and FNV vs SipHash ownership divergence.
Default interop remains FNV; a keyed daemon matrix (`--worker-hash-seed`) is still optional follow-up.

### 2. Secure-profile interoperability

First-slice smoke exists: `scripts/test-secure-profile-interop.sh` (CI `sdk-clients`) proves mTLS +
`--authz-map` + pinned `--worker-hash-seed` under `--secure-profile` for cpp/python/go. The broader
TLS matrix in `test-sdk-interop.sh` is not equivalent to a full secure-profile matrix. Remaining:
every SDK plus prefix scope, quotas and CRL. Documentation must not call the SDK security train
complete before that matrix passes.

### 3. Released-artifact compatibility

Source-tree interoperability is implemented. Still required:

- install every built package artifact and rerun conformance from the installed copy (Python wheel
  and sdist plus the Perl tarball are now covered; the remaining SDK artifacts are still open);
- test supported old-client/new-server combinations under the 0.x compatibility policy;
- publish checksums, provenance and SBOMs with tagged artifacts;
- record registry publication state rather than saying “install from registry” unconditionally.

### 4. Comparable performance evidence

`scripts/benchmark_sdk_clients.sh` is the comparison harness. A valid comparison fixes server
commit, routing mode, Worker/client counts, pipeline depth, value size, operation mix, validation,
TLS/durability mode, affinity, warmup and sample count. Report throughput together with p50/p95/p99
where the harness exposes latency; do not infer a language limit from one pipeline depth.

For Perl specifically, the next work is profile-led. Client routing now reuses the normalized INIT
identity, and the benchmark can generate both default-FNV and keyed-SipHash workloads. Continue to
measure scalar/hash allocation, frame copies, buffer compaction, parser cost, `IO::Select`, syscalls
and cross-Worker overlap before choosing an implementation technique. Reduce pure-Perl
allocation/copy costs first; evaluate a narrow XS
codec/routing kernel only if profiles show that boundary dominates. XS is not assumed to be the only
large lever. See the [Perl README](../../sdk/perl/README.md).

Configurable connections per Worker remains measurement-gated for every SDK. It may improve
same-Worker concurrency but changes ordering, memory, reconnect and backpressure behavior.

### 5. Mutation idempotency (protocol candidate)

Clients classify uncertain mutations correctly, but applications cannot automatically retry an
`indeterminate` result. A future `client_id + operation_id` mechanism would require a protocol ADR,
bounded server retention and recovery semantics. It is not part of wire v2 today.

## Implemented shared foundation

- Golden request/response fixtures are verified across the official SDKs.
- `scripts/test-sdk-interop.sh` covers default FNV routing, Workers 1/2/4/8, plus keyed SipHash
  cleartext for workers 2/4 when `INTEROP_KEYED=1` (default); binary keys, empty values, TTL,
  pipelines, structured `NOT_FOUND`, oversized local rejection and cross-client PUT→GET; TLS
  (FNV) is covered when dependencies are available.
- `scripts/test-secure-profile-interop.sh` covers mTLS + authz + prefix + CRL + principal quotas +
  keyed seed (cpp/python/go; perl/ruby/erlang when available) and is wired into CI `sdk-clients`.
- Supply-chain CI packages SDKs, writes `SHA256SUMS`, requires syft SPDX JSON, Cosign-signs tags,
  and cross-checks archive digests across Linux builders (`.github/workflows/supply-chain.yml`).
- Pipeline APIs operate on one Worker and never auto-retry. Batch APIs group by Worker, overlap
  independent connections where supported and restore caller order; they are not transactions.
- Per-call deadlines reuse one absolute monotonic deadline across any permitted retry.
- Uncertain mutations are not mislabeled as rejected.
- Each SDK documents its actual thread/process/Fiber/OTP ownership model.
- Packaging verification scripts exist for every source package; registry publication is a separate
  release action.

## Admission rule for another language

A new SDK is not official until it has an isomorphism plan and passes:

1. wire fixtures, including both routing identities;
2. client-semantics error, timeout, retry and indeterminate-outcome tests;
3. default and keyed-routing interop;
4. cleartext, TLS and complete secure-profile interop;
5. pipeline/batch ordering and concurrency-contract tests;
6. install-from-artifact packaging verification;
7. the shared benchmark workload with results labeled by runtime and execution model.

Related documents: [C++ client reference](../reference/cpp-client-api.md),
[SDK packaging standard](sdk-packaging.md), [where performance matters](where-performance-matters.md),
[production readiness](../production-readiness.md), and
[Ruby implementation roadmap](ruby-sdk-roadmap.md).
