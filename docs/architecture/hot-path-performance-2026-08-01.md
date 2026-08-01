Status: lab evidence (architectural prototype)
Applies to: embedded Store GET/PUT and TCP daemon hot paths on macOS Apple Silicon
Owner: performance / storage maintainers
Last reviewed: 2026-08-01

# Hot-path performance program (2026-08-01)

Claim ceiling: numbers below are **same-machine lab evidence** on Apple M4 /
`macos-release`. They are not production capacity claims, E3 durability closure,
or hostile-public deployment readiness.

Baseline reference: [`benchmarks/results/local-macos-2026-08-01/`](../../benchmarks/results/local-macos-2026-08-01/)
(`629bc68`). Candidate measurements:
[`benchmarks/results/local-macos-2026-08-01-perf/`](../../benchmarks/results/local-macos-2026-08-01-perf/).

## Goal

Reduce the gap between internal Index (~11–16 M ops/s) and observable Store API /
TCP daemon performance without weakening correctness, durability, recovery,
backpressure, fail-closed behaviour, RAW semantics, or memory bounds.

## Phase instrumentation

Optional compile-time counters (`-DGLYPHASTORE_HOT_PATH_PHASES=ON`):

- Header: `include/glyphastore/core/hot_path_phases.hpp`
- Preallocated `alignas(64)` atomic buckets; no global mutex; no hot-path logging
- Dump via `GLYPHASTORE_HOT_PATH_PHASE_REPORT=1` at process exit
- Default builds keep macros as no-ops

### Cost map (lab, phases ON; relative %)

Measured on `store-put-get` (1 worker, 200k ops, 5 repeats) with instrumentation
enabled (absolute ns include timer overhead; use **percentages** for attribution):

| Path | Phase | Share | Notes |
| --- | --- | ---: | --- |
| GET | `index_lookup` (+ decode) | ~67% | Delta/base Swiss probe + pin decode |
| GET | `hash` / `route` / `admit` / `lease_adopt` | ~8% each | Admission + ReadLease fence |
| GET | `value_copy` | small after SSO | 64 B inline path |
| PUT | `ack` | ~93% | Caller wait for Writer apply+publish |
| PUT | `enqueue` | ~4% | Sync list push + wake |
| PUT | `admit` | ~3% | Submission admission |
| TCP | decode / dispatch / store_op / encode / write | instrumented | See phase dump after server benches |

## Hot-path diagram (embedded paired)

```text
GET (embedded Store)
  hash(key) → route(shard) → OperationGuard(admit)
    → ReadLease (count + seq_cst fence + load published generation)
      → PairReadGeneration::get
           delta.find → (miss) base.get
           decode pinned record → OwnedValue (SSO ≤64 B)
    → lease drop (no Writer wake)

PUT (embedded Store)
  hash → route → OperationGuard → maintenance check
    → ShardPairRuntime::mutate
         enqueue SyncMutation → wake Writer
         adaptive spin → park on done
    Writer: apply Store mutation → publish_incremental → proportional reclaim
         → notify done (ACK). No early ACK before publication.
```

```text
TCP daemon (owner-bound)
  Reactor turn: clock once → decode → authz/abuse → dispatch
    → local generation GET / async mutation lane
    → encode response (preallocated connection output) → write
  Connections are BIND_WORKER / owner-bound; SDKs open one connection per Worker.
```

## Concurrency invariants (unchanged)

1. **RAW**: a completed mutation happens-before any later read that observes it;
   ACK is only after Writer publication.
2. **No reclaim on GET**: ReadLease no longer wakes the Writer; reclaim is
   Writer-opportunistic / proportional to retired-generation debt.
3. **Admission closed bit**: OperationGuard / submission counters remain
   fail-closed for `close()`.
4. **Bounded memory**: mutation slots, queues, retired-generation cap, and
   connection watermarks are unchanged; no queue widening.
5. **Hash/routing**: single `PrehashedKey` / `HashedKey` computation per public op;
   span key overloads converge on `KeyView`.

## Absolute + ratio table (lab medians)

| Cell | Baseline (`629bc68`) | After (this program) | Ratio |
| --- | ---: | ---: | ---: |
| Index find hit (ref) | 11.59 M ops/s | (unchanged suite) | — |
| `store_get_copy` 64 B 1t | 2.20 M (455 ns) | **3.50 M (286 ns)** (15 repeats) | **1.59×** |
| worker-affine GET 2t | 5.07 M | **8.46 M** | **1.67×** |
| `store_put` 64 B 1t | 378 k | ~369 k | ~0.98× |
| worker-affine PUT 2t | 506 k | ~504 k | ~1.00× |
| uniform PUT 2t | 140 k | ~241 k | **1.72×** |
| TCP w1 p1 | 47.6 k | 46.5 k | ~0.98× |
| TCP w4 p32 | 282 k | **302 k** | **1.07×** |
| TCP w4 p128 | 206 k | **312 k** | **1.51×** |
| TCP w4 p32 / w1 p1 | 5.91× | **6.50×** | target ≥2.5× |

Quantitative targets:

| Target | Result |
| --- | --- |
| get_copy 64 B 1t stably < 300 ns (≥3.3 M) | **Met** (median 286 ns / 3.50 M over 15 repeats) |
| worker-affine GET 2t ≥ 6 M | **Met** (~8.5 M) |
| volatile PUT 1t ≥ 600 k | **Not met** (~369 k); residual is Writer ack/publish |
| worker-affine PUT 2t ≥ 900 k | **Not met** (~504 k); same residual |
| uniform PUT 2t ≥ single-thread PUT | **Not met** (~241 k vs ~369 k); cross-shard sync still serializes per owner |
| TCP p32 4-pair ≥ 2.5× 1-pair | **Met** (~6.5×) |
| p128 not regress >5% vs p32 | **Met** (w4 p128 ≥ p32 in this run) |

## Changes landed

1. Disableable hot-path phase counters + exit report.
2. GET: span→`KeyView` convergence; `PrehashedKey` alias; ReadLease **no Writer wake**
   on last lease drop; `OwnedBytes` SSO ≤64 B; Lane false-sharing separation for
   lease vs sync atomics.
3. PUT: adaptive spin before park (bounded); Writer short spin before park;
   proportional reclaim (skip empty / non-quiescent; quantum reclaim).
4. TCP: phase scopes; clock comment / single sample per `process_frames` turn;
   encode/write scopes. p128 improvement attributed to reduced GET↔Writer wake
   storms under pipelined load plus encode/write path hygiene — **not** queue widening.

## Rejected optimizations

| Idea | Reason rejected |
| --- | --- |
| Widen mutation / TCP queues | Violates bounded-memory / backpressure non-negotiable |
| Remove length/overflow checks | Safety ban |
| Early ACK before publication | Breaks RAW |
| Drop ReadLease fence / counts without epoch protocol | Use-after-free hazard vs Writer reclaim |
| Replace production `shared_ptr` generation publish with prototype slot pool | Requires ADR + proofs; not silent |
| Disable OperationGuard on GET | Breaks close/admission linearization |
| Game benchmarks / loosen thresholds | Explicitly forbidden |
| Dual base/candidate worktrees | Out of scope for this execution |

## Memory honesty

Fixed costs dominate small-value RSS in these benches (Segment arenas, paired
Writer threads, generation pins). `store_get_copy` median RSS ~240 MiB with
~0 delta during the measured loop — capacity is not “bytes per key” for hot
64 B values. Segment size and per-shard Writer stacks are structural; see
`docs/architecture/storage-model.md` and paired ADR 0031/0032.

## Follow-ups (residuals)

- PUT ack still ~2.5 µs median for single `Store::put`: publish_incremental +
  generation ownership churn. Nested Writer `OperationGuard` on the embedded sync
  path is removed via `PublishedAdmission::caller_holds_guard`.
- `Store::put_batch` + Writer sync coalesce (≤32 / publish) amortizes publication
  when the caller stages multiple same-shard mutations in one call. Lab median
  ~527 k ops/s (batch 32) vs ~372 k single put on Apple M4 — honest gain without
  changing single-op semantics. Further single-op wins need generation slot pool ADR.
- Thread-local Delta COW freelist was measured and rejected (allocator/custom-deleter
  overhead regressed uniform parallel PUT on Apple Silicon).
- Uniform multi-thread single-op PUT still below 1t PUT: combine `put_batch` with
  caller-side group-by-owner (or a future shard-bound session).
- Consider ADR for generation slot pool (prototype) if measured publish cost remains dominant.
