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
- TCP reports only additive leaf spans: decode, route, store operation, response
  encode, transport write call, and poller update. In particular, `socket_write`
  does not include pipeline resumption or request dispatch.

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
| TCP | `socket_write` | ~76% | `send` / `sendmsg` / TLS write call only |
| TCP | `store_op` | ~11% | Local GET or mutation submission; excludes response encode |
| TCP | `poller_update` | ~10% | kqueue/epoll interest modification |
| TCP | decode / route / encode | ~3% | Additive remainder of instrumented leaf spans |

The TCP shares above are from a same-machine Apple Silicon lab run with phase
instrumentation enabled: volatile read-after-write, one Worker, one client,
pipeline 128, 200k PUT+GET pairs, two warmups and seven measured repeats. They
are diagnostic attribution, not release evidence or a production claim.

### Redundant poller-transition removal (2026-08-25)

Async store submission used to modify poller interest inside `execute_local`.
Both callers then reconciled that interest before any intervening poller wait:
`read_ready` immediately flushed queued output, while `write_ready` performed
its own final transition. Interest ownership now remains with those callers.
No queue, acknowledgement point, request ordering, output watermark, or wire
outcome changed.

In the instrumented pipeline-128 workload, poller-update samples fell from
about 3.59M to 1.79M across warmups plus measured runs. A non-instrumented
Release A/B, alternated to reduce order bias, measured:

| Run | Baseline | Candidate | Difference |
| --- | ---: | ---: | ---: |
| A | 228,212 ops/s | 241,696 ops/s | +5.9% |
| B (reverse order) | 229,004 ops/s | 243,526 ops/s | +6.3% |

Pipeline 1 remained effectively flat in a shorter check (47,011 vs 47,278
ops/s, +0.6%). These local figures are advisory and retain the same-machine,
non-isolated benchmark limitations.

A second same-state transition remained after a synchronous output flush. When
`write_ready` is entered from `read_ready`, drains the queued response, and has
never armed writable interest, an in-flight request leaves the socket already
registered for reads. Skipping that redundant `read → read` modification is
behavior-neutral; half-closed and write-armed sockets still take the complete
reconciliation path.

With phase instrumentation, this reduced poller-update samples from about 1.79M
to zero in the same pipeline-128 workload. A Release A/B against `fe32cfc`,
alternated to reduce order bias, measured:

| Run | `fe32cfc` | Candidate | Difference |
| --- | ---: | ---: | ---: |
| A | 242,112 ops/s | 268,755 ops/s | +11.0% |
| B (reverse order) | 243,485 ops/s | 272,709 ops/s | +12.0% |

Pipeline 1 remained flat in a shorter check (47,606 vs 47,755 ops/s, +0.3%).
The figures remain advisory same-machine evidence, not a release claim.

### Completion resume with Writer/socket overlap (2026-08-25)

The mutation completion path now resumes already-buffered frames before draining
its contiguous decided output. `process_frames` still admits at most one new
asynchronous operation on the connection. Consequently the next Writer request
can overlap the socket write of prior ordered responses without a response
reorder buffer, queue widening, or early acknowledgement. A trailing decode or
admission failure preserves prior decided responses through `close_after_flush`.
Resume is suppressed when output was already pending at completion, retaining
the historical slow-client backpressure boundary.

In the instrumented pipeline-128 workload, socket-write samples fell from about
3.60M to 1.80M while poller-update samples remained zero. A Release A/B against
`dbf246d`, alternated to reduce order bias, measured under the same host state:

| Run | `dbf246d` | Candidate | Difference |
| --- | ---: | ---: | ---: |
| A | 197,994 ops/s | 306,616 ops/s | +54.9% |
| B (reverse order) | 200,705 ops/s | 309,928 ops/s | +54.4% |

An earlier candidate reached 419,101 ops/s under a less contended host state;
after adding the explicit pending-output backpressure guard, a fresh seven-sample
Release run measured 423,376 ops/s median. Neither absolute value is used for the
A/B ratio. Pipeline 1 showed no regression in the sampled check. All figures
remain advisory same-machine evidence.

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
4. TCP: additive leaf phase scopes; clock comment / single sample per
   `process_frames` turn; encode, socket-write, and poller-update scopes. p128
   improvement attributed to reduced GET↔Writer wake
   storms under pipelined load plus encode/write path hygiene — **not** queue widening.
5. TCP: removed redundant async-submission poller modifications; the read/write
   caller remains the sole owner of the next interest transition.
6. TCP: skip the remaining same-state read-interest update after a synchronous
   flush that never armed writable interest; half-close and writable states
   still reconcile through the poller.
7. TCP: resume bounded buffered work after mutation completion before draining
   contiguous output, preserving one-in-flight response order while overlapping
   the next Writer operation with the socket write.

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
  path is removed via `PublishedAdmission::caller_holds_guard` (maintenance
  emergency gate still checked).
- `Store::put_batch` + Writer sync coalesce (≤32 / publish) amortizes publication
  when the caller stages multiple same-shard mutations in one call. Lab median
  ~527 k ops/s (batch 32) vs ~372 k single put on Apple M4 — honest gain without
  changing single-op semantics.
- ADR 0035 (rejected): TLS shell freelist under existing publish protocol — mild
  1t PUT gain, **−16% affine PUT 2t** on A/B; not shipped. Residual remains
  Delta COW spine + Writer apply, not shell malloc alone.
- `make_shared` co-allocation for generation shells, then embed `DeltaState` in
  the same allocation: lab `store_put` ~370–378 k (vs ~344 k plain same day).
  Affine stays in prior interleaved noise; still far from 600 k single-op target.
- Writer sync volatile path attributes `worker_apply` vs `publish` when
  `GLYPHASTORE_HOT_PATH_PHASES` is on. Lab: apply ~381 ns, publish ~640 ns,
  caller `ack` ~3.0 µs (wake/schedule dominates the residual). Rejected:
  post-sync Writer busy-spin (−50% `store_put`), sync-before-merge reorder
  (no reliable 1t win), and conditional `writer_waiting` notify (A/B within
  noise; sync PUT leaves the Writer parked so notify still fires). See
  `benchmarks/results/local-macos-2026-08-02-sync-first/` and
  `.../local-macos-2026-08-02-writer-waiting/`.
- DeltaArena keeps segment pins in a `deque<SegmentPtr>` (stable raw addresses,
  no per-pin `unique_ptr` shell). Moving `SegmentPtr` out of `ReadMutation` into
  the arena was considered and deferred: `publish_incremental` may be retried
  after `resource_exhausted`, and a consuming move would break pin validation on
  retry unless publications were rebuilt.
- Apply sub-phases wired: `encode_copy` (Segment append) and `index_publish`
  (`Worker::publish`). Lab means: encode ~66 ns, index ~158 ns, outer
  `worker_apply` ~514 ns, `publish` ~603 ns, `ack` ~2.7 µs (~64%). Removed
  redundant second `encoded_record_size` inside encode when the caller already
  sized the buffer (`encode_record(out, input, size)`). Encode is not the
  binding limit.
- Thread-local Delta COW freelist was measured and rejected (allocator/custom-deleter
  overhead regressed uniform parallel PUT on Apple Silicon).
- Uniform multi-thread single-op PUT still below 1t PUT: combine `put_batch` with
  caller-side group-by-owner (or a future shard-bound session).
- Full generation slot-pool publish protocol is **ADR 0036 (proposed)**: design bar +
  V1–V14 matrix in `docs/adr/0036-generation-slot-pool-publish.md`. Production stays
  on `shared_ptr` + ReadLease until that ADR is accepted and gates close. Prototype
  evidence now covers V1/V2/V3/V6/V7/V9/V13 (bounded slot acquire → `resource_exhausted`;
  rejected publication never visible; `macos-tsan` and `macos-asan` ASan+UBSan green on
  ADR gates + `paired Store`). Production-baseline notes for V3/V4/V6/V8/V10/V14 under
  `benchmarks/results/local-macos-2026-08-02-adr0036-v6-v8-v14/`,
  `…/local-macos-2026-08-02-adr0036-v6-failclosed/`, and
  `…/local-macos-2026-08-02-adr0036-v14-sync-matrix/` (91 sync crash checkpoints).
  V6 production seam also covers async/sync durable sibling drain-snapshot after
  later-item failure (incl. `allow_fail_closed` when durable already self-closed),
  rejects success-ACK for abandoned unflushed writer-batch siblings
  (`durable_through` gate on commit-fail and unhealthy no-op; flushed siblings keep
  ACK + snapshot), sticky-fails + stops Writer batches on deferred-TTL drain
  indeterminate, preserves staged async sibling success ACKs across Writer catch,
  and keeps immutable published-generation `Store::get` RAW after sticky fail-closed
  (mutable Index GET / mutations still rejected). Sync `durable_sync` single-op
  also drain-snapshots before sticky close on post-commit publication failure.
  Async durable ACK-after-drain clears capture-fail errors on clean commits once the
  snapshot is published (no GET-visible + error-ACK). Happy-path incremental publish
  failure also drains before sticky close. Sync single-op ACK-after-visibility for
  committed+error after drain and preserves success across catch after publish.
  Sync batch + async durable extend the same visibility upgrade for sticky
  Index-visible `committed+error`; catalog advances `durable_through` before
  secondary Index accounting (`Site::index_account`).   Sync durable batch never
  success-ACKs unprocessed items after catch-path drain (sticky Index-committed
  only — never presence-upgrade of attempted puts on pre-existing keys). Index/hot flush publication catches allocation throws
  into fail-closed (clears pending). Volatile sync preserves
  success ACK across catch after publish (put+erase); async Writer ACK-after-publish for
  staged indices and preserves completions across post-publish catch (put+erase).
  Sync durable catch keeps resolved error polarity (no success promotion).
  `writer_durable_through` locks the Worker mutex for finalize gating.
  Evidence under paired durable litmus + allocation-fault suite.
  Reference remains `src/experimental/paired_shard.cpp` (lab only).
