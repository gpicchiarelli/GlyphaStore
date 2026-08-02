# ADR 0036: Generation slot-pool publish protocol — design bar before production

- Status: proposed
- Date: 2026-08-02
- Deciders: storage and performance maintainers
- Applies to: paired `PairReadGeneration` publication and reclaim (embedded Store and
  daemon Reader–Writer pairs); **not implemented in production**
- Amends: none yet (would amend [ADR 0031](paired-reader-writer-shards.md) §Protocollo di
  publication / §Protocollo di reclamation **only after** the verification matrix below is
  closed and this ADR is accepted)
- Supersedes: none
- Depends on: [ADR 0031](paired-reader-writer-shards.md), [ADR 0032](0032-paired-concurrency-embedded-store.md)
- Related: [ADR 0035](0035-generation-shell-recycling.md) (rejected shell freelist under the
  *existing* protocol; Alternative A deferred here)

## Context

Production publish keeps Writer-owned `shared_ptr<const PairReadGeneration>` (today co-allocated
with embedded `DeltaState` via `make_shared`), exposes a raw pointer to Readers, and reclaims via
ReadLease / `reader_safe_epoch` QSBR ([ADR 0031](paired-reader-writer-shards.md)). Lab attribution
on Apple Silicon shows single-op PUT dominated by caller `ack` (~64%, wake/schedule) then
`publish_incremental` (~600 ns) and Writer apply (~500 ns). Encode is ~66 ns and is not the binding
limit (`benchmarks/results/local-macos-2026-08-02-encode-size/`).

ADR 0035 rejected recycling generation *shells* under the current protocol after a **−16%**
worker-affine PUT 2t regression. The remaining publish lever that removes `shared_ptr` control-block
traffic is a **fixed generation slot pool** with a different lifetime protocol. A lab prototype
lives in `src/experimental/paired_shard.cpp` (capacity ≈ mutation queue + 2, reader-turn QSBR,
per-slot `output_pins`). That prototype is **not** promotable: it is not the production delta/base
model, lacks durable refresh/merge integration, and has not closed ADR 0031’s proof matrix.

This ADR freezes the **design bar** and **acceptance gates** before any production integration.
No silent protocol swap; no partial land that weakens RAW, lease safety, or reclaim bounds.

## Decision drivers

- **RAW**: ACK only after the mutation is in the published generation (release-store).
- **No generation UAF**: concurrent GET / cold I/O / scatter output must not observe freed storage.
- **Atomic adoption**: Readers never see torn epoch vs generation (single release descriptor).
- **Bounded memory**: fixed pool capacity; exhaust → backpressure, never unbounded retire growth.
- **ADR 0031 compatibility**: incremental merge, durable catalog refresh, shutdown drain, fail-closed
  publication failure after Store mutation remain intact or are explicitly re-specified.
- **Measured non-regression**: especially worker-affine parallel PUT (ADR 0035 lesson).
- **No silent format / wire / routing change**.

## Alternatives considered

### A. Keep production `shared_ptr` + ReadLease reclaim (status quo)

- Advantage: proven under ADR 0031; embed-delta / `make_shared` already reduce one alloc per publish.
- Disadvantage: control-block and COW spine cost remain on every single-op publish.
- Remains the **production default** until this ADR is accepted *and* verification closes.

### B. TLS / freelist shell recycle under current protocol (ADR 0035)

- Rejected after measured affine PUT regression. Not reopened here.

### C. Full generation slot pool (this ADR’s subject)

Fixed slots; Writer publishes by filling a free slot and release-storing a descriptor; reclaim waits
for a proven quiescent boundary (reader turns and/or lease/epoch frontier) plus zero outstanding
pins that borrow generation storage.

- Advantage: removes per-publish `shared_ptr` generation control blocks; bounds live generations.
- Disadvantage: new reclaim protocol; must prove equivalence (or a strict strengthening) of ADR 0031
  lease/epoch rules for embedded Store, daemon GET, cold I/O, and future borrowed iovec GET.

### D. Hybrid: slot-pool shells, keep ReadLease counters

Map slots to the existing lease fence without reader-turn QSBR.

- Advantage: smaller delta from production Reader code.
- Disadvantage: still need a precise “slot free only when…” rule; easy to get wrong if leases and
  slots disagree. Allowed as an *implementation strategy* only if it satisfies the same proof matrix.

## Decision

**Proposed design bar (not yet production law):**

1. Production must not adopt the experimental slot-pool publish path until this ADR is **accepted**
   and the Verification matrix is marked closed with linked evidence.
2. Any production slot-pool must preserve:
   - single atomic publication of `{epoch, visible_through, generation graph}`;
   - ACK-after-publication RAW;
   - bounded pool + explicit backpressure when no free slot exists after reclaim;
   - fail-closed behavior when Store mutation succeeded but publication cannot complete;
   - shutdown drain that never frees a generation still observable by a Reader or pin.
3. Reclaim must define a **single** quiescence story that covers at least:
   - embedded `ReadLease` (or an explicitly documented replacement with the same happens-before);
   - daemon per-turn adoption;
   - async cold-read / `reader_safe_epoch` (or pin equivalent);
   - any output path that borrows generation bytes (today’s owning `OwnedValue` may release early;
     a future borrowed `get-into` must extend the frontier — same rule as ADR 0031).
4. Capacity: document the formula (e.g. related to max retired debt, mutation queue depth, and
   in-flight pins). Exhaustion is backpressure or fail-closed admission — never silent overwrite.
5. Incremental merge and durable snapshot replace remain Writer-private; slot reuse must not hand a
   Reader a generation whose delta/base arenas were reset in place without a new epoch.
6. Claim ceiling for benches remains architectural prototype / lab evidence until E3 durability
   closure says otherwise.

**Until acceptance: keep Alternative A in production.** Implementation work may proceed only behind
compile-time / runtime guards that do not change default paired Store behavior, or as isolated
prototype/tests that do not link into `glyphastored` defaults.

## Consequences

### Positive (if accepted and shipped later)

- Clear path to remove generation `shared_ptr` churn from `publish_incremental`.
- Fixed upper bound on concurrent live generations per lane.
- Forces explicit pin accounting for borrowed reads before they exist.

### Negative / risks

- Large patch surface in `shard_pair_runtime` and Reader adoption.
- High risk of subtle UAF or lost-wakeup reclaim bugs; TSAN alone is necessary but not sufficient.
- May not move single-op PUT to the 600 k ops/s target if wake/schedule remains ~2.7 µs of `ack`.
- Affine parallel PUT must be gated; ADR 0035 showed “allocator win” can reverse under contention.

### Deferred

- Production code landing.
- Changing default reclaim from ReadLease to reader-turn QSBR (only with proofs).
- Any freelist of DeltaPage/Block/Chunk (already rejected separately).

## Compatibility and migration

- **Disk / wire / routing:** no change.
- **Public API:** no change required for `Store::put` / `get` semantics; internal publish/reclaim only.
- **Embedded vs daemon:** one protocol for both, or an explicit documented dual-mode with identical
  RAW and UAF guarantees (dual-mode discouraged unless proofs share the same matrix).
- **Migration:** feature flag or build flag with default **off** until soak; no silent flip on upgrade.

## Verification

All items are **required** before accepting this ADR as *production-implemented*.
Prototype evidence closes lab gates only; production still uses Alternative A.

| # | Gate | Evidence required | Status |
| --- | --- | --- | --- |
| V1 | Litmus: publish release ↔ Reader acquire adopts one immutable generation | Unit / model or formal note + test | **prototype-evidence** — `ADR 0036 V1 prototype…` |
| V2 | Litmus: reclaim never frees epoch ≥ any active lease / pin / safe epoch | TSAN stress + fault injection | **prototype-evidence** — `ADR 0036 V2…` + V13 stress under `macos-tsan`; production TSAN soak still open |
| V3 | Two-boundary (or lease-equivalent) race: Reader mid-GET during publish+retire | Dedicated race test + TSAN | **prototype-evidence** — `ADR 0036 V3…` + V13 under `macos-tsan`; **production-baseline** — overwrite-storm under `macos-tsan` (`…/local-macos-2026-08-02-adr0036-v6-failclosed/`) |
| V4 | Cold I/O / `reader_safe_epoch` (or pin) holds reclaim across async completion | Integration | **prototype-evidence** (pin) — `paired slow-output pin…`; **production-baseline** — `paired Reader refreshes compacted durable pins…` + `durable cold read pin survives…` (`…/local-macos-2026-08-02-adr0036-v6-failclosed/`) |
| V5 | Shutdown drain: no UAF after stop; late mutations linearized or rejected fail-closed | Paired Store close tests | **prototype-evidence** + production close/linearize tests |
| V6 | Publication failure after Store mutation remains fail-closed | Existing paired fail-closed tests + new slot-exhaust case | **prototype-evidence** — `ADR 0036 V6…`; **production-baseline** — allocation-fault + paired durable litmus. Sync/async drain-snapshot + ACK-after-visibility/drain (no inverted RAW on catch, capture-fail, Index-visible `committed+error`, unprocessed batch items after drain, Index publish throw fail-close, catch-after-publish put+erase incl. volatile sync erase, or resolved-error catch overwrite); `writer_durable_through` mutex-synchronized; immutable published GET RAW after close. Slot-pool candidate still open |
| V7 | Incremental merge + post-cut publish under slot pressure | Pair read generation + Writer merge tests | **prototype-evidence** — `ADR 0036 V7 prototype…` (merge under pin pressure); production merge+slot open |
| V8 | Durable catalog refresh / rotation does not invalidate in-flight GET | Paired durable refresh tests | **production-baseline** — `paired Reader refreshes…` + `durable read catalog refresh…` (`…/local-macos-2026-08-02-adr0036-v6-v8-v14/`); slot-pool candidate still open |
| V9 | Pool exhaustion applies backpressure (no overwrite, no unbounded wait without admission bound) | Unit + overload | **prototype-evidence** — backpressure + bounded spin then `resource_exhausted`; recovery put |
| V10 | Same-key FIFO within `put_batch`; RAW after ACK | Existing paired Store batch tests | **production-baseline** (status-quo path) |
| V11 | Lab A/B vs status quo on Apple Silicon `macos-release` | Recorded under `benchmarks/results/` | open (requires production candidate) |
| V12 | **Hard reject** if worker-affine PUT 2t regresses >5% median vs interleaved baseline | Same as V11 | open |
| V13 | ASan/UBSan/TSAN clean on paired + publish stress | CI jobs | **prototype-partial** — `ADR 0036 V13…` + full `paired Store` green under `macos-tsan` (`…/local-macos-2026-08-02-adr0036-tsan/`) and `macos-asan` ASan+UBSan (`…/local-macos-2026-08-02-adr0036-asan/`); full multi-OS CI matrix still open |
| V14 | Crash / fault matrix unchanged for durability boundaries (no ACK-before-publish) | Crash harness subset | **production-baseline** — full sync `crash_persistence --mode matrix` (91 checkpoints) under `macos-release` (`…/local-macos-2026-08-02-adr0036-v14-sync-matrix/`); also crash-daemon pre-commit/post-ack + group-matrix. Slot-pool candidate still open |

Prototype reference (non-production): `src/experimental/paired_shard.cpp` (`publish`,
`reclaim_generations`, `acquire_generation_slot`, `adopt_publication`, `pin_read_generation`,
`stop_and_drain`).

## Compatibility with landed hot-path work

The following remain valid under Alternative A and must keep working under any slot-pool landing:

- `Store::put_batch` sync coalesce (≤32)
- `PublishedAdmission::caller_holds_guard`
- Embedded `DeltaState` in `make_shared` generation allocation
- Proportional reclaim quantum
- Phase scopes (`encode_copy`, `index_publish`, `worker_apply`, `publish`, `ack`)

## References

- [ADR 0031](paired-reader-writer-shards.md) — paired Reader–Writer; publication and reclaim
- [ADR 0032](0032-paired-concurrency-embedded-store.md) — embedded paired concurrency
- [ADR 0035](0035-generation-shell-recycling.md) — rejected shell freelist; deferred full slot pool
- [hot-path-performance-2026-08-01.md](../architecture/hot-path-performance-2026-08-01.md)
- [hot-path-rejected-optimizations.md](../architecture/hot-path-rejected-optimizations.md)
- [paired-shard-volatile-prototype.md](../architecture/paired-shard-volatile-prototype.md)
- Lab: `benchmarks/results/local-macos-2026-08-02-encode-size/`,
  `.../local-macos-2026-08-02-embed-delta/`, `.../local-macos-2026-08-02-gen-shell/`
