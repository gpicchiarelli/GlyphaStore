Status: normative policy for 0.1.x
Applies to: embedded `Store::open` concurrency modes
Owner: storage / concurrency maintainers
Last reviewed: 2026-08-01
Requirements: `GS-CONCUR-PAIR-001`, `GS-CONCUR-LEGACY-001`
Authority: [ADR 0032](../adr/0032-paired-concurrency-embedded-store.md)

# Policy: `legacy_mutex` (temporary through 0.2)

## 1. Purpose

`StoreConcurrencyMode::legacy_mutex` is a **temporary compatibility escape hatch** for embedded
Store callers that cannot yet adopt the default paired Reader–Writer runtime (ADR 0031 / 0032).
It is **not** a second product runtime and must not grow feature surface.

## 2. Lifetime

| Phase | Rule |
| --- | --- |
| 0.1.x | Deprecated but supported for embedded `Store::open` when explicitly selected. |
| 0.2 | **Removed.** Opening with `legacy_mutex` must fail closed (clear error). |
| Daemon (`glyphastored`) | Paired-only for the entire 0.1.x line; never offers `legacy_mutex`. |

Experimental `src/experimental/paired_*` remains lab/microbench only and is not a selectable
product runtime (ADR 0032).

## 3. Feature freeze

While `legacy_mutex` exists, maintainers **must not**:

- add public API, storage, or wire behavior that works only under `legacy_mutex`;
- document performance commitments unique to the mutex path;
- extend white-box tests that couple new product features to Worker mutex internals, except for
  security/correctness/recovery fixes that also apply to paired mode.

Allowed work on the legacy path:

- correctness, security, and recovery fixes with paired equivalence where applicable;
- test and CI support for the equivalence matrix below;
- deprecation warnings and migration guidance.

## 4. Deprecation

1. API: `StoreConcurrencyMode::legacy_mutex` remains marked deprecated in
   [`config.hpp`](../../include/glyphastore/store/config.hpp) and public docs.
2. Docs: glossary, public API contract, worker model, and this policy state removal in 0.2.
3. Release notes: each 0.1.x release that still ships the hatch must mention the 0.2 removal date
   intent.
4. Mixing: configuration is open-time immutable and public callers select exactly one enum value.
   There is no supported operation that changes the mode of a live Store; any detected internal
   ownership violation fails closed (ADR 0032).

## 5. Equivalence matrix (observable outcomes)

Paired (default) and `legacy_mutex` must agree on **public** outcomes for the same inputs under
identical `StoreConfig` fields other than `concurrency`:

| Class | Required agreement |
| --- | --- |
| Sequential volatile | `put` / overwrite / `get` / `erase` / TTL expiry visibility |
| Concurrent distinct keys | Final per-key values after concurrent puts |
| Concurrent same key | Last writer wins; index verify succeeds |
| Close | Open → traffic → `close` drain completes without lost acknowledged volatile puts |
| Durable reopen (selected) | Persistence v1 / wire v2 bytes and reopen visibility unchanged across modes |

Explicitly **out of scope** for equivalence (paired-only or legacy-only internals):

- `ReadGeneration` leases, Writer queue metrics, bounded lane depth;
- daemon async pair queues (daemon has no legacy mode);
- crash/TSan harnesses that intentionally pin `legacy_mutex` to keep startup noise bounded;
- white-box Worker mutable-state inspection tests.

Proof: `tests/integration/runtime_equivalence_tests.cpp` (`GS-CONCUR-LEGACY-001`).

## 6. Migration

Callers on `legacy_mutex` should:

1. open with default `StoreConcurrencyMode::paired` (or omit the field);
2. keep owning `get` / `OwnedValue` usage (ADR 0009 unchanged);
3. tolerate Writer-queue latency under multi-thread `put`/`erase` bursts;
4. drop any dependency on Worker-mutex fairness.

## 7. Residual risk

Until 0.2 removal, dual-path maintenance cost remains. Equivalence tests cover public outcomes, not
full linearizability formalization (Phase B1). No production-readiness claim attaches to either
path while quality gates remain below `ACCETTATA_PER_RILASCIO`.
