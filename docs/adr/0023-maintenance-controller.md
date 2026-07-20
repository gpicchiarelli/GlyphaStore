# ADR 0023: Optional Store MaintenanceController

- Status: accepted
- Date: 2026-07-20
- Deciders: persistence maintainers
- Applies to: embedded Store and glyphastored
- Amends: none
- Supersedes: none
- Related: ADR 0015

## Context

`Store::compact()` is a correct, crash-safe, non-queuing whole-Worker (durable) or selective
vacuum (volatile) primitive. It is cooperative: physical reclaim depends on an external caller.
Without regular compaction, dead bytes and sealed Segment count can grow toward resource limits.
Automatic loops that call `compact()` without budgets risk write amplification and fast-path
interference. The project needs an optional controller that schedules the existing primitive under
explicit modes and budgets without changing ownership, formats, or acknowledgement semantics.

## Decision drivers

- Preserve Worker ownership and the copy-build-validate-publish-retire transaction (ADR 0015).
- Embedded Store must default to no hidden threads.
- Daemon should be able to reclaim space without an external compact caller.
- At most one compaction per Store; never queue concurrent maintenance.
- Policy must be budgeted and observable; Phase 0 must not enable aggressive automatic compact.

## Alternatives considered

- Always-on aggressive background compact: rejected (fast-path and WA risk; hidden work in embedded).
- Parallel per-Worker compaction: rejected for v1 without experimental evidence.
- Rewriting the durable compaction transaction inside a new thread: rejected; reuse `Store::compact()`.
- Compaction only inside mutation paths: rejected (latency coupling; still cooperative for reads-only).

## Decision

1. Add `MaintenanceMode`: `cooperative` (default for embedded), `background`, `disabled`.
2. `glyphastored` defaults to `background`; CLI can override.
3. `MaintenanceController` is Store-owned. In `background` it runs one `std::jthread` with stop/join
   semantics aligned to `DurableFlushCoordinator`. Cooperative and disabled start no thread.
4. The controller never mutates Worker Index or Segments directly. Future policy may only observe
   published snapshots/stats and invoke `Store::compact()`, which already serializes on
   `compaction_mutex`.
5. Phase 3 enables automatic `Store::compact()` under **normal**, **pressure**, and **emergency**
   policies when mode is `background`. Under emergency, `put`/`erase` are rejected with
   `ErrorCode::storage_exhausted` while reads, flush, compact, and close continue.
6. Concurrent manual `compact()` and controller-driven compact share the same try-lock; conflicts
   return `sequence_conflict` without queuing.
7. `Store::close()` stops maintenance wake, waits for admitted operations (including in-flight
   compact), joins the controller, then proceeds with existing flush/runtime teardown.
8. Policies must not introduce unbounded queues; emergency never buffers rejected mutations.

## Consequences

Positive: clear lifecycle and config surface; daemon path ready for autosufficient reclaim; no format
change; compact remains the ground-truth transaction; emergency fails closed on capacity.

Negative / deferred: production reclaim benches remain follow-up. Background mode adds one Store
thread when enabled. Durable compaction crash/I/O matrices remain under ADR 0015.

## Compatibility and migration

- New optional `StoreConfig::maintenance` fields; defaults preserve cooperative embedded behavior.
- No disk, wire, or Manifest format change.
- Public API gains maintenance snapshot diagnostics; `Store::compact()` semantics unchanged.

## Verification

- Unit tests: cooperative starts no thread; background starts and joins on close; invalid intervals
  rejected; close with background mode is clean; pressure continues under no-gain budget; emergency
  rejects put/erase with `storage_exhausted` and recovers when watermarks clear; emergency gate
  survives compact fault; close/flush under emergency remain correct.
- Integration: durable catalog-driven emergency; Store close during blocked background compact drains
  then joins; manual `compact()` still works; concurrent compact still returns `sequence_conflict`.
- Remaining before claiming production automatic reclaim efficacy: production benches (not lifecycle).

## References

- [ADR 0015](0015-whole-worker-compaction.md)
- [Crash-safe durable compaction](../architecture/durable-compaction.md)
- [Maintenance controller](../architecture/maintenance-controller.md)
- [Public API contract](../architecture/public-api-contract.md)
