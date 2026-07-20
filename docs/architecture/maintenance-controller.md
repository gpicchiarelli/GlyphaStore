# MaintenanceController

Status: Phase 4 lifecycle + emergency fail-closed (ADR 0023)
Applies to: embedded Store and glyphastored
Owner: persistence maintainers
Last reviewed: 2026-07-20

# Overview

`MaintenanceController` optionally schedules Store physical maintenance. It does **not** replace
`Store::compact()`. The durable whole-Worker transaction and volatile selective vacuum remain the
sole compaction primitives (ADR 0015).

## Modes

| Mode | Embedded default | Daemon default | Thread |
| --- | --- | --- | --- |
| `cooperative` | yes | no | none; caller invokes `compact()` |
| `background` | no | yes | one Store-owned `std::jthread` |
| `disabled` | no | no | none; `compact()` still available |

## Phase 4 behavior (current)

In `background`, the controller:

1. Observes catalog-level stats (`MaintenanceObservation`) without touching Worker Index or mutable
   Segments. Observation still runs when auto-compact is disabled so emergency admission stays honest.
2. Classifies pressure via `classify_maintenance_pressure` (highest severity wins):
   - **emergency** when `segment_count >= max_segment_count`, or when available free bytes cannot
     cover `reserved_free_bytes + kSegmentSizeBytes` (create/rotate at risk);
   - **segment pressure** when `segment_count >= ceil(max_segment_count * segment_count_pressure_pct / 100)`;
   - **free-space pressure** when `available_free_bytes <= reserved_free_bytes + free_bytes_pressure_margin`.
     With the default `free_bytes_pressure_margin = 0`, free-space pressure collapses into the
     emergency free-space rule unless operators raise the margin above `kSegmentSizeBytes`.
3. **Normal** policy: skip `no_candidate`; backoff after `max_no_gain_attempts`; honor
   `max_copy_bytes_per_cycle`; mid eval interval.
4. **Pressure / emergency** policy: use `min_eval_interval_ms`; continue compact attempts despite
   no-gain / copy budgets; record activation reason (`segment_pressure` / `free_space_pressure` /
   `emergency_capacity`).
5. **Emergency mutation gate**: publish `mutations_rejected`; `Store::put` / `Store::erase` (and
   exclusive `StoreAccess` paths) return `ErrorCode::storage_exhausted` with a stable message.
   `get`, `flush`, `compact`, and `close` continue. No mutation queue.
6. **Fail-closed on reclaim fault**: compact/observe faults that disable auto-compact **keep** an
   already-published emergency gate. `MaintenanceState::faulted` is sticky until a later evaluation
   recovers or `request_stop` runs. The gate clears only on a later non-emergency observation,
   `unavailable`/`store_closed`, or `request_stop`.
7. Still at most one `Store::compact()` in flight (shared try-lock).

Telemetry in `MaintenanceSnapshot` includes pressure level, `mutations_rejected`, activation reason,
eval/compact durations, bytes/records copied, suspend count, and time since last useful compaction.

Wire note: the Reactor maps `storage_exhausted` to `ResponseStatus::overloaded` (existing
many-to-one). Clients may retry; that is intentional for v1 wire reuse, not a silent success.

## Shutdown

`Store::close()`:

1. stops admission of new Store operations;
2. `MaintenanceController::request_stop()` (clears callbacks, clears the mutation gate, wakes the
   thread);
3. requests durable flush and waits until no admitted operations remain (including an in-flight
   `compact()`);
4. `join()`s the maintenance thread;
5. closes the durable runtime and releases Store resources.

## Concurrency

Manual and automatic compact share `compaction_mutex` with `try_to_lock`. A busy gate returns
`sequence_conflict`. There is no compaction queue.

## Explicitly deferred

- Production reclaim benches (idle overhead vs cooperative; long sealed-churn reclaim efficacy).
- Multi-output randomized crash histories and power-loss certification (owned by ADR 0015 compaction
  transaction, not this scheduler).
