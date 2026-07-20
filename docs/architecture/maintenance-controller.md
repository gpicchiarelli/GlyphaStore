# MaintenanceController

Status: Phase 2 normal + pressure (ADR 0023)
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

## Phase 2 behavior (current)

In `background`, the controller:

1. Observes catalog-level stats (`MaintenanceObservation`) without touching Worker Index or mutable
   Segments.
2. Classifies pressure via `classify_maintenance_pressure`:
   - **segment pressure** when `segment_count >= ceil(max_segment_count * segment_count_pressure_pct / 100)`;
   - **free-space pressure** when `available_free_bytes <= reserved_free_bytes + free_bytes_pressure_margin`.
3. **Normal** policy: skip `no_candidate`; backoff after `max_no_gain_attempts`; honor
   `max_copy_bytes_per_cycle`; mid eval interval.
4. **Pressure** policy: use `min_eval_interval_ms`; continue compact attempts despite no-gain / copy
   budgets; record activation reason (`segment_pressure` / `free_space_pressure`).
5. Still at most one `Store::compact()` in flight (shared try-lock). Emergency reject of mutations is
   Phase 3.

Telemetry in `MaintenanceSnapshot` includes pressure level, activation reason, eval/compact
durations, bytes/records copied, suspend count, and time since last useful compaction.

## Shutdown

`Store::close()`:

1. stops admission of new Store operations;
2. `MaintenanceController::request_stop()` (clears callbacks, wakes the thread);
3. requests durable flush and waits until no admitted operations remain (including an in-flight
   `compact()`);
4. `join()`s the maintenance thread;
5. closes the durable runtime and releases Store resources.

## Concurrency

Manual and automatic compact share `compaction_mutex` with `try_to_lock`. A busy gate returns
`sequence_conflict`. There is no compaction queue.
