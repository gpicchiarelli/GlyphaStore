# MaintenanceController

Status: Phase 1 normal policy (ADR 0023)
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

## Phase 1 behavior (current)

In `background`, the controller:

1. Observes catalog-level stats (`MaintenanceObservation`: segment/sealed counts, free space) without
   touching Worker Index or mutable Segments.
2. Under **normal** policy only:
   - skips when durable sealed count is zero (`no_candidate`);
   - backs off after `max_no_gain_attempts` consecutive empty compact results (`budget` / `suspended`);
   - respects `max_copy_bytes_per_cycle` when nonzero;
   - otherwise calls `Store::compact()` (existing mutex + round-robin path).
3. Pressure and emergency policies remain Phase 2/3.

White-box tests may call `set_auto_compact_enabled(false)` to keep the eval thread without compact.

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
