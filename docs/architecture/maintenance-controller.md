# MaintenanceController

Status: Phase 5 critical fail-closed + lifecycle; Phase 6 wire retry honesty (ADR 0023 / 0019)
Applies to: embedded Store and glyphastored
Owner: persistence maintainers
Last reviewed: 2026-07-23

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

## Phase 5 behavior (current)

In `background`, the controller:

1. Observes catalog-level stats (`MaintenanceObservation`) without scanning Worker Indexes or
   reading Segment Records. Recovery and already-serialized Index publication, lazy expiry,
   rotation, and compaction maintain exact active/sealed Index-referenced byte counters. The
   observation selects the next round-robin Worker with sealed history and reports its sealed,
   live, dead, and dead-ratio counters. Observation still runs when auto-compact is disabled so
   emergency admission stays honest.
2. Classifies pressure via `classify_maintenance_pressure` (highest severity wins):
   - **emergency** when `segment_count >= max_segment_count`, or when available free bytes cannot
     cover `reserved_free_bytes + rotate_additional_bytes` (create/rotate at risk; catalog sets
     `rotate_additional_bytes` to Segment + next manifest size);
   - **segment pressure** when `segment_count >= ceil(max_segment_count * segment_count_pressure_pct / 100)`;
   - **free-space pressure** when `available_free_bytes <= reserved_free_bytes + free_bytes_pressure_margin`
     and emergency does not already apply. With default margin `0`, free-space pressure is only
     reachable when margin is raised above rotate headroom.
3. **Normal** policy: skip `no_candidate`; skip a candidate whose dead ratio is below the inclusive
   `dead_byte_ratio_bp_normal` threshold; backoff after `max_no_gain_attempts`; preflight the
   candidate's exact Index-referenced live Record bytes against `max_copy_bytes_per_cycle`; mid eval
   interval. The selected Worker identity is passed to the automatic compact call so policy and
   execution address the same round-robin candidate. The default limit is 128 MiB per evaluation
   (at most one compaction); equality is allowed and zero explicitly disables the limit. A rejected
   candidate reports `copy_budget`, distinct from no-gain `budget_backoff`. The durable runtime
   rechecks the same inclusive limit while holding the Worker snapshot lock; growth between
   observation and snapshot returns a finite `sequence_conflict` before scan or copy.
4. **Pressure / emergency** policy: use `min_eval_interval_ms`; continue compact attempts despite
   no-gain / copy budgets; record activation reason (`segment_pressure` / `free_space_pressure` /
   `emergency_capacity`).
5. **Emergency mutation gate**: publish `mutations_rejected`; `Store::put` / `Store::erase` (and
   exclusive `StoreAccess` paths) return `ErrorCode::storage_exhausted` with a stable message.
   `get`, `flush`, `compact`, and `close` continue. No mutation queue.
6. **Fail-closed on reclaim fault**: compact/observe faults **keep** an already-published emergency
   gate. While the gate is armed, auto-compact is **not** latched off — reclaim keeps retrying under
   budget. Non-emergency faults still disable auto-compact. `faulted` remains sticky for telemetry
   until a later evaluation recovers or `request_stop` runs. The gate clears only on a later
   non-emergency observation, `unavailable`/`store_closed`, or `request_stop`/`join`.
7. Still at most one `Store::compact()` in flight (shared try-lock).
8. Background start requests an immediate first evaluation (no mid-interval blind window).
9. Free-space emergency uses `rotate_additional_bytes` from catalog observation (Segment + next
   manifest), matching `rotate_active` headroom.

Telemetry in `MaintenanceSnapshot` includes pressure level, `mutations_rejected`, activation reason,
eval/compact durations, bytes/records copied, `expired_records_dropped` (last and total), suspend
count, time since last useful compaction, candidate Worker, candidate sealed/live/dead Record bytes,
and dead ratio in basis points. Durable snapshots also include rotation attempts, commits,
compaction waits, final-Record attempts/commits, and last/total/maximum durations for publication
wait, Segment seal, replacement Segment creation, Manifest publication, aggregate execution,
complete rotation, and the post-rotation final Record commit. Daemon `STATS` exports both candidate
and rotation counters.

The live-byte counter means “currently Index-referenced,” not “guaranteed unexpired at observation
time.” Expiry discovered by validated GET immediately updates it; cold, unread TTL entries remain
conservatively live until GET, recovery, or a pressure-triggered compaction visits the Record.

Wire note: the Reactor maps `storage_exhausted` to `ResponseStatus::overloaded` (existing
many-to-one collapse with admission limits). Official clients advertise `retryability=never` for
`overloaded` because the wire cannot distinguish capacity exhaustion from queue pressure. Applications
that still want backoff-retry for true admission overload must opt in explicitly.

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

The automatic candidate cursor advances when a Worker is observed, not only when compaction starts.
Threshold, copy-budget, or other policy skips therefore cannot pin evaluation to one Worker and
starve reclaimable peers. `MaintenanceSnapshot::sequence_conflicts` and daemon
`maintenance_sequence_conflicts` count compact attempts rejected by concurrent state change.

## Explicitly deferred

- Production reclaim tuning: expose rejected-plan work and decide whether unread TTL needs a
  bounded normal-mode probe independent of pressure. The first
  isolated benefit/cost measurement is recorded in
  the [2026-07-23 durable compaction benchmark](../benchmarks/durable-compaction-2026-07-23.md);
  the clean [concurrent-maintenance follow-up](../benchmarks/concurrent-maintenance-2026-07-23.md)
  measures a roughly 18% median throughput cost and 54--57% p99 increase while useful reclaim
  overlaps foreground work. The second
  [rotation/idle/churn follow-up](../benchmarks/maintenance-rotation-idle-churn-2026-07-23.md)
  measures condition-wait latency, product-default and aggressive idle CPU, and seven validated
  1 GiB churn samples. The
  [macOS phase attribution](../benchmarks/maintenance-rotation-phases-macos-2026-07-23.md) then
  separates publication wait, rotation execution, and residual PUT time. Runtime telemetry now also
  splits Segment seal, replacement creation, Manifest publication, residual in-memory execution,
  and the post-rotation final Record commit. The
  [deep-phase macOS matrix](../benchmarks/maintenance-rotation-deep-phases-macos-2026-07-23.md)
  localizes 65--72% of forced rotation execution to replacement creation; controlled-hardware
  evidence remains.
- Shorter compaction publication leases. Measure the deeper rotation phases first; only then decide
  whether replacement Segment construction should move before publication authority, with
  generation revalidation/rebase at commit.
- Native power-loss certification (owned by ADR 0015 compaction transaction, not this scheduler).
